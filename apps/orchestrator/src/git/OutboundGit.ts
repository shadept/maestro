import {
  BranchDivergedError,
  type DbError,
  type ForgeError,
  type GitError,
  type Project,
  type Session,
  type TaskContext,
} from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import type { PrProposal } from "../agent/AgentContract.ts";
import { AppConfig } from "../config/AppConfig.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { GitHubForge } from "../forge/GitHubForge.ts";
import { worktreeDir } from "../storage/paths.ts";
import { GitCache } from "./GitCache.ts";
import { forgeCredentials, type GitCommandOptions, runGit } from "./git-command.ts";
import { RepoLocks } from "./RepoLocks.ts";

export type PublishError = GitError | ForgeError | DbError;

export type PublishOutcome =
  /** No new commits (or nothing changed since the last publish) — skipped silently. */
  | { readonly _tag: "NothingToPublish" }
  | {
      readonly _tag: "Published";
      readonly prNumber: number;
      readonly prUrl: string;
      /** True when this publish opened the draft PR (first push). */
      readonly prCreated: boolean;
    };

/**
 * What the locked git phase decided; the forge phase runs outside the lock.
 * The fourth classification — remote exists but DIVERGED from the local tip
 * (e.g. GitHub's "Update branch" added a merge commit) — never leaves the
 * locked phase: a clean reconciliation merge collapses it into "synced", a
 * conflicted one fails the phase with BranchDivergedError.
 */
type SyncResult = "nothing" | "up-to-date" | "synced";

// The agent proposes its own PR title/description (trailing block of its
// final message, extracted by AgentContract); the ticket-derived fallback
// covers proposal-less turns. Either way the body ends with a footer carrying
// the ticket identifier — Linear's GitHub integration turns the magic word
// into a bidirectional link, and generic sources get a readable reference.
// The title is prefixed with the identifier unless the agent already did so.
const prTitle = (session: Session, context: TaskContext, proposal: PrProposal | null): string => {
  const id = session.ticketReference.externalId;
  if (proposal !== null && proposal.title.length > 0) {
    return proposal.title.toLowerCase().includes(id.toLowerCase())
      ? proposal.title
      : `${id}: ${proposal.title}`;
  }
  return context.title !== null && context.title.length > 0 ? `${id}: ${context.title}` : id;
};

const prFooter = (session: Session): string =>
  [
    "---",
    `Ticket: ${session.ticketReference.externalId} · Maestro session: ${session.id}`,
    "🤖 Opened by Maestro.",
  ].join("\n");

const prBody = (session: Session, proposal: PrProposal | null): string =>
  proposal !== null && proposal.body.length > 0
    ? `${proposal.body}\n\n${prFooter(session)}`
    : `Automated pull request opened by Maestro for ${session.ticketReference.externalId}.\n\n${prFooter(session)}`;

/**
 * Outbound half of the git story (PRD §3.2): workers commit locally only; the
 * orchestrator pushes with its own credentials and manages the PR. `publish`
 * is idempotent — replaying it after any partial failure converges (push is
 * skipped when the remote is current, the forge call self-heals a lost PR).
 */
export class OutboundGit extends Context.Service<
  OutboundGit,
  {
    /**
     * Detects new commits on the session branch; if any, pushes the branch to
     * origin and creates (first push, draft) or updates the session's PR.
     * No-commit turns return NothingToPublish without touching the remote.
     * A remote branch that diverged (a human updated the PR branch) is
     * reconciled by merging it into the local branch first — never a force
     * push; a conflicted merge is aborted and fails with BranchDivergedError.
     */
    readonly publish: (args: {
      readonly session: Session;
      readonly project: Project;
      readonly context: TaskContext;
      /** Agent-authored PR title/description, when its final message carried the block. */
      readonly proposal?: PrProposal | null;
    }) => Effect.Effect<PublishOutcome, PublishError>;
  }
>()("maestro/git/OutboundGit") {
  static readonly layer = Layer.effect(
    OutboundGit,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const gitCache = yield* GitCache;
      const repoLocks = yield* RepoLocks;
      const sessionRepo = yield* SessionRepo;
      const forge = yield* GitHubForge;

      const localSha = (cwd: string, branch: string) =>
        runGit(["rev-parse", "--verify", "--quiet", `refs/heads/${branch}`], { cwd }).pipe(
          Effect.map((sha): string | null => sha),
          // exit 1 = ref does not exist; anything else is a real failure
          Effect.catch((error) =>
            error.exitCode === 1 ? Effect.succeed(null) : Effect.fail(error),
          ),
        );

      const remoteSha = (options: GitCommandOptions, branch: string) =>
        runGit(["ls-remote", "origin", `refs/heads/${branch}`], options).pipe(
          Effect.map((out): string | null =>
            out.length === 0 ? null : (out.split(/\s+/)[0] ?? null),
          ),
        );

      const isAncestor = (cwd: string, ancestor: string, descendant: string) =>
        runGit(["merge-base", "--is-ancestor", ancestor, descendant], { cwd }).pipe(
          Effect.as(true),
          // exit 1 = not an ancestor; anything else is a real failure
          Effect.catch((error) =>
            error.exitCode === 1 ? Effect.succeed(false) : Effect.fail(error),
          ),
        );

      /**
       * The remote session branch moved without us (a human pressed GitHub's
       * "Update branch", or pushed to the PR branch) — the deliberately
       * non-forced push would be rejected as non-fast-forward. Reconcile by
       * merging the remote tip INTO the local branch in the session worktree
       * (the remote history is never rewritten, never force-pushed). Runs
       * inside the caller's repo lock: the fetch writes refs/remotes in the
       * bare repo and the merge moves refs/heads/<branch> + the worktree.
       */
      const reconcileDiverged = Effect.fn("OutboundGit.reconcileDiverged")(function* (args: {
        readonly session: Session;
        readonly gitDir: string;
        readonly remoteOptions: GitCommandOptions;
      }) {
        const branch = args.session.gitBranch;
        // NEVER fetch into refs/heads/<branch>: the branch is checked out in
        // the session worktree, so git refuses. refs/remotes is safe in the
        // bare repo and gives the merge a stable name (FETCH_HEAD would land
        // in the bare repo's gitdir, not the worktree's). `--refmap=` is
        // load-bearing: without it the clone's mirror fetch refspec
        // (+refs/heads/*:refs/heads/*) opportunistically maps the fetched
        // branch back onto its checked-out head — the same refusal.
        const remoteRef = `refs/remotes/origin/${branch}`;
        yield* runGit(
          ["fetch", "--refmap=", "origin", `+refs/heads/${branch}:${remoteRef}`],
          args.remoteOptions,
        );
        if (yield* isAncestor(args.gitDir, remoteRef, `refs/heads/${branch}`)) {
          return; // race: the "divergence" was history we already have — plain push suffices
        }
        const worktreePath = worktreeDir(config.storageRoot, args.session.id);
        yield* runGit(
          [
            // First consumer of the configured Maestro commit identity: the
            // merge commit is orchestrator-made, not agent-made.
            "-c",
            `user.name=${config.gitAuthorName}`,
            "-c",
            `user.email=${config.gitAuthorEmail}`,
            "merge",
            "--no-edit",
            "-m",
            `Merge remote-tracking branch 'origin/${branch}' into ${branch}`,
            remoteRef,
          ],
          { cwd: worktreePath },
        ).pipe(
          Effect.catch((mergeError) =>
            // Leave the worktree pristine on the local tip; a half-merged
            // worktree would poison the next agent turn. Abort is best-effort:
            // some merge failures (dirty worktree) never start a merge.
            runGit(["merge", "--abort"], { cwd: worktreePath }).pipe(
              Effect.ignore,
              Effect.andThen(
                Effect.fail(
                  new BranchDivergedError({
                    branch,
                    message:
                      `session branch '${branch}' has diverged from origin (someone updated the PR branch) ` +
                      `and the automatic merge hit conflicts (${mergeError.stderr.trim() || mergeError.command}). ` +
                      `To resolve: comment on the ticket mentioning @${config.linearMentionHandle} and ask it to ` +
                      `merge origin/${branch} into the session branch and resolve the conflicts.`,
                  }),
                ),
              ),
            ),
          ),
        );
      });

      return {
        publish: Effect.fn("OutboundGit.publish")(function* (args: {
          readonly session: Session;
          readonly project: Project;
          readonly context: TaskContext;
          /** Agent-authored PR title/description, when its final message carried the block. */
          readonly proposal?: PrProposal | null;
        }) {
          const { session, project, context } = args;
          const proposal = args.proposal ?? null;
          const gitDir = gitCache.cachePathFor(project);
          const branch = session.gitBranch;
          const credentials = forgeCredentials(config.githubToken);
          const remoteOptions: GitCommandOptions = {
            cwd: gitDir,
            ...(credentials !== undefined && { credentials }),
          };
          // read-only, and RepoLocks is not reentrant — resolve before locking
          const baseBranch = yield* gitCache.baseBranch(project);

          // Locked: push writes local refs too — the mirror fetch refspec maps
          // the pushed branch's tracking ref back onto refs/heads/*, and git
          // fails fast on ref/packed-refs lock contention instead of waiting.
          // Same collision class as worktree adds, so same per-project mutex.
          const sync: SyncResult = yield* repoLocks.withRepoLock(project.id)(
            Effect.gen(function* () {
              const local = yield* localSha(gitDir, branch);
              if (local === null) return "nothing" as const; // branch never provisioned
              const remote = yield* remoteSha(remoteOptions, branch);
              if (remote === null) {
                const ahead = yield* runGit(["rev-list", "--count", `${baseBranch}..${branch}`], {
                  cwd: gitDir,
                });
                if (Number(ahead) === 0) return "nothing" as const; // no-commit turn, never pushed
              } else if (remote === local) {
                // remote current; only proceed to heal a lost PR record
                return session.prNumber === null ? ("synced" as const) : ("up-to-date" as const);
              } else {
                // Remote at a different tip. The common case — the remote tip
                // is the previous turn's push, i.e. already in local history —
                // needs no reconciliation (the push below fast-forwards) and
                // is decided locally: we pushed that sha, so we have it. Any
                // other shape is divergence, the fourth sync state.
                const remoteKnown = yield* runGit(["cat-file", "-e", `${remote}^{commit}`], {
                  cwd: gitDir,
                }).pipe(
                  Effect.as(true),
                  Effect.catch(() => Effect.succeed(false)),
                );
                const fastForward =
                  remoteKnown && (yield* isAncestor(gitDir, remote, `refs/heads/${branch}`));
                if (!fastForward) {
                  yield* reconcileDiverged({ session, gitDir, remoteOptions });
                }
              }
              yield* runGit(
                ["push", "origin", `refs/heads/${branch}:refs/heads/${branch}`],
                remoteOptions,
              );
              return "synced" as const;
            }),
          );

          if (sync !== "synced") {
            return { _tag: "NothingToPublish" } as const;
          }

          const pr = yield* forge.ensurePullRequest({
            repoGitUrl: project.repoGitUrl,
            headBranch: branch,
            baseBranch,
            title: prTitle(session, context, proposal),
            body: prBody(session, proposal),
            // Non-draft by default since 2026-07-13 (operator decision): the
            // agent's Result means "ready" — draftPr: true remains available
            // as a per-project override for cautious repos.
            draft: project.gitConventions.draftPr ?? false,
            existingNumber: session.prNumber,
          });

          if (session.prNumber !== pr.number || session.prUrl !== pr.url) {
            yield* sessionRepo.setPullRequest(session.id, { number: pr.number, url: pr.url });
          }
          return {
            _tag: "Published",
            prNumber: pr.number,
            prUrl: pr.url,
            prCreated: pr.created,
          } as const;
        }),
      };
    }),
  );
}
