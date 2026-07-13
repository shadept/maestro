import type { DbError, ForgeError, GitError, Project, Session, TaskContext } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import type { PrProposal } from "../agent/AgentContract.ts";
import { AppConfig } from "../config/AppConfig.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { GitHubForge } from "../forge/GitHubForge.ts";
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

/** What the locked git phase decided; the forge phase runs outside the lock. */
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
