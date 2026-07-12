import { access, rm } from "node:fs/promises";
import type { GitError, Project, Session, TicketReference } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { repoCacheDir, worktreeDir } from "../storage/paths.ts";
import { GitCache } from "./GitCache.ts";
import { runGit } from "./git-command.ts";

/**
 * Computes the session branch from the project's convention overrides.
 * Used at session creation (ingest) — the result is stored on the Session.
 */
export const branchNameFor = (ticket: TicketReference, project: Project): string =>
  (project.gitConventions.branchPattern ?? "maestro/{ticketKey}").replaceAll(
    "{ticketKey}",
    ticket.externalId,
  );

export interface WorktreePaths {
  /** Mounted read-write into the worker. */
  readonly worktreePath: string;
  /** The parent (bare) git dir, mounted read-only per PRD §3.2. */
  readonly gitDir: string;
}

const exists = (p: string) =>
  Effect.promise(() =>
    access(p).then(
      () => true,
      () => false,
    ),
  );

export class WorktreeManager extends Context.Service<
  WorktreeManager,
  {
    /**
     * Provisions (or reuses — DORMANT sessions rehydrate) the session's
     * isolated worktree on its branch. The branch is created from the
     * project's base branch on first provision.
     */
    readonly provision: (args: {
      readonly session: Session;
      readonly project: Project;
    }) => Effect.Effect<WorktreePaths, GitError>;
    /** Worktree remove + branch cleanup. Idempotent — safe on partial state. */
    readonly remove: (args: {
      readonly session: Session;
      readonly project: Project;
    }) => Effect.Effect<void, GitError>;
    readonly pathsFor: (args: {
      readonly session: Session;
      readonly project: Project;
    }) => WorktreePaths;
  }
>()("maestro/git/WorktreeManager") {
  static readonly layer = Layer.effect(
    WorktreeManager,
    Effect.gen(function* () {
      const { storageRoot } = yield* AppConfig;
      const gitCache = yield* GitCache;

      const pathsFor = ({ session, project }: { session: Session; project: Project }) => ({
        worktreePath: worktreeDir(storageRoot, session.id),
        gitDir: repoCacheDir(storageRoot, project.id),
      });

      const branchExists = (cwd: string, branch: string) =>
        runGit(["rev-parse", "--verify", "--quiet", `refs/heads/${branch}`], { cwd }).pipe(
          Effect.as(true),
          Effect.catch(() => Effect.succeed(false)),
        );

      return {
        pathsFor,
        provision: Effect.fn("WorktreeManager.provision")(function* (args: {
          session: Session;
          project: Project;
        }) {
          const paths = pathsFor(args);
          if (yield* exists(paths.worktreePath)) {
            return paths;
          }
          const branch = args.session.gitBranch;
          if (yield* branchExists(paths.gitDir, branch)) {
            // crash recovery / re-provision after eviction that lost the dir
            yield* runGit(["worktree", "add", paths.worktreePath, branch], {
              cwd: paths.gitDir,
            });
          } else {
            const base =
              args.project.gitConventions.baseBranch ??
              (yield* gitCache.defaultBranch(args.project));
            yield* runGit(["worktree", "add", "-b", branch, paths.worktreePath, base], {
              cwd: paths.gitDir,
            });
          }
          return paths;
        }),
        remove: Effect.fn("WorktreeManager.remove")(function* (args: {
          session: Session;
          project: Project;
        }) {
          const paths = pathsFor(args);
          if (yield* exists(paths.worktreePath)) {
            yield* runGit(["worktree", "remove", "--force", paths.worktreePath], {
              cwd: paths.gitDir,
            });
          } else {
            // directory already gone — clear stale metadata if any
            yield* runGit(["worktree", "prune"], { cwd: paths.gitDir });
          }
          yield* Effect.promise(() => rm(paths.worktreePath, { recursive: true, force: true }));
          if (yield* branchExists(paths.gitDir, args.session.gitBranch)) {
            yield* runGit(["branch", "-D", args.session.gitBranch], { cwd: paths.gitDir });
          }
        }),
      };
    }),
  );
}
