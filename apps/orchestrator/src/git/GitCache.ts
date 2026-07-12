import { access, mkdir } from "node:fs/promises";
import path from "node:path";
import type { GitError, Project } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { repoCacheDir } from "../storage/paths.ts";
import { type GitCredentials, runGit } from "./git-command.ts";
import { RepoLocks } from "./RepoLocks.ts";

const exists = (p: string) =>
  Effect.promise(() =>
    access(p).then(
      () => true,
      () => false,
    ),
  );

export class GitCache extends Context.Service<
  GitCache,
  {
    /**
     * Ensures the bare master clone for a project exists under the storage
     * root and returns its path. Idempotent. Credentials are injected for
     * this invocation only; the stored config keeps the credential-free URL.
     */
    readonly ensureClone: (
      project: Project,
      credentials?: GitCredentials,
    ) => Effect.Effect<string, GitError>;
    /**
     * Updates ONLY the project's base branch from origin, with an explicit
     * `+refs/heads/<base>:refs/heads/<base>` refspec. Deliberately NOT the
     * stored mirror refspec: a bare `git fetch origin` force-updates every
     * head, which (verified, git 2.54) hard-fails with "refusing to fetch
     * into branch ... checked out at <worktree>" as soon as a session branch
     * exists on origin — every session branch is checked out in a linked
     * worktree — and a forced all-heads update could rewind a session branch
     * whose local state is ahead of the remote (e.g. after a failed publish).
     * The base is always safe to update: session branches come from
     * branchNameFor's pattern, so no session worktree ever checks out the base.
     */
    readonly fetchBase: (
      project: Project,
      credentials?: GitCredentials,
    ) => Effect.Effect<void, GitError>;
    /**
     * The branch session branches are cut from (and PRs target): the
     * project's configured base branch, else the cached clone's default
     * branch (symbolic HEAD). Single source of truth for this resolution.
     */
    readonly baseBranch: (project: Project) => Effect.Effect<string, GitError>;
    readonly cachePathFor: (project: Project) => string;
  }
>()("maestro/git/GitCache") {
  static readonly layer = Layer.effect(
    GitCache,
    Effect.gen(function* () {
      const { storageRoot } = yield* AppConfig;
      const repoLocks = yield* RepoLocks;
      const cachePathFor = (project: Project) => repoCacheDir(storageRoot, project.id);

      const baseBranch = Effect.fn("GitCache.baseBranch")(function* (project: Project) {
        if (project.gitConventions.baseBranch !== undefined) {
          return project.gitConventions.baseBranch;
        }
        const ref = yield* runGit(["symbolic-ref", "HEAD"], { cwd: cachePathFor(project) });
        return ref.replace(/^refs\/heads\//, "");
      });

      return {
        cachePathFor,
        ensureClone: Effect.fn("GitCache.ensureClone")(function* (
          project: Project,
          credentials?: GitCredentials,
        ) {
          // Locked including the existence probe: two concurrent ensureClones
          // would otherwise both see a missing clone and race `git clone`.
          return yield* repoLocks.withRepoLock(project.id)(
            Effect.gen(function* () {
              const cachePath = cachePathFor(project);
              if (yield* exists(path.join(cachePath, "HEAD"))) {
                return cachePath;
              }
              yield* Effect.promise(() => mkdir(path.dirname(cachePath), { recursive: true }));
              yield* runGit(
                ["clone", "--bare", project.repoGitUrl, cachePath],
                credentials ? { credentials } : {},
              );
              // Bare clones get no fetch refspec; keep local heads mirroring origin's.
              yield* runGit(["config", "remote.origin.fetch", "+refs/heads/*:refs/heads/*"], {
                cwd: cachePath,
              });
              return cachePath;
            }),
          );
        }),
        fetchBase: Effect.fn("GitCache.fetchBase")(function* (
          project: Project,
          credentials?: GitCredentials,
        ) {
          const cwd = cachePathFor(project);
          // read-only resolution stays outside the lock — RepoLocks is not reentrant
          const base = yield* baseBranch(project);
          // fetch rewrites refs/heads/<base> — locked so it cannot collide
          // with worktree/branch mutations on the same repo.
          yield* repoLocks.withRepoLock(project.id)(
            runGit(
              ["fetch", "origin", `+refs/heads/${base}:refs/heads/${base}`],
              credentials ? { cwd, credentials } : { cwd },
            ),
          );
        }),
        baseBranch,
      };
    }),
  );
}
