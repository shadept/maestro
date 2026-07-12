import { access, mkdir } from "node:fs/promises";
import path from "node:path";
import type { GitError, Project } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { repoCacheDir } from "../storage/paths.ts";
import { type GitCredentials, runGit } from "./git-command.ts";

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
    readonly fetch: (
      project: Project,
      credentials?: GitCredentials,
    ) => Effect.Effect<void, GitError>;
    /** Default branch of the cached clone (symbolic HEAD), e.g. "main". */
    readonly defaultBranch: (project: Project) => Effect.Effect<string, GitError>;
    readonly cachePathFor: (project: Project) => string;
  }
>()("maestro/git/GitCache") {
  static readonly layer = Layer.effect(
    GitCache,
    Effect.gen(function* () {
      const { storageRoot } = yield* AppConfig;
      const cachePathFor = (project: Project) => repoCacheDir(storageRoot, project.id);

      return {
        cachePathFor,
        ensureClone: Effect.fn("GitCache.ensureClone")(function* (
          project: Project,
          credentials?: GitCredentials,
        ) {
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
        fetch: Effect.fn("GitCache.fetch")(function* (
          project: Project,
          credentials?: GitCredentials,
        ) {
          const cwd = cachePathFor(project);
          yield* runGit(["fetch", "origin"], credentials ? { cwd, credentials } : { cwd });
        }),
        defaultBranch: Effect.fn("GitCache.defaultBranch")(function* (project: Project) {
          const ref = yield* runGit(["symbolic-ref", "HEAD"], { cwd: cachePathFor(project) });
          return ref.replace(/^refs\/heads\//, "");
        }),
      };
    }),
  );
}
