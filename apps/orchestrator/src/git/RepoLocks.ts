import type { ProjectId } from "@maestro/domain";
import { Context, Effect, Layer, Semaphore } from "effect";

/**
 * Per-project mutex for git operations that mutate a project's bare repository.
 *
 * `git clone`, `worktree add/remove/prune`, `branch -D`, and `fetch` all take
 * short-lived locks on files shared across the whole repo (`config.lock`,
 * `packed-refs.lock`, worktree metadata). git does not wait on these locks — a
 * collision fails immediately ("could not lock config file config: File
 * exists"), so two sessions provisioning worktrees on the same project race.
 * The orchestrator is single-process and owns all git operations, so an
 * in-memory mutex per project is authoritative.
 *
 * Not reentrant: a locked operation must never invoke another locked operation
 * for the same project (read-only operations like `defaultBranch` stay
 * unlocked for this reason).
 */
export class RepoLocks extends Context.Service<
  RepoLocks,
  {
    readonly withRepoLock: (
      projectId: ProjectId,
    ) => <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  }
>()("maestro/git/RepoLocks") {
  static readonly layer = Layer.sync(RepoLocks, () => {
    // One mutex per project, created on first use. Creation is synchronous so
    // get-or-create is atomic; projects are few and long-lived, so entries are
    // never evicted.
    const locks = new Map<ProjectId, Semaphore.Semaphore>();
    const lockFor = (projectId: ProjectId): Semaphore.Semaphore => {
      const existing = locks.get(projectId);
      if (existing !== undefined) return existing;
      const created = Semaphore.makeUnsafe(1);
      locks.set(projectId, created);
      return created;
    };
    return {
      // Not Effect.fn: this is a generic combinator, not an effectful method;
      // the span serves the one-span-per-call convention instead.
      withRepoLock: (projectId) => (effect) =>
        lockFor(projectId)
          .withPermit(effect)
          .pipe(Effect.withSpan("RepoLocks.withRepoLock", { attributes: { projectId } })),
    };
  });
}
