import { rm } from "node:fs/promises";
import type { DbError, GitError, Session, SessionId, TaskRunId } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { ProjectRepo } from "../db/ProjectRepo.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { WorktreeManager } from "../git/WorktreeManager.ts";
import { sessionConfigDir } from "../storage/paths.ts";

/** What one terminal signal did. Every variant is a success — terminate is idempotent. */
export type TerminationOutcome =
  | { readonly _tag: "Terminated" }
  | { readonly _tag: "AlreadyTerminated" }
  | {
      /** A turn is still active: it finishes, then the executor finalizes the teardown. */
      readonly _tag: "Deferred";
      readonly awaiting: ReadonlyArray<TaskRunId>;
    };

export type TerminationError = DbError | GitError;

/**
 * Terminal cleanup (M1.15, PRD §4.1): ticket closure is the single
 * authoritative terminal signal. On it, the session's queued turns are
 * cancelled, the session goes TERMINATED (the repo transition publishes
 * SessionStateChanged), the worktree is removed, and the session's
 * CLAUDE_CONFIG_DIR is purged.
 *
 * MVP "let it finish" semantics: an active (PROVISIONING/EXECUTING) turn is
 * never killed mid-turn. The terminal signal is persisted on the session
 * (terminationRequestedAt) and teardown is DEFERRED — TurnExecutor re-invokes
 * terminate after the turn settles. The persisted marker survives a restart,
 * but nothing scans for it in M1: if the orchestrator dies while a marked
 * session's turn is in flight, the teardown is re-driven only by a replayed
 * queue job or the M2 retention-window fallback.
 *
 * Cancelled queued turns keep their pg-boss jobs: the dispatcher eventually
 * fetches them and TurnExecutor.execute's non-PENDING guard drains them as
 * no-ops (asserted in the M1.15 suite) — the single-dispatcher invariant
 * stays untouched, no queue-side cancel API needed.
 */
export class SessionTerminator extends Context.Service<
  SessionTerminator,
  {
    /**
     * Idempotent terminal-signal handler. A second signal for the same
     * session is a no-op (AlreadyTerminated re-runs only the idempotent
     * filesystem cleanup, converging any crash- or race-leftover state).
     */
    readonly terminate: (args: {
      readonly sessionId: SessionId;
    }) => Effect.Effect<TerminationOutcome, TerminationError>;
  }
>()("maestro/engine/SessionTerminator") {
  static readonly layer = Layer.effect(
    SessionTerminator,
    Effect.gen(function* () {
      const { storageRoot } = yield* AppConfig;
      const projectRepo = yield* ProjectRepo;
      const sessionRepo = yield* SessionRepo;
      const taskRunRepo = yield* TaskRunRepo;
      const worktreeManager = yield* WorktreeManager;

      // PENDING → FAILED/CANCELLED via the CAS guard: a run that already got
      // dispatched (PROVISIONING) won't match — it is treated as active and
      // deferred to, never cancelled mid-flight (MVP: no mid-turn kill).
      const cancelQueued = (sessionId: SessionId) =>
        Effect.gen(function* () {
          const runs = yield* taskRunRepo.listBySession(sessionId);
          yield* Effect.forEach(
            runs.filter((run) => run.state === "PENDING"),
            (run) =>
              taskRunRepo.transition(run.id, "FAILED", { cause: "CANCELLED" }).pipe(
                // lost the race to dispatch — the active-run check picks it up
                Effect.catchTag("StateTransitionError", () => Effect.void),
              ),
            { discard: true },
          );
        });

      const purgeFilesystem = (session: Session) =>
        Effect.gen(function* () {
          const project = yield* projectRepo.get(session.projectId);
          yield* worktreeManager.remove({ session, project });
          yield* Effect.promise(() =>
            rm(sessionConfigDir(storageRoot, session.id), { recursive: true, force: true }),
          );
        });

      return {
        terminate: Effect.fn("SessionTerminator.terminate")(function* (args: {
          sessionId: SessionId;
        }) {
          const session = yield* sessionRepo.get(args.sessionId);
          if (session.state === "TERMINATED") {
            // no-op, but re-run the idempotent cleanup so a crash between the
            // TERMINATED flip and the filesystem purge converges on retry
            yield* purgeFilesystem(session);
            return { _tag: "AlreadyTerminated" } as const;
          }

          // Persist the signal FIRST: whatever fails below, the intent
          // survives and every later terminate/finalize converges on it.
          const marked = yield* sessionRepo.requestTermination(args.sessionId);
          yield* cancelQueued(args.sessionId);

          const runs = yield* taskRunRepo.listBySession(args.sessionId);
          const active = runs.filter(
            (run) => run.state === "PROVISIONING" || run.state === "EXECUTING",
          );
          if (active.length > 0) {
            // let it finish; TurnExecutor finalizes after the turn settles
            return { _tag: "Deferred", awaiting: active.map((run) => run.id) } as const;
          }

          // TERMINATED first (SessionStateChanged published by the repo):
          // once flipped, ingest can no longer queue turns on this session,
          // so the sweep below sees the final set of PENDING runs.
          yield* sessionRepo.transition(args.sessionId, "TERMINATED").pipe(
            // concurrent terminate won the CAS — converge on cleanup below
            Effect.catchTag("StateTransitionError", () => Effect.void),
          );
          yield* cancelQueued(args.sessionId);
          yield* purgeFilesystem(marked);
          return { _tag: "Terminated" } as const;
        }),
      };
    }),
  );
}
