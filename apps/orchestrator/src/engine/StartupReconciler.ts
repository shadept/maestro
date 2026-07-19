import type { DbError, GitError, TaskRun } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { WorkerRuntime } from "../runtime/WorkerRuntime.ts";
import { SessionTerminator } from "./SessionTerminator.ts";
import { prOf, TurnSettlement } from "./TurnSettlement.ts";
import { turnWorkerName } from "./worker-name.ts";

/**
 * Startup reconciliation (FUR-40): an orchestrator crash/restart mid-turn
 * kills the worker (local-cli workers die with the process) but leaves the
 * TaskRun row PROVISIONING/EXECUTING forever — the pg-boss job expires
 * terminally (retryLimit 0) so nothing ever settles it. On boot, BEFORE the
 * turn worker registers with the queue (so no new dispatch races the sweep):
 *
 * 1. Orphan sweep — every PROVISIONING/EXECUTING run whose worker container
 *    is not RUNNING (probed by deterministic name via WorkerRuntime.status)
 *    settles FAILED. PENDING runs are NOT candidates: their queue jobs
 *    survived the restart and dispatch normally.
 * 2. Termination-marker sweep — sessions whose terminal signal survived the
 *    crash (terminationRequestedAt set, not TERMINATED) get their deferred
 *    teardown re-driven (closes the FUR-19 leftover). Runs AFTER the orphan
 *    sweep: a teardown deferred behind a now-orphaned turn only proceeds
 *    once that turn is settled.
 *
 * CAUSE DECISION: orphans settle with cause CANCELLED, not a new INTERRUPTED
 * literal. An interrupted turn was ended by orchestrator lifecycle, exactly
 * like a kill — it says nothing about agent health, and CANCELLED is what
 * the FUR-39 streak derivation already skips, so a restart storm can never
 * trip the failure circuit breaker. A new cause would buy one word of
 * precision for domain + streak + UI surgery.
 *
 * A run whose container IS still running (the client died but the container
 * survived) is left untouched: re-attaching is out of scope (M2 / FUR-23),
 * and settling a live turn would race its own worker.
 *
 * Idempotent by construction: settled runs drop out of the candidate query,
 * terminate() is idempotent — the boot retry loop can re-run this freely.
 */
export class StartupReconciler extends Context.Service<
  StartupReconciler,
  {
    readonly reconcile: () => Effect.Effect<void, DbError | GitError>;
  }
>()("maestro/engine/StartupReconciler") {
  static readonly layer = Layer.effect(
    StartupReconciler,
    Effect.gen(function* () {
      const taskRunRepo = yield* TaskRunRepo;
      const sessionRepo = yield* SessionRepo;
      const runtime = yield* WorkerRuntime;
      const settlement = yield* TurnSettlement;
      const terminator = yield* SessionTerminator;

      const settleOrphan = Effect.fn(
        function* (run: TaskRun) {
          const session = yield* sessionRepo.get(run.sessionId);
          yield* settlement.settleFailed({
            taskRunId: run.id,
            sessionId: run.sessionId,
            ticket: session.ticketReference,
            cause: "CANCELLED",
            // Per-run text (run id included) so every orphan produces its own
            // ticket comment: the turn-failed idempotency key hashes
            // session + summary, and a session orphaned by two different
            // restarts must not have its second callback collapse into the
            // first (decided per FUR-40; see outcomeIdempotencyKey).
            summary:
              `Maestro restarted while this turn was executing; the worker did not survive ` +
              `(run ${run.id}). The turn was not retried — comment again to run it fresh.`,
            pr: prOf(session),
          });
          yield* Effect.logWarning("StartupReconciler: settled orphaned turn", {
            taskRunId: run.id,
            sessionId: run.sessionId,
            orphanedIn: run.state,
          });
        },
        // lost a race with a concurrent settle — the run is already consistent
        Effect.catchTag("StateTransitionError", () => Effect.void),
      );

      const sweepOrphans = Effect.gen(function* () {
        const active = yield* taskRunRepo.listActive();
        const candidates = active.filter(
          (run) => run.state === "PROVISIONING" || run.state === "EXECUTING",
        );
        for (const run of candidates) {
          const alive = yield* runtime.status({ id: turnWorkerName(run.id) }).pipe(
            Effect.map((status) => status === "RUNNING"),
            Effect.catchTag("WorkerNotFoundError", () => Effect.succeed(false)),
          );
          if (alive) {
            yield* Effect.logInfo("StartupReconciler: worker still running, leaving run alone", {
              taskRunId: run.id,
              sessionId: run.sessionId,
            });
            continue;
          }
          yield* settleOrphan(run);
        }
      }).pipe(
        // A runtime that cannot report status (K8s no-op until FUR-23, or a
        // broken template) forfeits the orphan sweep rather than guessing:
        // never settle a run whose worker might be alive.
        Effect.catchTag("NotImplementedError", () =>
          Effect.logWarning(
            "StartupReconciler: runtime cannot report worker status; skipping orphan sweep (K8s reconciliation lands with FUR-23)",
          ),
        ),
        Effect.catchTag("WorkerSpawnError", (error) =>
          Effect.logWarning(
            "StartupReconciler: worker status probe failed; skipping orphan sweep",
            {
              error: String(error),
            },
          ),
        ),
      );

      const sweepTerminationMarkers = Effect.gen(function* () {
        const marked = yield* sessionRepo.listTerminationRequested();
        for (const session of marked) {
          const outcome = yield* terminator.terminate({ sessionId: session.id });
          yield* Effect.logInfo("StartupReconciler: re-drove interrupted session teardown", {
            sessionId: session.id,
            outcome: outcome._tag,
          });
        }
      });

      return {
        reconcile: Effect.fn("StartupReconciler.reconcile")(function* () {
          yield* sweepOrphans;
          yield* sweepTerminationMarkers;
        }),
      };
    }),
  );
}
