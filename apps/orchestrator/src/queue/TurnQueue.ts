import {
  type QueueError,
  QueueOperationError,
  type SessionId,
  type TaskRunId,
} from "@maestro/domain";
import { Context, Effect, Layer, type Scope } from "effect";
import { type JobInsert, PgBoss } from "pg-boss";
import { AppConfig } from "../config/AppConfig.ts";

/**
 * A queued turn. The pg-boss job carries the TaskRun id as its job id (no
 * payload — the payload lives in Postgres) and the session id as its group id
 * (scheduling metadata, used for per-session serialization).
 */
export interface TurnJob {
  readonly taskRunId: TaskRunId;
  readonly sessionId: SessionId;
}

const QUEUE = "turns";

/**
 * Turns run for minutes; the pg-boss default 15-minute expiration would fail a
 * job (and unblock its session) under a still-running handler. The dispatcher
 * heartbeats active jobs instead, so only an orchestrator crash lets a job go
 * stale, and the absolute expiration is a generous backstop for the turn
 * timeout owned by the executor (M1.10).
 */
const HEARTBEAT_SECONDS = 60;
const EXPIRE_SECONDS = 4 * 60 * 60;

/** Dispatch poll cadence — the latency floor for picking up a runnable turn. */
const POLL_MILLIS = 500;

// pg-boss 12.26 `insert()` does not translate the typed `group: { id }` option
// into the wire format its insert SQL reads — that SQL extracts a flat
// "groupId" key (the same shape `send()` serializes to internally). Verified
// empirically against pgboss.job.group_id; without this the job would carry no
// group and per-session serialization would silently not apply.
interface WireJobInsert extends JobInsert {
  readonly groupId: string;
}

const operationError = (operation: string) => (error: unknown) =>
  new QueueOperationError({
    operation,
    message: error instanceof Error ? error.message : String(error),
  });

/**
 * Per-session FIFO turn queue (Tech Requirements §3 watch-item, resolved by
 * the FUR-13 spike, dispatch rebuilt after the FUR-13 encoding was disproven):
 *
 * - pg-boss's `key_strict_fifo` queue policy was investigated first and
 *   disproven: its fetch relies on "oldest job wins, unique index rejects the
 *   rest", so a queued turn of a busy session at the head of the queue starves
 *   every other session for the whole duration of the active turn.
 * - `boss.work()` with `localConcurrency = cap` + `localGroupConcurrency: 1`
 *   (the original FUR-13 encoding) was ALSO disproven: it spawns `cap`
 *   independent pollers, and SKIP LOCKED lets poller B claim a session's
 *   second turn while poller A still holds its first. Group admission then
 *   happens client-side in fetch-response arrival order, so whichever
 *   response lands first runs and the earlier turn is restore()d — an
 *   out-of-order start (reproduced ~1 in 3 under load).
 * - The winning shape is a single in-process dispatcher over the public
 *   `fetch`/`complete`/`fail`/`touch` API: one fetcher means no cross-fetcher
 *   race by construction, and `groupConcurrency: 1` inside the fetch statement
 *   atomically excludes sessions that already have an active turn and admits
 *   at most one job per session per batch. Cross-session dispatch order is
 *   `created_on` (fair, arrival order); saturated sessions are filtered out
 *   before the batch limit, so no head-of-line blocking.
 *
 * Within a session, pg-boss's group ranking picks the candidate by job id.
 * Job ids are TaskRunIds, minted app-side as monotonic UUIDv7, so id order ==
 * enqueue order per session — this invariant is what makes the in-statement
 * ranking equivalent to FIFO.
 *
 * If Maestro ever runs multiple orchestrator processes, a single dispatcher
 * per queue is no longer given and this must move to the documented
 * `FOR UPDATE SKIP LOCKED` fallback with DB-side admission behind this same
 * port.
 */
export class TurnQueue extends Context.Service<
  TurnQueue,
  {
    /**
     * Enqueue a turn. Idempotent: the TaskRun id is the pg-boss job id, so a
     * replayed enqueue is dropped by the primary-key conflict.
     */
    readonly enqueue: (job: TurnJob) => Effect.Effect<void, QueueError>;
    /**
     * Start the turn dispatcher: up to the global cap of concurrent handlers,
     * at most one per session, per-session arrival order. A handler failure
     * fails the job permanently (retryLimit 0 — explicit-resume-only per
     * design). The dispatcher and all running handlers are interrupted when
     * the scope closes; their jobs stay active and are recovered by heartbeat
     * expiry, mirroring a crash.
     */
    readonly work: (
      handler: (job: TurnJob) => Effect.Effect<void, unknown>,
    ) => Effect.Effect<void, QueueError, Scope.Scope>;
  }
>()("maestro/queue/TurnQueue") {
  static readonly layer = Layer.effect(
    TurnQueue,
    Effect.gen(function* () {
      const { databaseUrl, maxConcurrentWorkers } = yield* AppConfig;

      const boss = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: async () => {
            const instance = new PgBoss({ connectionString: databaseUrl });
            instance.on("error", (error) => {
              Effect.runFork(Effect.logError("pg-boss error", error));
            });
            await instance.start();
            await instance.createQueue(QUEUE, {
              policy: "standard",
              retryLimit: 0,
              heartbeatSeconds: HEARTBEAT_SECONDS,
              expireInSeconds: EXPIRE_SECONDS,
            });
            return instance;
          },
          catch: operationError("TurnQueue.start"),
        }),
        (instance) =>
          // graceful:false — a graceful stop would wait out an in-flight turn
          // (minutes); the job stays active and is recovered by heartbeat
          // expiry after a crash, or finishes normally on the same process.
          Effect.tryPromise(() => instance.stop({ graceful: false, close: true })).pipe(
            Effect.ignore,
          ),
      );

      return {
        enqueue: Effect.fn("TurnQueue.enqueue")(function* (job: TurnJob) {
          const wire: WireJobInsert = { id: job.taskRunId, groupId: job.sessionId };
          yield* Effect.tryPromise({
            try: () => boss.insert(QUEUE, [wire]),
            catch: operationError("TurnQueue.enqueue"),
          });
        }),
        work: Effect.fn("TurnQueue.work")(function* (
          handler: (job: TurnJob) => Effect.Effect<void, unknown>,
        ) {
          // Sessions with a turn running in this process. Only the dispatcher
          // fiber and handler-completion continuations touch it, and the
          // admission read-modify-write below is fully synchronous, so plain
          // Map mutation is race-free on the single-threaded runtime.
          const running = new Map<SessionId, TaskRunId>();

          const settle = (operation: string, promise: () => Promise<unknown>) =>
            Effect.tryPromise({ try: promise, catch: operationError(operation) });

          const runJob = (job: TurnJob) =>
            handler(job).pipe(
              Effect.matchEffect({
                onSuccess: () =>
                  settle("TurnQueue.complete", () => boss.complete(QUEUE, job.taskRunId)),
                onFailure: (error) =>
                  settle("TurnQueue.fail", () =>
                    boss.fail(QUEUE, job.taskRunId, { message: String(error) }),
                  ),
              }),
              // A failed settlement leaves the job active; heartbeat expiry
              // fails it visibly rather than silently double-running it.
              Effect.catch((error) => Effect.logError("turn settlement failed", error)),
              Effect.ensuring(Effect.sync(() => running.delete(job.sessionId))),
            );

          const dispatch = Effect.gen(function* () {
            const free = maxConcurrentWorkers - running.size;
            if (free <= 0) return;
            const jobs = yield* Effect.tryPromise({
              try: () =>
                boss.fetch(QUEUE, {
                  batchSize: free,
                  includeMetadata: true,
                  // enforced inside the fetch statement: sessions with an
                  // active turn are excluded, at most one job per session
                  groupConcurrency: 1,
                }),
              catch: operationError("TurnQueue.fetch"),
            });
            // UPDATE ... RETURNING does not preserve the fetch query's
            // created_on ordering — restore arrival order before starting.
            const ordered = [...jobs].sort(
              (a, b) => a.createdOn.getTime() - b.createdOn.getTime() || (a.id < b.id ? -1 : 1),
            );
            for (const job of ordered) {
              if (!job.groupId) {
                // Cannot happen for jobs enqueued through this port; fail it
                // rather than run a turn outside per-session serialization.
                yield* settle("TurnQueue.fail", () =>
                  boss.fail(QUEUE, job.id, { message: `turn job ${job.id} has no session group` }),
                );
                continue;
              }
              const turnJob: TurnJob = {
                taskRunId: job.id as TaskRunId,
                sessionId: job.groupId as SessionId,
              };
              running.set(turnJob.sessionId, turnJob.taskRunId);
              yield* Effect.forkChild(runJob(turnJob));
            }
          });

          const heartbeat = Effect.suspend(() => {
            const active = [...running.values()];
            if (active.length === 0) return Effect.void;
            return settle("TurnQueue.touch", () => boss.touch(QUEUE, active));
          });

          // Transient DB errors must not kill the loops — log and keep polling.
          const loop = (step: Effect.Effect<unknown, QueueError>, intervalMillis: number) =>
            step.pipe(
              Effect.catch((error) => Effect.logError("turn dispatch failed", error)),
              Effect.andThen(Effect.sleep(intervalMillis)),
              Effect.forever,
            );

          yield* Effect.forkScoped(loop(dispatch, POLL_MILLIS));
          yield* Effect.forkScoped(loop(heartbeat, (HEARTBEAT_SECONDS * 1000) / 2));
        }),
      };
    }),
  );
}
