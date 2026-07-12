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
 * job (and unblock its session) under a still-running handler. Workers
 * heartbeat active jobs instead, so only an orchestrator crash lets a job go
 * stale, and the absolute expiration is a generous backstop for the turn
 * timeout owned by the executor (M1.10).
 */
const HEARTBEAT_SECONDS = 60;
const EXPIRE_SECONDS = 4 * 60 * 60;

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
 * the FUR-13 spike):
 *
 * - pg-boss's `key_strict_fifo` queue policy was investigated first and
 *   disproven: its fetch relies on "oldest job wins, unique index rejects the
 *   rest", so a queued turn of a busy session at the head of the queue starves
 *   every other session for the whole duration of the active turn.
 * - The winning encoding is job groups: `group_id = sessionId` with
 *   `localGroupConcurrency: 1` (at most one active turn per session, enforced
 *   by synchronous in-process tracking — authoritative because the
 *   orchestrator is single-process) and `localConcurrency = global cap`.
 *   Saturated groups are excluded from the fetch query, so no head-of-line
 *   blocking; dispatch order is `created_on` (arrival order across sessions).
 *
 * If Maestro ever runs multiple orchestrator processes, this must move to
 * pg-boss's DB-enforced `groupConcurrency` (weaker: racy under concurrent
 * fetchers) or the documented `FOR UPDATE SKIP LOCKED` fallback behind this
 * same port.
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
     * Register the turn worker: up to the global cap of concurrent handlers,
     * at most one per session, per-session arrival order. A handler failure
     * fails the job permanently (retryLimit 0 — explicit-resume-only per
     * design). The worker is unregistered when the scope closes.
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
            const instance = new PgBoss({
              connectionString: databaseUrl,
              // Wake workers via NOTIFY the moment a turn is enqueued instead
              // of waiting out a polling interval; polling stays on as floor.
              useListenNotify: true,
            });
            instance.on("error", (error) => {
              Effect.runFork(Effect.logError("pg-boss error", error));
            });
            await instance.start();
            await instance.createQueue(QUEUE, {
              policy: "standard",
              retryLimit: 0,
              notify: true,
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
          yield* Effect.acquireRelease(
            Effect.tryPromise({
              try: () =>
                boss.work(
                  QUEUE,
                  {
                    batchSize: 1,
                    localConcurrency: maxConcurrentWorkers,
                    localGroupConcurrency: 1,
                    // NOTIFY only fires on job creation. A turn that became
                    // runnable because its session's previous turn finished
                    // (or because a fetched-over-capacity job was restored) is
                    // only picked up by polling, so keep the poll at the 500ms
                    // floor in both plain and notify mode.
                    pollingIntervalSeconds: 0.5,
                    notifyPollingIntervalSeconds: 0.5,
                  },
                  async ([job]) => {
                    if (!job) return;
                    if (!job.groupId) {
                      // Cannot happen for jobs enqueued through this port; a
                      // groupless job would bypass per-session serialization.
                      throw new Error(`turn job ${job.id} has no session group`);
                    }
                    await Effect.runPromise(
                      handler({
                        taskRunId: job.id as TaskRunId,
                        sessionId: job.groupId as SessionId,
                      }),
                    );
                  },
                ),
              catch: operationError("TurnQueue.work"),
            }),
            (workerId) =>
              Effect.tryPromise(() => boss.offWork(QUEUE, { id: workerId })).pipe(Effect.ignore),
          );
        }),
      };
    }),
  );
}
