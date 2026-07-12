import { TaskRunStateChanged } from "@maestro/api";
import {
  canTaskRunTransition,
  type DbError,
  EntityNotFoundError,
  type SessionId,
  StateTransitionError,
  TaskContext,
  TaskRun,
  type TaskRunCause,
  type TaskRunId,
  type TaskRunState,
  taskRunTransitions,
} from "@maestro/domain";
import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import { EventBus } from "../events/EventBus.ts";
import { Db } from "./Db.ts";
import { taskRuns } from "./schema/index.ts";
import { dbTry } from "./support.ts";

const decode = Schema.decodeUnknownSync(TaskRun);
const decodeContext = Schema.decodeUnknownSync(TaskContext);
const toTaskRun = (row: typeof taskRuns.$inferSelect): TaskRun =>
  decode({
    id: row.id,
    sessionId: row.sessionId,
    state: row.state,
    createdAt: row.createdAt,
    expiresAt: row.expiresAt,
    evictableAfter: row.evictableAfter,
    cause: row.cause,
    resultText: row.resultText,
  });

const allStates = Object.keys(taskRunTransitions) as ReadonlyArray<TaskRunState>;

export interface TaskRunTransitionOptions {
  readonly cause?: TaskRunCause;
  readonly expiresAt?: Date;
  readonly evictableAfter?: Date;
  readonly resultText?: string;
}

export class TaskRunRepo extends Context.Service<
  TaskRunRepo,
  {
    readonly create: (
      sessionId: SessionId,
      context: TaskContext,
    ) => Effect.Effect<TaskRun, DbError>;
    readonly get: (id: TaskRunId) => Effect.Effect<TaskRun, DbError>;
    /** The turn's TaskContext payload (kept off the entity — jsonb, read on demand). */
    readonly getContext: (id: TaskRunId) => Effect.Effect<TaskContext, DbError>;
    readonly listBySession: (
      sessionId: SessionId,
    ) => Effect.Effect<ReadonlyArray<TaskRun>, DbError>;
    /** Runs not yet settled (PENDING/PROVISIONING/EXECUTING), oldest first — SSE snapshot. */
    readonly listActive: () => Effect.Effect<ReadonlyArray<TaskRun>, DbError>;
    /**
     * Circuit-breaker input (FUR-39): how many of the session's most recent
     * settled turns FAILED in a row, with no COMPLETED in between. Derived
     * from rows (id order — monotonic UUIDv7), never a counter, so it is
     * crash-safe and replay-safe. CANCELLED failures are skipped, not counted
     * and not a reset: cancellation is an orchestrator/human action, it says
     * nothing about agent health. Saturates at a small window — callers only
     * compare against a threshold far below it.
     */
    readonly countConsecutiveFailures: (sessionId: SessionId) => Effect.Effect<number, DbError>;
    /**
     * Compare-and-swap state transition (see SessionRepo.transition).
     * Optional fields (cause, deadlines) are set atomically with the state.
     */
    readonly transition: (
      id: TaskRunId,
      to: TaskRunState,
      options?: TaskRunTransitionOptions,
    ) => Effect.Effect<TaskRun, DbError>;
    /** Append a chunk to the run's log blob (append-only, ordered). */
    readonly appendLogs: (id: TaskRunId, chunk: string) => Effect.Effect<void, DbError>;
    readonly getLogs: (id: TaskRunId) => Effect.Effect<string, DbError>;
    readonly setTraceId: (id: TaskRunId, traceId: string) => Effect.Effect<void, DbError>;
  }
>()("maestro/db/TaskRunRepo") {
  static readonly layer = Layer.effect(
    TaskRunRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      const bus = yield* EventBus;

      // Repos publish on every successful state write (see SessionRepo for
      // the FUR-16 decision): create (initial PENDING) and transition.
      const publishChanged = (taskRun: TaskRun) =>
        bus.publish(TaskRunStateChanged.make({ taskRun }));

      const getById = (operation: string) => (id: TaskRunId) =>
        dbTry(operation)(() => client.select().from(taskRuns).where(eq(taskRuns.id, id))).pipe(
          Effect.flatMap((rows) =>
            rows[0]
              ? Effect.succeed(rows[0])
              : Effect.fail(new EntityNotFoundError({ entity: "TaskRun", entityId: id })),
          ),
        );

      return {
        create: Effect.fn("TaskRunRepo.create")(function* (
          sessionId: SessionId,
          context: TaskContext,
        ) {
          const rows = yield* dbTry("TaskRunRepo.create")(() =>
            client.insert(taskRuns).values({ sessionId, state: "PENDING", context }).returning(),
          );
          // biome-ignore lint/style/noNonNullAssertion: insert returning always yields one row
          const taskRun = toTaskRun(rows[0]!);
          yield* publishChanged(taskRun);
          return taskRun;
        }),
        get: Effect.fn("TaskRunRepo.get")(function* (id: TaskRunId) {
          return toTaskRun(yield* getById("TaskRunRepo.get")(id));
        }),
        getContext: Effect.fn("TaskRunRepo.getContext")(function* (id: TaskRunId) {
          const row = yield* getById("TaskRunRepo.getContext")(id);
          return decodeContext(row.context);
        }),
        listBySession: Effect.fn("TaskRunRepo.listBySession")(function* (sessionId: SessionId) {
          const rows = yield* dbTry("TaskRunRepo.listBySession")(() =>
            client
              .select()
              .from(taskRuns)
              .where(eq(taskRuns.sessionId, sessionId))
              .orderBy(asc(taskRuns.createdAt)),
          );
          return rows.map(toTaskRun);
        }),
        listActive: Effect.fn("TaskRunRepo.listActive")(function* () {
          const rows = yield* dbTry("TaskRunRepo.listActive")(() =>
            client
              .select()
              .from(taskRuns)
              .where(inArray(taskRuns.state, ["PENDING", "PROVISIONING", "EXECUTING"]))
              .orderBy(asc(taskRuns.createdAt)),
          );
          return rows.map(toTaskRun);
        }),
        countConsecutiveFailures: Effect.fn("TaskRunRepo.countConsecutiveFailures")(function* (
          sessionId: SessionId,
        ) {
          const rows = yield* dbTry("TaskRunRepo.countConsecutiveFailures")(() =>
            client
              .select({ state: taskRuns.state, cause: taskRuns.cause })
              .from(taskRuns)
              .where(
                and(
                  eq(taskRuns.sessionId, sessionId),
                  inArray(taskRuns.state, ["COMPLETED", "FAILED"]),
                ),
              )
              .orderBy(desc(taskRuns.id))
              .limit(20),
          );
          let failures = 0;
          for (const row of rows) {
            if (row.state === "COMPLETED") break;
            if (row.cause === "CANCELLED") continue;
            failures += 1;
          }
          return failures;
        }),
        transition: Effect.fn("TaskRunRepo.transition")(function* (
          id: TaskRunId,
          to: TaskRunState,
          options?: TaskRunTransitionOptions,
        ) {
          const legalFrom = allStates.filter((from) => canTaskRunTransition(from, to));
          const rows = yield* dbTry("TaskRunRepo.transition")(() =>
            client
              .update(taskRuns)
              .set({
                state: to,
                ...(options?.cause !== undefined && { cause: options.cause }),
                ...(options?.expiresAt !== undefined && { expiresAt: options.expiresAt }),
                ...(options?.evictableAfter !== undefined && {
                  evictableAfter: options.evictableAfter,
                }),
                ...(options?.resultText !== undefined && { resultText: options.resultText }),
              })
              .where(and(eq(taskRuns.id, id), inArray(taskRuns.state, [...legalFrom])))
              .returning(),
          );
          if (rows[0]) {
            const taskRun = toTaskRun(rows[0]);
            yield* publishChanged(taskRun);
            return taskRun;
          }
          const current = yield* getById("TaskRunRepo.transition")(id);
          return yield* new StateTransitionError({
            entity: "TaskRun",
            entityId: id,
            from: current.state,
            to,
          });
        }),
        appendLogs: Effect.fn("TaskRunRepo.appendLogs")(function* (id: TaskRunId, chunk: string) {
          const rows = yield* dbTry("TaskRunRepo.appendLogs")(() =>
            client
              .update(taskRuns)
              .set({ logOutput: sql`coalesce(${taskRuns.logOutput}, '') || ${chunk}` })
              .where(eq(taskRuns.id, id))
              .returning({ id: taskRuns.id }),
          );
          if (!rows[0]) {
            return yield* new EntityNotFoundError({ entity: "TaskRun", entityId: id });
          }
        }),
        getLogs: Effect.fn("TaskRunRepo.getLogs")(function* (id: TaskRunId) {
          const row = yield* getById("TaskRunRepo.getLogs")(id);
          return row.logOutput ?? "";
        }),
        setTraceId: Effect.fn("TaskRunRepo.setTraceId")(function* (id: TaskRunId, traceId: string) {
          const rows = yield* dbTry("TaskRunRepo.setTraceId")(() =>
            client
              .update(taskRuns)
              .set({ traceId })
              .where(eq(taskRuns.id, id))
              .returning({ id: taskRuns.id }),
          );
          if (!rows[0]) {
            return yield* new EntityNotFoundError({ entity: "TaskRun", entityId: id });
          }
        }),
      };
    }),
  );
}
