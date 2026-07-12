import {
  canTaskRunTransition,
  type DbError,
  EntityNotFoundError,
  type SessionId,
  StateTransitionError,
  TaskRun,
  type TaskRunCause,
  type TaskRunId,
  type TaskRunState,
  taskRunTransitions,
} from "@maestro/domain";
import { and, asc, eq, inArray, sql } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import { Db } from "./Db.ts";
import { taskRuns } from "./schema/index.ts";
import { dbTry } from "./support.ts";

const decode = Schema.decodeUnknownSync(TaskRun);
const toTaskRun = (row: typeof taskRuns.$inferSelect): TaskRun =>
  decode({
    id: row.id,
    sessionId: row.sessionId,
    state: row.state,
    createdAt: row.createdAt,
    expiresAt: row.expiresAt,
    evictableAfter: row.evictableAfter,
    cause: row.cause,
  });

const allStates = Object.keys(taskRunTransitions) as ReadonlyArray<TaskRunState>;

export interface TaskRunTransitionOptions {
  readonly cause?: TaskRunCause;
  readonly expiresAt?: Date;
  readonly evictableAfter?: Date;
}

export class TaskRunRepo extends Context.Service<
  TaskRunRepo,
  {
    readonly create: (sessionId: SessionId) => Effect.Effect<TaskRun, DbError>;
    readonly get: (id: TaskRunId) => Effect.Effect<TaskRun, DbError>;
    readonly listBySession: (
      sessionId: SessionId,
    ) => Effect.Effect<ReadonlyArray<TaskRun>, DbError>;
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

      const getById = (operation: string) => (id: TaskRunId) =>
        dbTry(operation)(() => client.select().from(taskRuns).where(eq(taskRuns.id, id))).pipe(
          Effect.flatMap((rows) =>
            rows[0]
              ? Effect.succeed(rows[0])
              : Effect.fail(new EntityNotFoundError({ entity: "TaskRun", entityId: id })),
          ),
        );

      return {
        create: Effect.fn("TaskRunRepo.create")(function* (sessionId: SessionId) {
          const rows = yield* dbTry("TaskRunRepo.create")(() =>
            client.insert(taskRuns).values({ sessionId, state: "PENDING" }).returning(),
          );
          // biome-ignore lint/style/noNonNullAssertion: insert returning always yields one row
          return toTaskRun(rows[0]!);
        }),
        get: Effect.fn("TaskRunRepo.get")(function* (id: TaskRunId) {
          return toTaskRun(yield* getById("TaskRunRepo.get")(id));
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
              })
              .where(and(eq(taskRuns.id, id), inArray(taskRuns.state, [...legalFrom])))
              .returning(),
          );
          if (rows[0]) return toTaskRun(rows[0]);
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
