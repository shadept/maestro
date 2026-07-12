import type { DbError, TaskRunId } from "@maestro/domain";
import { EntityNotFoundError } from "@maestro/domain";
import { and, asc, eq, isNull, lte, or, sql } from "drizzle-orm";
import { Context, Effect, Layer } from "effect";
import { Db } from "./Db.ts";
import { outbox } from "./schema/index.ts";
import { dbTry } from "./support.ts";

export type OutboxEntry = typeof outbox.$inferSelect;

export interface OutboxEnqueue {
  readonly taskRunId?: TaskRunId;
  readonly target: string;
  readonly payload: unknown;
  readonly idempotencyKey: string;
}

export class OutboxRepo extends Context.Service<
  OutboxRepo,
  {
    /** Idempotent enqueue: an existing idempotency key is a no-op. */
    readonly enqueue: (input: OutboxEnqueue) => Effect.Effect<OutboxEntry, DbError>;
    /** PENDING entries due now (next_attempt_at unset or reached), oldest first. */
    readonly listPending: (limit: number) => Effect.Effect<ReadonlyArray<OutboxEntry>, DbError>;
    readonly markSent: (id: string) => Effect.Effect<void, DbError>;
    /**
     * Records a delivery failure; the entry stays PENDING for retry.
     * `nextAttemptAt` (persisted, so backoff survives restarts) gates when
     * `listPending` surfaces the row again.
     */
    readonly recordFailure: (
      id: string,
      error: string,
      nextAttemptAt?: Date,
    ) => Effect.Effect<void, DbError>;
  }
>()("maestro/db/OutboxRepo") {
  static readonly layer = Layer.effect(
    OutboxRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      return {
        enqueue: Effect.fn("OutboxRepo.enqueue")(function* (input: OutboxEnqueue) {
          const inserted = yield* dbTry("OutboxRepo.enqueue")(() =>
            client
              .insert(outbox)
              .values({
                taskRunId: input.taskRunId ?? null,
                target: input.target,
                payload: input.payload,
                idempotencyKey: input.idempotencyKey,
              })
              .onConflictDoNothing({ target: outbox.idempotencyKey })
              .returning(),
          );
          if (inserted[0]) return inserted[0];
          const existing = yield* dbTry("OutboxRepo.enqueue")(() =>
            client.select().from(outbox).where(eq(outbox.idempotencyKey, input.idempotencyKey)),
          );
          // biome-ignore lint/style/noNonNullAssertion: conflict implies the row exists
          return existing[0]!;
        }),
        listPending: Effect.fn("OutboxRepo.listPending")(function* (limit: number) {
          return yield* dbTry("OutboxRepo.listPending")(() =>
            client
              .select()
              .from(outbox)
              .where(
                and(
                  eq(outbox.status, "PENDING"),
                  or(isNull(outbox.nextAttemptAt), lte(outbox.nextAttemptAt, new Date())),
                ),
              )
              .orderBy(asc(outbox.createdAt))
              .limit(limit),
          );
        }),
        markSent: Effect.fn("OutboxRepo.markSent")(function* (id: string) {
          const rows = yield* dbTry("OutboxRepo.markSent")(() =>
            client
              .update(outbox)
              .set({ status: "SENT", sentAt: new Date() })
              .where(eq(outbox.id, id))
              .returning({ id: outbox.id }),
          );
          if (!rows[0]) {
            return yield* new EntityNotFoundError({ entity: "OutboxEntry", entityId: id });
          }
        }),
        recordFailure: Effect.fn("OutboxRepo.recordFailure")(function* (
          id: string,
          error: string,
          nextAttemptAt?: Date,
        ) {
          const rows = yield* dbTry("OutboxRepo.recordFailure")(() =>
            client
              .update(outbox)
              .set({
                attempts: sql`${outbox.attempts} + 1`,
                lastError: error,
                nextAttemptAt: nextAttemptAt ?? null,
              })
              .where(eq(outbox.id, id))
              .returning({ id: outbox.id }),
          );
          if (!rows[0]) {
            return yield* new EntityNotFoundError({ entity: "OutboxEntry", entityId: id });
          }
        }),
      };
    }),
  );
}
