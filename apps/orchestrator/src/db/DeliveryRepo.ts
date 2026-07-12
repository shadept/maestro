import type { DbError, TaskSource } from "@maestro/domain";
import { Context, Effect, Layer } from "effect";
import { Db } from "./Db.ts";
import { webhookDeliveries } from "./schema/index.ts";
import { dbTry } from "./support.ts";

export interface DeliveryRecord {
  readonly source: TaskSource;
  readonly deliveryId: string;
  readonly payload: unknown;
}

export class DeliveryRepo extends Context.Service<
  DeliveryRepo,
  {
    /**
     * Records a webhook delivery. Returns true when the delivery is new,
     * false when (source, deliveryId) was already seen — the dedup primitive
     * that makes at-least-once delivery safe.
     */
    readonly recordIfNew: (input: DeliveryRecord) => Effect.Effect<boolean, DbError>;
  }
>()("maestro/db/DeliveryRepo") {
  static readonly layer = Layer.effect(
    DeliveryRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      return {
        recordIfNew: Effect.fn("DeliveryRepo.recordIfNew")(function* (input: DeliveryRecord) {
          const rows = yield* dbTry("DeliveryRepo.recordIfNew")(() =>
            client
              .insert(webhookDeliveries)
              .values({
                source: input.source,
                deliveryId: input.deliveryId,
                payload: input.payload,
              })
              .onConflictDoNothing()
              .returning({ id: webhookDeliveries.id }),
          );
          return rows.length > 0;
        }),
      };
    }),
  );
}
