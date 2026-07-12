import type { TaskSource } from "@maestro/domain";
import { jsonb, pgTable, text, timestamp, unique, uuid } from "drizzle-orm/pg-core";

// Idempotent ingestion (Tech Requirements §5): deliveries are deduplicated by
// (source, delivery id); the payload column is immutable and opaque so
// outbound responders can trace platform-native state back.
export const webhookDeliveries = pgTable(
  "webhook_deliveries",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    source: text("source").$type<TaskSource>().notNull(),
    deliveryId: text("delivery_id").notNull(),
    payload: jsonb("payload").$type<unknown>().notNull(),
    receivedAt: timestamp("received_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [unique("webhook_deliveries_source_delivery_idx").on(table.source, table.deliveryId)],
);
