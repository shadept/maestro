import { integer, jsonb, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";
import { taskRuns } from "./task-runs.ts";

// Callback outbox (Tech Requirements §16): turn results are committed here in
// the same transaction as the state change, then posted to the ticketing
// platform with retries — a crash between "posted" and "marked COMPLETED"
// never duplicates comments or loses results.
export const outbox = pgTable("outbox", {
  id: uuidV7PrimaryKey(),
  taskRunId: uuid("task_run_id").references(() => taskRuns.id),
  /** Delivery target, e.g. "linear" or a generic-API callback URL reference. */
  target: text("target").notNull(),
  payload: jsonb("payload").$type<unknown>().notNull(),
  status: text("status").$type<"PENDING" | "SENT" | "FAILED">().notNull().default("PENDING"),
  attempts: integer("attempts").notNull().default(0),
  lastError: text("last_error"),
  idempotencyKey: text("idempotency_key").notNull().unique(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  sentAt: timestamp("sent_at", { withTimezone: true }),
});
