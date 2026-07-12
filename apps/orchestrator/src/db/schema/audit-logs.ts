import { pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";

export const auditLogs = pgTable("audit_logs", {
  id: uuid("id").primaryKey().defaultRandom(),
  actor: text("actor").notNull(),
  action: text("action").notNull(),
  targetEntity: text("target_entity").notNull(),
  priorState: text("prior_state"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
