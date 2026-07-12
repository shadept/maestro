import { pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";

export const auditLogs = pgTable("audit_logs", {
  id: uuidV7PrimaryKey(),
  actor: text("actor").notNull(),
  action: text("action").notNull(),
  targetEntity: text("target_entity").notNull(),
  priorState: text("prior_state"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
