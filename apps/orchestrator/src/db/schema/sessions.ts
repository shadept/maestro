import type { SessionState, TaskSource } from "@maestro/domain";
import { index, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { projects } from "./projects.ts";

export const sessions = pgTable(
  "sessions",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
    ticketSource: text("ticket_source").$type<TaskSource>().notNull(),
    ticketExternalId: text("ticket_external_id").notNull(),
    gitBranch: text("git_branch").notNull(),
    claudeSessionUuid: uuid("claude_session_uuid"),
    state: text("state").$type<SessionState>().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    lastActivityAt: timestamp("last_activity_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [index("sessions_ticket_idx").on(table.ticketSource, table.ticketExternalId)],
);
