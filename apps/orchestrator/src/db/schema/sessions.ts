import type { SessionState, TaskSource } from "@maestro/domain";
import { index, integer, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";
import { projects } from "./projects.ts";

export const sessions = pgTable(
  "sessions",
  {
    id: uuidV7PrimaryKey(),
    projectId: uuid("project_id")
      .notNull()
      .references(() => projects.id),
    ticketSource: text("ticket_source").$type<TaskSource>().notNull(),
    ticketExternalId: text("ticket_external_id").notNull(),
    gitBranch: text("git_branch").notNull(),
    claudeSessionUuid: uuid("claude_session_uuid"),
    prNumber: integer("pr_number"),
    prUrl: text("pr_url"),
    state: text("state").$type<SessionState>().notNull(),
    terminationRequestedAt: timestamp("termination_requested_at", { withTimezone: true }),
    pausedAt: timestamp("paused_at", { withTimezone: true }),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    lastActivityAt: timestamp("last_activity_at", { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [index("sessions_ticket_idx").on(table.ticketSource, table.ticketExternalId)],
);
