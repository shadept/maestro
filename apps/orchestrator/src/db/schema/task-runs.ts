import type { TaskRunCause, TaskRunState } from "@maestro/domain";
import { index, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { sessions } from "./sessions.ts";

export const taskRuns = pgTable(
  "task_runs",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    sessionId: uuid("session_id")
      .notNull()
      .references(() => sessions.id),
    state: text("state").$type<TaskRunState>().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    evictableAfter: timestamp("evictable_after", { withTimezone: true }),
    cause: text("cause").$type<TaskRunCause>(),
    /** Captured worker stdout/stderr for the turn (append-heavy, read on demand). */
    logOutput: text("log_output"),
    traceId: text("trace_id"),
  },
  (table) => [index("task_runs_session_idx").on(table.sessionId)],
);
