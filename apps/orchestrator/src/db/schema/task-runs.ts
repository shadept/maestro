import type { TaskContext, TaskRunCause, TaskRunState } from "@maestro/domain";
import { index, jsonb, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";
import { sessions } from "./sessions.ts";

export const taskRuns = pgTable(
  "task_runs",
  {
    id: uuidV7PrimaryKey(),
    sessionId: uuid("session_id")
      .notNull()
      .references(() => sessions.id),
    state: text("state").$type<TaskRunState>().notNull(),
    /**
     * The normalized TaskContext that drives this turn. The queue job carries
     * no payload (FUR-13) — this column is the payload the executor reads.
     */
    context: jsonb("context").$type<TaskContext>().notNull(),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    evictableAfter: timestamp("evictable_after", { withTimezone: true }),
    cause: text("cause").$type<TaskRunCause>(),
    /** Final agent text from the turn's Result event. */
    resultText: text("result_text"),
    /** Failure reason, written atomically with the FAILED transition. */
    failureSummary: text("failure_summary"),
    /** Captured worker stdout/stderr for the turn (append-heavy, read on demand). */
    logOutput: text("log_output"),
    traceId: text("trace_id"),
  },
  (table) => [index("task_runs_session_idx").on(table.sessionId)],
);
