import { Schema } from "effect";
import { SessionId, TaskRunId } from "./ids.ts";

// TaskRun state machine (PRD §5.2). One TaskRun = one turn = a single pass.
// FAILED and COMPLETED are terminal: retry is an explicit new resume turn
// (never a transition back), so claude-code sees its own partial work.

export const TaskRunState = Schema.Literals([
  "PENDING",
  "PROVISIONING",
  "EXECUTING",
  "COMPLETED",
  "FAILED",
]);
export type TaskRunState = typeof TaskRunState.Type;

export const taskRunTransitions: Record<TaskRunState, ReadonlyArray<TaskRunState>> = {
  // queue slot acquired, or dropped/cancelled before provisioning
  PENDING: ["PROVISIONING", "FAILED"],
  // worktree + container ready, or provisioning failure
  PROVISIONING: ["EXECUTING", "FAILED"],
  EXECUTING: ["COMPLETED", "FAILED"],
  COMPLETED: [],
  FAILED: [],
};

export const canTaskRunTransition = (from: TaskRunState, to: TaskRunState): boolean =>
  taskRunTransitions[from].includes(to);

/** Failure cause classification, captured when a turn goes FAILED. */
// RATE_LIMIT is part of the FUR-6 domain spec but has no producer yet — nothing classifies it.
export const TaskRunCause = Schema.Literals(["ERROR", "OOM", "TIMEOUT", "CANCELLED", "RATE_LIMIT"]);
export type TaskRunCause = typeof TaskRunCause.Type;

export const TaskRun = Schema.Struct({
  id: TaskRunId,
  sessionId: SessionId,
  state: TaskRunState,
  createdAt: Schema.Date,
  /** Turn execution deadline (timeout backstop); null while PENDING. */
  expiresAt: Schema.NullOr(Schema.Date),
  /** When the session becomes eligible for LRU eviction after this turn. */
  evictableAfter: Schema.NullOr(Schema.Date),
  /** Set exactly when state is FAILED. */
  cause: Schema.NullOr(TaskRunCause),
  /** Final agent text from the turn's Result event; null until the turn settles. */
  resultText: Schema.NullOr(Schema.String),
  /**
   * Human-readable failure reason, set atomically with the FAILED transition
   * (like `cause`). The same text the turn-failed ticket comment carries — the
   * one source of truth an operator reads in the admin UI without querying
   * Postgres.
   */
  failureSummary: Schema.NullOr(Schema.String),
  /** Root span trace id (M2.10), persisted as soon as the turn's handler starts. */
  traceId: Schema.NullOr(Schema.String),
});
export type TaskRun = typeof TaskRun.Type;
