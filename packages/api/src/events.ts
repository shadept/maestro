import { Session, SessionId, TaskRun, TaskRunId } from "@maestro/domain";
import { Schema } from "effect";

// The single discriminated union of SSE event schemas (CLAUDE.md). The
// orchestrator's EventBus publishes these; the admin UI's SSE parser decodes
// them. Design decision: state-change events carry the FULL entity after the
// write, not a delta — snapshot events and live events then share one shape,
// consumers converge by upserting on id, and at-least-once delivery around the
// snapshot/live boundary is harmless.

/** A session row changed (created, state transition, or PR recorded). */
export const SessionStateChanged = Schema.TaggedStruct("SessionStateChanged", {
  session: Session,
});
export type SessionStateChanged = typeof SessionStateChanged.Type;

/** A task run row changed (created or state transition). */
export const TaskRunStateChanged = Schema.TaggedStruct("TaskRunStateChanged", {
  taskRun: TaskRun,
});
export type TaskRunStateChanged = typeof TaskRunStateChanged.Type;

/**
 * Queue activity. DECISION: carries the trigger + the affected turn plus the
 * in-process active-turn count (cheap — tracked by the dispatcher; a queued
 * count would cost a DB query per event). Queue depth beyond that is derivable
 * client-side from TaskRunStateChanged events.
 */
export const QueueChanged = Schema.TaggedStruct("QueueChanged", {
  trigger: Schema.Literals(["enqueued", "dispatched", "settled"]),
  taskRunId: TaskRunId,
  sessionId: SessionId,
  /** Turns running in the orchestrator at publish time. */
  activeCount: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
});
export type QueueChanged = typeof QueueChanged.Type;

/** A raw worker log chunk, published by the executor's log tee. */
export const LogChunk = Schema.TaggedStruct("LogChunk", {
  taskRunId: TaskRunId,
  /** Carried so a session-filtered SSE subscription can match log chunks. */
  sessionId: SessionId,
  chunk: Schema.String,
});
export type LogChunk = typeof LogChunk.Type;

/**
 * Orchestrator-level status. Emitted as the first snapshot event of every SSE
 * subscription; no live producer publishes it in M1 (periodic/live status is
 * an M2 observability concern).
 */
export const SystemStatus = Schema.TaggedStruct("SystemStatus", {
  activeTurns: Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
  maxConcurrentWorkers: Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  dbReachable: Schema.Boolean,
});
export type SystemStatus = typeof SystemStatus.Type;

export const MaestroEvent = Schema.Union([
  SessionStateChanged,
  TaskRunStateChanged,
  QueueChanged,
  LogChunk,
  SystemStatus,
]);
export type MaestroEvent = typeof MaestroEvent.Type;

/**
 * Wire codec for the SSE `data:` field: JSON string ⇆ MaestroEvent. Both ends
 * derive from this one schema (Dates travel as ISO-8601 strings via the
 * canonical JSON codec), so server and client can never drift.
 */
export const MaestroEventFromJsonString = Schema.fromJsonString(Schema.toCodecJson(MaestroEvent));
