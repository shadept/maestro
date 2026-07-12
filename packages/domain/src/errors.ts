import { Schema } from "effect";

// Tagged schema errors, one union per area. Class names end in `Error`.
// Unions grow as their area's services land; members stay concrete and small.

// ── state machine (db area) ────────────────────────────────────────────────

export class StateTransitionError extends Schema.TaggedErrorClass<StateTransitionError>()(
  "StateTransitionError",
  {
    entity: Schema.Literals(["Session", "TaskRun"]),
    entityId: Schema.String,
    from: Schema.String,
    to: Schema.String,
  },
) {}

export class DbQueryError extends Schema.TaggedErrorClass<DbQueryError>()("DbQueryError", {
  /** The repo method that failed, e.g. "SessionRepo.create". */
  operation: Schema.String,
  message: Schema.String,
}) {}

export class EntityNotFoundError extends Schema.TaggedErrorClass<EntityNotFoundError>()(
  "EntityNotFoundError",
  { entity: Schema.String, entityId: Schema.String },
) {}

export type DbError = DbQueryError | EntityNotFoundError | StateTransitionError;

// ── git area ───────────────────────────────────────────────────────────────

export class GitCommandError extends Schema.TaggedErrorClass<GitCommandError>()("GitCommandError", {
  /** The git subcommand that failed (never includes credentials). */
  command: Schema.String,
  exitCode: Schema.NullOr(Schema.Number),
  stderr: Schema.String,
}) {}

export type GitError = GitCommandError;

// ── worker runtime area ────────────────────────────────────────────────────

export class WorkerSpawnError extends Schema.TaggedErrorClass<WorkerSpawnError>()(
  "WorkerSpawnError",
  { reason: Schema.String },
) {}

export class WorkerNotFoundError extends Schema.TaggedErrorClass<WorkerNotFoundError>()(
  "WorkerNotFoundError",
  { workerId: Schema.String },
) {}

export class NotImplementedError extends Schema.TaggedErrorClass<NotImplementedError>()(
  "NotImplementedError",
  { feature: Schema.String },
) {}

export type RuntimeError = WorkerSpawnError | WorkerNotFoundError | NotImplementedError;

// ── ingestion area ─────────────────────────────────────────────────────────

export class WebhookVerificationError extends Schema.TaggedErrorClass<WebhookVerificationError>()(
  "WebhookVerificationError",
  { source: Schema.String, reason: Schema.String },
) {}

export class IngestMappingError extends Schema.TaggedErrorClass<IngestMappingError>()(
  "IngestMappingError",
  { deliveryId: Schema.String, reason: Schema.String },
) {}

export type IngestError = WebhookVerificationError | IngestMappingError;
