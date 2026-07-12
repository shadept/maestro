import { Schema } from "effect";
import { AuditLogId } from "./ids.ts";

// Records manual corrections issued via the admin UI (PRD §5.1).
// Every corrective action lands here: who, what, target, prior state, when.

export const AuditLog = Schema.Struct({
  id: AuditLogId,
  actor: Schema.NonEmptyString,
  action: Schema.NonEmptyString,
  /** Entity reference, e.g. "session:<uuid>" or "task-run:<uuid>". */
  targetEntity: Schema.NonEmptyString,
  /** State before the correction; null when the action created the target. */
  priorState: Schema.NullOr(Schema.String),
  createdAt: Schema.Date,
});
export type AuditLog = typeof AuditLog.Type;
