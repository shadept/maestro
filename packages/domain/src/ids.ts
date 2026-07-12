import { Schema } from "effect";

// Branded entity ids. All ids are UUIDs; the brand keeps them from crossing wires.

export const ProjectId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("ProjectId"));
export type ProjectId = typeof ProjectId.Type;

export const SessionId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("SessionId"));
export type SessionId = typeof SessionId.Type;

export const TaskRunId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("TaskRunId"));
export type TaskRunId = typeof TaskRunId.Type;

export const AuditLogId = Schema.String.check(Schema.isUUID()).pipe(Schema.brand("AuditLogId"));
export type AuditLogId = typeof AuditLogId.Type;
