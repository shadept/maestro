import { Schema } from "effect";

// Agent model/effort control (FUR-41). Three levels, most specific wins:
// task (ingest-parsed override) > project (row overrides) > deployment
// (MAESTRO_AGENT_MODEL / MAESTRO_AGENT_EFFORT). Absent everywhere = the
// claude CLI's own default, byte-for-byte the pre-FUR-41 behavior.

/** The effort levels claude-code 2.1.207 accepts for `--effort <level>`. */
export const AgentEffort = Schema.Literals(["low", "medium", "high", "xhigh", "max"]);
export type AgentEffort = typeof AgentEffort.Type;

/**
 * Per-project agent overrides. Absent key = fall through to the deployment
 * default. `model` is deliberately free-form: the claude CLI validates model
 * ids itself, and an invalid id fails the turn loudly (visible logs) instead
 * of Maestro maintaining a model whitelist that would rot.
 */
export const AgentOverrides = Schema.Struct({
  model: Schema.optionalKey(Schema.NonEmptyString),
  effort: Schema.optionalKey(AgentEffort),
});
export type AgentOverrides = typeof AgentOverrides.Type;
