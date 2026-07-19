import { Schema } from "effect";
import type { ResourceTiers } from "./Project.ts";

// Two-tier resource model (Tech Requirements §8, M2.5): the agent tier
// (orchestrator-wide default, every worker gets it) composes with the project
// tier (per-Project override, ResourceTiers in Project.ts) into a Burstable
// WorkerSpec: `request = agent + project baseline`, `limit = request ×
// multiplier`. CPU carries a request only — soft limits, never hard-capped.

/** Orchestrator-wide baseline, configured once (AppConfig), independent of any Project. */
export const AgentResourceTier = Schema.Struct({
  memoryBaselineMib: Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  cpuBaselineMillicores: Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
});
export type AgentResourceTier = typeof AgentResourceTier.Type;

/**
 * Resolved request/limit a worker is built from — the local CLI runtime
 * renders it as docker flags, the future K8s runtime (M2.10+) as
 * resources.requests/limits on the Job spec. Persisted on the TaskRun that
 * used it (see TaskRun.resources) so the admin UI shows exactly what a
 * settled run was constrained to, immune to later Project config edits.
 */
export const ResourceSpec = Schema.Struct({
  memoryRequestMib: Schema.Number,
  memoryLimitMib: Schema.Number,
  cpuRequestMillicores: Schema.Number,
});
export type ResourceSpec = typeof ResourceSpec.Type;

const DEFAULT_BURST_MULTIPLIER = 2;

export const computeResourceSpec = (
  agentTier: AgentResourceTier,
  project: ResourceTiers,
): ResourceSpec => {
  const memoryRequestMib = agentTier.memoryBaselineMib + (project.memoryBaselineMib ?? 0);
  const multiplier = project.burstMultiplier ?? DEFAULT_BURST_MULTIPLIER;
  return {
    memoryRequestMib,
    memoryLimitMib: Math.round(memoryRequestMib * multiplier),
    cpuRequestMillicores: agentTier.cpuBaselineMillicores + (project.cpuBaselineMillicores ?? 0),
  };
};
