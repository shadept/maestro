import { describe, expect, it } from "vitest";

import { computeResourceSpec } from "../src/index.ts";

// M2.5 acceptance: unit tests on the two-tier spec computation (Tech
// Requirements §8) — defaults, per-Project overrides, and the burst
// multiplier. `request = agent + project baseline`, `limit = request ×
// multiplier`; CPU carries a request only (soft, never hard-capped).

const agentTier = { memoryBaselineMib: 1024, cpuBaselineMillicores: 1000 };

describe("computeResourceSpec", () => {
  it("with no Project override: request = agent tier, limit = request × default multiplier (2)", () => {
    const spec = computeResourceSpec(agentTier, {});
    expect(spec).toEqual({
      memoryRequestMib: 1024,
      memoryLimitMib: 2048,
      cpuRequestMillicores: 1000,
    });
  });

  it("Project memory/cpu baselines add to the agent tier", () => {
    const spec = computeResourceSpec(agentTier, {
      memoryBaselineMib: 2048,
      cpuBaselineMillicores: 500,
    });
    expect(spec).toEqual({
      memoryRequestMib: 3072,
      memoryLimitMib: 6144,
      cpuRequestMillicores: 1500,
    });
  });

  it("an explicit burstMultiplier overrides the default 2×", () => {
    const spec = computeResourceSpec(agentTier, {
      memoryBaselineMib: 1024,
      burstMultiplier: 3,
    });
    expect(spec.memoryRequestMib).toBe(2048);
    expect(spec.memoryLimitMib).toBe(6144);
  });

  it("CPU never carries a limit — only a request, regardless of multiplier", () => {
    const spec = computeResourceSpec(agentTier, {
      cpuBaselineMillicores: 2000,
      burstMultiplier: 4,
    });
    expect(spec.cpuRequestMillicores).toBe(3000);
    expect(Object.keys(spec)).not.toContain("cpuLimitMillicores");
  });
});
