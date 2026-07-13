import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import {
  AuditLog,
  GitCommandError,
  Project,
  Session,
  StateTransitionError,
  TaskContext,
  TaskRun,
} from "../src/index.ts";

const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;

const roundTrip = <A, I>(schema: Schema.Codec<A, I>, input: unknown): A => {
  const decoded = Schema.decodeUnknownSync(schema)(input);
  const encoded = Schema.encodeSync(schema)(decoded);
  const redecoded = Schema.decodeUnknownSync(schema)(encoded);
  expect(redecoded).toEqual(decoded);
  return decoded;
};

describe("Project", () => {
  const valid = {
    id: uuid(1),
    repoGitUrl: "https://github.com/shadept/maestro",
    linearTeamKey: "FUR",
    localCachePath: null,
    gitConventions: { branchPattern: "maestro/{ticketKey}", draftPr: true },
    resources: { memoryBaselineMib: 2048 },
    agent: { model: "claude-sonnet-4-5", effort: "low" },
    createdAt: new Date("2026-07-12T00:00:00Z"),
  };

  it("round-trips", () => {
    const project = roundTrip(Project, valid);
    expect(project.repoGitUrl).toBe(valid.repoGitUrl);
    expect(project.gitConventions.branchPattern).toBe("maestro/{ticketKey}");
  });

  it("accepts empty override structs", () => {
    roundTrip(Project, { ...valid, gitConventions: {}, resources: {}, agent: {} });
  });

  it("rejects an unknown agent effort level and an empty model (FUR-41)", () => {
    expect(() =>
      Schema.decodeUnknownSync(Project)({ ...valid, agent: { effort: "turbo" } }),
    ).toThrow();
    expect(() => Schema.decodeUnknownSync(Project)({ ...valid, agent: { model: "" } })).toThrow();
  });

  it("rejects a non-uuid id", () => {
    expect(() => Schema.decodeUnknownSync(Project)({ ...valid, id: "not-a-uuid" })).toThrow();
  });

  it("rejects non-positive memory baseline and multiplier <= 1", () => {
    expect(() =>
      Schema.decodeUnknownSync(Project)({ ...valid, resources: { memoryBaselineMib: 0 } }),
    ).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(Project)({ ...valid, resources: { burstMultiplier: 1 } }),
    ).toThrow();
  });
});

describe("Session", () => {
  const valid = {
    id: uuid(2),
    projectId: uuid(1),
    ticketReference: { source: "linear", externalId: "FUR-42" },
    gitBranch: "maestro/FUR-42",
    claudeSessionUuid: null,
    prNumber: null,
    prUrl: null,
    terminationRequestedAt: null,
    pausedAt: null,
    agentModel: null,
    agentEffort: null,
    state: "WARM_IDLE",
    createdAt: new Date("2026-07-12T00:00:00Z"),
    lastActivityAt: new Date("2026-07-12T01:00:00Z"),
  };

  it("round-trips", () => {
    const session = roundTrip(Session, valid);
    expect(session.state).toBe("WARM_IDLE");
    expect(session.claudeSessionUuid).toBeNull();
  });

  it("accepts a stored claude session uuid", () => {
    roundTrip(Session, { ...valid, claudeSessionUuid: uuid(9) });
  });

  it("accepts a persisted termination marker (FUR-19 deferred teardown)", () => {
    const marked = roundTrip(Session, {
      ...valid,
      terminationRequestedAt: new Date("2026-07-12T02:00:00Z"),
    });
    expect(marked.terminationRequestedAt).toEqual(new Date("2026-07-12T02:00:00Z"));
  });

  it("accepts a persisted circuit-breaker pause marker (FUR-39)", () => {
    const paused = roundTrip(Session, { ...valid, pausedAt: new Date("2026-07-12T03:00:00Z") });
    expect(paused.pausedAt).toEqual(new Date("2026-07-12T03:00:00Z"));
  });

  it("accepts pinned agent settings and rejects an unknown effort (FUR-41)", () => {
    const pinned = roundTrip(Session, {
      ...valid,
      agentModel: "claude-opus-4-6",
      agentEffort: "max",
    });
    expect(pinned.agentModel).toBe("claude-opus-4-6");
    expect(pinned.agentEffort).toBe("max");
    expect(() => Schema.decodeUnknownSync(Session)({ ...valid, agentEffort: "turbo" })).toThrow();
  });

  it("accepts stored PR coordinates and rejects a non-positive PR number", () => {
    const withPr = roundTrip(Session, {
      ...valid,
      prNumber: 7,
      prUrl: "https://github.com/shadept/maestro/pull/7",
    });
    expect(withPr.prNumber).toBe(7);
    expect(() => Schema.decodeUnknownSync(Session)({ ...valid, prNumber: 0 })).toThrow();
    expect(() => Schema.decodeUnknownSync(Session)({ ...valid, prUrl: "" })).toThrow();
  });

  it("rejects an unknown state literal", () => {
    expect(() => Schema.decodeUnknownSync(Session)({ ...valid, state: "SLEEPING" })).toThrow();
  });

  it("rejects an empty git branch", () => {
    expect(() => Schema.decodeUnknownSync(Session)({ ...valid, gitBranch: "" })).toThrow();
  });
});

describe("TaskRun", () => {
  const valid = {
    id: uuid(3),
    sessionId: uuid(2),
    state: "PENDING",
    createdAt: new Date("2026-07-12T00:00:00Z"),
    expiresAt: null,
    evictableAfter: null,
    cause: null,
    resultText: null,
    failureSummary: null,
  };

  it("round-trips", () => {
    const run = roundTrip(TaskRun, valid);
    expect(run.state).toBe("PENDING");
  });

  it("round-trips a failed run with cause, deadlines, and failure summary", () => {
    const failed = roundTrip(TaskRun, {
      ...valid,
      state: "FAILED",
      expiresAt: new Date("2026-07-12T02:00:00Z"),
      evictableAfter: new Date("2026-07-12T03:00:00Z"),
      cause: "OOM",
      failureSummary: "worker exited with code 137 (OOM)",
    });
    expect(failed.cause).toBe("OOM");
    expect(failed.failureSummary).toBe("worker exited with code 137 (OOM)");
  });

  it("rejects an unknown cause", () => {
    expect(() =>
      Schema.decodeUnknownSync(TaskRun)({ ...valid, state: "FAILED", cause: "GREMLINS" }),
    ).toThrow();
  });
});

describe("AuditLog", () => {
  it("round-trips", () => {
    roundTrip(AuditLog, {
      id: uuid(4),
      actor: "admin",
      action: "retry-failed-turn",
      targetEntity: `task-run:${uuid(3)}`,
      priorState: "FAILED",
      createdAt: new Date("2026-07-12T00:00:00Z"),
    });
  });
});

describe("TaskContext", () => {
  const valid = {
    source: "linear",
    ticket: { source: "linear", externalId: "FUR-42" },
    actor: "shade",
    title: "Add a lint rule",
    body: "Please add a lint rule",
    deliveryId: "delivery-123",
    payload: { type: "Issue", action: "update", nested: { anything: [1, 2, 3] } },
  };

  it("round-trips and preserves the opaque payload", () => {
    const ctx = roundTrip(TaskContext, valid);
    expect(ctx.payload).toEqual(valid.payload);
  });

  it("rejects an unknown source", () => {
    expect(() => Schema.decodeUnknownSync(TaskContext)({ ...valid, source: "jira" })).toThrow();
  });

  it("rejects an empty delivery id", () => {
    expect(() => Schema.decodeUnknownSync(TaskContext)({ ...valid, deliveryId: "" })).toThrow();
  });

  it("tolerates legacy agentModel/agentEffort keys in stored contexts (removed FUR-41 task level)", () => {
    // Migration 0006 backfilled these keys into task_runs.context rows; the
    // fields were later removed as YAGNI. Decoding must strip, never reject.
    const ctx = Schema.decodeUnknownSync(TaskContext)({
      ...valid,
      agentModel: "claude-haiku-4-5",
      agentEffort: "low",
    });
    expect(ctx).not.toHaveProperty("agentModel");
    expect(ctx).not.toHaveProperty("agentEffort");
  });
});

describe("tagged errors", () => {
  it("carry their tag and fields", () => {
    const err = new StateTransitionError({
      entity: "Session",
      entityId: uuid(2),
      from: "TERMINATED",
      to: "WARM_IDLE",
    });
    expect(err._tag).toBe("StateTransitionError");
    expect(err.from).toBe("TERMINATED");

    const git = new GitCommandError({ command: "git fetch", exitCode: 128, stderr: "boom" });
    expect(git._tag).toBe("GitCommandError");
  });
});
