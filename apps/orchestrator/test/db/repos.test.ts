import type { Project, Session, TaskContext, TaskRun } from "@maestro/domain";
import { Effect, Exit, Layer, Option } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AuditRepo } from "../../src/db/AuditRepo.ts";
import { Db } from "../../src/db/Db.ts";
import { DeliveryRepo } from "../../src/db/DeliveryRepo.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

type Repos = ProjectRepo | SessionRepo | TaskRunRepo | AuditRepo | DeliveryRepo | OutboxRepo;

let testDb: TestDb;
let layer: Layer.Layer<Repos>;

beforeAll(async () => {
  testDb = await startTestDb();
  layer = Layer.mergeAll(
    ProjectRepo.layer,
    SessionRepo.layer,
    TaskRunRepo.layer,
    AuditRepo.layer,
    DeliveryRepo.layer,
    OutboxRepo.layer,
  ).pipe(Layer.provideMerge(Db.layerTest(testDb.connectionString)), Layer.provide(EventBus.layer));
});

afterAll(async () => {
  await testDb.stop();
});

const run = <A, E>(effect: Effect.Effect<A, E, Repos>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, layer));

const runExit = <A, E>(effect: Effect.Effect<A, E, Repos>) =>
  Effect.runPromiseExit(Effect.provide(effect, layer));

const makeProject = Effect.gen(function* () {
  const repo = yield* ProjectRepo;
  return yield* repo.create({ repoGitUrl: "https://github.com/shadept/maestro" });
});

const makeSession = (project: Project) =>
  Effect.gen(function* () {
    const repo = yield* SessionRepo;
    return yield* repo.create({
      projectId: project.id,
      ticketReference: { source: "linear", externalId: `FUR-${Math.floor(Math.random() * 1e9)}` },
      gitBranch: "maestro/FUR-9",
    });
  });

const taskContext = (session: Session): TaskContext => ({
  source: "linear",
  ticket: session.ticketReference,
  actor: "shade",
  title: "A ticket",
  body: "Do the thing.",
  agentModel: null,
  agentEffort: null,
  deliveryId: `d-${session.id}`,
  payload: {},
});

const makeRun = (session: Session) =>
  Effect.gen(function* () {
    const repo = yield* TaskRunRepo;
    return yield* repo.create(session.id, taskContext(session));
  });

describe("ProjectRepo", () => {
  it("create / get / list / setLocalCachePath", async () => {
    const project = await run(makeProject);
    expect(project.localCachePath).toBeNull();

    const fetched = await run(
      Effect.gen(function* () {
        const repo = yield* ProjectRepo;
        return yield* repo.get(project.id);
      }),
    );
    expect(fetched.id).toBe(project.id);

    const updated = await run(
      Effect.gen(function* () {
        const repo = yield* ProjectRepo;
        return yield* repo.setLocalCachePath(project.id, "/var/lib/maestro/cache/x");
      }),
    );
    expect(updated.localCachePath).toBe("/var/lib/maestro/cache/x");

    const all = await run(
      Effect.gen(function* () {
        const repo = yield* ProjectRepo;
        return yield* repo.list;
      }),
    );
    expect(all.some((p) => p.id === project.id)).toBe(true);
  });

  it("get of a missing project fails with EntityNotFoundError", async () => {
    const exit = await runExit(
      Effect.gen(function* () {
        const repo = yield* ProjectRepo;
        return yield* repo.get("00000000-0000-4000-8000-000000000000" as Project["id"]);
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(String(exit.cause)).toContain("EntityNotFoundError");
    }
  });
});

describe("SessionRepo", () => {
  it("create starts WARM_IDLE and findActiveByTicket sees it", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    expect(session.state).toBe("WARM_IDLE");

    const found = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.findActiveByTicket(session.ticketReference);
      }),
    );
    expect(Option.isSome(found)).toBe(true);
  });

  it("legal transition succeeds; illegal fails with StateTransitionError", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));

    const dormant = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.transition(session.id, "DORMANT_SAVED");
      }),
    );
    expect(dormant.state).toBe("DORMANT_SAVED");

    const terminated = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.transition(session.id, "TERMINATED");
      }),
    );
    expect(terminated.state).toBe("TERMINATED");

    // TERMINATED is terminal
    const exit = await runExit(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.transition(session.id, "WARM_IDLE");
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(String(exit.cause)).toContain("StateTransitionError");
    }

    // terminated session no longer active for its ticket
    const found = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.findActiveByTicket(session.ticketReference);
      }),
    );
    expect(Option.isNone(found)).toBe(true);
  });

  it("stores the claude session uuid and touches activity", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const uuid = "123e4567-e89b-42d3-a456-426614174000";

    const withUuid = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.setClaudeSessionUuid(session.id, uuid);
      }),
    );
    expect(withUuid.claudeSessionUuid).toBe(uuid);

    const touched = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.touchActivity(session.id);
      }),
    );
    expect(touched.lastActivityAt.getTime()).toBeGreaterThanOrEqual(
      session.lastActivityAt.getTime(),
    );
  });

  it("stores the forge PR reference", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    expect(session.prNumber).toBeNull();
    expect(session.prUrl).toBeNull();

    const withPr = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.setPullRequest(session.id, {
          number: 41,
          url: "https://github.com/shadept/maestro/pull/41",
        });
      }),
    );
    expect(withPr.prNumber).toBe(41);
    expect(withPr.prUrl).toBe("https://github.com/shadept/maestro/pull/41");
  });

  it("pause is set-once with a race-safe newlyPaused flag; resume clears it (FUR-39)", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    expect(session.pausedAt).toBeNull();

    const first = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.pause(session.id);
      }),
    );
    expect(first.newlyPaused).toBe(true);
    expect(first.session.pausedAt).toBeInstanceOf(Date);

    // a second trip is silent: same timestamp, newlyPaused false
    const second = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        return yield* repo.pause(session.id);
      }),
    );
    expect(second.newlyPaused).toBe(false);
    expect(second.session.pausedAt).toEqual(first.session.pausedAt);

    // resume clears the marker (idempotent), and a resumed session can trip again
    const again = await run(
      Effect.gen(function* () {
        const repo = yield* SessionRepo;
        const resumed = yield* repo.resume(session.id);
        expect(resumed.pausedAt).toBeNull();
        yield* repo.resume(session.id); // no-op
        return yield* repo.pause(session.id);
      }),
    );
    expect(again.newlyPaused).toBe(true);
  });

  it("concurrent conflicting transitions: exactly one wins", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));

    const attempt = () =>
      runExit(
        Effect.gen(function* () {
          const repo = yield* SessionRepo;
          return yield* repo.transition(session.id, "DORMANT_SAVED");
        }),
      );

    const [a, b] = await Promise.all([attempt(), attempt()]);
    const successes = [a, b].filter(Exit.isSuccess);
    const failures = [a, b].filter(Exit.isFailure);
    expect(successes).toHaveLength(1);
    expect(failures).toHaveLength(1);
    expect(String(failures[0]?.cause)).toContain("StateTransitionError");
  });
});

describe("TaskRunRepo", () => {
  it("walks the full happy path with CAS transitions", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const created = await run(makeRun(session));
    expect(created.state).toBe("PENDING");

    const result = await run(
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        const context = yield* repo.getContext(created.id);
        expect(context).toEqual(taskContext(session));
        yield* repo.transition(created.id, "PROVISIONING");
        const executing = yield* repo.transition(created.id, "EXECUTING", {
          expiresAt: new Date(Date.now() + 60_000),
        });
        expect(executing.expiresAt).toBeInstanceOf(Date);
        return yield* repo.transition(created.id, "COMPLETED", {
          evictableAfter: new Date(Date.now() + 3_600_000),
          resultText: "All done.",
        });
      }),
    );
    expect(result.state).toBe("COMPLETED");
    expect(result.evictableAfter).toBeInstanceOf(Date);
    expect(result.resultText).toBe("All done.");
  });

  it("records a failure cause atomically with the transition", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const created = await run(makeRun(session));

    const failed = await run(
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        yield* repo.transition(created.id, "PROVISIONING");
        return yield* repo.transition(created.id, "FAILED", { cause: "OOM" });
      }),
    );
    expect(failed.state).toBe("FAILED");
    expect(failed.cause).toBe("OOM");
  });

  it("illegal transition fails with StateTransitionError", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const created = await run(makeRun(session));

    // PENDING -> COMPLETED skips two states
    const exit = await runExit(
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        return yield* repo.transition(created.id, "COMPLETED");
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(String(exit.cause)).toContain("StateTransitionError");
    }
  });

  it("concurrent conflicting transitions: exactly one wins", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const created = await run(makeRun(session));

    const attempt = () =>
      runExit(
        Effect.gen(function* () {
          const repo = yield* TaskRunRepo;
          return yield* repo.transition(created.id, "PROVISIONING");
        }),
      );

    const results = await Promise.all([attempt(), attempt(), attempt()]);
    expect(results.filter(Exit.isSuccess)).toHaveLength(1);
    expect(results.filter(Exit.isFailure)).toHaveLength(2);
  });

  it("countConsecutiveFailures: trailing failures only, success resets, CANCELLED is skipped (FUR-39)", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));

    const settle = (to: "COMPLETED" | "FAILED", cause?: "ERROR" | "CANCELLED") =>
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        const created = yield* repo.create(session.id, taskContext(session));
        yield* repo.transition(created.id, "PROVISIONING");
        yield* repo.transition(created.id, "EXECUTING");
        yield* repo.transition(created.id, to, cause !== undefined ? { cause } : {});
      });

    const count = Effect.gen(function* () {
      const repo = yield* TaskRunRepo;
      return yield* repo.countConsecutiveFailures(session.id);
    });

    expect(await run(count)).toBe(0);

    await run(settle("FAILED", "ERROR"));
    await run(settle("FAILED", "ERROR"));
    expect(await run(count)).toBe(2);

    // a success resets the streak
    await run(settle("COMPLETED"));
    expect(await run(count)).toBe(0);

    // an unsettled (PENDING) run neither counts nor breaks the streak
    await run(settle("FAILED", "ERROR"));
    await run(makeRun(session));
    expect(await run(count)).toBe(1);

    // CANCELLED says nothing about agent health: skipped, not a reset
    await run(settle("FAILED", "CANCELLED"));
    await run(settle("FAILED", "ERROR"));
    expect(await run(count)).toBe(2);
  });

  it("appendLogs accumulates chunks in order", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const created = await run(makeRun(session));

    const logs = await run(
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        yield* repo.appendLogs(created.id, "chunk-1\n");
        yield* repo.appendLogs(created.id, "chunk-2\n");
        yield* repo.appendLogs(created.id, "chunk-3\n");
        return yield* repo.getLogs(created.id);
      }),
    );
    expect(logs).toBe("chunk-1\nchunk-2\nchunk-3\n");
  });

  it("lists runs for a session in creation order", async () => {
    const project = await run(makeProject);
    const session = await run(makeSession(project));
    const first = await run(makeRun(session));
    const second = await run(makeRun(session));

    const runs = await run(
      Effect.gen(function* () {
        const repo = yield* TaskRunRepo;
        return yield* repo.listBySession(session.id);
      }),
    );
    expect(runs.map((r: TaskRun) => r.id)).toEqual([first.id, second.id]);
  });
});

describe("AuditRepo", () => {
  it("records and lists corrections", async () => {
    const entry = await run(
      Effect.gen(function* () {
        const repo = yield* AuditRepo;
        return yield* repo.record({
          actor: "admin",
          action: "force-terminate",
          targetEntity: "session:x",
          priorState: "WARM_IDLE",
        });
      }),
    );
    expect(entry.priorState).toBe("WARM_IDLE");

    const all = await run(
      Effect.gen(function* () {
        const repo = yield* AuditRepo;
        return yield* repo.list;
      }),
    );
    expect(all.some((a) => a.id === entry.id)).toBe(true);
  });
});

describe("DeliveryRepo", () => {
  it("recordIfNew is true once and false on redelivery", async () => {
    const record = {
      source: "linear" as const,
      deliveryId: `d-${Date.now()}`,
      payload: { action: "update" },
    };
    const [first, second] = await run(
      Effect.gen(function* () {
        const repo = yield* DeliveryRepo;
        const a = yield* repo.recordIfNew(record);
        const b = yield* repo.recordIfNew(record);
        return [a, b] as const;
      }),
    );
    expect(first).toBe(true);
    expect(second).toBe(false);
  });
});

describe("OutboxRepo", () => {
  it("enqueue is idempotent; lifecycle marks sent / records failure", async () => {
    const key = `outbox-${Date.now()}`;
    const [a, b, pendingBefore] = await run(
      Effect.gen(function* () {
        const repo = yield* OutboxRepo;
        const first = yield* repo.enqueue({
          target: "linear",
          payload: { c: 1 },
          idempotencyKey: key,
        });
        const dup = yield* repo.enqueue({
          target: "linear",
          payload: { c: 1 },
          idempotencyKey: key,
        });
        const pending = yield* repo.listPending(100);
        return [first, dup, pending] as const;
      }),
    );
    expect(a.id).toBe(b.id);
    expect(pendingBefore.some((e) => e.id === a.id)).toBe(true);

    await run(
      Effect.gen(function* () {
        const repo = yield* OutboxRepo;
        yield* repo.recordFailure(a.id, "network sad");
        yield* repo.markSent(a.id);
      }),
    );

    const pendingAfter = await run(
      Effect.gen(function* () {
        const repo = yield* OutboxRepo;
        return yield* repo.listPending(100);
      }),
    );
    expect(pendingAfter.some((e) => e.id === a.id)).toBe(false);
  });
});
