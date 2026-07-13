import { NodeHttpServer } from "@effect/platform-node";
import type { Session } from "@maestro/domain";
import { sql } from "drizzle-orm";
import { Effect, Layer, Option, Redacted, type Scope } from "effect";
import { HttpBody, HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { MAESTRO_COMMENT_MARKER } from "../../src/callback/format.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { AuditRepo } from "../../src/db/AuditRepo.ts";
import { Db } from "../../src/db/Db.ts";
import { DeliveryRepo } from "../../src/db/DeliveryRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { SessionTerminator } from "../../src/engine/SessionTerminator.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { WebhookRoutes } from "../../src/http/webhooks.ts";
import { IngestPipeline } from "../../src/ingest/IngestPipeline.ts";
import { LinearIngest } from "../../src/ingest/LinearIngest.ts";
import { TurnQueue } from "../../src/queue/TurnQueue.ts";
import { LINEAR_TEST_SECRET, loadLinearFixture, signLinearDelivery } from "../support/linear.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// FUR-18: Linear webhook ingest, end to end over a real HTTP server, real
// Postgres, and the real pg-boss TurnQueue — recorded fixtures only.

const BOT_USER_ID = "b07b07b0-7b07-4b07-8b07-b07b07b07b07";
const TICKET = "FUR-42";

type Services = ProjectRepo | SessionRepo | TaskRunRepo | AuditRepo | HttpClient.HttpClient;

let testDb: TestDb;
let layer: Layer.Layer<Services>;
let layerWithoutSecret: Layer.Layer<Services>;
let layerWithoutBotUserId: Layer.Layer<Services>;

beforeAll(async () => {
  testDb = await startTestDb();
  const makeLayer = (
    secret: Option.Option<Redacted.Redacted>,
    botUserId: Option.Option<string> = Option.some(BOT_USER_ID),
  ) => {
    const config = AppConfig.layerTest({
      databaseUrl: testDb.connectionString,
      linearWebhookSecret: secret,
      linearBotUserId: botUserId,
    });
    const infra = Layer.mergeAll(EventBus.layer, Db.layerTest(testDb.connectionString), config);
    const repos = Layer.mergeAll(
      ProjectRepo.layer,
      SessionRepo.layer,
      TaskRunRepo.layer,
      AuditRepo.layer,
      DeliveryRepo.layer,
    );
    // FUR-19: recordTerminal now tears the session down, so the pipeline
    // needs the real terminator (whose worktree removal is a guarded no-op
    // here — no project was ever cloned under the test storage root).
    const terminator = SessionTerminator.layer.pipe(
      Layer.provide(
        Layer.mergeAll(GitCache.layer, WorktreeManager.layer).pipe(
          Layer.provideMerge(GitCache.layer),
          Layer.provide(RepoLocks.layer),
        ),
      ),
    );
    const ingest = LinearIngest.layer.pipe(
      Layer.provideMerge(IngestPipeline.layer),
      Layer.provide(terminator),
    );
    return HttpRouter.serve(WebhookRoutes, { disableLogger: true, disableListenLog: true }).pipe(
      Layer.provideMerge(NodeHttpServer.layerTest),
      Layer.provide(ingest),
      Layer.provideMerge(repos),
      Layer.provide(TurnQueue.layer),
      Layer.provide(infra),
      Layer.orDie,
    );
  };
  layer = makeLayer(Option.some(Redacted.make(LINEAR_TEST_SECRET)));
  layerWithoutSecret = makeLayer(Option.none());
  layerWithoutBotUserId = makeLayer(Option.some(Redacted.make(LINEAR_TEST_SECRET)), Option.none());
}, 120_000);

afterAll(async () => {
  await testDb.stop();
});

const run = <A, E>(
  effect: Effect.Effect<A, E, Services | Scope.Scope>,
  l: Layer.Layer<Services> = layer,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(l)));

const post = (delivery: { body: string; headers: Record<string, string> }) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.post("/api/webhooks/linear", {
      headers: delivery.headers,
      body: HttpBody.text(delivery.body, "application/json"),
    });
    const text = yield* response.text;
    return { status: response.status, json: JSON.parse(text) as Record<string, unknown> };
  });

/** The fixture with fields of its `data` object replaced. */
const withData = (
  fixture: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> => ({
  ...fixture,
  data: { ...(fixture.data as Record<string, unknown>), ...patch },
});

const activeSession = Effect.gen(function* () {
  const sessions = yield* SessionRepo;
  return yield* sessions.findActiveByTicket({ source: "linear", externalId: TICKET });
});

const runsOf = (session: Session) =>
  Effect.gen(function* () {
    const taskRuns = yield* TaskRunRepo;
    return yield* taskRuns.listBySession(session.id);
  });

const queuedJobs = async (): Promise<ReadonlyArray<{ id: string; group_id: string }>> => {
  const result = await testDb.db.execute(
    sql`select id::text as id, group_id::text as group_id from pgboss.job where name = 'turns' order by created_on, id`,
  );
  return result.rows as Array<{ id: string; group_id: string }>;
};

describe("POST /api/webhooks/linear", () => {
  it("rejects a tampered payload", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-labeled"), {
      tamper: (body) => body.replace("Fix the flux capacitor", "Do something evil"),
    });
    const response = await run(post(delivery));
    expect(response.status).toBe(401);
    expect(await run(activeSession)).toEqual(Option.none());
  });

  it("rejects a delivery outside the replay window", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-labeled"), {
      webhookTimestamp: Date.now() - 10 * 60_000,
    });
    const response = await run(post(delivery));
    expect(response.status).toBe(401);
  });

  it("rejects everything when no webhook secret is configured", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-labeled"));
    const response = await run(post(delivery), layerWithoutSecret);
    expect(response.status).toBe(401);
  });

  it("label event creates a session and queues the first turn", async () => {
    await run(
      Effect.gen(function* () {
        const projects = yield* ProjectRepo;
        yield* projects.create({
          repoGitUrl: "https://github.com/acme/flux.git",
          linearTeamKey: "FUR",
        });
      }),
    );

    const delivery = signLinearDelivery(loadLinearFixture("issue-labeled"));
    const response = await run(post(delivery));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("SessionStarted");

    const session = Option.getOrThrow(await run(activeSession));
    expect(session.gitBranch).toBe(`maestro/${TICKET}`);
    expect(session.state).toBe("WARM_IDLE");

    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(1);
    expect(runs[0]?.state).toBe("PENDING");

    const context = await run(
      Effect.gen(function* () {
        const taskRuns = yield* TaskRunRepo;
        // biome-ignore lint/style/noNonNullAssertion: asserted one run above
        return yield* taskRuns.getContext(runs[0]!.id);
      }),
    );
    expect(context.ticket).toEqual({ source: "linear", externalId: TICKET });
    expect(context.title).toBe("Fix the flux capacitor");
    expect(context.body).toContain("overheats above 88 mph");
    expect(context.actor).toBe("João Furtado");
    expect(context.deliveryId).toBe(delivery.deliveryId);

    const jobs = await queuedJobs();
    expect(jobs.map((job) => job.id)).toEqual([runs[0]?.id]);
    expect(jobs[0]?.group_id).toBe(session.id);
  });

  it("a replayed delivery id is a 200 no-op", async () => {
    const session = Option.getOrThrow(await run(activeSession));
    const before = await run(runsOf(session));

    // Same delivery id as far as Linear is concerned — a redelivery.
    const delivery = signLinearDelivery(loadLinearFixture("issue-labeled"), {
      deliveryId: "d0d0d0d0-aaaa-4bbb-8ccc-eeeeeeeeeee1",
    });
    const first = await run(post(delivery));
    expect(first.status).toBe(200);
    const replay = await run(post(delivery));
    expect(replay.status).toBe(200);
    expect(replay.json.outcome).toBe("Duplicate");

    const after = await run(runsOf(session));
    expect(after).toHaveLength(before.length);
  });

  it("comment event queues turn 2, FIFO after turn 1", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("comment-created"));
    const response = await run(post(delivery));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TurnQueued");

    const session = Option.getOrThrow(await run(activeSession));
    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(2);
    // Monotonic UUIDv7 TaskRunIds are the queue's FIFO invariant (FUR-13):
    // arrival order == id order.
    // biome-ignore lint/style/noNonNullAssertion: length asserted above
    expect(runs[0]!.id < runs[1]!.id).toBe(true);

    const context = await run(
      Effect.gen(function* () {
        const taskRuns = yield* TaskRunRepo;
        // biome-ignore lint/style/noNonNullAssertion: length asserted above
        return yield* taskRuns.getContext(runs[1]!.id);
      }),
    );
    expect(context.title).toBeNull();
    expect(context.body).toBe("Also update the operator handbook, please.");

    const jobs = await queuedJobs();
    expect(jobs.map((job) => job.id)).toEqual(runs.map((r) => r.id));
    expect(new Set(jobs.map((job) => job.group_id))).toEqual(new Set([session.id]));
  });

  it("Maestro's own comment does NOT queue a turn", async () => {
    // Marker-free body so this exercises the bot-userId guard on its own.
    const fixture = withData(loadLinearFixture("comment-created"), {
      userId: BOT_USER_ID,
      user: { id: BOT_USER_ID, name: "Maestro" },
      body: "Turn completed.\n\nDone.",
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(2);
  });

  it("a marker-prefixed comment does NOT queue a turn, whoever authored it (FUR-39)", async () => {
    // Single-account setup: Maestro posts with the human's own API token, so
    // the author id matches the human, not the configured bot id. The content
    // marker must break the loop on its own.
    const fixture = withData(loadLinearFixture("comment-created"), {
      body: `${MAESTRO_COMMENT_MARKER} turn completed.\n\nDone.`,
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(2);
  });

  it("a marker-prefixed comment does NOT queue a turn even with no bot user id configured (FUR-39)", async () => {
    const fixture = withData(loadLinearFixture("comment-created"), {
      body: `${MAESTRO_COMMENT_MARKER} turn failed (ERROR).\n\nBoom.`,
    });
    const response = await run(post(signLinearDelivery(fixture)), layerWithoutBotUserId);
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(2);
  });

  it("a genuine human comment still queues a turn when no bot user id is configured", async () => {
    const fixture = withData(loadLinearFixture("comment-created"), {
      body: "One more thing: bump the changelog.",
    });
    const response = await run(post(signLinearDelivery(fixture)), layerWithoutBotUserId);
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TurnQueued");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("a comment on an untriggered issue is ignored", async () => {
    const fixture = withData(loadLinearFixture("comment-created"), {
      issue: {
        id: "11111111-2222-4333-8444-555555555555",
        identifier: "FUR-777",
        title: "Never labeled",
      },
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
  });

  it("a label event for an unregistered team is ignored", async () => {
    const fixture = loadLinearFixture("issue-labeled");
    const data = fixture.data as Record<string, unknown>;
    const patched = withData(fixture, {
      identifier: "ZZZ-1",
      team: { ...(data.team as Record<string, unknown>), key: "ZZZ" },
    });
    const response = await run(post(signLinearDelivery(patched)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
  });

  it("a paused session ignores genuine human comments until the label resumes it (FUR-39)", async () => {
    // Fresh ticket on the registered team; pause is set directly (the breaker
    // trip itself is executor territory — see the circuit-breaker suite).
    const TICKET_P = "FUR-88";
    const label = withData(loadLinearFixture("issue-labeled"), { identifier: TICKET_P });
    const started = await run(post(signLinearDelivery(label)));
    expect(started.json.outcome).toBe("SessionStarted");

    const paused = await run(
      Effect.gen(function* () {
        const sessions = yield* SessionRepo;
        const session = Option.getOrThrow(
          yield* sessions.findActiveByTicket({ source: "linear", externalId: TICKET_P }),
        );
        return yield* sessions.pause(session.id);
      }),
    );
    expect(paused.newlyPaused).toBe(true);
    const session = paused.session;

    // a genuine human comment (no marker, no bot id) does NOT queue a turn
    const comment = withData(loadLinearFixture("comment-created"), {
      body: "Any progress?",
      issue: {
        id: "9a3b5f80-1e2a-4b0e-9f3d-2c7a8f1e6b01",
        identifier: TICKET_P,
        title: "Fix the flux capacitor",
      },
    });
    const ignored = await run(post(signLinearDelivery(comment)), layerWithoutBotUserId);
    expect(ignored.status).toBe(200);
    expect(ignored.json.outcome).toBe("Ignored");
    expect(String(ignored.json.reason)).toContain("paused");
    expect(await run(runsOf(session))).toHaveLength(1);

    // an issue update whose label set did NOT change must not silently resume
    const unrelatedUpdate = {
      ...withData(loadLinearFixture("issue-labeled"), { identifier: TICKET_P }),
      updatedFrom: { updatedAt: "2026-07-10T11:59:00.000Z", title: "Old title" },
    };
    const noResume = await run(post(signLinearDelivery(unrelatedUpdate)));
    expect(noResume.status).toBe(200);
    expect(noResume.json.outcome).toBe("Ignored");
    expect(await run(runsOf(session))).toHaveLength(1);

    // re-applying the trigger label (updatedFrom carries labelIds) resumes
    // the session and queues a fresh turn from the ticket
    const relabel = withData(loadLinearFixture("issue-labeled"), { identifier: TICKET_P });
    const resumed = await run(post(signLinearDelivery(relabel)));
    expect(resumed.status).toBe(200);
    expect(resumed.json.outcome).toBe("SessionResumed");

    const after = await run(
      Effect.gen(function* () {
        const sessions = yield* SessionRepo;
        return yield* sessions.get(session.id);
      }),
    );
    expect(after.pausedAt).toBeNull();
    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(2);
    expect(runs[1]?.state).toBe("PENDING");

    // the human action is audited
    const audits = await run(
      Effect.gen(function* () {
        const audit = yield* AuditRepo;
        return yield* audit.list;
      }),
    );
    expect(
      audits.some(
        (a) => a.action === "session-resumed" && a.targetEntity === `session:${session.id}`,
      ),
    ).toBe(true);

    // ...and the resumed session accepts comments again
    const followUp = withData(loadLinearFixture("comment-created"), {
      body: "Welcome back.",
      issue: {
        id: "9a3b5f80-1e2a-4b0e-9f3d-2c7a8f1e6b01",
        identifier: TICKET_P,
        title: "Fix the flux capacitor",
      },
    });
    const queued = await run(post(signLinearDelivery(followUp)));
    expect(queued.json.outcome).toBe("TurnQueued");
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  // Task-level `maestro:model=`/`maestro:effort=` labels (FUR-41) were removed
  // as YAGNI — but issues in the wild may still carry them, so they must stay
  // inert: ordinary labels that neither trigger, fail, nor configure anything.
  it("leftover agent-override-shaped labels are inert ordinary labels", async () => {
    const TICKET_M = "FUR-91";
    const label = (name: string, n: number) => ({
      id: `cd1e7f3a-2b8c-4a9d-b5e6-0f4a7c1d8e${n}0`,
      color: "#5e6ad2",
      name,
      parentId: null,
    });
    const fixture = withData(loadLinearFixture("issue-labeled"), {
      identifier: TICKET_M,
      labels: [
        label("maestro", 1),
        label("maestro:model=claude-sonnet-4-5", 2),
        label("maestro:effort=turbo", 3),
      ],
    });
    const started = await run(post(signLinearDelivery(fixture)));
    expect(started.status).toBe(200);
    expect(started.json.outcome).toBe("SessionStarted");

    const session = Option.getOrThrow(
      await run(
        Effect.gen(function* () {
          const sessions = yield* SessionRepo;
          return yield* sessions.findActiveByTicket({ source: "linear", externalId: TICKET_M });
        }),
      ),
    );
    // nothing was parsed off the labels onto the session
    expect(session.agentModel).toBeNull();
    expect(session.agentEffort).toBeNull();
    expect(await run(runsOf(session))).toHaveLength(1);
  });

  it("issue moved to done terminates the session and cancels its queued turns", async () => {
    // FUR-19: the terminal signal is acted on, so capture the session first.
    const session = Option.getOrThrow(await run(activeSession));

    const response = await run(post(signLinearDelivery(loadLinearFixture("issue-completed"))));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TerminalRecorded");

    const after = await run(
      Effect.gen(function* () {
        const sessions = yield* SessionRepo;
        return yield* sessions.get(session.id);
      }),
    );
    expect(after.state).toBe("TERMINATED");
    expect(after.terminationRequestedAt).toBeInstanceOf(Date);
    // the session no longer counts as active for its ticket
    expect(await run(activeSession)).toEqual(Option.none());

    // all queued (PENDING) turns were cancelled — nothing was executing
    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(3);
    for (const taskRun of runs) {
      expect(taskRun.state).toBe("FAILED");
      expect(taskRun.cause).toBe("CANCELLED");
    }

    const audits = await run(
      Effect.gen(function* () {
        const audit = yield* AuditRepo;
        return yield* audit.list;
      }),
    );
    const entry = audits.find((a) => a.targetEntity === `session:${session.id}`);
    expect(entry?.action).toBe("ticket-done");
    expect(entry?.priorState).toBe("WARM_IDLE");
  });

  it("a second terminal delivery for the same ticket is a no-op (double-close)", async () => {
    const response = await run(post(signLinearDelivery(loadLinearFixture("issue-completed"))));
    expect(response.status).toBe(200);
    // no active session anymore — the signal has nothing to act on
    expect(response.json.outcome).toBe("Ignored");
    expect(await run(activeSession)).toEqual(Option.none());
  });

  it("issue moved to canceled terminates its session too", async () => {
    // fresh session on a different ticket of the registered team
    const TICKET_2 = "FUR-77";
    const labelFixture = withData(loadLinearFixture("issue-labeled"), { identifier: TICKET_2 });
    const started = await run(post(signLinearDelivery(labelFixture)));
    expect(started.json.outcome).toBe("SessionStarted");

    const fixture = loadLinearFixture("issue-completed");
    const data = fixture.data as Record<string, unknown>;
    const patched = withData(fixture, {
      identifier: TICKET_2,
      state: { ...(data.state as Record<string, unknown>), name: "Canceled", type: "canceled" },
    });
    const response = await run(post(signLinearDelivery(patched)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TerminalRecorded");

    const terminated = await run(
      Effect.gen(function* () {
        const sessions = yield* SessionRepo;
        return yield* sessions.findActiveByTicket({ source: "linear", externalId: TICKET_2 });
      }),
    );
    expect(terminated).toEqual(Option.none());

    const audits = await run(
      Effect.gen(function* () {
        const audit = yield* AuditRepo;
        return yield* audit.list;
      }),
    );
    expect(audits.some((a) => a.action === "ticket-canceled")).toBe(true);
  });
});
