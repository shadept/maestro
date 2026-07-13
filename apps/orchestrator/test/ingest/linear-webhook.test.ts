import { NodeHttpServer } from "@effect/platform-node";
import type { Session, TaskRunId } from "@maestro/domain";
import { sql } from "drizzle-orm";
import { Effect, Layer, Option, Redacted, type Scope } from "effect";
import { HttpBody, HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { MAESTRO_COMMENT_MARKER } from "../../src/callback/format.ts";
import { LinearCallback } from "../../src/callback/LinearCallback.ts";
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

// FUR-18/FUR-37: Linear webhook ingest, end to end over a real HTTP server,
// real Postgres, and the real pg-boss TurnQueue — recorded fixtures only.
// Trigger model under test is FUR-37's: issue delegation to the Maestro app
// user starts work, @maestro mentions queue follow-up turns, plain comments
// are inert, and the label trigger is gone (its tests are replaced below with
// delegation equivalents, not deleted).

// The Maestro app user — matches issue-delegated.json's data.delegateId.
const BOT_USER_ID = "eff9ea77-7653-46b1-b7e0-cd649140807b";
const TICKET = "FUR-37";
const TEAM = {
  id: "6d81e504-6a8a-44f0-ae14-000191e67ac0",
  key: "FUR",
  name: "Furtado Interactive",
};

// Session-less mention targets, resolved via LinearCallback.layerTest's
// seeded delegation lookups (comment webhooks carry no delegate).
const ISSUE_DELEGATED_SESSIONLESS = "1efc90aa-0000-4000-8000-000000000090"; // FUR-90
const ISSUE_NOT_DELEGATED = "1efc90aa-0000-4000-8000-000000000091"; // FUR-91
const ISSUE_LOOKUP_FAILS = "1efc90aa-0000-4000-8000-000000000092"; // FUR-92, unseeded

const DELEGATIONS = {
  [ISSUE_DELEGATED_SESSIONLESS]: {
    delegateId: BOT_USER_ID,
    description: "Polish the operator handbook.",
  },
  [ISSUE_NOT_DELEGATED]: { delegateId: null, description: "Humans only." },
};

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
      Layer.provide(LinearCallback.layerTest({ delegations: DELEGATIONS })),
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

/** The delegation event, optionally retargeted at another ticket. */
const delegationEvent = (patch: Record<string, unknown> = {}) =>
  withData(loadLinearFixture("issue-delegated"), patch);

/** A human comment on some issue (defaults to the delegated FUR-37 one). */
const commentOn = (args: {
  readonly body: string;
  readonly identifier?: string;
  readonly issueId?: string;
  readonly title?: string;
}) =>
  withData(loadLinearFixture("comment-mention"), {
    body: args.body,
    ...(args.issueId !== undefined && { issueId: args.issueId }),
    ...((args.identifier !== undefined || args.issueId !== undefined) && {
      issue: {
        id: args.issueId ?? "976f65f1-83e6-45d6-82a8-d10161e116bc",
        identifier: args.identifier ?? TICKET,
        title: args.title ?? "Fix the flux capacitor",
        team: TEAM,
      },
    }),
  });

const activeSessionFor = (ticket: string) =>
  Effect.gen(function* () {
    const sessions = yield* SessionRepo;
    return yield* sessions.findActiveByTicket({ source: "linear", externalId: ticket });
  });

const activeSession = activeSessionFor(TICKET);

const runsOf = (session: Session) =>
  Effect.gen(function* () {
    const taskRuns = yield* TaskRunRepo;
    return yield* taskRuns.listBySession(session.id);
  });

const contextOf = (taskRunId: TaskRunId) =>
  Effect.gen(function* () {
    const taskRuns = yield* TaskRunRepo;
    return yield* taskRuns.getContext(taskRunId);
  });

const queuedJobs = async (): Promise<ReadonlyArray<{ id: string; group_id: string }>> => {
  const result = await testDb.db.execute(
    sql`select id::text as id, group_id::text as group_id from pgboss.job where name = 'turns' order by created_on, id`,
  );
  return result.rows as Array<{ id: string; group_id: string }>;
};

describe("POST /api/webhooks/linear", () => {
  it("rejects a tampered payload", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-delegated"), {
      tamper: (body) => body.replace("agent-delegation trigger", "something evil"),
    });
    const response = await run(post(delivery));
    expect(response.status).toBe(401);
    expect(await run(activeSession)).toEqual(Option.none());
  });

  it("rejects a delivery outside the replay window", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-delegated"), {
      webhookTimestamp: Date.now() - 10 * 60_000,
    });
    const response = await run(post(delivery));
    expect(response.status).toBe(401);
  });

  it("rejects everything when no webhook secret is configured", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("issue-delegated"));
    const response = await run(post(delivery), layerWithoutSecret);
    expect(response.status).toBe(401);
  });

  it("delegating the issue to the Maestro app user creates a session and queues the first turn", async () => {
    await run(
      Effect.gen(function* () {
        const projects = yield* ProjectRepo;
        yield* projects.create({
          repoGitUrl: "https://github.com/acme/flux.git",
          linearTeamKey: "FUR",
        });
      }),
    );

    const delivery = signLinearDelivery(loadLinearFixture("issue-delegated"));
    const response = await run(post(delivery));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("SessionStarted");

    const session = Option.getOrThrow(await run(activeSession));
    expect(session.gitBranch).toBe(`maestro/${TICKET}`);
    expect(session.state).toBe("WARM_IDLE");

    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(1);
    expect(runs[0]?.state).toBe("PENDING");

    // biome-ignore lint/style/noNonNullAssertion: asserted one run above
    const context = await run(contextOf(runs[0]!.id));
    expect(context.ticket).toEqual({ source: "linear", externalId: TICKET });
    expect(context.title).toBe("M2.17 — Linear agent-delegation trigger (spike-dependent)");
    expect(context.body).toContain("native agent delegation");
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
    const delivery = signLinearDelivery(loadLinearFixture("issue-delegated"), {
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

  it("a re-delivered delegation for an already-active ticket never double-triggers", async () => {
    const session = Option.getOrThrow(await run(activeSession));
    const before = await run(runsOf(session));

    // Fresh delivery id, same delegation content — reshuffled webhook.
    const response = await run(post(signLinearDelivery(loadLinearFixture("issue-delegated"))));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("already active");
    expect(await run(runsOf(session))).toHaveLength(before.length);
  });

  it("an issue update without a delegate change does not trigger (assignee changes included)", async () => {
    // Captured shape: assigning the HUMAN carries assigneeId in updatedFrom
    // while data.delegateId is still set — "still delegated" is not evidence.
    const fixture = {
      ...delegationEvent({ identifier: "FUR-550", id: "1efc90aa-0000-4000-8000-000000000550" }),
      updatedFrom: { updatedAt: "2026-07-13T10:54:50.693Z", assigneeId: null },
    };
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("no delegation change");
    expect(await run(activeSessionFor("FUR-550"))).toEqual(Option.none());
  });

  it("a delegation to a different agent user is ignored", async () => {
    const fixture = delegationEvent({
      identifier: "FUR-555",
      id: "1efc90aa-0000-4000-8000-000000000555",
      delegateId: "99999999-9999-4999-8999-999999999999",
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(await run(activeSessionFor("FUR-555"))).toEqual(Option.none());
  });

  it("a delegation with no bot user id configured is loudly ignored", async () => {
    const fixture = delegationEvent({
      identifier: "FUR-556",
      id: "1efc90aa-0000-4000-8000-000000000556",
    });
    const response = await run(post(signLinearDelivery(fixture)), layerWithoutBotUserId);
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("MAESTRO_LINEAR_BOT_USER_ID");
    expect(await run(activeSessionFor("FUR-556"))).toEqual(Option.none());
  });

  it("a delegation for an unregistered team is ignored", async () => {
    const fixture = delegationEvent({
      identifier: "ZZZ-1",
      id: "1efc90aa-0000-4000-8000-000000000999",
      team: { ...TEAM, key: "ZZZ" },
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
  });

  it("the maestro trigger label no longer starts a session (FUR-37 replacement)", async () => {
    // The old trigger, verbatim (label present + updatedFrom.labelIds): the
    // label event must now be an inert issue update.
    const response = await run(post(signLinearDelivery(loadLinearFixture("issue-labeled"))));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("no delegation change");
    expect(await run(activeSessionFor("FUR-42"))).toEqual(Option.none());
  });

  it("an @maestro mention queues turn 2 with the comment body as prompt, FIFO after turn 1", async () => {
    const delivery = signLinearDelivery(loadLinearFixture("comment-mention"));
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

    // biome-ignore lint/style/noNonNullAssertion: length asserted above
    const context = await run(contextOf(runs[1]!.id));
    expect(context.title).toBeNull();
    expect(context.body).toBe("@maestro test");

    const jobs = await queuedJobs();
    expect(jobs.map((job) => job.id)).toEqual(runs.map((r) => r.id));
    expect(new Set(jobs.map((job) => job.group_id))).toEqual(new Set([session.id]));
  });

  it("mention matching is case-insensitive", async () => {
    const response = await run(
      post(signLinearDelivery(commentOn({ body: "@MAESTRO also bump the changelog" }))),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TurnQueued");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("a plain human comment on an active session queues NOTHING (deliberate FUR-37 change)", async () => {
    const response = await run(
      post(signLinearDelivery(commentOn({ body: "Nice work so far, reviewing tomorrow." }))),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("plain comment");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("handle lookalikes are not mentions (word boundaries)", async () => {
    for (const body of ["reach me at ops@maestro.dev", "@maestrofoo take note", "@maestro-bot?"]) {
      const response = await run(post(signLinearDelivery(commentOn({ body }))));
      expect(response.status).toBe(200);
      expect(response.json.outcome).toBe("Ignored");
    }
    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("Maestro's own marker comment does NOT queue a turn even when it contains @maestro", async () => {
    // The paused-session message literally tells humans to "mention
    // @maestro" — the marker guard must run BEFORE mention detection.
    const response = await run(
      post(
        signLinearDelivery(
          commentOn({ body: `${MAESTRO_COMMENT_MARKER} mention @maestro in a comment to resume.` }),
        ),
      ),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("marker");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("a bot-authored comment does NOT queue a turn even when it contains @maestro", async () => {
    // Marker-free body so this exercises the bot-userId guard on its own,
    // ordered before mention handling.
    const fixture = withData(commentOn({ body: "Working on it (@maestro will report back)." }), {
      userId: BOT_USER_ID,
      user: { id: BOT_USER_ID, name: "Maestro" },
    });
    const response = await run(post(signLinearDelivery(fixture)));
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("bot user");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  it("a mention on an active session still queues a turn when no bot user id is configured", async () => {
    const response = await run(
      post(signLinearDelivery(commentOn({ body: "@maestro one more thing: run the linter" }))),
      layerWithoutBotUserId,
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("TurnQueued");

    const session = Option.getOrThrow(await run(activeSession));
    expect(await run(runsOf(session))).toHaveLength(4);
  });

  it("a mention on a delegated but session-less issue starts a session with the comment as first-turn context", async () => {
    const response = await run(
      post(
        signLinearDelivery(
          commentOn({
            body: "@maestro please get on it",
            identifier: "FUR-90",
            issueId: ISSUE_DELEGATED_SESSIONLESS,
            title: "Polish the handbook",
          }),
        ),
      ),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("SessionStarted");

    const session = Option.getOrThrow(await run(activeSessionFor("FUR-90")));
    const runs = await run(runsOf(session));
    expect(runs).toHaveLength(1);

    // FIRST-TURN COMPOSITION (documented in LinearIngest): issue description
    // first, summoning comment appended under a divider.
    // biome-ignore lint/style/noNonNullAssertion: length asserted above
    const context = await run(contextOf(runs[0]!.id));
    expect(context.title).toBe("Polish the handbook");
    expect(context.body).toContain("Polish the operator handbook.");
    expect(context.body).toContain("@maestro please get on it");
    expect(context.body.indexOf("Polish the operator handbook.")).toBeLessThan(
      context.body.indexOf("@maestro please get on it"),
    );
  });

  it("a mention on a non-delegated session-less issue is ignored", async () => {
    const response = await run(
      post(
        signLinearDelivery(
          commentOn({
            body: "@maestro can you take this?",
            identifier: "FUR-91",
            issueId: ISSUE_NOT_DELEGATED,
          }),
        ),
      ),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("not delegated");
    expect(await run(activeSessionFor("FUR-91"))).toEqual(Option.none());
  });

  it("a session-less mention whose delegation lookup fails is ignored, not an error", async () => {
    const response = await run(
      post(
        signLinearDelivery(
          commentOn({
            body: "@maestro hello?",
            identifier: "FUR-92",
            issueId: ISSUE_LOOKUP_FAILS,
          }),
        ),
      ),
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("could not verify delegation");
    expect(await run(activeSessionFor("FUR-92"))).toEqual(Option.none());
  });

  it("a session-less mention with no bot user id configured is loudly ignored", async () => {
    const response = await run(
      post(
        signLinearDelivery(
          commentOn({
            body: "@maestro anyone home?",
            identifier: "FUR-93",
            issueId: "1efc90aa-0000-4000-8000-000000000093",
          }),
        ),
      ),
      layerWithoutBotUserId,
    );
    expect(response.status).toBe(200);
    expect(response.json.outcome).toBe("Ignored");
    expect(String(response.json.reason)).toContain("MAESTRO_LINEAR_BOT_USER_ID");
  });

  it("a paused session resumes via mention or re-delegation, never via inert events (FUR-39)", async () => {
    // Fresh ticket on the registered team; pause is set directly (the breaker
    // trip itself is executor territory — see the circuit-breaker suite).
    const TICKET_P = "FUR-88";
    const ISSUE_P = "1efc90aa-0000-4000-8000-000000000088";
    const started = await run(
      post(signLinearDelivery(delegationEvent({ identifier: TICKET_P, id: ISSUE_P }))),
    );
    expect(started.json.outcome).toBe("SessionStarted");

    const pauseIt = Effect.gen(function* () {
      const sessions = yield* SessionRepo;
      const session = Option.getOrThrow(
        yield* sessions.findActiveByTicket({ source: "linear", externalId: TICKET_P }),
      );
      return yield* sessions.pause(session.id);
    });
    const paused = await run(pauseIt);
    expect(paused.newlyPaused).toBe(true);
    const session = paused.session;

    // a plain human comment stays inert (it never even reaches the pipeline)
    const inert = await run(
      post(
        signLinearDelivery(
          commentOn({ body: "Any progress?", identifier: TICKET_P, issueId: ISSUE_P }),
        ),
      ),
    );
    expect(inert.status).toBe(200);
    expect(inert.json.outcome).toBe("Ignored");
    expect(await run(runsOf(session))).toHaveLength(1);

    // an issue update without a delegate change must not silently resume
    const unrelatedUpdate = {
      ...delegationEvent({ identifier: TICKET_P, id: ISSUE_P }),
      updatedFrom: { updatedAt: "2026-07-13T10:59:00.000Z", title: "Old title" },
    };
    const noResume = await run(post(signLinearDelivery(unrelatedUpdate)));
    expect(noResume.status).toBe(200);
    expect(noResume.json.outcome).toBe("Ignored");
    expect(await run(runsOf(session))).toHaveLength(1);

    // an explicit @maestro mention resumes the session and queues the turn
    const mentioned = await run(
      post(
        signLinearDelivery(
          commentOn({ body: "@maestro try again", identifier: TICKET_P, issueId: ISSUE_P }),
        ),
      ),
    );
    expect(mentioned.status).toBe(200);
    expect(mentioned.json.outcome).toBe("SessionResumed");

    const afterMention = await run(
      Effect.gen(function* () {
        const sessions = yield* SessionRepo;
        return yield* sessions.get(session.id);
      }),
    );
    expect(afterMention.pausedAt).toBeNull();
    const runsAfterMention = await run(runsOf(session));
    expect(runsAfterMention).toHaveLength(2);
    expect(runsAfterMention[1]?.state).toBe("PENDING");

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

    // ...and re-delegating the issue (un-delegate first — updatedFrom must
    // show the delegate changed) resumes a paused session too.
    const repaused = await run(pauseIt);
    expect(repaused.newlyPaused).toBe(true);
    const redelegated = await run(
      post(signLinearDelivery(delegationEvent({ identifier: TICKET_P, id: ISSUE_P }))),
    );
    expect(redelegated.status).toBe(200);
    expect(redelegated.json.outcome).toBe("SessionResumed");
    expect(await run(runsOf(session))).toHaveLength(3);
  });

  // Task-level `maestro:model=`/`maestro:effort=` labels (FUR-41) were removed
  // as YAGNI — but issues in the wild may still carry them, so they must stay
  // inert: ordinary labels that neither trigger, fail, nor configure anything.
  it("leftover agent-override-shaped labels on a delegated issue are inert ordinary labels", async () => {
    const TICKET_M = "FUR-95";
    const label = (name: string, n: number) => ({
      id: `cd1e7f3a-2b8c-4a9d-b5e6-0f4a7c1d8e${n}0`,
      color: "#5e6ad2",
      name,
      parentId: null,
    });
    const fixture = delegationEvent({
      identifier: TICKET_M,
      id: "1efc90aa-0000-4000-8000-000000000095",
      labels: [
        label("maestro", 1),
        label("maestro:model=claude-sonnet-4-5", 2),
        label("maestro:effort=turbo", 3),
      ],
    });
    const started = await run(post(signLinearDelivery(fixture)));
    expect(started.status).toBe(200);
    expect(started.json.outcome).toBe("SessionStarted");

    const session = Option.getOrThrow(await run(activeSessionFor(TICKET_M)));
    // nothing was parsed off the labels onto the session
    expect(session.agentModel).toBeNull();
    expect(session.agentEffort).toBeNull();
    expect(await run(runsOf(session))).toHaveLength(1);
  });

  it("issue moved to done terminates the session and cancels its queued turns", async () => {
    // FUR-19: the terminal signal is acted on, so capture the session first.
    const session = Option.getOrThrow(await run(activeSession));

    const done = withData(loadLinearFixture("issue-completed"), { identifier: TICKET });
    const response = await run(post(signLinearDelivery(done)));
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
    expect(runs).toHaveLength(4);
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
    const done = withData(loadLinearFixture("issue-completed"), { identifier: TICKET });
    const response = await run(post(signLinearDelivery(done)));
    expect(response.status).toBe(200);
    // no active session anymore — the signal has nothing to act on
    expect(response.json.outcome).toBe("Ignored");
    expect(await run(activeSession)).toEqual(Option.none());
  });

  it("issue moved to canceled terminates its session too", async () => {
    // fresh session on a different ticket of the registered team
    const TICKET_2 = "FUR-77";
    const started = await run(
      post(
        signLinearDelivery(
          delegationEvent({ identifier: TICKET_2, id: "1efc90aa-0000-4000-8000-000000000077" }),
        ),
      ),
    );
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

    const terminated = await run(activeSessionFor(TICKET_2));
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
