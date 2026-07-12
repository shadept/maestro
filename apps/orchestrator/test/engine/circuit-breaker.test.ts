import { execFileSync } from "node:child_process";
import { mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { NodeHttpServer } from "@effect/platform-node";
import type { SessionId } from "@maestro/domain";
import { Effect, Layer, Option, Redacted, Schema, type Scope } from "effect";
import { HttpBody, HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { AgentContract } from "../../src/agent/AgentContract.ts";
import { MAESTRO_COMMENT_MARKER } from "../../src/callback/format.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { AuditRepo } from "../../src/db/AuditRepo.ts";
import { Db } from "../../src/db/Db.ts";
import { DeliveryRepo } from "../../src/db/DeliveryRepo.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { SessionTerminator } from "../../src/engine/SessionTerminator.ts";
import {
  CONSECUTIVE_FAILURE_LIMIT,
  TurnExecutor,
  TurnOutcomePayload,
} from "../../src/engine/TurnExecutor.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { GitHubForge } from "../../src/forge/GitHubForge.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { OutboundGit } from "../../src/git/OutboundGit.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { WebhookRoutes } from "../../src/http/webhooks.ts";
import { IngestPipeline } from "../../src/ingest/IngestPipeline.ts";
import { LinearIngest } from "../../src/ingest/LinearIngest.ts";
import { TurnQueue } from "../../src/queue/TurnQueue.ts";
import { WorkerRuntime } from "../../src/runtime/WorkerRuntime.ts";
import {
  buildFakeAgentImage,
  cleanStorageViaContainer,
  FAKE_AGENT_IMAGE,
  fakeAgentRuntimeTemplate,
} from "../support/fake-agent.ts";
import { LINEAR_TEST_SECRET, loadLinearFixture, signLinearDelivery } from "../support/linear.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// FUR-39 layers 2+3, end to end on the REAL path: webhook HTTP → LinearIngest
// → IngestPipeline → pg-boss queue → TurnExecutor → fake agent (MODE=FAIL) →
// circuit breaker. No live API calls anywhere. This is the acceptance walk:
// N instantly-failing turns trip the breaker (paused outbox message + audit
// entry, failure comments deduped), a genuine human comment is then Ignored
// even with NO bot user id configured, and re-applying the trigger label
// resumes the session and queues a turn again.

const TICKET = "FUR-300";
const ISSUE_UUID = "9a3b5f80-1e2a-4b0e-9f3d-2c7a8f1e6b01";

type Services =
  | TurnExecutor
  | TurnQueue
  | IngestPipeline
  | ProjectRepo
  | SessionRepo
  | TaskRunRepo
  | OutboxRepo
  | AuditRepo
  | EventBus
  | HttpClient.HttpClient;

let testDb: TestDb;
let root: string;
let storageRoot: string;
let originDir: string;
let layer: Layer.Layer<Services>;

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

const run = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layer)));

const decodeOutcome = Schema.decodeUnknownSync(TurnOutcomePayload);

/** Polls `read` until `until` holds; dies past the deadline. */
const waitFor = <A, E>(
  read: Effect.Effect<A, E, Services>,
  until: (value: A) => boolean,
  what: string,
  deadlineMillis = 60_000,
) =>
  Effect.gen(function* () {
    const deadline = Date.now() + deadlineMillis;
    while (true) {
      const value = yield* read;
      if (until(value)) return value;
      if (Date.now() > deadline) return yield* Effect.die(new Error(`timed out: ${what}`));
      yield* Effect.sleep(100);
    }
  });

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

const withData = (
  fixture: Record<string, unknown>,
  patch: Record<string, unknown>,
): Record<string, unknown> => ({
  ...fixture,
  data: { ...(fixture.data as Record<string, unknown>), ...patch },
});

/** The label-trigger event for our ticket; description drives the fake agent into failure. */
const labelEvent = () =>
  withData(loadLinearFixture("issue-labeled"), { identifier: TICKET, description: "MODE=FAIL" });

/** A human comment on our ticket. */
const commentEvent = (body: string) =>
  withData(loadLinearFixture("comment-created"), {
    body,
    issue: { id: ISSUE_UUID, identifier: TICKET, title: "Fix the flux capacitor" },
  });

const sessionRuns = (sessionId: SessionId) =>
  Effect.gen(function* () {
    const taskRunRepo = yield* TaskRunRepo;
    return yield* taskRunRepo.listBySession(sessionId);
  });

const getSession = (sessionId: SessionId) =>
  Effect.gen(function* () {
    const sessionRepo = yield* SessionRepo;
    return yield* sessionRepo.get(sessionId);
  });

/** All outbox payloads for the session, decoded, grouped by kind. */
const outboxByKind = (sessionId: SessionId) =>
  Effect.gen(function* () {
    const outboxRepo = yield* OutboxRepo;
    const pending = yield* outboxRepo.listPending(100);
    const payloads = pending
      .map((entry) => decodeOutcome(entry.payload))
      .filter((payload) => payload.sessionId === sessionId);
    return {
      failed: payloads.filter((p) => p.kind === "turn-failed"),
      paused: payloads.filter((p) => p.kind === "session-paused"),
    };
  });

beforeAll(async () => {
  testDb = await startTestDb();
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-breaker-")));
  storageRoot = path.join(root, "storage");

  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");

  buildFakeAgentImage();

  const repos = Layer.mergeAll(
    ProjectRepo.layer,
    SessionRepo.layer,
    TaskRunRepo.layer,
    OutboxRepo.layer,
    AuditRepo.layer,
    DeliveryRepo.layer,
  );
  const gitLayer = Layer.mergeAll(GitCache.layer, WorktreeManager.layer, OutboundGit.layer).pipe(
    Layer.provideMerge(GitCache.layer),
    Layer.provide(Layer.mergeAll(RepoLocks.layer, GitHubForge.layerTest({}))),
  );
  const terminator = SessionTerminator.layer.pipe(Layer.provide(gitLayer));
  const executor = TurnExecutor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(AgentContract.layer, WorkerRuntime.layerLocalCli, gitLayer, terminator),
    ),
  );
  const ingest = LinearIngest.layer.pipe(
    Layer.provideMerge(IngestPipeline.layer),
    Layer.provide(terminator),
  );
  const http = HttpRouter.serve(WebhookRoutes, {
    disableLogger: true,
    disableListenLog: true,
  }).pipe(Layer.provideMerge(NodeHttpServer.layerTest), Layer.provideMerge(ingest));

  layer = Layer.mergeAll(http, executor).pipe(
    Layer.provideMerge(TurnQueue.layer),
    Layer.provideMerge(repos),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.provideMerge(EventBus.layer),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: testDb.connectionString,
        storageRoot,
        workerImage: FAKE_AGENT_IMAGE,
        runtimeTemplate: fakeAgentRuntimeTemplate(),
        turnTimeoutSeconds: 120,
        maxConcurrentWorkers: 2,
        linearWebhookSecret: Option.some(Redacted.make(LINEAR_TEST_SECRET)),
        // ACCEPTANCE: no bot user id configured — the breaker alone must cap
        // a failing session at CONSECUTIVE_FAILURE_LIMIT failed turns.
        linearBotUserId: Option.none(),
      }),
    ),
    Layer.orDie,
  );
}, 180_000);

afterAll(async () => {
  cleanStorageViaContainer(root, storageRoot);
  await rm(root, { recursive: true, force: true });
  await testDb.stop();
});

describe("failure circuit breaker (FUR-39)", () => {
  it("N failures pause the session; comments are ignored until the label resumes it", async () => {
    await run(
      Effect.gen(function* () {
        const projectRepo = yield* ProjectRepo;
        yield* projectRepo.create({
          repoGitUrl: `file://${originDir}`,
          linearTeamKey: "FUR",
        });

        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        yield* queue.work(executor.execute);

        // ── trip: N instantly-failing turns through the real path ─────────
        const started = yield* post(signLinearDelivery(labelEvent()));
        expect(started.status).toBe(200);
        expect(started.json.outcome).toBe("SessionStarted");

        const sessionRepo = yield* SessionRepo;
        const session = Option.getOrThrow(
          yield* sessionRepo.findActiveByTicket({ source: "linear", externalId: TICKET }),
        );

        const settledFailures = (count: number) =>
          waitFor(
            sessionRuns(session.id),
            (runs) => runs.length === count && runs.every((taskRun) => taskRun.state === "FAILED"),
            `${count} turns settle FAILED`,
          );

        yield* settledFailures(1);
        expect((yield* getSession(session.id)).pausedAt).toBeNull();

        for (let turn = 2; turn <= CONSECUTIVE_FAILURE_LIMIT; turn += 1) {
          const queued = yield* post(
            signLinearDelivery(commentEvent(`Try again please. MODE=FAIL (${turn})`)),
          );
          expect(queued.json.outcome).toBe("TurnQueued");
          yield* settledFailures(turn);
        }

        // crossing the threshold pauses the session...
        const paused = yield* waitFor(
          getSession(session.id),
          (s) => s.pausedAt !== null,
          "breaker pauses the session",
          15_000,
        );
        expect(paused.pausedAt).toBeInstanceOf(Date);

        // ...with an audit entry and EXACTLY ONE distinct outbox message,
        // while the N identical failure comments deduped into one row.
        // (Polled: the audit + outbox writes land right AFTER the pause
        // marker the waitFor above observed.)
        const outbox = yield* waitFor(
          outboxByKind(session.id),
          (o) => o.paused.length >= 1,
          "paused outbox message enqueued",
          15_000,
        );
        expect(outbox.paused).toHaveLength(1);
        expect(outbox.paused[0]?.summary).toContain(
          `paused this session after ${CONSECUTIVE_FAILURE_LIMIT} consecutive failures`,
        );
        expect(outbox.failed).toHaveLength(1); // 3 failures, identical text, one comment
        expect(outbox.failed[0]?.summary).toBe("fake agent exploded");

        const auditRepo = yield* AuditRepo;
        expect(
          (yield* auditRepo.list).filter(
            (a) => a.action === "session-paused" && a.targetEntity === `session:${session.id}`,
          ),
        ).toHaveLength(1);

        // ── paused: nothing auto-triggers turns any more ───────────────────
        // Maestro's own comment echo (layer 1 guard — marker, any author):
        const echo = yield* post(
          signLinearDelivery(commentEvent(`${MAESTRO_COMMENT_MARKER} turn failed (ERROR).`)),
        );
        expect(echo.json.outcome).toBe("Ignored");
        // and a GENUINE human comment, with no bot user id configured:
        const human = yield* post(signLinearDelivery(commentEvent("Are you stuck?")));
        expect(human.json.outcome).toBe("Ignored");
        expect(String(human.json.reason)).toContain("paused");
        expect(yield* sessionRuns(session.id)).toHaveLength(CONSECUTIVE_FAILURE_LIMIT);

        // ── manual resume: the human re-applies the trigger label ─────────
        const resumed = yield* post(signLinearDelivery(labelEvent()));
        expect(resumed.json.outcome).toBe("SessionResumed");
        expect((yield* getSession(session.id)).pausedAt).toBeNull();

        // the resume queued a fresh turn — the session accepts work again
        yield* waitFor(
          sessionRuns(session.id),
          (all) => all.length === CONSECUTIVE_FAILURE_LIMIT + 1,
          "resume queues a turn",
          15_000,
        );
        expect(
          (yield* auditRepo.list).some(
            (a) => a.action === "session-resumed" && a.targetEntity === `session:${session.id}`,
          ),
        ).toBe(true);

        // ── still broken: ONE further failure re-trips immediately ────────
        // (the streak derives from settled runs; a resume without a success
        // in between leaves the session suspect — documented in TurnExecutor)
        yield* waitFor(
          sessionRuns(session.id),
          (all) => all.every((taskRun) => taskRun.state === "FAILED"),
          "resumed turn fails too",
        );
        yield* waitFor(
          getSession(session.id),
          (s) => s.pausedAt !== null,
          "breaker re-trips after the post-resume failure",
          15_000,
        );

        // a SECOND distinct paused message (new trip, keyed by the new turn),
        // while the repeated identical failure text stays deduped
        const finalOutbox = yield* waitFor(
          outboxByKind(session.id),
          (o) => o.paused.length >= 2,
          "second paused outbox message enqueued",
          15_000,
        );
        expect(finalOutbox.paused).toHaveLength(2);
        expect(finalOutbox.failed).toHaveLength(1);
      }),
    );
  }, 300_000);
});
