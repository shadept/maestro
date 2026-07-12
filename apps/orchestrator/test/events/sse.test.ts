import { NodeHttpServer } from "@effect/platform-node";
import {
  LogChunk,
  type MaestroEvent,
  MaestroEventFromJsonString,
  type SessionStateChanged,
  type SystemStatus,
  type TaskRunStateChanged,
} from "@maestro/api";
import { Effect, Layer, Queue, Schema, type Scope, Stream } from "effect";
import { Sse } from "effect/unstable/encoding";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { Db } from "../../src/db/Db.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { EventsRoutes } from "../../src/http/events.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// SSE pipeline integration test (FUR-16 acceptance): a real HTTP server and
// real Postgres. The "fake turn" is a synthetic executor walk — the repo CAS
// transitions ARE the product publish path; LogChunk publishes simulate the
// executor's log tee (the tee itself is exercised by the executor suite).

const TOKEN = "test-admin-token";

type Services = ProjectRepo | SessionRepo | TaskRunRepo | EventBus | HttpClient.HttpClient;

let testDb: TestDb;
let layer: Layer.Layer<Services>;

beforeAll(async () => {
  testDb = await startTestDb();
  const services = Layer.mergeAll(ProjectRepo.layer, SessionRepo.layer, TaskRunRepo.layer).pipe(
    Layer.provideMerge(EventBus.layer),
    Layer.provide(Db.layerTest(testDb.connectionString)),
  );
  layer = HttpRouter.serve(EventsRoutes, { disableLogger: true, disableListenLog: true }).pipe(
    Layer.provideMerge(NodeHttpServer.layerTest),
    Layer.provideMerge(services),
    Layer.provide(AppConfig.layerTest({ databaseUrl: testDb.connectionString })),
    Layer.orDie,
  );
});

afterAll(async () => {
  await testDb.stop();
});

const run = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layer)));

const decodeEvent = Schema.decodeUnknownSync(MaestroEventFromJsonString);

/** Creates project + session + PENDING run; consumes the resulting bus events. */
const setup = (externalId: string) =>
  Effect.gen(function* () {
    const projectRepo = yield* ProjectRepo;
    const sessionRepo = yield* SessionRepo;
    const taskRunRepo = yield* TaskRunRepo;
    const project = yield* projectRepo.create({ repoGitUrl: "https://github.com/acme/repo.git" });
    const session = yield* sessionRepo.create({
      projectId: project.id,
      ticketReference: { source: "linear", externalId },
      gitBranch: `maestro/${externalId.toLowerCase()}`,
    });
    const taskRun = yield* taskRunRepo.create(session.id, {
      source: "linear",
      ticket: { source: "linear", externalId },
      actor: "shade",
      title: `Ticket ${externalId}`,
      body: "do the thing",
      deliveryId: `d-${externalId}`,
      payload: {},
    });
    return { session, taskRun };
  });

/**
 * Connects to /api/events and pumps decoded MaestroEvents into a queue.
 * Returns a `next` effect that takes one event (bounded by a timeout so a
 * missing event fails the test instead of hanging it).
 */
const connect = (path: string, headers?: Record<string, string>) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.get(path, headers ? { headers } : {});
    expect(response.status).toBe(200);
    expect(response.headers["content-type"]).toBe("text/event-stream");
    const queue = yield* Queue.unbounded<MaestroEvent>();
    yield* Effect.forkScoped(
      response.stream.pipe(
        Stream.decodeText(),
        Stream.pipeThroughChannel(Sse.decode()),
        Stream.map((event) => decodeEvent(event.data)),
        Stream.runForEach((event) => Queue.offer(queue, event)),
      ),
    );
    const next = Queue.take(queue).pipe(
      Effect.timeoutOrElse({
        duration: "10 seconds",
        orElse: () => Effect.die(new Error("timed out waiting for an SSE event")),
      }),
    );
    return { next };
  });

describe("GET /api/events (SSE)", () => {
  it("rejects requests without or with a wrong admin token", () =>
    run(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const missing = yield* client.get("/api/events");
        expect(missing.status).toBe(401);
        const wrongQuery = yield* client.get("/api/events?token=wrong");
        expect(wrongQuery.status).toBe(401);
        const wrongBearer = yield* client.get("/api/events", {
          headers: { authorization: "Bearer wrong" },
        });
        expect(wrongBearer.status).toBe(401);
        // Bearer-header auth works too (and the stream is consumed so the
        // server's graceful shutdown isn't held open by an idle handler).
        const { next } = yield* connect("/api/events", {
          authorization: `Bearer ${TOKEN}`,
        });
        expect((yield* next)._tag).toBe("SystemStatus");
      }),
    ));

  it("rejects a malformed ?session filter and 404s an unknown one", () =>
    run(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const malformed = yield* client.get(`/api/events?token=${TOKEN}&session=not-a-uuid`);
        expect(malformed.status).toBe(400);
        const unknown = yield* client.get(
          `/api/events?token=${TOKEN}&session=0198ffff-0000-7000-8000-00000000dead`,
        );
        expect(unknown.status).toBe(404);
      }),
    ));

  it("streams the snapshot, then the live turn walk with ordered log chunks", () =>
    run(
      Effect.gen(function* () {
        const { session, taskRun } = yield* setup("FUR-16");
        const taskRunRepo = yield* TaskRunRepo;
        const bus = yield* EventBus;

        const { next } = yield* connect(`/api/events?token=${TOKEN}`);

        // Snapshot: SystemStatus first, then sessions, then active runs.
        const status = (yield* next) as SystemStatus;
        expect(status._tag).toBe("SystemStatus");
        expect(status.activeTurns).toBe(1);
        expect(status.dbReachable).toBe(true);

        const snapshotSession = (yield* next) as SessionStateChanged;
        expect(snapshotSession._tag).toBe("SessionStateChanged");
        expect(snapshotSession.session.id).toBe(session.id);
        expect(snapshotSession.session.state).toBe("WARM_IDLE");

        const snapshotRun = (yield* next) as TaskRunStateChanged;
        expect(snapshotRun._tag).toBe("TaskRunStateChanged");
        expect(snapshotRun.taskRun.id).toBe(taskRun.id);
        expect(snapshotRun.taskRun.state).toBe("PENDING");

        // Fake turn: walk the run through the repo CAS transitions and
        // publish log chunks the way the executor tee does.
        const chunks = ['{"type":"system"}\n', '{"type":"assistant"}\n', '{"type":"result"}\n'];
        yield* taskRunRepo.transition(taskRun.id, "PROVISIONING");
        yield* taskRunRepo.transition(taskRun.id, "EXECUTING");
        for (const chunk of chunks) {
          yield* taskRunRepo.appendLogs(taskRun.id, chunk);
          yield* bus.publish(
            LogChunk.make({ taskRunId: taskRun.id, sessionId: session.id, chunk }),
          );
        }
        yield* taskRunRepo.transition(taskRun.id, "COMPLETED", { resultText: "done" });

        const walk: Array<MaestroEvent> = [];
        for (let i = 0; i < 6; i++) walk.push(yield* next);

        expect(
          walk.map((event) =>
            event._tag === "TaskRunStateChanged"
              ? event.taskRun.state
              : event._tag === "LogChunk"
                ? `log:${event.chunk}`
                : event._tag,
          ),
        ).toEqual([
          "PROVISIONING",
          "EXECUTING",
          ...chunks.map((chunk) => `log:${chunk}`),
          "COMPLETED",
        ]);
        const completed = walk[5] as TaskRunStateChanged;
        expect(completed.taskRun.resultText).toBe("done");
      }),
    ));

  it("?session=<id> delivers only that session's events (plus system-wide ones)", () =>
    run(
      Effect.gen(function* () {
        const a = yield* setup("FUR-16A");
        const b = yield* setup("FUR-16B");
        const taskRunRepo = yield* TaskRunRepo;
        const sessionRepo = yield* SessionRepo;

        const { next } = yield* connect(`/api/events?token=${TOKEN}&session=${a.session.id}`);

        // Filtered snapshot: SystemStatus + session A + A's pending run only.
        const status = yield* next;
        expect(status._tag).toBe("SystemStatus");
        const snapshotSession = (yield* next) as SessionStateChanged;
        expect(snapshotSession.session.id).toBe(a.session.id);
        const snapshotRun = (yield* next) as TaskRunStateChanged;
        expect(snapshotRun.taskRun.sessionId).toBe(a.session.id);

        // Session B's activity must not reach this subscriber; session A's
        // (published after B's) must be the very next event.
        yield* taskRunRepo.transition(b.taskRun.id, "PROVISIONING");
        yield* sessionRepo.transition(b.session.id, "TERMINATED");
        yield* taskRunRepo.transition(a.taskRun.id, "PROVISIONING");

        const live = (yield* next) as TaskRunStateChanged;
        expect(live._tag).toBe("TaskRunStateChanged");
        expect(live.taskRun.id).toBe(a.taskRun.id);
        expect(live.taskRun.state).toBe("PROVISIONING");
      }),
    ));
});
