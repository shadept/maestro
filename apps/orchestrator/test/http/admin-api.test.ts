import { NodeHttpServer } from "@effect/platform-node";
import { SessionWorkspace } from "@maestro/api";
import { Session, TaskContext, TaskRun } from "@maestro/domain";
import { Effect, Layer, Schema, type Scope } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { Db } from "../../src/db/Db.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { AdminApiRoutes } from "../../src/http/admin.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// Admin read API (FUR-16): the orchestrator implements the @maestro/api
// contract; these tests exercise it over a real HTTP server + real Postgres.

const TOKEN = "test-admin-token";
const auth = { headers: { authorization: `Bearer ${TOKEN}` } };
const UNKNOWN_ID = "0198ffff-0000-7000-8000-00000000dead";

type Services = ProjectRepo | SessionRepo | TaskRunRepo | HttpClient.HttpClient;

let testDb: TestDb;
let layer: Layer.Layer<Services>;

beforeAll(async () => {
  testDb = await startTestDb();
  const services = Layer.mergeAll(ProjectRepo.layer, SessionRepo.layer, TaskRunRepo.layer).pipe(
    Layer.provide(EventBus.layer),
    Layer.provide(Db.layerTest(testDb.connectionString)),
  );
  layer = HttpRouter.serve(AdminApiRoutes, { disableLogger: true, disableListenLog: true }).pipe(
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

// The wire is the contract's JSON encoding; decode with the same schemas the
// admin UI client derives from.
const decodeSessions = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(Schema.Array(Session))),
);
const decodeSession = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.toCodecJson(Session)));
const decodeTaskRuns = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(Schema.Array(TaskRun))),
);
const decodeWorkspace = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(SessionWorkspace)),
);
const decodeContext = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(TaskContext)),
);

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
      agentModel: null,
      agentEffort: null,
      deliveryId: `d-${externalId}`,
      payload: {},
    });
    return { session, taskRun };
  });

describe("admin read API", () => {
  it("rejects requests without or with a wrong admin token", () =>
    run(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const missing = yield* client.get("/api/sessions");
        expect(missing.status).toBe(401);
        const wrong = yield* client.get("/api/sessions", {
          headers: { authorization: "Bearer wrong" },
        });
        expect(wrong.status).toBe(401);
      }),
    ));

  it("lists sessions, session detail, task runs, and logs", () =>
    run(
      Effect.gen(function* () {
        const { session, taskRun } = yield* setup("FUR-16C");
        const taskRunRepo = yield* TaskRunRepo;
        yield* taskRunRepo.appendLogs(taskRun.id, '{"type":"system"}\n');
        yield* taskRunRepo.appendLogs(taskRun.id, '{"type":"result"}\n');
        const client = yield* HttpClient.HttpClient;

        const listResponse = yield* client.get("/api/sessions", auth);
        expect(listResponse.status).toBe(200);
        const sessions = decodeSessions(yield* listResponse.text);
        expect(sessions.map((s) => s.id)).toContain(session.id);

        const detailResponse = yield* client.get(`/api/sessions/${session.id}`, auth);
        expect(detailResponse.status).toBe(200);
        expect(decodeSession(yield* detailResponse.text)).toEqual(session);

        const runsResponse = yield* client.get(`/api/sessions/${session.id}/runs`, auth);
        expect(runsResponse.status).toBe(200);
        const runs = decodeTaskRuns(yield* runsResponse.text);
        expect(runs).toHaveLength(1);
        expect(runs[0]?.id).toBe(taskRun.id);

        const logsResponse = yield* client.get(`/api/runs/${taskRun.id}/logs`, auth);
        expect(logsResponse.status).toBe(200);
        expect(JSON.parse(yield* logsResponse.text)).toBe('{"type":"system"}\n{"type":"result"}\n');
      }),
    ));

  it("serves the session workspace path and the run's inbound context", () =>
    run(
      Effect.gen(function* () {
        const { session, taskRun } = yield* setup("FUR-17A");
        const client = yield* HttpClient.HttpClient;

        const workspaceResponse = yield* client.get(`/api/sessions/${session.id}/workspace`, auth);
        expect(workspaceResponse.status).toBe(200);
        const workspace = decodeWorkspace(yield* workspaceResponse.text);
        // AppConfig.layerTest storageRoot + the storage layout convention.
        expect(workspace.worktreePath).toBe(`/tmp/maestro-test/worktrees/${session.id}`);

        const contextResponse = yield* client.get(`/api/runs/${taskRun.id}/context`, auth);
        expect(contextResponse.status).toBe(200);
        const context = decodeContext(yield* contextResponse.text);
        expect(context.ticket.externalId).toBe("FUR-17A");
        expect(context.body).toBe("do the thing");
      }),
    ));

  it("404s unknown sessions and runs", () =>
    run(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        expect((yield* client.get(`/api/sessions/${UNKNOWN_ID}`, auth)).status).toBe(404);
        expect((yield* client.get(`/api/sessions/${UNKNOWN_ID}/runs`, auth)).status).toBe(404);
        expect((yield* client.get(`/api/sessions/${UNKNOWN_ID}/workspace`, auth)).status).toBe(404);
        expect((yield* client.get(`/api/runs/${UNKNOWN_ID}/logs`, auth)).status).toBe(404);
        expect((yield* client.get(`/api/runs/${UNKNOWN_ID}/context`, auth)).status).toBe(404);
      }),
    ));
});
