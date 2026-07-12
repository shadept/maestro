import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Db } from "../../src/db/Db.ts";
import { HealthRoutes } from "../../src/http/health.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

let testDb: TestDb;

beforeAll(async () => {
  testDb = await startTestDb();
});

afterAll(async () => {
  await testDb.stop();
});

// Boots the real HTTP server on an ephemeral port with the given Db layer and
// runs `f` with a client pointed at it.
const withServer = <A>(
  dbLayer: Layer.Layer<Db>,
  f: (client: HttpClient.HttpClient) => Effect.Effect<A, unknown, HttpClient.HttpClient>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      return yield* f(client);
    }).pipe(
      Effect.provide(
        HttpRouter.serve(HealthRoutes, { disableLogger: true, disableListenLog: true }).pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
          Layer.provide(dbLayer),
        ),
      ),
    ),
  );

const unreachableDb = Db.layerTest("postgresql://nobody:wrong@127.0.0.1:9/nothing");

describe("health endpoints", () => {
  it("GET /livez returns 200 with a reachable database", async () => {
    const status = await withServer(Db.layerTest(testDb.connectionString), (client) =>
      client.get("/livez").pipe(Effect.map((res) => res.status)),
    );
    expect(status).toBe(200);
  });

  it("GET /readyz returns 200 with a reachable database", async () => {
    const status = await withServer(Db.layerTest(testDb.connectionString), (client) =>
      client.get("/readyz").pipe(Effect.map((res) => res.status)),
    );
    expect(status).toBe(200);
  });

  it("GET /livez returns 200 even when the database is unreachable", async () => {
    const status = await withServer(unreachableDb, (client) =>
      client.get("/livez").pipe(Effect.map((res) => res.status)),
    );
    expect(status).toBe(200);
  });

  it("GET /readyz returns 503 when the database is unreachable", async () => {
    const status = await withServer(unreachableDb, (client) =>
      client.get("/readyz").pipe(Effect.map((res) => res.status)),
    );
    expect(status).toBe(503);
  });
});
