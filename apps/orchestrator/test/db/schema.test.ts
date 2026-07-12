import { Effect } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { Db } from "../../src/db/Db.ts";
import {
  auditLogs,
  outbox,
  projects,
  sessions,
  taskRuns,
  webhookDeliveries,
} from "../../src/db/schema/index.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

let testDb: TestDb;

beforeAll(async () => {
  testDb = await startTestDb();
});

afterAll(async () => {
  await testDb.stop();
});

describe("schema", () => {
  it("inserts and selects a row in every table", async () => {
    const db = testDb.db;

    const [project] = await db
      .insert(projects)
      .values({ repoGitUrl: "https://github.com/shadept/maestro" })
      .returning();
    expect(project?.id).toBeTruthy();
    expect(project?.gitConventions).toEqual({});

    const [session] = await db
      .insert(sessions)
      .values({
        // biome-ignore lint/style/noNonNullAssertion: asserted above
        projectId: project!.id,
        ticketSource: "linear",
        ticketExternalId: "FUR-7",
        gitBranch: "maestro/FUR-7",
        state: "WARM_IDLE",
      })
      .returning();
    expect(session?.state).toBe("WARM_IDLE");

    const [run] = await db
      .insert(taskRuns)
      // biome-ignore lint/style/noNonNullAssertion: asserted above
      .values({ sessionId: session!.id, state: "PENDING" })
      .returning();
    expect(run?.cause).toBeNull();

    const [audit] = await db
      .insert(auditLogs)
      .values({
        actor: "admin",
        action: "test-insert",
        // biome-ignore lint/style/noNonNullAssertion: asserted above
        targetEntity: `task-run:${run!.id}`,
      })
      .returning();
    expect(audit?.priorState).toBeNull();

    const [delivery] = await db
      .insert(webhookDeliveries)
      .values({
        source: "linear",
        deliveryId: "delivery-1",
        payload: { action: "update", nested: [1, 2, 3] },
      })
      .returning();
    expect(delivery?.payload).toEqual({ action: "update", nested: [1, 2, 3] });

    const [entry] = await db
      .insert(outbox)
      .values({
        // biome-ignore lint/style/noNonNullAssertion: asserted above
        taskRunId: run!.id,
        target: "linear",
        payload: { body: "done" },
        idempotencyKey: "task-run-1:result",
      })
      .returning();
    expect(entry?.status).toBe("PENDING");
    expect(entry?.attempts).toBe(0);

    const selected = await db.select().from(sessions);
    expect(selected).toHaveLength(1);
  });

  it("rejects duplicate webhook deliveries per source", async () => {
    const db = testDb.db;
    await db
      .insert(webhookDeliveries)
      .values({ source: "linear", deliveryId: "dup-1", payload: {} });
    const failure = await db
      .insert(webhookDeliveries)
      .values({ source: "linear", deliveryId: "dup-1", payload: {} })
      .then(
        () => null,
        (e: unknown) => e as Error & { cause?: { code?: string } },
      );
    // 23505 = Postgres unique_violation; drizzle wraps the pg error as `cause`
    expect(failure?.cause?.code).toBe("23505");
    // same delivery id from a different source is a different delivery
    await db
      .insert(webhookDeliveries)
      .values({ source: "generic", deliveryId: "dup-1", payload: {} });
  });

  it("provides the Drizzle client through the Db service test layer", async () => {
    const rows = await Effect.runPromise(
      Effect.gen(function* () {
        const { client } = yield* Db;
        return yield* Effect.promise(() => client.select().from(projects));
      }).pipe(Effect.provide(Db.layerTest(testDb.connectionString))),
    );
    expect(rows.length).toBeGreaterThan(0);
  });
});
