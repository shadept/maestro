import { randomUUID } from "node:crypto";
import { CallbackDeliveryError, type CallbackError } from "@maestro/domain";
import { eq } from "drizzle-orm";
import { Effect, Layer } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { CallbackWorker, MAX_DELIVERY_ATTEMPTS } from "../../src/callback/CallbackWorker.ts";
import { LinearCallback, type LinearCommentCall } from "../../src/callback/LinearCallback.ts";
import { Db } from "../../src/db/Db.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { outbox } from "../../src/db/schema/index.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import type { TurnOutcomePayload } from "../../src/engine/TurnExecutor.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// FUR-18: the callback worker drains the outbox into Linear comments — real
// Postgres, fake LinearCallback (observable calls + injectable failure).

const ISSUE_ID = "9a3b5f80-1e2a-4b0e-9f3d-2c7a8f1e6b01";

type Services = CallbackWorker | OutboxRepo | ProjectRepo | SessionRepo | TaskRunRepo;

let testDb: TestDb;

beforeAll(async () => {
  testDb = await startTestDb();
});

afterAll(async () => {
  await testDb.stop();
});

interface Fake {
  readonly calls: Array<LinearCommentCall>;
  readonly failWith: { current: CallbackError | undefined };
}

const makeFake = (): Fake => ({ calls: [], failWith: { current: undefined } });

const makeLayer = (fake: Fake): Layer.Layer<Services> =>
  CallbackWorker.layer.pipe(
    Layer.provide(LinearCallback.layerTest(fake)),
    Layer.provideMerge(
      Layer.mergeAll(OutboxRepo.layer, ProjectRepo.layer, SessionRepo.layer, TaskRunRepo.layer),
    ),
    Layer.provide(EventBus.layer),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.orDie,
  );

const run = <A, E>(fake: Fake, effect: Effect.Effect<A, E, Services>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, makeLayer(fake)));

/** Project → session → run whose context payload carries the Linear issue id. */
const setup = (overrides: Partial<TurnOutcomePayload> = {}) =>
  Effect.gen(function* () {
    const projects = yield* ProjectRepo;
    const sessions = yield* SessionRepo;
    const taskRuns = yield* TaskRunRepo;
    const outboxRepo = yield* OutboxRepo;

    const externalId = `FUR-${Math.floor(Math.random() * 1e9)}`;
    const project = yield* projects.create({ repoGitUrl: "https://github.com/acme/flux.git" });
    const session = yield* sessions.create({
      projectId: project.id,
      ticketReference: { source: "linear", externalId },
      gitBranch: `maestro/${externalId}`,
    });
    const taskRun = yield* taskRuns.create(session.id, {
      source: "linear",
      ticket: session.ticketReference,
      actor: "João Furtado",
      title: null,
      body: "Also update the operator handbook, please.",
      deliveryId: randomUUID(),
      // the raw Linear webhook payload, preserved for exactly this consumer
      payload: { type: "Comment", action: "create", data: { id: randomUUID(), issueId: ISSUE_ID } },
    });

    const outcome: TurnOutcomePayload = {
      kind: "turn-completed",
      taskRunId: taskRun.id,
      sessionId: session.id,
      ticket: session.ticketReference,
      summary: "Handbook updated; 3 files changed.",
      cause: null,
      pr: { number: 7, url: "https://github.test/acme/flux/pull/7" },
      ...overrides,
    };
    const entry = yield* outboxRepo.enqueue({
      taskRunId: taskRun.id,
      target: "linear",
      payload: outcome,
      idempotencyKey: `turn-result:${taskRun.id}`,
    });
    return { entry, outcome };
  });

const outboxRow = async (id: string) => {
  const rows = await testDb.db.select().from(outbox).where(eq(outbox.id, id));
  // biome-ignore lint/style/noNonNullAssertion: the row was just created by the test
  return rows[0]!;
};

const drain = (fake: Fake) =>
  run(
    fake,
    Effect.gen(function* () {
      const worker = yield* CallbackWorker;
      return yield* worker.drainOnce();
    }),
  );

describe("CallbackWorker", () => {
  it("posts a turn result as a single Linear comment — draining again never duplicates", async () => {
    const fake = makeFake();
    const { entry, outcome } = await run(fake, setup());

    await drain(fake);
    expect(fake.calls).toHaveLength(1);
    expect(fake.calls[0]?.issueId).toBe(ISSUE_ID);
    expect(fake.calls[0]?.body).toContain("turn completed");
    expect(fake.calls[0]?.body).toContain(outcome.summary);
    // PR link included when present
    expect(fake.calls[0]?.body).toContain("https://github.test/acme/flux/pull/7");

    const sent = await outboxRow(entry.id);
    expect(sent.status).toBe("SENT");
    expect(sent.sentAt).not.toBeNull();

    // idempotency: the SENT row is never picked up again
    await drain(fake);
    expect(fake.calls).toHaveLength(1);
  });

  it("a failed post retries with backoff and is marked sent only after success", async () => {
    const fake = makeFake();
    fake.failWith.current = new CallbackDeliveryError({ target: "linear", reason: "linear 500" });
    const { entry } = await run(fake, setup());

    await drain(fake);
    expect(fake.calls).toHaveLength(1); // the attempt happened...
    let row = await outboxRow(entry.id);
    expect(row.status).toBe("PENDING"); // ...but the row was NOT marked sent
    expect(row.attempts).toBe(1);
    expect(row.lastError).toContain("linear 500");
    expect(row.nextAttemptAt).not.toBeNull();

    // Still inside the backoff window: the row is not due, nothing is retried.
    await drain(fake);
    expect(fake.calls).toHaveLength(1);

    // After the backoff (first retry: 1s) the post succeeds and settles SENT.
    fake.failWith.current = undefined;
    await new Promise((resolve) => setTimeout(resolve, 1_100));
    await drain(fake);
    expect(fake.calls).toHaveLength(2);
    row = await outboxRow(entry.id);
    expect(row.status).toBe("SENT");

    // and success is final — no further posts
    await drain(fake);
    expect(fake.calls).toHaveLength(2);
  }, 15_000);

  it("a permanently failing row settles FAILED at the attempt threshold — never retried again", async () => {
    const fake = makeFake();
    fake.failWith.current = new CallbackDeliveryError({ target: "linear", reason: "linear down" });
    const { entry } = await run(fake, setup());
    // fast-forward the retry history to one attempt shy of the give-up threshold
    await testDb.db
      .update(outbox)
      .set({ attempts: MAX_DELIVERY_ATTEMPTS - 1 })
      .where(eq(outbox.id, entry.id));

    await drain(fake);
    expect(fake.calls).toHaveLength(1); // the final attempt happened...
    let row = await outboxRow(entry.id);
    expect(row.status).toBe("FAILED"); // ...and crossed the threshold: terminal
    expect(row.attempts).toBe(MAX_DELIVERY_ATTEMPTS);
    // the failure stays observable on the row
    expect(row.lastError).toContain("linear down");

    // terminal means terminal: even a now-healthy platform never sees the row
    fake.failWith.current = undefined;
    const processed = await drain(fake);
    expect(processed).toBe(0);
    expect(fake.calls).toHaveLength(1);
    row = await outboxRow(entry.id);
    expect(row.status).toBe("FAILED");
  });

  it("a row that succeeds before the threshold still settles SENT", async () => {
    const fake = makeFake();
    const { entry } = await run(fake, setup());
    await testDb.db
      .update(outbox)
      .set({ attempts: MAX_DELIVERY_ATTEMPTS - 1 })
      .where(eq(outbox.id, entry.id));

    await drain(fake);
    expect(fake.calls).toHaveLength(1);
    const row = await outboxRow(entry.id);
    expect(row.status).toBe("SENT");
    expect(row.sentAt).not.toBeNull();
  });

  it("a row without a resolvable Linear issue id is recorded as failed, not dropped", async () => {
    const fake = makeFake();
    const { entry } = await run(fake, setup());
    // corrupt the context linkage by pointing the payload at a run-less shape
    await testDb.db
      .update(outbox)
      .set({ payload: { nonsense: true } })
      .where(eq(outbox.id, entry.id));

    await drain(fake);
    expect(fake.calls).toHaveLength(0);
    const row = await outboxRow(entry.id);
    expect(row.status).toBe("PENDING");
    expect(row.attempts).toBe(1);
    expect(row.lastError).toContain("TurnOutcomePayload");
  });
});
