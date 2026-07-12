import type { SessionId, TaskRunId } from "@maestro/domain";
import { sql } from "drizzle-orm";
import { Effect, Layer, type Scope } from "effect";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { type TurnJob, TurnQueue } from "../../src/queue/TurnQueue.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

let testDb: TestDb;

beforeAll(async () => {
  testDb = await startTestDb();
});

afterAll(async () => {
  await testDb.stop();
});

beforeEach(async () => {
  // pg-boss owns its own schema; wipe leftover jobs between tests (the schema
  // only exists once the first TurnQueue layer has started).
  await testDb.db.execute(
    sql`do $$ begin
      if to_regclass('pgboss.job') is not null then delete from pgboss.job; end if;
    end $$`,
  );
});

const layerFor = (maxConcurrentWorkers: number) =>
  TurnQueue.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        EventBus.layer,
        AppConfig.layerTest({
          databaseUrl: testDb.connectionString,
          maxConcurrentWorkers,
        }),
      ),
    ),
  );

// Each layer build is a fresh pg-boss instance over the same database — one
// scoped run per simulated orchestrator process.
const run = <A, E>(cap: number, effect: Effect.Effect<A, E, TurnQueue | Scope.Scope>) =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layerFor(cap))));

const taskRunId = (n: number): TaskRunId =>
  `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}` as TaskRunId;
const sessionId = (n: number): SessionId =>
  `11111111-0000-4000-8000-${n.toString().padStart(12, "0")}` as SessionId;

interface Ev {
  readonly taskRunId: TaskRunId;
  readonly kind: "start" | "end";
  readonly at: number;
}

/** Records start/end events plus the concurrency high-water mark. */
const makeRecorder = (durationMillis: number) => {
  const events: Ev[] = [];
  let inFlight = 0;
  let maxInFlight = 0;
  const handler = (job: TurnJob) =>
    Effect.gen(function* () {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      events.push({ taskRunId: job.taskRunId, kind: "start", at: Date.now() });
      yield* Effect.sleep(durationMillis);
      events.push({ taskRunId: job.taskRunId, kind: "end", at: Date.now() });
      inFlight -= 1;
    });
  const starts = () => events.filter((e) => e.kind === "start");
  const ends = () => events.filter((e) => e.kind === "end");
  return { events, handler, starts, ends, maxInFlight: () => maxInFlight };
};

const eventAt = (events: readonly Ev[], id: TaskRunId, kind: "start" | "end"): number => {
  const event = events.find((e) => e.taskRunId === id && e.kind === kind);
  if (!event) throw new Error(`missing ${kind} event for ${id}`);
  return event.at;
};

const waitFor = (predicate: () => boolean, timeoutMillis = 30_000): Effect.Effect<void> =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMillis;
    while (!predicate()) {
      if (Date.now() > deadline) {
        return yield* Effect.die(new Error("timed out waiting for queue activity"));
      }
      yield* Effect.sleep(50);
    }
  });

describe("TurnQueue", () => {
  it("two turns on one session run strictly sequentially, in arrival order", async () => {
    const session = sessionId(1);
    const [t1, t2] = [taskRunId(1), taskRunId(2)];
    const recorder = makeRecorder(400);

    await run(
      4, // cap above session count: only per-session FIFO can serialize these
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        yield* queue.work(recorder.handler);
        yield* queue.enqueue({ taskRunId: t1, sessionId: session });
        yield* queue.enqueue({ taskRunId: t2, sessionId: session });
        yield* waitFor(() => recorder.ends().length === 2);
      }),
    );

    expect(recorder.starts().map((e) => e.taskRunId)).toEqual([t1, t2]);
    expect(eventAt(recorder.events, t2, "start")).toBeGreaterThanOrEqual(
      eventAt(recorder.events, t1, "end"),
    );
    expect(recorder.maxInFlight()).toBe(1);
  });

  it("turns on different sessions run concurrently, capped by the global limit", async () => {
    const [a, b, c] = [taskRunId(11), taskRunId(12), taskRunId(13)];
    const recorder = makeRecorder(1_000);

    await run(
      2,
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        yield* queue.enqueue({ taskRunId: a, sessionId: sessionId(11) });
        yield* queue.enqueue({ taskRunId: b, sessionId: sessionId(12) });
        yield* queue.enqueue({ taskRunId: c, sessionId: sessionId(13) });
        yield* queue.work(recorder.handler);
        yield* waitFor(() => recorder.ends().length === 3);
      }),
    );

    const at = (id: TaskRunId, kind: "start" | "end") => eventAt(recorder.events, id, kind);
    // a and b overlapped (real cross-session concurrency)…
    expect(at(a, "start")).toBeLessThan(at(b, "end"));
    expect(at(b, "start")).toBeLessThan(at(a, "end"));
    // …but c had to wait for a free slot (cap 2).
    expect(at(c, "start")).toBeGreaterThanOrEqual(Math.min(at(a, "end"), at(b, "end")));
    expect(recorder.maxInFlight()).toBe(2);
  });

  it("burst of 12 enqueues respects the cap, per-session order, and fair dispatch", async () => {
    const sessions = [sessionId(21), sessionId(22), sessionId(23), sessionId(24)];
    // Arrival order: turn 1 of every session, then turn 2 of every session, …
    const turnsBySession = new Map<SessionId, TaskRunId[]>(sessions.map((s) => [s, []]));
    const jobs: TurnJob[] = [];
    for (let turn = 0; turn < 3; turn++) {
      for (const [i, session] of sessions.entries()) {
        const id = taskRunId(100 + turn * 10 + i);
        turnsBySession.get(session)?.push(id);
        jobs.push({ taskRunId: id, sessionId: session });
      }
    }
    const recorder = makeRecorder(150);

    await run(
      3,
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        for (const job of jobs) {
          yield* queue.enqueue(job);
        }
        yield* queue.work(recorder.handler);
        yield* waitFor(() => recorder.ends().length === 12);
      }),
    );

    expect(recorder.ends()).toHaveLength(12);
    // Global cap respected, and real concurrency happened.
    expect(recorder.maxInFlight()).toBeLessThanOrEqual(3);
    expect(recorder.maxInFlight()).toBeGreaterThanOrEqual(2);
    // Strict FIFO within each session.
    for (const [session, turns] of turnsBySession) {
      const observed = recorder
        .starts()
        .map((e) => e.taskRunId)
        .filter((id) => turns.includes(id));
      expect(observed, `session ${session}`).toEqual(turns);
    }
    // Fair dispatch: every session's first turn runs before any second turn.
    const firstFour = recorder
      .starts()
      .slice(0, 4)
      .map((e) => e.taskRunId)
      .sort();
    expect(firstFour).toEqual(sessions.map((s) => turnsBySession.get(s)?.[0]).sort());
  });

  it("jobs survive an orchestrator restart: a fresh worker picks them up in order", async () => {
    const session = sessionId(31);
    const turns = [taskRunId(31), taskRunId(32), taskRunId(33)];

    // Process one: enqueue only, then die (scope close stops this pg-boss).
    await run(
      2,
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        for (const id of turns) {
          yield* queue.enqueue({ taskRunId: id, sessionId: session });
        }
      }),
    );

    // Process two: brand-new pg-boss instance over the same database.
    const recorder = makeRecorder(100);
    await run(
      2,
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        yield* queue.work(recorder.handler);
        yield* waitFor(() => recorder.ends().length === 3);
      }),
    );

    expect(recorder.starts().map((e) => e.taskRunId)).toEqual(turns);
  });

  it("re-enqueueing the same task run is a no-op (replay safety)", async () => {
    const job: TurnJob = { taskRunId: taskRunId(41), sessionId: sessionId(41) };
    const recorder = makeRecorder(50);

    await run(
      2,
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        yield* queue.enqueue(job);
        yield* queue.enqueue(job);
        yield* queue.work(recorder.handler);
        yield* waitFor(() => recorder.ends().length === 1);
        // A duplicate would dispatch within this window.
        yield* Effect.sleep(750);
      }),
    );

    expect(recorder.starts()).toHaveLength(1);
  });
});
