import { execFileSync } from "node:child_process";
import { access, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { SessionId, TaskContext, TaskRunId } from "@maestro/domain";
import { sql } from "drizzle-orm";
import { Effect, Layer, PubSub, type Scope } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AgentContract } from "../../src/agent/AgentContract.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { AuditRepo } from "../../src/db/AuditRepo.ts";
import { Db } from "../../src/db/Db.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { SessionTerminator } from "../../src/engine/SessionTerminator.ts";
import { TurnExecutor } from "../../src/engine/TurnExecutor.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { GitHubForge } from "../../src/forge/GitHubForge.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { OutboundGit } from "../../src/git/OutboundGit.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { IngestPipeline } from "../../src/ingest/IngestPipeline.ts";
import { TurnQueue } from "../../src/queue/TurnQueue.ts";
import { WorkerRuntime } from "../../src/runtime/WorkerRuntime.ts";
import { repoCacheDir, sessionConfigDir, worktreeDir } from "../../src/storage/paths.ts";
import {
  buildFakeAgentImage,
  cleanStorageViaContainer,
  FAKE_AGENT_IMAGE,
  fakeAgentRuntimeTemplate,
} from "../support/fake-agent.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// FUR-19 (M1.15): terminal cleanup — ticket closure → session teardown, end
// to end against real Postgres, real git, the real pg-boss queue, and the
// fake-agent worker image. No live API calls anywhere.

type Services =
  | TurnExecutor
  | TurnQueue
  | SessionTerminator
  | IngestPipeline
  | ProjectRepo
  | SessionRepo
  | TaskRunRepo
  | OutboxRepo
  | AuditRepo
  | EventBus;

let testDb: TestDb;
let root: string;
let storageRoot: string;
let originDir: string;
let layer: Layer.Layer<Services>;

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

const exists = (p: string) =>
  access(p).then(
    () => true,
    () => false,
  );

const run = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layer)));

const taskContext = (ticketKey: string, body: string): TaskContext => ({
  source: "linear",
  ticket: { source: "linear", externalId: ticketKey },
  actor: "shade",
  title: `Ticket ${ticketKey}`,
  body,
  deliveryId: `d-${ticketKey}-${body.length}`,
  payload: {},
});

/** Polls `read` until `until` holds; dies past the deadline. */
const waitFor = <A, E>(
  read: Effect.Effect<A, E, Services>,
  until: (value: A) => boolean,
  what: string,
  deadlineMillis = 45_000,
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

const getRun = (id: TaskRunId) =>
  Effect.gen(function* () {
    const taskRunRepo = yield* TaskRunRepo;
    return yield* taskRunRepo.get(id);
  });

const getSession = (id: SessionId) =>
  Effect.gen(function* () {
    const sessionRepo = yield* SessionRepo;
    return yield* sessionRepo.get(id);
  });

const pgBossJobState = async (jobId: string): Promise<string | undefined> => {
  const result = await testDb.db.execute(
    sql`select state::text as state from pgboss.job where name = 'turns' and id = ${jobId}`,
  );
  return (result.rows as Array<{ state: string }>)[0]?.state;
};

beforeAll(async () => {
  testDb = await startTestDb();
  // realpath: worktree metadata stores resolved paths and the identity mounts
  // must match them exactly (macOS tmpdir is a symlink).
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-terminator-")));
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
  const pipeline = IngestPipeline.layer.pipe(Layer.provide(terminator));
  layer = Layer.mergeAll(executor, pipeline, terminator).pipe(
    Layer.provideMerge(TurnQueue.layer),
    Layer.provideMerge(repos),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.provideMerge(EventBus.layer),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: testDb.connectionString,
        storageRoot,
        workerImage: FAKE_AGENT_IMAGE,
        // run the worker as the test process uid so it can write the mounts
        // on Linux hosts (bind mounts preserve real ownership there)
        runtimeTemplate: fakeAgentRuntimeTemplate(),
        turnTimeoutSeconds: 120,
        maxConcurrentWorkers: 2,
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

describe("SessionTerminator", () => {
  it("full lifecycle: completed turn + queued turn, terminal signal tears everything down", async () => {
    const ticket = { source: "linear", externalId: "FUR-201" } as const;

    // Turn 1 through the real pipeline + queue + executor, to completion.
    const started = await run(
      Effect.gen(function* () {
        const projectRepo = yield* ProjectRepo;
        const pipeline = yield* IngestPipeline;
        const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
        const outcome = yield* pipeline.startTask({
          project,
          context: taskContext("FUR-201", "Please do the work."),
        });
        if (outcome._tag !== "SessionStarted") {
          return yield* Effect.die(new Error(`unexpected outcome ${outcome._tag}`));
        }
        return { project, sessionId: outcome.sessionId, turn1: outcome.taskRunId };
      }),
    );

    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        yield* queue.work(executor.execute);
        yield* waitFor(getRun(started.turn1), (r) => r.state === "COMPLETED", "turn 1 settles");
        // wait for the pg-boss job to settle too: closing the scope while the
        // handler is still completing would leave the job 'active' and its
        // session group blocked for the phase-3 dispatcher below
        yield* waitFor(
          Effect.promise(() => pgBossJobState(started.turn1)),
          (state) => state === "completed",
          "turn 1 job completes",
          15_000,
        );
      }),
    );
    // scope closed — the dispatcher is gone, turn 2 below stays queued

    // Turn 2: queued but never dispatched.
    const turn2 = await run(
      Effect.gen(function* () {
        const pipeline = yield* IngestPipeline;
        const outcome = yield* pipeline.queueTurn({
          context: taskContext("FUR-201", "One more thing."),
        });
        if (outcome._tag !== "TurnQueued") {
          return yield* Effect.die(new Error(`unexpected outcome ${outcome._tag}`));
        }
        return outcome.taskRunId;
      }),
    );

    const worktree = worktreeDir(storageRoot, started.sessionId);
    const configDir = sessionConfigDir(storageRoot, started.sessionId);
    const bareRepo = repoCacheDir(storageRoot, started.project.id);
    expect(await exists(worktree)).toBe(true);
    expect(await exists(configDir)).toBe(true);

    // The terminal signal, via the same seam ingest uses.
    const events = await run(
      Effect.gen(function* () {
        const bus = yield* EventBus;
        const pipeline = yield* IngestPipeline;
        const subscription = yield* bus.subscribe();
        const outcome = yield* pipeline.recordTerminal({
          ticket,
          actor: "tester",
          signal: "done",
        });
        expect(outcome._tag).toBe("TerminalRecorded");
        return yield* PubSub.takeUpTo(subscription, 1_000);
      }),
    );

    // session TERMINATED in Postgres, visible live via SessionStateChanged
    const session = await run(getSession(started.sessionId));
    expect(session.state).toBe("TERMINATED");
    expect(session.terminationRequestedAt).toBeInstanceOf(Date);
    expect(
      events.some(
        (event) =>
          event._tag === "SessionStateChanged" &&
          event.session.id === started.sessionId &&
          event.session.state === "TERMINATED",
      ),
    ).toBe(true);

    // queued turn cancelled; the completed turn untouched
    const cancelled = await run(getRun(turn2));
    expect(cancelled.state).toBe("FAILED");
    expect(cancelled.cause).toBe("CANCELLED");
    expect((await run(getRun(started.turn1))).state).toBe("COMPLETED");

    // worktree gone — directory removed AND `git worktree list` clean
    expect(await exists(worktree)).toBe(false);
    expect(git(bareRepo, "worktree", "list", "--porcelain")).not.toContain(started.sessionId);
    expect(git(bareRepo, "branch", "--list", session.gitBranch)).toBe("");
    // session CLAUDE_CONFIG_DIR purged
    expect(await exists(configDir)).toBe(false);

    // audit trail carries the signal
    const audits = await run(
      Effect.gen(function* () {
        const audit = yield* AuditRepo;
        return yield* audit.list;
      }),
    );
    expect(
      audits.some(
        (a) => a.action === "ticket-done" && a.targetEntity === `session:${started.sessionId}`,
      ),
    ).toBe(true);

    // double-close is a no-op: ingest finds no active session, and a direct
    // second terminate reports AlreadyTerminated without changing anything
    const second = await run(
      Effect.gen(function* () {
        const pipeline = yield* IngestPipeline;
        const terminator = yield* SessionTerminator;
        const viaIngest = yield* pipeline.recordTerminal({
          ticket,
          actor: "tester",
          signal: "done",
        });
        const direct = yield* terminator.terminate({ sessionId: started.sessionId });
        return { viaIngest, direct };
      }),
    );
    expect(second.viaIngest._tag).toBe("Ignored");
    expect(second.direct._tag).toBe("AlreadyTerminated");
    expect((await run(getSession(started.sessionId))).state).toBe("TERMINATED");

    // The cancelled turn's pg-boss job is still queued — the dispatcher must
    // drain it via the executor's non-PENDING guard (the documented reason no
    // queue-side cancel API exists), with no agent pass.
    expect(await pgBossJobState(turn2)).toBe("created");
    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        yield* queue.work(executor.execute);
        yield* waitFor(
          Effect.promise(() => pgBossJobState(turn2)),
          (state) => state === "completed",
          "cancelled turn's job drains",
          15_000,
        );
      }),
    );
    const drained = await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        return {
          run: yield* taskRunRepo.get(turn2),
          logs: yield* taskRunRepo.getLogs(turn2),
        };
      }),
    );
    expect(drained.run.state).toBe("FAILED");
    expect(drained.logs).toBe("");
  }, 120_000);

  it("terminal signal during EXECUTING: the turn finishes, then teardown runs", async () => {
    const started = await run(
      Effect.gen(function* () {
        const projectRepo = yield* ProjectRepo;
        const pipeline = yield* IngestPipeline;
        const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
        const outcome = yield* pipeline.startTask({
          project,
          context: taskContext("FUR-202", "MODE=SLOW"),
        });
        if (outcome._tag !== "SessionStarted") {
          return yield* Effect.die(new Error(`unexpected outcome ${outcome._tag}`));
        }
        return { sessionId: outcome.sessionId, turn1: outcome.taskRunId };
      }),
    );
    const worktree = worktreeDir(storageRoot, started.sessionId);
    const configDir = sessionConfigDir(storageRoot, started.sessionId);

    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        const terminator = yield* SessionTerminator;
        yield* queue.work(executor.execute);
        yield* waitFor(getRun(started.turn1), (r) => r.state === "EXECUTING", "turn executing");

        // mid-turn terminal signal: deferred, never a mid-turn kill
        const outcome = yield* terminator.terminate({ sessionId: started.sessionId });
        expect(outcome).toEqual({ _tag: "Deferred", awaiting: [started.turn1] });

        const marked = yield* getSession(started.sessionId);
        expect(marked.state).toBe("WARM_IDLE");
        expect(marked.terminationRequestedAt).toBeInstanceOf(Date);
        expect(yield* Effect.promise(() => exists(worktree))).toBe(true);

        // the turn is allowed to finish...
        const settled = yield* waitFor(
          getRun(started.turn1),
          (r) => r.state === "COMPLETED" || r.state === "FAILED",
          "slow turn settles",
        );
        expect(settled.state).toBe("COMPLETED");
        expect(settled.resultText).toBe("Finished slowly.");

        // ...and the executor finalizes the deferred teardown right after
        yield* waitFor(
          getSession(started.sessionId),
          (s) => s.state === "TERMINATED",
          "deferred teardown finalizes",
          15_000,
        );
      }),
    );

    expect(await exists(worktree)).toBe(false);
    expect(await exists(configDir)).toBe(false);
  }, 120_000);
});
