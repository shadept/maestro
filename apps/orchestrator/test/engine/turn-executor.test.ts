import { execFileSync } from "node:child_process";
import { access, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { TaskContext, TaskRun, TaskRunState } from "@maestro/domain";
import { Effect, Layer, type Scope } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AgentContract } from "../../src/agent/AgentContract.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { Db } from "../../src/db/Db.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { TurnExecutor, type TurnOutcomePayload } from "../../src/engine/TurnExecutor.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { branchNameFor, WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { TurnQueue } from "../../src/queue/TurnQueue.ts";
import { WorkerRuntime } from "../../src/runtime/WorkerRuntime.ts";
import { worktreeDir } from "../../src/storage/paths.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// The fake agent (test/fixtures/fake-agent): a `claude` shell script honoring
// the stream-json contract — emits valid events and commits a file into the
// mounted worktree. Built once per suite; never calls any API.
const FAKE_IMAGE = "maestro-fake-agent:fur14";
const FAKE_SESSION_UUID = "7f0e8a3c-0000-4000-8000-feedfacecafe";

type Services = TurnExecutor | TurnQueue | ProjectRepo | SessionRepo | TaskRunRepo | OutboxRepo;

let testDb: TestDb;
let root: string;
let storageRoot: string;
let originDir: string;

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

const makeLayer = (turnTimeoutSeconds: number): Layer.Layer<Services> => {
  const repos = Layer.mergeAll(
    ProjectRepo.layer,
    SessionRepo.layer,
    TaskRunRepo.layer,
    OutboxRepo.layer,
  );
  const gitLayer = Layer.mergeAll(GitCache.layer, WorktreeManager.layer).pipe(
    Layer.provideMerge(GitCache.layer),
    Layer.provide(RepoLocks.layer),
  );
  const executor = TurnExecutor.layer.pipe(
    Layer.provide(Layer.mergeAll(AgentContract.layer, WorkerRuntime.layerLocalCli, gitLayer)),
  );
  return Layer.mergeAll(executor, TurnQueue.layer).pipe(
    Layer.provideMerge(repos),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: testDb.connectionString,
        storageRoot,
        workerImage: FAKE_IMAGE,
        turnTimeoutSeconds,
        maxConcurrentWorkers: 2,
      }),
    ),
    Layer.orDie,
  );
};

let layer: Layer.Layer<Services>;

const run = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(layer)));

const taskContext = (ticketKey: string, body: string): TaskContext => ({
  source: "linear",
  ticket: { source: "linear", externalId: ticketKey },
  actor: "shade",
  title: `Ticket ${ticketKey}`,
  body,
  deliveryId: `d-${ticketKey}`,
  payload: {},
});

/** Creates project + session + PENDING TaskRun carrying the given body. */
const setupTurn = (ticketKey: string, body: string) =>
  Effect.gen(function* () {
    const projectRepo = yield* ProjectRepo;
    const sessionRepo = yield* SessionRepo;
    const taskRunRepo = yield* TaskRunRepo;
    const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
    const session = yield* sessionRepo.create({
      projectId: project.id,
      ticketReference: { source: "linear", externalId: ticketKey },
      gitBranch: branchNameFor({ source: "linear", externalId: ticketKey }, project),
    });
    const taskRun = yield* taskRunRepo.create(session.id, taskContext(ticketKey, body));
    return { project, session, taskRun };
  });

const outboxEntryFor = (taskRun: TaskRun) =>
  Effect.gen(function* () {
    const outboxRepo = yield* OutboxRepo;
    const pending = yield* outboxRepo.listPending(100);
    return pending.filter((entry) => entry.taskRunId === taskRun.id);
  });

const exists = (p: string) =>
  access(p).then(
    () => true,
    () => false,
  );

beforeAll(async () => {
  testDb = await startTestDb();
  // realpath: worktree/bare-repo metadata stores resolved absolute paths, and
  // the identity mounts must match them exactly (macOS tmpdir is a symlink).
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-executor-")));
  storageRoot = path.join(root, "storage");

  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");

  execFileSync(
    "docker",
    ["build", "-t", FAKE_IMAGE, path.resolve(import.meta.dirname, "../fixtures/fake-agent")],
    { stdio: "pipe" },
  );

  layer = makeLayer(120);
});

afterAll(async () => {
  // Workers run as container root; on Linux their files in the mounts are
  // root-owned, so clean the storage tree with the same runtime before rm.
  try {
    execFileSync(
      "docker",
      ["run", "--rm", "-v", `${root}:${root}`, FAKE_IMAGE, "rm", "-rf", storageRoot],
      {
        stdio: "pipe",
      },
    );
  } catch {
    // best effort — plain rm below handles the macOS case
  }
  await rm(root, { recursive: true, force: true });
  await testDb.stop();
});

describe("TurnExecutor", () => {
  it("happy path via the queue: full state walk to COMPLETED, logs, session uuid, outbox", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-101", "Please do the work."));

    const observed: TaskRunState[] = [];
    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        const taskRunRepo = yield* TaskRunRepo;
        yield* queue.work(executor.execute);
        observed.push((yield* taskRunRepo.get(taskRun.id)).state);
        yield* queue.enqueue({ taskRunId: taskRun.id, sessionId: session.id });
        const deadline = Date.now() + 45_000;
        while (true) {
          const current = yield* taskRunRepo.get(taskRun.id);
          if (observed.at(-1) !== current.state) observed.push(current.state);
          if (current.state === "COMPLETED" || current.state === "FAILED") return;
          if (Date.now() > deadline) return yield* Effect.die(new Error("turn never settled"));
          yield* Effect.sleep(50);
        }
      }),
    );

    // Full state walk observable in Postgres: starts at PENDING, only moves
    // forward along the machine, ends COMPLETED.
    const order: TaskRunState[] = ["PENDING", "PROVISIONING", "EXECUTING", "COMPLETED"];
    expect(observed[0]).toBe("PENDING");
    expect(observed.at(-1)).toBe("COMPLETED");
    expect(observed).toContain("EXECUTING");
    const indexes = observed.map((state) => order.indexOf(state));
    expect([...indexes].sort((a, b) => a - b)).toEqual(indexes);

    const settled = await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        const sessionRepo = yield* SessionRepo;
        return {
          taskRun: yield* taskRunRepo.get(taskRun.id),
          logs: yield* taskRunRepo.getLogs(taskRun.id),
          session: yield* sessionRepo.get(session.id),
          outbox: yield* outboxEntryFor(taskRun),
        };
      }),
    );

    // final text + deadlines persisted on the run
    expect(settled.taskRun.resultText).toBe("Committed agent-output.txt.");
    expect(settled.taskRun.cause).toBeNull();
    expect(settled.taskRun.expiresAt).toBeInstanceOf(Date);
    expect(settled.taskRun.evictableAfter).toBeInstanceOf(Date);

    // raw stream-json logs persisted on the run
    expect(settled.logs).toContain('"type":"system"');
    expect(settled.logs).toContain('"type":"result"');

    // claude session uuid stored for --resume; session settled WARM_IDLE
    expect(settled.session.claudeSessionUuid).toBe(FAKE_SESSION_UUID);
    expect(settled.session.state).toBe("WARM_IDLE");

    // outbox callback entry created
    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.kind).toBe("turn-completed");
    expect(payload.summary).toBe("Committed agent-output.txt.");
    expect(settled.outbox[0]?.idempotencyKey).toBe(`turn-result:${taskRun.id}`);

    // the agent's commit landed in the session worktree on its branch
    const worktree = worktreeDir(storageRoot, session.id);
    expect(await exists(path.join(worktree, "agent-output.txt"))).toBe(true);
    expect(git(worktree, "log", "--oneline")).toContain("fake agent work");
    expect(git(worktree, "rev-parse", "--abbrev-ref", "HEAD")).toBe(session.gitBranch);
  });

  it("agent exit 1: FAILED with cause, worktree intact, outbox failure entry", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-102", "MODE=FAIL"));

    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
      }),
    );

    const settled = await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        const sessionRepo = yield* SessionRepo;
        return {
          taskRun: yield* taskRunRepo.get(taskRun.id),
          session: yield* sessionRepo.get(session.id),
          outbox: yield* outboxEntryFor(taskRun),
        };
      }),
    );

    expect(settled.taskRun.state).toBe("FAILED");
    expect(settled.taskRun.cause).toBe("ERROR");
    expect(settled.taskRun.resultText).toBe("fake agent exploded");
    expect(settled.session.state).toBe("WARM_IDLE");

    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.kind).toBe("turn-failed");
    expect(payload.cause).toBe("ERROR");
    expect(payload.summary).toBe("fake agent exploded");

    // worktree preserved untouched for the next explicit resume
    const worktree = worktreeDir(storageRoot, session.id);
    expect(await exists(path.join(worktree, "README.md"))).toBe(true);
    expect(git(worktree, "status", "--porcelain")).toBe("");
  });

  it("timeout: worker killed, FAILED with cause=TIMEOUT", async () => {
    const shortLayer = makeLayer(3);
    const runShort = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
      Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(shortLayer)));

    const { session, taskRun } = await runShort(setupTurn("FUR-103", "MODE=HANG"));

    await runShort(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
      }),
    );

    const settled = await runShort(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        return {
          taskRun: yield* taskRunRepo.get(taskRun.id),
          outbox: yield* outboxEntryFor(taskRun),
        };
      }),
    );

    expect(settled.taskRun.state).toBe("FAILED");
    expect(settled.taskRun.cause).toBe("TIMEOUT");
    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.cause).toBe("TIMEOUT");
  }, 60_000);

  it("replayed job for a settled turn is a no-op (no second agent pass)", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-104", "MODE=FAIL"));

    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
        const before = yield* Effect.gen(function* () {
          const taskRunRepo = yield* TaskRunRepo;
          return yield* taskRunRepo.getLogs(taskRun.id);
        });
        // crash-recovered / duplicate delivery of the same job
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
        const taskRunRepo = yield* TaskRunRepo;
        expect(yield* taskRunRepo.getLogs(taskRun.id)).toBe(before);
        expect((yield* taskRunRepo.get(taskRun.id)).state).toBe("FAILED");
      }),
    );
  });
});
