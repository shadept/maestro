import { execFileSync } from "node:child_process";
import { access, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { SessionId, TaskContext, TaskRunId } from "@maestro/domain";
import { Effect, Layer, PubSub, Schema, type Scope } from "effect";
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
import { StartupReconciler } from "../../src/engine/StartupReconciler.ts";
import { TurnExecutor } from "../../src/engine/TurnExecutor.ts";
import { TurnOutcomePayload, TurnSettlement } from "../../src/engine/TurnSettlement.ts";
import { turnWorkerName } from "../../src/engine/worker-name.ts";
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

// FUR-40: startup reconciliation. Every run() call below builds a FRESH layer
// over the same database — one scoped run per simulated orchestrator process,
// so "tear down and rebuild" is the harness's default shape (same pattern as
// the FUR-13 restart test): phase A is the process that crashes mid-turn,
// phase B is the boot that must find and settle the wreckage.

type Services =
  | StartupReconciler
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

/** Starts a task through the real pipeline; returns session + first run. */
const startTask = (ticketKey: string, body: string) =>
  Effect.gen(function* () {
    const projectRepo = yield* ProjectRepo;
    const pipeline = yield* IngestPipeline;
    const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
    const outcome = yield* pipeline.startTask({ project, context: taskContext(ticketKey, body) });
    if (outcome._tag !== "SessionStarted") {
      return yield* Effect.die(new Error(`unexpected outcome ${outcome._tag}`));
    }
    return { project, sessionId: outcome.sessionId, taskRunId: outcome.taskRunId };
  });

const decodeOutcome = Schema.decodeUnknownSync(TurnOutcomePayload);

/** All pending outbox payloads for one session (no callback worker runs here). */
const outboxFor = (sessionId: SessionId) =>
  Effect.gen(function* () {
    const outboxRepo = yield* OutboxRepo;
    const pending = yield* outboxRepo.listPending(100);
    return pending
      .map((entry) => decodeOutcome(entry.payload))
      .filter((payload) => payload.sessionId === sessionId);
  });

const reconcileCollectingEvents = () =>
  run(
    Effect.gen(function* () {
      const bus = yield* EventBus;
      const reconciler = yield* StartupReconciler;
      const subscription = yield* bus.subscribe();
      yield* reconciler.reconcile();
      return yield* PubSub.takeUpTo(subscription, 1_000);
    }),
  );

const containerExists = (name: string): boolean => {
  try {
    execFileSync("docker", ["inspect", name], { stdio: "pipe" });
    return true;
  } catch {
    return false;
  }
};

const dockerKill = (name: string): void => {
  try {
    execFileSync("docker", ["kill", name], { stdio: "pipe" });
  } catch {
    // already gone
  }
};

const waitForContainerGone = async (name: string): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (containerExists(name)) {
    if (Date.now() > deadline) throw new Error(`container ${name} never went away`);
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
};

const waitForContainerRunning = async (name: string): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const state = execFileSync("docker", ["inspect", "--format", "{{.State.Running}}", name], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
      }).trim();
      if (state === "true") return;
    } catch {
      // not created yet
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`container ${name} never started running`);
};

beforeAll(async () => {
  testDb = await startTestDb();
  // realpath: worktree metadata stores resolved paths and the identity mounts
  // must match them exactly (macOS tmpdir is a symlink).
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-reconciler-")));
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
  const settlement = TurnSettlement.layer;
  const executor = TurnExecutor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentContract.layer,
        WorkerRuntime.layerLocalCli,
        gitLayer,
        terminator,
        settlement,
      ),
    ),
  );
  // the production wiring under test: reconciler over the same runtime,
  // terminator, and settlement path as the executor
  const reconciler = StartupReconciler.layer.pipe(
    Layer.provide(Layer.mergeAll(WorkerRuntime.layerLocalCli, terminator, settlement)),
  );
  const pipeline = IngestPipeline.layer.pipe(Layer.provide(terminator));
  layer = Layer.mergeAll(executor, reconciler, pipeline, terminator).pipe(
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

describe("StartupReconciler", () => {
  it("settles a crash-orphaned EXECUTING run FAILED/CANCELLED; live workers and PENDING runs untouched", async () => {
    // ── phase A: the process that crashes ────────────────────────────────
    // A real turn through pipeline + queue + executor, interrupted (scope
    // closed) mid-EXECUTING — exactly the tsx-watch restart observed live.
    const orphaned = await run(startTask("FUR-401", "MODE=SLOW"));
    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        yield* queue.work(executor.execute);
        yield* waitFor(
          getRun(orphaned.taskRunId),
          (r) => r.state === "EXECUTING",
          "orphan executing",
        );
        // the EXECUTING transition lands BEFORE the container starts — hold
        // the "process" open until the worker genuinely exists, otherwise the
        // kill below can race the container into being created afterwards
        yield* Effect.promise(() => waitForContainerRunning(turnWorkerName(orphaned.taskRunId)));
      }),
    );
    // the worker dies with the orchestrator (local-cli semantics); wait until
    // docker agrees so the boot below deterministically finds it gone
    dockerKill(turnWorkerName(orphaned.taskRunId));
    await waitForContainerGone(turnWorkerName(orphaned.taskRunId));
    expect((await run(getRun(orphaned.taskRunId))).state).toBe("EXECUTING");

    // A second session whose worker is GENUINELY still alive at boot (the
    // container survives a client-side crash): same deterministic name, row
    // walked to EXECUTING — must not be touched.
    const alive = await run(
      Effect.gen(function* () {
        const started = yield* startTask("FUR-402", "still running");
        const taskRunRepo = yield* TaskRunRepo;
        yield* taskRunRepo.transition(started.taskRunId, "PROVISIONING");
        yield* taskRunRepo.transition(started.taskRunId, "EXECUTING");
        return started;
      }),
    );
    execFileSync(
      "docker",
      [
        "run",
        "-d",
        "--rm",
        "--name",
        turnWorkerName(alive.taskRunId),
        FAKE_AGENT_IMAGE,
        "sleep",
        "120",
      ],
      { stdio: "pipe" },
    );
    await waitForContainerRunning(turnWorkerName(alive.taskRunId));

    // A PENDING run is NOT an orphan: its queue job survived the restart.
    const pending = await run(startTask("FUR-403", "queued but never dispatched"));

    try {
      // ── phase B: boot ───────────────────────────────────────────────────
      const events = await reconcileCollectingEvents();

      // the orphan settled FAILED with the deliberate CANCELLED cause
      const settled = await run(getRun(orphaned.taskRunId));
      expect(settled.state).toBe("FAILED");
      expect(settled.cause).toBe("CANCELLED");
      // ...with the reason persisted on the run (shared TurnSettlement path)
      expect(settled.failureSummary).toContain("Maestro restarted");
      // ...announced through the normal event pipeline
      expect(
        events.some(
          (event) =>
            event._tag === "TaskRunStateChanged" &&
            event.taskRun.id === orphaned.taskRunId &&
            event.taskRun.state === "FAILED",
        ),
      ).toBe(true);

      // outbox failure callback created, per-run text carrying the run id
      const outbox = await run(outboxFor(orphaned.sessionId));
      expect(outbox).toHaveLength(1);
      expect(outbox[0]?.kind).toBe("turn-failed");
      expect(outbox[0]?.cause).toBe("CANCELLED");
      expect(outbox[0]?.summary).toContain("Maestro restarted");
      expect(outbox[0]?.summary).toContain(orphaned.taskRunId);

      // session consistent: back at rest, breaker NOT tripped (CANCELLED says
      // nothing about agent health), worktree preserved for an explicit resume
      const session = await run(getSession(orphaned.sessionId));
      expect(session.state).toBe("WARM_IDLE");
      expect(session.pausedAt).toBeNull();
      expect(session.terminationRequestedAt).toBeNull();
      expect(await exists(worktreeDir(storageRoot, orphaned.sessionId))).toBe(true);

      // the genuinely running worker's turn is untouched, no callback for it
      expect((await run(getRun(alive.taskRunId))).state).toBe("EXECUTING");
      expect(await run(outboxFor(alive.sessionId))).toHaveLength(0);

      // the PENDING run is untouched — its job dispatches normally later
      expect((await run(getRun(pending.taskRunId))).state).toBe("PENDING");

      // reconcile is idempotent: a second boot pass changes nothing
      await run(
        Effect.gen(function* () {
          const reconciler = yield* StartupReconciler;
          yield* reconciler.reconcile();
        }),
      );
      expect(await run(outboxFor(orphaned.sessionId))).toHaveLength(1);
      expect((await run(getRun(alive.taskRunId))).state).toBe("EXECUTING");
    } finally {
      dockerKill(turnWorkerName(alive.taskRunId));
    }
    // settle the live-worker run so later reconciles in this suite see a
    // clean slate (its container is gone now)
    await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        yield* taskRunRepo.transition(alive.taskRunId, "FAILED", { cause: "CANCELLED" });
      }),
    );
  }, 180_000);

  it("finalizes a session whose termination marker survived the crash (no active turn)", async () => {
    // phase A: a completed turn, then the terminal signal lands but the
    // process dies right after persisting the marker — before teardown ran
    const started = await run(startTask("FUR-404", "Please do the work."));
    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        yield* queue.work(executor.execute);
        yield* waitFor(getRun(started.taskRunId), (r) => r.state === "COMPLETED", "turn completes");
      }),
    );
    await run(
      Effect.gen(function* () {
        const sessionRepo = yield* SessionRepo;
        yield* sessionRepo.requestTermination(started.sessionId);
      }),
    );
    const worktree = worktreeDir(storageRoot, started.sessionId);
    const configDir = sessionConfigDir(storageRoot, started.sessionId);
    expect(await exists(worktree)).toBe(true);

    // phase B: boot finalizes the interrupted teardown
    await reconcileCollectingEvents();

    const session = await run(getSession(started.sessionId));
    expect(session.state).toBe("TERMINATED");
    expect(await exists(worktree)).toBe(false);
    expect(await exists(configDir)).toBe(false);
    const bareRepo = repoCacheDir(storageRoot, started.project.id);
    expect(git(bareRepo, "worktree", "list", "--porcelain")).not.toContain(started.sessionId);
    // the completed turn's history is untouched
    expect((await run(getRun(started.taskRunId))).state).toBe("COMPLETED");
  }, 120_000);

  it("orphan sweep unblocks a deferred teardown: marked session with an orphaned run ends TERMINATED", async () => {
    // phase A (synthetic crash state): a run orphaned in PROVISIONING (the
    // worker never even started) on a session already marked for termination
    // — the FUR-19 deferred-teardown leftover.
    const started = await run(
      Effect.gen(function* () {
        const state = yield* startTask("FUR-405", "doomed");
        const taskRunRepo = yield* TaskRunRepo;
        const sessionRepo = yield* SessionRepo;
        yield* taskRunRepo.transition(state.taskRunId, "PROVISIONING");
        yield* sessionRepo.requestTermination(state.sessionId);
        return state;
      }),
    );

    // phase B: the orphan settles first, which unblocks the marker sweep
    await reconcileCollectingEvents();

    const settled = await run(getRun(started.taskRunId));
    expect(settled.state).toBe("FAILED");
    expect(settled.cause).toBe("CANCELLED");
    expect((await run(getSession(started.sessionId))).state).toBe("TERMINATED");
    expect(await exists(sessionConfigDir(storageRoot, started.sessionId))).toBe(false);
  }, 120_000);
});
