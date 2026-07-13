import { execFileSync } from "node:child_process";
import {
  access,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  type AgentOverrides,
  ForgeApiError,
  type TaskContext,
  type TaskRun,
  type TaskRunState,
} from "@maestro/domain";
import { Effect, Layer, Option, PubSub, Redacted, type Scope } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AgentContract, standingOrders } from "../../src/agent/AgentContract.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { AuditRepo } from "../../src/db/AuditRepo.ts";
import { Db } from "../../src/db/Db.ts";
import { OutboxRepo } from "../../src/db/OutboxRepo.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { TaskRunRepo } from "../../src/db/TaskRunRepo.ts";
import { SessionTerminator } from "../../src/engine/SessionTerminator.ts";
import { TurnExecutor } from "../../src/engine/TurnExecutor.ts";
import { type TurnOutcomePayload, TurnSettlement } from "../../src/engine/TurnSettlement.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { type ForgeCall, GitHubForge } from "../../src/forge/GitHubForge.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { OutboundGit } from "../../src/git/OutboundGit.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { branchNameFor, WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { TurnQueue } from "../../src/queue/TurnQueue.ts";
import { WorkerRuntime } from "../../src/runtime/WorkerRuntime.ts";
import { repoCacheDir, worktreeDir } from "../../src/storage/paths.ts";
import {
  buildFakeAgentImage,
  cleanStorageViaContainer,
  FAKE_AGENT_IMAGE,
  fakeAgentRuntimeTemplate,
} from "../support/fake-agent.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

const FAKE_SESSION_UUID = "7f0e8a3c-0000-4000-8000-feedfacecafe";

type Services =
  | TurnExecutor
  | TurnQueue
  | ProjectRepo
  | SessionRepo
  | TaskRunRepo
  | OutboxRepo
  | EventBus;

let testDb: TestDb;
let root: string;
let storageRoot: string;
let originDir: string;
let gitShimLog: string;

const ORCHESTRATOR_TOKEN = "SUPER-SECRET-TOKEN";

/**
 * Transparent `git` shim prepended to PATH: records argv + every GIT_CONFIG_*
 * env var per invocation, then execs the real git. This is how the suite
 * proves credentials reach remote operations via per-invocation env (the
 * FUR-10 mechanism) and never via argv.
 */
const installGitShim = async () => {
  const realGit = execFileSync("/bin/sh", ["-c", "command -v git"], { encoding: "utf8" }).trim();
  gitShimLog = path.join(root, "git-shim.log");
  const shimDir = path.join(root, "git-shim-bin");
  await mkdir(shimDir, { recursive: true });
  await writeFile(
    path.join(shimDir, "git"),
    [
      "#!/bin/sh",
      `{ printf 'argv: %s\\n' "$*"; env | grep '^GIT_CONFIG' || true; echo '==='; } >> '${gitShimLog}'`,
      `exec '${realGit}' "$@"`,
      "",
    ].join("\n"),
    { mode: 0o755 },
  );
  process.env.PATH = `${shimDir}:${process.env.PATH}`;
};

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

const makeLayer = (
  turnTimeoutSeconds: number,
  forge: { readonly calls?: ForgeCall[]; readonly failWith?: ForgeApiError } = {},
): Layer.Layer<Services> => {
  const repos = Layer.mergeAll(
    ProjectRepo.layer,
    SessionRepo.layer,
    TaskRunRepo.layer,
    OutboxRepo.layer,
    AuditRepo.layer,
  );
  const gitLayer = Layer.mergeAll(GitCache.layer, WorktreeManager.layer, OutboundGit.layer).pipe(
    Layer.provideMerge(GitCache.layer),
    Layer.provide(Layer.mergeAll(RepoLocks.layer, GitHubForge.layerTest(forge))),
  );
  const terminator = SessionTerminator.layer.pipe(Layer.provide(gitLayer));
  const executor = TurnExecutor.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        AgentContract.layer,
        WorkerRuntime.layerLocalCli,
        gitLayer,
        terminator,
        TurnSettlement.layer,
      ),
    ),
  );
  return Layer.mergeAll(executor, TurnQueue.layer).pipe(
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
        turnTimeoutSeconds,
        maxConcurrentWorkers: 2,
        // a file:// origin ignores credentials, but they must be OFFERED to
        // the clone (asserted via the git shim) and must never persist
        githubToken: Option.some(Redacted.make(ORCHESTRATOR_TOKEN)),
      }),
    ),
    Layer.orDie,
  );
};

let layer: Layer.Layer<Services>;
const forgeCalls: ForgeCall[] = [];

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
const setupTurn = (ticketKey: string, body: string, agent: AgentOverrides = {}) =>
  Effect.gen(function* () {
    const projectRepo = yield* ProjectRepo;
    const sessionRepo = yield* SessionRepo;
    const taskRunRepo = yield* TaskRunRepo;
    const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}`, agent });
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
  await installGitShim();

  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");

  buildFakeAgentImage();

  layer = makeLayer(120, { calls: forgeCalls });
});

afterAll(async () => {
  cleanStorageViaContainer(root, storageRoot);
  await rm(root, { recursive: true, force: true });
  await testDb.stop();
});

describe("TurnExecutor", () => {
  it("happy path via the queue: full state walk to COMPLETED, logs, session uuid, outbox", async () => {
    const { project, session, taskRun } = await run(setupTurn("FUR-101", "Please do the work."));

    const observed: TaskRunState[] = [];
    await run(
      Effect.gen(function* () {
        const queue = yield* TurnQueue;
        const executor = yield* TurnExecutor;
        const taskRunRepo = yield* TaskRunRepo;
        const bus = yield* EventBus;
        // FUR-16: the executor's log tee must publish every worker chunk.
        const subscription = yield* bus.subscribe();
        yield* queue.work(executor.execute);
        observed.push((yield* taskRunRepo.get(taskRun.id)).state);
        yield* queue.enqueue({ taskRunId: taskRun.id, sessionId: session.id });
        const deadline = Date.now() + 45_000;
        while (true) {
          const current = yield* taskRunRepo.get(taskRun.id);
          if (observed.at(-1) !== current.state) observed.push(current.state);
          if (current.state === "COMPLETED" || current.state === "FAILED") break;
          if (Date.now() > deadline) return yield* Effect.die(new Error("turn never settled"));
          yield* Effect.sleep(50);
        }
        // Every persisted log byte was also published live, in order (the log
        // chunks all precede the settling transition, so they are buffered).
        const events = yield* PubSub.takeUpTo(subscription, 10_000);
        const chunks = events.flatMap((event) =>
          event._tag === "LogChunk" && event.taskRunId === taskRun.id ? [event.chunk] : [],
        );
        expect(chunks.length).toBeGreaterThan(0);
        expect(chunks.join("")).toBe(yield* taskRunRepo.getLogs(taskRun.id));
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

    // outbox callback entry created, linking the PR
    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.kind).toBe("turn-completed");
    expect(payload.summary).toBe("Committed agent-output.txt.");
    expect(settled.outbox[0]?.idempotencyKey).toBe(`turn-result:${taskRun.id}`);
    expect(payload.pr).not.toBeNull();
    expect(payload.pr?.url).toContain("github.test");

    // the agent's commit landed in the session worktree on its branch
    const worktree = worktreeDir(storageRoot, session.id);
    expect(await exists(path.join(worktree, "agent-output.txt"))).toBe(true);
    expect(git(worktree, "log", "--oneline")).toContain("fake agent work");
    expect(git(worktree, "rev-parse", "--abbrev-ref", "HEAD")).toBe(session.gitBranch);

    // FUR-15: the orchestrator pushed the agent's commit and opened a draft PR
    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBe(
      git(worktree, "rev-parse", "HEAD"),
    );
    const createCall = forgeCalls.find((c) => c.args.headBranch === session.gitBranch);
    expect(createCall?.op).toBe("create");
    expect(createCall?.args.draft).toBe(false);
    expect(createCall?.args.title).toBe("FUR-101: Ticket FUR-101");
    expect(settled.session.prNumber).toBe(payload.pr?.number);
    expect(settled.session.prUrl).toBe(payload.pr?.url);

    // Private-repo provisioning: the orchestrator token authenticated the
    // cache clone via per-invocation GIT_CONFIG_* env — never argv, never
    // stored config (the shim saw every git invocation of this turn).
    const expectedHeader = `Authorization: Basic ${Buffer.from(
      `x-access-token:${ORCHESTRATOR_TOKEN}`,
    ).toString("base64")}`;
    const invocations = (await readFile(gitShimLog, "utf8")).split("===");
    const cloneInvocations = invocations.filter((block) => block.includes("argv: clone"));
    expect(cloneInvocations.length).toBeGreaterThan(0);
    for (const invocation of cloneInvocations) {
      expect(invocation).toContain(`GIT_CONFIG_VALUE_0=${expectedHeader}`);
    }
    // frozen-cache fix: the base was refreshed from origin before provisioning
    // — base-only refspec (never the all-heads mirror), same credential path
    const fetchInvocations = invocations.filter((block) => block.includes("argv: fetch"));
    expect(fetchInvocations.length).toBeGreaterThan(0);
    for (const invocation of fetchInvocations) {
      expect(invocation).toContain("+refs/heads/main:refs/heads/main");
      expect(invocation).toContain(`GIT_CONFIG_VALUE_0=${expectedHeader}`);
    }
    // the raw token never crossed a git process boundary in the clear
    expect(invocations.join("")).not.toContain(ORCHESTRATOR_TOKEN);
    // and never landed at rest in the cached clone's config
    const cacheConfig = await readFile(
      path.join(repoCacheDir(storageRoot, project.id), "config"),
      "utf8",
    );
    expect(cacheConfig).not.toContain(ORCHESTRATOR_TOKEN);
    expect(cacheConfig.toLowerCase()).not.toContain("authorization");
  });

  it("no-commit turn: COMPLETED, publish skipped silently, callback without PR", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-105", "MODE=NOCOMMIT"));

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

    expect(settled.taskRun.state).toBe("COMPLETED");
    expect(settled.taskRun.resultText).toBe("Answered without changes.");
    expect(settled.session.state).toBe("WARM_IDLE");

    // callback still goes out, with no PR to link
    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.kind).toBe("turn-completed");
    expect(payload.pr).toBeNull();

    // nothing pushed, no PR opened, nothing persisted
    expect(git(originDir, "branch", "--list", session.gitBranch)).toBe("");
    expect(forgeCalls.some((c) => c.args.headBranch === session.gitBranch)).toBe(false);
    expect(settled.session.prNumber).toBeNull();
    expect(settled.session.prUrl).toBeNull();
  });

  it("project-level agent override reaches the worker command; the first turn pins it (FUR-41)", async () => {
    const { session, taskRun } = await run(
      setupTurn("FUR-108", "MODE=ARGS", { model: "claude-sonnet-4-5", effort: "low" }),
    );

    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
      }),
    );

    // the fake agent recorded its exact argv (one arg per line) in the worktree
    const argvFile = await readFile(
      path.join(worktreeDir(storageRoot, session.id), "agent-argv.txt"),
      "utf8",
    );
    expect(argvFile).toContain("--model\nclaude-sonnet-4-5\n--effort\nlow");

    const settled = await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        const sessionRepo = yield* SessionRepo;
        return {
          taskRun: yield* taskRunRepo.get(taskRun.id),
          session: yield* sessionRepo.get(session.id),
        };
      }),
    );
    expect(settled.taskRun.state).toBe("COMPLETED");
    // the first turn pinned its resolution — resume turns will keep it
    expect(settled.session.agentModel).toBe("claude-sonnet-4-5");
    expect(settled.session.agentEffort).toBe("low");
  });

  it("absent agent config: worker argv is byte-for-byte today's command (FUR-41)", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-109", "MODE=ARGS"));

    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
      }),
    );

    const argvFile = await readFile(
      path.join(worktreeDir(storageRoot, session.id), "agent-argv.txt"),
      "utf8",
    );
    const prompt = `Ticket FUR-109\n\nMODE=ARGS\n\n${standingOrders({
      branchName: session.gitBranch,
      ticketId: "FUR-109",
    })}`;
    expect(argvFile).toBe(
      `${[
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
      ].join("\n")}\n`,
    );

    // nothing resolved, nothing pinned
    const after = await run(
      Effect.gen(function* () {
        const sessionRepo = yield* SessionRepo;
        return yield* sessionRepo.get(session.id);
      }),
    );
    expect(after.agentModel).toBeNull();
    expect(after.agentEffort).toBeNull();
  });

  it("publish failure after agent success: FAILED with cause ERROR, summary in outbox", async () => {
    const failLayer = makeLayer(120, {
      failWith: new ForgeApiError({ operation: "test", message: "github is down", status: 503 }),
    });
    const runFail = <A, E>(effect: Effect.Effect<A, E, Services | Scope.Scope>): Promise<A> =>
      Effect.runPromise(effect.pipe(Effect.scoped, Effect.provide(failLayer)));

    const { session, taskRun } = await runFail(setupTurn("FUR-106", "Please do the work."));

    // publish failure is an orchestration error: execute settles FAILED, then fails
    const failed = await runFail(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        return yield* executor
          .execute({ taskRunId: taskRun.id, sessionId: session.id })
          .pipe(Effect.flip);
      }),
    );
    expect(failed._tag).toBe("ForgeApiError");

    const settled = await runFail(
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

    // never COMPLETED with a silently missing PR
    expect(settled.taskRun.state).toBe("FAILED");
    expect(settled.taskRun.cause).toBe("ERROR");
    // the failure reason is persisted on the run itself (admin UI reads it
    // straight off the entity — no ticket, no Postgres query)
    expect(settled.taskRun.failureSummary).toContain("publishing failed");
    expect(settled.taskRun.failureSummary).toContain("github is down");
    // the agent's final text is still preserved on the run
    expect(settled.taskRun.resultText).toBe("Committed agent-output.txt.");
    expect(settled.session.state).toBe("WARM_IDLE");
    expect(settled.session.prNumber).toBeNull();

    expect(settled.outbox).toHaveLength(1);
    const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
    expect(payload.kind).toBe("turn-failed");
    expect(payload.cause).toBe("ERROR");
    expect(payload.summary).toContain("publishing failed");
    // one source of truth: the row carries exactly the ticket-comment text
    expect(settled.taskRun.failureSummary).toBe(payload.summary);
    expect(payload.pr).toBeNull();

    // the branch push itself succeeded — a later retry only needs the forge
    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBeTruthy();
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
    // agent failures persist their reason on the run too
    expect(settled.taskRun.failureSummary).toBe("fake agent exploded");
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

  it("origin unreachable on a later turn: base fetch degrades to a warning, agent still executes", async () => {
    const { session, taskRun } = await run(setupTurn("FUR-107", "Please do the work."));

    // turn 1 with origin reachable: clone + worktree + push all succeed
    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: session.id });
        const taskRunRepo = yield* TaskRunRepo;
        expect((yield* taskRunRepo.get(taskRun.id)).state).toBe("COMPLETED");
      }),
    );

    // origin goes dark; the clone and worktree survive
    await rename(originDir, `${originDir}.down`);
    try {
      const taskRun2 = await run(
        Effect.gen(function* () {
          const taskRunRepo = yield* TaskRunRepo;
          return yield* taskRunRepo.create(session.id, taskContext("FUR-107", "MODE=NOCOMMIT"));
        }),
      );
      // publish genuinely needs the remote, so the turn still settles FAILED
      // at the END — the frozen-cache degradation contract is that the base
      // fetch failure alone must never abort PROVISIONING.
      await run(
        Effect.gen(function* () {
          const executor = yield* TurnExecutor;
          yield* executor
            .execute({ taskRunId: taskRun2.id, sessionId: session.id })
            .pipe(Effect.ignore);
        }),
      );
      const settled = await run(
        Effect.gen(function* () {
          const taskRunRepo = yield* TaskRunRepo;
          return {
            taskRun: yield* taskRunRepo.get(taskRun2.id),
            outbox: yield* outboxEntryFor(taskRun2),
          };
        }),
      );
      // provisioning proceeded on the cached base and the agent actually ran:
      // the EXECUTING deadline was set and the agent's final text survived
      expect(settled.taskRun.expiresAt).toBeInstanceOf(Date);
      expect(settled.taskRun.resultText).toBe("Answered without changes.");
      // the only failure is the publish step's dead remote, not the fetch
      expect(settled.taskRun.state).toBe("FAILED");
      const payload = settled.outbox[0]?.payload as TurnOutcomePayload;
      expect(payload.summary).toContain("publishing failed");
    } finally {
      await rename(`${originDir}.down`, originDir);
    }
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
