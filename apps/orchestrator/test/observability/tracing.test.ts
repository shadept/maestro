import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { Duration, Effect, Layer, Option } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { OtlpSerialization, OtlpTracer } from "effect/unstable/observability";
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
import { TurnSettlement } from "../../src/engine/TurnSettlement.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { GitHubForge } from "../../src/forge/GitHubForge.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { OutboundGit } from "../../src/git/OutboundGit.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { branchNameFor, WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { WorkerRuntime } from "../../src/runtime/WorkerRuntime.ts";
import {
  buildFakeAgentImage,
  cleanStorageViaContainer,
  FAKE_AGENT_IMAGE,
  fakeAgentRuntimeTemplate,
} from "../support/fake-agent.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// M2.10 acceptance criterion, verified end to end: a full fake turn produces
// one root span ("TurnExecutor.execute", the Effect.fn span TurnQueue's
// dispatcher would otherwise call into) with nested service spans exported
// over OTLP, and the root span's trace id is persisted on the TaskRun row. A
// tiny local HTTP server stands in for the "in-memory/collector-stub
// exporter" the ticket asks for. executor.execute is invoked directly (the
// same pattern turn-executor.test.ts uses for most of its cases) — the queue
// is just a dispatcher in front of this same call, not part of what's tested.

interface OtlpSpan {
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId?: string;
  readonly name: string;
}

interface TraceData {
  readonly resourceSpans: ReadonlyArray<{
    readonly scopeSpans: ReadonlyArray<{ readonly spans: ReadonlyArray<OtlpSpan> }>;
  }>;
}

const startCollector = (): Promise<{
  readonly url: string;
  readonly server: Server;
  readonly spans: () => OtlpSpan[];
}> => {
  const bodies: TraceData[] = [];
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        try {
          bodies.push(JSON.parse(Buffer.concat(chunks).toString("utf8")) as TraceData);
        } catch {
          // malformed body — ignore, the assertions below will just see fewer spans
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end("{}");
      });
    });
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${port}`,
        server,
        spans: () =>
          bodies.flatMap((b) =>
            b.resourceSpans.flatMap((r) => r.scopeSpans.flatMap((s) => s.spans)),
          ),
      });
    });
  });
};

type Services = TurnExecutor | ProjectRepo | SessionRepo | TaskRunRepo | OutboxRepo | EventBus;

let testDb: TestDb;
let root: string;
let storageRoot: string;
let originDir: string;
let collector: Awaited<ReturnType<typeof startCollector>>;

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

beforeAll(async () => {
  testDb = await startTestDb();
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-tracing-")));
  storageRoot = path.join(root, "storage");
  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");
  await mkdir(storageRoot, { recursive: true });

  buildFakeAgentImage();
  collector = await startCollector();
});

afterAll(async () => {
  collector.server.close();
  cleanStorageViaContainer(root, storageRoot);
  await rm(root, { recursive: true, force: true });
  await testDb.stop();
});

const makeLayer = (): Layer.Layer<Services> => {
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
      Layer.mergeAll(
        AgentContract.layer,
        WorkerRuntime.layerLocalCli,
        gitLayer,
        terminator,
        TurnSettlement.layer,
      ),
    ),
  );
  // The tracer under test: real OTLP/JSON export to the local collector, with
  // a short interval so the test does not have to wait out a production
  // batching window.
  const tracerLive = OtlpTracer.layer({
    url: `${collector.url}/v1/traces`,
    // Without an explicit resource, OtlpResource.fromConfig hard-requires the
    // OTEL_SERVICE_NAME env var (main.ts always passes one).
    resource: { serviceName: "maestro-orchestrator-test" },
    exportInterval: Duration.millis(50),
    maxBatchSize: 500,
  }).pipe(Layer.provide(OtlpSerialization.layerJson), Layer.provide(FetchHttpClient.layer));

  return executor.pipe(
    Layer.provideMerge(repos),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.provideMerge(EventBus.layer),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: testDb.connectionString,
        storageRoot,
        workerImage: FAKE_AGENT_IMAGE,
        runtimeTemplate: fakeAgentRuntimeTemplate(),
        turnTimeoutSeconds: 120,
        maxConcurrentWorkers: 2,
        githubToken: Option.none(),
      }),
    ),
    // provideMerge, not provide: the tracer must be in the OUTPUT context so the
    // effects run at test time (executor.execute) create spans with it — plain
    // provide would only expose it to service construction, and runtime spans
    // would fall back to the no-op NativeSpan tracer.
    Layer.provideMerge(tracerLive),
    Layer.orDie,
  );
};

describe("OTLP tracing (M2.10)", () => {
  it("a full turn produces one root span with nested service spans; the trace id lands on the TaskRun", async () => {
    const layer = makeLayer();
    const run = <A, E>(effect: Effect.Effect<A, E, Services>): Promise<A> =>
      Effect.runPromise(Effect.provide(effect, layer));

    const { taskRun } = await run(
      Effect.gen(function* () {
        const projectRepo = yield* ProjectRepo;
        const sessionRepo = yield* SessionRepo;
        const taskRunRepo = yield* TaskRunRepo;
        const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
        const ticketReference = { source: "linear" as const, externalId: "FUR-110" };
        const session = yield* sessionRepo.create({
          projectId: project.id,
          ticketReference,
          gitBranch: branchNameFor(ticketReference, project),
        });
        const taskRun = yield* taskRunRepo.create(session.id, {
          source: "linear",
          ticket: ticketReference,
          actor: "shade",
          title: "Ticket FUR-110",
          body: "MODE=NOCOMMIT",
          deliveryId: "d-FUR-110",
          payload: {},
        });
        return { session, taskRun };
      }),
    );

    await run(
      Effect.gen(function* () {
        const executor = yield* TurnExecutor;
        yield* executor.execute({ taskRunId: taskRun.id, sessionId: taskRun.sessionId });
      }),
    );

    // The exporter posts on its own 50ms schedule — poll rather than assume
    // one interval was enough for every span the turn produced.
    const deadline = Date.now() + 10_000;
    let rootSpan: OtlpSpan | undefined;
    while (Date.now() < deadline) {
      rootSpan = collector.spans().find((s) => s.name === "TurnExecutor.execute");
      if (rootSpan) break;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(rootSpan).toBeDefined();
    // biome-ignore lint/style/noNonNullAssertion: asserted defined above
    const turnRootSpan = rootSpan!;
    expect(turnRootSpan.parentSpanId).toBeUndefined();

    // give one more export cycle for any spans still in flight, then collect everything
    await new Promise((resolve) => setTimeout(resolve, 300));
    const allSpans = collector.spans();
    const sameTrace = allSpans.filter((s) => s.traceId === turnRootSpan.traceId);
    const nestedUnderRoot = sameTrace.filter((s) => s.spanId !== turnRootSpan.spanId);
    expect(nestedUnderRoot.length).toBeGreaterThan(0);
    // every Effect.fn span in TurnExecutor.execute's call tree carries the
    // same trace id — a direct child of the root span must be among them
    expect(nestedUnderRoot.some((s) => s.parentSpanId === turnRootSpan.spanId)).toBe(true);

    const settled = await run(
      Effect.gen(function* () {
        const taskRunRepo = yield* TaskRunRepo;
        return yield* taskRunRepo.get(taskRun.id);
      }),
    );
    expect(settled.state).toBe("COMPLETED");
    expect(settled.traceId).toBe(turnRootSpan.traceId);
  }, 30_000);
});
