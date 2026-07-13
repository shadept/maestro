// The single composition root. This is the ONLY file in the codebase that
// imports and wires layer implementations — everything else depends on
// service classes and receives implementations from here.
import { createServer } from "node:http";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { Context, Deferred, Effect, Layer, Logger, Option, Schedule } from "effect";
import { FetchHttpClient, HttpRouter } from "effect/unstable/http";
import { OtlpMetrics, OtlpSerialization, OtlpTracer } from "effect/unstable/observability";
import { AgentContract } from "./agent/AgentContract.ts";
import { CallbackWorker } from "./callback/CallbackWorker.ts";
import { LinearCallback } from "./callback/LinearCallback.ts";
import { AppConfig } from "./config/AppConfig.ts";
import { loadDotEnv } from "./config/loadDotEnv.ts";
import { AuditRepo } from "./db/AuditRepo.ts";
import { Db } from "./db/Db.ts";
import { DeliveryRepo } from "./db/DeliveryRepo.ts";
import { OutboxRepo } from "./db/OutboxRepo.ts";
import { ProjectRepo } from "./db/ProjectRepo.ts";
import { SessionRepo } from "./db/SessionRepo.ts";
import { TaskRunRepo } from "./db/TaskRunRepo.ts";
import { SessionTerminator } from "./engine/SessionTerminator.ts";
import { StartupReconciler } from "./engine/StartupReconciler.ts";
import { TurnExecutor } from "./engine/TurnExecutor.ts";
import { TurnSettlement } from "./engine/TurnSettlement.ts";
import { EventBus } from "./events/EventBus.ts";
import { GitHubForge } from "./forge/GitHubForge.ts";
import { GitCache } from "./git/GitCache.ts";
import { OutboundGit } from "./git/OutboundGit.ts";
import { RepoLocks } from "./git/RepoLocks.ts";
import { WorktreeManager } from "./git/WorktreeManager.ts";
import { AdminApiRoutes } from "./http/admin.ts";
import { EventsRoutes } from "./http/events.ts";
import { HealthRoutes } from "./http/health.ts";
import { MetricsRoutes } from "./http/metrics.ts";
import { StaticRoutes } from "./http/static.ts";
import { WebhookRoutes } from "./http/webhooks.ts";
import { IngestPipeline } from "./ingest/IngestPipeline.ts";
import { LinearIngest } from "./ingest/LinearIngest.ts";
import * as Metrics from "./observability/metrics.ts";
import { TurnQueue } from "./queue/TurnQueue.ts";
import { WorkerRuntime } from "./runtime/WorkerRuntime.ts";

// Seed process.env from optional .env files (dev convenience) before any
// config resolution. Real environment variables always win; AppConfig below
// stays the only reader/validator of process.env.
loadDotEnv();

const LoggerLive = Layer.unwrap(
  Effect.gen(function* () {
    const { logFormat } = yield* AppConfig;
    return Logger.layer([logFormat === "json" ? Logger.consoleJson : Logger.consolePretty()]);
  }),
);

const ServerLive = Layer.unwrap(
  Effect.gen(function* () {
    const { port } = yield* AppConfig;
    return NodeHttpServer.layer(createServer, { port });
  }),
);

/**
 * OTLP tracing + metrics export (M2.10, Tech Requirements §15): every
 * Effect.fn span already exists (CLAUDE.md convention since M1.1) — this
 * layer only installs the exporter. Layer.empty when MAESTRO_OTLP_ENDPOINT is
 * unset, so an unconfigured deployment pays zero network cost; Effect's
 * native in-memory Tracer still assigns real span/trace ids either way
 * (TurnExecutor persists them on the TaskRun regardless of export).
 */
const TracingLive = Layer.unwrap(
  Effect.gen(function* () {
    const { otlpEndpoint, otlpServiceName } = yield* AppConfig;
    if (Option.isNone(otlpEndpoint)) return Layer.empty;
    const resource = { serviceName: otlpServiceName };
    const endpoint = otlpEndpoint.value;
    return Layer.mergeAll(
      OtlpTracer.layer({ url: `${endpoint}/v1/traces`, resource }),
      OtlpMetrics.layer({ url: `${endpoint}/v1/metrics`, resource }),
    );
  }),
).pipe(Layer.provide(OtlpSerialization.layerJson), Layer.provide(FetchHttpClient.layer));

/** Touches every Metric instrument once so GET /metrics is non-empty from boot. */
const MetricsInitLive = Layer.effectDiscard(Metrics.initialize);

const ReposLive = Layer.mergeAll(
  ProjectRepo.layer,
  SessionRepo.layer,
  TaskRunRepo.layer,
  OutboxRepo.layer,
  DeliveryRepo.layer,
  AuditRepo.layer,
);

/**
 * ONE TurnQueue for the whole process (the FUR-13 single-dispatcher
 * invariant), lazily connected: pg-boss needs a reachable database to start,
 * but boot must never depend on the DB (FUR-8). This proxy layer builds
 * TurnQueue.layer in a background fiber (retrying until Postgres answers)
 * and parks callers on a Deferred until then — the turn worker just waits,
 * and a webhook enqueue can only reach the queue after its own DB writes
 * succeeded, i.e. when the queue is (about to be) up.
 */
const TurnQueueLive = Layer.effect(
  TurnQueue,
  Effect.gen(function* () {
    const ready = yield* Deferred.make<TurnQueue["Service"]>();
    // Resources attach to this layer's scope (inherited by forkScoped), so
    // the queue lives — and stops — with the app.
    yield* Effect.forkScoped(
      Layer.build(TurnQueue.layer).pipe(
        Effect.flatMap((services) => Deferred.succeed(ready, Context.get(services, TurnQueue))),
        Effect.tapError((error) =>
          Effect.logWarning("turn queue failed to start; retrying", error),
        ),
        Effect.retry(Schedule.spaced("5 seconds")),
      ),
    );
    const withQueue = <A, E, R>(
      f: (queue: TurnQueue["Service"]) => Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> => Effect.flatMap(Deferred.await(ready), f);
    return {
      enqueue: (job) => withQueue((queue) => queue.enqueue(job)),
      work: (handler) => withQueue((queue) => queue.work(handler)),
    };
  }),
);

// GitHub is the only M1 forge; a second forge becomes a config-selected layer here.
const GitLive = Layer.mergeAll(GitCache.layer, WorktreeManager.layer, OutboundGit.layer).pipe(
  Layer.provideMerge(GitCache.layer),
  Layer.provide(Layer.mergeAll(RepoLocks.layer, GitHubForge.layer)),
);

// Shared by TurnExecutor (deferred finalize after an in-flight turn settles)
// and IngestPipeline (terminal signal from ingest) — same layer reference, so
// memoization yields one instance.
const SessionTerminatorLive = SessionTerminator.layer.pipe(Layer.provide(GitLive));

// Shared settle path (turn transition + outbox callback + circuit breaker):
// one layer reference for TurnExecutor and StartupReconciler, so memoization
// yields a single instance and the two settle paths cannot drift.
const TurnSettlementLive = TurnSettlement.layer;

const TurnExecutorLive = TurnExecutor.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      AgentContract.layer,
      WorkerRuntime.layerFromConfig,
      GitLive,
      SessionTerminatorLive,
      TurnSettlementLive,
    ),
  ),
  Layer.provideMerge(ReposLive),
);

// Startup reconciliation (FUR-40): settles crash-orphaned turns and re-drives
// interrupted teardowns before any new dispatch (see TurnWorkerLive).
const StartupReconcilerLive = StartupReconciler.layer.pipe(
  Layer.provide(
    Layer.mergeAll(WorkerRuntime.layerFromConfig, SessionTerminatorLive, TurnSettlementLive),
  ),
  Layer.provideMerge(ReposLive),
);

/**
 * Registers TurnExecutor as the TurnQueue handler — in the background. The
 * shared lazy TurnQueue (above) absorbs the wait for the database, so the
 * fork simply parks until pg-boss is up and then registers the dispatcher
 * into this layer's scope; boot (and the /livez//readyz probes, verified in
 * FUR-8) never depends on the DB.
 *
 * Startup reconciliation (FUR-40) runs FIRST, inside the same background
 * fiber: work(...) only registers once reconcile() has succeeded, so no new
 * dispatch can race the orphan sweep. The reconcile retry keeps the
 * boot-without-DB invariant — a dead database just means the whole fiber
 * (reconcile, then worker registration) keeps retrying in the background
 * while /livez stays green. reconcile() is idempotent, so retrying a
 * partially-applied sweep is safe.
 */
const TurnWorkerLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const reconciler = yield* StartupReconciler;
    const executor = yield* TurnExecutor;
    const queue = yield* TurnQueue;
    yield* Effect.forkScoped(
      reconciler.reconcile().pipe(
        Effect.tapError((error) =>
          Effect.logWarning("startup reconciliation failed; retrying", error),
        ),
        Effect.retry(Schedule.spaced("5 seconds")),
        Effect.andThen(Effect.logInfo("startup reconciliation complete; registering turn worker")),
        Effect.andThen(queue.work(executor.execute)),
      ),
    );
  }),
).pipe(Layer.provide(Layer.mergeAll(TurnExecutorLive, StartupReconcilerLive)));

/**
 * Drains the callback outbox: turn results become ticket comments (FUR-18).
 * A plain polling fiber (pg-boss buys nothing here — the outbox table with
 * its persisted next_attempt_at backoff already is the queue); transient
 * failures are logged and the next tick retries.
 */
const CALLBACK_POLL_MILLIS = 1_000;
const CallbackWorkerLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const worker = yield* CallbackWorker;
    yield* Effect.forkScoped(
      worker.drainOnce().pipe(
        Effect.catch((error) => Effect.logWarning("callback drain failed", error)),
        Effect.andThen(Effect.sleep(CALLBACK_POLL_MILLIS)),
        Effect.forever,
      ),
    );
  }),
).pipe(Layer.provide(CallbackWorker.layer), Layer.provide(LinearCallback.layer));

// Linear webhook ingestion (FUR-18): the Linear adapter maps deliveries into
// the forge-agnostic pipeline (sessions/turns/queue). M2's generic REST API
// plugs a second adapter into the same IngestPipeline. LinearCallback (same
// layer reference as CallbackWorkerLive's — memoized to one instance) backs
// the FUR-37 delegation lookup for session-less mentions.
const IngestLive = LinearIngest.layer.pipe(
  Layer.provideMerge(IngestPipeline.layer),
  Layer.provide(SessionTerminatorLive),
  Layer.provide(LinearCallback.layer),
);

// Health probes, the SSE firehose, the admin read API (FUR-16), the admin UI
// bundle at `/` (FUR-17), the Linear webhook endpoint (FUR-18), and the
// Prometheus scrape endpoint (M2.10). Handlers pull repos + EventBus + ingest
// services from the shared layers below.
const HttpRoutes = Layer.mergeAll(
  HealthRoutes,
  EventsRoutes,
  AdminApiRoutes,
  StaticRoutes,
  WebhookRoutes,
  MetricsRoutes,
);

const AppLive = HttpRouter.serve(HttpRoutes).pipe(
  Layer.merge(TurnWorkerLive),
  Layer.merge(CallbackWorkerLive),
  Layer.merge(MetricsInitLive),
  Layer.provide(ServerLive),
  Layer.provide(IngestLive),
  Layer.provide(TurnQueueLive),
  Layer.provide(ReposLive),
  // One EventBus for the whole process: repos, queue, executor, and the SSE
  // endpoint all see the same instance (layer memoization by reference).
  Layer.provide(EventBus.layer),
  Layer.provide(Db.layer),
  Layer.provide(LoggerLive),
  Layer.provide(TracingLive),
  Layer.provide(AppConfig.layer),
);

NodeRuntime.runMain(Layer.launch(AppLive));
