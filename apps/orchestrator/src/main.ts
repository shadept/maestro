// The single composition root. This is the ONLY file in the codebase that
// imports and wires layer implementations — everything else depends on
// service classes and receives implementations from here.
import { createServer } from "node:http";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { Effect, Layer, Logger, Schedule } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { AgentContract } from "./agent/AgentContract.ts";
import { AppConfig } from "./config/AppConfig.ts";
import { Db } from "./db/Db.ts";
import { OutboxRepo } from "./db/OutboxRepo.ts";
import { ProjectRepo } from "./db/ProjectRepo.ts";
import { SessionRepo } from "./db/SessionRepo.ts";
import { TaskRunRepo } from "./db/TaskRunRepo.ts";
import { TurnExecutor } from "./engine/TurnExecutor.ts";
import { GitCache } from "./git/GitCache.ts";
import { RepoLocks } from "./git/RepoLocks.ts";
import { WorktreeManager } from "./git/WorktreeManager.ts";
import { HealthRoutes } from "./http/health.ts";
import { TurnQueue } from "./queue/TurnQueue.ts";
import { WorkerRuntime } from "./runtime/WorkerRuntime.ts";

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

const ReposLive = Layer.mergeAll(
  ProjectRepo.layer,
  SessionRepo.layer,
  TaskRunRepo.layer,
  OutboxRepo.layer,
);

const GitLive = Layer.mergeAll(GitCache.layer, WorktreeManager.layer).pipe(
  Layer.provideMerge(GitCache.layer),
  Layer.provide(RepoLocks.layer),
);

const TurnExecutorLive = TurnExecutor.layer.pipe(
  Layer.provide(Layer.mergeAll(AgentContract.layer, WorkerRuntime.layerFromConfig, GitLive)),
  Layer.provideMerge(ReposLive),
);

/**
 * Registers TurnExecutor as the TurnQueue handler — in the background.
 * TurnQueue's pg-boss needs a reachable database to start, but boot (and the
 * /livez//readyz probes, verified in FUR-8) must never depend on the DB:
 * the worker is forked as a daemon that retries queue startup until the
 * database answers, instead of failing the whole layer build.
 */
const TurnWorkerLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const executor = yield* TurnExecutor;
    const runWorker = Effect.gen(function* () {
      const queue = yield* TurnQueue;
      yield* queue.work(executor.execute);
      yield* Effect.never; // hold the queue + worker scope for the app lifetime
    }).pipe(Effect.provide(TurnQueue.layer), Effect.scoped);
    yield* Effect.forkScoped(
      runWorker.pipe(
        Effect.tapError((error) =>
          Effect.logWarning("turn worker failed to start; retrying", error),
        ),
        Effect.retry(Schedule.spaced("5 seconds")),
      ),
    );
  }),
).pipe(Layer.provide(TurnExecutorLive));

const AppLive = HttpRouter.serve(HealthRoutes).pipe(
  Layer.merge(TurnWorkerLive),
  Layer.provide(ServerLive),
  Layer.provide(Db.layer),
  Layer.provide(LoggerLive),
  Layer.provide(AppConfig.layer),
);

NodeRuntime.runMain(Layer.launch(AppLive));
