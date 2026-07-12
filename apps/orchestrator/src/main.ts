// The single composition root. This is the ONLY file in the codebase that
// imports and wires layer implementations — everything else depends on
// service classes and receives implementations from here.
import { createServer } from "node:http";
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { Effect, Layer, Logger } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { AppConfig } from "./config/AppConfig.ts";
import { Db } from "./db/Db.ts";
import { HealthRoutes } from "./http/health.ts";
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

const AppLive = HttpRouter.serve(HealthRoutes).pipe(
  Layer.merge(WorkerRuntime.layerFromConfig),
  Layer.provide(ServerLive),
  Layer.provide(Db.layer),
  Layer.provide(LoggerLive),
  Layer.provide(AppConfig.layer),
);

NodeRuntime.runMain(Layer.launch(AppLive));
