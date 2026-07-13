import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Layer, Metric } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import { describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { MetricsRoutes } from "../../src/http/metrics.ts";

// AppConfig.layerTest never touches the environment or a database, so — like
// health.test.ts's HttpRouter.serve pattern — this suite needs neither
// Postgres nor docker. A metric touched via the process-global default
// MetricRegistry (not one of src/observability/metrics.ts's singletons, to
// stay independent of what other test files in this worker have registered)
// proves the route actually renders live registry state, not a canned body.
const probe = Metric.gauge("test_http_metrics_probe");

const withServer = <A>(
  configLayer: Layer.Layer<AppConfig>,
  f: (client: HttpClient.HttpClient) => Effect.Effect<A, unknown, HttpClient.HttpClient>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      yield* Metric.update(probe, 42);
      const client = yield* HttpClient.HttpClient;
      return yield* f(client);
    }).pipe(
      Effect.provide(
        HttpRouter.serve(MetricsRoutes, { disableLogger: true, disableListenLog: true }).pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
          Layer.provide(configLayer),
        ),
      ),
    ),
  );

describe("GET /metrics", () => {
  it("serves Prometheus text format when enabled", async () => {
    const response = await withServer(
      AppConfig.layerTest({ prometheusMetricsEnabled: true }),
      (client) => client.get("/metrics"),
    );
    expect(response.status).toBe(200);
    const body = await Effect.runPromise(response.text);
    expect(body).toContain("test_http_metrics_probe 42");
  });

  it("404s when disabled by config", async () => {
    const status = await withServer(
      AppConfig.layerTest({ prometheusMetricsEnabled: false }),
      (client) => client.get("/metrics").pipe(Effect.map((res) => res.status)),
    );
    expect(status).toBe(404);
  });
});
