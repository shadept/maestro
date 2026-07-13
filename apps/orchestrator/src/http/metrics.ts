import { Effect, Layer } from "effect";
import { PrometheusMetrics } from "effect/unstable/observability";
import { AppConfig } from "../config/AppConfig.ts";

// GET /metrics: Prometheus text-format scrape endpoint (Tech Requirements
// §15, M2.10) over every Effect Metric instrument. Unauthenticated, same as
// /livez and /readyz — pull-based and harmless until something scrapes it.
// Layer.empty when MAESTRO_PROMETHEUS_METRICS_ENABLED is off.

export const MetricsRoutes = Layer.unwrap(
  Effect.gen(function* () {
    const { prometheusMetricsEnabled } = yield* AppConfig;
    return prometheusMetricsEnabled ? PrometheusMetrics.layerHttp() : Layer.empty;
  }),
);
