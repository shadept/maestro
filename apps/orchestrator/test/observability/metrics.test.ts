import { Duration, Effect, Metric } from "effect";
import { PrometheusMetrics } from "effect/unstable/observability";
import { describe, expect, it } from "vitest";

import * as Metrics from "../../src/observability/metrics.ts";

// A Metric object caches its registry hooks on first touch (Metric$#metadata
// in effect's internals) — every later touch of that same object reuses
// whichever MetricRegistry it first registered into, regardless of what's in
// context at the time. Since Metrics.* are module-level singletons shared by
// every test in this file, all assertions about them must run inside ONE
// Effect.provideService(Metric.MetricRegistry, ...) call with a fresh Map —
// splitting them across separate isolated() calls would silently read back
// an empty snapshot for every call after the first.
const isolated = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provideService(Metric.MetricRegistry, new Map())));

describe("observability metrics", () => {
  it("registers, zeroes, and updates every M2.10 instrument", async () => {
    const [beforeUpdate, afterUpdate] = await isolated(
      Effect.gen(function* () {
        yield* Metrics.initialize;
        const before = yield* PrometheusMetrics.format();

        yield* Metric.update(Metrics.poolOccupancy, 2);
        yield* Metric.update(Metrics.poolOccupancy, 1);
        yield* Metric.update(Metrics.queueDepth, 5);
        yield* Metric.update(Metrics.turnDuration, Duration.seconds(2));
        yield* Metric.update(Metrics.preemptionCount, 1);
        yield* Metric.update(Metrics.preemptionCount, 1);
        const after = yield* PrometheusMetrics.format();

        return [before, after] as const;
      }),
    );

    // every instrument is visible before any traffic, all at zero
    expect(beforeUpdate).toContain("maestro_pool_occupancy 0");
    expect(beforeUpdate).toContain("maestro_queue_depth 0");
    expect(beforeUpdate).toContain("maestro_preemption_total 0");
    expect(beforeUpdate).toContain("maestro_rate_limit_suspension_total 0");
    expect(beforeUpdate).toContain("maestro_callback_outbox_lag_ms 0");
    expect(beforeUpdate).toContain("# TYPE maestro_turn_duration histogram");

    // gauges reflect the latest update, not an accumulation
    expect(afterUpdate).toContain("maestro_pool_occupancy 1");
    expect(afterUpdate).toContain("maestro_queue_depth 5");
    // the turn duration timer bucketed the recorded duration, on top of
    // initialize's own zero-duration touch (count 2: the touch, then this one)
    expect(afterUpdate).toContain("maestro_turn_duration_count 2");
    expect(afterUpdate).toContain("maestro_turn_duration_sum 2000");
    // counters accumulate across updates
    expect(afterUpdate).toContain("maestro_preemption_total 2");
  });
});
