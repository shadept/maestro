import { Duration, Effect, Metric } from "effect";

// Effect Metric instruments (Tech Requirements §15, M2.10). A Metric registers
// lazily in the process's MetricRegistry on first touch, so `initialize` runs
// once at boot to guarantee every instrument below is visible on GET /metrics
// immediately — even one whose producing event has never fired.

/** In-flight turn workers, out of AppConfig.maxConcurrentWorkers. */
export const poolOccupancy = Metric.gauge("maestro_pool_occupancy", {
  description: "In-flight turn workers, out of the configured concurrency cap",
});

/** Turns admitted to the queue but not yet dispatched to a worker slot. */
export const queueDepth = Metric.gauge("maestro_queue_depth", {
  description: "Turns waiting for a free worker slot",
});

/** Wall-clock duration of one TurnExecutor.runTurn pass (any outcome). */
export const turnDuration = Metric.timer("maestro_turn_duration", {
  description: "Wall-clock duration of one turn execution",
});

/**
 * Reserved: no orchestrator mechanism preempts a running turn yet. Registered
 * now so dashboards/alerts have a stable name ahead of the producing feature.
 */
export const preemptionCount = Metric.counter("maestro_preemption_total", {
  description: "Turns preempted before completion",
});

/**
 * Reserved: TaskRunCause carries RATE_LIMIT but nothing classifies it yet
 * (see packages/domain/src/TaskRun.ts) — same status as preemptionCount.
 */
export const rateLimitSuspensionCount = Metric.counter("maestro_rate_limit_suspension_total", {
  description: "Turns suspended by an upstream rate limit",
});

/** Age of the oldest PENDING callback outbox row; 0 when the outbox is empty. */
export const callbackOutboxLag = Metric.gauge("maestro_callback_outbox_lag_ms", {
  description: "Age of the oldest pending callback outbox entry, in milliseconds",
});

/** Touches every instrument once so each appears in a snapshot before any traffic. */
export const initialize: Effect.Effect<void> = Effect.all(
  [
    Metric.update(poolOccupancy, 0),
    Metric.update(queueDepth, 0),
    Metric.update(turnDuration, Duration.zero),
    Metric.update(preemptionCount, 0),
    Metric.update(rateLimitSuspensionCount, 0),
    Metric.update(callbackOutboxLag, 0),
  ],
  { discard: true },
);
