import type { MaestroEvent } from "@maestro/api";
import { Context, Effect, Layer, PubSub, type Scope } from "effect";

/**
 * In-process pub/sub for every observable state change (Tech Requirements —
 * SSE pipeline). Repos publish entity state changes on successful writes; the
 * turn pipeline publishes log chunks and queue activity; the SSE endpoint
 * subscribes.
 *
 * DECISIONS:
 * - Unbounded PubSub: publishers (repo transition methods) must never be
 *   back-pressured or dropped by a slow SSE consumer. Each subscription
 *   buffers independently; the memory risk of a stalled subscriber is
 *   accepted for the single-process MVP (a stuck SSE client eventually times
 *   out and its subscription is released with the request scope).
 * - Single process only: cross-replica fan-out (LISTEN/NOTIFY) is an M2
 *   concern, out of scope per FUR-16.
 *
 * No `.layerTest`: the live implementation is already pure in-memory.
 */
export class EventBus extends Context.Service<
  EventBus,
  {
    /** Publish an event to all current subscribers. Never fails, never blocks. */
    readonly publish: (event: MaestroEvent) => Effect.Effect<void>;
    /**
     * Subscribe to all events published after this point. Buffering starts at
     * subscription time — subscribe BEFORE reading any snapshot so nothing is
     * missed between snapshot read and live tail.
     */
    readonly subscribe: () => Effect.Effect<PubSub.Subscription<MaestroEvent>, never, Scope.Scope>;
  }
>()("maestro/events/EventBus") {
  static readonly layer = Layer.effect(
    EventBus,
    Effect.gen(function* () {
      const pubsub = yield* PubSub.unbounded<MaestroEvent>();
      return {
        publish: Effect.fn("EventBus.publish")(function* (event: MaestroEvent) {
          yield* PubSub.publish(pubsub, event);
        }),
        subscribe: Effect.fn("EventBus.subscribe")(function* () {
          return yield* PubSub.subscribe(pubsub);
        }),
      };
    }),
  );
}
