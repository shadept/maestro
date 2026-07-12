import { MaestroEvent, MaestroEventFromJsonString } from "@maestro/api";
import { Schema } from "effect";
import type { EventStore } from "./store.ts";

// EventSource wiring for GET /api/events. The server names every frame after
// its tag (`event: SessionStateChanged`), so listeners are registered per tag —
// the tag list is derived from the MaestroEvent union itself, no hand-written
// duplicate. Auth travels as ?token= because EventSource cannot set headers
// (FUR-16 decision). Comment heartbeats are ignored by EventSource natively.

const decodeEvent = Schema.decodeUnknownSync(MaestroEventFromJsonString);

const eventTags: ReadonlyArray<string> = MaestroEvent.members.map(
  (member) => member.fields._tag.ast.literal as string,
);

/**
 * Connects the SSE firehose and feeds decoded events into the store. Returns
 * a disposer. EventSource auto-reconnects; every open (first or re-) triggers
 * `resetForSnapshot` because the server replays a fresh snapshot per
 * subscription.
 *
 * `onAuthRejected` fires when the stream fails permanently. Per spec,
 * EventSource retries transient failures itself (readyState stays CONNECTING)
 * but a non-2xx response — for this endpoint, the 401 token reject — fails
 * the connection for good (readyState CLOSED, no retry). That CLOSED state is
 * the only auth-failure signal EventSource exposes, so it is what we hook to
 * drop a stale stored token instead of spinning on "reconnecting" forever.
 */
export const connectEvents = (
  token: string,
  store: EventStore,
  onAuthRejected?: () => void,
): (() => void) => {
  const source = new EventSource(`/api/events?token=${encodeURIComponent(token)}`);

  source.onopen = () => store.resetForSnapshot();
  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) onAuthRejected?.();
    else store.setConnection("reconnecting");
  };

  const onEvent = (message: MessageEvent<string>): void => {
    try {
      store.apply(decodeEvent(message.data));
    } catch (error) {
      // A malformed frame means a contract drift bug — surface it, keep the tail alive.
      console.error("failed to decode SSE event", error, message.data);
    }
  };
  for (const tag of eventTags) {
    source.addEventListener(tag, onEvent);
  }

  return () => source.close();
};
