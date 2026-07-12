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
 */
export const connectEvents = (token: string, store: EventStore): (() => void) => {
  const source = new EventSource(`/api/events?token=${encodeURIComponent(token)}`);

  source.onopen = () => store.resetForSnapshot();
  source.onerror = () => store.setConnection("reconnecting");

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
