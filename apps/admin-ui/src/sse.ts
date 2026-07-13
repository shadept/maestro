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

/** EventSource.readyState values, inlined so the supervisor runs in node tests. */
const CONNECTING = 0;
const CLOSED = 2;

const INITIAL_RETRY_MS = 1_000;
const MAX_RETRY_MS = 30_000;

/** The slice of EventSource the supervisor touches — substitutable in tests. */
export interface EventSourceLike {
  readonly readyState: number;
  onopen: ((event: Event) => void) | null;
  onerror: ((event: Event) => void) | null;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}

/** What a probe of the REST API concluded about a permanently CLOSED stream. */
export type ProbeVerdict = "unauthorized" | "retry";

export interface SseDeps {
  readonly createSource: (url: string) => EventSourceLike;
  /** Cheap authenticated GET against the REST API to disambiguate a CLOSED stream. */
  readonly probeAuth: (token: string) => Promise<ProbeVerdict>;
  /** setTimeout-shaped scheduler; returns a cancel function. */
  readonly schedule: (fn: () => void, delayMs: number) => () => void;
}

/**
 * Only a 401 means the token is bad. Anything else — 5xx during an
 * orchestrator boot window, a tunnel/proxy error page, the network being down
 * (fetch throws) — is a transient outage worth retrying forever.
 */
const probeAuth = async (token: string): Promise<ProbeVerdict> => {
  try {
    const response = await fetch("/api/sessions", {
      headers: { authorization: `Bearer ${token}` },
    });
    return response.status === 401 ? "unauthorized" : "retry";
  } catch {
    return "retry";
  }
};

const defaultDeps: SseDeps = {
  createSource: (url) => new EventSource(url),
  probeAuth,
  schedule: (fn, delayMs) => {
    const id = setTimeout(fn, delayMs);
    return () => clearTimeout(id);
  },
};

/**
 * Connects the SSE firehose and feeds decoded events into the store. Returns
 * a disposer. Every open (first or re-) triggers `resetForSnapshot` because
 * the server replays a fresh snapshot per subscription.
 *
 * Reconnect supervision: native EventSource auto-retry only covers
 * network-level failures (readyState stays CONNECTING). Any COMPLETED
 * response that is not a 200 SSE stream — the 401 token reject, the 503 the
 * endpoint returns while the DB snapshot read fails during boot, a proxy
 * error page — permanently CLOSEs the source. CLOSED alone is therefore NOT
 * an auth signal: the supervisor probes the REST API with the token and only
 * a confirmed 401 fires `onAuthRejected` (clear token, back to the gate);
 * every other verdict rebuilds the EventSource with capped exponential
 * backoff, forever — an admin page should outlive long outages.
 */
export const connectEvents = (
  token: string,
  store: EventStore,
  onAuthRejected?: () => void,
  deps: SseDeps = defaultDeps,
): (() => void) => {
  let disposed = false;
  let attempt = 0;
  let cancelRetry: (() => void) | undefined;
  /** True while a probe is in flight or a retry is pending — dedupes error bursts. */
  let recovering = false;
  let source: EventSourceLike;

  const onEvent = (message: MessageEvent<string>): void => {
    try {
      store.apply(decodeEvent(message.data));
    } catch (error) {
      // A malformed frame means a contract drift bug — surface it, keep the tail alive.
      console.error("failed to decode SSE event", error, message.data);
    }
  };

  const scheduleReconnect = (): void => {
    attempt += 1;
    const delayMs = Math.min(INITIAL_RETRY_MS * 2 ** (attempt - 1), MAX_RETRY_MS);
    store.setRetry({ attempt, delayMs });
    cancelRetry = deps.schedule(() => {
      cancelRetry = undefined;
      recovering = false;
      open();
    }, delayMs);
  };

  const handleClosed = (): void => {
    if (disposed || recovering) return;
    recovering = true;
    store.setConnection("reconnecting");
    void deps.probeAuth(token).then((verdict) => {
      if (disposed) return;
      if (verdict === "unauthorized") onAuthRejected?.();
      else scheduleReconnect();
    });
  };

  const open = (): void => {
    source = deps.createSource(`/api/events?token=${encodeURIComponent(token)}`);
    source.onopen = () => {
      attempt = 0;
      store.resetForSnapshot();
    };
    source.onerror = () => {
      if (source.readyState === CLOSED) handleClosed();
      else if (source.readyState === CONNECTING) store.setConnection("reconnecting");
    };
    for (const tag of eventTags) {
      source.addEventListener(tag, onEvent);
    }
  };

  open();

  // Background tabs throttle timers; on refocus, skip the remaining wait if a
  // retry is pending so the operator sees fresh state immediately.
  const onVisibilityChange = (): void => {
    if (document.visibilityState !== "visible" || cancelRetry === undefined) return;
    cancelRetry();
    cancelRetry = undefined;
    recovering = false;
    open();
  };
  if (typeof document !== "undefined") {
    document.addEventListener("visibilitychange", onVisibilityChange);
  }

  return () => {
    disposed = true;
    cancelRetry?.();
    cancelRetry = undefined;
    if (typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", onVisibilityChange);
    }
    source.close();
  };
};
