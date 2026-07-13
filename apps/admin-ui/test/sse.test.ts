import { createRoot } from "solid-js";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  connectEvents,
  type EventSourceLike,
  type ProbeVerdict,
  type SseDeps,
} from "../src/sse.ts";
import { createEventStore, type EventStore } from "../src/store.ts";

// The SSE reconnect supervisor (fix: permanent EventSource CLOSED must not be
// treated as an auth reject). Decision logic is tested against injected fakes —
// EventSource factory, auth probe, and scheduler — following the token-storage
// stub pattern; no real browser EventSource involved.

class FakeSource implements EventSourceLike {
  readyState = 0;
  onopen: ((event: Event) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  closed = false;
  addEventListener(_type: string, _listener: (event: MessageEvent<string>) => void): void {}
  close(): void {
    this.closed = true;
  }
  emitOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }
  /** A completed non-SSE response: readyState CLOSED, no native retry. */
  emitFatal(): void {
    this.readyState = 2;
    this.onerror?.(new Event("error"));
  }
  /** A network-level failure: native retry keeps readyState CONNECTING. */
  emitTransient(): void {
    this.readyState = 0;
    this.onerror?.(new Event("error"));
  }
}

interface Timer {
  readonly fn: () => void;
  readonly delayMs: number;
  cancelled: boolean;
}

const makeHarness = (verdict: ProbeVerdict | ((call: number) => Promise<ProbeVerdict>)) => {
  const sources: FakeSource[] = [];
  const timers: Timer[] = [];
  const probedTokens: string[] = [];
  const deps: SseDeps = {
    createSource: () => {
      const source = new FakeSource();
      sources.push(source);
      return source;
    },
    probeAuth: (token) => {
      probedTokens.push(token);
      return typeof verdict === "function"
        ? verdict(probedTokens.length)
        : Promise.resolve(verdict);
    },
    schedule: (fn, delayMs) => {
      const timer: Timer = { fn, delayMs, cancelled: false };
      timers.push(timer);
      return () => {
        timer.cancelled = true;
      };
    },
  };
  return { sources, timers, probedTokens, deps };
};

/** Let the probeAuth promise chain settle. */
const flush = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0));

const withStore = (f: (store: EventStore) => Promise<void>): Promise<void> =>
  createRoot(async (dispose) => {
    try {
      await f(createEventStore());
    } finally {
      dispose();
    }
  });

afterEach(() => vi.unstubAllGlobals());

describe("SSE reconnect supervisor", () => {
  it("CLOSED + probe 401 → auth-rejected path, no retry scheduled", () =>
    withStore(async (store) => {
      const { sources, timers, probedTokens, deps } = makeHarness("unauthorized");
      const onAuthRejected = vi.fn();
      connectEvents("tok", store, onAuthRejected, deps);

      sources[0]?.emitFatal();
      await flush();

      expect(probedTokens).toEqual(["tok"]);
      expect(onAuthRejected).toHaveBeenCalledTimes(1);
      expect(timers).toHaveLength(0);
      expect(sources).toHaveLength(1);
    }));

  it("CLOSED + probe non-401 → keeps the token, schedules a retry, rebuilds the source", () =>
    withStore(async (store) => {
      const { sources, timers, deps } = makeHarness("retry");
      const onAuthRejected = vi.fn();
      connectEvents("tok", store, onAuthRejected, deps);

      sources[0]?.emitFatal();
      await flush();

      expect(onAuthRejected).not.toHaveBeenCalled();
      expect(store.connection()).toBe("reconnecting");
      expect(store.retry()).toEqual({ attempt: 1, delayMs: 1000 });
      expect(timers[0]?.delayMs).toBe(1000);

      timers[0]?.fn();
      expect(sources).toHaveLength(2);
    }));

  it("backoff doubles per attempt and caps at 30s", () =>
    withStore(async (store) => {
      const { sources, timers, deps } = makeHarness("retry");
      connectEvents("tok", store, undefined, deps);

      for (let round = 0; round < 7; round += 1) {
        sources.at(-1)?.emitFatal();
        await flush();
        timers.at(-1)?.fn();
      }

      expect(timers.map((timer) => timer.delayMs)).toEqual([
        1000, 2000, 4000, 8000, 16000, 30000, 30000,
      ]);
    }));

  it("successful reconnect resets the store, the retry hint, and the backoff", () =>
    withStore(async (store) => {
      const { sources, timers, deps } = makeHarness("retry");
      connectEvents("tok", store, undefined, deps);

      sources[0]?.emitFatal();
      await flush();
      timers[0]?.fn();
      sources[1]?.emitOpen();

      expect(store.connection()).toBe("open");
      expect(store.retry()).toBeNull();

      // A later outage starts the backoff over at 1s, not where it left off.
      sources[1]?.emitFatal();
      await flush();
      expect(store.retry()).toEqual({ attempt: 1, delayMs: 1000 });
      expect(timers[1]?.delayMs).toBe(1000);
    }));

  it("transient CONNECTING error → native retry, no probe, no supervisor timer", () =>
    withStore(async (store) => {
      const { sources, timers, probedTokens, deps } = makeHarness("retry");
      connectEvents("tok", store, undefined, deps);

      sources[0]?.emitTransient();
      await flush();

      expect(store.connection()).toBe("reconnecting");
      expect(probedTokens).toHaveLength(0);
      expect(timers).toHaveLength(0);
    }));

  it("duplicate CLOSED errors while recovering do not double-probe or double-schedule", () =>
    withStore(async (store) => {
      const { sources, timers, probedTokens, deps } = makeHarness("retry");
      connectEvents("tok", store, undefined, deps);

      sources[0]?.emitFatal();
      sources[0]?.emitFatal();
      await flush();
      sources[0]?.emitFatal();
      await flush();

      expect(probedTokens).toHaveLength(1);
      expect(timers).toHaveLength(1);
    }));

  it("dispose cancels the pending retry and closes the source", () =>
    withStore(async (store) => {
      const { sources, timers, deps } = makeHarness("retry");
      const disconnect = connectEvents("tok", store, undefined, deps);

      sources[0]?.emitFatal();
      await flush();
      disconnect();

      expect(timers[0]?.cancelled).toBe(true);
      expect(sources[0]?.closed).toBe(true);
    }));

  it("a probe verdict landing after dispose is ignored", () =>
    withStore(async (store) => {
      let resolveProbe: ((verdict: ProbeVerdict) => void) | undefined;
      const { sources, timers, deps } = makeHarness(
        () => new Promise((resolve) => (resolveProbe = resolve)),
      );
      const onAuthRejected = vi.fn();
      const disconnect = connectEvents("tok", store, onAuthRejected, deps);

      sources[0]?.emitFatal();
      disconnect();
      resolveProbe?.("unauthorized");
      await flush();

      expect(onAuthRejected).not.toHaveBeenCalled();
      expect(timers).toHaveLength(0);
    }));

  it("tab becoming visible skips the remaining backoff wait", () =>
    withStore(async (store) => {
      let onVisibilityChange: (() => void) | undefined;
      vi.stubGlobal("document", {
        visibilityState: "visible",
        addEventListener: (_type: string, listener: () => void) => {
          onVisibilityChange = listener;
        },
        removeEventListener: () => {},
      });

      const { sources, timers, deps } = makeHarness("retry");
      connectEvents("tok", store, undefined, deps);

      sources[0]?.emitFatal();
      await flush();
      expect(timers[0]?.cancelled).toBe(false);

      onVisibilityChange?.();
      expect(timers[0]?.cancelled).toBe(true);
      expect(sources).toHaveLength(2);
    }));
});
