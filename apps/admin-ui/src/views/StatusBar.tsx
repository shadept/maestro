import { Show } from "solid-js";
import type { EventStore } from "../store.ts";

// System status bar: SSE connection state plus the latest SystemStatus (which
// arrives once per subscription in M1 — snapshot-only) with the live active
// turn count layered on top from QueueChanged events.

export const StatusBar = (props: { store: EventStore }) => {
  const activeTurns = () => props.store.activeTurns() ?? props.store.systemStatus()?.activeTurns;

  return (
    <header class="status-bar">
      <a href="#/" class="brand">
        Maestro
      </a>
      <span class={`chip connection-${props.store.connection()}`}>{props.store.connection()}</span>
      <Show
        when={props.store.systemStatus()}
        fallback={<span class="muted">awaiting status…</span>}
      >
        {(status) => (
          <>
            <span>
              turns {activeTurns()}/{status().maxConcurrentWorkers}
            </span>
            <span class={status().dbReachable ? "chip ok" : "chip bad"}>
              db {status().dbReachable ? "up" : "down"}
            </span>
          </>
        )}
      </Show>
    </header>
  );
};
