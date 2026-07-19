import { Show } from "solid-js";
import type { Notifier } from "../notifications.ts";
import type { EventStore } from "../store.ts";

// System status bar: SSE connection state plus the latest SystemStatus (which
// arrives once per subscription in M1 — snapshot-only) with the live active
// turn count layered on top from QueueChanged events. Also hosts the opt-in
// browser-notification toggle and the log-out affordance — the manual escape
// hatch for a wrong persisted token.

export const StatusBar = (props: {
  store: EventStore;
  notifier: Notifier;
  onLogout: () => void;
}) => {
  const activeTurns = () => props.store.activeTurns() ?? props.store.systemStatus()?.activeTurns;

  // "reconnecting (retry 3 in ≤8s)" while the SSE supervisor waits out backoff.
  const connectionLabel = () => {
    const retry = props.store.retry();
    return retry === null
      ? props.store.connection()
      : `${props.store.connection()} (retry ${retry.attempt} in ≤${Math.ceil(retry.delayMs / 1000)}s)`;
  };

  return (
    <header class="status-bar">
      <a href="#/" class="brand">
        Maestro
      </a>
      <span class={`chip connection-${props.store.connection()}`}>{connectionLabel()}</span>
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
      <Show when={props.notifier.supported}>
        <button
          type="button"
          class="notify-toggle"
          aria-pressed={props.notifier.enabled() ? "true" : "false"}
          title={
            props.notifier.permission() === "denied"
              ? "Browser notifications are blocked — allow them in the browser's site settings"
              : "Notify on turn start and completion (success or failure)"
          }
          onClick={() => props.notifier.toggle()}
        >
          {props.notifier.enabled() ? "🔔 notifications on" : "🔕 notifications off"}
        </button>
      </Show>
      <button type="button" class="logout" onClick={() => props.onLogout()}>
        log out
      </button>
    </header>
  );
};
