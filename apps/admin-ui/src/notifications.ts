import type { Session, TaskRun } from "@maestro/domain";
import { createSignal } from "solid-js";
import type { EventStore } from "./store.ts";

// Optional browser notifications for the turn lifecycle (start / success /
// failure). Purely an admin-UI concern: it rides the SSE TaskRunStateChanged
// events the store already applies. The store hands us (previous, next) for
// every run change; we classify the transition and fire a Web Notification
// when the operator has opted in and the browser granted permission.
//
// Snapshot safety: on (re)connect the store clears its run map and the server
// replays a full snapshot, so every run is first-seen with previous ===
// undefined. describeRunTransition treats a first-seen run as silent — a
// reconnect never re-alerts historical runs, only genuine live transitions do.

const STORAGE_KEY = "maestro-notifications-enabled";

const loadEnabled = (): boolean => {
  try {
    return globalThis.localStorage?.getItem(STORAGE_KEY) === "true";
  } catch {
    return false;
  }
};

const persistEnabled = (on: boolean): void => {
  try {
    globalThis.localStorage?.setItem(STORAGE_KEY, on ? "true" : "false");
  } catch {
    // Storage unusable (privacy mode, quota): the toggle degrades to this session only.
  }
};

/** The ticket ref is the operator's mental key for a run; fall back to a short id. */
const runLabel = (run: TaskRun, session: Session | undefined): string =>
  session ? session.ticketReference.externalId : `session ${run.sessionId.slice(0, 8)}`;

const clip = (text: string): string => (text.length > 140 ? `${text.slice(0, 139)}…` : text);

/** Notification content for a run transition, or null when it should stay silent. */
export const describeRunTransition = (
  previous: TaskRun | undefined,
  next: TaskRun,
  label: string,
): { readonly title: string; readonly body: string } | null => {
  // First sight of a run is snapshot replay of history, not a live change.
  if (previous === undefined || previous.state === next.state) return null;
  switch (next.state) {
    case "EXECUTING":
      return { title: `${label} started`, body: "The turn is now running." };
    case "COMPLETED":
      return {
        title: `${label} completed`,
        body: next.resultText ? clip(next.resultText) : "The turn finished successfully.",
      };
    case "FAILED":
      return {
        title: `${label} failed`,
        body: clip(next.failureSummary ?? next.cause ?? "The turn failed."),
      };
    default:
      // PENDING / PROVISIONING hops are intermediate, not lifecycle boundaries.
      return null;
  }
};

export type NotifyPermission = NotificationPermission | "unsupported";

/**
 * Reactive notifier: owns the opt-in signal + permission state and turns store
 * run-transitions into Web Notifications. Created once at the app root and
 * registered as the store's run listener.
 */
export const createNotifier = (store: EventStore) => {
  const supported = typeof Notification !== "undefined";
  // Effectively on only when the operator opted in AND permission is currently
  // granted — a stored "true" with revoked permission reads as off (and stays
  // silent) until they re-enable.
  const [enabled, setEnabled] = createSignal(
    loadEnabled() && supported && Notification.permission === "granted",
  );
  const [permission, setPermission] = createSignal<NotifyPermission>(
    supported ? Notification.permission : "unsupported",
  );

  /** Fires from the store for every TaskRunStateChanged, with the prior row. */
  const handleRunChange = (previous: TaskRun | undefined, next: TaskRun): void => {
    if (!enabled() || permission() !== "granted") return;
    const content = describeRunTransition(
      previous,
      next,
      runLabel(next, store.session(next.sessionId)),
    );
    if (content === null) return;
    try {
      // tag keyed by run+state: dedupes at-least-once duplicates while keeping
      // a run's start and completion as two distinct alerts.
      new Notification(content.title, { body: content.body, tag: `${next.id}:${next.state}` });
    } catch {
      // Permission can flip underfoot (revoked in another tab); ignore the throw.
    }
  };

  /** Enable: request permission while still "default"; only stick if granted. */
  const enable = async (): Promise<void> => {
    if (!supported) return;
    const current =
      Notification.permission === "default"
        ? await Notification.requestPermission()
        : Notification.permission;
    setPermission(current);
    const granted = current === "granted";
    setEnabled(granted);
    persistEnabled(granted);
  };

  const disable = (): void => {
    setEnabled(false);
    persistEnabled(false);
  };

  const toggle = (): void => {
    if (enabled()) disable();
    else void enable();
  };

  return { enabled, permission, supported, handleRunChange, toggle };
};

export type Notifier = ReturnType<typeof createNotifier>;
