import type { QueueChanged } from "@maestro/api";
import type { Session, TaskRun } from "@maestro/domain";
import { createSignal } from "solid-js";
import type { EventStore } from "./store.ts";

// Optional browser notifications for the turn lifecycle (start / success /
// failure). Purely an admin-UI concern, driven off the SSE events the store
// already applies — but the two boundaries need different signals:
//
//  - START rides the live-only QueueChanged("dispatched") event. The queue
//    stream is never part of the reconnect snapshot (the snapshot is only
//    SystemStatus + sessions + active runs), so every turn a worker picks up
//    alerts exactly once — first turn or Nth turn on a session alike. Keying
//    START off a TaskRun→EXECUTING transition instead would miss turns: a run
//    in flight during a reconnect is replayed in the snapshot already
//    EXECUTING, indistinguishable from a fresh start, so it has to be
//    suppressed — and reconnects (orchestrator restarts, HMR reloads) are
//    routine.
//  - COMPLETION rides the TaskRun terminal transition, where the prior state
//    matters: success vs failure, and staying silent for a run first seen
//    already-terminal in a reconnect snapshot (a finished turn must not
//    re-alert on every reconnect).

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

/** The ticket ref is the operator's mental key for a session; fall back to a short id. */
const sessionLabel = (session: Session | undefined, sessionId: string): string =>
  session ? session.ticketReference.externalId : `session ${sessionId.slice(0, 8)}`;

const clip = (text: string): string => (text.length > 140 ? `${text.slice(0, 139)}…` : text);

/**
 * Notification content for a run's terminal transition, or null when it should
 * stay silent — an unchanged state, a first-seen run from a reconnect snapshot,
 * or any non-terminal hop (start is handled via the queue signal, not here).
 */
export const describeCompletion = (
  previous: TaskRun | undefined,
  next: TaskRun,
  label: string,
): { readonly title: string; readonly body: string } | null => {
  if (previous === undefined || previous.state === next.state) return null;
  switch (next.state) {
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
      // PENDING / PROVISIONING / EXECUTING are not completion boundaries.
      return null;
  }
};

export type NotifyPermission = NotificationPermission | "unsupported";

/**
 * Reactive notifier: owns the opt-in signal + permission state and turns store
 * events into Web Notifications. Registered as the store's queue + run
 * listeners at the app root.
 */
export const createNotifier = (store: EventStore) => {
  const supported = typeof Notification !== "undefined";
  // Effectively on only when the operator opted in AND permission is currently
  // granted — a stored "true" with revoked permission reads as off until they
  // re-enable.
  const [enabled, setEnabled] = createSignal(
    loadEnabled() && supported && Notification.permission === "granted",
  );
  const [permission, setPermission] = createSignal<NotifyPermission>(
    supported ? Notification.permission : "unsupported",
  );

  const armed = (): boolean => enabled() && permission() === "granted";

  const fire = (content: { readonly title: string; readonly body: string }, tag: string): void => {
    try {
      new Notification(content.title, { body: content.body, tag });
    } catch {
      // Permission can flip underfoot (revoked in another tab); ignore the throw.
    }
  };

  /** START: fires for every turn a worker picks up (live-only queue event). */
  const handleQueueChange = (event: QueueChanged): void => {
    if (!armed() || event.trigger !== "dispatched") return;
    const label = sessionLabel(store.session(event.sessionId), event.sessionId);
    fire(
      { title: `${label} started`, body: "A worker picked up the turn." },
      `${event.taskRunId}:started`,
    );
  };

  /** COMPLETION: fires from each TaskRunStateChanged, with the prior row. */
  const handleRunChange = (previous: TaskRun | undefined, next: TaskRun): void => {
    if (!armed()) return;
    const content = describeCompletion(
      previous,
      next,
      sessionLabel(store.session(next.sessionId), next.sessionId),
    );
    if (content === null) return;
    fire(content, `${next.id}:${next.state}`);
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

  return { enabled, permission, supported, handleQueueChange, handleRunChange, toggle };
};

export type Notifier = ReturnType<typeof createNotifier>;
