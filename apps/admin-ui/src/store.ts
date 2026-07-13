import type { MaestroEvent, SystemStatus } from "@maestro/api";
import type { Session, SessionId, TaskRun, TaskRunId } from "@maestro/domain";
import { createSignal } from "solid-js";

// The SSE store: one reactive module that consumes decoded MaestroEvents and
// exposes Solid signals the views render from. Events carry FULL entities, so
// every apply is an upsert keyed by id — at-least-once duplicates around the
// SSE snapshot boundary converge to the same state (FUR-16 contract).
//
// Reconnect semantics: EventSource auto-reconnects and the server replays a
// fresh snapshot, so `resetForSnapshot` clears entity state and lets the
// snapshot rebuild it. Log buffers survive the reset — LogChunks are live-only
// (never in the snapshot); the log tail view owns rebasing its buffer onto the
// historical log fetch.

export type ConnectionState = "connecting" | "open" | "reconnecting";

/** A pending SSE reconnect attempt — surfaced by the status bar. */
export type RetryState = { readonly attempt: number; readonly delayMs: number };

/** Keep only the newest log bytes per run — a debug tail, not an archive. */
const MAX_LOG_CHARS = 512 * 1024;

const upsert = <K, V>(map: ReadonlyMap<K, V>, key: K, value: V): ReadonlyMap<K, V> =>
  new Map(map).set(key, value);

export const createEventStore = () => {
  const [sessions, setSessions] = createSignal<ReadonlyMap<SessionId, Session>>(new Map());
  const [runs, setRuns] = createSignal<ReadonlyMap<TaskRunId, TaskRun>>(new Map());
  const [logs, setLogs] = createSignal<ReadonlyMap<TaskRunId, string>>(new Map());
  const [systemStatus, setSystemStatus] = createSignal<SystemStatus | null>(null);
  /** In-process active-turn count from the latest QueueChanged; null until one arrives. */
  const [activeTurns, setActiveTurns] = createSignal<number | null>(null);
  const [connection, setConnection] = createSignal<ConnectionState>("connecting");
  /** Non-null while the SSE supervisor is waiting out a reconnect backoff. */
  const [retry, setRetry] = createSignal<RetryState | null>(null);

  const apply = (event: MaestroEvent): void => {
    switch (event._tag) {
      case "SessionStateChanged":
        setSessions((map) => upsert(map, event.session.id, event.session));
        return;
      case "TaskRunStateChanged":
        setRuns((map) => upsert(map, event.taskRun.id, event.taskRun));
        return;
      case "QueueChanged":
        setActiveTurns(event.activeCount);
        return;
      case "LogChunk":
        setLogs((map) => {
          const appended = (map.get(event.taskRunId) ?? "") + event.chunk;
          return upsert(map, event.taskRunId, appended.slice(-MAX_LOG_CHARS));
        });
        return;
      case "SystemStatus":
        setSystemStatus(event);
        return;
    }
  };

  /** A (re)connect happened: the server replays a full snapshot next. */
  const resetForSnapshot = (): void => {
    setSessions(new Map());
    setRuns(new Map());
    setSystemStatus(null);
    setActiveTurns(null);
    setConnection("open");
    setRetry(null);
  };

  /** The log tail view rebases a run's buffer onto the historical log fetch. */
  const rebaseLogs = (taskRunId: TaskRunId, historical: string): void => {
    setLogs((map) => upsert(map, taskRunId, historical.slice(-MAX_LOG_CHARS)));
  };

  // ---- derived reads (plain functions over signals — track when called in a
  // reactive scope, and stay honest in tests without one) ----

  /** Sessions ordered by last activity, newest first. */
  const sessionList = (): ReadonlyArray<Session> =>
    [...sessions().values()].sort(
      (a, b) => b.lastActivityAt.getTime() - a.lastActivityAt.getTime(),
    );

  const session = (id: SessionId): Session | undefined => sessions().get(id);

  /** A session's runs, oldest first (createdAt, then UUIDv7 id as tiebreak). */
  const runsForSession = (sessionId: SessionId): ReadonlyArray<TaskRun> =>
    [...runs().values()]
      .filter((run) => run.sessionId === sessionId)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime() || (a.id < b.id ? -1 : 1));

  /**
   * Queue depth = PENDING runs for the session. DECISION: derived from
   * TaskRunStateChanged alone — the snapshot delivers every unsettled run and
   * live events walk their states, so the count is correct from connect;
   * QueueChanged only carries the global active count (shown in the status
   * bar), not per-session queue membership.
   */
  const queueDepth = (sessionId: SessionId): number =>
    [...runs().values()].filter((run) => run.sessionId === sessionId && run.state === "PENDING")
      .length;

  const logFor = (taskRunId: TaskRunId): string => logs().get(taskRunId) ?? "";

  return {
    apply,
    resetForSnapshot,
    rebaseLogs,
    setConnection,
    connection,
    setRetry,
    retry,
    systemStatus,
    activeTurns,
    sessionList,
    session,
    runsForSession,
    queueDepth,
    logFor,
  };
};

export type EventStore = ReturnType<typeof createEventStore>;
