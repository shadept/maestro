import type { MaestroEvent } from "@maestro/api";
import { Session, type SessionId, TaskRun, type TaskRunId } from "@maestro/domain";
import { Schema } from "effect";
import { createMemo, createRoot } from "solid-js";
import { describe, expect, it } from "vitest";
import { createEventStore } from "../src/store.ts";

// FUR-17 acceptance: component tests for the SSE store — decoded events in,
// signal updates out. Tested as plain Solid reactivity (createRoot + memos),
// no DOM involved; the vite config resolves solid's browser build so signal
// propagation behaves exactly as in the bundle.

const SID_A = "0198aaaa-0000-7000-8000-000000000001" as SessionId;
const SID_B = "0198aaaa-0000-7000-8000-000000000002" as SessionId;
const RID_1 = "0198bbbb-0000-7000-8000-000000000001" as TaskRunId;
const RID_2 = "0198bbbb-0000-7000-8000-000000000002" as TaskRunId;

const decodeSession = Schema.decodeUnknownSync(Session);
const decodeTaskRun = Schema.decodeUnknownSync(TaskRun);

const session = (overrides: Record<string, unknown> = {}): Session =>
  decodeSession({
    id: SID_A,
    projectId: "0198cccc-0000-7000-8000-000000000001",
    ticketReference: { source: "linear", externalId: "FUR-17" },
    gitBranch: "maestro/fur-17",
    claudeSessionUuid: null,
    prNumber: null,
    prUrl: null,
    terminationRequestedAt: null,
    pausedAt: null,
    agentModel: null,
    agentEffort: null,
    state: "WARM_IDLE",
    createdAt: new Date("2026-07-12T10:00:00Z"),
    lastActivityAt: new Date("2026-07-12T10:00:00Z"),
    ...overrides,
  });

const taskRun = (overrides: Record<string, unknown> = {}): TaskRun =>
  decodeTaskRun({
    id: RID_1,
    sessionId: SID_A,
    state: "PENDING",
    createdAt: new Date("2026-07-12T10:01:00Z"),
    expiresAt: null,
    evictableAfter: null,
    cause: null,
    resultText: null,
    ...overrides,
  });

const sessionChanged = (s: Session): MaestroEvent => ({ _tag: "SessionStateChanged", session: s });
const runChanged = (r: TaskRun): MaestroEvent => ({ _tag: "TaskRunStateChanged", taskRun: r });

const withStore = (f: (store: ReturnType<typeof createEventStore>) => void): void =>
  createRoot((dispose) => {
    f(createEventStore());
    dispose();
  });

describe("event store", () => {
  it("upserts sessions idempotently and orders by last activity", () =>
    withStore((store) => {
      const list = createMemo(() => store.sessionList());
      expect(list()).toHaveLength(0);

      const a = session();
      const b = session({ id: SID_B, lastActivityAt: new Date("2026-07-12T11:00:00Z") });
      store.apply(sessionChanged(a));
      store.apply(sessionChanged(b));
      // at-least-once around the snapshot boundary: the duplicate converges
      store.apply(sessionChanged(a));

      expect(list()).toHaveLength(2);
      expect(list().map((s) => s.id)).toEqual([SID_B, SID_A]); // newest activity first

      // a state transition replaces the row in place — the chip flips live
      store.apply(sessionChanged(session({ state: "DORMANT_SAVED" })));
      expect(list()).toHaveLength(2);
      expect(store.session(SID_A)?.state).toBe("DORMANT_SAVED");
    }));

  it("tracks task runs per session and derives queue depth from PENDING runs", () =>
    withStore((store) => {
      const depth = createMemo(() => store.queueDepth(SID_A));
      const runs = createMemo(() => store.runsForSession(SID_A));
      expect(depth()).toBe(0);

      store.apply(runChanged(taskRun()));
      store.apply(runChanged(taskRun({ id: RID_2, createdAt: new Date("2026-07-12T10:02:00Z") })));
      expect(depth()).toBe(2);
      expect(runs().map((r) => r.id)).toEqual([RID_1, RID_2]); // oldest first

      // the first run gets dispatched: no longer queued, still listed
      store.apply(runChanged(taskRun({ state: "EXECUTING" })));
      expect(depth()).toBe(1);
      expect(runs()).toHaveLength(2);
      expect(runs()[0]?.state).toBe("EXECUTING");

      // runs of other sessions never leak in
      expect(store.runsForSession(SID_B)).toHaveLength(0);
      expect(store.queueDepth(SID_B)).toBe(0);
    }));

  it("appends log chunks in order and rebases onto historical fetches", () =>
    withStore((store) => {
      const log = createMemo(() => store.logFor(RID_1));
      expect(log()).toBe("");

      store.apply({ _tag: "LogChunk", taskRunId: RID_1, sessionId: SID_A, chunk: "line 1\n" });
      store.apply({ _tag: "LogChunk", taskRunId: RID_1, sessionId: SID_A, chunk: "line 2\n" });
      expect(log()).toBe("line 1\nline 2\n");

      // the log tail view replaces the buffer with the historical fetch...
      store.rebaseLogs(RID_1, "historical\n");
      expect(log()).toBe("historical\n");
      // ...and live chunks keep appending afterwards
      store.apply({ _tag: "LogChunk", taskRunId: RID_1, sessionId: SID_A, chunk: "live\n" });
      expect(log()).toBe("historical\nlive\n");

      // buffers are independent per run
      expect(store.logFor(RID_2)).toBe("");
    }));

  it("caps a run's log buffer at the newest bytes", () =>
    withStore((store) => {
      store.apply({
        _tag: "LogChunk",
        taskRunId: RID_1,
        sessionId: SID_A,
        chunk: "x".repeat(600 * 1024),
      });
      store.apply({ _tag: "LogChunk", taskRunId: RID_1, sessionId: SID_A, chunk: "tail" });
      const log = store.logFor(RID_1);
      expect(log.length).toBe(512 * 1024);
      expect(log.endsWith("tail")).toBe(true);
    }));

  it("surfaces system status and the live active-turn count", () =>
    withStore((store) => {
      const status = createMemo(() => store.systemStatus());
      const active = createMemo(() => store.activeTurns());
      expect(status()).toBeNull();
      expect(active()).toBeNull();

      store.apply({
        _tag: "SystemStatus",
        activeTurns: 1,
        maxConcurrentWorkers: 2,
        dbReachable: true,
      });
      expect(status()?.maxConcurrentWorkers).toBe(2);

      store.apply({
        _tag: "QueueChanged",
        trigger: "dispatched",
        taskRunId: RID_1,
        sessionId: SID_A,
        activeCount: 2,
      });
      expect(active()).toBe(2);
    }));

  it("reset-and-rebuilds on reconnect, preserving log buffers", () =>
    withStore((store) => {
      store.apply(sessionChanged(session()));
      store.apply(runChanged(taskRun()));
      store.apply({ _tag: "LogChunk", taskRunId: RID_1, sessionId: SID_A, chunk: "kept\n" });
      store.apply({
        _tag: "SystemStatus",
        activeTurns: 0,
        maxConcurrentWorkers: 2,
        dbReachable: true,
      });
      store.setConnection("reconnecting");

      // EventSource reconnected: the server replays a fresh snapshot
      store.resetForSnapshot();
      expect(store.connection()).toBe("open");
      expect(store.sessionList()).toHaveLength(0);
      expect(store.runsForSession(SID_A)).toHaveLength(0);
      expect(store.systemStatus()).toBeNull();
      expect(store.activeTurns()).toBeNull();
      // LogChunks are live-only (never in the snapshot) — the buffer survives
      expect(store.logFor(RID_1)).toBe("kept\n");

      // the replayed snapshot rebuilds the state
      store.apply(sessionChanged(session()));
      expect(store.sessionList()).toHaveLength(1);
    }));
});
