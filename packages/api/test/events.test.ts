import type { Session, SessionId, TaskRun, TaskRunId } from "@maestro/domain";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import {
  LogChunk,
  type MaestroEvent,
  MaestroEventFromJsonString,
  QueueChanged,
  SessionStateChanged,
  SystemStatus,
  TaskRunStateChanged,
} from "../src/index.ts";

// Both ends share this contract: the orchestrator encodes with
// MaestroEventFromJsonString, the admin UI decodes with the same schema.
// These tests round-trip every union member through the wire format.

const sessionId = "0198ffff-0000-7000-8000-000000000001" as SessionId;
const taskRunId = "0198ffff-0000-7000-8000-000000000002" as TaskRunId;

const session: Session = {
  id: sessionId,
  projectId: "0198ffff-0000-7000-8000-000000000003" as Session["projectId"],
  ticketReference: { source: "linear", externalId: "FUR-16" },
  gitBranch: "maestro/fur-16",
  claudeSessionUuid: null,
  prNumber: 7,
  prUrl: "https://github.com/acme/repo/pull/7",
  state: "WARM_IDLE",
  createdAt: new Date("2026-07-12T10:00:00.000Z"),
  lastActivityAt: new Date("2026-07-12T10:05:00.000Z"),
};

const taskRun: TaskRun = {
  id: taskRunId,
  sessionId,
  state: "EXECUTING",
  createdAt: new Date("2026-07-12T10:01:00.000Z"),
  expiresAt: new Date("2026-07-12T10:31:00.000Z"),
  evictableAfter: null,
  cause: null,
  resultText: null,
};

const encode = Schema.encodeSync(MaestroEventFromJsonString);
const decode = Schema.decodeUnknownSync(MaestroEventFromJsonString);

const events: ReadonlyArray<MaestroEvent> = [
  SessionStateChanged.make({ session }),
  TaskRunStateChanged.make({ taskRun }),
  QueueChanged.make({ trigger: "dispatched", taskRunId, sessionId, activeCount: 1 }),
  LogChunk.make({ taskRunId, sessionId, chunk: '{"type":"system"}\n' }),
  SystemStatus.make({ activeTurns: 1, maxConcurrentWorkers: 2, dbReachable: true }),
];

describe("MaestroEvent wire codec", () => {
  it.each(
    events.map((event) => [event._tag, event] as const),
  )("round-trips %s through the JSON string wire format", (_tag, event) => {
    const wire = encode(event);
    expect(typeof wire).toBe("string");
    expect(decode(wire)).toEqual(event);
  });

  it("encodes Dates as ISO-8601 strings on the wire", () => {
    const wire = JSON.parse(encode(SessionStateChanged.make({ session })));
    expect(wire.session.createdAt).toBe("2026-07-12T10:00:00.000Z");
  });

  it("the union discriminates on _tag", () => {
    const decoded = decode(encode(SystemStatus.make(events[4] as SystemStatus)));
    expect(decoded._tag).toBe("SystemStatus");
  });

  it("rejects an unknown event tag", () => {
    expect(() => decode(JSON.stringify({ _tag: "Bogus" }))).toThrow();
  });

  it("rejects a LogChunk with a non-UUID task run id", () => {
    expect(() =>
      decode(JSON.stringify({ _tag: "LogChunk", taskRunId: "nope", sessionId, chunk: "x" })),
    ).toThrow();
  });
});
