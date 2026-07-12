import { describe, expect, it } from "vitest";

import {
  canSessionTransition,
  canTaskRunTransition,
  type SessionState,
  sessionTransitions,
  type TaskRunState,
  taskRunTransitions,
} from "../src/index.ts";

// Expected legal transitions are spelled out independently of the
// implementation tables — every (from, to) pair is asserted, so adding or
// removing an edge must be reflected here deliberately.

const sessionStates = [
  "WARM_IDLE",
  "DORMANT_SAVED",
  "TERMINATED",
] as const satisfies ReadonlyArray<SessionState>;
const taskRunStates = [
  "PENDING",
  "PROVISIONING",
  "EXECUTING",
  "COMPLETED",
  "FAILED",
] as const satisfies ReadonlyArray<TaskRunState>;

const legalSession: ReadonlyArray<readonly [SessionState, SessionState]> = [
  ["WARM_IDLE", "DORMANT_SAVED"],
  ["WARM_IDLE", "TERMINATED"],
  ["DORMANT_SAVED", "WARM_IDLE"],
  ["DORMANT_SAVED", "TERMINATED"],
];

const legalTaskRun: ReadonlyArray<readonly [TaskRunState, TaskRunState]> = [
  ["PENDING", "PROVISIONING"],
  ["PENDING", "FAILED"],
  ["PROVISIONING", "EXECUTING"],
  ["PROVISIONING", "FAILED"],
  ["EXECUTING", "COMPLETED"],
  ["EXECUTING", "FAILED"],
];

describe("Session state machine", () => {
  it("covers every state in the transition table", () => {
    expect(Object.keys(sessionTransitions).sort()).toEqual([...sessionStates].sort());
  });

  for (const from of sessionStates) {
    for (const to of sessionStates) {
      const legal = legalSession.some(([f, t]) => f === from && t === to);
      it(`${from} -> ${to} is ${legal ? "legal" : "illegal"}`, () => {
        expect(canSessionTransition(from, to)).toBe(legal);
      });
    }
  }

  it("TERMINATED is terminal", () => {
    expect(sessionTransitions.TERMINATED).toHaveLength(0);
  });
});

describe("TaskRun state machine", () => {
  it("covers every state in the transition table", () => {
    expect(Object.keys(taskRunTransitions).sort()).toEqual([...taskRunStates].sort());
  });

  for (const from of taskRunStates) {
    for (const to of taskRunStates) {
      const legal = legalTaskRun.some(([f, t]) => f === from && t === to);
      it(`${from} -> ${to} is ${legal ? "legal" : "illegal"}`, () => {
        expect(canTaskRunTransition(from, to)).toBe(legal);
      });
    }
  }

  it("COMPLETED and FAILED are terminal", () => {
    expect(taskRunTransitions.COMPLETED).toHaveLength(0);
    expect(taskRunTransitions.FAILED).toHaveLength(0);
  });
});
