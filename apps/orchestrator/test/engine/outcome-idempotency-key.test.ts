import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import { outcomeIdempotencyKey, TurnOutcomePayload } from "../../src/engine/TurnExecutor.ts";

// FUR-39 layer 3: failure-comment dedup happens at ENQUEUE time, in the outbox
// idempotency key — identical failure text on a session maps to the same key,
// so the existing ON CONFLICT DO NOTHING collapses the repeat. These unit
// tests pin the key derivation contract.

const decodeOutcome = Schema.decodeUnknownSync(TurnOutcomePayload);

const RUN_A = "0197f2a0-0000-7000-8000-00000000000a";
const RUN_B = "0197f2a0-0000-7000-8000-00000000000b";
const SESSION_1 = "0197f2a0-0000-7000-8000-000000000001";
const SESSION_2 = "0197f2a0-0000-7000-8000-000000000002";

const outcome = (overrides: Record<string, unknown>): TurnOutcomePayload =>
  decodeOutcome({
    kind: "turn-failed",
    taskRunId: RUN_A,
    sessionId: SESSION_1,
    ticket: { source: "linear", externalId: "FUR-39" },
    summary: "fake agent exploded",
    cause: "ERROR",
    pr: null,
    ...overrides,
  });

describe("outcomeIdempotencyKey", () => {
  it("identical failure text on the same session collapses to one key across turns", () => {
    const first = outcomeIdempotencyKey(outcome({ taskRunId: RUN_A }));
    const repeat = outcomeIdempotencyKey(outcome({ taskRunId: RUN_B }));
    expect(repeat).toBe(first);
  });

  it("a new distinct failure text gets a new key (still posts)", () => {
    const first = outcomeIdempotencyKey(outcome({ summary: "fake agent exploded" }));
    const changed = outcomeIdempotencyKey(outcome({ summary: "worker exited with code 137" }));
    expect(changed).not.toBe(first);
  });

  it("the same failure text on a different session posts independently", () => {
    const one = outcomeIdempotencyKey(outcome({ sessionId: SESSION_1 }));
    const other = outcomeIdempotencyKey(outcome({ sessionId: SESSION_2 }));
    expect(other).not.toBe(one);
  });

  it("a different cause with the same text is a different failure", () => {
    const error = outcomeIdempotencyKey(outcome({ cause: "ERROR" }));
    const timeout = outcomeIdempotencyKey(outcome({ cause: "TIMEOUT" }));
    expect(timeout).not.toBe(error);
  });

  it("turn-completed stays keyed per turn (replayed settlements are no-ops)", () => {
    const completed = outcome({ kind: "turn-completed", cause: null, summary: "Done." });
    expect(outcomeIdempotencyKey(completed)).toBe(`turn-result:${RUN_A}`);
  });

  it("session-paused is keyed by the tripping turn (one message per trip)", () => {
    const paused = outcome({ kind: "session-paused", cause: null });
    expect(outcomeIdempotencyKey(paused)).toBe(`session-paused:${RUN_A}`);
  });
});
