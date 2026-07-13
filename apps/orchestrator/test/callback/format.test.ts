import { Schema } from "effect";
import { describe, expect, it } from "vitest";

import { formatTurnComment, MAESTRO_COMMENT_MARKER } from "../../src/callback/format.ts";
import { TurnOutcomePayload } from "../../src/engine/TurnSettlement.ts";

// FUR-39 layer 1: ingest drops any comment whose body starts with
// MAESTRO_COMMENT_MARKER. These tests pin the other side of that contract —
// every body format.ts can render MUST start with the marker, so the
// formatter and the ingest guard cannot drift apart.

const decodeOutcome = Schema.decodeUnknownSync(TurnOutcomePayload);

const outcome = (overrides: Record<string, unknown>): TurnOutcomePayload =>
  decodeOutcome({
    kind: "turn-completed",
    taskRunId: "0197f2a0-0000-7000-8000-000000000001",
    sessionId: "0197f2a0-0000-7000-8000-000000000002",
    ticket: { source: "linear", externalId: "FUR-42" },
    summary: "Handbook updated; 3 files changed.",
    cause: null,
    pr: null,
    ...overrides,
  });

describe("formatTurnComment", () => {
  it("completed-turn comments start with the Maestro marker", () => {
    const body = formatTurnComment(
      outcome({ pr: { number: 7, url: "https://github.test/acme/flux/pull/7" } }),
    );
    expect(body.startsWith(MAESTRO_COMMENT_MARKER)).toBe(true);
  });

  it("failed-turn comments start with the Maestro marker", () => {
    const body = formatTurnComment(
      outcome({ kind: "turn-failed", cause: "ERROR", summary: "Boom." }),
    );
    expect(body.startsWith(MAESTRO_COMMENT_MARKER)).toBe(true);
  });

  it("even an empty-summary comment starts with the marker", () => {
    const body = formatTurnComment(outcome({ summary: "" }));
    expect(body.startsWith(MAESTRO_COMMENT_MARKER)).toBe(true);
  });

  it("session-paused comments start with the marker and carry the summary verbatim", () => {
    const body = formatTurnComment(
      outcome({
        kind: "session-paused",
        summary: "Paused this session after 3 consecutive failures.",
      }),
    );
    expect(body.startsWith(MAESTRO_COMMENT_MARKER)).toBe(true);
    expect(body).toContain("Paused this session after 3 consecutive failures");
  });
});
