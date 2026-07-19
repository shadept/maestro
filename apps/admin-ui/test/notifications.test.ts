import { TaskRun, type TaskRunId } from "@maestro/domain";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import { describeCompletion } from "../src/notifications.ts";

// The completion classifier is the notifier's testable core: it decides which
// terminal run transitions surface a browser notification and with what text.
// Start notifications ride the live-only QueueChanged("dispatched") event
// (wired store → notifier), and the reactive/permission wrapper touches the
// browser Notification global — both are exercised in the app, not here.

const decodeRun = Schema.decodeUnknownSync(TaskRun);

const run = (state: string, overrides: Record<string, unknown> = {}): TaskRun =>
  decodeRun({
    id: "0198bbbb-0000-7000-8000-000000000001" as TaskRunId,
    sessionId: "0198aaaa-0000-7000-8000-000000000001",
    state,
    createdAt: new Date("2026-07-19T10:00:00Z"),
    expiresAt: null,
    evictableAfter: null,
    cause: null,
    resultText: null,
    failureSummary: null,
    traceId: null,
    ...overrides,
  });

describe("describeCompletion", () => {
  it("stays silent for a first-seen run (snapshot replay, no prior state)", () => {
    // The reconnect snapshot re-delivers every active run with previous ===
    // undefined; a terminal one must not re-alert.
    expect(describeCompletion(undefined, run("COMPLETED"), "FUR-9")).toBeNull();
    expect(describeCompletion(undefined, run("FAILED"), "FUR-9")).toBeNull();
  });

  it("stays silent when the state did not actually change (at-least-once duplicate)", () => {
    expect(describeCompletion(run("COMPLETED"), run("COMPLETED"), "FUR-9")).toBeNull();
  });

  it("stays silent for non-terminal transitions (start is a queue-driven concern)", () => {
    expect(describeCompletion(run("PENDING"), run("PROVISIONING"), "FUR-9")).toBeNull();
    expect(describeCompletion(run("PROVISIONING"), run("EXECUTING"), "FUR-9")).toBeNull();
  });

  it("announces success with the result text, or a default when absent", () => {
    const withResult = describeCompletion(
      run("EXECUTING"),
      run("COMPLETED", { resultText: "Opened PR #42" }),
      "FUR-9",
    );
    expect(withResult).toEqual({ title: "FUR-9 completed", body: "Opened PR #42" });

    const noResult = describeCompletion(run("EXECUTING"), run("COMPLETED"), "FUR-9");
    expect(noResult?.body).toBe("The turn finished successfully.");
  });

  it("announces failure, preferring the human summary over the cause code", () => {
    const summarised = describeCompletion(
      run("EXECUTING"),
      run("FAILED", { cause: "ERROR", failureSummary: "npm install exited 1" }),
      "FUR-9",
    );
    expect(summarised).toEqual({ title: "FUR-9 failed", body: "npm install exited 1" });

    const causeOnly = describeCompletion(
      run("EXECUTING"),
      run("FAILED", { cause: "OOM" }),
      "FUR-9",
    );
    expect(causeOnly?.body).toBe("OOM");
  });

  it("clips an overlong body so the notification stays legible", () => {
    const long = "x".repeat(500);
    const content = describeCompletion(
      run("EXECUTING"),
      run("COMPLETED", { resultText: long }),
      "FUR-9",
    );
    expect(content?.body.length).toBe(140);
    expect(content?.body.endsWith("…")).toBe(true);
  });
});
