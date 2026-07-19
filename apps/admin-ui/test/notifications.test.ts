import { TaskRun, type TaskRunId } from "@maestro/domain";
import { Schema } from "effect";
import { describe, expect, it } from "vitest";
import { describeRunTransition } from "../src/notifications.ts";

// The notifier's testable core: the pure transition classifier. It decides
// which run state-changes surface a browser notification and with what text.
// The reactive/permission wrapper (createNotifier) touches the browser
// Notification global and is exercised in the app, not here.

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

describe("describeRunTransition", () => {
  it("stays silent for a first-seen run (snapshot replay, no prior state)", () => {
    // The reconnect snapshot re-delivers every run with previous === undefined;
    // none of these may re-alert.
    expect(describeRunTransition(undefined, run("EXECUTING"), "FUR-9")).toBeNull();
    expect(describeRunTransition(undefined, run("COMPLETED"), "FUR-9")).toBeNull();
    expect(describeRunTransition(undefined, run("FAILED"), "FUR-9")).toBeNull();
  });

  it("stays silent when the state did not actually change (at-least-once duplicate)", () => {
    expect(describeRunTransition(run("EXECUTING"), run("EXECUTING"), "FUR-9")).toBeNull();
  });

  it("stays silent for intermediate hops that are not lifecycle boundaries", () => {
    expect(describeRunTransition(run("PENDING"), run("PROVISIONING"), "FUR-9")).toBeNull();
  });

  it("announces a start when a run reaches EXECUTING", () => {
    const content = describeRunTransition(run("PROVISIONING"), run("EXECUTING"), "FUR-9");
    expect(content?.title).toBe("FUR-9 started");
  });

  it("announces success with the result text, or a default when absent", () => {
    const withResult = describeRunTransition(
      run("EXECUTING"),
      run("COMPLETED", { resultText: "Opened PR #42" }),
      "FUR-9",
    );
    expect(withResult).toEqual({ title: "FUR-9 completed", body: "Opened PR #42" });

    const noResult = describeRunTransition(run("EXECUTING"), run("COMPLETED"), "FUR-9");
    expect(noResult?.body).toBe("The turn finished successfully.");
  });

  it("announces failure, preferring the human summary over the cause code", () => {
    const summarised = describeRunTransition(
      run("EXECUTING"),
      run("FAILED", { cause: "ERROR", failureSummary: "npm install exited 1" }),
      "FUR-9",
    );
    expect(summarised).toEqual({ title: "FUR-9 failed", body: "npm install exited 1" });

    const causeOnly = describeRunTransition(
      run("EXECUTING"),
      run("FAILED", { cause: "OOM" }),
      "FUR-9",
    );
    expect(causeOnly?.body).toBe("OOM");
  });

  it("clips an overlong body so the notification stays legible", () => {
    const long = "x".repeat(500);
    const content = describeRunTransition(
      run("EXECUTING"),
      run("COMPLETED", { resultText: long }),
      "FUR-9",
    );
    expect(content?.body.length).toBe(140);
    expect(content?.body.endsWith("…")).toBe(true);
  });
});
