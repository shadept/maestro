import { readFile } from "node:fs/promises";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { parseChatLog } from "../src/chat-log.ts";

// FUR-43 acceptance: parser tests driven by the FUR-12 recorded stream-json
// fixtures (apps/orchestrator/test/fixtures/agent) — no new fixture
// infrastructure, per the ticket.

const fixture = (name: string) =>
  readFile(
    path.resolve(import.meta.dirname, "../../orchestrator/test/fixtures/agent", name),
    "utf8",
  );

describe("parseChatLog", () => {
  it("renders the happy path as a readable conversation with zero raw rows", async () => {
    const items = parseChatLog(await fixture("happy-path.jsonl"));

    expect(items.map((item) => item.kind)).toEqual([
      "thinking",
      "text",
      "tool-use",
      "text",
      "result",
    ]);
    expect(items.some((item) => item.kind === "raw")).toBe(false);

    const [, firstText, toolUse, , result] = items;
    expect(firstText).toMatchObject({ kind: "text", text: "Working on the lint rule now." });
    expect(toolUse).toMatchObject({
      kind: "tool-use",
      name: "Bash",
      input: { command: "pnpm lint" },
      result: null,
    });
    expect(result).toMatchObject({
      kind: "result",
      ok: true,
      text: "Done — rule added and tests pass.",
      costUsd: 0.17,
      durationMs: 4206,
    });
  });

  it("mid-stream error yields a failed result banner with the error text", async () => {
    const items = parseChatLog(await fixture("mid-stream-error.jsonl"));

    expect(items.map((item) => item.kind)).toEqual(["text", "result"]);
    expect(items[1]).toMatchObject({
      kind: "result",
      ok: false,
      text: "Execution failed: process was terminated",
      durationMs: 120000,
    });
  });

  it("garbage and unrecognized lines fall back to raw rows, never crash", async () => {
    const items = parseChatLog(await fixture("unknown-events.jsonl"));

    expect(items.map((item) => item.kind)).toEqual(["raw", "raw", "text", "result"]);
    expect(items[0]).toMatchObject({ kind: "raw", line: "this line is not json at all {{{" });
    expect(items[1]).toMatchObject({
      kind: "raw",
      line: '{"no_type_field":true,"just":"garbage-shaped json"}',
    });
  });
});

describe("parseChatLog (hand-written cases)", () => {
  it("pairs a tool_use with a later tool_result by tool_use_id", () => {
    const lines = [
      JSON.stringify({
        type: "assistant",
        message: {
          content: [{ type: "tool_use", id: "toolu_9", name: "Read", input: { path: "a" } }],
        },
      }),
      JSON.stringify({
        type: "user",
        message: {
          content: [
            {
              type: "tool_result",
              tool_use_id: "toolu_9",
              content: "file contents",
              is_error: false,
            },
          ],
        },
      }),
    ].join("\n");

    const items = parseChatLog(lines);
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      kind: "tool-use",
      toolUseId: "toolu_9",
      name: "Read",
      result: { content: "file contents", isError: false },
    });
  });

  it("an unpaired tool_use (no matching tool_use_id) is dropped, not crashed on", () => {
    const line = JSON.stringify({
      type: "user",
      message: { content: [{ type: "tool_result", tool_use_id: "toolu_missing", content: "x" }] },
    });

    expect(parseChatLog(line)).toEqual([]);
  });

  it("empty lines are skipped, not rendered as raw rows", () => {
    const lines = [
      "",
      "   ",
      JSON.stringify({ type: "result", is_error: false, result: "ok" }),
    ].join("\n");

    expect(parseChatLog(lines).map((item) => item.kind)).toEqual(["result"]);
  });

  it("a partial trailing line (chunk cut mid-JSON) falls back to a raw row instead of crashing", () => {
    const lines = [
      JSON.stringify({ type: "assistant", message: { content: [{ type: "text", text: "hi" }] } }),
      '{"type":"assistant","message":{"content":[{"type":"text","text":"cu',
    ].join("\n");

    const items = parseChatLog(lines);
    expect(items.map((item) => item.kind)).toEqual(["text", "raw"]);
  });
});
