import { describe, expect, it } from "vitest";
import { mentionsHandle } from "../../src/ingest/LinearIngest.ts";

// FUR-37: plain-text @handle detection — the only mention evidence Linear's
// webhook comment bodies carry (captured payloads serialize app mentions as
// literal "@maestro" with no id markup).

describe("mentionsHandle", () => {
  it("matches a plain mention", () => {
    expect(mentionsHandle("@maestro test", "maestro")).toBe(true);
  });

  it("matches mid-sentence, with punctuation boundaries", () => {
    expect(mentionsHandle("hey @maestro, take a look", "maestro")).toBe(true);
    expect(mentionsHandle("(@maestro)", "maestro")).toBe(true);
    expect(mentionsHandle("done?\n@maestro verify please", "maestro")).toBe(true);
    expect(mentionsHandle("@maestro's turn", "maestro")).toBe(true);
  });

  it("is case-insensitive", () => {
    expect(mentionsHandle("@Maestro please", "maestro")).toBe(true);
    expect(mentionsHandle("@MAESTRO", "maestro")).toBe(true);
  });

  it("does not match emails or address-like text", () => {
    expect(mentionsHandle("mail ops@maestro.dev about it", "maestro")).toBe(false);
    expect(mentionsHandle("user@maestro", "maestro")).toBe(false);
  });

  it("does not match longer handles that merely start with ours", () => {
    expect(mentionsHandle("@maestrofoo ping", "maestro")).toBe(false);
    expect(mentionsHandle("@maestro-bot ping", "maestro")).toBe(false);
    expect(mentionsHandle("@maestro2 ping", "maestro")).toBe(false);
  });

  it("does not match the bare handle without the @", () => {
    expect(mentionsHandle("maestro should do this", "maestro")).toBe(false);
  });

  it("escapes regex metacharacters in custom handles", () => {
    expect(mentionsHandle("@c3-po go", "c3-po")).toBe(true);
    expect(mentionsHandle("@c3.po go", "c3-po")).toBe(false);
  });
});
