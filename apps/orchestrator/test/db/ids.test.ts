import { describe, expect, it } from "vitest";
import { mintId } from "../../src/db/schema/columns.ts";

const UUID_V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("mintId", () => {
  it("mints RFC 9562 UUID version 7 ids", () => {
    for (let i = 0; i < 100; i++) {
      expect(mintId()).toMatch(UUID_V7);
    }
  });

  it("mints ids that sort lexicographically in creation order within a process", () => {
    const ids = Array.from({ length: 1000 }, () => mintId());
    const sorted = [...ids].sort();
    expect(sorted).toEqual(ids);
    // and they are all distinct
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("encodes the current time in the leading 48 bits", () => {
    const before = Date.now();
    const id = mintId();
    const after = Date.now();
    const millis = Number.parseInt(id.slice(0, 8) + id.slice(9, 13), 16);
    expect(millis).toBeGreaterThanOrEqual(before);
    expect(millis).toBeLessThanOrEqual(after);
  });
});
