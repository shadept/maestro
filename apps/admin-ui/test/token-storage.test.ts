import { afterEach, describe, expect, it, vi } from "vitest";
import { clearToken, loadToken, saveToken } from "../src/token-storage.ts";

// The persistence override (token survives refresh — sanctioned deviation
// from FUR-17). The vitest env is node, so `globalThis.localStorage` is
// stubbed: a Map-backed fake for the happy path, absent/throwing globals for
// the degraded paths the module must swallow.

const fakeStorage = () => {
  const map = new Map<string, string>();
  return {
    getItem: (key: string) => map.get(key) ?? null,
    setItem: (key: string, value: string) => void map.set(key, value),
    removeItem: (key: string) => void map.delete(key),
  } as unknown as Storage;
};

afterEach(() => vi.unstubAllGlobals());

describe("token storage", () => {
  it("round-trips save → load → clear through localStorage", () => {
    vi.stubGlobal("localStorage", fakeStorage());

    expect(loadToken()).toBeNull();
    saveToken("s3cret");
    expect(loadToken()).toBe("s3cret");
    saveToken("rotated");
    expect(loadToken()).toBe("rotated");
    clearToken();
    expect(loadToken()).toBeNull();
  });

  it("degrades silently when localStorage is absent", () => {
    vi.stubGlobal("localStorage", undefined);

    expect(loadToken()).toBeNull();
    expect(() => saveToken("s3cret")).not.toThrow();
    expect(() => clearToken()).not.toThrow();
  });

  it("degrades silently when localStorage throws (privacy mode)", () => {
    const throwing = new Proxy(
      {},
      {
        get() {
          throw new Error("SecurityError");
        },
      },
    );
    vi.stubGlobal("localStorage", throwing);

    expect(loadToken()).toBeNull();
    expect(() => saveToken("s3cret")).not.toThrow();
    expect(() => clearToken()).not.toThrow();
  });
});
