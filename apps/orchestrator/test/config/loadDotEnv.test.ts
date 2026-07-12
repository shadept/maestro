import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";

import { loadDotEnv } from "../../src/config/loadDotEnv.ts";

// Keys are namespaced per-test-file so parallel suites can't collide, and
// restored after each test to keep process.env clean.
const KEYS = ["MAESTRO_DOTENV_TEST_A", "MAESTRO_DOTENV_TEST_B"] as const;

let tempDirs: string[] = [];

const makeDotEnv = (contents: string): string => {
  const dir = mkdtempSync(join(tmpdir(), "maestro-dotenv-"));
  tempDirs.push(dir);
  const path = join(dir, ".env");
  writeFileSync(path, contents);
  return path;
};

afterEach(() => {
  for (const key of KEYS) delete process.env[key];
  for (const dir of tempDirs) rmSync(dir, { recursive: true, force: true });
  tempDirs = [];
});

describe("loadDotEnv", () => {
  it("never fails when .env files are missing", () => {
    expect(() => loadDotEnv(["/nonexistent/.env", "/also/nonexistent/.env"])).not.toThrow();
  });

  it("seeds process.env from a .env file", () => {
    const path = makeDotEnv("MAESTRO_DOTENV_TEST_A=from-file\n");
    loadDotEnv([path]);
    expect(process.env.MAESTRO_DOTENV_TEST_A).toBe("from-file");
  });

  it("real environment variables win over .env values", () => {
    process.env.MAESTRO_DOTENV_TEST_A = "from-env";
    const path = makeDotEnv("MAESTRO_DOTENV_TEST_A=from-file\n");
    loadDotEnv([path]);
    expect(process.env.MAESTRO_DOTENV_TEST_A).toBe("from-env");
  });

  it("earlier files win over later files, missing entries fall through", () => {
    const first = makeDotEnv("MAESTRO_DOTENV_TEST_A=first\n");
    const second = makeDotEnv("MAESTRO_DOTENV_TEST_A=second\nMAESTRO_DOTENV_TEST_B=second-only\n");
    loadDotEnv([first, second]);
    expect(process.env.MAESTRO_DOTENV_TEST_A).toBe("first");
    expect(process.env.MAESTRO_DOTENV_TEST_B).toBe("second-only");
  });
});
