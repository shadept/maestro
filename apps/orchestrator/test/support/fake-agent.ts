import { execFileSync } from "node:child_process";
import path from "node:path";

// Shared fake-agent image helpers (FUR-14 harness, extracted for FUR-19):
// a `claude` shell script honoring the FUR-12 stream-json contract — emits
// valid events and (by default) commits a file into the mounted worktree.
// Built once per suite from test/fixtures/fake-agent; zero API calls ever.

export const FAKE_AGENT_IMAGE = "maestro-fake-agent:local";

export const buildFakeAgentImage = (): void => {
  execFileSync(
    "docker",
    ["build", "-t", FAKE_AGENT_IMAGE, path.resolve(import.meta.dirname, "../fixtures/fake-agent")],
    { stdio: "pipe" },
  );
};

/**
 * Workers run as uid 1000, so on Linux hosts the mounts can accumulate files
 * the test process (a different uid) cannot delete — clean the storage tree
 * with the same runtime (explicitly as root, overriding the image's non-root
 * USER) before the host-side rm.
 */
export const cleanStorageViaContainer = (root: string, storageRoot: string): void => {
  try {
    execFileSync(
      "docker",
      [
        "run",
        "--rm",
        "-u",
        "0",
        "-v",
        `${root}:${root}`,
        FAKE_AGENT_IMAGE,
        "rm",
        "-rf",
        storageRoot,
      ],
      { stdio: "pipe" },
    );
  } catch {
    // best effort — the caller's plain rm handles the macOS case
  }
};
