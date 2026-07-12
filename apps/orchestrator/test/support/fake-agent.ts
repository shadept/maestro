import { execFileSync } from "node:child_process";
import path from "node:path";

// Shared fake-agent image helpers (FUR-14 harness, extracted for FUR-19):
// a `claude` shell script honoring the FUR-12 stream-json contract — emits
// valid events and (by default) commits a file into the mounted worktree.
// Built once per suite from test/fixtures/fake-agent; zero API calls ever.

export const FAKE_AGENT_IMAGE = "maestro-fake-agent:local";

/**
 * Runtime template for suites that bind-mount harness-provisioned directories
 * (worktrees, bare repos, config dirs) into the fake agent.
 *
 * On Linux hosts bind mounts preserve real ownership, and the image's baked-in
 * uid 1000 does not necessarily match the test process (GitHub runners are uid
 * 1001) — the agent then cannot write the mounted worktree at all. Running the
 * container as the CURRENT uid/gid is correct by construction: the test
 * process created every mounted path, so it always owns them. On macOS Docker
 * Desktop ownership is mapped loosely and this is effectively a no-op.
 *
 * HOME=/tmp because an arbitrary -u uid has no passwd entry and cannot write
 * the image's /home/agent; the fake agent's `git config --global` needs a
 * writable HOME, and a container-private /tmp (mode 1777) always is.
 */
export const fakeAgentRuntimeTemplate = (): string =>
  `docker run -u ${process.getuid?.() ?? 0}:${process.getgid?.() ?? 0} -e HOME=/tmp`;

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
