import { configDefaults, defineConfig } from "vitest/config";

// Suites that spawn the `docker` CLI binary directly (WorkerRuntime's
// local-cli runtime, or the shared fake-agent harness's `docker build`/
// `docker run`/`docker rm` — see test/support/fake-agent.ts) — as opposed to
// testcontainers-based suites, which talk to the Docker socket directly and
// never need the CLI on PATH. Excluded only when MAESTRO_SKIP_DOCKER_TESTS
// is set: the base-image dogfood job (images/base, FUR-34) mounts the host's
// docker socket but the base image itself ships no docker CLI — workers
// never need one, only this test harness does. The host `ci` job leaves the
// env var unset and always runs the full suite unchanged.
const DOCKER_CLI_TESTS = [
  "test/runtime/worker-runtime.test.ts",
  "test/observability/tracing.test.ts",
  "test/engine/circuit-breaker.test.ts",
  "test/engine/session-terminator.test.ts",
  "test/engine/turn-executor.test.ts",
  "test/engine/startup-reconciler.test.ts",
];

export default defineConfig({
  test: {
    // Testcontainers: image pull + container boot can be slow on first run.
    hookTimeout: 120_000,
    testTimeout: 60_000,
    exclude: [
      ...configDefaults.exclude,
      ...(process.env.MAESTRO_SKIP_DOCKER_TESTS === "true" ? DOCKER_CLI_TESTS : []),
    ],
  },
});
