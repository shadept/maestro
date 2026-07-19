import path from "node:path";
import { Effect, Fiber, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { WorkerRuntime, type WorkerSpec } from "../../src/runtime/WorkerRuntime.ts";

/**
 * Integration suite against a REAL cluster (M2.11 acceptance criterion):
 * opt-in only, via the CI-provisioned kind cluster (.github/workflows/
 * k8s-runtime.yml) — never runs by default (unlike the local-cli docker
 * suite, most dev machines and this sandbox have no cluster at all).
 */
const enabled = process.env.MAESTRO_K8S_INTEGRATION_TESTS === "true";

const namespace = process.env.MAESTRO_K8S_NAMESPACE ?? "maestro-test";
const claimName = process.env.MAESTRO_K8S_STORAGE_CLAIM_NAME ?? "maestro-storage";
// Doesn't need to exist on the test runner's own filesystem — only used to
// compute the PVC subPath each mount renders to (see WorkerRuntime's
// k8sVolumeMount), the same way TurnExecutor's identity mounts are always
// paths under AppConfig.storageRoot.
const storageRoot = "/data";

const k8sLayer = WorkerRuntime.layerK8s.pipe(
  Layer.provide(
    AppConfig.layerTest({
      runtimeMode: "k8s",
      storageRoot,
      k8sNamespace: namespace,
      k8sStorageClaimName: claimName,
    }),
  ),
  Layer.orDie,
);

const run = <A, E>(effect: Effect.Effect<A, E, WorkerRuntime>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, k8sLayer));

const mount = (subPath: string, readOnly = false) => {
  const absolute = path.posix.join(storageRoot, subPath);
  return { hostPath: absolute, containerPath: absolute, readOnly };
};

const spec = (partial: Partial<WorkerSpec>): WorkerSpec => ({
  name: `maestro-k8s-test-${Math.random().toString(36).slice(2, 10)}`,
  image: "alpine:3",
  command: ["sh", "-c", "echo hello"],
  env: {},
  mounts: [],
  timeoutMillis: 60_000,
  ...partial,
});

describe.skipIf(!enabled)("WorkerRuntime.layerK8s (kind/k3d)", () => {
  it("captures stdout+stderr in order and reports the exit code", async () => {
    const { chunks, exit } = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: [
              "sh",
              "-c",
              "echo out-one; sleep 0.3; echo err-two >&2; sleep 0.3; echo out-three; exit 3",
            ],
          }),
        );
        const logsFiber = yield* Effect.forkChild(Stream.runCollect(runtime.logs(handle)));
        const exit = yield* runtime.wait(handle);
        const chunks = yield* Fiber.join(logsFiber);
        return { chunks, exit };
      }),
    );
    const joined = chunks.join("");
    expect(joined.indexOf("out-one")).toBeGreaterThanOrEqual(0);
    expect(joined.indexOf("out-one")).toBeLessThan(joined.indexOf("err-two"));
    expect(joined.indexOf("err-two")).toBeLessThan(joined.indexOf("out-three"));
    expect(exit.exitCode).toBe(3);
    expect(exit.cause).toBe("ERROR");
  }, 60_000);

  it("clean exit has no cause; env values reach the container via a Secret, not argv", async () => {
    const { chunks, exit } = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: ["sh", "-c", "echo value=$MAESTRO_TEST_SECRET"],
            env: { MAESTRO_TEST_SECRET: "s3cr3t-value" },
          }),
        );
        const logsFiber = yield* Effect.forkChild(Stream.runCollect(runtime.logs(handle)));
        const exit = yield* runtime.wait(handle);
        const chunks = yield* Fiber.join(logsFiber);
        return { chunks, exit };
      }),
    );
    expect(chunks.join("")).toContain("value=s3cr3t-value");
    expect(exit.exitCode).toBe(0);
    expect(exit.cause).toBeNull();
  }, 60_000);

  it("resource spec renders as requests/limits without breaking the run (M2.5)", async () => {
    const exit = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: ["sh", "-c", "echo ok"],
            memoryRequestMib: 64,
            memoryLimitMib: 128,
            cpuRequestMillicores: 250,
          }),
        );
        return yield* runtime.wait(handle);
      }),
    );
    expect(exit.exitCode).toBe(0);
    expect(exit.cause).toBeNull();
  }, 60_000);

  it("activeDeadlineSeconds kills the worker and classifies as TIMEOUT", async () => {
    const exit = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({ command: ["sleep", "60"], timeoutMillis: 5_000 }),
        );
        return yield* runtime.wait(handle);
      }),
    );
    expect(exit.cause).toBe("TIMEOUT");
  }, 90_000);

  it("OOM: a memory bomb under a tiny limit classifies as OOM", async () => {
    const exit = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: ["sh", "-c", 'x="a"; while true; do x="$x$x"; done'],
            memoryRequestMib: 16,
            memoryLimitMib: 16,
          }),
        );
        return yield* runtime.wait(handle);
      }),
    );
    expect(exit.cause).toBe("OOM");
  }, 60_000);

  it("kill works mid-run and classifies as CANCELLED; status flips to EXITED", async () => {
    const { before, after, exit } = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(spec({ command: ["sleep", "60"] }));
        // Give the scheduler/kubelet time to actually start the container.
        yield* Effect.sleep(3_000);
        const before = yield* runtime.status(handle);
        yield* runtime.kill(handle);
        const exit = yield* runtime.wait(handle);
        const after = yield* runtime.status(handle);
        return { before, after, exit };
      }),
    );
    expect(before).toBe("RUNNING");
    expect(exit.cause).toBe("CANCELLED");
    expect(after).toBe("EXITED");
  }, 60_000);

  it("status resolves by pod name across runtime instances (restart survival)", async () => {
    const name = `maestro-k8s-test-${Math.random().toString(36).slice(2, 10)}`;
    // Every run() through this shared k8sLayer builds against the SAME live
    // cluster but starts from a fresh in-memory worker map each time this
    // helper is (re)provided — good enough to simulate an orchestrator
    // restart: the handle is reconstructed from the pod name alone, exactly
    // FUR-40's cross-restart contract, unchanged by which runtime backs it.
    await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(spec({ name, command: ["sleep", "30"] }));
        expect(handle.id).toBe(name);
      }),
    );

    const deadline = Date.now() + 30_000;
    let status: string | null = null;
    while (Date.now() < deadline) {
      status = await run(
        Effect.gen(function* () {
          const runtime = yield* WorkerRuntime;
          return yield* runtime.status({ id: name });
        }),
      );
      if (status === "RUNNING") break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
    expect(status).toBe("RUNNING");

    await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        yield* runtime.kill({ id: name });
      }),
    );

    let gone = false;
    const goneDeadline = Date.now() + 30_000;
    while (!gone && Date.now() < goneDeadline) {
      const probe = await run(
        Effect.gen(function* () {
          const runtime = yield* WorkerRuntime;
          return yield* Effect.result(runtime.status({ id: name }));
        }),
      );
      gone = probe._tag === "Failure" && probe.failure._tag === "WorkerNotFoundError";
      if (!gone) await new Promise((resolve) => setTimeout(resolve, 500));
    }
    expect(gone).toBe(true);
  }, 90_000);

  it("status for a pod name no runtime has ever seen fails WorkerNotFoundError", async () => {
    const error = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        return yield* runtime
          .status({ id: "maestro-k8s-test-definitely-not-there" })
          .pipe(Effect.flip);
      }),
    );
    expect(error._tag).toBe("WorkerNotFoundError");
  });

  it("unknown handle fails wait with WorkerNotFoundError", async () => {
    const waitError = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        return yield* runtime.wait({ id: "nope" }).pipe(Effect.flip);
      }),
    );
    expect(waitError._tag).toBe("WorkerNotFoundError");
  });

  it("evict + rehydrate cycle preserves worktree contents on the shared PVC", async () => {
    const subPath = `worktrees/persist-test-${Math.random().toString(36).slice(2, 10)}`;
    const containerPath = path.posix.join(storageRoot, subPath);

    // Worker A: writes a marker file, then is killed mid-run (evict) — the
    // write from before the kill must survive on the PVC.
    await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: [
              "sh",
              "-c",
              `mkdir -p ${containerPath} && echo before-evict > ${containerPath}/marker.txt && sleep 60`,
            ],
            mounts: [mount(subPath)],
          }),
        );
        yield* Effect.sleep(3_000);
        yield* runtime.kill(handle);
        yield* runtime.wait(handle);
      }),
    );

    // Worker B: fresh pod, same subPath — rehydrate and read the marker back.
    const { chunks, exit } = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({
            command: ["cat", `${containerPath}/marker.txt`],
            mounts: [mount(subPath)],
          }),
        );
        const logsFiber = yield* Effect.forkChild(Stream.runCollect(runtime.logs(handle)));
        const exit = yield* runtime.wait(handle);
        const chunks = yield* Fiber.join(logsFiber);
        return { chunks, exit };
      }),
    );
    expect(exit.exitCode).toBe(0);
    expect(chunks.join("")).toContain("before-evict");
  }, 90_000);
});
