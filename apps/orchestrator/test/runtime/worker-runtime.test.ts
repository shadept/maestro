import { Effect, Exit, Fiber, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { WorkerRuntime, type WorkerSpec } from "../../src/runtime/WorkerRuntime.ts";

const localLayer = WorkerRuntime.layerLocalCli.pipe(
  Layer.provide(AppConfig.layerTest()),
  Layer.orDie,
);

const k8sByConfig = WorkerRuntime.layerFromConfig.pipe(
  Layer.provide(AppConfig.layerTest({ runtimeMode: "k8s" })),
  Layer.orDie,
);

const run = <A, E>(effect: Effect.Effect<A, E, WorkerRuntime>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, localLayer));

const spec = (partial: Partial<WorkerSpec>): WorkerSpec => ({
  name: `maestro-test-${Math.random().toString(36).slice(2, 10)}`,
  image: "alpine:3",
  command: ["sh", "-c", "echo hello"],
  env: {},
  mounts: [],
  timeoutMillis: 60_000,
  ...partial,
});

describe("WorkerRuntime.layerLocalCli (docker)", () => {
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
  });

  it("clean exit has no cause; env values reach the container without argv", async () => {
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
  });

  it("timeout kills the worker and classifies the cause", async () => {
    const exit = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(
          spec({ command: ["sleep", "30"], timeoutMillis: 1_500 }),
        );
        return yield* runtime.wait(handle);
      }),
    );
    expect(exit.cause).toBe("TIMEOUT");
  }, 30_000);

  it("kill works mid-run and classifies as CANCELLED; status flips to EXITED", async () => {
    const { before, after, exit } = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        const handle = yield* runtime.start(spec({ command: ["sleep", "30"] }));
        const before = yield* runtime.status(handle);
        yield* Effect.sleep(300);
        yield* runtime.kill(handle);
        const exit = yield* runtime.wait(handle);
        const after = yield* runtime.status(handle);
        return { before, after, exit };
      }),
    );
    expect(before).toBe("RUNNING");
    expect(exit.cause).toBe("CANCELLED");
    expect(after).toBe("EXITED");
  }, 30_000);

  it("unknown handle fails with WorkerNotFoundError", async () => {
    const error = await run(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        return yield* runtime.wait({ id: "nope" }).pipe(Effect.flip);
      }),
    );
    expect(error._tag).toBe("WorkerNotFoundError");
  });

  it("spawn failure surfaces as WorkerSpawnError", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        return yield* runtime.start(spec({})).pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          WorkerRuntime.layerLocalCli.pipe(
            Layer.provide(
              AppConfig.layerTest({ runtimeTemplate: "definitely-not-a-real-binary run" }),
            ),
            Layer.orDie,
          ),
        ),
      ),
    );
    expect(error._tag).toBe("WorkerSpawnError");
  });
});

describe("WorkerRuntime.layerK8sNoop via config", () => {
  it("config runtimeMode=k8s selects the no-op layer; methods fail NotImplementedError", async () => {
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const runtime = yield* WorkerRuntime;
        return yield* runtime.start(spec({}));
      }).pipe(Effect.provide(k8sByConfig)),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) {
      expect(String(exit.cause)).toContain("NotImplementedError");
    }
  });
});
