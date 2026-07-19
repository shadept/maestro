import { type ChildProcess, execFile, spawn } from "node:child_process";
import {
  NotImplementedError,
  type RuntimeError,
  type TaskRunCause,
  WorkerNotFoundError,
  WorkerSpawnError,
} from "@maestro/domain";
import { type Cause, Context, Deferred, Effect, Layer, Queue, Stream } from "effect";
import { AppConfig } from "../config/AppConfig.ts";

export interface WorkerMount {
  readonly hostPath: string;
  readonly containerPath: string;
  readonly readOnly: boolean;
}

export interface WorkerSpec {
  /** Container name — also the target for external kills. */
  readonly name: string;
  readonly image: string;
  readonly command: ReadonlyArray<string>;
  /** Values are passed via the client process environment, never argv. */
  readonly env: Readonly<Record<string, string>>;
  readonly mounts: ReadonlyArray<WorkerMount>;
  readonly workdir?: string;
  /**
   * Two-tier resource spec (M2.5, Tech Requirements §8): request renders as
   * docker's soft `--memory-reservation`, limit as the hard `--memory` (the
   * cap whose breach triggers the OOM kill classified below). CPU is a
   * request only — soft (`--cpu-shares`), never hard-capped.
   */
  readonly memoryRequestMib?: number;
  readonly memoryLimitMib?: number;
  readonly cpuRequestMillicores?: number;
  readonly timeoutMillis: number;
}

/**
 * Docker's `--cpu-shares` is a relative weight, not an absolute quantity —
 * 1024 is the daemon's own default and conventionally represents "one CPU's
 * worth" of weight, so millicores convert on that same 1000m == 1024 scale.
 * The engine floors shares at 2 (its documented minimum).
 */
const dockerCpuShares = (cpuRequestMillicores: number): number =>
  Math.max(2, Math.round((cpuRequestMillicores / 1000) * 1024));

/**
 * Handle id = the container name (WorkerSpec.name). Deterministic on purpose:
 * a handle can be reconstructed from the name alone, so `status` works across
 * orchestrator restarts (startup reconciliation, FUR-40) — the in-memory
 * worker map is only a fast path for workers this process started.
 */
export interface WorkerHandle {
  readonly id: string;
}

export type WorkerStatus = "RUNNING" | "EXITED";

export interface ExitInfo {
  readonly exitCode: number | null;
  /** null = clean exit. OOM is a heuristic in local mode (exit 137, unprompted). */
  readonly cause: TaskRunCause | null;
}

interface LocalWorker {
  readonly name: string;
  readonly child: ChildProcess;
  readonly logs: Queue.Queue<string, Cause.Done>;
  readonly exit: Deferred.Deferred<ExitInfo>;
  killCause: TaskRunCause | null;
  exited: boolean;
  timer: NodeJS.Timeout | null;
}

export class WorkerRuntime extends Context.Service<
  WorkerRuntime,
  {
    readonly start: (spec: WorkerSpec) => Effect.Effect<WorkerHandle, RuntimeError>;
    /** Interleaved stdout+stderr in arrival order. Single consumer. */
    readonly logs: (handle: WorkerHandle) => Stream.Stream<string, RuntimeError>;
    readonly wait: (handle: WorkerHandle) => Effect.Effect<ExitInfo, RuntimeError>;
    readonly kill: (handle: WorkerHandle) => Effect.Effect<void, RuntimeError>;
    readonly status: (handle: WorkerHandle) => Effect.Effect<WorkerStatus, RuntimeError>;
  }
>()("maestro/runtime/WorkerRuntime") {
  static readonly layerLocalCli = Layer.effect(
    WorkerRuntime,
    Effect.gen(function* () {
      const { runtimeTemplate } = yield* AppConfig;
      const templateArgv = runtimeTemplate.split(/\s+/).filter((s) => s.length > 0);
      const workers = new Map<string, LocalWorker>();

      const get = (handle: WorkerHandle) => {
        const worker = workers.get(handle.id);
        return worker
          ? Effect.succeed(worker)
          : Effect.fail(new WorkerNotFoundError({ workerId: handle.id }));
      };

      const killWorker = (worker: LocalWorker, cause: TaskRunCause) => {
        if (worker.exited) return;
        worker.killCause ??= cause;
        // Ask the runtime to kill the container (client exits on its own),
        // then hard-kill the client as a fallback for non-container templates.
        const runtimeBin = templateArgv[0];
        if (runtimeBin) {
          execFile(runtimeBin, ["kill", worker.name], () => {
            // best effort — template may not support `kill`
          });
        }
        setTimeout(() => {
          if (!worker.exited) worker.child.kill("SIGKILL");
        }, 2_000);
      };

      const renderArgv = (spec: WorkerSpec): ReadonlyArray<string> => [
        ...templateArgv.slice(1),
        "--rm",
        "--name",
        spec.name,
        ...spec.mounts.flatMap((m) => [
          "-v",
          `${m.hostPath}:${m.containerPath}${m.readOnly ? ":ro" : ""}`,
        ]),
        // name-only -e: the runtime reads the value from the client env,
        // keeping secrets out of argv and process lists
        ...Object.keys(spec.env).flatMap((k) => ["-e", k]),
        ...(spec.workdir ? ["-w", spec.workdir] : []),
        ...(spec.memoryRequestMib ? ["--memory-reservation", `${spec.memoryRequestMib}m`] : []),
        ...(spec.memoryLimitMib ? ["--memory", `${spec.memoryLimitMib}m`] : []),
        ...(spec.cpuRequestMillicores
          ? ["--cpu-shares", `${dockerCpuShares(spec.cpuRequestMillicores)}`]
          : []),
        spec.image,
        ...spec.command,
      ];

      return {
        start: Effect.fn("WorkerRuntime.start")(function* (spec: WorkerSpec) {
          const bin = templateArgv[0];
          if (!bin) {
            return yield* new WorkerSpawnError({ reason: "empty runtime template" });
          }
          const logs = yield* Queue.unbounded<string, Cause.Done>();
          const exit = yield* Deferred.make<ExitInfo>();

          const worker = yield* Effect.callback<LocalWorker, WorkerSpawnError>((resume) => {
            const child = spawn(bin, renderArgv(spec), {
              env: { ...process.env, ...spec.env },
              stdio: ["ignore", "pipe", "pipe"],
            });
            const w: LocalWorker = {
              name: spec.name,
              child,
              logs,
              exit,
              killCause: null,
              exited: false,
              timer: null,
            };
            child.stdout?.setEncoding("utf8");
            child.stderr?.setEncoding("utf8");
            child.stdout?.on("data", (chunk: string) => Queue.offerUnsafe(logs, chunk));
            child.stderr?.on("data", (chunk: string) => Queue.offerUnsafe(logs, chunk));
            child.once("spawn", () => resume(Effect.succeed(w)));
            child.once("error", (error) => {
              w.exited = true;
              Queue.endUnsafe(logs);
              Deferred.doneUnsafe(exit, Effect.succeed({ exitCode: null, cause: "ERROR" }));
              resume(Effect.fail(new WorkerSpawnError({ reason: error.message })));
            });
            child.once("close", (code) => {
              w.exited = true;
              if (w.timer) clearTimeout(w.timer);
              Queue.endUnsafe(logs);
              const cause: TaskRunCause | null = w.killCause
                ? w.killCause
                : code === 0
                  ? null
                  : code === 137
                    ? "OOM"
                    : "ERROR";
              Deferred.doneUnsafe(exit, Effect.succeed({ exitCode: code, cause }));
            });
          });

          worker.timer = setTimeout(() => killWorker(worker, "TIMEOUT"), spec.timeoutMillis);
          const handle: WorkerHandle = { id: spec.name };
          workers.set(handle.id, worker);
          return handle;
        }),
        logs: (handle: WorkerHandle) =>
          Stream.unwrap(Effect.map(get(handle), (worker) => Stream.fromQueue(worker.logs))),
        wait: Effect.fn("WorkerRuntime.wait")(function* (handle: WorkerHandle) {
          const worker = yield* get(handle);
          return yield* Deferred.await(worker.exit);
        }),
        kill: Effect.fn("WorkerRuntime.kill")(function* (handle: WorkerHandle) {
          const worker = yield* get(handle);
          killWorker(worker, "CANCELLED");
        }),
        status: Effect.fn("WorkerRuntime.status")(function* (handle: WorkerHandle) {
          const worker = workers.get(handle.id);
          if (worker) return worker.exited ? ("EXITED" as const) : ("RUNNING" as const);
          // Cross-restart lookup (FUR-40): the handle id is the container
          // name, so a worker started by a previous orchestrator process is
          // still addressable — ask the runtime. Any inspect failure (unknown
          // name, daemon down, template without `inspect`) reads as
          // not-found: if the runtime cannot see the container, neither can a
          // turn — treating it as gone is the safe answer for reconciliation.
          const bin = templateArgv[0];
          if (!bin) {
            return yield* new WorkerNotFoundError({ workerId: handle.id });
          }
          return yield* Effect.callback<WorkerStatus, WorkerNotFoundError>((resume) => {
            execFile(
              bin,
              ["inspect", "--format", "{{.State.Running}}", handle.id],
              (error, stdout) => {
                if (error) {
                  resume(Effect.fail(new WorkerNotFoundError({ workerId: handle.id })));
                } else {
                  resume(Effect.succeed(stdout.trim() === "true" ? "RUNNING" : "EXITED"));
                }
              },
            );
          });
        }),
      };
    }),
  );

  /**
   * Compiling no-op proving the runtime seam: the full interface, every
   * method failing with NotImplementedError. Replaced by the real K8s
   * implementation in M2.11 without touching callers.
   */
  static readonly layerK8sNoop = Layer.succeed(WorkerRuntime)({
    start: () => Effect.fail(new NotImplementedError({ feature: "WorkerRuntime.k8s.start" })),
    logs: () => Stream.fail(new NotImplementedError({ feature: "WorkerRuntime.k8s.logs" })),
    wait: () => Effect.fail(new NotImplementedError({ feature: "WorkerRuntime.k8s.wait" })),
    kill: () => Effect.fail(new NotImplementedError({ feature: "WorkerRuntime.k8s.kill" })),
    status: () => Effect.fail(new NotImplementedError({ feature: "WorkerRuntime.k8s.status" })),
  });

  /** Config-driven selection, used by the composition root. */
  static readonly layerFromConfig = Layer.unwrap(
    Effect.gen(function* () {
      const { runtimeMode } = yield* AppConfig;
      return runtimeMode === "k8s" ? WorkerRuntime.layerK8sNoop : WorkerRuntime.layerLocalCli;
    }),
  );
}
