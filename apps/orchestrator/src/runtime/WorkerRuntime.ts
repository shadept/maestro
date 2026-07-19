import { type ChildProcess, execFile, spawn } from "node:child_process";
import path from "node:path";
import { PassThrough } from "node:stream";
import * as k8s from "@kubernetes/client-node";
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

interface K8sWorker {
  readonly name: string;
  readonly secretName: string | null;
  readonly logs: Queue.Queue<string, Cause.Done>;
  readonly exit: Deferred.Deferred<ExitInfo>;
  killCause: TaskRunCause | null;
  exited: boolean;
  pollTimer: NodeJS.Timeout | null;
  logAbort: AbortController | null;
  /** Reassigned once attachWorker's poll loop exists — lets kill() force an immediate check instead of waiting out the interval. */
  pollNow: () => void;
}

const K8S_STORAGE_VOLUME_NAME = "storage";
const K8S_WORKER_CONTAINER_NAME = "worker";
const K8S_LOG_RETRY_MILLIS = 300;
const K8S_POLL_INTERVAL_MILLIS = 1_000;

const describeK8sError = (error: unknown): string => {
  if (error instanceof k8s.ApiException) {
    const body = error.body as { message?: string } | null | undefined;
    return body?.message ?? error.message;
  }
  return error instanceof Error ? error.message : String(error);
};

/**
 * Every WorkerSpec mount is an identity mount under AppConfig.storageRoot
 * (TurnExecutor.identityMounts): hostPath and containerPath are the same
 * absolute path, because the worktree's `.git` metadata bakes in that host
 * path and must resolve unchanged inside the worker. K8s has no shared host
 * filesystem namespace, so the trick is replayed with one PVC (storageRoot's
 * in-cluster backing volume) mounted once per host-identical containerPath,
 * each at the `subPath` its absolute path occupies under storageRoot.
 */
const k8sVolumeMount = (
  storageRoot: string,
  mount: WorkerMount,
): Effect.Effect<k8s.V1VolumeMount, WorkerSpawnError> =>
  Effect.gen(function* () {
    const relative = path.relative(storageRoot, mount.hostPath);
    if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
      return yield* new WorkerSpawnError({
        reason: `mount path ${mount.hostPath} is not under storageRoot ${storageRoot}`,
      });
    }
    return {
      name: K8S_STORAGE_VOLUME_NAME,
      mountPath: mount.containerPath,
      subPath: relative,
      readOnly: mount.readOnly,
    };
  });

/**
 * Renders the two-tier spec (M2.5) as resources.requests/limits. CPU
 * deliberately never gets a `limits` entry — Tech Requirements §8 wants a
 * soft request only, the same policy layerLocalCli enforces via
 * `--cpu-shares` (a weight, never a hard cap).
 */
const k8sResourceRequirements = (spec: WorkerSpec): k8s.V1ResourceRequirements => {
  const requests: Record<string, string> = {};
  const limits: Record<string, string> = {};
  if (spec.memoryRequestMib) requests.memory = `${spec.memoryRequestMib}Mi`;
  if (spec.memoryLimitMib) limits.memory = `${spec.memoryLimitMib}Mi`;
  if (spec.cpuRequestMillicores) requests.cpu = `${spec.cpuRequestMillicores}m`;
  const resources: k8s.V1ResourceRequirements = {};
  if (Object.keys(requests).length > 0) resources.requests = requests;
  if (Object.keys(limits).length > 0) resources.limits = limits;
  return resources;
};

/**
 * ExitInfo from a terminal pod (Succeeded/Failed) or one WE just deleted
 * (killCause set by kill() beforehand). Mirrors layerLocalCli's
 * classifyOutcome ladder: an explicit kill wins, then the kubelet's own
 * DeadlineExceeded verdict — activeDeadlineSeconds is enforced server-side,
 * so unlike local mode's JS setTimeout this survives an orchestrator
 * restart — then OOMKilled, then the container's own exit code.
 */
const classifyPod = (pod: k8s.V1Pod, killCause: TaskRunCause | null): ExitInfo => {
  const terminated = pod.status?.containerStatuses?.[0]?.state?.terminated;
  const exitCode = terminated?.exitCode ?? null;
  if (killCause) return { exitCode, cause: killCause };
  if (pod.status?.reason === "DeadlineExceeded") return { exitCode, cause: "TIMEOUT" };
  if (terminated?.reason === "OOMKilled") return { exitCode, cause: "OOM" };
  if (exitCode === 0) return { exitCode, cause: null };
  return { exitCode, cause: "ERROR" };
};

/**
 * Real WorkerRuntime.layerK8s (M2.11): one bare Pod per turn (restartPolicy
 * Never — no controller-level retries, matching the no-auto-retry policy
 * everywhere else in the engine), addressed directly by name (handle.id ===
 * spec.name === pod name, no Job-style name indirection to resolve). A
 * Job was considered and rejected: deleting a Job's pod directly (what
 * kill() needs, to classify the cause ourselves before it disappears) can
 * make the Job controller schedule a REPLACEMENT pod even at backoffLimit 0
 * (backoff counts container failures, not vanished pods) — exactly the
 * surprise a no-retry engine must not risk. A bare Pod has no such
 * controller second-guessing us.
 *
 * Status is resolved by polling (readNamespacedPod every second) rather than
 * the Watch API: turn-level granularity doesn't need sub-second precision,
 * and polling sidesteps long-lived-watch reconnect handling entirely —
 * fewer failure modes for the same correctness.
 */
const k8sWorkerRuntimeEffect = Effect.gen(function* () {
  const { storageRoot, k8sNamespace: namespace, k8sStorageClaimName } = yield* AppConfig;

  const kc = new k8s.KubeConfig();
  kc.loadFromDefault();
  const core = kc.makeApiClient(k8s.CoreV1Api);
  const log = new k8s.Log(kc);

  const workers = new Map<string, K8sWorker>();

  const get = (handle: WorkerHandle) => {
    const worker = workers.get(handle.id);
    return worker
      ? Effect.succeed(worker)
      : Effect.fail(new WorkerNotFoundError({ workerId: handle.id }));
  };

  const secretNameFor = (workerName: string) => `${workerName}-env`;

  /**
   * Streams logs into the worker's Queue and resolves its exit Deferred —
   * the K8s analogue of layerLocalCli's ChildProcess event listeners. Runs
   * detached from the returned start() Effect (fire-and-forget, exactly like
   * the docker client's own event loop): start() only waits for the pod to
   * be accepted by the API server, same as local mode only waiting for spawn.
   */
  const attachWorker = (worker: K8sWorker) => {
    const finalize = (pod: k8s.V1Pod | null) => {
      if (worker.exited) return;
      worker.exited = true;
      if (worker.pollTimer) clearInterval(worker.pollTimer);
      worker.logAbort?.abort();
      const exitInfo = pod
        ? classifyPod(pod, worker.killCause)
        : { exitCode: null, cause: worker.killCause ?? ("ERROR" as const) };
      Queue.endUnsafe(worker.logs);
      Deferred.doneUnsafe(worker.exit, Effect.succeed(exitInfo));
      // Best-effort cleanup — the K8s analogue of docker's --rm. Never
      // blocks the turn on cluster cleanup latency; a failure here just
      // leaves a terminal pod for a future janitor sweep (M2/FUR-23
      // territory) — no different in kind from a settled TaskRun whose
      // container outlived a crash before local-cli's --rm removed it.
      core
        .deleteNamespacedPod({ name: worker.name, namespace, gracePeriodSeconds: 0 })
        .catch(() => {});
      if (worker.secretName) {
        const secretName = worker.secretName;
        core.deleteNamespacedSecret({ name: secretName, namespace }).catch(() => {});
      }
    };

    const poll = () => {
      if (worker.exited) return;
      core
        .readNamespacedPod({ name: worker.name, namespace })
        .then((pod) => {
          const phase = pod.status?.phase;
          if (phase === "Succeeded" || phase === "Failed") finalize(pod);
        })
        .catch((error: unknown) => {
          // A 404 means the pod is genuinely gone (our own cleanup racing a
          // second poll tick, or an out-of-band delete) — anything else is a
          // transient API hiccup, retried on the next tick rather than
          // mistaken for the worker's own exit.
          if (error instanceof k8s.ApiException && error.code === 404) finalize(null);
        });
    };
    worker.pollNow = poll;
    worker.pollTimer = setInterval(poll, K8S_POLL_INTERVAL_MILLIS);
    poll();

    const streamLogs = async () => {
      for (;;) {
        if (worker.exited) return;
        const sink = new PassThrough();
        sink.setEncoding("utf8");
        sink.on("data", (chunk: string) => Queue.offerUnsafe(worker.logs, chunk));
        try {
          worker.logAbort = await log.log(namespace, worker.name, K8S_WORKER_CONTAINER_NAME, sink, {
            follow: true,
          });
          await new Promise<void>((resolve) => {
            sink.once("end", resolve);
            sink.once("close", resolve);
            sink.once("error", resolve);
          });
          return; // the connection closed: the container exited (or finalize aborted it)
        } catch {
          if (worker.exited) return;
          // container not started yet (ContainerCreating/ImagePullBackOff) —
          // retry until it is, or until finalize() ends things (e.g. the
          // pod's own activeDeadlineSeconds firing while still Pending).
          await new Promise((resolve) => setTimeout(resolve, K8S_LOG_RETRY_MILLIS));
        }
      }
    };
    void streamLogs();
  };

  return {
    start: Effect.fn("WorkerRuntime.start")(function* (spec: WorkerSpec) {
      const volumeMounts = yield* Effect.forEach(spec.mounts, (m) =>
        k8sVolumeMount(storageRoot, m),
      );
      const hasEnv = Object.keys(spec.env).length > 0;
      const secretName = hasEnv ? secretNameFor(spec.name) : null;

      if (secretName) {
        yield* Effect.tryPromise({
          try: () =>
            core.createNamespacedSecret({
              namespace,
              body: { metadata: { name: secretName, namespace }, stringData: spec.env },
            }),
          catch: (error) =>
            new WorkerSpawnError({ reason: `secret create failed: ${describeK8sError(error)}` }),
        });
      }

      const podManifest: k8s.V1Pod = {
        metadata: {
          name: spec.name,
          namespace,
          labels: { "app.kubernetes.io/managed-by": "maestro" },
        },
        spec: {
          restartPolicy: "Never",
          // Workers hold no credentials and never talk to the K8s API.
          automountServiceAccountToken: false,
          // Enforced server-side by the kubelet — survives an orchestrator
          // restart, unlike layerLocalCli's JS setTimeout kill.
          activeDeadlineSeconds: Math.max(1, Math.ceil(spec.timeoutMillis / 1_000)),
          containers: [
            {
              name: K8S_WORKER_CONTAINER_NAME,
              image: spec.image,
              command: [...spec.command],
              ...(secretName ? { envFrom: [{ secretRef: { name: secretName } }] } : {}),
              ...(spec.workdir ? { workingDir: spec.workdir } : {}),
              volumeMounts,
              resources: k8sResourceRequirements(spec),
            },
          ],
          volumes: [
            {
              name: K8S_STORAGE_VOLUME_NAME,
              persistentVolumeClaim: { claimName: k8sStorageClaimName },
            },
          ],
        },
      };

      yield* Effect.tryPromise({
        try: () => core.createNamespacedPod({ namespace, body: podManifest }),
        catch: (error) =>
          new WorkerSpawnError({ reason: `pod create failed: ${describeK8sError(error)}` }),
      }).pipe(
        Effect.tapError(() =>
          secretName
            ? Effect.promise(() =>
                core.deleteNamespacedSecret({ name: secretName, namespace }).catch(() => {}),
              )
            : Effect.void,
        ),
      );

      const logs = yield* Queue.unbounded<string, Cause.Done>();
      const exit = yield* Deferred.make<ExitInfo>();
      const worker: K8sWorker = {
        name: spec.name,
        secretName,
        logs,
        exit,
        killCause: null,
        exited: false,
        pollTimer: null,
        logAbort: null,
        pollNow: () => {},
      };
      workers.set(spec.name, worker);
      attachWorker(worker);

      return { id: spec.name };
    }),
    logs: (handle: WorkerHandle, opts?: { readonly sinceTime?: Date }) => {
      const worker = workers.get(handle.id);
      if (worker && !opts?.sinceTime) return Stream.fromQueue(worker.logs);
      // Cross-restart / explicit-sinceTime reattach (M2.11): no in-memory
      // worker survives an orchestrator restart, so fetch straight from the
      // pod log API instead of failing WorkerNotFoundError — following if
      // the pod is still alive, a bounded one-shot read otherwise.
      return Stream.unwrap(
        Effect.gen(function* () {
          const pod = yield* Effect.tryPromise({
            try: () => core.readNamespacedPod({ name: handle.id, namespace }),
            catch: () => new WorkerNotFoundError({ workerId: handle.id }),
          });
          const phase = pod.status?.phase;
          const follow = phase === "Running" || phase === "Pending";
          const sink = new PassThrough();
          sink.setEncoding("utf8");
          yield* Effect.tryPromise({
            try: () =>
              log.log(namespace, handle.id, K8S_WORKER_CONTAINER_NAME, sink, {
                follow,
                ...(opts?.sinceTime ? { sinceTime: opts.sinceTime.toISOString() } : {}),
              }),
            catch: () => new WorkerNotFoundError({ workerId: handle.id }),
          });
          return Stream.fromAsyncIterable<string, RuntimeError>(
            sink,
            () => new WorkerNotFoundError({ workerId: handle.id }),
          );
        }),
      );
    },
    wait: Effect.fn("WorkerRuntime.wait")(function* (handle: WorkerHandle) {
      const worker = yield* get(handle);
      return yield* Deferred.await(worker.exit);
    }),
    kill: Effect.fn("WorkerRuntime.kill")(function* (handle: WorkerHandle) {
      const worker = yield* get(handle);
      if (worker.exited) return;
      worker.killCause ??= "CANCELLED";
      core
        .deleteNamespacedPod({ name: worker.name, namespace, gracePeriodSeconds: 0 })
        .catch(() => {});
      worker.pollNow();
    }),
    status: Effect.fn("WorkerRuntime.status")(function* (handle: WorkerHandle) {
      const worker = workers.get(handle.id);
      if (worker) return worker.exited ? ("EXITED" as const) : ("RUNNING" as const);
      // Cross-restart lookup (FUR-40), same contract as layerLocalCli's
      // `docker inspect` fallback: any read failure reads as not-found.
      return yield* Effect.tryPromise({
        try: () => core.readNamespacedPod({ name: handle.id, namespace }),
        catch: () => new WorkerNotFoundError({ workerId: handle.id }),
      }).pipe(
        Effect.map((pod) =>
          pod.status?.phase === "Running" || pod.status?.phase === "Pending"
            ? ("RUNNING" as const)
            : ("EXITED" as const),
        ),
      );
    }),
  };
});

export class WorkerRuntime extends Context.Service<
  WorkerRuntime,
  {
    readonly start: (spec: WorkerSpec) => Effect.Effect<WorkerHandle, RuntimeError>;
    /**
     * Interleaved stdout+stderr in arrival order. Single consumer per live
     * worker. `opts.sinceTime` is a K8s-only reattach primitive (M2.11): when
     * the in-memory worker map misses (a fresh process, e.g. after a
     * restart), layerK8s falls back to fetching straight from the pod log
     * API instead of failing WorkerNotFoundError; layerLocalCli ignores it
     * (docker has no server-side log history to reattach to).
     */
    readonly logs: (
      handle: WorkerHandle,
      opts?: { readonly sinceTime?: Date },
    ) => Stream.Stream<string, RuntimeError>;
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

  static readonly layerK8s = Layer.effect(WorkerRuntime, k8sWorkerRuntimeEffect);

  /**
   * Compiling no-op proving the runtime seam: the full interface, every
   * method failing with NotImplementedError. Superseded by layerK8s (M2.11)
   * as the config-selected implementation; kept as a standalone layer for
   * tests that want a runtime guaranteed not to touch a real cluster.
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
      return runtimeMode === "k8s" ? WorkerRuntime.layerK8s : WorkerRuntime.layerLocalCli;
    }),
  );
}
