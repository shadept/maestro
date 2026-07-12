import type { TaskRunId } from "@maestro/domain";

/**
 * Deterministic container name for a turn's worker. Shared by TurnExecutor
 * (starts the worker) and StartupReconciler (probes for it after a restart —
 * FUR-40): the name is the only cross-process handle on a worker, so both
 * sides must derive it identically.
 */
export const turnWorkerName = (taskRunId: TaskRunId): string => `maestro-turn-${taskRunId}`;
