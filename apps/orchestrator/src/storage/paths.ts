import path from "node:path";
import type { ProjectId, SessionId } from "@maestro/domain";

// Single source of truth for the on-disk layout under the storage root.
// Everything Maestro persists on the volume hangs off these three trees.

export const repoCacheDir = (storageRoot: string, projectId: ProjectId): string =>
  path.join(storageRoot, "repos", `${projectId}.git`);

export const worktreeDir = (storageRoot: string, sessionId: SessionId): string =>
  path.join(storageRoot, "worktrees", sessionId);

/** Per-session CLAUDE_CONFIG_DIR, persisted across turns (PRD §3.3). */
export const sessionConfigDir = (storageRoot: string, sessionId: SessionId): string =>
  path.join(storageRoot, "sessions", sessionId);
