import type { TurnOutcomePayload } from "../engine/TurnExecutor.ts";

// Platform-agnostic turn-result rendering (markdown works on every target we
// plan for). The generic-API callback (M2) reuses this; only delivery is
// platform-specific.

/** The ticket comment body for a settled turn. */
export const formatTurnComment = (outcome: TurnOutcomePayload): string => {
  const header =
    outcome.kind === "turn-completed"
      ? "**Maestro** — turn completed."
      : `**Maestro** — turn failed (${outcome.cause ?? "ERROR"}).`;
  const parts = [header, outcome.summary.trim()];
  if (outcome.pr !== null) {
    parts.push(`Pull request: ${outcome.pr.url}`);
  }
  return parts.filter((part) => part.length > 0).join("\n\n");
};
