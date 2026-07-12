import type { TurnOutcomePayload } from "../engine/TurnExecutor.ts";

// Platform-agnostic turn-result rendering (markdown works on every target we
// plan for). The generic-API callback (M2) reuses this; only delivery is
// platform-specific.

/**
 * Every Maestro-authored ticket comment starts with this marker. Ingest uses
 * it as a content-based self-trigger guard (FUR-39 layer 1): in single-account
 * setups the bot-user-id guard cannot distinguish Maestro from the human, so
 * the comment body itself must. Single source of truth — comment bodies below
 * are built from it, and LinearIngest imports it; never inline the string.
 */
export const MAESTRO_COMMENT_MARKER = "**Maestro** —";

/** The ticket comment body for a settled turn. */
export const formatTurnComment = (outcome: TurnOutcomePayload): string => {
  const header =
    outcome.kind === "turn-completed"
      ? `${MAESTRO_COMMENT_MARKER} turn completed.`
      : `${MAESTRO_COMMENT_MARKER} turn failed (${outcome.cause ?? "ERROR"}).`;
  const parts = [header, outcome.summary.trim()];
  if (outcome.pr !== null) {
    parts.push(`Pull request: ${outcome.pr.url}`);
  }
  return parts.filter((part) => part.length > 0).join("\n\n");
};
