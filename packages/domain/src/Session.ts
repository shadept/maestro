import { Schema } from "effect";
import { ProjectId, SessionId } from "./ids.ts";
import { TicketReference } from "./TaskContext.ts";

// Session state machine (PRD §5.2). Sessions loop between warm and dormant
// across many turns; TERMINATED is terminal (worktree and config destroyed).

export const SessionState = Schema.Literals(["WARM_IDLE", "DORMANT_SAVED", "TERMINATED"]);
export type SessionState = typeof SessionState.Type;

export const sessionTransitions: Record<SessionState, ReadonlyArray<SessionState>> = {
  // eviction (cooldown / LRU preemption) or terminal signal
  WARM_IDLE: ["DORMANT_SAVED", "TERMINATED"],
  // rehydrate on next turn, or terminal signal / retention expiry
  DORMANT_SAVED: ["WARM_IDLE", "TERMINATED"],
  TERMINATED: [],
};

export const canSessionTransition = (from: SessionState, to: SessionState): boolean =>
  sessionTransitions[from].includes(to);

export const Session = Schema.Struct({
  id: SessionId,
  projectId: ProjectId,
  ticketReference: TicketReference,
  gitBranch: Schema.NonEmptyString,
  /** Claude session uuid for --resume; null until the first turn has run. */
  claudeSessionUuid: Schema.NullOr(Schema.String.check(Schema.isUUID())),
  /** Forge PR number; null until the orchestrator's first push opens the draft PR. */
  prNumber: Schema.NullOr(Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0))),
  /** Forge PR html URL, linked from ticket callbacks. Set together with prNumber. */
  prUrl: Schema.NullOr(Schema.NonEmptyString),
  state: SessionState,
  /**
   * When the ticket's terminal signal arrived while a turn was still active:
   * teardown is deferred until that turn settles, and this persisted marker is
   * what survives in between (set once, first signal wins). Null = no terminal
   * signal received.
   */
  terminationRequestedAt: Schema.NullOr(Schema.Date),
  createdAt: Schema.Date,
  /** Drives LRU eviction ordering. Updated on every turn activity. */
  lastActivityAt: Schema.Date,
});
export type Session = typeof Session.Type;
