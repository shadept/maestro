import { createHash } from "node:crypto";
import {
  type DbError,
  type Session,
  SessionId,
  TaskRunCause,
  TaskRunId,
  TicketReference,
} from "@maestro/domain";
import { Context, Effect, Layer, Schema } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { AuditRepo } from "../db/AuditRepo.ts";
import { OutboxRepo } from "../db/OutboxRepo.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";

/**
 * Outbox payload written when a turn settles — or when the failure circuit
 * breaker pauses the session ("session-paused", FUR-39). The callback worker
 * (FUR-18) drains these entries and posts them back to the ticketing platform
 * identified by `ticket.source`. A Schema (not just a type) because the
 * worker decodes it back out of the outbox's jsonb column.
 */
export const TurnOutcomePayload = Schema.Struct({
  kind: Schema.Literals(["turn-completed", "turn-failed", "session-paused"]),
  taskRunId: TaskRunId,
  sessionId: SessionId,
  ticket: TicketReference,
  /** Final agent text on completion; failure summary on failure. */
  summary: Schema.String,
  cause: Schema.NullOr(TaskRunCause),
  /** The session's PR, so the ticket comment links it. Null until a first push. */
  pr: Schema.NullOr(Schema.Struct({ number: Schema.Number, url: Schema.String })),
});
export type TurnOutcomePayload = typeof TurnOutcomePayload.Type;

/** PR coordinates for the ticket comment. */
export interface PrReference {
  readonly number: number;
  readonly url: string;
}

/** The session's persisted PR reference, if the orchestrator has pushed before. */
export const prOf = (session: Session): PrReference | null =>
  session.prNumber !== null && session.prUrl !== null
    ? { number: session.prNumber, url: session.prUrl }
    : null;

/**
 * Failure circuit breaker (FUR-39 layer 2): this many consecutive FAILED
 * turns with no intervening success pauses the session — ingest stops
 * accepting auto-triggered turns until a human resumes it (mentions the agent
 * in a comment, or re-delegates the issue to it — FUR-37 mechanism).
 * In-code constant by design: a misconfigured deployment must
 * not be able to raise it. NOTE: resume does not reset the count (it is
 * derived from settled runs, not stored), so after a manual resume a single
 * further failure re-trips the breaker — deliberate: the session is still
 * suspect until a turn actually succeeds.
 */
export const CONSECUTIVE_FAILURE_LIMIT = 3;

/**
 * Outbox idempotency keys per payload kind (FUR-39 layer 3 lives here):
 * - turn-completed: one per turn — replayed settlements are no-ops.
 * - turn-failed: session + failure-text hash — REPEATED IDENTICAL failures on
 *   a session collapse into the one already-enqueued row (ON CONFLICT DO
 *   NOTHING), so a failing-in-a-loop session posts its failure comment once
 *   and stays silent until the failure text changes. Accepted edge: a text
 *   that reappears after a different one in between stays silent too (the
 *   human already saw it on this session).
 * - session-paused: one per breaker trip, keyed by the turn that tripped it.
 */
export const outcomeIdempotencyKey = (payload: TurnOutcomePayload): string => {
  switch (payload.kind) {
    case "turn-completed":
      return `turn-result:${payload.taskRunId}`;
    case "turn-failed": {
      const digest = createHash("sha256")
        .update(`${payload.cause}:${payload.summary}`)
        .digest("hex");
      return `turn-failure:${payload.sessionId}:${digest.slice(0, 32)}`;
    }
    case "session-paused":
      return `session-paused:${payload.taskRunId}`;
  }
};

/** Identifies one settled turn to the settlement paths below. */
export interface SettleTarget {
  readonly taskRunId: TaskRunId;
  readonly sessionId: SessionId;
  readonly ticket: TicketReference;
  readonly pr: PrReference | null;
}

/**
 * The single way a turn settles (extracted from TurnExecutor for FUR-40):
 * TaskRun transition + outbox callback + session back to rest — plus the
 * failure circuit breaker on FAILED settles. Shared by TurnExecutor (normal
 * turn pipeline) and StartupReconciler (crash-orphan settlement), so the two
 * paths cannot drift.
 */
export class TurnSettlement extends Context.Service<
  TurnSettlement,
  {
    /** COMPLETED transition + turn-completed callback + session to rest. */
    readonly settleCompleted: (
      args: SettleTarget & { readonly resultText: string },
    ) => Effect.Effect<void, DbError>;
    /**
     * FAILED transition (cause + summary preserved) + turn-failed callback +
     * circuit-breaker evaluation (skipped for CANCELLED — orchestrator/human
     * action says nothing about agent health) + session to rest.
     */
    readonly settleFailed: (
      args: SettleTarget & {
        readonly cause: TaskRunCause;
        readonly summary: string;
        readonly resultText?: string;
      },
    ) => Effect.Effect<void, DbError>;
  }
>()("maestro/engine/TurnSettlement") {
  static readonly layer = Layer.effect(
    TurnSettlement,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const sessionRepo = yield* SessionRepo;
      const taskRunRepo = yield* TaskRunRepo;
      const outboxRepo = yield* OutboxRepo;
      const auditRepo = yield* AuditRepo;

      const evictableAt = () => new Date(Date.now() + config.cooldownMinutes * 60_000);

      const enqueueOutcome = (payload: TurnOutcomePayload) =>
        outboxRepo.enqueue({
          taskRunId: payload.taskRunId,
          target: payload.ticket.source,
          payload,
          idempotencyKey: outcomeIdempotencyKey(payload),
        });

      /**
       * Failure circuit breaker (FUR-39 layer 2), evaluated after every
       * non-CANCELLED FAILED settle. Query-based (consecutive failures are
       * derived from settled rows), but the trip itself is the set-once
       * pause marker: only the settle that flips it emits the audit entry
       * and the "session paused" ticket comment — crossing the threshold
       * speaks exactly once per pause.
       */
      const maybeTripBreaker = Effect.fn(function* (args: SettleTarget) {
        const failures = yield* taskRunRepo.countConsecutiveFailures(args.sessionId);
        if (failures < CONSECUTIVE_FAILURE_LIMIT) return;
        const { session, newlyPaused } = yield* sessionRepo.pause(args.sessionId);
        if (!newlyPaused) return;
        yield* auditRepo.record({
          actor: "maestro",
          action: "session-paused",
          targetEntity: `session:${args.sessionId}`,
          priorState: session.state,
        });
        yield* enqueueOutcome({
          kind: "session-paused",
          taskRunId: args.taskRunId,
          sessionId: args.sessionId,
          ticket: args.ticket,
          summary:
            // No leading "Maestro" — formatTurnComment already prefixes the
            // marker; doubling read as "Maestro — Maestro paused" (FUR-42).
            `Paused this session after ${CONSECUTIVE_FAILURE_LIMIT} consecutive failures. ` +
            `New turns will not be triggered; to resume, mention ` +
            `@${config.linearMentionHandle} in a comment on this issue ` +
            `(or un-delegate and re-delegate it to Maestro).`,
          cause: null,
          pr: prOf(session),
        });
        yield* Effect.logWarning("TurnSettlement: circuit breaker paused session", {
          sessionId: args.sessionId,
          consecutiveFailures: failures,
        });
      });

      // WARM_IDLE is where every settled turn leaves its session. Sessions
      // spend the turn WARM_IDLE already in MVP (eviction lands later), so
      // only a DORMANT_SAVED rehydration needs an actual transition.
      const settleSession = Effect.fn(function* (sessionId: SessionId) {
        const fresh = yield* sessionRepo.get(sessionId);
        if (fresh.state === "DORMANT_SAVED") {
          yield* sessionRepo.transition(sessionId, "WARM_IDLE");
        }
        yield* sessionRepo.touchActivity(sessionId);
      });

      return {
        settleCompleted: Effect.fn("TurnSettlement.settleCompleted")(function* (args) {
          yield* taskRunRepo.transition(args.taskRunId, "COMPLETED", {
            evictableAfter: evictableAt(),
            resultText: args.resultText,
          });
          yield* enqueueOutcome({
            kind: "turn-completed",
            taskRunId: args.taskRunId,
            sessionId: args.sessionId,
            ticket: args.ticket,
            summary: args.resultText,
            cause: null,
            pr: args.pr,
          });
          yield* settleSession(args.sessionId);
        }),
        settleFailed: Effect.fn("TurnSettlement.settleFailed")(function* (args) {
          yield* taskRunRepo.transition(args.taskRunId, "FAILED", {
            cause: args.cause,
            evictableAfter: evictableAt(),
            // The same text the ticket comment carries, persisted on the run so
            // the admin UI shows WHY without a trip to the ticket (or Postgres).
            failureSummary: args.summary,
            ...(args.resultText !== undefined && { resultText: args.resultText }),
          });
          yield* enqueueOutcome({
            kind: "turn-failed",
            taskRunId: args.taskRunId,
            sessionId: args.sessionId,
            ticket: args.ticket,
            summary: args.summary,
            cause: args.cause,
            pr: args.pr,
          });
          // cancellations say nothing about agent health — never trip on them
          if (args.cause !== "CANCELLED") {
            yield* maybeTripBreaker(args);
          }
          yield* settleSession(args.sessionId);
        }),
      };
    }),
  );
}
