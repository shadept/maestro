import type {
  DbError,
  GitError,
  Project,
  QueueError,
  SessionId,
  TaskContext,
  TaskRunId,
  TicketReference,
} from "@maestro/domain";
import { Context, Effect, Layer, Option } from "effect";
import { AuditRepo } from "../db/AuditRepo.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { SessionTerminator } from "../engine/SessionTerminator.ts";
import { branchNameFor } from "../git/WorktreeManager.ts";
import { TurnQueue } from "../queue/TurnQueue.ts";

/**
 * What ingesting one event did. Adapters surface these to the webhook
 * response (and their logs); every variant is a 200 — only verification and
 * malformed payloads are HTTP errors.
 */
export type IngestOutcome =
  | {
      readonly _tag: "SessionStarted";
      readonly sessionId: SessionId;
      readonly taskRunId: TaskRunId;
    }
  | { readonly _tag: "TurnQueued"; readonly sessionId: SessionId; readonly taskRunId: TaskRunId }
  | {
      readonly _tag: "SessionResumed";
      readonly sessionId: SessionId;
      readonly taskRunId: TaskRunId;
    }
  | { readonly _tag: "TerminalRecorded"; readonly sessionId: SessionId }
  | { readonly _tag: "Duplicate" }
  | { readonly _tag: "Ignored"; readonly reason: string };

export type IngestPipelineError = DbError | QueueError | GitError;

/**
 * The forge-agnostic half of ingestion (Tech Requirements §5): normalized
 * TaskContexts in, sessions/turns/queue jobs out. Platform adapters
 * (LinearIngest now, the generic REST API in M2) own webhook verification,
 * dedup, and payload mapping, then hand normalized work to this pipeline —
 * the seam CLAUDE.md sketches as Ingest.layerLinear / Ingest.layerGenericApi.
 *
 * Per-session FIFO holds because TaskRun rows are created here in arrival
 * order (webhook handling is synchronous in-request in M1) and TaskRunIds are
 * monotonic UUIDv7 — the TurnQueue dispatch invariant (FUR-13).
 */
export class IngestPipeline extends Context.Service<
  IngestPipeline,
  {
    /**
     * A ticket was handed to Maestro (Linear: issue delegated to the Maestro
     * app user, FUR-37). Creates the session and its first turn; an
     * already-active session makes this a no-op so replayed/reshuffled
     * delegation events never double-trigger.
     *
     * `resumeSignal` marks an explicit human (re-)trigger — Linear: the
     * delegate actually changed on the event (updatedFrom evidence), not
     * merely "still delegated on some issue update". On a session the failure
     * circuit breaker paused (FUR-39), a resume signal clears the breaker and
     * queues a fresh turn; without it a paused session stays paused.
     */
    readonly startTask: (args: {
      readonly project: Project;
      readonly context: TaskContext;
      readonly resumeSignal?: boolean;
    }) => Effect.Effect<IngestOutcome, IngestPipelineError>;
    /**
     * Whether the ticket has an active (non-terminated) session — the branch
     * point for platform adapters whose follow-up signal can also start work
     * (Linear: a mention on a session-less-but-delegated issue starts a
     * session instead of queueing a turn, FUR-37).
     */
    readonly hasActiveSession: (
      ticket: TicketReference,
    ) => Effect.Effect<boolean, IngestPipelineError>;
    /**
     * A follow-up (Linear: @maestro mention on a worked issue) — one more
     * turn on the ticket's active session. No session = not a Maestro ticket.
     *
     * `resumeSignal` marks an explicit human summon (Linear: every mention
     * is one — plain comments never reach the pipeline since FUR-37): on a
     * breaker-paused session it clears the breaker and queues the turn
     * (SessionResumed); without it a paused session ignores the event.
     */
    readonly queueTurn: (args: {
      readonly context: TaskContext;
      readonly resumeSignal?: boolean;
    }) => Effect.Effect<IngestOutcome, IngestPipelineError>;
    /**
     * The ticket reached a terminal platform state (done/canceled) — the
     * single authoritative teardown trigger (PRD §4.1). Audit-logs the signal
     * and hands the session to SessionTerminator: queued turns cancelled,
     * session TERMINATED, worktree + config dir purged (deferred until an
     * executing turn settles). No active session (incl. a second signal
     * after teardown) = Ignored — the double-close no-op.
     */
    readonly recordTerminal: (args: {
      readonly ticket: TicketReference;
      readonly actor: string;
      readonly signal: "done" | "canceled";
    }) => Effect.Effect<IngestOutcome, IngestPipelineError>;
  }
>()("maestro/ingest/IngestPipeline") {
  static readonly layer = Layer.effect(
    IngestPipeline,
    Effect.gen(function* () {
      const sessionRepo = yield* SessionRepo;
      const taskRunRepo = yield* TaskRunRepo;
      const auditRepo = yield* AuditRepo;
      const queue = yield* TurnQueue;
      const terminator = yield* SessionTerminator;

      const createTurn = (sessionId: SessionId, context: TaskContext) =>
        Effect.gen(function* () {
          const taskRun = yield* taskRunRepo.create(sessionId, context);
          yield* queue.enqueue({ taskRunId: taskRun.id, sessionId });
          return taskRun;
        });

      /** Clears the FUR-39 breaker on an explicit human resume signal. */
      const resume = (sessionId: SessionId, actor: string) =>
        Effect.gen(function* () {
          yield* sessionRepo.resume(sessionId);
          yield* auditRepo.record({
            actor,
            action: "session-resumed",
            targetEntity: `session:${sessionId}`,
          });
        });

      return {
        startTask: Effect.fn("IngestPipeline.startTask")(function* (args) {
          const existing = yield* sessionRepo.findActiveByTicket(args.context.ticket);
          if (Option.isSome(existing)) {
            const session = existing.value;
            if (session.pausedAt !== null && args.resumeSignal === true) {
              // Manual resume (FUR-39, mechanism migrated by FUR-37): the
              // human re-delegated a breaker-paused session's issue to
              // Maestro. Clear the breaker, audit the human action, and
              // queue a fresh turn from the ticket.
              yield* resume(session.id, args.context.actor);
              const taskRun = yield* createTurn(session.id, args.context);
              return {
                _tag: "SessionResumed",
                sessionId: session.id,
                taskRunId: taskRun.id,
              } satisfies IngestOutcome;
            }
            return {
              _tag: "Ignored",
              reason: `session ${session.id} already active for ${args.context.ticket.externalId}`,
            } satisfies IngestOutcome;
          }
          const session = yield* sessionRepo.create({
            projectId: args.project.id,
            ticketReference: args.context.ticket,
            gitBranch: branchNameFor(args.context.ticket, args.project),
          });
          const taskRun = yield* createTurn(session.id, args.context);
          return {
            _tag: "SessionStarted",
            sessionId: session.id,
            taskRunId: taskRun.id,
          } satisfies IngestOutcome;
        }),
        hasActiveSession: Effect.fn("IngestPipeline.hasActiveSession")(function* (ticket) {
          return Option.isSome(yield* sessionRepo.findActiveByTicket(ticket));
        }),
        queueTurn: Effect.fn("IngestPipeline.queueTurn")(function* (args) {
          const session = yield* sessionRepo.findActiveByTicket(args.context.ticket);
          if (Option.isNone(session)) {
            return {
              _tag: "Ignored",
              reason: `no active session for ${args.context.ticket.externalId}`,
            } satisfies IngestOutcome;
          }
          if (session.value.pausedAt !== null) {
            // Circuit breaker (FUR-39): a resume signal (Linear: a mention is
            // an explicit human summon, FUR-37) clears the breaker and the
            // turn proceeds; anything else bounces off the paused session.
            if (args.resumeSignal !== true) {
              return {
                _tag: "Ignored",
                reason:
                  `session ${session.value.id} is paused after repeated failures; ` +
                  `mention Maestro in a comment (or re-delegate the issue) to resume`,
              } satisfies IngestOutcome;
            }
            yield* resume(session.value.id, args.context.actor);
            const taskRun = yield* createTurn(session.value.id, args.context);
            return {
              _tag: "SessionResumed",
              sessionId: session.value.id,
              taskRunId: taskRun.id,
            } satisfies IngestOutcome;
          }
          const taskRun = yield* createTurn(session.value.id, args.context);
          return {
            _tag: "TurnQueued",
            sessionId: session.value.id,
            taskRunId: taskRun.id,
          } satisfies IngestOutcome;
        }),
        recordTerminal: Effect.fn("IngestPipeline.recordTerminal")(function* (args) {
          const session = yield* sessionRepo.findActiveByTicket(args.ticket);
          if (Option.isNone(session)) {
            return {
              _tag: "Ignored",
              reason: `no active session for ${args.ticket.externalId}`,
            } satisfies IngestOutcome;
          }
          yield* auditRepo.record({
            actor: args.actor,
            action: `ticket-${args.signal}`,
            targetEntity: `session:${session.value.id}`,
            priorState: session.value.state,
          });
          yield* terminator.terminate({ sessionId: session.value.id });
          return { _tag: "TerminalRecorded", sessionId: session.value.id } satisfies IngestOutcome;
        }),
      };
    }),
  );
}
