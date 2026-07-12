import type {
  DbError,
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
  | { readonly _tag: "TerminalRecorded"; readonly sessionId: SessionId }
  | { readonly _tag: "Duplicate" }
  | { readonly _tag: "Ignored"; readonly reason: string };

export type IngestPipelineError = DbError | QueueError;

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
     * A ticket was handed to Maestro (Linear: trigger label present). Creates
     * the session and its first turn; an already-active session makes this a
     * no-op so replayed/reshuffled label events never double-trigger.
     */
    readonly startTask: (args: {
      readonly project: Project;
      readonly context: TaskContext;
    }) => Effect.Effect<IngestOutcome, IngestPipelineError>;
    /**
     * A follow-up (Linear: new comment on a triggered issue) — one more turn
     * on the ticket's active session. No session = not a Maestro ticket.
     */
    readonly queueTurn: (args: {
      readonly context: TaskContext;
    }) => Effect.Effect<IngestOutcome, IngestPipelineError>;
    /**
     * The ticket reached a terminal platform state (done/canceled). M1
     * records the signal in the audit log only; acting on it (worktree
     * teardown, session TERMINATED) is the M1.15 lifecycle ticket.
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

      const createTurn = (sessionId: SessionId, context: TaskContext) =>
        Effect.gen(function* () {
          const taskRun = yield* taskRunRepo.create(sessionId, context);
          yield* queue.enqueue({ taskRunId: taskRun.id, sessionId });
          return taskRun;
        });

      return {
        startTask: Effect.fn("IngestPipeline.startTask")(function* (args) {
          const existing = yield* sessionRepo.findActiveByTicket(args.context.ticket);
          if (Option.isSome(existing)) {
            return {
              _tag: "Ignored",
              reason: `session ${existing.value.id} already active for ${args.context.ticket.externalId}`,
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
        queueTurn: Effect.fn("IngestPipeline.queueTurn")(function* (args) {
          const session = yield* sessionRepo.findActiveByTicket(args.context.ticket);
          if (Option.isNone(session)) {
            return {
              _tag: "Ignored",
              reason: `no active session for ${args.context.ticket.externalId}`,
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
          return { _tag: "TerminalRecorded", sessionId: session.value.id } satisfies IngestOutcome;
        }),
      };
    }),
  );
}
