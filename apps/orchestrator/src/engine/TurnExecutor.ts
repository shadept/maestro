import { mkdir } from "node:fs/promises";
import { LogChunk } from "@maestro/api";
import {
  type DbError,
  type ForgeError,
  type GitError,
  type RuntimeError,
  type Session,
  SessionId,
  TaskRunCause,
  TaskRunId,
  TicketReference,
} from "@maestro/domain";
import { Context, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { AgentContract, type AgentEvent } from "../agent/AgentContract.ts";
import { AppConfig } from "../config/AppConfig.ts";
import { OutboxRepo } from "../db/OutboxRepo.ts";
import { ProjectRepo } from "../db/ProjectRepo.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { EventBus } from "../events/EventBus.ts";
import { GitCache } from "../git/GitCache.ts";
import { forgeCredentials } from "../git/git-command.ts";
import { OutboundGit } from "../git/OutboundGit.ts";
import { WorktreeManager } from "../git/WorktreeManager.ts";
import type { TurnJob } from "../queue/TurnQueue.ts";
import { type ExitInfo, WorkerRuntime } from "../runtime/WorkerRuntime.ts";
import { sessionConfigDir } from "../storage/paths.ts";
import { SessionTerminator } from "./SessionTerminator.ts";

export type TurnExecutionError = DbError | GitError | RuntimeError | ForgeError;

/** PR coordinates for the ticket comment. */
export interface PrReference {
  readonly number: number;
  readonly url: string;
}

/**
 * Outbox payload written when a turn settles. The callback worker (FUR-18)
 * drains these entries and posts them back to the ticketing platform
 * identified by `ticket.source`. A Schema (not just a type) because the
 * worker decodes it back out of the outbox's jsonb column.
 */
export const TurnOutcomePayload = Schema.Struct({
  kind: Schema.Literals(["turn-completed", "turn-failed"]),
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

/** The session's persisted PR reference, if the orchestrator has pushed before. */
const prOf = (session: Session): PrReference | null =>
  session.prNumber !== null && session.prUrl !== null
    ? { number: session.prNumber, url: session.prUrl }
    : null;

type ResultEvent = Extract<AgentEvent, { _tag: "Result" }>;

/**
 * MVP worker mount strategy (resolves the FUR-10 WATCH item):
 *
 * Worktree, parent bare repo, and session config dir are all mounted at their
 * HOST-IDENTICAL absolute paths ("identity mounts"). The worktree's `.git`
 * file and the bare repo's `worktrees/<id>/gitdir` metadata both contain
 * absolute host paths pointing at each other; identity mounting makes them
 * resolve unchanged inside the container, so plain `git commit` just works.
 *
 * DELIBERATE PRD DEVIATION: PRD §3.2 wants the parent .git mounted read-only,
 * but a worktree commit must write objects and refs into the parent bare repo
 * — read-only would break the agent's ability to commit at all. MVP mounts
 * the bare repo read-write and accepts that a worker can see (not push —
 * workers hold no credentials) other sessions' refs on the same project.
 * Hardening (per-session object spool / alternates) is deferred to M1.16+.
 */
const identityMounts = (paths: {
  readonly worktreePath: string;
  readonly gitDir: string;
  readonly configDir: string;
}) =>
  [paths.worktreePath, paths.gitDir, paths.configDir].map((p) => ({
    hostPath: p,
    containerPath: p,
    readOnly: false,
  }));

/** Runtime kill/timeout classification wins; a clean exit still needs an ok Result. */
const classifyOutcome = (exit: ExitInfo, result: ResultEvent | null): TaskRunCause | null =>
  exit.cause ?? (exit.exitCode === 0 && result?.ok === true ? null : "ERROR");

/**
 * The end-to-end turn pipeline (Tech Requirements §8, MVP cold-container
 * model): TaskRun PENDING → PROVISIONING (clone + worktree + config dir) →
 * EXECUTING (worker runs the AgentContract command; logs stream to the
 * TaskRun row and the stream-json parser) → COMPLETED/FAILED + outbox entry,
 * session back to WARM_IDLE (a DB state only — the container has exited).
 *
 * Failure semantics: an agent failure (non-zero exit, timeout, kill) settles
 * the TaskRun as FAILED with its cause and SUCCEEDS as an effect — the turn
 * pipeline did its job. `execute` only fails on orchestration errors
 * (git/db/runtime), after best-effort FAILED marking. No auto-retry ever;
 * the worktree is always preserved untouched for the next explicit resume.
 */
export class TurnExecutor extends Context.Service<
  TurnExecutor,
  {
    /** Executes one queued turn; registered as the TurnQueue handler. */
    readonly execute: (job: TurnJob) => Effect.Effect<void, TurnExecutionError>;
  }
>()("maestro/engine/TurnExecutor") {
  static readonly layer = Layer.effect(
    TurnExecutor,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const projectRepo = yield* ProjectRepo;
      const sessionRepo = yield* SessionRepo;
      const taskRunRepo = yield* TaskRunRepo;
      const outboxRepo = yield* OutboxRepo;
      const gitCache = yield* GitCache;
      const worktreeManager = yield* WorktreeManager;
      const outboundGit = yield* OutboundGit;
      const runtime = yield* WorkerRuntime;
      const agent = yield* AgentContract;
      const bus = yield* EventBus;
      const terminator = yield* SessionTerminator;

      /**
       * Deferred terminal cleanup (M1.15): a ticket-closure signal that
       * arrived while this session's turn was active only set the persisted
       * marker — the executor finishes the job here, after the turn settled.
       * Best-effort by design: a failure is logged, the marker survives, and
       * the next replayed job (or the M2 retention fallback) converges.
       */
      const finalizeTermination = (sessionId: SessionId) =>
        Effect.gen(function* () {
          const session = yield* sessionRepo.get(sessionId);
          if (session.terminationRequestedAt === null) return;
          const outcome = yield* terminator.terminate({ sessionId });
          yield* Effect.logInfo("TurnExecutor: finalized deferred session teardown", {
            sessionId,
            outcome: outcome._tag,
          });
        }).pipe(
          Effect.catch((error) =>
            Effect.logError("TurnExecutor: deferred session teardown failed", error),
          ),
        );

      const evictableAt = () => new Date(Date.now() + config.cooldownMinutes * 60_000);

      const enqueueOutcome = (payload: TurnOutcomePayload) =>
        outboxRepo.enqueue({
          taskRunId: payload.taskRunId,
          target: payload.ticket.source,
          payload,
          // one outcome per turn — replayed settlements are no-ops
          idempotencyKey: `turn-result:${payload.taskRunId}`,
        });

      // WARM_IDLE is where every settled turn leaves its session. Sessions
      // spend the turn WARM_IDLE already in MVP (eviction lands later), so
      // only a DORMANT_SAVED rehydration needs an actual transition.
      const settleSession = (sessionId: SessionId) =>
        Effect.gen(function* () {
          const fresh = yield* sessionRepo.get(sessionId);
          if (fresh.state === "DORMANT_SAVED") {
            yield* sessionRepo.transition(sessionId, "WARM_IDLE");
          }
          yield* sessionRepo.touchActivity(sessionId);
        });

      const settleFailed = (args: {
        readonly job: TurnJob;
        readonly ticket: TicketReference;
        readonly cause: TaskRunCause;
        readonly summary: string;
        readonly pr: PrReference | null;
        readonly resultText?: string;
      }) =>
        Effect.gen(function* () {
          yield* taskRunRepo.transition(args.job.taskRunId, "FAILED", {
            cause: args.cause,
            evictableAfter: evictableAt(),
            ...(args.resultText !== undefined && { resultText: args.resultText }),
          });
          yield* enqueueOutcome({
            kind: "turn-failed",
            taskRunId: args.job.taskRunId,
            sessionId: args.job.sessionId,
            ticket: args.ticket,
            summary: args.summary,
            cause: args.cause,
            pr: args.pr,
          });
          yield* settleSession(args.job.sessionId);
        });

      /** Streams worker logs into the TaskRun row AND the agent parser; returns the last Result. */
      const observeWorker = (args: {
        readonly handle: { readonly id: string };
        readonly session: Session;
        readonly taskRunId: TaskRunId;
      }) =>
        Effect.gen(function* () {
          // Local once-guard: the session snapshot is stale after the first
          // persist, so persistSessionUuid alone would re-write on every
          // subsequent system event in the same stream.
          let uuidPersisted = args.session.claudeSessionUuid !== null;
          const pump = yield* Effect.forkChild(
            runtime.logs(args.handle).pipe(
              // The tee is also the SSE log pipeline (FUR-16): every chunk is
              // persisted AND published live, in arrival order.
              Stream.tap((chunk) =>
                taskRunRepo.appendLogs(args.taskRunId, chunk).pipe(
                  Effect.andThen(
                    bus.publish(
                      LogChunk.make({
                        taskRunId: args.taskRunId,
                        sessionId: args.session.id,
                        chunk,
                      }),
                    ),
                  ),
                ),
              ),
              agent.parseStream,
              Stream.tap((event) => {
                if (event._tag !== "SessionStarted" || uuidPersisted) return Effect.void;
                uuidPersisted = true;
                return agent.persistSessionUuid(args.session, event);
              }),
              Stream.runFold(
                () => null as ResultEvent | null,
                (last, event) => (event._tag === "Result" ? event : last),
              ),
            ),
          );
          const exit = yield* runtime.wait(args.handle);
          const result = yield* Fiber.join(pump);
          return { exit, result };
        });

      const runTurn = Effect.fn("TurnExecutor.runTurn")(function* (job: TurnJob, session: Session) {
        const project = yield* projectRepo.get(session.projectId);
        const context = yield* taskRunRepo.getContext(job.taskRunId);

        yield* taskRunRepo.transition(job.taskRunId, "PROVISIONING");
        // Private repos: the orchestrator token authenticates clone + fetch
        // via per-invocation GIT_CONFIG_* env (FUR-10 mechanism — never argv,
        // never stored config). Absent token = anonymous, public repos work.
        const credentials = forgeCredentials(config.githubToken);
        const cachePath = yield* gitCache.ensureClone(project, credentials);
        if (project.localCachePath === null) {
          yield* projectRepo.setLocalCachePath(project.id, cachePath);
        }
        // Frozen-cache fix: refresh the base from origin so a first provision
        // cuts the session branch from origin's CURRENT base, not the
        // clone-time snapshot. Runs before every provision (not only branch
        // creation): only refs/heads/<base> moves, so existing session
        // branches — including this session's — are untouched, and the
        // branch-lost recovery path stays as fresh as a first provision.
        // DEGRADATION: the clone exists at this point, so an unreachable
        // origin logs a warning and the turn proceeds on the cached (possibly
        // stale) base — resilience outranks freshness; a later publish will
        // surface a genuinely dead remote anyway.
        yield* gitCache.fetchBase(project, credentials).pipe(
          Effect.catch((error) =>
            Effect.logWarning("TurnExecutor: base fetch failed; provisioning from cached base", {
              projectId: project.id,
              error: String(error),
            }),
          ),
        );
        const paths = yield* worktreeManager.provision({ session, project });
        const configDir = sessionConfigDir(config.storageRoot, session.id);
        yield* Effect.promise(() => mkdir(configDir, { recursive: true }));

        const command = agent.buildCommand({ session, context, configDir });
        const timeoutMillis = config.turnTimeoutSeconds * 1000;
        yield* taskRunRepo.transition(job.taskRunId, "EXECUTING", {
          expiresAt: new Date(Date.now() + timeoutMillis),
        });

        const memoryMib = project.resources.memoryBaselineMib;
        const handle = yield* runtime.start({
          name: `maestro-turn-${job.taskRunId}`,
          image: config.workerImage,
          command: command.argv,
          env: command.env,
          mounts: identityMounts({ ...paths, configDir }),
          workdir: paths.worktreePath,
          ...(memoryMib !== undefined && { memoryMib }),
          timeoutMillis,
        });

        const { exit, result } = yield* observeWorker({
          handle,
          session,
          taskRunId: job.taskRunId,
        }).pipe(
          // an orchestration failure mid-stream must not leak a running worker
          Effect.onError(() => runtime.kill(handle).pipe(Effect.ignore)),
        );

        const cause = classifyOutcome(exit, result);
        if (cause === null && result !== null) {
          // Outbound publish (FUR-15): push new commits + ensure the PR before
          // the run is recorded COMPLETED. DECISION: a publish failure after an
          // ok agent Result settles the turn FAILED (cause ERROR) with the
          // publish error as the callback summary — never COMPLETED with a
          // silently missing PR — and then propagates as an orchestration
          // error (pg-boss failure record). The generic settlement in
          // `execute` re-fires on the propagated error but its FAILED→FAILED
          // transition is rejected, so this tailored settlement wins.
          const published = yield* outboundGit.publish({ session, project, context }).pipe(
            Effect.tapError((error) =>
              settleFailed({
                job,
                ticket: session.ticketReference,
                cause: "ERROR",
                summary: `agent succeeded but publishing failed: ${String(error)}`,
                pr: prOf(session),
                resultText: result.finalText,
              }).pipe(
                Effect.catch((settleError) =>
                  Effect.logError(
                    "TurnExecutor: publish-failure settlement incomplete",
                    settleError,
                  ),
                ),
              ),
            ),
          );
          // no-commit turns publish nothing; the callback still links a PR
          // opened by an earlier turn, if any
          const pr =
            published._tag === "Published"
              ? { number: published.prNumber, url: published.prUrl }
              : prOf(session);
          yield* taskRunRepo.transition(job.taskRunId, "COMPLETED", {
            evictableAfter: evictableAt(),
            resultText: result.finalText,
          });
          yield* enqueueOutcome({
            kind: "turn-completed",
            taskRunId: job.taskRunId,
            sessionId: job.sessionId,
            ticket: session.ticketReference,
            summary: result.finalText,
            cause: null,
            pr,
          });
          yield* settleSession(job.sessionId);
        } else {
          const failureCause = cause ?? "ERROR";
          const summary =
            result !== null && result.finalText.length > 0
              ? result.finalText
              : `worker exited with code ${exit.exitCode} (${failureCause})`;
          yield* settleFailed({
            job,
            ticket: session.ticketReference,
            cause: failureCause,
            summary,
            pr: prOf(session),
            ...(result !== null && { resultText: result.finalText }),
          });
        }
      });

      return {
        execute: Effect.fn("TurnExecutor.execute")(function* (job: TurnJob) {
          const taskRun = yield* taskRunRepo.get(job.taskRunId);
          if (taskRun.state !== "PENDING") {
            // replayed or crash-recovered job for an already-started turn —
            // never re-run an agent pass (no auto-retry by design)
            yield* Effect.logWarning("TurnExecutor: skipping non-PENDING turn", {
              taskRunId: job.taskRunId,
              state: taskRun.state,
            });
            // a replayed job may be the only remaining driver of a teardown
            // that was deferred and then lost to a crash — finish it here
            yield* finalizeTermination(job.sessionId);
            return;
          }
          const session = yield* sessionRepo.get(job.sessionId);
          if (session.state === "TERMINATED" || session.terminationRequestedAt !== null) {
            // the terminal signal raced this dispatch: never start a turn on
            // a terminating session — cancel the run instead
            yield* taskRunRepo
              .transition(job.taskRunId, "FAILED", { cause: "CANCELLED" })
              .pipe(Effect.catchTag("StateTransitionError", () => Effect.void));
            yield* finalizeTermination(job.sessionId);
            return;
          }

          yield* runTurn(job, session).pipe(
            // Orchestration errors (git/db/runtime) still settle the turn as
            // FAILED with the cause captured, best-effort, then propagate.
            Effect.tapError((error) =>
              settleFailed({
                job,
                ticket: session.ticketReference,
                cause: "ERROR",
                summary: String(error),
                pr: prOf(session),
              }).pipe(
                Effect.catch((settleError) =>
                  Effect.logError("TurnExecutor: failure settlement incomplete", settleError),
                ),
              ),
            ),
            // Runs while this session still occupies its dispatcher slot, so
            // the same-session queue cannot dispatch concurrently with the
            // teardown. Also runs on interrupt: reading a null marker is a
            // no-op, and an unsettled (still-EXECUTING) run defers again.
            Effect.ensuring(finalizeTermination(job.sessionId)),
          );
        }),
      };
    }),
  );
}
