import { SessionStateChanged } from "@maestro/api";
import {
  type AgentEffort,
  canSessionTransition,
  type DbError,
  EntityNotFoundError,
  type ProjectId,
  Session,
  type SessionId,
  type SessionState,
  StateTransitionError,
  sessionTransitions,
  type TicketReference,
} from "@maestro/domain";
import { and, desc, eq, inArray, isNotNull, isNull, ne, sql } from "drizzle-orm";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { EventBus } from "../events/EventBus.ts";
import { Db } from "./Db.ts";
import { sessions } from "./schema/index.ts";
import { dbTry } from "./support.ts";

const decode = Schema.decodeUnknownSync(Session);
const toSession = (row: typeof sessions.$inferSelect): Session =>
  decode({
    id: row.id,
    projectId: row.projectId,
    ticketReference: { source: row.ticketSource, externalId: row.ticketExternalId },
    gitBranch: row.gitBranch,
    claudeSessionUuid: row.claudeSessionUuid,
    prNumber: row.prNumber,
    prUrl: row.prUrl,
    state: row.state,
    terminationRequestedAt: row.terminationRequestedAt,
    pausedAt: row.pausedAt,
    agentModel: row.agentModel,
    agentEffort: row.agentEffort,
    createdAt: row.createdAt,
    lastActivityAt: row.lastActivityAt,
  });

const allStates = Object.keys(sessionTransitions) as ReadonlyArray<SessionState>;

export interface SessionCreate {
  readonly projectId: ProjectId;
  readonly ticketReference: TicketReference;
  readonly gitBranch: string;
}

export class SessionRepo extends Context.Service<
  SessionRepo,
  {
    readonly create: (input: SessionCreate) => Effect.Effect<Session, DbError>;
    readonly get: (id: SessionId) => Effect.Effect<Session, DbError>;
    /** All sessions, most recently active first (admin list + SSE snapshot). */
    readonly list: () => Effect.Effect<ReadonlyArray<Session>, DbError>;
    /** The non-terminated session bound to a ticket, if any. */
    readonly findActiveByTicket: (
      ticket: TicketReference,
    ) => Effect.Effect<Option.Option<Session>, DbError>;
    /**
     * Sessions whose terminal signal is persisted but whose teardown never
     * finished (marker set, not yet TERMINATED) — the startup reconciliation
     * sweep (FUR-40) re-drives SessionTerminator.terminate for each.
     */
    readonly listTerminationRequested: () => Effect.Effect<ReadonlyArray<Session>, DbError>;
    /**
     * Compare-and-swap state transition: the UPDATE only matches rows whose
     * current state may legally reach `to` per the domain transition table.
     */
    readonly transition: (id: SessionId, to: SessionState) => Effect.Effect<Session, DbError>;
    /**
     * Persists the terminal signal (sets terminationRequestedAt once; a second
     * signal keeps the first timestamp — idempotent). The marker is what makes
     * a teardown deferred behind an executing turn survive until that turn
     * settles (M1.15).
     */
    readonly requestTermination: (id: SessionId) => Effect.Effect<Session, DbError>;
    /**
     * Trips the failure circuit breaker (FUR-39): sets pausedAt once.
     * `newlyPaused` reports whether THIS call flipped the marker — the
     * caller's exactly-once hook for the paused audit entry + outbox message
     * (a concurrent/replayed trip sees false and stays silent).
     */
    readonly pause: (
      id: SessionId,
    ) => Effect.Effect<{ session: Session; newlyPaused: boolean }, DbError>;
    /** Clears the circuit breaker (manual human resume). Idempotent. */
    readonly resume: (id: SessionId) => Effect.Effect<Session, DbError>;
    readonly setClaudeSessionUuid: (id: SessionId, uuid: string) => Effect.Effect<Session, DbError>;
    /**
     * Pins the model/effort the session's first turn resolved (FUR-41), so
     * resume turns keep the settings the claude session started with.
     */
    readonly setAgentSettings: (
      id: SessionId,
      settings: { readonly model: string | null; readonly effort: AgentEffort | null },
    ) => Effect.Effect<Session, DbError>;
    /** Records the forge PR opened for this session's branch (first outbound publish). */
    readonly setPullRequest: (
      id: SessionId,
      pr: { readonly number: number; readonly url: string },
    ) => Effect.Effect<Session, DbError>;
    readonly touchActivity: (id: SessionId) => Effect.Effect<Session, DbError>;
  }
>()("maestro/db/SessionRepo") {
  static readonly layer = Layer.effect(
    SessionRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      const bus = yield* EventBus;

      // DECISION (FUR-16): repos publish on every successful state-affecting
      // write — inside the repo, not the caller, so no transition can go
      // unpublished. Published: create (initial state), transition, and
      // setPullRequest (the PR appearing is a UI-relevant session change).
      // Not published: touchActivity (noise) and setClaudeSessionUuid
      // (internal bookkeeping).
      const publishChanged = (session: Session) =>
        bus.publish(SessionStateChanged.make({ session }));

      const getById = (operation: string) => (id: SessionId) =>
        dbTry(operation)(() => client.select().from(sessions).where(eq(sessions.id, id))).pipe(
          Effect.flatMap((rows) =>
            rows[0]
              ? Effect.succeed(rows[0])
              : Effect.fail(new EntityNotFoundError({ entity: "Session", entityId: id })),
          ),
        );

      return {
        create: Effect.fn("SessionRepo.create")(function* (input: SessionCreate) {
          const rows = yield* dbTry("SessionRepo.create")(() =>
            client
              .insert(sessions)
              .values({
                projectId: input.projectId,
                ticketSource: input.ticketReference.source,
                ticketExternalId: input.ticketReference.externalId,
                gitBranch: input.gitBranch,
                state: "WARM_IDLE",
              })
              .returning(),
          );
          // biome-ignore lint/style/noNonNullAssertion: insert returning always yields one row
          const session = toSession(rows[0]!);
          yield* publishChanged(session);
          return session;
        }),
        get: Effect.fn("SessionRepo.get")(function* (id: SessionId) {
          return toSession(yield* getById("SessionRepo.get")(id));
        }),
        list: Effect.fn("SessionRepo.list")(function* () {
          const rows = yield* dbTry("SessionRepo.list")(() =>
            client.select().from(sessions).orderBy(desc(sessions.lastActivityAt)),
          );
          return rows.map(toSession);
        }),
        findActiveByTicket: Effect.fn("SessionRepo.findActiveByTicket")(function* (
          ticket: TicketReference,
        ) {
          const rows = yield* dbTry("SessionRepo.findActiveByTicket")(() =>
            client
              .select()
              .from(sessions)
              .where(
                and(
                  eq(sessions.ticketSource, ticket.source),
                  eq(sessions.ticketExternalId, ticket.externalId),
                  ne(sessions.state, "TERMINATED"),
                ),
              ),
          );
          return Option.map(Option.fromNullishOr(rows[0]), toSession);
        }),
        listTerminationRequested: Effect.fn("SessionRepo.listTerminationRequested")(function* () {
          const rows = yield* dbTry("SessionRepo.listTerminationRequested")(() =>
            client
              .select()
              .from(sessions)
              .where(
                and(isNotNull(sessions.terminationRequestedAt), ne(sessions.state, "TERMINATED")),
              ),
          );
          return rows.map(toSession);
        }),
        transition: Effect.fn("SessionRepo.transition")(function* (
          id: SessionId,
          to: SessionState,
        ) {
          const legalFrom = allStates.filter((from) => canSessionTransition(from, to));
          const rows = yield* dbTry("SessionRepo.transition")(() =>
            client
              .update(sessions)
              .set({ state: to })
              .where(and(eq(sessions.id, id), inArray(sessions.state, [...legalFrom])))
              .returning(),
          );
          if (rows[0]) {
            const session = toSession(rows[0]);
            yield* publishChanged(session);
            return session;
          }
          const current = yield* getById("SessionRepo.transition")(id);
          return yield* new StateTransitionError({
            entity: "Session",
            entityId: id,
            from: current.state,
            to,
          });
        }),
        requestTermination: Effect.fn("SessionRepo.requestTermination")(function* (id: SessionId) {
          const rows = yield* dbTry("SessionRepo.requestTermination")(() =>
            client
              .update(sessions)
              .set({
                terminationRequestedAt: sql`coalesce(${sessions.terminationRequestedAt}, now())`,
              })
              .where(eq(sessions.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Session", entityId: id });
          }
          const session = toSession(row);
          // published like a transition: "terminating" is a UI-relevant change
          yield* publishChanged(session);
          return session;
        }),
        pause: Effect.fn("SessionRepo.pause")(function* (id: SessionId) {
          // Set-once via the WHERE guard: only the call that finds the marker
          // unset flips it, so `newlyPaused` is race-safe inside the statement.
          const rows = yield* dbTry("SessionRepo.pause")(() =>
            client
              .update(sessions)
              .set({ pausedAt: new Date() })
              .where(and(eq(sessions.id, id), isNull(sessions.pausedAt)))
              .returning(),
          );
          if (rows[0]) {
            const session = toSession(rows[0]);
            yield* publishChanged(session);
            return { session, newlyPaused: true };
          }
          const current = toSession(yield* getById("SessionRepo.pause")(id));
          return { session: current, newlyPaused: false };
        }),
        resume: Effect.fn("SessionRepo.resume")(function* (id: SessionId) {
          const rows = yield* dbTry("SessionRepo.resume")(() =>
            client
              .update(sessions)
              .set({ pausedAt: null })
              .where(and(eq(sessions.id, id), isNotNull(sessions.pausedAt)))
              .returning(),
          );
          if (rows[0]) {
            const session = toSession(rows[0]);
            yield* publishChanged(session);
            return session;
          }
          // already resumed (or never paused) — converge without an event
          return toSession(yield* getById("SessionRepo.resume")(id));
        }),
        setClaudeSessionUuid: Effect.fn("SessionRepo.setClaudeSessionUuid")(function* (
          id: SessionId,
          uuid: string,
        ) {
          const rows = yield* dbTry("SessionRepo.setClaudeSessionUuid")(() =>
            client
              .update(sessions)
              .set({ claudeSessionUuid: uuid })
              .where(eq(sessions.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Session", entityId: id });
          }
          return toSession(row);
        }),
        // like setClaudeSessionUuid: internal bookkeeping, not published
        setAgentSettings: Effect.fn("SessionRepo.setAgentSettings")(function* (
          id: SessionId,
          settings: { readonly model: string | null; readonly effort: AgentEffort | null },
        ) {
          const rows = yield* dbTry("SessionRepo.setAgentSettings")(() =>
            client
              .update(sessions)
              .set({ agentModel: settings.model, agentEffort: settings.effort })
              .where(eq(sessions.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Session", entityId: id });
          }
          return toSession(row);
        }),
        setPullRequest: Effect.fn("SessionRepo.setPullRequest")(function* (
          id: SessionId,
          pr: { readonly number: number; readonly url: string },
        ) {
          const rows = yield* dbTry("SessionRepo.setPullRequest")(() =>
            client
              .update(sessions)
              .set({ prNumber: pr.number, prUrl: pr.url })
              .where(eq(sessions.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Session", entityId: id });
          }
          const session = toSession(row);
          yield* publishChanged(session);
          return session;
        }),
        touchActivity: Effect.fn("SessionRepo.touchActivity")(function* (id: SessionId) {
          const rows = yield* dbTry("SessionRepo.touchActivity")(() =>
            client
              .update(sessions)
              .set({ lastActivityAt: new Date() })
              .where(eq(sessions.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Session", entityId: id });
          }
          return toSession(row);
        }),
      };
    }),
  );
}
