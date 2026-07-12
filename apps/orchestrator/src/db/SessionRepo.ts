import {
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
import { and, eq, inArray, ne } from "drizzle-orm";
import { Context, Effect, Layer, Option, Schema } from "effect";
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
    state: row.state,
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
    /** The non-terminated session bound to a ticket, if any. */
    readonly findActiveByTicket: (
      ticket: TicketReference,
    ) => Effect.Effect<Option.Option<Session>, DbError>;
    /**
     * Compare-and-swap state transition: the UPDATE only matches rows whose
     * current state may legally reach `to` per the domain transition table.
     */
    readonly transition: (id: SessionId, to: SessionState) => Effect.Effect<Session, DbError>;
    readonly setClaudeSessionUuid: (id: SessionId, uuid: string) => Effect.Effect<Session, DbError>;
    readonly touchActivity: (id: SessionId) => Effect.Effect<Session, DbError>;
  }
>()("maestro/db/SessionRepo") {
  static readonly layer = Layer.effect(
    SessionRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;

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
          return toSession(rows[0]!);
        }),
        get: Effect.fn("SessionRepo.get")(function* (id: SessionId) {
          return toSession(yield* getById("SessionRepo.get")(id));
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
          if (rows[0]) return toSession(rows[0]);
          const current = yield* getById("SessionRepo.transition")(id);
          return yield* new StateTransitionError({
            entity: "Session",
            entityId: id,
            from: current.state,
            to,
          });
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
