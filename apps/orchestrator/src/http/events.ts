import {
  type MaestroEvent,
  MaestroEventFromJsonString,
  SessionStateChanged,
  SystemStatus,
  TaskRunStateChanged,
} from "@maestro/api";
import { EntityNotFoundError, SessionId } from "@maestro/domain";
import { Effect, Option, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { AppConfig } from "../config/AppConfig.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { EventBus } from "../events/EventBus.ts";
import { bearerToken, tokenMatches } from "./auth.ts";

// GET /api/events — the SSE firehose (FUR-16). Deliberately a raw streaming
// route, not part of the AdminApi HttpApi contract; its payload contract is
// MaestroEventFromJsonString in @maestro/api.

const HEARTBEAT_MILLIS = 15_000;

const encodeEvent = Schema.encodeSync(MaestroEventFromJsonString);
const frame = (event: MaestroEvent): string =>
  `event: ${event._tag}\ndata: ${encodeEvent(event)}\n\n`;

/** The session an event belongs to; null = system-wide (always delivered). */
const sessionIdOf = (event: MaestroEvent): SessionId | null => {
  switch (event._tag) {
    case "SessionStateChanged":
      return event.session.id;
    case "TaskRunStateChanged":
      return event.taskRun.sessionId;
    case "QueueChanged":
    case "LogChunk":
      return event.sessionId;
    case "SystemStatus":
      return null;
  }
};

const decodeSessionId = Schema.decodeUnknownOption(SessionId);

/**
 * Snapshot-then-live semantics (DECISION): the handler subscribes FIRST, then
 * reads the snapshot (all sessions + unsettled runs), emits it, then drains
 * the live tail. Nothing published after the subscription can be missed;
 * events published between subscribe and snapshot read may be delivered twice
 * (once inside the snapshot, once from the tail). No dedupe is attempted —
 * events carry full entities, so consumers converge by upserting on id; the
 * only artifact is that a replayed tail event can briefly rewind a row the
 * snapshot already showed fresher, and the tail itself re-converges.
 */
export const EventsRoutes = HttpRouter.add(
  "GET",
  "/api/events",
  Effect.gen(function* () {
    const { adminToken, maxConcurrentWorkers } = yield* AppConfig;
    const request = yield* HttpServerRequest.HttpServerRequest;
    const searchParams = yield* HttpServerRequest.ParsedSearchParams;

    // Auth (DECISION, mirrors AdminAuth in @maestro/api): Bearer header
    // preferred; `?token=` accepted because EventSource cannot set headers.
    const queryToken = typeof searchParams.token === "string" ? searchParams.token : undefined;
    const provided = bearerToken(request.headers.authorization) ?? queryToken;
    if (provided === undefined || !tokenMatches(provided, adminToken)) {
      return HttpServerResponse.text("unauthorized", { status: 401 });
    }

    // Optional ?session=<id> filter: that session's events, its runs' events
    // and their log chunks — plus system-wide events.
    let filter: SessionId | null = null;
    if (searchParams.session !== undefined) {
      const decoded = decodeSessionId(searchParams.session);
      if (Option.isNone(decoded)) {
        return HttpServerResponse.text("invalid session id", { status: 400 });
      }
      filter = decoded.value;
    }
    const matches = (event: MaestroEvent): boolean => {
      const owner = sessionIdOf(event);
      return filter === null || owner === null || owner === filter;
    };

    const bus = yield* EventBus;
    const sessionRepo = yield* SessionRepo;
    const taskRunRepo = yield* TaskRunRepo;

    // Subscribe BEFORE the snapshot read; the request scope holds the
    // subscription until the client disconnects.
    const subscription = yield* bus.subscribe();

    const sessions = filter === null ? yield* sessionRepo.list() : [yield* sessionRepo.get(filter)];
    const activeRuns = yield* taskRunRepo.listActive();
    const snapshot: ReadonlyArray<MaestroEvent> = [
      SystemStatus.make({
        activeTurns: activeRuns.length,
        maxConcurrentWorkers,
        dbReachable: true, // the snapshot reads above just succeeded
      }),
      ...sessions.map((session) => SessionStateChanged.make({ session })),
      ...activeRuns.map((taskRun) => TaskRunStateChanged.make({ taskRun })),
    ].filter(matches);

    const events = Stream.concat(
      Stream.fromIterable(snapshot),
      Stream.fromSubscription(subscription).pipe(Stream.filter(matches)),
    ).pipe(Stream.map(frame));
    // SSE comment lines keep proxies and EventSource reconnect timers alive.
    const heartbeats = Stream.tick(HEARTBEAT_MILLIS).pipe(Stream.map(() => ": heartbeat\n\n"));

    return HttpServerResponse.stream(Stream.merge(events, heartbeats).pipe(Stream.encodeText), {
      contentType: "text/event-stream",
      headers: { "cache-control": "no-cache" },
    });
  }).pipe(
    Effect.catch((error) =>
      error instanceof EntityNotFoundError
        ? Effect.succeed(HttpServerResponse.text("session not found", { status: 404 }))
        : Effect.succeed(HttpServerResponse.text("database unavailable", { status: 503 })),
    ),
  ),
);
