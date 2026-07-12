import { Session, SessionId, TaskContext, TaskRun, TaskRunId } from "@maestro/domain";
import { Schema } from "effect";
import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiError,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSecurity,
} from "effect/unstable/httpapi";

// Admin read API contract (consumed by the M1.13 admin UI). The orchestrator
// implements it with HttpApiBuilder; the UI derives its typed client from this
// same value. No hand-written fetch types anywhere (CLAUDE.md).

/**
 * Admin-token auth for the whole API. DECISION (auth scheme, M1.13 must
 * follow): `Authorization: Bearer <MAESTRO_ADMIN_TOKEN>` on every admin API
 * request. The SSE endpoint (`GET /api/events` — a raw streaming route outside
 * this contract, see below) additionally accepts `?token=<...>` because
 * EventSource cannot set headers.
 *
 * The middleware key lives here in the contract; its implementation (constant
 * time comparison against the configured token) lives in the orchestrator.
 */
export class AdminAuth extends HttpApiMiddleware.Service<AdminAuth>()("maestro/api/AdminAuth", {
  security: { adminToken: HttpApiSecurity.bearer },
  error: HttpApiError.Unauthorized,
}) {}

/**
 * Server-derived workspace facts for a session. The worktree path lives under
 * the orchestrator's storage root — only the server knows it, so it travels
 * via the API instead of being guessed client-side.
 */
export const SessionWorkspace = Schema.Struct({
  worktreePath: Schema.NonEmptyString,
});
export type SessionWorkspace = typeof SessionWorkspace.Type;

/**
 * NOTE: `GET /api/events` (SSE) is deliberately NOT part of this contract —
 * it is a long-lived text/event-stream endpoint served by a raw route. Its
 * payload contract is `MaestroEventFromJsonString` in `events.ts`.
 */
export const AdminApi = HttpApi.make("maestro-admin")
  .add(
    HttpApiGroup.make("admin")
      .add(HttpApiEndpoint.get("listSessions", "/sessions", { success: Schema.Array(Session) }))
      .add(
        HttpApiEndpoint.get("getSession", "/sessions/:sessionId", {
          params: { sessionId: SessionId },
          success: Session,
          error: HttpApiError.NotFound,
        }),
      )
      .add(
        HttpApiEndpoint.get("listTaskRuns", "/sessions/:sessionId/runs", {
          params: { sessionId: SessionId },
          success: Schema.Array(TaskRun),
          error: HttpApiError.NotFound,
        }),
      )
      .add(
        HttpApiEndpoint.get("getSessionWorkspace", "/sessions/:sessionId/workspace", {
          params: { sessionId: SessionId },
          success: SessionWorkspace,
          error: HttpApiError.NotFound,
        }),
      )
      .add(
        HttpApiEndpoint.get("getTaskRunLogs", "/runs/:taskRunId/logs", {
          params: { taskRunId: TaskRunId },
          success: Schema.String,
          error: HttpApiError.NotFound,
        }),
      )
      .add(
        // The normalized inbound payload that triggered the turn (M1.13
        // session-detail view). TaskContext.payload carries the original
        // platform payload opaquely.
        HttpApiEndpoint.get("getTaskRunContext", "/runs/:taskRunId/context", {
          params: { taskRunId: TaskRunId },
          success: TaskContext,
          error: HttpApiError.NotFound,
        }),
      ),
  )
  .middleware(AdminAuth)
  .prefix("/api");
