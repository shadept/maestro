import { AdminApi } from "@maestro/api";
import type { SessionId, TaskRunId } from "@maestro/domain";
import { Effect } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { HttpApiClient } from "effect/unstable/httpapi";

// The typed admin client, derived from the AdminApi contract in @maestro/api
// via HttpApiClient — request/response types come from the contract schemas,
// no hand-written fetch types (CLAUDE.md). Wrapped as promise-returning
// functions because the views are plain Solid, not Effect programs.

const clientFor = (token: string) =>
  HttpApiClient.make(AdminApi, {
    // AdminAuth is Bearer security; the derived client attaches the header here.
    transformClient: HttpClient.mapRequest(HttpClientRequest.bearerToken(token)),
  }).pipe(Effect.provide(FetchHttpClient.layer));

type Derived = Effect.Success<ReturnType<typeof clientFor>>;

export const createAdminClient = (token: string) => {
  const client = clientFor(token);
  const call = <A, E>(f: (derived: Derived) => Effect.Effect<A, E>): Promise<A> =>
    Effect.runPromise(Effect.flatMap(client, f));

  return {
    listSessions: () => call((c) => c.admin.listSessions()),
    getSession: (sessionId: SessionId) =>
      call((c) => c.admin.getSession({ params: { sessionId } })),
    listTaskRuns: (sessionId: SessionId) =>
      call((c) => c.admin.listTaskRuns({ params: { sessionId } })),
    getSessionWorkspace: (sessionId: SessionId) =>
      call((c) => c.admin.getSessionWorkspace({ params: { sessionId } })),
    getTaskRunLogs: (taskRunId: TaskRunId) =>
      call((c) => c.admin.getTaskRunLogs({ params: { taskRunId } })),
    getTaskRunContext: (taskRunId: TaskRunId) =>
      call((c) => c.admin.getTaskRunContext({ params: { taskRunId } })),
    getObservabilityConfig: () => call((c) => c.admin.getObservabilityConfig()),
  };
};

export type AdminClient = ReturnType<typeof createAdminClient>;
