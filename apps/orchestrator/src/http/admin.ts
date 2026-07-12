import { AdminApi, AdminAuth } from "@maestro/api";
import { type DbError, EntityNotFoundError } from "@maestro/domain";
import { Effect, Layer, Redacted } from "effect";
import { HttpApiBuilder, HttpApiError } from "effect/unstable/httpapi";
import { AppConfig } from "../config/AppConfig.ts";
import { SessionRepo } from "../db/SessionRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { worktreeDir } from "../storage/paths.ts";
import { tokenMatches } from "./auth.ts";

// Server side of the @maestro/api AdminApi contract (FUR-16). The contract
// (endpoints, schemas, the AdminAuth middleware key) lives in packages/api;
// this file provides the implementations.

/** Verifies `Authorization: Bearer <MAESTRO_ADMIN_TOKEN>` in constant time. */
const AdminAuthLive = Layer.effect(
  AdminAuth,
  Effect.gen(function* () {
    const { adminToken } = yield* AppConfig;
    return {
      adminToken: (httpEffect, { credential }) =>
        tokenMatches(Redacted.value(credential), adminToken)
          ? httpEffect
          : Effect.fail(new HttpApiError.Unauthorized({})),
    };
  }),
);

const AdminHandlersLive = HttpApiBuilder.group(AdminApi, "admin", (handlers) =>
  Effect.gen(function* () {
    const sessionRepo = yield* SessionRepo;
    const taskRunRepo = yield* TaskRunRepo;
    const { storageRoot } = yield* AppConfig;

    // Missing entities map to the contract's 404; any other DbError is an
    // infrastructure defect here (500), not part of the API contract.
    const orNotFound = <A>(effect: Effect.Effect<A, DbError>) =>
      effect.pipe(
        Effect.catch((error) =>
          error instanceof EntityNotFoundError
            ? Effect.fail(new HttpApiError.NotFound({}))
            : Effect.die(error),
        ),
      );

    return handlers
      .handle("listSessions", () => Effect.orDie(sessionRepo.list()))
      .handle("getSession", ({ params }) => orNotFound(sessionRepo.get(params.sessionId)))
      .handle("listTaskRuns", ({ params }) =>
        // listBySession is [] for unknown ids — probe the session so unknown
        // sessions 404 instead of returning an empty list.
        orNotFound(
          sessionRepo
            .get(params.sessionId)
            .pipe(Effect.andThen(taskRunRepo.listBySession(params.sessionId))),
        ),
      )
      .handle("getSessionWorkspace", ({ params }) =>
        // The worktree path is deterministic from storage root + session id;
        // probe the session so unknown ids 404 like every other endpoint.
        orNotFound(
          sessionRepo
            .get(params.sessionId)
            .pipe(Effect.map(() => ({ worktreePath: worktreeDir(storageRoot, params.sessionId) }))),
        ),
      )
      .handle("getTaskRunLogs", ({ params }) => orNotFound(taskRunRepo.getLogs(params.taskRunId)))
      .handle("getTaskRunContext", ({ params }) =>
        orNotFound(taskRunRepo.getContext(params.taskRunId)),
      );
  }),
);

/** Mounts the admin read API onto the HttpRouter; consumed by main.ts. */
export const AdminApiRoutes = HttpApiBuilder.layer(AdminApi).pipe(
  Layer.provide(AdminHandlersLive),
  Layer.provide(AdminAuthLive),
);
