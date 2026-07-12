import { Effect } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import { Db } from "../db/Db.ts";

// K8s-style probes (Tech Requirements §15).
// /livez: process alive — never tied to dependencies, stays green through a
// Postgres outage so the pod is not restarted for a fault a restart can't fix.
// /readyz: reflects actual DB connectivity.

export const HealthRoutes = HttpRouter.addAll([
  HttpRouter.route("GET", "/livez", Effect.succeed(HttpServerResponse.text("ok"))),
  HttpRouter.route(
    "GET",
    "/readyz",
    Effect.gen(function* () {
      const { ping } = yield* Db;
      const reachable = yield* ping;
      return reachable
        ? HttpServerResponse.text("ok")
        : HttpServerResponse.text("database unreachable", { status: 503 });
    }),
  ),
]);
