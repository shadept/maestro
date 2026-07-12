import { Effect, Layer } from "effect";
import { HttpStaticServer } from "effect/unstable/http";
import { AppConfig } from "../config/AppConfig.ts";

// Serves the admin UI bundle (apps/admin-ui Vite output) from the orchestrator
// process itself — no separate static server (FUR-17). Mounted as a `GET /*`
// wildcard, which find-my-way ranks below every explicit route, so /api/*,
// /livez and /readyz always win.
//
// DECISIONS:
// - No SPA fallback: the admin UI uses hash-based routing (#/...), so every
//   document request is `/` and unknown paths are honest 404s.
// - Missing bundle dir degrades gracefully: HttpStaticServer stats files
//   per-request, so a dev orchestrator without a built UI boots fine and
//   serves 404s at `/` instead of crashing.
export const StaticRoutes = Layer.unwrap(
  Effect.gen(function* () {
    const { adminUiDist } = yield* AppConfig;
    return HttpStaticServer.layer({ root: adminUiDist, index: "index.html" });
  }),
);
