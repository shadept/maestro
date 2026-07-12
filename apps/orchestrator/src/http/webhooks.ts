import { Effect } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { LinearIngest } from "../ingest/LinearIngest.ts";

// POST /api/webhooks/linear (FUR-18). No admin token here — the HMAC
// signature IS the authentication. The raw body text is read before any JSON
// parsing because the signature covers the exact bytes on the wire.
//
// M1 DECISION: ingestion (session/turn creation + queue insert) runs
// synchronously in-request — a few DB statements, well within Linear's
// webhook timeout — so a 200 means the turn is durably queued. Deferring the
// heavy work behind an ack is an M2 concern if mapping ever grows slow.

export const WebhookRoutes = HttpRouter.add(
  "POST",
  "/api/webhooks/linear",
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const rawBody = yield* request.text;
    const ingest = yield* LinearIngest;
    const outcome = yield* ingest.handleDelivery({
      rawBody,
      signature: request.headers["linear-signature"],
      deliveryId: request.headers["linear-delivery"],
    });
    // Duplicates are 200 by contract: Linear already delivered this once and
    // must not retry it.
    return HttpServerResponse.jsonUnsafe({
      outcome: outcome._tag,
      ...(outcome._tag === "Ignored" && { reason: outcome.reason }),
    });
  }).pipe(
    Effect.catch((error) => {
      switch (error._tag) {
        case "WebhookVerificationError":
          return Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: error.reason }, { status: 401 }),
          );
        case "IngestMappingError":
          return Effect.succeed(
            HttpServerResponse.jsonUnsafe({ error: error.reason }, { status: 400 }),
          );
        default:
          // DB/queue trouble: 500 so Linear retries the delivery later.
          return Effect.logError("linear webhook ingestion failed", error).pipe(
            Effect.as(HttpServerResponse.jsonUnsafe({ error: "internal" }, { status: 500 })),
          );
      }
    }),
  ),
);
