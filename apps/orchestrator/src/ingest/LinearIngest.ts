import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import {
  type DbError,
  IngestMappingError,
  type QueueError,
  type TaskContext,
  WebhookVerificationError,
} from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted, Schema } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { DeliveryRepo } from "../db/DeliveryRepo.ts";
import { ProjectRepo } from "../db/ProjectRepo.ts";
import { type IngestOutcome, IngestPipeline } from "./IngestPipeline.ts";

/**
 * Linear webhook signing convention (verified against @linear/sdk 88's
 * LinearWebhookClient): `linear-signature` header = HMAC-SHA256 hex digest of
 * the raw request body with the webhook secret; the signed body carries a
 * `webhookTimestamp` (unix ms) that must be within one minute of now.
 */
const REPLAY_WINDOW_MILLIS = 60_000;

/** Linear workflow-state types that end a ticket (of triage/backlog/unstarted/started/...). */
const TERMINAL_STATE_TYPES: Record<string, "done" | "canceled"> = {
  completed: "done",
  canceled: "canceled",
};

// ── the slices of Linear's webhook payloads we consume ─────────────────────
// Shaped after @linear/sdk's EntityWebhookPayload / IssueWebhookPayload /
// CommentWebhookPayload types; unknown extra fields are ignored, fields we
// merely echo stay untyped inside `payload`.

const LinearUser = Schema.Struct({
  id: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
});

const Envelope = Schema.Struct({
  type: Schema.String,
  action: Schema.String,
  actor: Schema.optionalKey(Schema.NullOr(LinearUser)),
  data: Schema.Unknown,
  updatedFrom: Schema.optionalKey(Schema.NullOr(Schema.Unknown)),
  webhookTimestamp: Schema.optionalKey(Schema.Number),
});

const IssueData = Schema.Struct({
  id: Schema.String,
  identifier: Schema.String,
  title: Schema.String,
  description: Schema.optionalKey(Schema.NullOr(Schema.String)),
  labels: Schema.optionalKey(
    Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String })),
  ),
  team: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ id: Schema.String, key: Schema.String, name: Schema.String })),
  ),
  state: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ id: Schema.String, name: Schema.String, type: Schema.String })),
  ),
});

const CommentData = Schema.Struct({
  id: Schema.String,
  body: Schema.String,
  userId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  issueId: Schema.String,
  issue: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        id: Schema.String,
        identifier: Schema.optionalKey(Schema.String),
        title: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
  user: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ id: Schema.String, name: Schema.optionalKey(Schema.String) })),
  ),
});

const decodeEnvelope = Schema.decodeUnknownEffect(Envelope);
const decodeIssue = Schema.decodeUnknownEffect(IssueData);
const decodeComment = Schema.decodeUnknownEffect(CommentData);

/** Constant-time signature compare (same hashing trick as http/auth.ts). */
const signatureMatches = (rawBody: string, signature: string, secret: Redacted.Redacted) => {
  const expected = createHmac("sha256", Redacted.value(secret)).update(rawBody).digest("hex");
  const a = createHash("sha256").update(expected).digest();
  const b = createHash("sha256").update(signature).digest();
  return timingSafeEqual(a, b);
};

const hasKey = (value: unknown, key: string): boolean =>
  typeof value === "object" && value !== null && key in value;

export interface LinearDelivery {
  /** The exact request body bytes — the HMAC input. Parsed only after verification. */
  readonly rawBody: string;
  /** `linear-signature` header. */
  readonly signature: string | undefined;
  /** `linear-delivery` header — Linear's per-delivery UUID, our dedup key. */
  readonly deliveryId: string | undefined;
}

export type LinearIngestError = WebhookVerificationError | IngestMappingError;

/**
 * The Linear webhook adapter (FUR-18): verification, immutable delivery
 * persistence + dedup, and Linear-payload → TaskContext mapping. Trigger
 * model is the ticket's default — a configured label (`linearTriggerLabel`)
 * hands an issue to Maestro; agent-delegation is out of scope until the M2
 * spike. All actual session/turn work happens in the forge-agnostic
 * IngestPipeline.
 */
export class LinearIngest extends Context.Service<
  LinearIngest,
  {
    /**
     * Handles one webhook delivery end to end. Fails with
     * WebhookVerificationError (401) or IngestMappingError (400); every
     * understood-but-irrelevant event is a successful Ignored outcome.
     */
    readonly handleDelivery: (
      delivery: LinearDelivery,
    ) => Effect.Effect<IngestOutcome, LinearIngestError | DbError | QueueError>;
  }
>()("maestro/ingest/LinearIngest") {
  static readonly layer = Layer.effect(
    LinearIngest,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const deliveryRepo = yield* DeliveryRepo;
      const projectRepo = yield* ProjectRepo;
      const pipeline = yield* IngestPipeline;

      const triggerLabel = config.linearTriggerLabel.toLowerCase();

      const verify = (delivery: LinearDelivery) =>
        Effect.gen(function* () {
          if (Option.isNone(config.linearWebhookSecret)) {
            // Never guess: an unverifiable delivery is a rejected delivery.
            return yield* new WebhookVerificationError({
              source: "linear",
              reason: "MAESTRO_LINEAR_WEBHOOK_SECRET is not configured",
            });
          }
          if (delivery.signature === undefined) {
            return yield* new WebhookVerificationError({
              source: "linear",
              reason: "missing linear-signature header",
            });
          }
          if (
            !signatureMatches(
              delivery.rawBody,
              delivery.signature,
              config.linearWebhookSecret.value,
            )
          ) {
            return yield* new WebhookVerificationError({
              source: "linear",
              reason: "signature mismatch",
            });
          }
        });

      const mapIssueEvent = (
        envelope: typeof Envelope.Type,
        deliveryId: string,
        payload: unknown,
      ) =>
        Effect.gen(function* () {
          const issue = yield* decodeIssue(envelope.data).pipe(
            Effect.mapError(
              (error) => new IngestMappingError({ deliveryId, reason: String(error) }),
            ),
          );
          const actor = envelope.actor?.name ?? "linear";
          const ticket = { source: "linear", externalId: issue.identifier } as const;

          // Terminal signal first: a done/canceled move outranks the trigger
          // label still being present on the issue.
          const terminal =
            envelope.action === "update" && hasKey(envelope.updatedFrom, "stateId")
              ? TERMINAL_STATE_TYPES[issue.state?.type ?? ""]
              : undefined;
          if (terminal !== undefined) {
            return yield* pipeline.recordTerminal({ ticket, actor, signal: terminal });
          }

          const labeled = (issue.labels ?? []).some(
            (label) => label.name.toLowerCase() === triggerLabel,
          );
          if (!labeled) {
            return { _tag: "Ignored", reason: "trigger label absent" } satisfies IngestOutcome;
          }
          const teamKey = issue.team?.key;
          if (teamKey === undefined) {
            return yield* new IngestMappingError({
              deliveryId,
              reason: "issue payload has no team key",
            });
          }
          const project = yield* projectRepo.findByLinearTeamKey(teamKey);
          if (Option.isNone(project)) {
            // Someone labeled an issue of an unregistered team: not an error
            // (Linear would retry forever), but loudly logged.
            yield* Effect.logWarning("LinearIngest: no project registered for team", { teamKey });
            return {
              _tag: "Ignored",
              reason: `no project registered for Linear team ${teamKey}`,
            } satisfies IngestOutcome;
          }
          const context: TaskContext = {
            source: "linear",
            ticket,
            actor,
            title: issue.title,
            body: issue.description ?? "",
            deliveryId,
            payload,
          };
          return yield* pipeline.startTask({ project: project.value, context });
        });

      const mapCommentEvent = (
        envelope: typeof Envelope.Type,
        deliveryId: string,
        payload: unknown,
      ) =>
        Effect.gen(function* () {
          if (envelope.action !== "create") {
            return {
              _tag: "Ignored",
              reason: `comment ${envelope.action} events are not turns`,
            } satisfies IngestOutcome;
          }
          const comment = yield* decodeComment(envelope.data).pipe(
            Effect.mapError(
              (error) => new IngestMappingError({ deliveryId, reason: String(error) }),
            ),
          );
          // Self-trigger guard: the callback worker posts turn results as
          // comments, and those comments come right back as webhooks. The
          // configured bot user id (the identity behind MAESTRO_LINEAR_API_TOKEN)
          // is dropped here, breaking the loop.
          if (
            Option.isSome(config.linearBotUserId) &&
            comment.userId === config.linearBotUserId.value
          ) {
            return {
              _tag: "Ignored",
              reason: "comment authored by the Maestro bot user",
            } satisfies IngestOutcome;
          }
          const identifier = comment.issue?.identifier;
          if (identifier === undefined) {
            return yield* new IngestMappingError({
              deliveryId,
              reason: "comment payload has no issue identifier",
            });
          }
          const context: TaskContext = {
            source: "linear",
            ticket: { source: "linear", externalId: identifier },
            actor: comment.user?.name ?? envelope.actor?.name ?? "linear",
            title: null,
            body: comment.body,
            deliveryId,
            payload,
          };
          return yield* pipeline.queueTurn({ context });
        });

      return {
        handleDelivery: Effect.fn("LinearIngest.handleDelivery")(function* (
          delivery: LinearDelivery,
        ) {
          yield* verify(delivery);

          const deliveryId = delivery.deliveryId;
          if (deliveryId === undefined || deliveryId.length === 0) {
            return yield* new IngestMappingError({
              deliveryId: "<missing>",
              reason: "missing linear-delivery header",
            });
          }

          const payload = yield* Effect.try({
            try: () => JSON.parse(delivery.rawBody) as unknown,
            catch: () => new IngestMappingError({ deliveryId, reason: "body is not valid JSON" }),
          });
          const envelope = yield* decodeEnvelope(payload).pipe(
            Effect.mapError(
              (error) => new IngestMappingError({ deliveryId, reason: String(error) }),
            ),
          );

          // Replay protection on top of the signature (Linear convention).
          if (
            envelope.webhookTimestamp !== undefined &&
            Math.abs(Date.now() - envelope.webhookTimestamp) > REPLAY_WINDOW_MILLIS
          ) {
            return yield* new WebhookVerificationError({
              source: "linear",
              reason: "webhookTimestamp outside the replay window",
            });
          }

          // Immutable raw persistence + dedup in one statement (FUR-9).
          const isNew = yield* deliveryRepo.recordIfNew({ source: "linear", deliveryId, payload });
          if (!isNew) {
            return { _tag: "Duplicate" } satisfies IngestOutcome;
          }

          switch (envelope.type) {
            case "Issue":
              return yield* mapIssueEvent(envelope, deliveryId, payload);
            case "Comment":
              return yield* mapCommentEvent(envelope, deliveryId, payload);
            default:
              return {
                _tag: "Ignored",
                reason: `unhandled event type ${envelope.type}`,
              } satisfies IngestOutcome;
          }
        }),
      };
    }),
  );
}
