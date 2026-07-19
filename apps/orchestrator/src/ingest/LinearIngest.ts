import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import {
  type DbError,
  type GitError,
  IngestMappingError,
  type QueueError,
  type TaskContext,
  WebhookVerificationError,
} from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted, Schema } from "effect";
import { MAESTRO_COMMENT_MARKER } from "../callback/format.ts";
import { LinearCallback } from "../callback/LinearCallback.ts";
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

/**
 * Case-insensitive word-boundary `@<handle>` detection in comment bodies
 * (FUR-37). Linear serializes app mentions as literal plain-text `@maestro`
 * in webhook comment bodies (captured payload evidence — no id markup
 * survives), so the handle string is the only mention signal deliveries
 * carry. Boundaries: the char before `@` must not be a word char or another
 * `@` (test@maestro.dev is an email, not a summon), and the handle must not
 * continue into a longer word (`@maestrofoo`, `@maestro-bot` are someone
 * else).
 */
export const mentionsHandle = (body: string, handle: string): boolean => {
  const escaped = handle.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`(?<![\\w@])@${escaped}(?![\\w-])`, "i").test(body);
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

const LinearTeam = Schema.Struct({
  id: Schema.String,
  key: Schema.String,
  name: Schema.String,
});

const IssueData = Schema.Struct({
  id: Schema.String,
  identifier: Schema.String,
  title: Schema.String,
  description: Schema.optionalKey(Schema.NullOr(Schema.String)),
  /** The agent user the issue is delegated to (FUR-37) — THE trigger identity. */
  delegateId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  team: Schema.optionalKey(Schema.NullOr(LinearTeam)),
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
        team: Schema.optionalKey(Schema.NullOr(LinearTeam)),
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
 * model (FUR-37, replacing the original label trigger): delegating an issue
 * to the Maestro app user hands it to Maestro; an `@<handle>` mention in a
 * comment queues a follow-up turn; plain comments are inert. All actual
 * session/turn work happens in the forge-agnostic IngestPipeline.
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
    ) => Effect.Effect<IngestOutcome, LinearIngestError | DbError | QueueError | GitError>;
  }
>()("maestro/ingest/LinearIngest") {
  static readonly layer = Layer.effect(
    LinearIngest,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const deliveryRepo = yield* DeliveryRepo;
      const projectRepo = yield* ProjectRepo;
      const pipeline = yield* IngestPipeline;
      const callback = yield* LinearCallback;

      const verify = Effect.fn(function* (delivery: LinearDelivery) {
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
          !signatureMatches(delivery.rawBody, delivery.signature, config.linearWebhookSecret.value)
        ) {
          return yield* new WebhookVerificationError({
            source: "linear",
            reason: "signature mismatch",
          });
        }
      });

      /**
       * Team key → registered project, shared by both trigger paths. None =
       * unregistered team: not an error (Linear would retry forever), but
       * loudly logged.
       */
      const projectForTeam = Effect.fn(function* (teamKey: string | undefined, deliveryId: string) {
        if (teamKey === undefined) {
          return yield* new IngestMappingError({
            deliveryId,
            reason: "payload has no team key",
          });
        }
        const project = yield* projectRepo.findByLinearTeamKey(teamKey);
        if (Option.isNone(project)) {
          yield* Effect.logWarning("LinearIngest: no project registered for team", { teamKey });
        }
        return project;
      });

      const mapIssueEvent = Effect.fn(function* (
        envelope: typeof Envelope.Type,
        deliveryId: string,
        payload: unknown,
      ) {
        const issue = yield* decodeIssue(envelope.data).pipe(
          Effect.mapError((error) => new IngestMappingError({ deliveryId, reason: String(error) })),
        );
        const actor = envelope.actor?.name ?? "linear";
        const ticket = { source: "linear", externalId: issue.identifier } as const;

        // Terminal signal first: a done/canceled move outranks any
        // delegation evidence riding the same issue update.
        const terminal =
          envelope.action === "update" && hasKey(envelope.updatedFrom, "stateId")
            ? TERMINAL_STATE_TYPES[issue.state?.type ?? ""]
            : undefined;
        if (terminal !== undefined) {
          return yield* pipeline.recordTerminal({ ticket, actor, signal: terminal });
        }

        // Delegation trigger (FUR-37): the handoff signal is the delegate
        // actually CHANGING to the Maestro app user on this very event —
        // updatedFrom carries the prior delegateId (same evidence pattern
        // as the terminal stateId check above), so "still delegated on
        // some unrelated issue edit" never re-triggers. `assigneeId` is
        // deliberately not consulted: Linear's assign-to-agent UX sets the
        // HUMAN as assignee and the agent as delegate in the same update
        // (captured payload evidence), so assignee evidence would
        // mis-trigger on ordinary human assignments.
        const delegateChanged =
          envelope.action === "update" && hasKey(envelope.updatedFrom, "delegateId");
        if (!delegateChanged) {
          return {
            _tag: "Ignored",
            reason: "issue event carries no delegation change",
          } satisfies IngestOutcome;
        }
        const delegateId = issue.delegateId ?? null;
        if (delegateId === null) {
          return {
            _tag: "Ignored",
            reason: "issue was un-delegated",
          } satisfies IngestOutcome;
        }
        if (Option.isNone(config.linearBotUserId)) {
          // Load-bearing config gap: without the app user id we cannot
          // recognize delegations addressed to us. Ignored (never a boot
          // failure), but loud enough to diagnose from the logs.
          yield* Effect.logWarning(
            "LinearIngest: issue delegation received but MAESTRO_LINEAR_BOT_USER_ID is not configured — delegations cannot be recognized as Maestro's",
            { issue: issue.identifier },
          );
          return {
            _tag: "Ignored",
            reason: "MAESTRO_LINEAR_BOT_USER_ID is not configured; delegation not recognized",
          } satisfies IngestOutcome;
        }
        if (delegateId !== config.linearBotUserId.value) {
          return {
            _tag: "Ignored",
            reason: "issue delegated to a different agent user",
          } satisfies IngestOutcome;
        }

        const project = yield* projectForTeam(issue.team?.key, deliveryId);
        if (Option.isNone(project)) {
          return {
            _tag: "Ignored",
            reason: `no project registered for Linear team ${issue.team?.key}`,
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
        // A delegate change is always an explicit human act, so it doubles
        // as the FUR-39 resume signal: re-delegating a breaker-paused
        // session's issue (necessarily after un-delegating it first —
        // updatedFrom must show the delegate changed) clears the breaker
        // and queues a fresh turn.
        return yield* pipeline.startTask({
          project: project.value,
          context,
          resumeSignal: true,
        });
      });

      const mapCommentEvent = Effect.fn(function* (
        envelope: typeof Envelope.Type,
        deliveryId: string,
        payload: unknown,
      ) {
        if (envelope.action !== "create") {
          return {
            _tag: "Ignored",
            reason: `comment ${envelope.action} events are not turns`,
          } satisfies IngestOutcome;
        }
        const comment = yield* decodeComment(envelope.data).pipe(
          Effect.mapError((error) => new IngestMappingError({ deliveryId, reason: String(error) })),
        );
        // Self-trigger guards — BOTH run before mention detection, because
        // Maestro's own comments legitimately contain "@maestro" text (the
        // paused-session message tells the human to mention it).
        //
        // Layer 1 (FUR-39): content marker. Every comment format.ts renders
        // starts with MAESTRO_COMMENT_MARKER, so a body carrying it is ours
        // regardless of author — essential when MAESTRO_LINEAR_API_TOKEN is
        // a personal key (single-account setups), where the author id can't
        // separate Maestro from the human. The other FUR-39 layers live
        // elsewhere: the consecutive-failure circuit breaker in
        // TurnSettlement/IngestPipeline, failure-comment dedup in the outbox
        // idempotency key (TurnSettlement.outcomeIdempotencyKey).
        if (comment.body.startsWith(MAESTRO_COMMENT_MARKER)) {
          return {
            _tag: "Ignored",
            reason: "comment carries the Maestro marker (self-trigger guard)",
          } satisfies IngestOutcome;
        }
        // Layer 2 of the guard: the configured bot user id (the identity
        // behind MAESTRO_LINEAR_API_TOKEN) is dropped too, when set.
        if (
          Option.isSome(config.linearBotUserId) &&
          comment.userId === config.linearBotUserId.value
        ) {
          return {
            _tag: "Ignored",
            reason: "comment authored by the Maestro bot user",
          } satisfies IngestOutcome;
        }
        // Plain comments are inert (FUR-37, deliberate behavior change):
        // only an explicit @<handle> mention summons Maestro — humans
        // talking to each other on a worked ticket no longer queue turns.
        if (!mentionsHandle(comment.body, config.linearMentionHandle)) {
          return {
            _tag: "Ignored",
            reason: `plain comment (no @${config.linearMentionHandle} mention)`,
          } satisfies IngestOutcome;
        }
        const identifier = comment.issue?.identifier;
        if (identifier === undefined) {
          return yield* new IngestMappingError({
            deliveryId,
            reason: "comment payload has no issue identifier",
          });
        }
        const ticket = { source: "linear", externalId: identifier } as const;
        const actor = comment.user?.name ?? envelope.actor?.name ?? "linear";
        if (yield* pipeline.hasActiveSession(ticket)) {
          // No delegation re-check for existing sessions: any active
          // session accepts mention-driven turns, so sessions started
          // before FUR-37 (label-triggered) keep working unchanged. Every
          // mention is an explicit human summon, so it doubles as the
          // FUR-39 resume signal on a breaker-paused session.
          const context: TaskContext = {
            source: "linear",
            ticket,
            actor,
            title: null,
            body: comment.body,
            deliveryId,
            payload,
          };
          return yield* pipeline.queueTurn({ context, resumeSignal: true });
        }

        // Session-less mention: it may START work, but only on an issue
        // actually delegated to Maestro. Comment webhooks carry neither the
        // delegate nor the issue description, so both come from the Linear
        // API. A mention on a non-delegated issue is Ignored — being
        // mentionable must not let any bystander comment start a session.
        if (Option.isNone(config.linearBotUserId)) {
          yield* Effect.logWarning(
            "LinearIngest: mention on an issue with no active session, but MAESTRO_LINEAR_BOT_USER_ID is not configured — cannot verify the issue is delegated to Maestro",
            { issue: identifier },
          );
          return {
            _tag: "Ignored",
            reason: "MAESTRO_LINEAR_BOT_USER_ID is not configured; cannot verify delegation",
          } satisfies IngestOutcome;
        }
        const botUserId = config.linearBotUserId.value;
        // Lookup failures are Ignored, not HTTP errors: the delivery is
        // already recorded for dedup, so a Linear retry would no-op as
        // Duplicate anyway — surfacing a 500 could not recover the event.
        // The human re-mentions once the token/connectivity is fixed.
        const delegation = yield* callback.fetchIssueDelegation({ issueId: comment.issueId }).pipe(
          Effect.map(Option.some),
          Effect.catch((error) =>
            Effect.logWarning("LinearIngest: delegation lookup failed for a session-less mention", {
              issue: identifier,
              error: String(error),
            }).pipe(Effect.as(Option.none())),
          ),
        );
        if (Option.isNone(delegation)) {
          return {
            _tag: "Ignored",
            reason: `could not verify delegation for ${identifier} (Linear lookup failed); re-mention once MAESTRO_LINEAR_API_TOKEN/connectivity is fixed`,
          } satisfies IngestOutcome;
        }
        if (delegation.value.delegateId !== botUserId) {
          return {
            _tag: "Ignored",
            reason: `mention on ${identifier}, which is not delegated to Maestro`,
          } satisfies IngestOutcome;
        }
        const project = yield* projectForTeam(comment.issue?.team?.key, deliveryId);
        if (Option.isNone(project)) {
          return {
            _tag: "Ignored",
            reason: `no project registered for Linear team ${comment.issue?.team?.key}`,
          } satisfies IngestOutcome;
        }
        // FIRST-TURN COMPOSITION (decided here, FUR-37): the issue
        // description leads — it is the task, and the agent should see the
        // ticket exactly as a delegation-started session would — with the
        // summoning comment appended under a divider as the instruction
        // that woke Maestro up.
        const description = delegation.value.description?.trim() ?? "";
        const body =
          description === ""
            ? comment.body
            : `${description}\n\n---\n\nSummoning comment:\n\n${comment.body}`;
        const context: TaskContext = {
          source: "linear",
          ticket,
          actor,
          title: comment.issue?.title ?? null,
          body,
          deliveryId,
          payload,
        };
        return yield* pipeline.startTask({
          project: project.value,
          context,
          resumeSignal: true,
        });
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
