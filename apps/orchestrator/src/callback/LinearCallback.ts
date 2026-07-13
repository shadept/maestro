import { LinearClient } from "@linear/sdk";
import { CallbackDeliveryError, type CallbackError } from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted } from "effect";
import { AppConfig } from "../config/AppConfig.ts";

/** A comment the fake callback observed, recorded for test assertions. */
export interface LinearCommentCall {
  readonly issueId: string;
  readonly body: string;
}

/** How a Linear token authenticates — mirrors the SDK's two constructor options. */
export type LinearTokenKind = "api-key" | "oauth";

/**
 * Personal API keys carry the `lin_api_` prefix (Linear → Security & access);
 * OAuth app-actor access tokens do not (FUR-42). Legacy unprefixed personal
 * keys are the one case the heuristic misreads — MAESTRO_LINEAR_TOKEN_KIND
 * overrides it explicitly.
 */
export const detectLinearTokenKind = (token: string): LinearTokenKind =>
  token.startsWith("lin_api_") ? "api-key" : "oauth";

/**
 * The @linear/sdk constructor options for a token. The two kinds send
 * different Authorization headers (verified in @linear/sdk 88
 * parseClientOptions): `apiKey` goes out raw, as personal keys require;
 * `accessToken` goes out as `Bearer <token>`, as OAuth demands.
 */
export const linearClientOptionsFor = (
  token: string,
  kind: Option.Option<LinearTokenKind> = Option.none(),
): { readonly apiKey: string } | { readonly accessToken: string } => {
  const resolved = Option.getOrElse(kind, () => detectLinearTokenKind(token));
  return resolved === "api-key" ? { apiKey: token } : { accessToken: token };
};

/**
 * The Linear issue a raw webhook payload (TaskContext.payload — "preserved
 * opaquely for outbound responders") belongs to: `data.issueId` on Comment
 * events, `data.id` on Issue events. Null when the payload is not a Linear
 * webhook shape.
 */
export const linearIssueIdFrom = (payload: unknown): string | null => {
  if (typeof payload !== "object" || payload === null || !("data" in payload)) return null;
  const data = (payload as { data: unknown }).data;
  if (typeof data !== "object" || data === null) return null;
  if ("issueId" in data && typeof data.issueId === "string") return data.issueId;
  if ("id" in data && typeof data.id === "string") return data.id;
  return null;
};

/** What FUR-37's mention-on-a-session-less-issue check needs to know about an issue. */
export interface LinearIssueDelegation {
  /** The agent user the issue is delegated to; null when not delegated. */
  readonly delegateId: string | null;
  /** The issue description — comment webhooks don't carry it. */
  readonly description: string | null;
}

/**
 * Raw GraphQL for the delegation lookup: the installed @linear/sdk 88's
 * generated Issue model predates the `delegate` field (its GraphQL schema
 * types already list it — Issue.delegate, "the agent user that is delegated
 * to work on this issue"), so we query it directly via the SDK's rawRequest.
 */
const ISSUE_DELEGATION_QUERY = /* GraphQL */ `
  query MaestroIssueDelegation($id: String!) {
    issue(id: $id) {
      description
      delegate {
        id
      }
    }
  }
`;

interface IssueDelegationResponse {
  readonly issue?: {
    readonly description?: string | null;
    readonly delegate?: { readonly id: string } | null;
  } | null;
}

/**
 * The @linear/sdk wrapper (FUR-18) — the only place in the codebase that
 * imports the SDK. Absent MAESTRO_LINEAR_API_TOKEN never fails boot; every
 * call fails individually until the operator configures it.
 */
export class LinearCallback extends Context.Service<
  LinearCallback,
  {
    /** Creates a comment on a Linear issue (by issue UUID). */
    readonly postComment: (args: {
      readonly issueId: string;
      readonly body: string;
    }) => Effect.Effect<void, CallbackError>;
    /**
     * Who an issue is delegated to, plus its description (FUR-37): comment
     * webhooks carry neither, and a mention on a session-less issue must
     * verify the issue is actually delegated to Maestro before starting work.
     */
    readonly fetchIssueDelegation: (args: {
      readonly issueId: string;
    }) => Effect.Effect<LinearIssueDelegation, CallbackError>;
  }
>()("maestro/callback/LinearCallback") {
  static readonly layer = Layer.effect(
    LinearCallback,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const client = Option.map(
        config.linearApiToken,
        (token) =>
          new LinearClient(linearClientOptionsFor(Redacted.value(token), config.linearTokenKind)),
      );

      return {
        postComment: Effect.fn("LinearCallback.postComment")(function* (args) {
          if (Option.isNone(client)) {
            return yield* new CallbackDeliveryError({
              target: "linear",
              reason: "MAESTRO_LINEAR_API_TOKEN is not configured",
            });
          }
          const payload = yield* Effect.tryPromise({
            try: () => client.value.createComment({ issueId: args.issueId, body: args.body }),
            catch: (error) =>
              new CallbackDeliveryError({
                target: "linear",
                reason: error instanceof Error ? error.message : String(error),
              }),
          });
          if (!payload.success) {
            return yield* new CallbackDeliveryError({
              target: "linear",
              reason: "Linear commentCreate returned success: false",
            });
          }
        }),
        fetchIssueDelegation: Effect.fn("LinearCallback.fetchIssueDelegation")(function* (args) {
          if (Option.isNone(client)) {
            return yield* new CallbackDeliveryError({
              target: "linear",
              reason: "MAESTRO_LINEAR_API_TOKEN is not configured",
            });
          }
          const response = yield* Effect.tryPromise({
            try: () =>
              client.value.client.rawRequest<IssueDelegationResponse, { id: string }>(
                ISSUE_DELEGATION_QUERY,
                { id: args.issueId },
              ),
            catch: (error) =>
              new CallbackDeliveryError({
                target: "linear",
                reason: error instanceof Error ? error.message : String(error),
              }),
          });
          const issue = response.data?.issue;
          if (issue === undefined || issue === null) {
            return yield* new CallbackDeliveryError({
              target: "linear",
              reason: `Linear returned no issue for id ${args.issueId}`,
            });
          }
          return {
            delegateId: issue.delegate?.id ?? null,
            description: issue.description ?? null,
          };
        }),
      };
    }),
  );

  /**
   * In-memory fake per the .layerTest convention — never talks to Linear.
   * Pass `calls` to observe attempted posts (recorded whether or not they
   * fail, like real API traffic); `failWith` holds an injectable failure that
   * can be flipped on and off mid-test (retry paths). `delegations` seeds
   * fetchIssueDelegation by issue UUID — an unseeded issue fails the lookup,
   * exactly like an unconfigured token.
   */
  static readonly layerTest = (options: {
    readonly calls?: Array<LinearCommentCall>;
    readonly failWith?: { current: CallbackError | undefined };
    readonly delegations?: Record<string, LinearIssueDelegation>;
  }) =>
    Layer.succeed(LinearCallback)({
      postComment: Effect.fn("LinearCallback.postComment")(function* (args) {
        options.calls?.push({ issueId: args.issueId, body: args.body });
        const failure = options.failWith?.current;
        if (failure !== undefined) {
          return yield* Effect.fail(failure);
        }
      }),
      fetchIssueDelegation: Effect.fn("LinearCallback.fetchIssueDelegation")(function* (args) {
        const failure = options.failWith?.current;
        if (failure !== undefined) {
          return yield* Effect.fail(failure);
        }
        const delegation = options.delegations?.[args.issueId];
        if (delegation === undefined) {
          return yield* new CallbackDeliveryError({
            target: "linear",
            reason: `no fixture delegation for issue ${args.issueId}`,
          });
        }
        return delegation;
      }),
    });
}
