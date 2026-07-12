import { LinearClient } from "@linear/sdk";
import { CallbackDeliveryError, type CallbackError } from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted } from "effect";
import { AppConfig } from "../config/AppConfig.ts";

/** A comment the fake callback observed, recorded for test assertions. */
export interface LinearCommentCall {
  readonly issueId: string;
  readonly body: string;
}

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

/**
 * The @linear/sdk wrapper (FUR-18) — the only place in the codebase that
 * imports the SDK. Absent MAESTRO_LINEAR_API_TOKEN never fails boot; every
 * post fails individually until the operator configures it.
 */
export class LinearCallback extends Context.Service<
  LinearCallback,
  {
    /** Creates a comment on a Linear issue (by issue UUID). */
    readonly postComment: (args: {
      readonly issueId: string;
      readonly body: string;
    }) => Effect.Effect<void, CallbackError>;
  }
>()("maestro/callback/LinearCallback") {
  static readonly layer = Layer.effect(
    LinearCallback,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const client = Option.map(
        config.linearApiToken,
        (token) => new LinearClient({ apiKey: Redacted.value(token) }),
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
      };
    }),
  );

  /**
   * In-memory fake per the .layerTest convention — never talks to Linear.
   * Pass `calls` to observe attempted posts (recorded whether or not they
   * fail, like real API traffic); `failWith` holds an injectable failure that
   * can be flipped on and off mid-test (retry paths).
   */
  static readonly layerTest = (options: {
    readonly calls?: Array<LinearCommentCall>;
    readonly failWith?: { current: CallbackError | undefined };
  }) =>
    Layer.succeed(LinearCallback)({
      postComment: Effect.fn("LinearCallback.postComment")(function* (args) {
        options.calls?.push({ issueId: args.issueId, body: args.body });
        const failure = options.failWith?.current;
        if (failure !== undefined) {
          return yield* Effect.fail(failure);
        }
      }),
    });
}
