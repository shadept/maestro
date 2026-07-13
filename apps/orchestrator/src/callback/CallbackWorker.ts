import { CallbackDeliveryError, type DbError } from "@maestro/domain";
import { Context, Effect, Layer, Metric, Schema } from "effect";
import { type OutboxEntry, OutboxRepo } from "../db/OutboxRepo.ts";
import { TaskRunRepo } from "../db/TaskRunRepo.ts";
import { TurnOutcomePayload } from "../engine/TurnSettlement.ts";
import * as Metrics from "../observability/metrics.ts";
import { formatTurnComment } from "./format.ts";
import { LinearCallback, linearIssueIdFrom } from "./LinearCallback.ts";

const BATCH_SIZE = 20;

/** Exponential backoff for failed posts: 1s, 2s, 4s, ... capped at 5 minutes. */
const BASE_BACKOFF_MILLIS = 1_000;
const MAX_BACKOFF_MILLIS = 5 * 60_000;
const backoffMillis = (priorAttempts: number): number =>
  Math.min(BASE_BACKOFF_MILLIS * 2 ** priorAttempts, MAX_BACKOFF_MILLIS);

/**
 * Terminal give-up: a row that has failed this many delivery attempts settles
 * FAILED and is never retried (>1h of capped backoff — a platform outage that
 * long means the result is stale; the row keeps lastError + attempts for
 * inspection and manual replay). Exported for the worker tests.
 */
export const MAX_DELIVERY_ATTEMPTS = 15;

const decodeOutcome = Schema.decodeUnknownEffect(TurnOutcomePayload);

/**
 * Drains the callback outbox (Tech Requirements §16): turn results committed
 * by the executor are posted back to the ticketing platform as comments.
 *
 * Delivery semantics — honestly stated: at-least-once posting with dedup at
 * the outbox-row level. A row is marked SENT only after the platform accepted
 * the post; failures record an error + a persisted next-attempt time
 * (exponential backoff that survives restarts) until MAX_DELIVERY_ATTEMPTS,
 * after which the row settles FAILED terminally. A crash in the instant
 * between a successful post and markSent re-posts once on recovery — the
 * idempotency key prevents double-enqueue, not platform-side double-posting.
 */
export class CallbackWorker extends Context.Service<
  CallbackWorker,
  {
    /**
     * One drain pass over due PENDING rows, oldest first. Row-level failures
     * never fail the pass (they are recorded on the row for retry — or as
     * terminal FAILED past the give-up threshold); only DB trouble does.
     * Returns the number of rows processed.
     */
    readonly drainOnce: () => Effect.Effect<number, DbError>;
  }
>()("maestro/callback/CallbackWorker") {
  static readonly layer = Layer.effect(
    CallbackWorker,
    Effect.gen(function* () {
      const outboxRepo = yield* OutboxRepo;
      const taskRunRepo = yield* TaskRunRepo;
      const linear = yield* LinearCallback;

      const deliveryError = (entry: OutboxEntry) => (reason: string) =>
        new CallbackDeliveryError({ target: entry.target, reason });

      /** Delivers one row; any problem becomes a CallbackDeliveryError for the retry record. */
      const deliver = (entry: OutboxEntry): Effect.Effect<void, CallbackDeliveryError> =>
        Effect.gen(function* () {
          const fail = deliveryError(entry);
          if (entry.target !== "linear") {
            // M2's generic API adds its own sender; until then such rows
            // retry (capped backoff, terminal FAILED after the give-up
            // threshold) rather than being silently dropped.
            return yield* fail(`no callback adapter for target "${entry.target}"`);
          }
          const outcome = yield* decodeOutcome(entry.payload).pipe(
            Effect.mapError((error) =>
              fail(`outbox payload is not a TurnOutcomePayload: ${error}`),
            ),
          );
          // The turn's TaskContext preserves the raw platform payload exactly
          // for this: it names the Linear issue UUID the comment goes to.
          const context = yield* taskRunRepo
            .getContext(outcome.taskRunId)
            .pipe(Effect.mapError((error) => fail(`loading turn context failed: ${error}`)));
          const issueId = linearIssueIdFrom(context.payload);
          if (issueId === null) {
            return yield* fail("turn context has no Linear issue id");
          }
          yield* linear.postComment({ issueId, body: formatTurnComment(outcome) });
        });

      return {
        drainOnce: Effect.fn("CallbackWorker.drainOnce")(function* () {
          const due = yield* outboxRepo.listPending(BATCH_SIZE);
          // Callback outbox lag (M2.10): age of the oldest PENDING row (due is
          // oldest-first); 0 when nothing is waiting.
          const oldest = due[0];
          yield* Metric.update(
            Metrics.callbackOutboxLag,
            oldest ? Date.now() - oldest.createdAt.getTime() : 0,
          );
          for (const entry of due) {
            yield* deliver(entry).pipe(
              Effect.matchEffect({
                // markSent strictly after the platform accepted the post.
                onSuccess: () => outboxRepo.markSent(entry.id),
                onFailure: (error) => {
                  const attempts = entry.attempts + 1;
                  const terminal = attempts >= MAX_DELIVERY_ATTEMPTS;
                  const log = terminal
                    ? Effect.logError("callback delivery abandoned; giving up permanently", {
                        outboxId: entry.id,
                        attempts,
                        reason: error.reason,
                      })
                    : Effect.logWarning("callback delivery failed; will retry", {
                        outboxId: entry.id,
                        attempts,
                        reason: error.reason,
                      });
                  return log.pipe(
                    Effect.andThen(
                      outboxRepo.recordFailure(
                        entry.id,
                        error.reason,
                        terminal
                          ? { terminal: true }
                          : { nextAttemptAt: new Date(Date.now() + backoffMillis(entry.attempts)) },
                      ),
                    ),
                  );
                },
              }),
            );
          }
          return due.length;
        }),
      };
    }),
  );
}
