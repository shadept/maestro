import { createHmac, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";

// Recorded-fixture helpers for the Linear webhook suite. Fixtures are
// checked-in JSON shaped after Linear's real webhook format (@linear/sdk
// payload types); `webhookTimestamp` is a placeholder re-stamped per test
// because the replay window is relative to "now", and the HMAC is computed
// over the final body with the suite's fixture secret — no live Linear
// anything.

export const LINEAR_TEST_SECRET = "test-linear-webhook-secret";

const fixturesDir = path.resolve(import.meta.dirname, "../fixtures/linear");

export const loadLinearFixture = (name: string): Record<string, unknown> =>
  JSON.parse(readFileSync(path.join(fixturesDir, `${name}.json`), "utf8"));

export interface SignedDelivery {
  readonly body: string;
  readonly headers: Record<string, string>;
  readonly deliveryId: string;
}

export const signLinearDelivery = (
  payload: Record<string, unknown>,
  options: {
    readonly deliveryId?: string;
    readonly secret?: string;
    readonly webhookTimestamp?: number;
    /** Applied AFTER signing — produces a tampered body with a stale-but-valid signature. */
    readonly tamper?: (signedBody: string) => string;
  } = {},
): SignedDelivery => {
  const deliveryId = options.deliveryId ?? randomUUID();
  const stamped = { ...payload, webhookTimestamp: options.webhookTimestamp ?? Date.now() };
  const signedBody = JSON.stringify(stamped);
  const signature = createHmac("sha256", options.secret ?? LINEAR_TEST_SECRET)
    .update(signedBody)
    .digest("hex");
  const body = options.tamper === undefined ? signedBody : options.tamper(signedBody);
  return {
    body,
    deliveryId,
    headers: {
      "content-type": "application/json",
      "linear-signature": signature,
      "linear-delivery": deliveryId,
    },
  };
};
