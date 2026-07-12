import { createHash, timingSafeEqual } from "node:crypto";
import { Redacted } from "effect";

/**
 * Constant-time admin-token comparison. Hashing both sides first equalizes
 * length (timingSafeEqual requires equal-length buffers and a length check
 * would itself leak), then the comparison runs in constant time.
 */
export const tokenMatches = (provided: string, expected: Redacted.Redacted): boolean => {
  const a = createHash("sha256").update(provided).digest();
  const b = createHash("sha256").update(Redacted.value(expected)).digest();
  return timingSafeEqual(a, b);
};

/** Extracts the token from an `Authorization: Bearer <token>` header, if present. */
export const bearerToken = (authorization: string | undefined): string | undefined =>
  authorization?.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : undefined;
