import { DbQueryError } from "@maestro/domain";
import { Effect } from "effect";

/**
 * Runs a Drizzle promise and maps any driver failure to a tagged
 * `DbQueryError` — no raw pg errors escape a repository.
 */
export const dbTry =
  (operation: string) =>
  <A>(f: () => Promise<A>): Effect.Effect<A, DbQueryError> =>
    Effect.tryPromise({
      try: () => f(),
      catch: (error) =>
        new DbQueryError({
          operation,
          message: error instanceof Error ? error.message : String(error),
        }),
    });
