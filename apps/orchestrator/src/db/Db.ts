import { sql } from "drizzle-orm";
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import { Context, Effect, Layer } from "effect";
import pg from "pg";
import { AppConfig } from "../config/AppConfig.ts";
import * as schema from "./schema/index.ts";

export type DrizzleClient = NodePgDatabase<typeof schema>;

const make = (connectionString: string) =>
  Effect.gen(function* () {
    const pool = yield* Effect.acquireRelease(
      Effect.sync(() => new pg.Pool({ connectionString })),
      (p) => Effect.promise(() => p.end()),
    );
    const client = drizzle(pool, { schema });
    return {
      client,
      ping: Effect.tryPromise(() => client.execute(sql`select 1`)).pipe(
        Effect.as(true),
        Effect.catch(() => Effect.succeed(false)),
      ),
    };
  });

export class Db extends Context.Service<
  Db,
  {
    readonly client: DrizzleClient;
    /** True when the database answers a trivial query; never fails. */
    readonly ping: Effect.Effect<boolean>;
  }
>()("maestro/db/Db") {
  static readonly layer = Layer.effect(
    Db,
    Effect.gen(function* () {
      const { databaseUrl } = yield* AppConfig;
      return yield* make(databaseUrl);
    }),
  );

  /** Test layer: connect to an externally managed database (testcontainer). */
  static readonly layerTest = (connectionString: string) =>
    Layer.effect(Db, make(connectionString));
}
