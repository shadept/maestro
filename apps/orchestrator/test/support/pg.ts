import path from "node:path";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import pg from "pg";
import type { DrizzleClient } from "../../src/db/Db.ts";
import * as schema from "../../src/db/schema/index.ts";

export interface TestDb {
  readonly connectionString: string;
  readonly db: DrizzleClient;
  readonly stop: () => Promise<void>;
}

const POSTGRES_IMAGE = "postgres:17-alpine";

/**
 * Shared integration-test harness: boots a Postgres container, applies all
 * migrations, and yields a Drizzle client. Every DB-touching suite uses this —
 * real Postgres, never mocks.
 */
export const startTestDb = async (): Promise<TestDb> => {
  const container: StartedPostgreSqlContainer = await new PostgreSqlContainer(
    POSTGRES_IMAGE,
  ).start();
  const connectionString = container.getConnectionUri();
  const pool = new pg.Pool({ connectionString });
  const db = drizzle(pool, { schema });
  await migrate(db, {
    migrationsFolder: path.resolve(import.meta.dirname, "../../drizzle"),
  });
  return {
    connectionString,
    db,
    stop: async () => {
      await pool.end();
      await container.stop();
    },
  };
};
