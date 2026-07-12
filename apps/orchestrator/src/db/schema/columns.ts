import { uuid } from "drizzle-orm/pg-core";
import { v7 as uuidv7 } from "uuid";

/**
 * Mints a UUIDv7. All database ids are v7 (time-ordered) so b-tree primary-key
 * indexes append instead of fragmenting. Generation happens application-side —
 * Postgres < 18 has no native uuidv7(), and app-side ids exist before insert.
 * The `uuid` package guarantees within-process monotonicity for v7.
 */
export const mintId = (): string => uuidv7();

/** Standard UUIDv7 primary-key column shared by every table. */
export const uuidV7PrimaryKey = () => uuid("id").primaryKey().$defaultFn(mintId);
