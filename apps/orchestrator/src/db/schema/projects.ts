import type { GitConventionOverrides, ResourceTiers } from "@maestro/domain";
import { jsonb, pgTable, text, timestamp, uuid } from "drizzle-orm/pg-core";

export const projects = pgTable("projects", {
  id: uuid("id").primaryKey().defaultRandom(),
  repoGitUrl: text("repo_git_url").notNull(),
  localCachePath: text("local_cache_path"),
  gitConventions: jsonb("git_conventions").$type<GitConventionOverrides>().notNull().default({}),
  resources: jsonb("resources").$type<ResourceTiers>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
