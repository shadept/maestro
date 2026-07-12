import type { GitConventionOverrides, ResourceTiers } from "@maestro/domain";
import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";

export const projects = pgTable("projects", {
  id: uuidV7PrimaryKey(),
  repoGitUrl: text("repo_git_url").notNull(),
  localCachePath: text("local_cache_path"),
  gitConventions: jsonb("git_conventions").$type<GitConventionOverrides>().notNull().default({}),
  resources: jsonb("resources").$type<ResourceTiers>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
