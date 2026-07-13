import type { AgentOverrides, GitConventionOverrides, ResourceTiers } from "@maestro/domain";
import { jsonb, pgTable, text, timestamp } from "drizzle-orm/pg-core";
import { uuidV7PrimaryKey } from "./columns.ts";

export const projects = pgTable("projects", {
  id: uuidV7PrimaryKey(),
  repoGitUrl: text("repo_git_url").notNull(),
  // Linear team key routing webhooks to this project (FUR-18). Unique so a
  // team's issues map to exactly one project; null = not Linear-connected.
  linearTeamKey: text("linear_team_key").unique(),
  localCachePath: text("local_cache_path"),
  gitConventions: jsonb("git_conventions").$type<GitConventionOverrides>().notNull().default({}),
  resources: jsonb("resources").$type<ResourceTiers>().notNull().default({}),
  // Project-level agent model/effort overrides (FUR-41).
  agent: jsonb("agent").$type<AgentOverrides>().notNull().default({}),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
});
