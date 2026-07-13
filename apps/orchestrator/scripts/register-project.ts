// Minimal M1 project registration (FUR-20). The registration UI is an M2
// feature, so this script upserts a Project row directly — run it with tsx:
//
//   pnpm --filter @maestro/orchestrator register-project \
//     --repo-git-url https://github.com/shadept/maestro \
//     --linear-team-key FUR \
//     [--base-branch main] [--branch-pattern "maestro/{ticketKey}"] \
//     [--agent-model claude-sonnet-4-5] [--agent-effort low]
//
// (No `--` separator — pnpm would forward it literally and parseArgs treats
// everything after it as positionals.)
//
// DATABASE_URL comes from the environment (repo-root .env is seeded like the
// orchestrator does). Idempotent: `linear_team_key` is unique, so re-running
// updates the existing row instead of inserting a duplicate. NOTE: the M1
// projects schema has no display-name column — the Linear team key IS the
// human handle; git conventions live in the `git_conventions` jsonb.
import { parseArgs } from "node:util";
import { AgentEffort, type AgentOverrides, type GitConventionOverrides } from "@maestro/domain";
import { drizzle } from "drizzle-orm/node-postgres";
import { Schema } from "effect";
import pg from "pg";
import { loadDotEnv } from "../src/config/loadDotEnv.ts";
import { projects } from "../src/db/schema/index.ts";

loadDotEnv();

const { values } = parseArgs({
  options: {
    "repo-git-url": { type: "string" },
    "linear-team-key": { type: "string" },
    "base-branch": { type: "string" },
    "branch-pattern": { type: "string" },
    "agent-model": { type: "string" },
    "agent-effort": { type: "string" },
  },
});

const repoGitUrl = values["repo-git-url"];
const linearTeamKey = values["linear-team-key"];
if (!repoGitUrl || !linearTeamKey) {
  console.error(
    "usage: register-project --repo-git-url <url> --linear-team-key <KEY> [--base-branch <branch>] [--branch-pattern <pattern>] [--agent-model <id>] [--agent-effort <low|medium|high|xhigh|max>]",
  );
  process.exit(1);
}
const agentEffort = values["agent-effort"];
if (agentEffort !== undefined && !Schema.is(AgentEffort)(agentEffort)) {
  console.error(`invalid --agent-effort "${agentEffort}" (low|medium|high|xhigh|max)`);
  process.exit(1);
}
const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) {
  console.error("DATABASE_URL is not set (environment or .env)");
  process.exit(1);
}

const gitConventions: GitConventionOverrides = {
  ...(values["base-branch"] !== undefined && { baseBranch: values["base-branch"] }),
  ...(values["branch-pattern"] !== undefined && { branchPattern: values["branch-pattern"] }),
};
const agent: AgentOverrides = {
  ...(values["agent-model"] !== undefined && { model: values["agent-model"] }),
  ...(agentEffort !== undefined && { effort: agentEffort }),
};

const pool = new pg.Pool({ connectionString: databaseUrl });
try {
  const client = drizzle(pool);
  const rows = await client
    .insert(projects)
    .values({ repoGitUrl, linearTeamKey, gitConventions, agent })
    .onConflictDoUpdate({
      target: projects.linearTeamKey,
      set: { repoGitUrl, gitConventions, agent },
    })
    .returning();
  console.log("project registered:", JSON.stringify(rows[0], null, 2));
} finally {
  await pool.end();
}
