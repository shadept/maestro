// Minimal M1 project registration (FUR-20). The registration UI is an M2
// feature, so this script upserts a Project row directly — run it with tsx:
//
//   pnpm --filter @maestro/orchestrator register-project \
//     --repo-git-url https://github.com/shadept/maestro \
//     --linear-team-key FUR \
//     [--base-branch main] [--branch-pattern "maestro/{ticketKey}"]
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
import type { GitConventionOverrides } from "@maestro/domain";
import { drizzle } from "drizzle-orm/node-postgres";
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
  },
});

const repoGitUrl = values["repo-git-url"];
const linearTeamKey = values["linear-team-key"];
if (!repoGitUrl || !linearTeamKey) {
  console.error(
    "usage: register-project --repo-git-url <url> --linear-team-key <KEY> [--base-branch <branch>] [--branch-pattern <pattern>]",
  );
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

const pool = new pg.Pool({ connectionString: databaseUrl });
try {
  const client = drizzle(pool);
  const rows = await client
    .insert(projects)
    .values({ repoGitUrl, linearTeamKey, gitConventions })
    .onConflictDoUpdate({
      target: projects.linearTeamKey,
      set: { repoGitUrl, gitConventions },
    })
    .returning();
  console.log("project registered:", JSON.stringify(rows[0], null, 2));
} finally {
  await pool.end();
}
