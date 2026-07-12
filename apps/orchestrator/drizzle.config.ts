import { defineConfig } from "drizzle-kit";
import { loadDotEnv } from "./src/config/loadDotEnv.ts";

// `drizzle-kit migrate` needs a connection; reuse the app's .env seeding so
// the CLI resolves DATABASE_URL exactly like the orchestrator does (real
// environment always wins). `drizzle-kit generate` ignores dbCredentials.
loadDotEnv();

export default defineConfig({
  dialect: "postgresql",
  schema: "./src/db/schema/index.ts",
  out: "./drizzle",
  dbCredentials: { url: process.env.DATABASE_URL ?? "" },
});
