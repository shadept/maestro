import { fileURLToPath } from "node:url";

/** Repo-root .env, resolved relative to this file so it holds regardless of cwd. */
const repoRootDotEnv = fileURLToPath(new URL("../../../../.env", import.meta.url));

/**
 * Dev-convenience .env seeding, called once at boot before any Effect layer
 * builds. Node's native `process.loadEnvFile` never overwrites variables that
 * are already set — neither real environment variables nor values from an
 * earlier file in the list — so precedence is:
 *
 *   real environment  >  ./.env (cwd)  >  <repo root>/.env
 *
 * `.env` files are optional: a missing or unreadable file is skipped and must
 * never fail boot. This function only seeds `process.env`; AppConfig remains
 * the single place where variables are read and validated (`.env.example` at
 * the repo root documents every variable it understands).
 */
export const loadDotEnv = (paths: ReadonlyArray<string> = [".env", repoRootDotEnv]): void => {
  for (const path of paths) {
    try {
      process.loadEnvFile(path);
    } catch {
      // .env is optional — absence (or unreadability) is not an error.
    }
  }
};
