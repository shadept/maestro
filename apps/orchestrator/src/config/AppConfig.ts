import { fileURLToPath } from "node:url";
import { Config, Context, Layer, Option, Redacted } from "effect";

// Every env var the orchestrator understands, validated at boot. Values are
// resolved once into this service; nothing else reads process.env.

/**
 * Default admin UI bundle location: the admin-ui workspace's Vite output,
 * resolved relative to this file so it holds regardless of cwd. Deployments
 * with a different layout set MAESTRO_ADMIN_UI_DIST.
 */
const defaultAdminUiDist = fileURLToPath(new URL("../../../admin-ui/dist", import.meta.url));

const config = {
  databaseUrl: Config.nonEmptyString("DATABASE_URL"),
  /** Root directory for git caches, worktrees, and session config dirs. */
  storageRoot: Config.nonEmptyString("MAESTRO_STORAGE_ROOT"),
  /** Container runtime CLI template (nerdctl/podman/docker). Semantics owned by WorkerRuntime. */
  runtimeTemplate: Config.nonEmptyString("MAESTRO_RUNTIME_TEMPLATE").pipe(
    Config.withDefault("docker run"),
  ),
  /** Which WorkerRuntime layer the composition root selects. */
  runtimeMode: Config.literals(["local-cli", "k8s"], "MAESTRO_RUNTIME_MODE").pipe(
    Config.withDefault("local-cli" as const),
  ),
  maxConcurrentWorkers: Config.int("MAESTRO_MAX_CONCURRENT_WORKERS").pipe(Config.withDefault(2)),
  /** Worker image every turn runs in (the official base image lands in M2.14). */
  workerImage: Config.nonEmptyString("MAESTRO_WORKER_IMAGE").pipe(
    Config.withDefault("maestro/worker-base:latest"),
  ),
  /** Hard per-turn execution deadline, enforced by WorkerRuntime. */
  turnTimeoutSeconds: Config.int("MAESTRO_TURN_TIMEOUT_SECONDS").pipe(Config.withDefault(1800)),
  cooldownMinutes: Config.int("MAESTRO_COOLDOWN_MINUTES").pipe(Config.withDefault(60)),
  retentionDays: Config.int("MAESTRO_RETENTION_DAYS").pipe(Config.withDefault(14)),
  adminToken: Config.redacted("MAESTRO_ADMIN_TOKEN"),
  /**
   * Orchestrator forge credential (GitHub PAT / app token): used both to push
   * session branches and for forge API calls. Optional at boot — outbound
   * publishing fails per-invocation without it, nothing else does.
   */
  githubToken: Config.option(Config.redacted("MAESTRO_GITHUB_TOKEN")),
  /** Commit identity defaults (PRD: distinct Maestro author). Config value only in M1. */
  gitAuthorName: Config.nonEmptyString("MAESTRO_GIT_AUTHOR_NAME").pipe(
    Config.withDefault("Maestro"),
  ),
  gitAuthorEmail: Config.nonEmptyString("MAESTRO_GIT_AUTHOR_EMAIL").pipe(
    Config.withDefault("maestro@localhost"),
  ),
  /** Subscription session token (preferred) — see Tech Requirements §9. */
  agentOauthToken: Config.option(Config.redacted("CLAUDE_CODE_OAUTH_TOKEN")),
  /** API key fallback for agent auth. */
  agentApiKey: Config.option(Config.redacted("ANTHROPIC_API_KEY")),
  logFormat: Config.literals(["json", "pretty"], "MAESTRO_LOG_FORMAT").pipe(
    Config.withDefault("json" as const),
  ),
  port: Config.port("MAESTRO_PORT").pipe(Config.withDefault(3000)),
  /** Directory the admin UI static bundle is served from. Missing dir = 404s, never a boot failure. */
  adminUiDist: Config.nonEmptyString("MAESTRO_ADMIN_UI_DIST").pipe(
    Config.withDefault(defaultAdminUiDist),
  ),
};

export class AppConfig extends Context.Service<
  AppConfig,
  {
    readonly databaseUrl: string;
    readonly storageRoot: string;
    readonly runtimeTemplate: string;
    readonly runtimeMode: "local-cli" | "k8s";
    readonly maxConcurrentWorkers: number;
    readonly workerImage: string;
    readonly turnTimeoutSeconds: number;
    readonly cooldownMinutes: number;
    readonly retentionDays: number;
    readonly adminToken: Redacted.Redacted;
    readonly githubToken: Option.Option<Redacted.Redacted>;
    readonly gitAuthorName: string;
    readonly gitAuthorEmail: string;
    readonly agentOauthToken: Option.Option<Redacted.Redacted>;
    readonly agentApiKey: Option.Option<Redacted.Redacted>;
    readonly logFormat: "json" | "pretty";
    readonly port: number;
    readonly adminUiDist: string;
  }
>()("maestro/config/AppConfig") {
  static readonly layer = Layer.effect(AppConfig, Config.all(config));

  /** Test layer: pure in-memory config — never touches the environment. */
  static readonly layerTest = (overrides: Partial<AppConfig["Service"]> = {}) =>
    Layer.succeed(AppConfig)({
      databaseUrl: "postgresql://localhost:5432/maestro-test",
      storageRoot: "/tmp/maestro-test",
      runtimeTemplate: "docker run",
      runtimeMode: "local-cli",
      maxConcurrentWorkers: 2,
      workerImage: "maestro/worker-base:latest",
      turnTimeoutSeconds: 1800,
      cooldownMinutes: 60,
      retentionDays: 14,
      adminToken: Redacted.make("test-admin-token"),
      githubToken: Option.none(),
      gitAuthorName: "Maestro",
      gitAuthorEmail: "maestro@localhost",
      agentOauthToken: Option.none(),
      agentApiKey: Option.none(),
      logFormat: "pretty",
      port: 0,
      adminUiDist: "/tmp/maestro-test/admin-ui-dist",
      ...overrides,
    });
}
