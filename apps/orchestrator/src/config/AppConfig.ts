import { Config, Context, Layer, Option, Redacted } from "effect";

// Every env var the orchestrator understands, validated at boot. Values are
// resolved once into this service; nothing else reads process.env.

const config = {
  databaseUrl: Config.nonEmptyString("DATABASE_URL"),
  /** Root directory for git caches, worktrees, and session config dirs. */
  storageRoot: Config.nonEmptyString("MAESTRO_STORAGE_ROOT"),
  /** Container runtime CLI template (nerdctl/podman/docker). Semantics owned by WorkerRuntime. */
  runtimeTemplate: Config.nonEmptyString("MAESTRO_RUNTIME_TEMPLATE").pipe(
    Config.withDefault("docker run"),
  ),
  maxConcurrentWorkers: Config.int("MAESTRO_MAX_CONCURRENT_WORKERS").pipe(Config.withDefault(2)),
  cooldownMinutes: Config.int("MAESTRO_COOLDOWN_MINUTES").pipe(Config.withDefault(60)),
  retentionDays: Config.int("MAESTRO_RETENTION_DAYS").pipe(Config.withDefault(14)),
  adminToken: Config.redacted("MAESTRO_ADMIN_TOKEN"),
  /** Subscription session token (preferred) — see Tech Requirements §9. */
  agentOauthToken: Config.option(Config.redacted("CLAUDE_CODE_OAUTH_TOKEN")),
  /** API key fallback for agent auth. */
  agentApiKey: Config.option(Config.redacted("ANTHROPIC_API_KEY")),
  logFormat: Config.literals(["json", "pretty"], "MAESTRO_LOG_FORMAT").pipe(
    Config.withDefault("json" as const),
  ),
  port: Config.port("MAESTRO_PORT").pipe(Config.withDefault(3000)),
};

export class AppConfig extends Context.Service<
  AppConfig,
  {
    readonly databaseUrl: string;
    readonly storageRoot: string;
    readonly runtimeTemplate: string;
    readonly maxConcurrentWorkers: number;
    readonly cooldownMinutes: number;
    readonly retentionDays: number;
    readonly adminToken: Redacted.Redacted;
    readonly agentOauthToken: Option.Option<Redacted.Redacted>;
    readonly agentApiKey: Option.Option<Redacted.Redacted>;
    readonly logFormat: "json" | "pretty";
    readonly port: number;
  }
>()("maestro/config/AppConfig") {
  static readonly layer = Layer.effect(AppConfig, Config.all(config));

  /** Test layer: pure in-memory config — never touches the environment. */
  static readonly layerTest = (overrides: Partial<AppConfig["Service"]> = {}) =>
    Layer.succeed(AppConfig)({
      databaseUrl: "postgresql://localhost:5432/maestro-test",
      storageRoot: "/tmp/maestro-test",
      runtimeTemplate: "docker run",
      maxConcurrentWorkers: 2,
      cooldownMinutes: 60,
      retentionDays: 14,
      adminToken: Redacted.make("test-admin-token"),
      agentOauthToken: Option.none(),
      agentApiKey: Option.none(),
      logFormat: "pretty",
      port: 0,
      ...overrides,
    });
}
