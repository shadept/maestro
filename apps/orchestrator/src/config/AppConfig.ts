import { fileURLToPath } from "node:url";
import type { AgentEffort } from "@maestro/domain";
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
  /**
   * Agent tier of the two-tier resource model (Tech Requirements §8, M2.5):
   * the baseline every worker gets regardless of Project, before any
   * per-Project override (Project.resources) composes on top. ~1Gi default.
   */
  agentTierMemoryMib: Config.int("MAESTRO_AGENT_TIER_MEMORY_MIB").pipe(Config.withDefault(1024)),
  /** Agent tier CPU baseline, millicores — a request only, never hard-capped. */
  agentTierCpuMillicores: Config.int("MAESTRO_AGENT_TIER_CPU_MILLICORES").pipe(
    Config.withDefault(1000),
  ),
  /** Worker image every turn runs in — the official base image (images/base, FUR-34). */
  workerImage: Config.nonEmptyString("MAESTRO_WORKER_IMAGE").pipe(
    Config.withDefault("ghcr.io/shadept/maestro-worker-base:latest"),
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
  /**
   * Commit identity for orchestrator-made commits (PRD: distinct Maestro
   * author) — consumed by OutboundGit's diverged-branch reconciliation merge.
   * Agent-made commits still carry claude-code's identity (M2 wiring).
   */
  gitAuthorName: Config.nonEmptyString("MAESTRO_GIT_AUTHOR_NAME").pipe(
    Config.withDefault("Maestro"),
  ),
  gitAuthorEmail: Config.nonEmptyString("MAESTRO_GIT_AUTHOR_EMAIL").pipe(
    Config.withDefault("maestro@localhost"),
  ),
  /**
   * Linear webhook signing secret (FUR-18). Optional at boot — without it the
   * webhook endpoint rejects every delivery (verification cannot run), nothing
   * else is affected.
   */
  linearWebhookSecret: Config.option(Config.redacted("MAESTRO_LINEAR_WEBHOOK_SECRET")),
  /** Linear API token for outbound callbacks. Optional at boot — absent token fails per-post. */
  linearApiToken: Config.option(Config.redacted("MAESTRO_LINEAR_API_TOKEN")),
  /**
   * How MAESTRO_LINEAR_API_TOKEN authenticates (FUR-42): "api-key" = personal
   * key (raw Authorization header), "oauth" = app-actor access token (Bearer).
   * Optional — absent means auto-detect by the `lin_api_` prefix; set it only
   * for legacy unprefixed personal keys the heuristic would misread as OAuth.
   */
  linearTokenKind: Config.option(
    Config.literals(["api-key", "oauth"], "MAESTRO_LINEAR_TOKEN_KIND"),
  ),
  /**
   * The @handle that summons Maestro in ticket comments (FUR-37). Must match
   * the Maestro OAuth app's name/handle in Linear — Linear renders app
   * mentions as plain `@<handle>` markdown in webhook comment bodies, so the
   * handle string is the only mention evidence deliveries carry.
   */
  linearMentionHandle: Config.nonEmptyString("MAESTRO_LINEAR_MENTION_HANDLE").pipe(
    Config.withDefault("maestro"),
  ),
  /**
   * Linear user id of the Maestro app user (the identity behind
   * MAESTRO_LINEAR_API_TOKEN). Load-bearing since FUR-37: delegating an issue
   * to this user is THE trigger, so without it delegation events are Ignored
   * (loudly logged) — and it still guards against self-triggering on
   * Maestro's own comments. Optional at boot by policy; unset = no triggering.
   */
  linearBotUserId: Config.option(Config.nonEmptyString("MAESTRO_LINEAR_BOT_USER_ID")),
  /** Subscription session token (preferred) — see Tech Requirements §9. */
  agentOauthToken: Config.option(Config.redacted("CLAUDE_CODE_OAUTH_TOKEN")),
  /** API key fallback for agent auth. */
  agentApiKey: Config.option(Config.redacted("ANTHROPIC_API_KEY")),
  /**
   * Deployment-default agent model/effort (FUR-41), the least specific of the
   * three override levels (task > project > deployment). Absent = the claude
   * CLI's own default, exactly the pre-FUR-41 behavior.
   */
  agentModel: Config.option(Config.nonEmptyString("MAESTRO_AGENT_MODEL")),
  agentEffort: Config.option(
    Config.literals(["low", "medium", "high", "xhigh", "max"], "MAESTRO_AGENT_EFFORT"),
  ),
  logFormat: Config.literals(["json", "pretty"], "MAESTRO_LOG_FORMAT").pipe(
    Config.withDefault("json" as const),
  ),
  port: Config.port("MAESTRO_PORT").pipe(Config.withDefault(3000)),
  /** Directory the admin UI static bundle is served from. Missing dir = 404s, never a boot failure. */
  adminUiDist: Config.nonEmptyString("MAESTRO_ADMIN_UI_DIST").pipe(
    Config.withDefault(defaultAdminUiDist),
  ),
  /**
   * OTLP collector base URL (M2.10, Tech Requirements §15): traces post to
   * `<endpoint>/v1/traces`, metrics to `<endpoint>/v1/metrics`. Optional — the
   * exporter layers are Layer.empty when unset, so an unconfigured deployment
   * pays zero network cost. Effect's native tracer still assigns real
   * span/trace ids either way; TurnExecutor persists them on the TaskRun
   * regardless of whether anything is exported.
   */
  otlpEndpoint: Config.option(Config.nonEmptyString("MAESTRO_OTLP_ENDPOINT")),
  /** OTLP resource service.name attribute. Optional, default: maestro-orchestrator. */
  otlpServiceName: Config.nonEmptyString("MAESTRO_OTLP_SERVICE_NAME").pipe(
    Config.withDefault("maestro-orchestrator"),
  ),
  /** Prometheus text-format scrape endpoint at GET /metrics. Optional, default: enabled. */
  prometheusMetricsEnabled: Config.boolean("MAESTRO_PROMETHEUS_METRICS_ENABLED").pipe(
    Config.withDefault(true),
  ),
  /**
   * Trace-viewer URL template for the admin UI TaskRun detail link (e.g. a
   * Grafana Explore URL). The admin UI substitutes the literal `{traceId}`
   * placeholder client-side. Optional — absent means no link is rendered.
   */
  traceViewerUrlTemplate: Config.option(Config.nonEmptyString("MAESTRO_TRACE_VIEWER_URL_TEMPLATE")),
};

export class AppConfig extends Context.Service<
  AppConfig,
  {
    readonly databaseUrl: string;
    readonly storageRoot: string;
    readonly runtimeTemplate: string;
    readonly runtimeMode: "local-cli" | "k8s";
    readonly maxConcurrentWorkers: number;
    readonly agentTierMemoryMib: number;
    readonly agentTierCpuMillicores: number;
    readonly workerImage: string;
    readonly turnTimeoutSeconds: number;
    readonly cooldownMinutes: number;
    readonly retentionDays: number;
    readonly adminToken: Redacted.Redacted;
    readonly githubToken: Option.Option<Redacted.Redacted>;
    readonly gitAuthorName: string;
    readonly gitAuthorEmail: string;
    readonly linearWebhookSecret: Option.Option<Redacted.Redacted>;
    readonly linearApiToken: Option.Option<Redacted.Redacted>;
    readonly linearTokenKind: Option.Option<"api-key" | "oauth">;
    readonly linearMentionHandle: string;
    readonly linearBotUserId: Option.Option<string>;
    readonly agentOauthToken: Option.Option<Redacted.Redacted>;
    readonly agentApiKey: Option.Option<Redacted.Redacted>;
    readonly agentModel: Option.Option<string>;
    readonly agentEffort: Option.Option<AgentEffort>;
    readonly logFormat: "json" | "pretty";
    readonly port: number;
    readonly adminUiDist: string;
    readonly otlpEndpoint: Option.Option<string>;
    readonly otlpServiceName: string;
    readonly prometheusMetricsEnabled: boolean;
    readonly traceViewerUrlTemplate: Option.Option<string>;
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
      agentTierMemoryMib: 1024,
      agentTierCpuMillicores: 1000,
      workerImage: "ghcr.io/shadept/maestro-worker-base:latest",
      turnTimeoutSeconds: 1800,
      cooldownMinutes: 60,
      retentionDays: 14,
      adminToken: Redacted.make("test-admin-token"),
      githubToken: Option.none(),
      gitAuthorName: "Maestro",
      gitAuthorEmail: "maestro@localhost",
      linearWebhookSecret: Option.none(),
      linearApiToken: Option.none(),
      linearTokenKind: Option.none(),
      linearMentionHandle: "maestro",
      linearBotUserId: Option.none(),
      agentOauthToken: Option.none(),
      agentApiKey: Option.none(),
      agentModel: Option.none(),
      agentEffort: Option.none(),
      logFormat: "pretty",
      port: 0,
      adminUiDist: "/tmp/maestro-test/admin-ui-dist",
      otlpEndpoint: Option.none(),
      otlpServiceName: "maestro-orchestrator",
      prometheusMetricsEnabled: true,
      traceViewerUrlTemplate: Option.none(),
      ...overrides,
    });
}
