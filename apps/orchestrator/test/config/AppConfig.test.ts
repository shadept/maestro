import { ConfigProvider, Effect, Layer, Option, Redacted } from "effect";
import { describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";

const load = (env: Record<string, string>) =>
  Effect.runPromise(
    Effect.gen(function* () {
      return yield* AppConfig;
    }).pipe(
      Effect.provide(
        AppConfig.layer.pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(env)))),
      ),
    ),
  );

const validEnv = {
  DATABASE_URL: "postgresql://localhost:5432/maestro",
  MAESTRO_STORAGE_ROOT: "/var/lib/maestro",
  MAESTRO_ADMIN_TOKEN: "secret-token",
};

describe("AppConfig", () => {
  it("loads a valid environment and applies defaults", async () => {
    const config = await load(validEnv);
    expect(config.databaseUrl).toBe(validEnv.DATABASE_URL);
    expect(config.storageRoot).toBe("/var/lib/maestro");
    expect(config.runtimeTemplate).toBe("docker run");
    expect(config.maxConcurrentWorkers).toBe(2);
    expect(config.cooldownMinutes).toBe(60);
    expect(config.retentionDays).toBe(14);
    expect(Redacted.value(config.adminToken)).toBe("secret-token");
    expect(Option.isNone(config.agentOauthToken)).toBe(true);
    expect(config.logFormat).toBe("json");
    expect(config.port).toBe(3000);
  });

  it("honors explicit overrides", async () => {
    const config = await load({
      ...validEnv,
      MAESTRO_MAX_CONCURRENT_WORKERS: "5",
      MAESTRO_LOG_FORMAT: "pretty",
      MAESTRO_PORT: "8080",
      CLAUDE_CODE_OAUTH_TOKEN: "oauth-token",
    });
    expect(config.maxConcurrentWorkers).toBe(5);
    expect(config.logFormat).toBe("pretty");
    expect(config.port).toBe(8080);
    expect(Option.map(config.agentOauthToken, Redacted.value)).toEqual(Option.some("oauth-token"));
  });

  it("fails with a readable error naming the missing variables", async () => {
    const error = await load({}).then(
      () => null,
      (e: unknown) => String(e),
    );
    expect(error).toContain("DATABASE_URL");
  });

  it("fails on invalid values", async () => {
    const error = await load({ ...validEnv, MAESTRO_PORT: "not-a-port" }).then(
      () => null,
      (e: unknown) => String(e),
    );
    expect(error).toContain("MAESTRO_PORT");
  });
});
