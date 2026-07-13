import { LinearClient } from "@linear/sdk";
import { Effect, Layer, Option, Redacted } from "effect";
import { describe, expect, it } from "vitest";

import {
  detectLinearTokenKind,
  LinearCallback,
  type LinearTokenKind,
  linearClientOptionsFor,
} from "../../src/callback/LinearCallback.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";

// FUR-42: Maestro posts as an OAuth app identity. The SDK authenticates the
// two token kinds differently — `apiKey` goes out as the raw Authorization
// header, `accessToken` as `Bearer <token>` — so these tests pin both the
// kind detection and the constructor-level header behavior; an SDK upgrade
// cannot silently flip either without failing here.

describe("detectLinearTokenKind", () => {
  it("detects lin_api_ personal keys", () => {
    expect(detectLinearTokenKind("lin_api_abc123")).toBe("api-key");
  });

  it("treats lin_oauth_-prefixed tokens as oauth", () => {
    expect(detectLinearTokenKind("lin_oauth_abc123")).toBe("oauth");
  });

  it("treats unprefixed (hex) OAuth access tokens as oauth", () => {
    expect(detectLinearTokenKind("00a21d8b0c4e2375114e49c067dfb81e")).toBe("oauth");
  });
});

describe("linearClientOptionsFor", () => {
  it("auto-detects a personal key into the apiKey option", () => {
    expect(linearClientOptionsFor("lin_api_abc123")).toEqual({ apiKey: "lin_api_abc123" });
  });

  it("auto-detects an access token into the accessToken option", () => {
    expect(linearClientOptionsFor("lin_oauth_abc123")).toEqual({
      accessToken: "lin_oauth_abc123",
    });
  });

  it("explicit api-key kind overrides detection (legacy unprefixed personal keys)", () => {
    expect(linearClientOptionsFor("legacy-unprefixed-key", Option.some("api-key"))).toEqual({
      apiKey: "legacy-unprefixed-key",
    });
  });

  it("explicit oauth kind overrides detection", () => {
    expect(linearClientOptionsFor("lin_api_actually_oauth", Option.some("oauth"))).toEqual({
      accessToken: "lin_api_actually_oauth",
    });
  });
});

describe("LinearClient constructor (@linear/sdk 88)", () => {
  const authHeaderOf = (client: LinearClient): string | undefined =>
    (client.options.headers as Record<string, string>).Authorization;

  it("apiKey option sends the personal key raw in the Authorization header", () => {
    const client = new LinearClient(linearClientOptionsFor("lin_api_abc123"));
    expect(authHeaderOf(client)).toBe("lin_api_abc123");
  });

  it("accessToken option sends a Bearer Authorization header", () => {
    const client = new LinearClient(linearClientOptionsFor("00a21d8b0c4e2375114e49c067dfb81e"));
    expect(authHeaderOf(client)).toBe("Bearer 00a21d8b0c4e2375114e49c067dfb81e");
  });
});

describe("LinearCallback.layer", () => {
  const buildWith = (token: string, kind?: LinearTokenKind) =>
    Effect.runPromise(
      Effect.gen(function* () {
        return yield* LinearCallback;
      }).pipe(
        Effect.provide(
          LinearCallback.layer.pipe(
            Layer.provide(
              AppConfig.layerTest({
                linearApiToken: Option.some(Redacted.make(token)),
                linearTokenKind: kind === undefined ? Option.none() : Option.some(kind),
              }),
            ),
          ),
        ),
      ),
    );

  it("builds for a personal key, an app token, and an explicit override", async () => {
    await expect(buildWith("lin_api_abc123")).resolves.toBeDefined();
    await expect(buildWith("00a21d8b0c4e2375114e49c067dfb81e")).resolves.toBeDefined();
    await expect(buildWith("legacy-unprefixed-key", "api-key")).resolves.toBeDefined();
  });
});
