import { ForgeApiError } from "@maestro/domain";
import { Effect, type Layer } from "effect";
import { describe, expect, it } from "vitest";

import type { EnsurePullRequest } from "../../src/forge/Forge.ts";
import { type ForgeCall, GitHubForge, parseGitHubRepoUrl } from "../../src/forge/GitHubForge.ts";

describe("parseGitHubRepoUrl", () => {
  it.each([
    ["https://github.com/shadept/maestro", "shadept", "maestro"],
    ["https://github.com/shadept/maestro.git", "shadept", "maestro"],
    ["https://github.com/shadept/maestro/", "shadept", "maestro"],
    ["http://github.example.internal/org/repo.git", "org", "repo"],
    ["git@github.com:shadept/maestro.git", "shadept", "maestro"],
    ["git@github.com:shadept/maestro", "shadept", "maestro"],
    ["ssh://git@github.com/shadept/maestro.git", "shadept", "maestro"],
    ["ssh://git@github.com:2222/shadept/maestro", "shadept", "maestro"],
    ["https://github.com/dotted.owner/repo.name.git", "dotted.owner", "repo.name"],
  ])("parses %s", (url, owner, repo) => {
    expect(parseGitHubRepoUrl(url)).toEqual({ owner, repo });
  });

  it.each([
    ["file:///tmp/maestro-test/origin"],
    ["/tmp/maestro-test/origin"],
    ["https://github.com/owner-only"],
    ["https://github.com/"],
    ["not a url at all"],
  ])("rejects %s", (url) => {
    expect(parseGitHubRepoUrl(url)).toBeNull();
  });
});

const args = (overrides: Partial<EnsurePullRequest> = {}): EnsurePullRequest => ({
  repoGitUrl: "file:///tmp/origin",
  headBranch: "maestro/FUR-1",
  baseBranch: "main",
  title: "FUR-1: do the work",
  body: "Ticket: FUR-1",
  draft: true,
  existingNumber: null,
  ...overrides,
});

describe("GitHubForge.layerTest", () => {
  const ensure = (layer: Layer.Layer<GitHubForge>, input: EnsurePullRequest) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const forge = yield* GitHubForge;
        return yield* forge.ensurePullRequest(input);
      }).pipe(Effect.provide(layer)),
    );

  it("creates once, then updates the same head branch", async () => {
    const calls: ForgeCall[] = [];
    const layer = GitHubForge.layerTest({ calls });

    const first = await ensure(layer, args());
    expect(first.created).toBe(true);

    const second = await ensure(layer, args({ existingNumber: first.number }));
    expect(second.created).toBe(false);
    expect(second.number).toBe(first.number);
    expect(second.url).toBe(first.url);

    expect(calls.map((c) => c.op)).toEqual(["create", "update"]);
    expect(calls[0]?.args.draft).toBe(true);
  });

  it("fails every call when failWith is set", async () => {
    const failure = new ForgeApiError({ operation: "test", message: "boom", status: 500 });
    const layer = GitHubForge.layerTest({ failWith: failure });
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const forge = yield* GitHubForge;
        return yield* forge.ensurePullRequest(args()).pipe(Effect.flip);
      }).pipe(Effect.provide(layer)),
    );
    expect(error).toBe(failure);
  });
});
