import { readFile } from "node:fs/promises";
import path from "node:path";
import { Session, type SessionId, TaskContext } from "@maestro/domain";
import { Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { AgentContract, type AgentEvent } from "../../src/agent/AgentContract.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";

const decodeSession = Schema.decodeUnknownSync(Session);
const decodeContext = Schema.decodeUnknownSync(TaskContext);

const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;
const CLAUDE_UUID = "89180ca9-fe2e-4631-bc86-dc0a7de6d9d7";

const makeSession = (claudeSessionUuid: string | null): Session =>
  decodeSession({
    id: uuid(1),
    projectId: uuid(2),
    ticketReference: { source: "linear", externalId: "FUR-12" },
    gitBranch: "maestro/FUR-12",
    claudeSessionUuid,
    prNumber: null,
    prUrl: null,
    terminationRequestedAt: null,
    state: "WARM_IDLE",
    createdAt: new Date(),
    lastActivityAt: new Date(),
  });

const firstTurnContext = decodeContext({
  source: "linear",
  ticket: { source: "linear", externalId: "FUR-12" },
  actor: "shade",
  title: "Add a lint rule",
  body: "Please add the no-console lint rule to the repo.",
  deliveryId: "d-1",
  payload: {},
});

const followupContext = decodeContext({
  source: "linear",
  ticket: { source: "linear", externalId: "FUR-12" },
  actor: "shade",
  title: null,
  body: "Also apply it to the test files, please.",
  deliveryId: "d-2",
  payload: {},
});

// Records setClaudeSessionUuid calls without a database.
const recordingSessionRepo = () => {
  const calls: Array<{ id: SessionId; uuid: string }> = [];
  const layer = Layer.succeed(SessionRepo)({
    setClaudeSessionUuid: (id: SessionId, u: string) => {
      calls.push({ id, uuid: u });
      return Effect.succeed(makeSession(u));
    },
  } as unknown as SessionRepo["Service"]);
  return { calls, layer };
};

const makeLayer = (config: Parameters<typeof AppConfig.layerTest>[0] = {}) => {
  const recorder = recordingSessionRepo();
  return {
    recorder,
    layer: AgentContract.layer.pipe(
      Layer.provide(AppConfig.layerTest(config)),
      Layer.provide(recorder.layer),
      Layer.orDie,
    ),
  };
};

const run = <A, E>(
  effect: Effect.Effect<A, E, AgentContract>,
  layer: Layer.Layer<AgentContract>,
): Promise<A> => Effect.runPromise(Effect.provide(effect, layer));

const parseFixture = async (name: string, layer: Layer.Layer<AgentContract>) => {
  const raw = await readFile(path.resolve(import.meta.dirname, "../fixtures/agent", name), "utf8");
  // feed as irregular chunks to prove line re-assembly
  const chunks = [raw.slice(0, 100), raw.slice(100, 350), raw.slice(350)];
  return run(
    Effect.gen(function* () {
      const agent = yield* AgentContract;
      return yield* Stream.runCollect(agent.parseStream(Stream.fromIterable(chunks)));
    }),
    layer,
  );
};

describe("AgentContract.buildCommand", () => {
  it("first turn: composes title + body, no --resume, subscription token preferred", async () => {
    const { layer } = makeLayer({
      agentOauthToken: Option.some(Redacted.make("oauth-123")),
      agentApiKey: Option.some(Redacted.make("api-456")),
    });
    const command = await run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        return agent.buildCommand({
          session: makeSession(null),
          context: firstTurnContext,
          configDir: "/session-config",
        });
      }),
      layer,
    );
    expect(command.argv[0]).toBe("claude");
    expect(command.argv).toContain("-p");
    expect(command.argv[2]).toBe(
      "Add a lint rule\n\nPlease add the no-console lint rule to the repo.",
    );
    expect(command.argv).toContain("--output-format");
    expect(command.argv).toContain("stream-json");
    expect(command.argv).toContain("--verbose");
    expect(command.argv).toContain("--dangerously-skip-permissions");
    expect(command.argv).not.toContain("--resume");
    expect(command.env.CLAUDE_CONFIG_DIR).toBe("/session-config");
    expect(command.env.CLAUDE_CODE_OAUTH_TOKEN).toBe("oauth-123");
    expect(command.env.ANTHROPIC_API_KEY).toBeUndefined();
  });

  it("resume turn: sends comment body only and resumes the stored session", async () => {
    const { layer } = makeLayer({ agentApiKey: Option.some(Redacted.make("api-456")) });
    const command = await run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        return agent.buildCommand({
          session: makeSession(CLAUDE_UUID),
          context: followupContext,
          configDir: "/session-config",
        });
      }),
      layer,
    );
    expect(command.argv[2]).toBe("Also apply it to the test files, please.");
    const resumeIndex = command.argv.indexOf("--resume");
    expect(resumeIndex).toBeGreaterThan(-1);
    expect(command.argv[resumeIndex + 1]).toBe(CLAUDE_UUID);
    // API key fallback when no subscription token
    expect(command.env.ANTHROPIC_API_KEY).toBe("api-456");
    expect(command.env.CLAUDE_CODE_OAUTH_TOKEN).toBeUndefined();
  });

  it("no auth configured: env carries only CLAUDE_CONFIG_DIR", async () => {
    const { layer } = makeLayer();
    const command = await run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        return agent.buildCommand({
          session: makeSession(null),
          context: followupContext,
          configDir: "/cfg",
        });
      }),
      layer,
    );
    expect(Object.keys(command.env)).toEqual(["CLAUDE_CONFIG_DIR"]);
  });
});

describe("AgentContract.parseStream", () => {
  it("parses the happy path into the right event sequence", async () => {
    const { layer } = makeLayer();
    const events = await parseFixture("happy-path.jsonl", layer);
    expect(events.map((e: AgentEvent) => e._tag)).toEqual([
      "SessionStarted",
      "Text",
      "ToolUse",
      "Text",
      "Result",
    ]);
    const [started, , tool, , result] = events;
    expect(started).toEqual({ _tag: "SessionStarted", claudeSessionUuid: CLAUDE_UUID });
    expect(tool).toEqual({ _tag: "ToolUse", name: "Bash" });
    expect(result).toEqual({
      _tag: "Result",
      finalText: "Done — rule added and tests pass.",
      ok: true,
    });
  });

  it("mid-stream error yields Result(ok=false) with the error text", async () => {
    const { layer } = makeLayer();
    const events = await parseFixture("mid-stream-error.jsonl", layer);
    expect(events.map((e: AgentEvent) => e._tag)).toEqual(["SessionStarted", "Text", "Result"]);
    expect(events.at(-1)).toEqual({
      _tag: "Result",
      finalText: "Execution failed: process was terminated",
      ok: false,
    });
  });

  it("unknown event types and garbage lines are skipped, never crash", async () => {
    const { layer } = makeLayer();
    const events = await parseFixture("unknown-events.jsonl", layer);
    expect(events.map((e: AgentEvent) => e._tag)).toEqual(["SessionStarted", "Text", "Result"]);
  });
});

describe("AgentContract.persistSessionUuid", () => {
  it("persists on first SessionStarted, no-ops afterwards and for other events", async () => {
    const { layer, recorder } = makeLayer();
    await run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        const fresh = makeSession(null);
        yield* agent.persistSessionUuid(fresh, { _tag: "Text", text: "hi" });
        yield* agent.persistSessionUuid(fresh, {
          _tag: "SessionStarted",
          claudeSessionUuid: CLAUDE_UUID,
        });
        // already has a uuid — must not overwrite
        yield* agent.persistSessionUuid(makeSession(CLAUDE_UUID), {
          _tag: "SessionStarted",
          claudeSessionUuid: uuid(99),
        });
      }),
      layer,
    );
    expect(recorder.calls).toHaveLength(1);
    expect(recorder.calls[0]?.uuid).toBe(CLAUDE_UUID);
  });
});
