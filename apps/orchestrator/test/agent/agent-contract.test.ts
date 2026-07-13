import { readFile } from "node:fs/promises";
import path from "node:path";
import {
  type AgentEffort,
  type AgentOverrides,
  Project,
  Session,
  type SessionId,
  TaskContext,
} from "@maestro/domain";
import { Effect, Layer, Option, Redacted, Schema, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { AgentContract, type AgentEvent, standingOrders } from "../../src/agent/AgentContract.ts";
import { AppConfig } from "../../src/config/AppConfig.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";

const decodeSession = Schema.decodeUnknownSync(Session);
const decodeProject = Schema.decodeUnknownSync(Project);
const decodeContext = Schema.decodeUnknownSync(TaskContext);

const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;
const CLAUDE_UUID = "89180ca9-fe2e-4631-bc86-dc0a7de6d9d7";

const makeSession = (
  claudeSessionUuid: string | null,
  pinned: { model?: string; effort?: AgentEffort } = {},
): Session =>
  decodeSession({
    id: uuid(1),
    projectId: uuid(2),
    ticketReference: { source: "linear", externalId: "FUR-12" },
    gitBranch: "maestro/FUR-12",
    claudeSessionUuid,
    prNumber: null,
    prUrl: null,
    terminationRequestedAt: null,
    pausedAt: null,
    agentModel: pinned.model ?? null,
    agentEffort: pinned.effort ?? null,
    state: "WARM_IDLE",
    createdAt: new Date(),
    lastActivityAt: new Date(),
  });

const makeProject = (agent: AgentOverrides = {}): Project =>
  decodeProject({
    id: uuid(2),
    repoGitUrl: "https://github.com/acme/flux.git",
    linearTeamKey: "FUR",
    localCachePath: null,
    gitConventions: {},
    resources: {},
    agent,
    createdAt: new Date(),
  });

const makeContext = (
  overrides: { model?: string; effort?: AgentEffort } = {},
  base: { title: string | null; body: string } = {
    title: "Add a lint rule",
    body: "Please add the no-console lint rule to the repo.",
  },
): TaskContext =>
  decodeContext({
    source: "linear",
    ticket: { source: "linear", externalId: "FUR-12" },
    actor: "shade",
    title: base.title,
    body: base.body,
    agentModel: overrides.model ?? null,
    agentEffort: overrides.effort ?? null,
    deliveryId: "d-1",
    payload: {},
  });

const firstTurnContext = makeContext();

const followupContext = makeContext(
  {},
  { title: null, body: "Also apply it to the test files, please." },
);

// The orders every prompt must end with, for the test sessions above.
const STANDING_ORDERS = standingOrders({ branchName: "maestro/FUR-12", ticketId: "FUR-12" });

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
          project: makeProject(),
          configDir: "/session-config",
        });
      }),
      layer,
    );
    expect(command.argv[0]).toBe("claude");
    expect(command.argv).toContain("-p");
    expect(command.argv[2]).toBe(
      `Add a lint rule\n\nPlease add the no-console lint rule to the repo.\n\n${STANDING_ORDERS}`,
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
          project: makeProject(),
          configDir: "/session-config",
        });
      }),
      layer,
    );
    expect(command.argv[2]).toBe(`Also apply it to the test files, please.\n\n${STANDING_ORDERS}`);
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
          project: makeProject(),
          configDir: "/cfg",
        });
      }),
      layer,
    );
    expect(Object.keys(command.env)).toEqual(["CLAUDE_CONFIG_DIR"]);
  });

  it("standing orders: every prompt ends with them and they carry branch, ticket, and rules", async () => {
    const { layer } = makeLayer();
    const [first, resume] = await run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        return [
          agent.buildCommand({
            session: makeSession(null),
            context: firstTurnContext,
            project: makeProject(),
            configDir: "/cfg",
          }),
          agent.buildCommand({
            session: makeSession(CLAUDE_UUID),
            context: followupContext,
            project: makeProject(),
            configDir: "/cfg",
          }),
        ];
      }),
      layer,
    );
    for (const command of [first, resume]) {
      const prompt = command?.argv[2] ?? "";
      expect(prompt.endsWith(STANDING_ORDERS)).toBe(true);
      expect(prompt).toContain("maestro/FUR-12");
    }
    expect(STANDING_ORDERS).toContain("branch maestro/FUR-12");
    expect(STANDING_ORDERS).toContain("referencing FUR-12");
    expect(STANDING_ORDERS).toContain("commit ALL changes");
    expect(STANDING_ORDERS).toContain("NEVER push");
    expect(STANDING_ORDERS).toContain("do not create an empty commit");
    expect(STANDING_ORDERS).toContain("quality gates");
  });
});

describe("AgentContract.buildCommand model/effort precedence (FUR-41)", () => {
  // Runs buildCommand with the given levels populated and returns the command.
  const build = async (args: {
    readonly config?: { model?: string; effort?: AgentEffort };
    readonly project?: AgentOverrides;
    readonly session?: { claudeSessionUuid?: string; model?: string; effort?: AgentEffort };
    readonly task?: { model?: string; effort?: AgentEffort };
  }) => {
    const { layer } = makeLayer({
      ...(args.config?.model !== undefined && { agentModel: Option.some(args.config.model) }),
      ...(args.config?.effort !== undefined && { agentEffort: Option.some(args.config.effort) }),
    });
    return run(
      Effect.gen(function* () {
        const agent = yield* AgentContract;
        return agent.buildCommand({
          session: makeSession(args.session?.claudeSessionUuid ?? null, args.session ?? {}),
          context: makeContext(args.task ?? {}),
          project: makeProject(args.project ?? {}),
          configDir: "/cfg",
        });
      }),
      layer,
    );
  };

  const flagValue = (argv: ReadonlyArray<string>, flag: string): string | undefined => {
    const index = argv.indexOf(flag);
    return index === -1 ? undefined : argv[index + 1];
  };

  it("nothing configured anywhere: argv is byte-for-byte the pre-FUR-41 command", async () => {
    const command = await build({});
    expect(command.argv).toEqual([
      "claude",
      "-p",
      `Add a lint rule\n\nPlease add the no-console lint rule to the repo.\n\n${STANDING_ORDERS}`,
      "--output-format",
      "stream-json",
      "--verbose",
      "--dangerously-skip-permissions",
    ]);
    expect(command.resolved).toEqual({ model: null, effort: null });
  });

  it("deployment level alone applies", async () => {
    const command = await build({ config: { model: "claude-haiku-4-5", effort: "low" } });
    expect(flagValue(command.argv, "--model")).toBe("claude-haiku-4-5");
    expect(flagValue(command.argv, "--effort")).toBe("low");
    expect(command.resolved).toEqual({ model: "claude-haiku-4-5", effort: "low" });
  });

  it("project level beats deployment", async () => {
    const command = await build({
      config: { model: "claude-haiku-4-5", effort: "low" },
      project: { model: "claude-sonnet-4-5", effort: "medium" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-sonnet-4-5");
    expect(flagValue(command.argv, "--effort")).toBe("medium");
  });

  it("task level beats project and deployment", async () => {
    const command = await build({
      config: { model: "claude-haiku-4-5", effort: "low" },
      project: { model: "claude-sonnet-4-5", effort: "medium" },
      task: { model: "claude-opus-4-6", effort: "max" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-opus-4-6");
    expect(flagValue(command.argv, "--effort")).toBe("max");
  });

  it("task level beats deployment with no project override in between", async () => {
    const command = await build({
      config: { model: "claude-haiku-4-5" },
      task: { model: "claude-opus-4-6" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-opus-4-6");
  });

  it("model and effort resolve independently across levels", async () => {
    const command = await build({
      config: { effort: "low" },
      project: { model: "claude-sonnet-4-5" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-sonnet-4-5");
    expect(flagValue(command.argv, "--effort")).toBe("low");
  });

  it("only one side configured: only that flag appears", async () => {
    const modelOnly = await build({ project: { model: "claude-sonnet-4-5" } });
    expect(flagValue(modelOnly.argv, "--model")).toBe("claude-sonnet-4-5");
    expect(modelOnly.argv).not.toContain("--effort");
    const effortOnly = await build({ project: { effort: "xhigh" } });
    expect(effortOnly.argv).not.toContain("--model");
    expect(flagValue(effortOnly.argv, "--effort")).toBe("xhigh");
  });

  it("resume turn keeps the session's pinned settings over project/deployment changes", async () => {
    const command = await build({
      config: { model: "claude-haiku-4-5", effort: "low" },
      project: { model: "claude-sonnet-4-5", effort: "medium" },
      session: { claudeSessionUuid: CLAUDE_UUID, model: "claude-opus-4-6", effort: "high" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-opus-4-6");
    expect(flagValue(command.argv, "--effort")).toBe("high");
    expect(flagValue(command.argv, "--resume")).toBe(CLAUDE_UUID);
  });

  it("a task-level override deliberately beats the session pin on a resume turn", async () => {
    const command = await build({
      session: { claudeSessionUuid: CLAUDE_UUID, model: "claude-opus-4-6", effort: "high" },
      task: { model: "claude-haiku-4-5", effort: "low" },
    });
    expect(flagValue(command.argv, "--model")).toBe("claude-haiku-4-5");
    expect(flagValue(command.argv, "--effort")).toBe("low");
  });

  it("flags precede --resume so the resumed session picks them up", async () => {
    const command = await build({
      project: { model: "claude-sonnet-4-5" },
      session: { claudeSessionUuid: CLAUDE_UUID },
    });
    const model = command.argv.indexOf("--model");
    const resume = command.argv.indexOf("--resume");
    expect(model).toBeGreaterThan(-1);
    expect(resume).toBeGreaterThan(model);
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
