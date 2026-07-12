import type { DbError, Session, TaskContext } from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted, Result, Stream } from "effect";
import { AppConfig } from "../config/AppConfig.ts";
import { SessionRepo } from "../db/SessionRepo.ts";

// The claude-code invocation contract (Tech Requirements §9), verified
// against claude-code 2.1.207:
//   claude -p <prompt> --output-format stream-json --verbose \
//     --dangerously-skip-permissions [--resume <sessionUuid>]
// `--verbose` is mandatory with `-p --output-format stream-json`.
// Auth: CLAUDE_CODE_OAUTH_TOKEN (subscription, preferred) or ANTHROPIC_API_KEY.

export type AgentEvent =
  | { readonly _tag: "SessionStarted"; readonly claudeSessionUuid: string }
  | { readonly _tag: "Text"; readonly text: string }
  | { readonly _tag: "ToolUse"; readonly name: string }
  | { readonly _tag: "Result"; readonly finalText: string; readonly ok: boolean };

export interface AgentCommand {
  readonly argv: ReadonlyArray<string>;
  readonly env: Readonly<Record<string, string>>;
}

/**
 * Standing orders appended to EVERY worker prompt (first turn and resume).
 * claude-code rightly never commits unless instructed — without these the
 * agent finishes its work, reports "Done", and OutboundGit finds zero commits
 * (the FUR-20 dogfood defect). The instruction must come from Maestro, every
 * turn. Deliberately not configurable (YAGNI).
 */
export const standingOrders = (args: {
  readonly branchName: string;
  readonly ticketId: string;
}): string =>
  [
    "--- STANDING ORDERS (Maestro orchestrator) ---",
    `You are working in a git worktree on branch ${args.branchName} of the project repository.`,
    `When your work is complete, commit ALL changes to this branch with a clear message referencing ${args.ticketId}.`,
    "If the repository defines quality gates (CLAUDE.md or similar), run them before committing.",
    "NEVER push, never create pull requests, never touch git remotes — Maestro publishes your commits with its own credentials after this turn.",
    "If the task requires no file changes (a pure question), just answer — do not create an empty commit.",
  ].join("\n");

/** Parses one stream-json line into an event; Option.none for tolerated noise. */
const parseLine = (line: string): Option.Option<AgentEvent> | "unknown" => {
  const trimmed = line.trim();
  if (trimmed.length === 0) return Option.none();
  let json: unknown;
  try {
    json = JSON.parse(trimmed);
  } catch {
    return "unknown";
  }
  const event = json as {
    type?: string;
    session_id?: string;
    is_error?: boolean;
    result?: string;
    message?: { content?: Array<{ type?: string; text?: string; name?: string }> };
  };
  switch (event.type) {
    case "system":
      return typeof event.session_id === "string"
        ? Option.some({ _tag: "SessionStarted", claudeSessionUuid: event.session_id })
        : Option.none();
    case "assistant": {
      const content = event.message?.content ?? [];
      for (const block of content) {
        if (block.type === "text" && typeof block.text === "string") {
          return Option.some({ _tag: "Text", text: block.text });
        }
        if (block.type === "tool_use" && typeof block.name === "string") {
          return Option.some({ _tag: "ToolUse", name: block.name });
        }
      }
      return Option.none(); // e.g. thinking-only messages
    }
    case "result":
      return Option.some({
        _tag: "Result",
        finalText: typeof event.result === "string" ? event.result : "",
        ok: event.is_error !== true,
      });
    case undefined:
      return "unknown";
    default:
      // known-shape but unhandled event type (e.g. rate_limit_event) — tolerated
      return Option.none();
  }
};

export class AgentContract extends Context.Service<
  AgentContract,
  {
    /**
     * Builds the worker command for a turn. First turn (no stored session
     * uuid) composes ticket title + body; follow-ups send the comment body
     * and resume the stored claude session. Every prompt ends with the
     * standing orders (branch, commit duty, no-push rule).
     */
    readonly buildCommand: (args: {
      readonly session: Session;
      readonly context: TaskContext;
      /** CLAUDE_CONFIG_DIR as seen by the worker (container path). */
      readonly configDir: string;
    }) => AgentCommand;
    /** Line-buffered stream-json parser; unknown events are logged and skipped. */
    readonly parseStream: <E>(chunks: Stream.Stream<string, E>) => Stream.Stream<AgentEvent, E>;
    /**
     * Persists the claude session uuid on first sight (PRD §3.3 stateful
     * resumption). No-op for events other than SessionStarted or when the
     * session already has a uuid.
     */
    readonly persistSessionUuid: (
      session: Session,
      event: AgentEvent,
    ) => Effect.Effect<void, DbError>;
  }
>()("maestro/agent/AgentContract") {
  static readonly layer = Layer.effect(
    AgentContract,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      const sessionRepo = yield* SessionRepo;

      return {
        buildCommand: ({ session, context, configDir }) => {
          const isFirstTurn = session.claudeSessionUuid === null;
          const task =
            isFirstTurn && context.title !== null
              ? `${context.title}\n\n${context.body}`
              : context.body;
          // Appended, not prepended: the task reads first (like a user
          // message), the orders land last where instruction-following is
          // strongest.
          const prompt = `${task}\n\n${standingOrders({
            branchName: session.gitBranch,
            ticketId: session.ticketReference.externalId,
          })}`;
          const argv = [
            "claude",
            "-p",
            prompt,
            "--output-format",
            "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
            ...(session.claudeSessionUuid !== null ? ["--resume", session.claudeSessionUuid] : []),
          ];
          const auth = Option.match(config.agentOauthToken, {
            onSome: (token) => ({ CLAUDE_CODE_OAUTH_TOKEN: Redacted.value(token) }),
            onNone: () =>
              Option.match(config.agentApiKey, {
                onSome: (key) => ({ ANTHROPIC_API_KEY: Redacted.value(key) }),
                onNone: () => ({}),
              }),
          });
          return { argv, env: { CLAUDE_CONFIG_DIR: configDir, ...auth } };
        },

        parseStream: <E>(chunks: Stream.Stream<string, E>) =>
          chunks.pipe(
            Stream.splitLines,
            Stream.filterMapEffect((line: string) => {
              const parsed = parseLine(line);
              if (parsed === "unknown") {
                return Effect.logWarning("AgentContract: skipping unparseable stream line", {
                  line: line.slice(0, 200),
                }).pipe(Effect.as(Result.fail(line)));
              }
              return Effect.succeed(
                Option.isSome(parsed) ? Result.succeed(parsed.value) : Result.fail(line),
              );
            }),
          ),

        persistSessionUuid: Effect.fn("AgentContract.persistSessionUuid")(function* (
          session: Session,
          event: AgentEvent,
        ) {
          if (event._tag === "SessionStarted" && session.claudeSessionUuid === null) {
            yield* sessionRepo.setClaudeSessionUuid(session.id, event.claudeSessionUuid);
          }
        }),
      };
    }),
  );
}
