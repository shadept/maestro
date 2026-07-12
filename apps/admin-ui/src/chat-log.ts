// Folds a raw stream-json log — persisted text plus appended LogChunks,
// already concatenated by the store (FUR-17) — into a chat-shaped item list.
// Tolerant like the orchestrator's AgentContract.parseLine (FUR-12): JSON that
// fails to parse, or has no recognizable "type", falls back to a raw row
// instead of breaking the render. Recognized-but-unrendered event types
// (system/init, rate_limit_event, ...) are dropped silently, same as
// AgentContract. Unlike AgentContract's reduced 4-variant AgentEvent, this
// keeps thinking blocks and pairs tool_use with a later tool_result, since the
// chat view needs to show both.

export interface ToolResult {
  readonly content: string;
  readonly isError: boolean;
}

export type ChatItem =
  | { readonly kind: "text"; readonly id: number; readonly text: string }
  | { readonly kind: "thinking"; readonly id: number; readonly text: string }
  | {
      readonly kind: "tool-use";
      readonly id: number;
      readonly toolUseId: string;
      readonly name: string;
      readonly input: unknown;
      result: ToolResult | null;
    }
  | {
      readonly kind: "result";
      readonly id: number;
      readonly ok: boolean;
      readonly text: string;
      readonly costUsd: number | null;
      readonly durationMs: number | null;
    }
  | { readonly kind: "raw"; readonly id: number; readonly line: string };

type UnknownRecord = Record<string, unknown>;

const isRecord = (value: unknown): value is UnknownRecord =>
  typeof value === "object" && value !== null;

const contentBlocksOf = (event: UnknownRecord): ReadonlyArray<unknown> => {
  const message = event.message;
  if (!isRecord(message)) return [];
  const content = message.content;
  return Array.isArray(content) ? content : [];
};

/** tool_result content is either a plain string or a list of content blocks. */
const textOf = (content: unknown): string => {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((block) => (isRecord(block) && typeof block.text === "string" ? block.text : ""))
    .filter((text) => text.length > 0)
    .join("\n");
};

export const parseChatLog = (rawText: string): ChatItem[] => {
  const items: ChatItem[] = [];
  const toolUseById = new Map<string, Extract<ChatItem, { kind: "tool-use" }>>();
  let nextId = 0;

  for (const line of rawText.split("\n")) {
    if (line.trim() === "") continue;

    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      items.push({ kind: "raw", id: nextId++, line });
      continue;
    }
    if (!isRecord(parsed) || typeof parsed.type !== "string") {
      items.push({ kind: "raw", id: nextId++, line });
      continue;
    }

    switch (parsed.type) {
      case "assistant":
      case "user":
        for (const block of contentBlocksOf(parsed)) {
          if (!isRecord(block)) continue;
          if (block.type === "text" && typeof block.text === "string") {
            items.push({ kind: "text", id: nextId++, text: block.text });
          } else if (block.type === "thinking" && typeof block.thinking === "string") {
            items.push({ kind: "thinking", id: nextId++, text: block.thinking });
          } else if (
            block.type === "tool_use" &&
            typeof block.id === "string" &&
            typeof block.name === "string"
          ) {
            const toolUse: Extract<ChatItem, { kind: "tool-use" }> = {
              kind: "tool-use",
              id: nextId++,
              toolUseId: block.id,
              name: block.name,
              input: block.input,
              result: null,
            };
            items.push(toolUse);
            toolUseById.set(block.id, toolUse);
          } else if (block.type === "tool_result" && typeof block.tool_use_id === "string") {
            const paired = toolUseById.get(block.tool_use_id);
            if (paired !== undefined) {
              paired.result = { content: textOf(block.content), isError: block.is_error === true };
            }
          }
        }
        break;
      case "result":
        items.push({
          kind: "result",
          id: nextId++,
          ok: parsed.is_error !== true,
          text: typeof parsed.result === "string" ? parsed.result : "",
          costUsd: typeof parsed.total_cost_usd === "number" ? parsed.total_cost_usd : null,
          durationMs: typeof parsed.duration_ms === "number" ? parsed.duration_ms : null,
        });
        break;
      default:
        // recognized-but-unrendered (system/init, rate_limit_event, ...) — tolerated
        break;
    }
  }

  return items;
};
