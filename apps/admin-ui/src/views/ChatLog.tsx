import { createMemo, Index, Match, Show, Switch } from "solid-js";
import { type ChatItem, parseChatLog } from "../chat-log.ts";

// Chat-derived view of a log tail (FUR-43). `<Index>` (keyed by position, not
// by value) is deliberate: parseChatLog re-derives the whole item list from
// the raw text on every update, so earlier items are new object references
// even though their content is unchanged. `<For>` would key by reference and
// tear down/rebuild every row on each chunk, collapsing any `<details>` the
// user had opened; `<Index>` keeps each row's DOM in place and only updates
// what actually changed.

export const ChatLog = (props: { text: string }) => {
  const items = createMemo(() => parseChatLog(props.text));

  return (
    <div class="chat-log">
      <Show when={items().length > 0} fallback={<p class="muted">(no log output yet)</p>}>
        <Index each={items()}>{(item) => <ChatItemRow item={item()} />}</Index>
      </Show>
    </div>
  );
};

const ChatItemRow = (props: { item: ChatItem }) => (
  <Switch>
    <Match when={props.item.kind === "text" && props.item}>
      {(item) => <p class="chat-bubble">{item().text}</p>}
    </Match>
    <Match when={props.item.kind === "thinking" && props.item}>
      {(item) => (
        <details class="chat-thinking">
          <summary>Thinking</summary>
          <p>{item().text.length > 0 ? item().text : "(empty)"}</p>
        </details>
      )}
    </Match>
    <Match when={props.item.kind === "tool-use" && props.item}>
      {(item) => (
        <details class="chat-tool">
          <summary>
            <span class="chip">tool</span> {item().name}
            <Show when={item().result}>
              {(result) => <span class={`chip ${result().isError ? "bad" : "ok"}`}>done</span>}
            </Show>
          </summary>
          <pre class="payload">{JSON.stringify(item().input, null, 2) ?? "null"}</pre>
          <Show when={item().result}>
            {(result) => (
              <>
                <h5 class={result().isError ? "error" : "muted"}>
                  {result().isError ? "tool error" : "tool result"}
                </h5>
                <pre class="payload">{result().content}</pre>
              </>
            )}
          </Show>
        </details>
      )}
    </Match>
    <Match when={props.item.kind === "result" && props.item}>
      {(item) => (
        <div class={`chat-result ${item().ok ? "ok" : "bad"}`}>
          <strong>{item().ok ? "✓ done" : "✗ failed"}</strong>
          <p>{item().text}</p>
          <Show when={item().durationMs !== null || item().costUsd !== null}>
            <p class="muted">
              {item().durationMs !== null ? `${item().durationMs}ms` : ""}
              {item().durationMs !== null && item().costUsd !== null ? " · " : ""}
              {item().costUsd !== null ? `$${(item().costUsd as number).toFixed(2)}` : ""}
            </p>
          </Show>
        </div>
      )}
    </Match>
    <Match when={props.item.kind === "raw" && props.item}>
      {(item) => <pre class="chat-raw">{item().line}</pre>}
    </Match>
  </Switch>
);
