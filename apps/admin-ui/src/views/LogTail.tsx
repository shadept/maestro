import type { TaskRunId } from "@maestro/domain";
import { createEffect, createSignal, onMount, Show } from "solid-js";
import type { AdminClient } from "../api.ts";
import type { EventStore } from "../store.ts";
import { ChatLog } from "./ChatLog.tsx";

// Live log tail per TaskRun: historical logs are fetched once and rebased into
// the store's buffer, then live SSE LogChunks keep appending (FUR-17). A chunk
// published in the instant between the fetch response and the rebase can be
// dropped from the view (it is still persisted server-side); at-least-once the
// other way — a chunk both in the fetch and the buffer — is resolved by the
// rebase replacing the buffer wholesale. Debug surface, not an archive.
//
// Auto-scroll pins to the bottom until the user scrolls up; scrolling back to
// the bottom re-pins. Storage and the SSE feed stay raw text (byte-identical
// to the worker's stdout/stderr, FUR-43); "chat" is a derived, UI-only
// interpretation via ChatLog — "raw" is the untouched tail, kept as a toggle
// since chat rendering can only approximate the underlying stream-json.

type ViewMode = "chat" | "raw";

export const LogTail = (props: {
  taskRunId: TaskRunId;
  store: EventStore;
  client: AdminClient;
}) => {
  const [fetchError, setFetchError] = createSignal(false);
  const [mode, setMode] = createSignal<ViewMode>("chat");
  let rawContainer: HTMLPreElement | undefined;
  let chatContainer: HTMLDivElement | undefined;
  let pinned = true;

  onMount(async () => {
    try {
      const historical = await props.client.getTaskRunLogs(props.taskRunId);
      props.store.rebaseLogs(props.taskRunId, historical);
    } catch {
      setFetchError(true);
    }
  });

  const content = () => props.store.logFor(props.taskRunId);
  const activeContainer = () => (mode() === "chat" ? chatContainer : rawContainer);

  createEffect(() => {
    content(); // track: re-run on every appended chunk
    mode(); // track: re-pin when switching views
    const el = activeContainer();
    if (pinned && el !== undefined) {
      el.scrollTop = el.scrollHeight;
    }
  });

  const onScroll = (event: Event) => {
    const el = event.currentTarget as HTMLElement;
    pinned = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  };

  return (
    <div>
      {fetchError() && <p class="error">Failed to fetch historical logs.</p>}
      <div class="view-toggle">
        <button
          type="button"
          classList={{ active: mode() === "chat" }}
          onClick={() => setMode("chat")}
        >
          Chat
        </button>
        <button
          type="button"
          classList={{ active: mode() === "raw" }}
          onClick={() => setMode("raw")}
        >
          Raw
        </button>
      </div>
      <Show
        when={mode() === "chat"}
        fallback={
          <pre class="log-tail" ref={rawContainer} onScroll={onScroll}>
            {content().length > 0 ? content() : "(no log output yet)"}
          </pre>
        }
      >
        <div class="log-tail" ref={chatContainer} onScroll={onScroll}>
          <ChatLog text={content()} />
        </div>
      </Show>
    </div>
  );
};
