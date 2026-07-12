import type { TaskRunId } from "@maestro/domain";
import { createEffect, createSignal, onMount } from "solid-js";
import type { AdminClient } from "../api.ts";
import type { EventStore } from "../store.ts";

// Live log tail per TaskRun: historical logs are fetched once and rebased into
// the store's buffer, then live SSE LogChunks keep appending (FUR-17). A chunk
// published in the instant between the fetch response and the rebase can be
// dropped from the view (it is still persisted server-side); at-least-once the
// other way — a chunk both in the fetch and the buffer — is resolved by the
// rebase replacing the buffer wholesale. Debug surface, not an archive.
//
// Auto-scroll pins to the bottom until the user scrolls up; scrolling back to
// the bottom re-pins.

export const LogTail = (props: {
  taskRunId: TaskRunId;
  store: EventStore;
  client: AdminClient;
}) => {
  const [fetchError, setFetchError] = createSignal(false);
  let container: HTMLPreElement | undefined;
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

  createEffect(() => {
    content(); // track: re-run on every appended chunk
    if (pinned && container !== undefined) {
      container.scrollTop = container.scrollHeight;
    }
  });

  const onScroll = () => {
    if (container === undefined) return;
    pinned = container.scrollHeight - container.scrollTop - container.clientHeight < 40;
  };

  return (
    <div>
      {fetchError() && <p class="error">Failed to fetch historical logs.</p>}
      <pre class="log-tail" ref={container} onScroll={onScroll}>
        {content().length > 0 ? content() : "(no log output yet)"}
      </pre>
    </div>
  );
};
