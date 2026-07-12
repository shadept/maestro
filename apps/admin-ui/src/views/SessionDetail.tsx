import type { SessionId, TaskRun } from "@maestro/domain";
import { createResource, createSignal, For, Show } from "solid-js";
import type { AdminClient } from "../api.ts";
import { shortId, timestamp } from "../format.ts";
import type { EventStore } from "../store.ts";
import { LogTail } from "./LogTail.tsx";

// Session detail: entity facts live from the SSE store (the snapshot carries
// every session), server-derived worktree path via the workspace endpoint, and
// the FULL TaskRun history — the SSE snapshot only carries unsettled runs, so
// history is fetched once and applied into the store, where the upsert
// semantics merge it with live events (dups converge, same as the SSE
// snapshot boundary).

export const SessionDetail = (props: {
  store: EventStore;
  client: AdminClient;
  sessionId: SessionId;
}) => {
  const [workspace] = createResource(
    () => props.sessionId,
    (sessionId) => props.client.getSessionWorkspace(sessionId),
  );

  const [historyError, setHistoryError] = createSignal<string | null>(null);
  const [historyLoaded] = createResource(
    () => props.sessionId,
    async (sessionId) => {
      try {
        for (const run of await props.client.listTaskRuns(sessionId)) {
          props.store.apply({ _tag: "TaskRunStateChanged", taskRun: run });
        }
        return true;
      } catch {
        setHistoryError("Failed to load run history.");
        return false;
      }
    },
  );

  const session = () => props.store.session(props.sessionId);
  const runs = () => props.store.runsForSession(props.sessionId);

  return (
    <section>
      <p>
        <a href="#/">← sessions</a>
      </p>
      <Show when={session()} fallback={<p class="muted">Waiting for session snapshot…</p>}>
        {(current) => (
          <>
            <h2>
              {current().ticketReference.source}:{current().ticketReference.externalId}{" "}
              <span class={`chip state-${current().state}`}>{current().state}</span>
            </h2>
            <dl class="facts">
              <dt>Session</dt>
              <dd>
                <code>{current().id}</code>
              </dd>
              <dt>Branch</dt>
              <dd>
                <code>{current().gitBranch}</code>
              </dd>
              <dt>Project</dt>
              <dd>
                <code>{current().projectId}</code>
              </dd>
              <dt>Claude session</dt>
              <dd>
                <code>{current().claudeSessionUuid ?? "— (no turn completed yet)"}</code>
              </dd>
              <dt>Worktree</dt>
              <dd>
                <code>
                  {workspace.error
                    ? "unavailable"
                    : (workspace() ?? { worktreePath: "…" }).worktreePath}
                </code>
              </dd>
              <dt>Pull request</dt>
              <dd>
                <Show when={current().prUrl} fallback={<span class="muted">not opened yet</span>}>
                  {(url) => (
                    <a href={url()} target="_blank" rel="noreferrer">
                      #{current().prNumber} — {url()}
                    </a>
                  )}
                </Show>
              </dd>
              <dt>Created</dt>
              <dd>{timestamp(current().createdAt)}</dd>
              <dt>Last activity</dt>
              <dd>{timestamp(current().lastActivityAt)}</dd>
            </dl>
          </>
        )}
      </Show>

      <h3>Turns</h3>
      <Show when={historyError()}>
        <p class="error">{historyError()}</p>
      </Show>
      <Show
        when={runs().length > 0}
        fallback={<p class="muted">{historyLoaded() ? "No turns recorded." : "Loading turns…"}</p>}
      >
        <For each={runs()}>
          {(run) => <TaskRunRow run={run} store={props.store} client={props.client} />}
        </For>
      </Show>
    </section>
  );
};

const TaskRunRow = (props: { run: TaskRun; store: EventStore; client: AdminClient }) => {
  const [expanded, setExpanded] = createSignal(false);
  const [context] = createResource(
    () => (expanded() ? props.run.id : null),
    (taskRunId) => props.client.getTaskRunContext(taskRunId),
  );

  return (
    <article class="run">
      <button type="button" class="run-header" onClick={() => setExpanded(!expanded())}>
        <span class={`chip state-${props.run.state}`}>{props.run.state}</span>
        <Show when={props.run.cause}>{(cause) => <span class="chip bad">{cause()}</span>}</Show>
        <code title={props.run.id}>{shortId(props.run.id)}</code>
        <span class="muted">{timestamp(props.run.createdAt)}</span>
        <span class="spacer" />
        <span class="muted">{expanded() ? "▾" : "▸"}</span>
      </button>
      <Show when={expanded()}>
        <div class="run-body">
          <Show when={props.run.resultText}>
            {(text) => (
              <>
                <h4>Result</h4>
                <pre class="result">{text()}</pre>
              </>
            )}
          </Show>
          <h4>Inbound payload</h4>
          <Show when={!context.error} fallback={<p class="error">Failed to load context.</p>}>
            <Show when={context()} fallback={<p class="muted">Loading…</p>}>
              {(ctx) => (
                <>
                  <p class="muted">
                    {ctx().actor} via {ctx().source} · delivery <code>{ctx().deliveryId}</code>
                    <Show when={ctx().title}>{(title) => <> · {title()}</>}</Show>
                  </p>
                  <pre class="payload">{ctx().body}</pre>
                  <details>
                    <summary>Raw platform payload</summary>
                    <pre class="payload">{JSON.stringify(ctx().payload, null, 2)}</pre>
                  </details>
                </>
              )}
            </Show>
          </Show>
          <h4>Logs</h4>
          <LogTail taskRunId={props.run.id} store={props.store} client={props.client} />
        </div>
      </Show>
    </article>
  );
};
