import { For, Show } from "solid-js";
import { relativeTime, shortId } from "../format.ts";
import { sessionHref } from "../route.ts";
import type { EventStore } from "../store.ts";

// Live session list — rendered entirely from the SSE store, so rows appear
// and state chips flip without any refresh. The "project" column shows the
// project id: the M1 admin contract has no projects endpoint, so the repo URL
// itself is not available here (honest limitation, M2 grows the contract).

export const SessionList = (props: { store: EventStore }) => (
  <section>
    <h2>Sessions</h2>
    <Show
      when={props.store.sessionList().length > 0}
      fallback={<p class="muted">No sessions yet — trigger a turn from the ticketing system.</p>}
    >
      <table>
        <thead>
          <tr>
            <th>Ticket</th>
            <th>State</th>
            <th>Branch</th>
            <th>Project</th>
            <th>PR</th>
            <th>Queue</th>
            <th>Last activity</th>
          </tr>
        </thead>
        <tbody>
          <For each={props.store.sessionList()}>
            {(session) => (
              <tr>
                <td>
                  <a href={sessionHref(session.id)}>
                    {session.ticketReference.source}:{session.ticketReference.externalId}
                  </a>
                </td>
                <td>
                  <span class={`chip state-${session.state}`}>{session.state}</span>
                </td>
                <td>
                  <code>{session.gitBranch}</code>
                </td>
                <td>
                  <code title={session.projectId}>{shortId(session.projectId)}</code>
                </td>
                <td>
                  <Show when={session.prUrl} fallback={<span class="muted">—</span>}>
                    {(url) => (
                      <a href={url()} target="_blank" rel="noreferrer">
                        #{session.prNumber}
                      </a>
                    )}
                  </Show>
                </td>
                <td>{props.store.queueDepth(session.id)}</td>
                <td title={session.lastActivityAt.toISOString()}>
                  {relativeTime(session.lastActivityAt)}
                </td>
              </tr>
            )}
          </For>
        </tbody>
      </table>
    </Show>
  </section>
);
