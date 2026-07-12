import { createSignal, onCleanup, Show } from "solid-js";
import { type AdminClient, createAdminClient } from "./api.ts";
import { useRoute } from "./route.ts";
import { connectEvents } from "./sse.ts";
import { createEventStore } from "./store.ts";
import { SessionDetail } from "./views/SessionDetail.tsx";
import { SessionList } from "./views/SessionList.tsx";
import { StatusBar } from "./views/StatusBar.tsx";
import { TokenGate } from "./views/TokenGate.tsx";

// Root component: admin-token gate, then the read-only debug surface driven by
// one app-wide SSE subscription (unfiltered — it carries sessions, runs, and
// log chunks; the views select what they need from the store).

export const App = () => {
  const store = createEventStore();
  // The token lives in this signal only — in memory, never persisted (FUR-17).
  const [client, setClient] = createSignal<AdminClient | null>(null);
  const [gateError, setGateError] = createSignal<string | null>(null);
  const [connecting, setConnecting] = createSignal(false);

  let disconnect: (() => void) | undefined;
  onCleanup(() => disconnect?.());

  const unlock = async (token: string) => {
    setConnecting(true);
    setGateError(null);
    const candidate = createAdminClient(token);
    try {
      // Probe the API before trusting the token; a 401 stays on the gate.
      await candidate.listSessions();
    } catch {
      setGateError("Token rejected — check MAESTRO_ADMIN_TOKEN.");
      setConnecting(false);
      return;
    }
    disconnect = connectEvents(token, store);
    setClient(() => candidate);
    setConnecting(false);
  };

  const route = useRoute();

  return (
    <Show
      when={client()}
      fallback={<TokenGate error={gateError()} busy={connecting()} onSubmit={unlock} />}
    >
      {(admin) => (
        <div class="app">
          <StatusBar store={store} />
          <main>
            <Show
              when={route().view === "session" ? route() : null}
              fallback={<SessionList store={store} />}
            >
              {(current) => {
                const r = current();
                return r.view === "session" ? (
                  <SessionDetail store={store} client={admin()} sessionId={r.sessionId} />
                ) : null;
              }}
            </Show>
          </main>
        </div>
      )}
    </Show>
  );
};
