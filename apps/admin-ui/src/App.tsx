import { createSignal, onCleanup, Show } from "solid-js";
import { type AdminClient, createAdminClient } from "./api.ts";
import { createNotifier } from "./notifications.ts";
import { useRoute } from "./route.ts";
import { connectEvents } from "./sse.ts";
import { createEventStore } from "./store.ts";
import { clearToken, loadToken, saveToken } from "./token-storage.ts";
import { SessionDetail } from "./views/SessionDetail.tsx";
import { SessionList } from "./views/SessionList.tsx";
import { StatusBar } from "./views/StatusBar.tsx";
import { TokenGate } from "./views/TokenGate.tsx";

// Root component: admin-token gate, then the read-only debug surface driven by
// one app-wide SSE subscription (unfiltered — it carries sessions, runs, and
// log chunks; the views select what they need from the store).

export const App = () => {
  const store = createEventStore();
  // Optional browser notifications on turn start/completion — the notifier
  // opts in via localStorage and rides the store's run-change transitions.
  const notifier = createNotifier(store);
  store.setQueueListener(notifier.handleQueueChange);
  store.setRunListener(notifier.handleRunChange);
  // FUR-17 kept the token in this signal only, never persisted. Explicitly
  // overridden by user request: the token now round-trips through
  // localStorage (token-storage.ts) so refreshes and dev reloads auto-login.
  const [client, setClient] = createSignal<AdminClient | null>(null);
  const [gateError, setGateError] = createSignal<string | null>(null);
  const [connecting, setConnecting] = createSignal(false);

  let disconnect: (() => void) | undefined;
  onCleanup(() => disconnect?.());

  const logout = (message: string | null = null): void => {
    clearToken();
    disconnect?.();
    disconnect = undefined;
    setGateError(message);
    setClient(null);
  };

  const connect = (token: string): void => {
    // The SSE supervisor reconnects through outages on its own; this callback
    // only fires on a probe-confirmed 401 (orchestrator restarted with a new
    // MAESTRO_ADMIN_TOKEN) — clear the token and fall back to the gate.
    disconnect = connectEvents(token, store, () =>
      logout("Stored token rejected — enter the current MAESTRO_ADMIN_TOKEN."),
    );
    setClient(() => createAdminClient(token));
  };

  const unlock = async (token: string) => {
    setConnecting(true);
    setGateError(null);
    try {
      // Probe the API before trusting the token; a 401 stays on the gate.
      await createAdminClient(token).listSessions();
    } catch {
      setGateError("Token rejected — check MAESTRO_ADMIN_TOKEN.");
      setConnecting(false);
      return;
    }
    saveToken(token);
    connect(token);
    setConnecting(false);
  };

  // Auto-login: read storage synchronously during setup so a stored token
  // renders the app directly — no gate flash on refresh. If the token turned
  // stale, the SSE auth-reject path above drops back to the gate.
  const stored = loadToken();
  if (stored !== null) connect(stored);

  const route = useRoute();

  return (
    <Show
      when={client()}
      fallback={<TokenGate error={gateError()} busy={connecting()} onSubmit={unlock} />}
    >
      {(admin) => (
        <div class="app">
          <StatusBar store={store} notifier={notifier} onLogout={() => logout()} />
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
