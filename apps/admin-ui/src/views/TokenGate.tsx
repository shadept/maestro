import { createSignal, Show } from "solid-js";

// Admin-token entry. The token is handed straight to the parent, which
// verifies it and persists it to localStorage (token-storage.ts) — FUR-17's
// "in memory only" rule was explicitly overridden by user request so the UI
// auto-logs-in across refreshes and dev reloads.

export const TokenGate = (props: {
  error: string | null;
  busy: boolean;
  onSubmit: (token: string) => void;
}) => {
  const [token, setToken] = createSignal("");

  return (
    <div class="token-gate">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (token().length > 0) props.onSubmit(token());
        }}
      >
        <h1>Maestro</h1>
        <p>Enter the admin token to inspect sessions, runs, and live logs.</p>
        <input
          type="password"
          placeholder="MAESTRO_ADMIN_TOKEN"
          value={token()}
          onInput={(event) => setToken(event.currentTarget.value)}
          autofocus
        />
        <button type="submit" disabled={props.busy || token().length === 0}>
          {props.busy ? "Checking…" : "Unlock"}
        </button>
        <Show when={props.error}>
          <p class="error">{props.error}</p>
        </Show>
      </form>
    </div>
  );
};
