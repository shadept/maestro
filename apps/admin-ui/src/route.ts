import { SessionId } from "@maestro/domain";
import { Option, Schema } from "effect";
import { createSignal, onCleanup } from "solid-js";

// Hash-based routing (DECISION): every document request hits `/`, so the
// orchestrator's static route needs no SPA fallback and unknown file paths
// stay honest 404s. Two routes only — the M1 read surface is small.

export type Route = { view: "sessions" } | { view: "session"; sessionId: SessionId };

const decodeSessionId = Schema.decodeUnknownOption(SessionId);

const parse = (hash: string): Route => {
  const match = /^#\/sessions\/(.+)$/.exec(hash);
  if (match !== null) {
    const sessionId = decodeSessionId(match[1]);
    if (Option.isSome(sessionId)) return { view: "session", sessionId: sessionId.value };
  }
  return { view: "sessions" };
};

export const sessionHref = (sessionId: SessionId): string => `#/sessions/${sessionId}`;

export const useRoute = (): (() => Route) => {
  const [route, setRoute] = createSignal<Route>(parse(window.location.hash));
  const onHashChange = () => setRoute(parse(window.location.hash));
  window.addEventListener("hashchange", onHashChange);
  onCleanup(() => window.removeEventListener("hashchange", onHashChange));
  return route;
};
