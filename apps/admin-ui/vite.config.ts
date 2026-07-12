/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import solid from "vite-plugin-solid";

export default defineConfig({
  plugins: [solid()],
  server: {
    // Dev-mode convenience: the Vite dev server proxies API + SSE calls to a
    // locally running orchestrator. Production serves the built bundle from
    // the orchestrator itself, same-origin — no proxy involved.
    proxy: {
      "/api": { target: "http://localhost:3000" },
    },
  },
  test: {
    // The SSE store is plain Solid reactivity — no DOM required. The browser
    // resolve conditions make vitest load solid's client build so signals
    // behave exactly as they do in the bundle (the node/ssr build disables
    // reactive propagation).
    environment: "node",
  },
  resolve: {
    conditions: ["browser", "development"],
  },
});
