import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Testcontainers: image pull + container boot can be slow on first run.
    hookTimeout: 120_000,
    testTimeout: 60_000,
  },
});
