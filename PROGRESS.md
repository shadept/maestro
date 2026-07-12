# Maestro build journal

One line per ticket: status, decisions made, anything left behind.

- FUR-5 (M1.1, monorepo scaffold + CLAUDE.md): DONE — pnpm workspace (orchestrator, admin-ui, domain, api + image/chart placeholders); Biome chosen over eslint+prettier (single fast tool, defaults as house style); TypeScript pinned ~5.9.3 (TS7 native too fresh for Effect v4 beta inference); effect pinned exact 4.0.0-beta.97 — discovered `ServiceMap` module is renamed `Context` in this beta, documented in CLAUDE.md; internal packages export TS source (no build step); CI = typecheck+lint+test on PR & main push. Left behind: nothing.
