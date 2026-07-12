# Maestro

## Purpose

Maestro is a self-hosted AI coding-agent orchestrator for home labs and local development
environments. It receives work from ticketing systems (Linear first, generic REST API second),
normalizes every interaction into turn-based sessions, and executes each turn with the
claude-code CLI inside an isolated, ephemeral worker (local container runtime or Kubernetes
Job). The orchestrator owns all state (Postgres), all git operations (master clones, per-session
worktrees, push + PR creation with its own credentials), and serves an admin UI for inspection
and correction. Workers hold no credentials and never talk to git forges directly.

Ground truth documents (Linear project "Maestro"): **PRD — Self-Hosted AI Coding Agent
Orchestrator (v1.3.0)** and **Technical Requirements — Maestro**.

## Project structure

```
maestro/
├── apps/
│   ├── orchestrator/        # Effect v4 app: HTTP, queue, engine, git, db — the only deployable backend
│   │   └── src/
│   │       ├── main.ts      # THE single composition root (see Service conventions)
│   │       └── db/schema/   # Drizzle table definitions (only place Drizzle types may leak from is repos)
│   └── admin-ui/            # SolidJS + Vite SPA, statically served by the orchestrator
├── packages/
│   ├── domain/              # Entities, state machines, tagged errors, Schemas — PURE: no IO, only `effect`
│   └── api/                 # HttpApi contracts + SSE event schemas, shared orchestrator ↔ admin-ui
├── images/base/             # Official Maestro base worker image (Dockerfile)
├── charts/maestro/          # Helm chart
└── docker-compose.yml       # Local development stack
```

Dependency direction: `apps/*` → `packages/api` → `packages/domain`. Never the reverse.
`packages/domain` depends on `effect` only.

## Commands

| Command | What |
| --- | --- |
| `pnpm install` | Install workspace (pnpm 10; build scripts are allowlisted via `pnpm.onlyBuiltDependencies`) |
| `pnpm typecheck` | `tsc --noEmit` in every workspace package |
| `pnpm lint` | Biome check (lint + format verification) across the repo |
| `pnpm format` | Biome auto-fix (format + safe lint fixes) |
| `pnpm test` | Vitest in every workspace package (`--passWithNoTests`) |
| `pnpm --filter @maestro/orchestrator dev` | Run orchestrator with tsx watch |
| `pnpm --filter <pkg> test -- <file>` | Run a single package's tests |

All four gates (`install`, `typecheck`, `lint`, `test`) must pass before every commit. CI
(`.github/workflows/ci.yml`) runs the same gates on every PR and push to main.

## Toolchain decisions (pinned, deliberate)

- **`effect` is pinned exact** (`4.0.0-beta.97`, no `^`). It is a beta with real churn — upgrades
  are deliberate, reviewed commits, never implicit.
- **TypeScript is pinned to the 7.0 line** (`~7.0.2`, native). Adopted 2026-07 after an
  empirical trial: full typecheck + test suite green against `effect@4.0.0-beta.97`
  (`moduleResolution: bundler` + `allowImportingTsExtensions` behave identically), and the
  orchestrator's `tsc --noEmit` dropped from multi-second to ~0.2 s. The earlier `~5.9.3` pin
  (FUR-5: early TS7-native builds choked on the Effect v4 beta) is obsolete. Keep the `~` pin;
  minor/major bumps remain deliberate, re-verified commits.
- **Biome** (not eslint + prettier): one fast tool for lint + format. House style = Biome
  defaults (semicolons, double quotes), `lineWidth: 100`.
- **Node ≥ 24**, pnpm workspaces. Internal packages export TypeScript source directly
  (`"exports": { ".": "./src/index.ts" }`); `moduleResolution: "bundler"` +
  `allowImportingTsExtensions` make tsc, tsx, vitest, and Vite all consume it without a build
  step. Import paths inside the repo use explicit `.ts` extensions.

## Core Priorities

Performance first. Profile hot paths, avoid N+1 queries, use async properly for I/O-bound work, watch memory on large batches.

Reliability first. Keep behavior predictable under load and during failures. Handlers must be idempotent; replays must be safe.

If a tradeoff is required, choose correctness and robustness over short-term convenience.

## Maintainability

Long term maintainability is a core priority. If you add new functionality, first check if there is shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.

## Effect v4 conventions

**Naming churn warning:** the tech-requirements doc says `ServiceMap.Service`; in
`4.0.0-beta.97` that module is named **`Context`** (`Context.Service`). Same pattern, new name.
If an Effect API mentioned in a ticket doesn't exist, check the installed sources in
`node_modules/effect/src/` first — the rename probably reached us before the docs.

### Services

One service per file, file named after the service (`WorktreeManager.ts`). Pattern:

```ts
import { Context, Effect, Layer } from "effect";

export class WorktreeManager extends Context.Service<
  WorktreeManager,
  {
    readonly provision: (args: {...}) => Effect.Effect<Worktree, GitError>;
    readonly remove: (args: {...}) => Effect.Effect<void, GitError>;
  }
>()("maestro/git/WorktreeManager") {
  static readonly layer = Layer.effect(
    WorktreeManager,
    Effect.gen(function* () {
      // acquire dependencies via yield*
      return {
        provision: Effect.fn("WorktreeManager.provision")(function* (args) {
          // ...
        }),
        remove: Effect.fn("WorktreeManager.remove")(function* (args) {
          // ...
        }),
      };
    }),
  );
}
```

- **Key ids are namespaced** `"maestro/<area>/<Name>"` — e.g. `"maestro/git/GitCache"`,
  `"maestro/engine/TurnExecutor"`, `"maestro/db/SessionRepo"`. The string is the runtime
  identity; never reuse one.
- **Layers are statics on the service class**: `.layer` (live), `.layerTest` (in-memory/fake),
  plus named variants where a port has multiple implementations —
  `WorkerRuntime.layerLocalCli` / `WorkerRuntime.layerK8s`, `Ingest.layerLinear` /
  `Ingest.layerGenericApi`. Selection happens by config **at the composition root only**.
- **`main.ts` is the only composition root.** Nothing else imports layer implementations;
  consumers import the service class and yield it. Tests compose their own layers locally.
- **Every service method is wrapped in `Effect.fn("ServiceName.method")`** — this is the tracing
  instrumentation (one span per method call, exported via OTLP in M2). No naked
  `Effect.gen` methods on services.

### Errors

- Tagged schema errors live in **`packages/domain`**, class names suffixed `Error`, defined with
  `Schema.TaggedErrorClass`:

  ```ts
  export class WorktreeConflictError extends Schema.TaggedErrorClass<WorktreeConflictError>()(
    "WorktreeConflictError",
    { sessionId: SessionId, path: Schema.String },
  ) {}
  ```

- One error union per area (`GitError`, `RuntimeError`, `QueueError`, ...), exported from the
  area's error file.
- No `throw` in domain/service code; failures are typed Effect failures. `Effect.die` only for
  genuine defects (programmer error).

### Schemas

- `packages/domain` is the single source of truth for entity/state Schemas — Effect **Schema v4**
  (`Schema.Struct`, `Schema.check(...)` filters, `Schema.Literals` unions for states).
- Entities carry branded ids (`SessionId`, `TaskRunId`, ...). State machines are expressed as
  literal unions + an explicit transition table; repositories enforce transitions on write.
- `packages/api` defines the REST/admin API as `HttpApi` contracts (`effect/unstable/httpapi`)
  plus one discriminated union of SSE event schemas. The orchestrator implements the contract;
  admin-ui derives its typed client and SSE parser from the same definitions. No hand-written
  fetch types anywhere.

## Coding conventions

- TypeScript strict mode everywhere; base config also enables `exactOptionalPropertyTypes` and
  `noUncheckedIndexedAccess`. Don't weaken compiler options per-package.
- **No cross-package implementation imports.** Packages talk through their public entry point
  (`@maestro/domain`, `@maestro/api`) — never `@maestro/domain/src/...` deep imports.
- Drizzle stays inside `apps/orchestrator`: schema in `src/db/schema/`, queries only inside
  repository services (`maestro/db/*`). Repositories map rows ⇆ domain types; nothing outside a
  repo sees a Drizzle type.
- External dependencies (Drizzle, pg-boss, K8s client, forge SDKs, container runtimes) are
  wrapped as Effect services with `.layerTest` fakes — never imported directly by business logic.
- Comments explain *why*, not *what*. Keep them sparse and load-bearing.

## Naming

- Services / classes / Schemas: **PascalCase**. Methods / functions / variables: **camelCase**.
- Directories: **kebab-case**. Service files named after the service (`GitCache.ts`).
- Schema error classes end in `Error`; SSE event types are PascalCase nouns
  (`SessionStateChanged`).

## Testing

- **vitest + testcontainers.** State machine transitions, repository guards, and queue semantics
  are integration-tested against **real Postgres** (testcontainers) — not mocks.
- Unit tests are fine for pure domain logic (state tables, schema validation, prompt
  composition).
- Test files live in `test/` per package, named `*.test.ts`. Shared testcontainer setup lives in
  the orchestrator's test harness (started once per suite, cleaned between tests).
- Container-runtime integration uses the local CLI runtime with a trivial image; agent-contract
  tests use recorded stream-json fixtures, never live API calls.

## Git & workflow

- Commit messages: `FUR-<n>: <summary>` referencing the Linear ticket.
- `PROGRESS.md` at the repo root is the running build journal — one line per ticket (status,
  decisions, leftovers). Append, don't rewrite history.
- Never commit failing gates. A red build outranks all feature work.
