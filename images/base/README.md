# Maestro base worker image

M1 minimal worker image (FUR-20): `node:24-slim` + git + pinned
`@anthropic-ai/claude-code` globally installed, so `claude` is on PATH — the
command AgentContract builds. No credentials are baked in; agent auth arrives
via the container environment at runtime.

```sh
docker build -t maestro/worker-base:m1 images/base
```

Point the orchestrator at it with `MAESTRO_WORKER_IMAGE=maestro/worker-base:m1`.

The fuller official image (pnpm, Python toolchain, non-root user) lands in
M2.14.
