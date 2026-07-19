# Maestro official base worker image

Every turn runs inside a container built from this image (directly, or via a
per-project extension — see "Extending this image" below). Contents:
`node:24-slim` (Debian-slim, Node LTS) + git + pnpm (via corepack, pinned to
the workspace's `packageManager`) + python3 + a pinned
`@anthropic-ai/claude-code` installed globally, so `claude` is on PATH — the
command `AgentContract` builds. Runs as the non-root `node` user (uid 1000);
no credentials are baked in — agent auth (`CLAUDE_CODE_OAUTH_TOKEN` /
`ANTHROPIC_API_KEY`) arrives via the container environment at runtime,
injected by `WorkerRuntime`.

```sh
docker build -t maestro/worker-base:local images/base
```

## Published image

CI (`.github/workflows/base-image.yml`) builds this Dockerfile, dogfoods it
(builds + tests Maestro's own repo inside the freshly-built image — proves
the base can work Maestro tickets), and on `main` publishes to GitHub
Container Registry:

```
ghcr.io/shadept/maestro-worker-base:latest
ghcr.io/shadept/maestro-worker-base:<claude-code-version>   # e.g. :2.1.207
```

**Versioning policy: a claude-code version bump *is* the image release.**
Bump the `ARG CLAUDE_CODE_VERSION` default in the `Dockerfile`, land it on
`main`, and CI publishes the new version tag + moves `latest`. There is no
independent image version counter to keep in sync.

Point the orchestrator at either tag with `MAESTRO_WORKER_IMAGE` (defaults to
`ghcr.io/shadept/maestro-worker-base:latest`, see `.env.example`); or build
`maestro/worker-base:local` above for iterating on the Dockerfile itself.

Image size is reported by the CI job's "Report image size" step
(`docker images`) on every build — check the latest `base-image` workflow run
for the current figure. It is expected to stay a modest multiple of
`node:24-slim` (git, pnpm, python3, and one npm-installed CLI add comparably
little); if a change balloons it, that's a signal something pulled in more
than the spec calls for.

## CLAUDE_CONFIG_DIR

The image does not hardcode a `CLAUDE_CONFIG_DIR` or pre-create one. The
orchestrator creates the per-session config directory on the host
(`apps/orchestrator/src/storage/paths.ts`, `sessionConfigDir`) and identity
bind-mounts it into the worker at that same host-absolute path, then sets
`CLAUDE_CONFIG_DIR` to match in the container environment
(`AgentContract.buildCommand`). Nothing in the image needs to anticipate the
path — it only needs `HOME` writable for the `node` user, which is already
set.

## Extending this image

Per-project images add only the toolchains that specific project's tickets
need, on top of this base — install as root, then drop back to the `node`
user (every turn's worker runs as it; `WorkerRuntime` doesn't override `USER`
at run time):

```dockerfile
FROM ghcr.io/shadept/maestro-worker-base:latest

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends <toolchain-packages> \
  && rm -rf /var/lib/apt/lists/*
# e.g. a Rust project: curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
#      a Go project:   downloading + extracting the Go toolchain tarball

USER node
```

Point `MAESTRO_WORKER_IMAGE` at the extended image's tag; everything else
(non-root user, `CLAUDE_CONFIG_DIR` injection, credential-free image) is
inherited unchanged from the base.
