# Maestro official base worker image (FUR-34 / M2.14).
#
# Every turn runs inside a container built FROM this image (directly, or via
# a per-project extension — see README "Extending this image"). It ships
# exactly what a turn needs: node + pnpm + git + python3 + a pinned
# claude-code CLI on PATH as `claude` (exactly what AgentContract.buildCommand
# invokes). NO credentials are baked in — agent auth
# (CLAUDE_CODE_OAUTH_TOKEN / ANTHROPIC_API_KEY) arrives via the container
# environment at runtime, injected by WorkerRuntime. CLAUDE_CONFIG_DIR is
# injected the same way, as an identity bind-mount at a host-absolute path
# (apps/orchestrator/src/storage/paths.ts) created by the orchestrator before
# the container starts — the image needs no baked-in default for it.
#
# Versioning: the published tag tracks CLAUDE_CODE_VERSION below
# (ghcr.io/shadept/maestro-worker-base:<claude-code-version> + :latest) —
# bumping this ARG's default *is* the release; see
# .github/workflows/base-image.yml, which reads it straight out of this file.
#
# Build:  docker build -t maestro/worker-base:local images/base
FROM node:24-slim

ARG CLAUDE_CODE_VERSION=2.1.207

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
       git \
       ca-certificates \
       python3 \
  && rm -rf /var/lib/apt/lists/*

# pnpm via corepack (bundled with Node) — pinned to the workspace's
# `packageManager` field (package.json) so a project's own `pnpm install`
# behaves identically inside the image and on a contributor's machine.
RUN corepack enable && corepack prepare pnpm@10.32.1 --activate

RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}

# claude hard-refuses --dangerously-skip-permissions when running as root, so
# the worker MUST be non-root: use the base image's `node` user (uid 1000).
# HOME must be a writable dir for that user — claude keeps per-session state
# in CLAUDE_CONFIG_DIR (injected by the orchestrator) but still touches ~.
# The identity mounts (worktree, bare repo, config dir) are host-owned; git
# would refuse them as "dubious ownership" for a non-owning uid, so trust is
# granted globally — each worker is single-session and ephemeral.
USER node
ENV HOME=/home/node
RUN git config --global safe.directory '*'

LABEL org.opencontainers.image.source="https://github.com/shadept/maestro" \
      org.opencontainers.image.description="Maestro official base worker image: node + pnpm + git + python3 + claude-code" \
      org.opencontainers.image.licenses="MIT" \
      maestro.claude-code-version="${CLAUDE_CODE_VERSION}"

# WorkerRuntime passes the full command (`claude -p ...`) as argv; clear the
# node image's entrypoint so that command runs verbatim.
ENTRYPOINT []
CMD []
