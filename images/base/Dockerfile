# Maestro official base worker image (FUR-34 / M2.14).
#
# Every turn runs inside a container built FROM this image (directly, or via
# a per-project extension — see README "Extending this image"). It ships what
# a turn needs: node + pnpm + git + python3 + a pinned claude-code CLI on PATH
# as `claude` (exactly what AgentContract.buildCommand invokes) — plus a
# pinned docker CLI *client* that no real turn uses, but that Maestro's own
# test suite needs to dogfood-build itself inside this image (see the docker
# block below). NO credentials are baked in — agent auth
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
       curl \
       python3 \
  && rm -rf /var/lib/apt/lists/*

# docker CLI *client only* (no daemon). Maestro's test suite exercises
# WorkerRuntime's local-cli runtime and the shared fake-agent harness, which
# shell out to `docker build`/`run`/`rm` against a daemon reached over a
# mounted socket — so the image must carry the client to build and test
# Maestro inside itself (the dogfood acceptance criterion, base-image.yml).
# Real turns never spawn nested containers and WorkerRuntime never mounts the
# socket into a worker, so in a real turn this binary is simply never invoked
# — it adds no capability or credential. We keep only the ~40 MB static
# client from Docker's official archive (the daemon binaries in the tarball
# are discarded); the buildx plugin is omitted, so `docker build` falls back
# to the daemon's classic build endpoint — enough for the trivial fake-agent
# image. Pinned deliberately, like every other tool here.
ARG DOCKER_CLI_VERSION=29.6.2
RUN arch="$(dpkg --print-architecture)" \
  && case "$arch" in \
       amd64) dockerArch=x86_64 ;; \
       arm64) dockerArch=aarch64 ;; \
       *) echo "unsupported architecture: $arch" >&2; exit 1 ;; \
     esac \
  && curl -fsSL "https://download.docker.com/linux/static/stable/${dockerArch}/docker-${DOCKER_CLI_VERSION}.tgz" \
       | tar -xz -C /usr/local/bin --strip-components=1 docker/docker \
  && docker --version

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
      org.opencontainers.image.description="Maestro official base worker image: node + pnpm + git + python3 + docker-cli + claude-code" \
      org.opencontainers.image.licenses="MIT" \
      maestro.claude-code-version="${CLAUDE_CODE_VERSION}"

# WorkerRuntime passes the full command (`claude -p ...`) as argv; clear the
# node image's entrypoint so that command runs verbatim.
ENTRYPOINT []
CMD []
