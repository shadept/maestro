# Maestro base worker image — M1 minimal (FUR-20 dogfood).
#
# Everything a turn needs and nothing more: node:24-slim + git + a pinned
# claude-code CLI on PATH as `claude` (exactly what AgentContract.buildCommand
# invokes; version matches the contract verified in FUR-12). NO credentials
# are baked in — agent auth (CLAUDE_CODE_OAUTH_TOKEN / ANTHROPIC_API_KEY)
# arrives via the container environment at runtime, injected by WorkerRuntime.
# The fuller official image (pnpm, Python) lands in M2.14.
#
# Build:  docker build -t maestro/worker-base:m1 images/base
FROM node:24-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code@2.1.207

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

# WorkerRuntime passes the full command (`claude -p ...`) as argv; clear the
# node image's entrypoint so that command runs verbatim.
ENTRYPOINT []
CMD []
