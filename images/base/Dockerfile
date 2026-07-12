# Maestro base worker image — M1 minimal (FUR-20 dogfood).
#
# Everything a turn needs and nothing more: node:24-slim + git + a pinned
# claude-code CLI on PATH as `claude` (exactly what AgentContract.buildCommand
# invokes; version matches the contract verified in FUR-12). NO credentials
# are baked in — agent auth (CLAUDE_CODE_OAUTH_TOKEN / ANTHROPIC_API_KEY)
# arrives via the container environment at runtime, injected by WorkerRuntime.
# The fuller official image (pnpm, Python, non-root) lands in M2.14.
#
# Build:  docker build -t maestro/worker-base:m1 images/base
FROM node:24-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code@2.1.207

# WorkerRuntime passes the full command (`claude -p ...`) as argv; clear the
# node image's entrypoint so that command runs verbatim.
ENTRYPOINT []
CMD []
