# Fake agent image for TurnExecutor integration tests (FUR-14).
# alpine/git ships sh + git without a network install step; the entrypoint is
# reset so the WorkerRuntime command (claude -p ...) executes our fake CLI.
# Runs non-root (uid 1000) to mirror the real worker-base image — claude
# refuses --dangerously-skip-permissions as root, so root workers are dead.
# Test suites that bind-mount host directories override the uid at run time
# (`docker run -u <test-uid>`, see fakeAgentRuntimeTemplate): on Linux hosts
# bind mounts preserve real ownership, and uid 1000 need not own them.
FROM alpine/git:latest
COPY claude /usr/local/bin/claude
RUN chmod +x /usr/local/bin/claude && adduser -D -u 1000 agent
USER agent
ENV HOME=/home/agent
ENTRYPOINT []
CMD []
