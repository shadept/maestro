# Fake agent image for TurnExecutor integration tests (FUR-14).
# alpine/git ships sh + git without a network install step; the entrypoint is
# reset so the WorkerRuntime command (claude -p ...) executes our fake CLI.
FROM alpine/git:latest
COPY claude /usr/local/bin/claude
RUN chmod +x /usr/local/bin/claude
ENTRYPOINT []
CMD []
