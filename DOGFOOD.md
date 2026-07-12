# FUR-20 dogfood runbook — E2E live run

The ordered checklist for the first real end-to-end run: Maestro working a
Linear ticket on its own repository. Local prep (everything that needs no real
credentials) is already done and verified — see "Already prepared" below. What
follows are the manual steps that need real secrets and a reachable webhook.

## Already prepared (verified live with fakes)

- `docker-compose.yml` Postgres 18 stack is **up** (`maestro-postgres`,
  port 5432, persistent volume `maestro-pgdata`) and **migrated**
  (`pnpm --filter @maestro/orchestrator db:migrate`).
- The real project is **registered**: team key `FUR` →
  `https://github.com/shadept/maestro`, base branch `main`.
- Worker base image **built**: `maestro/worker-base:m1` (node:24-slim + git +
  claude-code 2.1.207 on PATH as `claude`; no credentials baked in).
- Full pipeline smoke-tested with the fake agent against a throwaway file://
  project: signed webhook 200 → session + turn created → container executed →
  logs streamed (SSE + persisted) → run COMPLETED → admin UI served at `/`.
  Throwaway data was deleted; only the FUR project row remains.
- A skeleton `.env` exists at the repo root (gitignored) with working local
  values; the real credentials below are commented placeholders in it.

## 1. Stack up (if not already running)

```sh
docker compose up -d --wait
pnpm --filter @maestro/orchestrator db:migrate   # idempotent
```

## 2. Fill in real credentials in `.env`

| Variable | Where to get it |
| --- | --- |
| `MAESTRO_LINEAR_WEBHOOK_SECRET` | Linear → Settings → API → Webhooks → create the webhook (step 3). The signing secret is shown on the webhook's settings page. |
| `MAESTRO_LINEAR_API_TOKEN` | Linear → Settings → API → Personal API keys → create key. Used for posting turn-result comments. |
| `MAESTRO_LINEAR_BOT_USER_ID` | The user id the API token belongs to (self-trigger guard). Query it: `curl -s -H "Authorization: <token>" -H "Content-Type: application/json" -d '{"query":"{ viewer { id } }"}' https://api.linear.app/graphql` |
| `MAESTRO_GITHUB_TOKEN` | GitHub → Settings → Developer settings → tokens. Classic PAT with `repo` scope, or fine-grained token on `shadept/maestro` with **Contents: read/write** and **Pull requests: read/write** (push branches + open draft PRs). |
| `CLAUDE_CODE_OAUTH_TOKEN` | Run `claude setup-token` on any machine with an authenticated Claude Code subscription; paste the long-lived token. (`ANTHROPIC_API_KEY` is the fallback.) |

Also confirm in `.env`:

- `MAESTRO_STORAGE_ROOT` — must be a directory Docker Desktop can bind-mount
  (worktrees are identity-mounted into workers). The prepared default
  `/Users/shade/.maestro/storage` is fine.
- `MAESTRO_WORKER_IMAGE=maestro/worker-base:m1` (rebuild any time with
  `docker build -t maestro/worker-base:m1 images/base`).

Create the trigger label in Linear: team **FUR** → labels → add `maestro`
(or set `MAESTRO_LINEAR_TRIGGER_LABEL` to an existing label).

## 3. Tunnel + Linear webhook

The webhook must reach `POST /api/webhooks/linear` on port 3000:

```sh
# cloudflared (no account needed for a quick tunnel):
cloudflared tunnel --url http://localhost:3000
# or ngrok:
ngrok http 3000
```

In Linear → Settings → API → Webhooks → New webhook:

- URL: `https://<tunnel-host>/api/webhooks/linear`
- Team: FUR (or workspace-wide)
- Event types: **Issues** and **Comments** (label changes arrive as Issue
  update events)
- Copy the signing secret into `MAESTRO_LINEAR_WEBHOOK_SECRET`, restart the
  orchestrator after editing `.env`.

Note: Linear enforces a ~60 s `webhookTimestamp` replay window — keep the
orchestrator's clock sane.

## 4. Start the orchestrator (on the host, not in compose)

```sh
pnpm --filter @maestro/orchestrator dev
```

Sanity: `curl localhost:3000/livez` and `curl localhost:3000/readyz` both `ok`;
`open http://localhost:3000/` and paste `MAESTRO_ADMIN_TOKEN` — the admin UI
bundle is served by the orchestrator (rebuild it with
`pnpm --filter @maestro/admin-ui build` if you change the UI).

## 5. The observation walk

1. Create a small, real ticket in team FUR (something claude-code can finish
   in one turn — a doc tweak, a tiny refactor with clear acceptance).
2. Apply the `maestro` label. Watch the admin UI: session appears, turn goes
   PENDING → PROVISIONING → EXECUTING with live logs.
3. On completion: session branch `maestro/FUR-<n>` pushed, **draft PR** opened
   on shadept/maestro, result comment lands on the ticket.
4. Comment on the ticket (as a human, not the bot user) — turn 2 must start
   and **resume** the same claude session (`--resume <uuid>`; check the
   session's `claudeSessionUuid` stays constant and the agent has context).
5. Review + merge the PR, then move the ticket to Done — verify teardown:
   session TERMINATED, worktree and session config dir removed from
   `MAESTRO_STORAGE_ROOT`, queued turns cancelled.
6. Acceptance: merged PR authored by Maestro via the ticket flow; resumption
   verified; teardown verified; every failure fixed or ticketed.

## Known watch item (FUR-12 OPEN QUESTION)

Subscription-token renewal in headless containers: `CLAUDE_CODE_OAUTH_TOKEN`
works for cold non-interactive runs, but the first live run may hit renewal
behavior we have not observed (long-lived setup-token vs. session token
expiry mid-turn). If a turn fails with an auth error after previously
working, capture the worker logs before retrying — that is the data the
open question needs. Fallback: set `ANTHROPIC_API_KEY` instead.

## Troubleshooting

- Webhook 401: secret mismatch or missing `linear-signature` — check the
  tunnel forwards headers unmodified.
- Webhook 200 `{"outcome":"Ignored","reason":"no project registered..."}`:
  team key mismatch — re-run the register script (idempotent):
  `pnpm --filter @maestro/orchestrator register-project --repo-git-url https://github.com/shadept/maestro --linear-team-key FUR --base-branch main`
- Turn FAILED with "publishing failed": `MAESTRO_GITHUB_TOKEN` missing or
  under-scoped. The commit is preserved in the worktree; fix the token and
  comment on the ticket to trigger a follow-up turn.
- Result comment never lands but the PR exists: `MAESTRO_LINEAR_API_TOKEN`
  missing/invalid — the outbox retries with backoff, so fixing the token and
  restarting delivers the pending comment.
