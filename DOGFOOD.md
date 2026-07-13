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
| `MAESTRO_LINEAR_API_TOKEN` | **Recommended:** the Maestro OAuth app's app-actor token, so turn-result comments post under the app's own name/avatar instead of your personal account — see "Posting as the Maestro app identity" below. Quick start alternative: Linear → Settings → API → Personal API keys → create key (comments then appear authored by you). Either kind works unchanged; Maestro auto-detects which it is by the `lin_api_` prefix. |
| `MAESTRO_LINEAR_BOT_USER_ID` | The user id the API token belongs to (self-trigger guard). Personal key: `curl -s -H "Authorization: <token>" -H "Content-Type: application/json" -d '{"query":"{ viewer { id } }"}' https://api.linear.app/graphql`. App token: same query with `-H "Authorization: Bearer <token>"` — `viewer` resolves to the **app's own user**, whose id is exactly what comment webhooks carry, making this guard reliable again (see below). |
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

### Posting as the Maestro app identity (FUR-42)

With a personal API key, Maestro's callbacks appear authored by *you*. A Linear
OAuth application acting as itself (`app` actor) fixes that: comments post
under the app's own name and avatar, and the app has its own user id that
webhooks carry — so the `MAESTRO_LINEAR_BOT_USER_ID` guard becomes reliable
(the `**Maestro** —` content marker stays as the FUR-39 layer-1 defense).

Everything below is against Linear's current OAuth docs
(<https://linear.app/developers/oauth-2-0-authentication>) and the installed
`@linear/sdk` 88 (`accessToken` constructor option → `Authorization: Bearer`;
`apiKey` → raw header — verified in `parseClientOptions`).

**1. Create the OAuth app (workspace admin, one-time).** Linear → Settings →
API → OAuth applications → new application:

- Name **Maestro** + an avatar — this is the identity comments will show.
- Redirect callback URL: required by the form but never used on the
  client-credentials path below; a placeholder like
  `http://localhost:3000/oauth-unused` is fine.
- Enable the **client credentials tokens** toggle (available on create or
  edit). Keep the app private to the workspace (no public distribution).
- Copy the **client ID** and **client secret**.

**2. Mint the app-actor token (client-credentials grant — no browser, no
redirect catcher).** Linear supports `grant_type=client_credentials` for
server-to-server apps; the resulting token *is* an `app`-actor token:

```sh
curl -s https://api.linear.app/oauth/token \
  -u "$LINEAR_CLIENT_ID:$LINEAR_CLIENT_SECRET" \
  -d grant_type=client_credentials \
  -d "scope=read,write"
```

The response carries `access_token` (a plain hex string, no `lin_api_`
prefix), `token_type: Bearer`, and `expires_in: 2591999` (~30 days). There is
**no refresh token** on this grant — renewal is simply re-running the same
curl. Two documented sharp edges:

- Requesting a client-credentials token with a **different scope string
  revokes every existing app-actor token** for the app. Pick the scope once
  (`read,write` is all Maestro needs; add `app:mentionable` /
  `app:assignable` up front if you want the app @-mentionable/assignable) and
  reuse it verbatim on every renewal.
- Rotating the client secret also revokes all outstanding tokens.

Set `MAESTRO_LINEAR_API_TOKEN` to the `access_token`. No other config change:
Maestro auto-detects the missing `lin_api_` prefix and authenticates with a
`Bearer` header (`MAESTRO_LINEAR_TOKEN_KIND` exists only to force the choice
for legacy unprefixed personal keys).

*Why not the `actor=app` authorization-code flow?* It yields the same
app-actor identity but needs a one-time browser authorization against
`https://linear.app/oauth/authorize?...&actor=app` with a redirect catcher
for the `?code=` callback — and since Linear's April 1, 2026 migration those
access tokens **expire after 24 hours** with rotating refresh tokens, which
would force Maestro to persist and rotate refresh state just to post
comments. The client-credentials grant sidesteps both. (Full agent-platform
integration on top of `actor=app` is the M2 agent-delegation spike.)

**3. Fetch the app's user id and arm the guard.**

```sh
curl -s https://api.linear.app/graphql \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ viewer { id name } }"}'
```

With an app-actor token, `viewer` is the app user. Set
`MAESTRO_LINEAR_BOT_USER_ID` to that `id` and restart — comment webhooks for
Maestro's own posts carry this id as the comment's `userId`, so the id guard
drops them before the content-marker fallback is even consulted.

**4. Token rotation (deliberate M1 decision).** Long-lived token + manual
rotation, not an automated refresh flow: the client-credentials token lives
30 days, has no refresh token to manage, and renewal is one curl. When it
expires, comment posting fails per-post (`CallbackDeliveryError`) while the
outbox keeps retrying — mint a fresh token (same scope string!), update
`.env`, restart, and the pending comments deliver. If monthly rotation ever
grates, the cheap M2 upgrade is auto-re-minting via client credentials on
401, not the rotating-refresh-token flow.

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
   on shadept/maestro, result comment lands on the ticket — authored by the
   **Maestro app** if the FUR-42 app token is configured, and no self-triggered
   turn follows it (the id guard catches the app's own comment).
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
- "**Maestro** — Paused this session after 3 consecutive failures" comment
  (FUR-39 circuit breaker): the session stops accepting comment-triggered
  turns after 3 consecutive FAILED turns with no success in between.
  Diagnose via the failure comment / run logs, then resume by RE-APPLYING
  the trigger label to the issue (remove `maestro`, add it back) — that
  clears the breaker and queues a fresh turn from the ticket. One more
  failure right after a resume re-pauses immediately; only a completed turn
  resets the streak. Repeated identical failure comments are deduped: the
  same failure text posts once per session until the text changes.
