# CLAUDE.md

Orientation for Claude Code sessions in this repo. The fuller engineering
reference is `training/06-engineering-playbook.md` — read that for the repo
map, Neon layout, worktree rules, and commit discipline. This file covers the
things sessions most often get wrong.

## Deploy target: Render, not Vercel

`pgam-intelligence` deploys as a **Render worker** (`render.yaml`, `Procfile`:
`worker: python scheduler.py`). Its env vars are set in the **Render**
dashboard (Environment → Add Environment Variable), and `render.yaml` declares
them with `sync: false`, meaning the value is entered manually there.

Everything in `.env.example` — `LL_*`, `TB_*`, `MSN_*`, `PGAM_*`,
`MONDAY_API_TOKEN`, `SLACK_WEBHOOK`, `PUBMATIC_ACTIVATE_*` — belongs to
**Render**. Do not send a user to Vercel for these.

The Vercel projects (`pgam-www`, `pgam-direct-web`, `healthnation-web`,
`boxingnews`, `destination-com`, `attention-engine`, `adviceguru`,
`closer-web`, `attune-tv-ads`, …) are **separate repos**. See the repo map in
the playbook.

## Secrets are not present in this repo, ever

- `.env` is gitignored; `.env.example` holds names with empty placeholder values.
- Prod secrets live in Vercel env, Render env, or a local `.env` on Priyesh's
  machine (playbook: "`.env` files stay local, never committed").
- **Cloud sessions (claude.ai/code) start from a fresh clone and therefore
  have zero project credentials.** A cloud session cannot read, verify, or
  rotate a real key unless it was injected into the environment (below).

Do not claim to have a credential without checking `env` first.

## Setting Vercel env vars from a session

The Vercel MCP connector is **read-only for environment variables** — it
exposes `list_projects`, `get_deployment`, `get_runtime_logs`,
`deploy_to_vercel`, etc., but no env-var write tool. Use the REST API helper
instead:

```bash
export VERCEL_TOKEN=...            # vercel.com/account/settings/tokens

python3 scripts/vercel_env_sync.py --list-projects
python3 scripts/vercel_env_sync.py --project pgam-www --list
python3 scripts/vercel_env_sync.py --project pgam-www \
    --env-file ~/Desktop/pgam-www/.env.production --dry-run
python3 scripts/vercel_env_sync.py --project pgam-www \
    --env-file ~/Desktop/pgam-www/.env.production --target production
```

Notes:
- Upserts by default, so re-running is safe.
- Values are never printed — only key names.
- `NEXT_PUBLIC_*` is forced to `plain` (it is inlined into the client bundle,
  so marking it secret is misleading).
- Vercel bakes env vars in at **build** time. Always redeploy after a sync.
- Team id defaults to PGAM's (`team_8j7qA4FwBXkobcMfdhJj1umB`); override with
  `--team` or `$VERCEL_TEAM_ID`.

## Making credentials available to every cloud session

To stop re-supplying secrets each session, add them once as **environment
variables on the Claude Code environment** (claude.ai/code → environment
settings). They are then injected into every future session container and
readable as ordinary env vars. `VERCEL_TOKEN` is the highest-value one: with
it present, any session can run the sync script above unattended.

Treat that as granting standing access — scope tokens narrowly and rotate on
offboarding, per the playbook's secret-handling rules.
Docs: https://code.claude.com/docs/en/claude-code-on-the-web

## Do not leave self-re-arming PR check-in loops running

Investigated 2026-08-20 after a ~$300 Claude bill. The cost was **not** the
Render scheduler, the GitHub Actions crons, or the repo's `ANTHROPIC_API_KEY`
usage. It was `send_later` self-check-in routines that babysit draft PRs.

The failure mode: the routine fires into a **persistent session**, so every
wake re-reads that session's whole accumulated history, and the history only
grows. Three sessions racked up 159M cache-read tokens to produce 410k output
tokens — roughly 390 tokens of context re-read per token of real output. Each
wake found nothing changed, re-armed, and billed again.

Nothing was reviewing those PRs. All nine open PRs were self-authored drafts
with zero comments and zero reviews, so the loops could never terminate on
their own.

Rules:

- **Do not arm a recurring check-in on a PR that is already green and has no
  reviewer.** There is no event to wait for. Report the state and stop.
- If a check-in is genuinely warranted (waiting on real CI, a live reviewer),
  cap it — a couple of wakes, not an open-ended re-arm — and prefer a **fresh
  session per fire** over binding to a long-lived one, so context does not
  compound.
- Audit with `list_triggers` before adding another. Routines whose session was
  reclaimed show `ended_reason: auto_disabled_session_gone`; enabled ones with
  no live PR to watch should be deleted.
- Cost is visible per session via `list_sessions`
  (`external_metadata.usage.cost_usd`) — check it before assuming a scheduled
  job is to blame.

## Never inline a connection string in a routine prompt

The weekly `Hasib trigger check` routine stored two live Neon connection
strings, passwords included, in plaintext in its prompt. Routine configs are
not a secret store and are not covered by the rules above about `.env`.

Put the values in the Claude Code environment (previous section) and have the
prompt assert presence without printing them:

```bash
for v in PGAM_DIRECT_DATABASE_URL BOXINGNEWS_DATABASE_URL; do
  [ -n "${!v}" ] && echo "$v: present" || echo "$v: MISSING"
done
```

Moving a secret does not un-expose it — rotate anything that was ever stored
this way.
