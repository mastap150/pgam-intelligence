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
