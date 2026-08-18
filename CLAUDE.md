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
  rotate a real key unless it was supplied for that session — see "Cloud
  sessions and credentials" below.

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

## Cloud sessions and credentials

A cloud session cannot reach a secret unless it is in the environment
configuration, and **that configuration is not a secrets store.** Anthropic's
docs are explicit on both points:

> Anyone who uses the environment can read the values, and cloud environments
> have no dedicated secrets store, so don't add API keys or other credentials.

and, in the carries-over table: *Static API tokens and credentials — available
in cloud sessions: **No** — no dedicated secrets store exists yet.* The
environment dialog itself warns that values are visible to anyone using the
environment.

So there is no clean way to give cloud sessions standing credentials today.
Pick per credential:

- **Default — supply it per session.** Paste or `export` the value when a
  session actually needs it. Costs a step each time; leaves nothing at rest.
- **Environment variable, eyes open.** Technically works: values are injected
  as ordinary env vars at session startup. But they sit in plaintext config
  that every user of the environment can read. Only for tokens whose blast
  radius you accept, scoped as narrowly as the provider allows, and rotated on
  offboarding per the playbook's secret-handling rules. A Vercel token is
  broad — it can write env vars across every project on the team — so weigh it
  accordingly rather than treating it as the obvious default.

Never commit a credential to this repo to get it into a session. `.env` is
gitignored for a reason, and the playbook records a real leak (2026-07-02) from
exactly that shortcut.

### Where the environment variables live

There is no settings page and no direct URL — only the selector:

1. At claude.ai/code, click the cloud icon showing the environment name, in the
   row above the message box (PGAM's is `PGAM`).
2. Hover the environment in the **Cloud** section, click the gear icon.
3. Use the **Environment variables** box: `.env` format, one `KEY=value` per
   line. Quote any value containing `#` or spanning lines — unquoted, `#`
   starts a comment.

Each session copies the values **once at startup**, so edits reach only
sessions started afterwards; a running session keeps what it began with.

Docs: https://code.claude.com/docs/en/cloud-environments#set-environment-variables
