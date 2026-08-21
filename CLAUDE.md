# CLAUDE.md

Orientation for Claude Code sessions in this repo. The fuller engineering
reference is `training/06-engineering-playbook.md` — read that for the repo
map, Neon layout, worktree rules, and commit discipline. This file covers the
things sessions most often get wrong.

## External Platform Access Policy (read before asking Priyesh to do anything)

**Never ask Priyesh to manually access, log into, check, copy information
from, or make changes inside an external platform until you have first
exhaustively checked whether an existing API, MCP server, CLI, integration,
credential, environment variable, service account, webhook, SDK, or other
programmatic connection is already available that lets you do it yourself.**

Default behaviour: **Discover → Authenticate → Use existing integration →
Execute → Verify.** Manual intervention is the last resort, not the default.

The registry of what exists is **`.claude/INTEGRATIONS.md`** — read it before
concluding any platform is unreachable.

### The discovery loop (do this, in order)

1. **`ToolSearch` for the platform name.** MCP tools are *deferred* — they are
   absent from your base tool list and must be loaded before they can be
   called. **A missing tool name is not evidence of missing access.** This is
   the number-one cause of a session wrongly falling back to "check the
   dashboard."
2. Check `.claude/INTEGRATIONS.md` for the documented route.
3. Check env var *presence* (`env | grep -o '^NAME'` — never print values).
4. Check CLI presence: `command -v gh vercel psql`.
5. Check for an existing client module in `core/` or `agents/` (§2 of the
   registry).
6. Try the documented fallback chain (§4) — do not stop at the first failure.
7. **Only now** ask, using the template below.

`python3 scripts/check_integrations.py` answers steps 1–4 in one shot without
spending API calls.

### Known-good routes (verified 2026-08-21)

| Need | Do this | Not this |
|---|---|---|
| Why a Vercel deploy failed | `mcp__Vercel__list_deployments` → `get_deployment_build_logs` / `get_runtime_errors` | "Open Vercel → Deployments and send me the logs" |
| Whether a lead reached the CRM | `mcp__HubSpot__search_crm_objects` | "Check HubSpot" |
| Review / create / comment on repo changes | `mcp__github__*` | "Open GitHub and..." |
| Ticket status or updates | `mcp__monday_com__*` | "Look at Monday" |
| Why a scheduled job failed | `mcp__github__actions_list` → `get_job_logs` | "Check the Actions tab" |
| Ad or CTV campaign performance | `mcp__Meta__*`, `mcp__Vibe__*` | "Pull the numbers from the UI" |
| P&L / invoices | `mcp__Intuit_QuickBooks__*` | "Export it from QuickBooks" |

### Cloud vs local sessions — the reason capability differs

Sessions on **claude.ai/code** run in a fresh container with **no project
credentials** (`.env` is gitignored and never cloned) and an org egress policy
that **403s every outbound host except GitHub**. So in a cloud session, MCP
connectors and GitHub are the *only* working routes — direct `requests` calls
to `api.hubapi.com`, `api.vercel.com`, `stats.ortb.net` or Neon **cannot**
work, no matter the token.

`CONNECT tunnel failed, response 403` means **org egress policy**, not a bad
credential. Do not retry it, do not route around it, do not report it as an
auth failure — switch to the MCP connector for that platform.

Local sessions on Priyesh's Mac have `.env`, open egress, and `gh`. Both
classes are legitimate; state which one you are in when reporting a limit.

### If manual intervention genuinely is required

Never say just "go into Vercel and...". Say:

1. **What you attempted** (specific tools/commands).
2. **How it failed** (exact error, and which class — egress policy, expired
   credential, insufficient scope, connector not enabled, no route exists).
3. **What capability is missing** (name the env var, scope, or connector).
4. **The minimal action** needed — one click or one value, precisely located.

### Safeguards — discovery is automatic, consequential actions are not

Use read and diagnostic access freely and without asking. **Confirm with
Priyesh first** for: production deploys and promotions; deleting data,
branches, or repos; DNS and domain changes; writing or rotating production env
vars; sending email or Slack to anyone outside the team; creating or modifying
CRM records that touch a live deal; financial actions (invoices, payments,
payroll, ad spend and budget changes); destructive or schema-altering database
operations; and anything that consumes paid credits at scale. Honour
`LL_DRY_RUN` and the `*_ENFORCE` / `*_AUTO_APPLY` flags rather than overriding
them.

### Before building any new integration

Grep `.claude/INTEGRATIONS.md` §5 and `core/` first. We already have duplicate
HubSpot, monday.com, Vercel, and Apollo clients. **Extend and document the
existing integration** — do not add another.

@.claude/INTEGRATIONS.md

---

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

The Vercel MCP connector covers deployments, build logs, runtime logs and
errors, projects, and analytics — reach for it first (see
`.claude/INTEGRATIONS.md`). Environment variables are the one gap: it exposes
`list_projects`, `get_deployment`, `get_runtime_logs`, `deploy_to_vercel` and
the rest, but **no env-var write tool**. Use the REST API helper for those:

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
