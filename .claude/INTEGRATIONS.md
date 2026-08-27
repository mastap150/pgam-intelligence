# Integration Capability Registry

**Purpose.** Tell every Claude Code session what programmatic access already
exists, so no session ever asks Priyesh to open a dashboard for something it
can do itself. Paired with the **External Platform Access Policy** in
`CLAUDE.md` — read that first.

**No secrets here.** This file records *that* a credential exists and *how to
reach it*. Never the value. Never paste a token into this file.

Last verified: 2026-08-21 (live probes from a cloud session).

---

## 0. First: which session class are you in?

Capability differs sharply between the two. Establish this before concluding
anything is unavailable.

```bash
# Cloud session (claude.ai/code) or local (Priyesh's Mac)?
echo "${CLAUDE_CODE_REMOTE:-local}"          # "true" => cloud
ls .env >/dev/null 2>&1 && echo "has .env" || echo "no .env"
```

| | **Cloud session** (claude.ai/code) | **Local session** (Priyesh's Mac) |
|---|---|---|
| MCP connectors | ✅ Primary and often *only* route | ✅ Available |
| GitHub (MCP + git HTTPS) | ✅ Works | ✅ Works (+ `gh` CLI) |
| Project `.env` credentials | ❌ **None.** Fresh clone, `.env` gitignored | ✅ Full set |
| Direct API egress (non-GitHub) | ❌ **403 by org egress policy** | ✅ Open |
| `psql` → Neon | Binary present, but host blocked + no DSN | ✅ Works |
| `gh` / `vercel` / `aws` / `gcloud` CLI | ❌ Not installed | `gh` ✅, others verify |

**Cloud rule of thumb: MCP or GitHub, or it doesn't happen.** A `requests.get`
to `api.hubapi.com` from a cloud session returns `CONNECT tunnel failed, 403`.
That is org policy, not a broken token — do not retry, do not route around it,
and do not conclude the integration doesn't exist. Use the MCP connector.

---

## 1. MCP connectors — verified live

These are **account-scoped** (claude.ai connector settings), not repo-scoped.
`/root/.claude.json` correctly shows `mcpServers: []` for this project — that
is *not* evidence of no access.

> ⚠️ **All MCP tools are deferred.** They are absent from the base tool list
> and must be loaded with `ToolSearch` before they can be called. A session
> that skips `ToolSearch` will wrongly conclude it has no HubSpot/Vercel/etc.
> access. **This is the single most common cause of a session falling back to
> "please check the dashboard."** Always `ToolSearch` first.

| Platform | Account / scope verified | Read | Write | Preferred | Fallback | Verify with |
|---|---|---|---|---|---|---|
| **GitHub** | `mastap150` (PGAMDSP), id 103131000 | ✅ | ✅ | `mcp__github__*` | `git` over HTTPS (creds pre-injected) | `mcp__github__get_me` |
| **Vercel** | team `team_8j7qA4FwBXkobcMfdhJj1umB` — 17 projects | ✅ deployments, build logs, runtime logs/errors, analytics, projects | ⚠️ deploy, pause, protection. **No env-var write** | `mcp__Vercel__*` | `scripts/vercel_env_sync.py` (local only, needs `VERCEL_TOKEN`) | `mcp__Vercel__list_teams` |
| **HubSpot** | portal `21341543`, owner `142657676` | ✅ contacts, companies, deals, tickets, tasks, line items, calls, emails, notes, products, quotes, invoices, subscriptions, lists | ✅ contacts, companies, deals, tickets, tasks, line items, calls, emails, notes, products, meetings | `mcp__HubSpot__*` | `agents/outbound/sdr_agent.py` (local only) | `mcp__HubSpot__get_user_details` |
| **monday.com** | user `36157998`, Pro, 6 seats | ✅ | ✅ | `mcp__monday_com__*` | `scripts/monday_cli.py` (local only) | `mcp__monday_com__get_user_context` |
| **Apollo.io** | connected | ✅ | ✅ (credit-consuming — surface `mcp_credits`) | `mcp__Apollo_io__*` | `APOLLO_API_KEY` in `sdr_agent.py` (local) | `mcp__Apollo_io__apollo_users_api_profile` |
| **Gmail** | `ppatel@pgammedia.com` | ✅ | ⚠️ send/draft — **confirm before sending** | `mcp__Gmail__*` | SendGrid (`SENDGRID_KEY`, local) | `mcp__Gmail__list_labels` |
| **Meta Ads** | connected | ✅ insights, catalogs, audiences | ⚠️ campaigns, budgets, creatives — **spend-affecting** | `mcp__Meta__*` | — | `mcp__Meta__ads_get_ad_accounts` |
| **Intuit QuickBooks** | connected (authless) | ✅ P&L, balance sheet, AR/AP aging | ⚠️ invoices, customers, payroll — **financial** | `mcp__Intuit_QuickBooks__*` | — | `mcp__Intuit_QuickBooks__company_info` |
| **Vibe** (CTV DSP) | connected | ✅ campaigns, strategies, metrics | ⚠️ create/publish campaigns — **spend-affecting** | `mcp__Vibe__*` | — | `mcp__Vibe__list_advertisers` |
| **Higgsfield** | connected | ✅ | ⚠️ generation consumes credits | `mcp__Higgsfield_MCP__*` | — | `mcp__Higgsfield_MCP__balance` |

### HubSpot objects needing re-auth (not missing — degraded)

`FORM`, `USER`, `CAMPAIGN`, `SITE_PAGE`, `BLOG_POST`, `MARKETING_EMAIL`,
`LANDING_PAGE`, `MARKETING_EVENT` return `REQUIRES_REAUTHORIZATION`.
`PARTNER_CLIENT` returns `REQUIRES_ACCOUNT_MODIFICATION`. If a task needs
these, say so precisely and ask Priyesh to re-authorize the HubSpot connector
— that *is* a legitimate manual ask, because no programmatic route exists.

### Installed but NOT connected

Asana, Atlassian, Box, Canva, Figma, Intercom, Linear, Notion, **Stripe**.
Tools will not be present. Connecting them is a one-click action in claude.ai
connector settings — a legitimate manual ask, stated as such.

### Vercel project inventory

`destination-com`, `pgam-dsp-dashboard-chdn`, `boxingnews`,
`attention-engine-8838f8be`, `adviceguru`, `healthnation-web`,
`spectaagenticmediaplan`, `pgam-website`, `pgam-www`, `attention-engine`,
`pgam-direct-web`, `attune-tv-ads`, `destination-app-web`, `dst-app-web`,
`web`, `closer-web`, `pgam-direct`.

Note `pgam-website` vs `pgam-www`, and `destination-app-web` vs `dst-app-web`
— confirm which is live before acting on either pair.

---

## 2. Code-level integrations (this repo)

All require credentials from a local `.env`, and all target hosts that are
**egress-blocked in cloud sessions**. Treat this table as "local session or
GitHub Actions only."

| Platform | Module | Env vars (names only) | Notes |
|---|---|---|---|
| **LL / Limelight** (stats) | `core/api.py`, `core/ll_data.py`, `core/ll_report.py` | `LL_API_BASE_URL`, `LL_CLIENT_KEY`, `LL_SECRET_KEY` | Default base `stats.ortb.net/v1/stats`. ⚠️ Keys fall back to `TB_*` — see §5. `WINS==0` backfill workaround lives in `_patch_zero_wins_rows`. |
| **LL management** | `core/ll_mgmt.py`, `core/ui_nav.py` | `LL_UI_EMAIL`, `LL_UI_PASSWORD`, `LL_DRY_RUN`, `LL_MARGIN_MIN` | Playwright UI automation. Honour `LL_DRY_RUN`. |
| **TB / Teqblaze SSP** | `core/tb_api.py`, `tb_data.py`, `tb_mgmt.py`, `tb_ledger.py` | `TB_API_BASE_URL`, `TB_CLIENT_KEY`, `TB_SECRET_KEY`, `TB_ACCESS_TOKEN`, `TB_EMAIL`, `TB_PASSWORD`, `TB_MGMT_EMAIL`, `TB_MGMT_PASSWORD`, `TB_USER_ID` | `ssp.pgammedia.com` → Settings → API. |
| **Neon Postgres** | `core/neon.py`, `core/dsp_neon.py`, `core/boxingnews_db.py` | `PGAM_DIRECT_DATABASE_URL`, `DSP_DATABASE_URL`, `DATABASE_URL`, `FINANCE_DATABASE_URL`, `BOXINGNEWS_DATABASE_URL` | One project `round-frog-99233431`: DSP→`public`, SSP→`pgam_direct`. HealthNation separate (`ep-still-pine-aqbb3g84`). **Never cross-schema query blind.** |
| **PubMatic Activate** | `core/pubmatic_activate.py` | `PUBMATIC_ACTIVATE_CLIENT_ID`, `_CLIENT_SECRET`, `_TOKEN`, `_REFRESH_TOKEN`, `_PUBTOKEN`, `_ORG_ID` | OAuth2 `client_secret_basic`, seat `PGAM_Activate_US`, org 17496. Needs all four values to refresh. |
| **MSN Partner Hub** | `core/msn_partner_hub.py`, `agents/etl/msn_insights_etl.py`, `scripts/msn_*.py` | `MSN_EMAIL`, `MSN_PASSWORD`, `MSN_SESSION_DIR`, `MSN_HEADLESS` | Playwright + persisted profile `~/.pgam/msn-session/`. partnerId `AA1lKiff`. |
| **Slack** | `core/slack.py` | `SLACK_WEBHOOK`, `COMPLIANCE_SLACK_WEBHOOK`, `MSN_SLACK_WEBHOOK` | Incoming webhooks, **write-only** — cannot read Slack. Dedup state `/tmp/pgam_alert_state.json`. |
| **HubSpot (REST)** | `agents/outbound/sdr_agent.py` | `HUBSPOT_ACCESS_TOKEN`, `HUBSPOT_PIPELINE_ID` (`899621236`), `HUBSPOT_DEAL_STAGE_NEW` | Duplicate of the MCP connector — §5. |
| **Apollo (REST)** | `agents/outbound/sdr_agent.py` | `APOLLO_API_KEY` | Duplicate of MCP connector — §5. |
| **Instantly** | `agents/outbound/instantly_setup.py` | `INSTANTLY_API_KEY` | Cold email. No MCP equivalent. |
| **monday.com (CLI)** | `scripts/monday_cli.py` | `MONDAY_API_TOKEN` | GraphQL. Default board DSP Dev Work `18406313526`. Duplicate — §5. |
| **Vercel (REST)** | `scripts/vercel_env_sync.py` | `VERCEL_TOKEN`, `VERCEL_TEAM_ID` | **The only env-var write path** — MCP cannot do this. |
| **SendGrid** | `core/config.py` | `SENDGRID_KEY`, `EMAIL_FROM`, `EMAIL_TO` | Transactional email. |
| **Google (GA4 / GSC / Sheets)** | `reports/config.py`, `reports/daily_report_sync.py` | `reports/credentials.json`, `reports/token.pickle` (gitignored), GHA `GOOGLE_CREDENTIALS_JSON`, `GOOGLE_TOKEN_PICKLE_B64` | GA4 digest runs via **GHA Workload Identity Federation** — org policy blocks JSON keys. GSC via `npx tsx scripts/gsc.ts` in `boxingnews`. SA `analytics-digest@pgam-analytics`. |
| **WordPress** (HealthNation) | `healthnation-automation/*.py` | `WP_SITE_URL`, `WP_USERNAME`, `WP_APP_PASS` | Application-password auth. |
| **Unsplash** | `healthnation-automation/unsplash_image.py` | `UNSPLASH_ACCESS_KEY` | |
| **SpringServe** | referenced in agents | `SPRINGSERVE_BASE_URL`, `SPRINGSERVE_EMAIL`, `SPRINGSERVE_PASSWORD` | |
| **PGAM internal APIs** | agents / scheduler | `PGAM_DASHBOARD_BASE`, `PGAM_DASHBOARD_SERVICE_TOKEN`, `DSP_DASHBOARD_URL`, `CRON_SECRET`, `DSP_CRON_SECRET` | `admin.pgammedia.com` Partner Revenue Dashboard (LL+TB unified). |
| **Anthropic API** | `core/config.py` | `ANTHROPIC_API_KEY` | Used by agent reasoning paths. |

### Deploy / CI surfaces

- **Render** — this repo deploys as a worker (`render.yaml`, `Procfile:
  worker: python scheduler.py`). All `.env.example` vars are set in the
  **Render** dashboard with `sync: false`. There is **no Render API token in
  this repo and no Render MCP connector**, so reading or changing Render env
  is a genuine manual ask. Do not send anyone to Vercel for these.
- **Vercel** — the *other* repos deploy here. Env writes need
  `scripts/vercel_env_sync.py` + `VERCEL_TOKEN`; vars bake in at build time,
  so **always redeploy after a sync**.
- **GitHub Actions** — 10 workflows (`msn-insights`, `compliance-daily`,
  `compliance-watchdog`, `compliance-fallback`, `msn-daily-totals-rollup`,
  `msn-rejection-csv-loader`, `msn-puller-watchdog`, `msn-partner-reports`,
  `daily_report`). Secrets live in the GHA store. Readable/triggerable via
  `mcp__github__actions_*` and `mcp__github__get_job_logs` — **use these
  instead of asking Priyesh to open the Actions tab.**

---

## 3. Discovery order (the loop that replaces "please check the dashboard")

```
1. ToolSearch for the platform            → MCP tool available?     use it
2. This registry, §1/§2                   → documented route?       use it
3. env presence check (names only)        → credential injected?    use it
4. CLI presence: command -v <cli>         → installed + authed?     use it
5. Repo module in §2                      → existing client?        extend it
6. Documented fallback for that platform  → try it
7. Only now: a precise, minimal manual ask (template in CLAUDE.md)
```

Run `python3 scripts/check_integrations.py` for a one-shot answer to
steps 1–4 without spending API calls.

## 4. Self-healing fallback chains

| Platform | Chain |
|---|---|
| **GitHub** | `mcp__github__*` → `git` over HTTPS (creds pre-injected) → `curl api.github.com` with `$GH_TOKEN` (only non-GitHub-blocked host) → ask |
| **Vercel** (read) | `mcp__Vercel__get_deployment` / `get_deployment_build_logs` / `get_runtime_logs` / `get_runtime_errors` → `list_deployments` for the right id → ask |
| **Vercel** (env write) | `scripts/vercel_env_sync.py` + `VERCEL_TOKEN` → if cloud session (403 egress) or token absent, **say exactly that** and ask |
| **HubSpot** | `mcp__HubSpot__search_crm_objects` / `query_crm_data` → `get_user_details` to confirm the object's read/write status → if `REQUIRES_REAUTHORIZATION`, ask for re-auth by name → `sdr_agent.py` REST (local only) |
| **monday.com** | `mcp__monday_com__*` → `scripts/monday_cli.py` (local) → ask |
| **Neon** | `core/neon.py` / `psql` with the right DSN → confirm schema (`public` vs `pgam_direct`) → in cloud, host is blocked: say so |
| **LL / TB** | stats API (`core/api.py`) → UI automation (`core/ui_nav.py`, Playwright) → ask |
| **Render** | no programmatic route → ask immediately, and say why |

**Diagnose before escalating.** Distinguish these, and name which one it is:

- `CONNECT tunnel failed, 403` → org **egress policy** (cloud session). Not a
  token problem. Switch to MCP; never retry or tunnel around it.
- MCP tool name not found → you didn't `ToolSearch`. Load it and retry.
- Connector `enabledInChat: false` (via `ListConnectors`) → authenticated but
  toggled off *for this chat*. Ask Priyesh to enable it in this chat's
  connector settings — a one-click, precisely-scoped ask.
- HTTP 401 → expired/invalid credential. Say which env var.
- HTTP 403 from the API itself → insufficient scope. Say which scope.
- Env var absent → say which name is missing and where it belongs (Render
  dashboard for this repo, Vercel for the web repos, local `.env` otherwise).

## 5. Duplicate integrations — consolidate, don't add

Prefer the MCP connector for interactive work; keep the REST client only
where it is load-bearing for unattended automation (Render worker / GHA),
which cannot use MCP.

| Duplication | Resolution |
|---|---|
| HubSpot: MCP **+** `sdr_agent.py` REST | MCP for all interactive/diagnostic work. Keep REST in `sdr_agent.py` — it runs unattended. Do not add a third client. |
| monday.com: MCP **+** `scripts/monday_cli.py` | MCP for queries and ad-hoc updates. Keep the CLI for scripted ticket closing (documented in the playbook). |
| Vercel: MCP **+** `scripts/vercel_env_sync.py` | MCP for everything readable. The script stays — it is the **only** env-var write path. |
| Apollo: MCP **+** `APOLLO_API_KEY` in `sdr_agent.py` | MCP interactively (watch credit spend). REST stays for the unattended SDR run. |
| GitHub: MCP **+** git HTTPS **+** `GH_TOKEN`/`GITHUB_TOKEN`; GHA uses both `GH_TOKEN` and `GH_PAT` | MCP for API work, `git` for repo work. **Open item:** reconcile `GH_TOKEN` vs `GH_PAT` in workflows — two secrets for one purpose. |
| LL keys fall back to `TB_*` in `core/api.py:12-13` | **Real footgun.** A missing `LL_CLIENT_KEY` silently authenticates against TB and returns wrong-platform data instead of failing. Worth making explicit. |

**Before building any new client:** grep this registry and `core/` first. The
answer is usually "extend the existing one."

## 6. What still requires Priyesh (no programmatic route exists)

Ask directly for these — they are not policy failures:

- **Render** env vars / service config (no token, no MCP).
- **HubSpot re-authorization** for the `REQUIRES_REAUTHORIZATION` objects.
- **Connecting** Stripe / Linear / Notion / Figma / Asana / Atlassian / Box /
  Canva / Intercom, or enabling a connector that is off for a given chat.
- **Any credential value.** Sessions never see `.env`; rotation is manual.
- **DNS**, domain purchases, and billing changes.
- Anything in the `CLAUDE.md` confirm-first list.
