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

## Two Teqblaze APIs, one marketplace

`ssp.pgammedia.com` is the **old** Teqblaze platform PGAM was on;
`api.pgammedia.com` is its **successor**, serving the same underlying data —
Teqblaze confirmed on 2026-08-20 that the old ClickHouse was transferred
wholesale, so reports should match. So this is a migration in progress, and the
two APIs remain different *systems* even where the data agrees; conflating them
is the easiest mistake to make here.

**ID portability (answered by Teqblaze 2026-08-20):** placement IDs and their
settings are **unchanged** across the two hosts; inventory IDs are **new**;
publisher and demand-source IDs were **not covered either way** — and those are
what the `pgam_direct.tb_daily_*` ETL keys on, so do not assume them stable
(`docs/teqblaze-new-platform.md` §8.1.10d). Legacy shutdown happens only on
PGAM's confirmation, so keep the legacy leg until the report reconciliation
passes — it is the only independent check on TBX's numbers.

| | legacy "TB" | new "TBX" |
|---|---|---|
| Host | `ssp.pgammedia.com/api` | `api.pgammedia.com` |
| Auth | token in the URL path | `POST /login` → Bearer JWT |
| Modules | `core/tb_api.py`, `core/tb_mgmt.py` | `core/tbx_api.py`, `core/tbx_mgmt.py` |
| Env | `TB_EMAIL`, `TB_PASSWORD`, `TB_USER_ID` | `TBX_EMAIL`, `TBX_PASSWORD` |
| Entities | inventory → placement | supply/demand source → placement |

Both are live *today*, and the `tb_*` scheduler jobs (`tb_floor_nudge`,
`tb_contract_floor_sentry`) still run against the legacy host — do not repoint
or delete them. The new platform is unverified against real data, so the legacy
leg is the one currently carrying live floor decisions. Migrate deliberately
(`docs/teqblaze-new-platform.md` §7), never by swapping a base URL: an ID
mapping between the two does not exist yet, so contract floor minimums do not
carry across.

Writes to TBX are gated twice: `dry_run=True` by default on every call, plus
`TBX_ALLOW_WRITES=1` at the environment level. The prerequisites for opening
that gate are in `docs/teqblaze-new-platform.md` §6. Spec is vendored at
`docs/api/teqblaze-openapi.json`; probe with `scripts/tbx_probe.py`.

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

### A Neon credential does not help a cloud session anyway

Measured 2026-09-01, by port rather than by host:

- **Port 443 leaves the container.** Raw TCP to `pypi.org:443`,
  `neon.tech:443`, even `boxingnews.com:443` all connect. The HTTPS *proxy*
  denies most hosts by policy (`boxingnews.com` → 403 at CONNECT), but that
  policy is the proxy's, not the network's.
- **Port 5432 is silently dropped.** TCP to `us-east-2.aws.neon.tech:5432`
  and every other Neon region times out. Neon endpoint DNS resolves fine
  (the wildcard answers even for a made-up endpoint), so this is an egress
  rule on the port, not a name-resolution problem.

So **`psycopg` cannot connect to Neon from a cloud session** — a pasted DSN
does not change that, and it puts an owner-level secret in a transcript for
nothing. Neon's SQL-over-HTTP endpoint (`https://<endpoint>/sql`, port 443)
is the one untested possibility; do not assume it works until someone tries
it, and do not paste a DSN to find out.

That makes the routing rule simple. Anything needing Neon runs where Neon is
reachable:

| Need | Run it |
|---|---|
| Ad-hoc query, exploration | Priyesh's laptop, `.env` loaded |
| Scheduled or on-demand report | A GitHub Actions workflow, secrets as secrets |
| Continuous jobs | The Render worker |

A cloud session's job is to *write* that workflow, not to run the query. And
because **this repository is public, its Actions logs are world-readable** —
so a workflow handling revenue, cohort or contractor data delivers to Slack
(or another private sink) and prints only a confirmation. See
`.github/workflows/hasib-cohort-report.yml` for the shape.

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

## Claude usage efficiency policy

Spend on this account is dominated by **context re-ingestion, not generation**.
Measured across 18 cloud sessions on 2026-08-17→21: 285M context tokens
(cache read + write) against 1.03M output tokens — output was 0.36% of all
billed tokens. Three sessions kept alive for 2–4 days were 85% of the spend.
So the rules below are about *session shape*, not about writing less.

The two failure modes that actually cost money, in order:

1. **A session kept alive for days.** Every turn re-reads the whole
   accumulated history. A 4-day session is not 4× a 1-day session, it is
   quadratic-ish: cost per turn rises with everything said before it.
2. **A recurring check-in bound to such a session.** Each wake pays that
   session's full context just to answer "anything changed?" — and the
   answer is almost always no.

### Session shape

- **Start a fresh session per task.** Cheap and correct. Cost per turn scales
  with accumulated context, so a new session is the single biggest lever.
- **Don't resume a session older than ~a day** to ask something small. Open a
  new one; put anything worth keeping in the repo, not in scrollback.
- **Compact when the work continues but the history stops mattering** — after
  a milestone, once a long investigation has landed its conclusion in a file.
- **Split at natural seams.** Investigate in one session, implement in the
  next, with the findings written down in between. Don't carry a 60-turn
  debugging transcript into the fix.
- **Long-lived context belongs in files**, in this repo, under `docs/` or here
  — not in a session you keep warm. That is what makes a fresh session cheap.
- Watch for `short-cache-ttl` from `scripts/claude_usage.py`. A high share of
  cache writes on the 5-minute TTL means context is being *rewritten* each
  turn rather than reused, which is the expensive direction. Once an account
  is in usage overage the effective TTL shortens, so overage compounds itself
  — treat "we are on overage" as a reason to shorten sessions, not to push on.

### Do not leave self-re-arming PR check-in loops running

*(This section and the next were first written in PR #101, the 2026-08-20
audit. Recording them here because that PR was never merged, and the loops
regenerated the next day.)*

A `send_later` / Routine check-in bound to a **persistent** session re-reads
that session's whole history on every wake, and none of these loops can
terminate on their own: a draft PR with no reviewer never changes, so every
wake finds nothing, re-arms, and bills again.

- **Do not arm a recurring check-in on a PR that is green and has no
  reviewer.** There is nothing to drive. It is waiting on a human; leave it.
- If a check-in is genuinely warranted (CI actually running, a reviewer
  actually engaged), **give it a bounded number of fires**, and prefer a
  fresh session per fire over binding it to a long-lived one.
- **Run `list_triggers` before creating another.** Duplicates pointed at the
  same PR have happened twice now.
- Before blaming a scheduled job for a cost spike, **read per-session
  `cost_usd` from `list_sessions`.** In both audits so far the scheduled jobs
  were innocent and the interactive sessions were the cost.
- When a session's work is finished, **stop its check-ins** rather than
  letting them idle.

### Never inline a connection string in a routine prompt

Routine prompts are stored server-side and echoed back in full by
`list_triggers`. Read credentials from the environment instead. A routine
created through the web UI cannot be edited from a session, so a secret
pasted into one has to be rotated to be removed — moving it is not enough.

### Bounded work, not exhaustive work

- Prefer "run the failing tests → fix → re-run those tests" over "run
  everything until it all passes". Bound the loop, then report.
- Search before reading. Read the span you need, not the file; the file, not
  the tree. Don't re-read what is already in context.
- Don't pull large logs, dumps, `node_modules`, build output, or full API
  responses into context. Filter or summarize at the source — `jq`, `head`,
  `grep -c`, a `LIMIT` — then read the result.
- One subagent for genuinely parallel, independently-scoped work. Not as a
  default, and never several agents over the same ground.
- **Warn before starting work that will be unusually expensive** — scanning
  several repos, fanning out agents, a full-history audit — and offer a
  targeted alternative first. Normal development work needs no warning.
- Don't start a recurring or background AI process without saying so and
  saying when it stops.

### Model selection

`claude-opus-5` is the default and stays the default for anything requiring
judgement — architecture, tricky debugging, large refactors, reviews. Where
the harness lets a job pick its own model, a cheaper one is right for
mechanical work: file search, formatting, log reading, running a script and
reporting output. The weekly `Hasib trigger check` routine is the model here
— Sonnet 5, `Bash`+`Read` only, one script.

Don't downgrade a model to save money on work that needs the judgement.
Shorten the session instead — that is where the money actually is.

### Checking usage

`python3 scripts/claude_usage.py` reports local sessions (tokens, cache TTL
split, runaway signals, live processes) with no API call. `--runaway` is the
one-line health check; `--sessions` ranks sessions. It cannot see claude.ai
chat, Cowork, or cloud sessions, and its cost column is list-price
arithmetic that understates long agentic sessions — for cloud sessions use
`list_sessions`' real `cost_usd`, and for money use
console.anthropic.com → Usage / Billing.
