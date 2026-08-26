# Runbook — rotating the two exposed Neon credentials

**Status:** rotation not yet performed. This runbook is the plan; every step
needs a human with Neon console, GitHub, and Render access.

**Decided 2026-08-25 (Priyesh):** he runs the rotation himself from this
document — no Neon API key goes into a cloud session — and takes the
**read-only / writer role split** rather than a straight password swap.

## What happened

The routine `trig_01KfQFfT93WTsqjEifmtnwfr` — *"Hasib trigger check — weekly
MSN cohort monitor"*, cron `0 18 * * 0`, enabled, created `2026-07-27`, last
fired `2026-08-23` — has two full Neon connection strings, passwords included,
inlined in its stored prompt text as `export` lines.

Routine prompts are stored server-side and echoed back **in full** by
`list_triggers`. Anyone who can list this account's routines can read both
passwords. Both DSNs use the `neondb_owner` role, so the blast radius is full
read/write on both databases.

| Env var | Neon endpoint | Role |
|---|---|---|
| `PGAM_DIRECT_DATABASE_URL` | `ep-small-math-…` (pooler) | `neondb_owner` |
| `BOXINGNEWS_DATABASE_URL` | `ep-delicate-star-…` | `neondb_owner` |

**Exposure window:** 2026-07-27 (routine created) → now. Treat both passwords
as compromised from the creation date, not from discovery.

## The stored prompt cannot be edited from a session

`update_trigger` was attempted against this routine and refused:

> this routine was created via "http_api", not by an agent. Agents can only
> update routines they created (via `create_trigger`). A routine's own session
> may still disable itself (`enabled=false` only).

So the secret cannot be redacted in place. The only ways to remove it from
storage are to **delete the routine** (see step 7) or to rotate the passwords
so the stored text is worthless. Do both.

Note this is a *different* rule from what `CLAUDE.md` used to state. The
restriction is not "web-UI routines can't be edited" — it is **ownership**: an
agent can only update routines it created via `create_trigger`. This routine's
`created_via` is `http_api`, which is outside that set either way.

## Consumer inventory

Verified against the tree at `6292637`. This is larger than a first pass
suggests — do not work from a shorter list.

### `PGAM_DIRECT_DATABASE_URL`

**One GitHub repo secret** feeds every workflow: `secrets.PGAM_DIRECT_DATABASE_URL`.
Several workflows map it to **two** env names in the same job:

```yaml
PGAM_DIRECT_DATABASE_URL: ${{ secrets.PGAM_DIRECT_DATABASE_URL }}
DATABASE_URL:             ${{ secrets.PGAM_DIRECT_DATABASE_URL }}
```

`core/neon.py` resolves `PGAM_DIRECT_DATABASE_URL` first and falls back to
`DATABASE_URL`. There is **no separate `DATABASE_URL` repo secret** — updating
the one secret covers both names. (`FINANCE_DATABASE_URL` is a distinct secret
for a different database and is **not** in scope.)

13 workflows consume it:

`compliance-fallback`, `compliance-watchdog`, `msn-daily-totals-rollup`,
`msn-insights`, `msn-partner-reports`, `msn-puller-watchdog`,
`msn-rejection-csv-loader`, `tb-headroom`, `tb-today`, `tbx-backfill`,
`tbx-neon-reports`, `tbx-supply-gap`, `tbx-take-rate`.

Python consumers reach it through `core/neon.py` (`_resolve_dsn`), plus direct
readers: `agents/alerts/{tb_revenue,dashboard_alerts,marketplace_digest}.py`,
`agents/optimization/qps_waste_sentry.py`, `core/tb_unified.py`,
`core/boxingnews_db.py`, and ~15 scripts under `scripts/` including
`hasib_trigger_check.py`, `tbx_recon.py`, `tbx_supply_gap.py`,
`msn_refresh_puller.py`, and the `msn_oauth_*` family.

**The Render worker also needs it.** `Procfile` runs `worker: python
scheduler.py`; `scheduler.py:840` does `from core.neon import connect`. Note
that **`render.yaml` does not declare this variable at all** — it is set only
in the Render dashboard, so it is invisible from the repo and easy to miss.
Check Render → the worker service → Environment before declaring the rotation
done.

### `BOXINGNEWS_DATABASE_URL`

Only 4 references, and **no GitHub repo secret exists for it** — no workflow
references `secrets.BOXINGNEWS_DATABASE_URL`.

- `core/boxingnews_db.py` (`_resolve_dsn`, no fallback)
- `scripts/hasib_trigger_check.py`
- `scripts/msn_lane_performance.py`
- `docs/boxingnews-fightweek-2026-08-22-romero-lopez.md` (mention only)

Its only automated consumer today is the exposed routine itself, which reads it
from the inlined prompt rather than from the environment. Everything else is
run by hand from a local `.env`.

## Timing: the window is 15 minutes, not a day

`msn-insights.yml` runs on `*/15 * * * *` and reads the DSN. A reset-in-place
therefore breaks production within 15 minutes, not overnight.

Worse, it is **schedule-only** — it has no `workflow_dispatch`, so it cannot be
manually re-run to confirm a fix. Same for `compliance-fallback`,
`compliance-watchdog`, `msn-partner-reports`, and `daily_report`. These
workflows only tell you they are broken on their next scheduled tick.

This is why the staged sequence below exists. Do not reset `neondb_owner` in
place as the first step.

## Rotation sequence

### 1. Create new roles, do not reset the old ones

In the Neon console, for **each** of the two projects, create a *new* role
rather than resetting `neondb_owner`'s password. This gives an overlap window
where both old and new credentials work, so nothing breaks between steps 2 and
5.

**Decided 2026-08-25: take the read-only / writer split.** Two new roles per
project, not one.

The classification below was traced through each workflow to the script it
actually runs, then to whether that module contains write SQL — not guessed
from job names.

**Writers** — need `INSERT`/`UPDATE` on `pgam_direct`:

| Consumer | Writes via |
|---|---|
| `msn-daily-totals-rollup` | `scripts/msn_daily_totals_rollup.py` |
| `msn-insights` | `scripts/msn_oauth_capture.py` |
| `msn-partner-reports` | `agents.etl.msn_partner_reports_etl` |
| `msn-rejection-csv-loader` | `agents.etl.msn_rejection_etl`, `…_details_etl` |
| `tbx-backfill` | `agents.etl.tbx_revenue_etl` |
| `compliance-fallback` | `agents.compliance.runner.run_fallback_digest` |
| Render worker | `scheduler.py` → `core.neon.connect` |

`compliance-fallback` is the one to double-check: `runner.py` contains write
SQL, but whether `run_fallback_digest()` specifically writes was not confirmed.
Give it the writer role — over-granting one job is cheaper than a broken
compliance digest.

**Readers** — confirmed zero write SQL, and none import a writing module:

`tb-today`, `tb-headroom`, `tbx-neon-reports`, `tbx-supply-gap`,
`tbx-take-rate`, `compliance-watchdog`, `msn-puller-watchdog`, plus
`scripts/hasib_trigger_check.py`, `scripts/msn_lane_performance.py`, and the
alert agents (`marketplace_digest`, `dashboard_alerts`, `tb_revenue`,
`qps_waste_sentry`).

Sketch for the reader role — adjust schema names to what the project actually
uses:

```sql
CREATE ROLE pgam_ro WITH LOGIN PASSWORD '…';
GRANT CONNECT ON DATABASE neondb TO pgam_ro;
GRANT USAGE ON SCHEMA pgam_direct, public TO pgam_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA pgam_direct, public TO pgam_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgam_direct, public
  GRANT SELECT ON TABLES TO pgam_ro;
```

The writer role is the same plus `INSERT, UPDATE, DELETE` (and `USAGE` on
sequences). Neither role needs superuser.

The boxingnews project only ever gets read — `hasib_trigger_check.py` and
`msn_lane_performance.py` are its only consumers — so it needs the read-only
role alone.

Build the new DSNs but **do not paste them into this file, a commit, a PR, a
routine prompt, or a chat message.** They go straight into the stores below.

### 2. Update GitHub Actions

Repo → Settings → Secrets and variables → Actions.

**The split costs more here than a straight rotation, and the ordering is not
optional.** Today all 13 workflows read one secret, `PGAM_DIRECT_DATABASE_URL`.
Two roles means two secrets and a per-workflow edit:

1. Set `PGAM_DIRECT_DATABASE_URL` to the **writer** DSN. The 6 writer workflows
   need no file change — they keep reading the name they already read.
2. Add a **new** secret `PGAM_DIRECT_DATABASE_URL_RO` holding the reader DSN.
3. Edit the 7 reader workflows to reference `secrets.PGAM_DIRECT_DATABASE_URL_RO`
   instead: `tb-today`, `tb-headroom`, `tbx-neon-reports`, `tbx-supply-gap`,
   `tbx-take-rate`, `compliance-watchdog`, `msn-puller-watchdog`. Remember the
   `DATABASE_URL:` line in the same `env:` block where one exists — `core/neon.py`
   falls back to it.

**Create both secrets before merging the workflow edit.** A workflow pointing
at a secret that does not exist yet gets an empty string and fails its own
`-z "$PGAM_DIRECT_DATABASE_URL"` guard. That is why the workflow change is not
in the same PR as this runbook — this PR is docs-only and safe to merge at any
time.

No `BOXINGNEWS_DATABASE_URL` secret exists and none is needed; that database
has no workflow consumers.

### 3. Update Render

Render dashboard → the `pgam-intelligence` worker → Environment. Update
`PGAM_DIRECT_DATABASE_URL` (and `DATABASE_URL`, if it is set there too — check
both, since `core/neon.py` falls back). Not declared in `render.yaml`, so the
dashboard is the only source of truth. Redeploy or restart the worker so it
picks the value up.

### 4. Update the cloud environment and local `.env`

The `PGAM` cloud environment (`env_0112tdyC54U8EXEqtMucGBbB`) is what the
replacement routine in step 7 will read from. Add **both** variables there:
claude.ai/code → environment selector → gear icon → **Environment variables**.

Per `CLAUDE.md` "Cloud sessions and credentials", that box is not a secrets
store — values are readable by anyone using the environment. That is a real
downgrade from where these should live, and it is a reason to put the
**read-only** role there, not the writer.

Also update Priyesh's local `.env`. It is gitignored and never committed.

### 5. Verify before revoking

Run `tb-today` via **workflow_dispatch** — it uses
`PGAM_DIRECT_DATABASE_URL`, it is cheap, and it has a manual trigger. Confirm
it connects and completes.

> Do **not** use `tbx-probe` for this. It has a `workflow_dispatch` trigger but
> contains zero `DATABASE_URL` references — it does not touch Neon and will
> pass whether or not the rotation worked.

Then let one `*/15` tick of `msn-insights` pass and confirm it is green. For
boxingnews, run `scripts/hasib_trigger_check.py --weeks 6` locally with the new
DSN exported.

### 6. Only now, revoke the old credentials

With the new roles confirmed working, reset the `neondb_owner` password on both
projects (or drop the old role if you created a dedicated replacement). This is
the step that actually ends the exposure — everything before it is preparation.

After this, the DSNs sitting in the routine's stored prompt are dead strings.

### 7. Remove the routine and recreate it cleanly

Rotation kills the value of the leaked text but leaves it in storage. To remove
it, **delete** `trig_01KfQFfT93WTsqjEifmtnwfr` and recreate the monitor.

The replacement prompt is checked in at
`docs/routine-prompts/hasib-trigger-check.md` — it reads both DSNs from the
environment, fails loudly if either is missing, and never prints a value. Note
the recreated routine must keep the same shape: cron `0 18 * * 0`, model
`claude-sonnet-5`, tools `Bash` + `Read`, source `mastap150/pgam-intelligence`.

Deleting loses the routine's run history, so it is Priyesh's call. If a session
creates the replacement via `create_trigger`, that routine *will* be agent-
editable later, unlike this one.

### 8. Re-audit

Partially done already, on 2026-08-25:

- **All 10 live routines checked — only this one leaks.** `list_triggers` with
  the default `include_completed: false` returns every routine that can still
  fire. The other 9 are `send_later` PR check-ins containing no credentials.
  For "what can still execute with a stolen password", this is the complete
  answer.
- **The 100 most recent completed one-shots checked — clean.** Scanned for
  `npg_`, `postgres(ql)://user:pass@`, `neondb_owner:`, `sk-ant-`, `ghp_`,
  `github_pat_`, `AKIA…`, `xoxb-`. Zero hits.
- **Not yet checked: older completed one-shots.** That page returned a
  `next_cursor`, so the account has more than 100 already-fired routines and
  the sweep did not reach the end. Completed one-shots cannot fire again, but
  their prompts are still stored and still returned by `list_triggers`, so a
  credential in one is still exposed.

To finish it, page through with `include_completed: true` following
`next_cursor` until exhausted. Grep each page for the patterns above rather
than reading it — the pages are ~240 KB each, and grepping keeps any secret out
of the session transcript. Print match *counts and routine ids*, never the
matched text.

## Checklist

- [ ] 1. Reader + writer roles created on pgam-direct; reader role on boxingnews
- [ ] 2a. `PGAM_DIRECT_DATABASE_URL` repointed to the writer DSN
- [ ] 2b. `PGAM_DIRECT_DATABASE_URL_RO` created with the reader DSN
- [ ] 2c. The 7 reader workflows edited to use `_RO` (merge only after 2b)
- [ ] 3. Render worker env updated + redeployed
- [ ] 4. Cloud environment + local `.env` updated (both variables)
- [ ] 5. `tb-today` dispatched green; one `msn-insights` tick green
- [ ] 6. `neondb_owner` password reset on both projects
- [ ] 7. Routine `trig_01KfQFfT93WTsqjEifmtnwfr` deleted and recreated
- [ ] 8. `list_triggers` re-audited, including completed one-shots
