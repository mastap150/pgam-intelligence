# Runbook — rotating the two exposed Neon credentials

**Status:** rotation not yet performed. This runbook is the plan; steps 2–7
need a human with Neon console, GitHub, and Render access.

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

**Take least privilege while you are here.** Almost every consumer is
read-only reporting. Suggested split:

- A read-only role (`GRANT CONNECT`, `USAGE` on the relevant schemas, `SELECT`
  on tables, plus `ALTER DEFAULT PRIVILEGES … GRANT SELECT`) for reporting:
  `tb-today`, `tb-headroom`, `tbx-neon-reports`, `tbx-take-rate`,
  `tbx-supply-gap`, `hasib_trigger_check.py`, `msn_lane_performance.py`, and
  the alert agents.
- A writer role for the ETL paths that genuinely write:
  `msn-daily-totals-rollup`, `msn-rejection-csv-loader`, `msn-insights`,
  `tbx-backfill`, the compliance jobs (they write
  `pgam_direct.compliance_findings`), and the Render worker.

If splitting roles is more than you want to take on right now, a single new
non-superuser role for everything is still a strict improvement over
`neondb_owner`. Do not skip step 1 just to avoid the privilege question.

Build the new DSNs but **do not paste them into this file, a commit, a PR, a
routine prompt, or a chat message.** They go straight into the stores below.

### 2. Update GitHub Actions

Repo → Settings → Secrets and variables → Actions. Update the single secret
`PGAM_DIRECT_DATABASE_URL` to the new pgam-direct DSN. That covers all 13
workflows and both env names.

No `BOXINGNEWS_DATABASE_URL` secret exists; add one only if a workflow starts
needing it.

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

Run `list_triggers` and confirm no routine inlines a credential. As of
2026-08-25 the account has 10 routines; the other 9 are `send_later` PR
check-ins with no credentials in them. Re-check with `include_completed: true`
as well, since already-fired one-shots are hidden by default.

## Checklist

- [ ] 1. New Neon roles created on both projects (least-privilege where practical)
- [ ] 2. GitHub secret `PGAM_DIRECT_DATABASE_URL` updated
- [ ] 3. Render worker env updated + redeployed
- [ ] 4. Cloud environment + local `.env` updated (both variables)
- [ ] 5. `tb-today` dispatched green; one `msn-insights` tick green
- [ ] 6. `neondb_owner` password reset on both projects
- [ ] 7. Routine `trig_01KfQFfT93WTsqjEifmtnwfr` deleted and recreated
- [ ] 8. `list_triggers` re-audited, including completed one-shots
