# TB Playbook

TB is the second supply platform, running in parallel to LL. Target: **$15K/day**. TB data **is** in Neon (`pgam_direct` schema on the shared `round-frog-99233431` project). LL data is not.

## What TB is

TB is a supply-side platform. It's active, integrated with pgam-intelligence, and has its own scheduler jobs and monitoring. Ported into pgam-intelligence alongside LL.

## Active scheduler jobs (in `scheduler.py`)

- **`tb_floor_nudge`** — adjusts floors up/down based on fill and CPM signals. Dry-run mode default.
- **`tb_contract_floor_sentry`** — guards contractual floor minimums. Alerts if a floor drops below contract. Dry-run mode default.

Both jobs are currently dry-run. Do not flip live-write on without Priyesh sign-off + a live/dry diff run.

## TB 401 errors are real signal

If you see `401 Unauthorized` errors in TB job logs, it's **not noise** — it means auth broke and writes are silently failing. Investigate immediately: token rotated, IP allowlist changed, or API endpoint moved. Do not filter or suppress.

## Non-negotiable partner rules (TB side)

- **Unruly (dp=5)** — same freeze as LL. `core/partner_freeze.py` covers both platforms.
- **9 Dots contract floor $1.70 minimum** — applies to TB too if 9 Dots demands run through TB.
- **BidMachine (dp=40) QPS cap** — same rule.

## Where TB data lives

- Postgres: `round-frog-99233431` Neon project, `pgam_direct` schema
- Bloat watch: `ss_vast_events` table has tripped SSP probes before (2026-04-29). Upgraded to Neon Launch tier at that point to survive it. Monitor row counts.

## Partner Revenue Dashboard

TB revenue rolls up in `admin.pgammedia.com` alongside LL. Data source is Neon `pgam_direct` for TB.

## Common tasks

- **Read TB state:** query `pgam_direct.*` in Neon
- **Check a floor nudge decision:** grep scheduler logs for `tb_floor_nudge` — job logs what it would have done in dry-run mode
- **Investigate a fill drop:** check TB UI + Neon `ss_*` tables + freeze status
- **Onboard a new demand connection:** TB UI + confirm not on the freeze list + add contract floor to `core/contract_floors.py` (verify path)

## Cas.ai / Nimpha LTD (relevant to TB too)

Dual-role partner. Both supply and demand routes. QBO customer 155, Net 60, Cyprus billing entity. Handle supply/demand as two separate books.

## Escalation

- TB 401 spam → engineering, immediately
- Partner floor dispute → Priyesh
- Contract terms → Priyesh
- Neon bloat → engineering (has happened before, prepare to scale up or archive)
