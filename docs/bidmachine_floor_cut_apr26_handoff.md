# BidMachine Floor Cut — Handoff (Apr 26, 2026)

This file is a self-contained brief for the desktop session (or any teammate)
picking up the BidMachine floor cut. Everything you need to deploy and verify
the change is here.

## TL;DR — what to do

1. Merge PR **#20**: <https://github.com/mastap150/pgam-intelligence/pull/20>
2. Render dashboard → service `pgam-intelligence-scheduler` → Environment →
   add `BIDMACHINE_CUT_APR26_APPLY=1` → Save.
3. Render auto-redeploys (~1-2 min). On the next scheduler boot the floor
   cut runs **once**, writes the changes via the canonical LL API, and
   never runs again (idempotency guard via the floor ledger).
4. Watch the Render `Logs` tab for:
   `[scheduler] BidMachine floor cut complete: applied=N, failed=0`

After it has run, you can leave the env var in place or delete it — the
ledger guard blocks any re-execution either way.

## What was delivered (PR #20, branch `claude/setup-desktop-repo-access-Re5Rh`)

Two commits:

- `794798d` — `scripts/bidmachine_floor_cut_apr26.py`: one-shot CLI
  executor that drops demand-level `minBidFloor` from `$3.25` to `$2.00`
  on the BidMachine surfaces.
- `5074e11` — `scheduler.py` boot-hook + script refactor exposing
  `run_one_shot()` and `already_applied()` for programmatic invocation.

### Action

- Targets publishers `290115319` (BidMachine - In App Display & Video) and
  `290115333` (BidMachine - In App Interstitial) by default.
- For each: every wired demand currently at floor `>= $3.00` (status=1)
  is dropped to `$2.00`.
- Bypasses the portfolio optimizer's `±25%` per-proposal cap
  (`intelligence/optimizer.py:46`), which would otherwise refuse the
  ~38% step.
- Uses the canonical post-Apr-18 write path
  (`core.ll_mgmt.set_demand_floor()` → `PUT /v1/demands/{id}` + verify).
  Each write is re-GET'd within 1s.

### Why now

Apr-26 revenue review: at 16% of monthly target with an $842K gap. Floor
pressure is ~78% of immediately addressable opportunity, concentrated on
BidMachine. Display sits at 5.4% WR leaving ~$1,559/day on the table;
Interstitial at ~2% WR is wasting ~80% of its potential. Floors were
tuned for a higher-demand period that no longer reflects the market.

### Expected impact

Display win rate normalises toward 8–10%; ~$1,200/day recovery within
24–48h. Combined with the Pubmatic relationship review and geo-leak
follow-ups, total trajectory: $7.6K daily → ~$228K monthly run rate.

## Safety mechanisms

| Layer | Mechanism |
|---|---|
| Pre-write | `set_demand_floor()` honors contract-floor minimums (e.g. 9 Dots `$1.70`). Cap at `--max-changes 30` writes per run. |
| Multi-publisher | The script builds a portfolio-wide demand→publishers map. Demands wired only inside the BidMachine family (`290115319/332/333/334/340`) are safe; demands wired to non-BM publishers are **skipped** unless `--allow-multi-pub` is passed. |
| Post-write verify | Every PUT is followed by 1s sleep + re-GET. Mismatch raises `RuntimeError`. |
| Ledger | Every change recorded with `actor=bidmachine_floor_cut_apr26`. |
| Idempotency | The boot hook checks the ledger for prior LIVE entries and skips if found. The `BIDMACHINE_CUT_APR26_APPLY=1` env var can stay set forever — it will never re-fire. |
| Daily verifier | `intelligence/verifier.py` (07:30 ET) re-checks every recent ledger write within 24h. |
| 4-hour blast door | `agents/optimization/auto_revert_harmful.py` reverts any ledger write that correlates with `>20%` post-change revenue drop on the demand. Worst-case unwind: 4 hours. |

## Composition with existing ML / automation

| System | Effect | Why no conflict |
|---|---|---|
| **LL traffic-shaping ML** (their side) | Step-change in floors, then relearn over their window. | One-shot is *easier* for an ML system to adapt to than a moving target — a stable signal vs daily nudges. |
| **`intelligence/optimizer.py`** (07:45 ET daily) | Sees `$2.00` as the new baseline next run. | 24h cooldown per `(pub, demand)` tuple skips anything we just touched. ±25% cap then applies on top of `$2.00`. |
| **`intelligence/dayparting.py`** (hourly, gated by `PGAM_DAYPARTING_ENABLED`) | Treats `$2.00` as new midpoint if enabled. | 45-min cooldown, ±25% clip, separate `actor` field. |
| **`agents/optimization/contract_floor_sentry.py`** (hourly) | Would *raise* a write back to a contractual minimum. | `$2.00` is above all current contract minimums — sentry won't trip. |
| **`intelligence/verifier.py`** | Confirms or flags every ledger write within 24h. | Designed exactly for this. |
| **`auto_revert_harmful.py`** | Reverts on `>20%` revenue drop within 4h. | The blast door — feature, not bug. |

## Manual run path (alternative to env-var trigger)

If you'd rather not flip the Render env var, the script still runs
manually from any environment with the prod `.env`:

```sh
git fetch origin claude/setup-desktop-repo-access-Re5Rh
git checkout claude/setup-desktop-repo-access-Re5Rh

# preview
python3 scripts/bidmachine_floor_cut_apr26.py

# review logs/bidmachine_floor_cut_apr26.json — pay attention to any
# 'multi_pub_outside_bm_family' skips. Re-run with --allow-multi-pub
# only if you accept the propagation.

python3 scripts/bidmachine_floor_cut_apr26.py --apply
```

## Verification checklist

After the run, confirm in this order:

1. **Render logs** — `applied=N, failed=0`.
2. **`logs/bidmachine_floor_cut_apr26.json`** — full plan + outcome per
   change. (On Render, this is on the worker's disk; for cross-restart
   visibility check the floor ledger instead.)
3. **Floor ledger** — every change tagged
   `actor=bidmachine_floor_cut_apr26`.
4. **LL UI spot-check** — open 2-3 demand IDs from the summary, confirm
   `minBidFloor = $2.00`.
5. **24h watch** — BidMachine Display win rate climbing toward 8-10%;
   `auto_revert_harmful` not flagging the changes; `verifier` reports
   `verified` (not `drifted` / `reverted`).

## Rollback

If you need to undo before `auto_revert_harmful` does it for you:

```sh
# Quickest: list everything this run wrote and restore from ledger
python3 -c "
from core import floor_ledger, ll_mgmt
for row in floor_ledger.read_all():
    if row.get('actor') == 'bidmachine_floor_cut_apr26' and row.get('applied'):
        ll_mgmt.set_demand_floor(
            row['demand_id'], row['old_floor'],
            verify=True, allow_multi_pub=True, _publishers_running_it=10,
        )
        floor_ledger.record(
            publisher_id=row['publisher_id'], publisher_name=row['publisher_name'],
            demand_id=row['demand_id'], demand_name=row['demand_name'],
            old_floor=row['new_floor'], new_floor=row['old_floor'],
            actor='bidmachine_floor_cut_apr26_rollback',
            reason='Manual rollback of apr26 floor cut',
            dry_run=False, applied=True,
        )
"
```

## Future work captured (not built)

1. **Pubmatic relationship review (D + E)** — tighten
   `agents/alerts/partner_churn_radar.py` thresholds for a
   `CRITICAL_PARTNERS` watchlist, and build a `pubmatic_diagnostic`
   one-shot that pulls 14d daily revenue, eCPM, bid response rate, win
   rate by country/format and writes a markdown brief for the Monday
   review.
2. **Block-list / allow-list review agent** — scan current block & allow
   lists, cross-reference domains/apps performing well on other demand
   partners, surface unblock candidates. Fits next to
   `agents/optimization/dead_demand.py` and `config_health_scanner.py`.
3. **GitHub Actions `workflow_dispatch` for one-shot scripts** —
   mobile-friendly trigger path, requires copying LL secrets from
   Render to GitHub Secrets (one-time, needs Render access).

Branch: `claude/setup-desktop-repo-access-Re5Rh`
PR: <https://github.com/mastap150/pgam-intelligence/pull/20>
