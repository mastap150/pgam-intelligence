"""
scripts/startio_floor_optimizer_apr17.py

Floor optimizer for Start.IO demand entries on LL.

Background
----------
LL's reporting pipeline drops WINS events for Start.IO adapters (and ~7
other partners), so the standard floor optimizers see Start.IO as 0% win
rate and skip it. With the IMPRESSIONS-as-WINS proxy now applied at the
data layer (core/ll_report.py, core/api.py — see _patch_zero_wins), we
can finally compute meaningful win-rate / eCPM signals for Start.IO.

This script:
  1. Pulls POST /v1/report rows for PUBLISHER × DEMAND across all 48
     Start.IO demand entries (POST API is all-time — see core/ll_report.py
     docstring; that's fine for floor decisions, eCPM is stable).
  2. Computes proxy_win_rate = IMPRESSIONS/BIDS and effective_ecpm =
     (REVENUE/IMPRESSIONS)*1000 per (publisher, demand) entry.
  3. Joins with current floors from biddingpreferences (core.ll_mgmt).
  4. Recommends: ACTIVATE (null floor → set), RAISE (high WR + headroom),
     LOWER (low WR + revenue, gated behind --allow-lower), or HOLD.
  5. Dry-runs by default. Pass --live to push changes via core.ll_mgmt.

Heuristic
---------
For each (publisher, demand) row:
  • bids < 500 OR imps < 50  → SKIP (insufficient signal)
  • rev < $0.50              → SKIP (no economic value)
  • current_floor is None    → ACTIVATE at clamp(eff_ecpm * 0.40, $0.20, $2.00)
  • proxy_wr ≥ 30% AND eff_ecpm ≥ 2.5 × current_floor → RAISE to
                                min(eff_ecpm * 0.50, current_floor * 1.50)
  • proxy_wr ≤ 5%  AND rev > $5  → LOWER to current_floor * 0.70
                                   (only with --allow-lower)
  • else                      → HOLD

Run:
    python3.13 scripts/startio_floor_optimizer_apr17.py             # dry-run
    python3.13 scripts/startio_floor_optimizer_apr17.py --live      # apply
    python3.13 scripts/startio_floor_optimizer_apr17.py --allow-lower
    python3.13 scripts/startio_floor_optimizer_apr17.py --only 290115375
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_ROOT, ".env"), override=True)

import core.ll_mgmt as llm
import core.ll_report as llr

LOG_PATH      = os.path.join(_ROOT, "logs", "pilot_2026-04.json")
RESULTS_PATH  = os.path.join(_ROOT, "logs", "startio_floor_results_apr17.json")
ANALYSIS_PATH = os.path.join(_ROOT, "logs", "startio_floor_analysis_apr17.json")

# Heuristic thresholds — change here, not inline
MIN_BIDS              = 500
MIN_IMPS              = 50
MIN_REV               = 0.50
ACTIVATE_PCT_OF_ECPM  = 0.40   # 40% of effective eCPM
ACTIVATE_FLOOR_MIN    = 0.20
ACTIVATE_FLOOR_MAX    = 2.00
RAISE_WR_THRESHOLD    = 0.30
RAISE_HEADROOM_X      = 2.5    # eff_ecpm must be ≥ this × current_floor
RAISE_TARGET_PCT      = 0.50
RAISE_CAP_PCT         = 1.50   # cap at 1.5× current floor
LOWER_WR_THRESHOLD    = 0.05
LOWER_REV_THRESHOLD   = 5.00
LOWER_PCT             = 0.70


def _sf(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _today():
    return datetime.now().strftime("%Y-%m-%d")


def log_action(action_dict: dict):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    data = []
    if os.path.exists(LOG_PATH):
        try:
            with open(LOG_PATH) as f:
                data = json.load(f)
        except Exception:
            data = []
    if isinstance(data, dict):
        data = list(data.values())
    today = _today()
    entry = next((e for e in data if e.get("date") == today), None)
    if entry is None:
        entry = {"date": today, "actions_applied": []}
        data.append(entry)
    entry.setdefault("actions_applied", []).append(action_dict)
    with open(LOG_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Recommendation engine
# ---------------------------------------------------------------------------

def recommend(row: dict, current_floor: float | None, allow_lower: bool):
    """
    Return (action, new_floor, rationale) where action ∈
    {SKIP, HOLD, ACTIVATE, RAISE, LOWER}.
    """
    bids = _sf(row.get("BIDS"))
    imps = _sf(row.get("IMPRESSIONS"))
    rev  = _sf(row.get("GROSS_REVENUE"))

    if bids < MIN_BIDS or imps < MIN_IMPS:
        return ("SKIP", None, f"insufficient signal (bids={bids:.0f}, imps={imps:.0f})")
    if rev < MIN_REV:
        return ("SKIP", None, f"low revenue (${rev:.2f})")

    proxy_wr = imps / bids
    eff_ecpm = (rev / imps) * 1000

    # ACTIVATE — no current floor set
    if current_floor is None:
        new_floor = max(ACTIVATE_FLOOR_MIN,
                        min(eff_ecpm * ACTIVATE_PCT_OF_ECPM, ACTIVATE_FLOOR_MAX))
        new_floor = round(new_floor, 2)
        return ("ACTIVATE", new_floor,
                f"null floor; eff_ecpm=${eff_ecpm:.2f} → activate at {ACTIVATE_PCT_OF_ECPM*100:.0f}%")

    # RAISE — strong WR + headroom
    if proxy_wr >= RAISE_WR_THRESHOLD and eff_ecpm >= current_floor * RAISE_HEADROOM_X:
        target = min(eff_ecpm * RAISE_TARGET_PCT, current_floor * RAISE_CAP_PCT)
        target = round(target, 2)
        if target > current_floor:
            return ("RAISE", target,
                    f"WR={proxy_wr*100:.1f}%, eff_ecpm=${eff_ecpm:.2f} (>{RAISE_HEADROOM_X}× floor)")

    # LOWER — wins/clearing choking demand (gated)
    if proxy_wr <= LOWER_WR_THRESHOLD and rev > LOWER_REV_THRESHOLD:
        if allow_lower:
            target = round(current_floor * LOWER_PCT, 2)
            if target < current_floor and target >= 0.05:
                return ("LOWER", target,
                        f"WR={proxy_wr*100:.1f}% choking $${rev:.0f} rev — drop {(1-LOWER_PCT)*100:.0f}%")
        else:
            return ("HOLD", None,
                    f"WR={proxy_wr*100:.1f}% suggests LOWER but --allow-lower not set")

    return ("HOLD", None, f"WR={proxy_wr*100:.1f}%, eff_ecpm=${eff_ecpm:.2f}, floor=${current_floor:.2f}")


# ---------------------------------------------------------------------------
# Data load
# ---------------------------------------------------------------------------

def load_startio_rows() -> list[dict]:
    """Pull patched Start.IO rows from POST /v1/report (all-time)."""
    end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    start = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    rows = llr.report(
        ["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"],
        ["BIDS", "WINS", "IMPRESSIONS", "GROSS_REVENUE", "PUB_PAYOUT", "GROSS_ECPM"],
        start, end,
    )
    return [r for r in rows if "start" in str(r.get("DEMAND_NAME", "")).lower()]


def get_publisher_demand_floors(publisher_id: int) -> tuple[dict, dict]:
    """
    Return (publisher_object, {demand_id: demand_value_dict_with_floor}).
    """
    pub = llm.get_publisher(publisher_id)
    demand_map = {}
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if did is not None:
                demand_map[int(did)] = v
    return pub, demand_map


# ---------------------------------------------------------------------------
# Plan build
# ---------------------------------------------------------------------------

def build_plan(allow_lower: bool, only_pubs: set[int] | None = None):
    rows = load_startio_rows()
    print(f"Loaded {len(rows)} Start.IO (publisher × demand) rows from LL.\n")

    # Group rows by publisher
    by_pub: dict[int, list[dict]] = defaultdict(list)
    pub_names: dict[int, str] = {}
    for r in rows:
        try:
            pid = int(r.get("PUBLISHER_ID") or 0)
        except (TypeError, ValueError):
            continue
        if not pid:
            continue
        if only_pubs and pid not in only_pubs:
            continue
        by_pub[pid].append(r)
        pub_names[pid] = str(r.get("PUBLISHER_NAME") or f"pub-{pid}")

    # Rank publishers by total Start.IO revenue (high → low)
    pub_rev = {pid: sum(_sf(r.get("GROSS_REVENUE")) for r in rs)
               for pid, rs in by_pub.items()}
    ranked = sorted(by_pub.keys(), key=lambda p: -pub_rev[p])

    plan: list[dict] = []
    analysis_rows: list[dict] = []

    for pid in ranked:
        prows = by_pub[pid]
        pub_name = pub_names[pid]
        try:
            pub_obj, demand_map = get_publisher_demand_floors(pid)
        except Exception as e:
            print(f"  ⚠  fetch error pub={pid} ({pub_name}): {e}")
            continue

        actions: list[tuple] = []  # (demand_id, demand_label, new_floor, overwrite, action, rationale, pre_metrics)
        for r in sorted(prows, key=lambda x: -_sf(x.get("GROSS_REVENUE"))):
            try:
                did = int(r.get("DEMAND_ID") or 0)
            except (TypeError, ValueError):
                continue
            if not did:
                continue
            dname = str(r.get("DEMAND_NAME") or f"demand-{did}")

            v = demand_map.get(did)
            current_floor = _sf(v.get("minBidFloor")) if (v and v.get("minBidFloor") is not None) else None
            status = v.get("status") if v else None

            metrics = dict(
                bids=_sf(r.get("BIDS")),
                imps=_sf(r.get("IMPRESSIONS")),
                rev=_sf(r.get("GROSS_REVENUE")),
                proxy_wr=(_sf(r.get("IMPRESSIONS")) / _sf(r.get("BIDS"))) if _sf(r.get("BIDS")) else 0.0,
                eff_ecpm=(_sf(r.get("GROSS_REVENUE")) / _sf(r.get("IMPRESSIONS")) * 1000) if _sf(r.get("IMPRESSIONS")) else 0.0,
                current_floor=current_floor,
                status=status,
            )
            analysis_rows.append({
                "publisher_id": pid, "publisher_name": pub_name,
                "demand_id": did, "demand_name": dname, **metrics,
            })

            if v is None:
                # demand entry not present in this publisher's biddingprefs
                continue
            if status == 2:
                continue  # paused

            action, new_floor, rationale = recommend(r, current_floor, allow_lower)
            if action in ("ACTIVATE", "RAISE", "LOWER"):
                # only_if_none for ACTIVATE; overwrite=True for RAISE/LOWER
                overwrite = action in ("RAISE", "LOWER")
                actions.append((did, dname, new_floor, overwrite, action, rationale, metrics))

        if actions:
            plan.append({
                "publisher_id": pid,
                "publisher_label": pub_name,
                "publisher_revenue_startio": pub_rev[pid],
                "actions": actions,
            })

    return plan, analysis_rows


# ---------------------------------------------------------------------------
# Print + execute
# ---------------------------------------------------------------------------

def print_analysis(analysis_rows: list[dict]):
    print("=" * 110)
    print("  START.IO PER-DEMAND ANALYSIS (all-time, POST /v1/report — date filter is broken upstream)")
    print("=" * 110)
    hdr = f"  {'PUBLISHER':<32} {'DEMAND':<40} {'BIDS':>10} {'IMPS':>9} {'REV':>8} {'WR%':>6} {'eCPM':>7} {'FLR':>6}"
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    for r in sorted(analysis_rows, key=lambda x: (-x.get("rev", 0))):
        if r["rev"] < 1.0:
            continue
        flr = f"${r['current_floor']:.2f}" if r["current_floor"] is not None else "  -"
        print(f"  {r['publisher_name'][:31]:<32} {r['demand_name'][:39]:<40} "
              f"{r['bids']:>10,.0f} {r['imps']:>9,.0f} ${r['rev']:>6.0f} "
              f"{r['proxy_wr']*100:>5.1f}% ${r['eff_ecpm']:>5.2f} {flr:>6}")


def apply_plan(step: dict, dry_run: bool):
    pid = step["publisher_id"]
    pub_label = step["publisher_label"]

    result = {
        "action": "startio_floor_optimizer_apr17",
        "publisher_id": pid,
        "publisher_label": pub_label,
        "timestamp": _now_iso(),
        "applied": False,
        "dry_run": dry_run,
        "changes": [],
        "skipped": [],
    }

    try:
        pub = llm.get_publisher(pid)
    except Exception as e:
        print(f"  ✗ [{pub_label}] fetch error: {e}")
        result["error"] = str(e)
        return result

    pub_name = pub.get("name", pub_label)
    demand_map = {}
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if did is not None:
                demand_map[int(did)] = v

    modified = False
    for did, dname, new_floor, overwrite, action, rationale, metrics in step["actions"]:
        v = demand_map.get(did)
        if v is None:
            result["skipped"].append({"demand_id": did, "label": dname, "reason": "not_found"})
            continue
        old_floor = v.get("minBidFloor")
        status = v.get("status")
        if status == 2:
            result["skipped"].append({"demand_id": did, "label": dname, "reason": "paused"})
            continue
        if not overwrite and old_floor is not None:
            result["skipped"].append({"demand_id": did, "label": dname, "reason": f"already_set_{old_floor}"})
            continue
        if old_floor == new_floor:
            continue

        change = {
            "demand_id": did, "demand_name": dname,
            "old_floor": old_floor, "new_floor": new_floor,
            "action": action, "rationale": rationale,
            "metrics": metrics,
        }
        result["changes"].append(change)
        direction = "→" if old_floor is None else ("↑" if new_floor > _sf(old_floor) else "↓")
        tag = "DRY_RUN" if dry_run else "✓"
        print(f"  {tag} [{action:<8}] [{pub_name[:24]:<24}] {dname[:38]:<38} "
              f"id={did:<5} {old_floor} {direction} ${new_floor:.2f}  ({rationale})")

        if not dry_run:
            v["minBidFloor"] = new_floor
            modified = True

    if not dry_run and modified and result["changes"]:
        try:
            llm._put(f"/v1/publishers/{pid}", pub)
            result["applied"] = True
            log_action(result)
        except Exception as e:
            print(f"  ✗ [{pub_name}] PUT failed: {e}")
            result["error"] = str(e)
            return result

        # Register a watchdog entry so the next 7d of performance is auto-monitored.
        # Only after a successful PUT — never on dry-runs or failed applies.
        try:
            from scripts.pilot_watchdog import register_startio_watch
            watch_id = register_startio_watch(
                publisher_id=pid,
                publisher_name=pub_name,
                changes=result["changes"],
            )
            result["watch_id"] = watch_id
            print(f"  ✦ registered watchdog {watch_id} (Start.IO baseline captured)")
        except Exception as e:
            # Don't fail the whole run if watchdog registration breaks —
            # the floor change already landed, that's the important part.
            print(f"  ⚠  watchdog register failed (floors are still applied): {e}")
            result["watch_register_error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="Start.IO Floor Optimizer (LL)")
    p.add_argument("--live", action="store_true", help="Apply changes (default: dry-run)")
    p.add_argument("--only", type=str, default=None, help="Comma-separated publisher IDs")
    p.add_argument("--allow-lower", action="store_true", help="Permit LOWER floor recommendations")
    p.add_argument("--no-analysis", action="store_true", help="Skip the per-demand analysis table")
    args = p.parse_args()

    dry_run = not args.live
    only_pubs = set(int(x) for x in args.only.split(",")) if args.only else None

    mode = "LIVE" if not dry_run else "DRY-RUN"
    print("=" * 110)
    print(f"  START.IO FLOOR OPTIMIZER — {mode} — {_today()}  "
          f"(allow_lower={args.allow_lower})")
    print(f"  data: POST /v1/report all-time, WINS proxied via IMPRESSIONS (see core/ll_report.py)")
    print("=" * 110 + "\n")

    plan, analysis_rows = build_plan(allow_lower=args.allow_lower, only_pubs=only_pubs)

    if not args.no_analysis:
        print_analysis(analysis_rows)

    # Persist analysis snapshot
    os.makedirs(os.path.dirname(ANALYSIS_PATH), exist_ok=True)
    with open(ANALYSIS_PATH, "w") as f:
        json.dump({"date": _today(), "rows": analysis_rows}, f, indent=2, default=str)
    print(f"\n  analysis snapshot → {ANALYSIS_PATH}")

    if not plan:
        print("\n  No actionable changes found. Done.")
        return

    print("\n" + "=" * 110)
    print(f"  RECOMMENDED CHANGES — {sum(len(s['actions']) for s in plan)} actions across "
          f"{len(plan)} publishers (ranked by Start.IO revenue)")
    print("=" * 110)

    results = []
    for step_idx, step in enumerate(plan, 1):
        print(f"\n[{step_idx}/{len(plan)}] {step['publisher_label']} (id={step['publisher_id']})  "
              f"— ${step['publisher_revenue_startio']:.2f} Start.IO rev")
        print(f"      {len(step['actions'])} candidate changes")
        r = apply_plan(step, dry_run=dry_run)
        results.append(r)

    total_changes = sum(len(r.get("changes", [])) for r in results)
    total_skipped = sum(len(r.get("skipped", [])) for r in results)
    by_action: dict[str, int] = defaultdict(int)
    for r in results:
        for c in r.get("changes", []):
            by_action[c["action"]] += 1
    errors = sum(1 for r in results if r.get("error"))

    print("\n" + "=" * 110)
    print(f"  SUMMARY: {total_changes} changes ({dict(by_action)}) | "
          f"{total_skipped} skipped | {errors} errors")
    if dry_run:
        print("  ⚡ DRY-RUN — no changes pushed. Re-run with --live to apply.")
    else:
        print("  ✓ LIVE — changes pushed to LL. Monitor next 48h.")
    print("=" * 110)

    if not dry_run and results:
        os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n  results → {RESULTS_PATH}\n")


if __name__ == "__main__":
    main()
