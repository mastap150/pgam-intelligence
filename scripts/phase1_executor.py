"""
scripts/phase1_executor.py

Phase-1 of the Revenue Scaling Plan (docs/revenue_scaling_plan_apr17.md).

Target: lift combined daily revenue from ~$7.2k → $10k by 2026-05-01.

What this script does (one-shot, non-idempotent where marked):

  P1.1a  Algorix Display and Video       → activate Pubmatic, Magnite, Unruly floors
  P1.1b  Illumin Display & Video          → activate Unruly, Pubmatic, Stirista, Verve floors
  P1.1c  BidMachine - In App Interstitial → set meaningful floors on $11 eCPM publisher
  P1.1d  BidMachine - In App D&V          → activate secondary demands (Magnite, Illumin)
                                            WITHOUT touching Pubmatic (let apr16 results settle)

All changes are demand-level minBidFloor edits in biddingpreferences — exactly
the same technique that worked on apr16. Adunit-level bidFloor is NOT changed,
because adunits are shared across publishers and a change there would affect
every publisher that references the adunit.

Safety:
  - Default is --dry-run. No write happens without --live.
  - Every change is only_if_none unless explicitly marked overwrite=True.
    This guarantees we never step on an existing hand-tuned floor.
  - Every action logs to logs/pilot_2026-04.json under today's date.
  - A per-run JSON goes to logs/phase1_results_apr17.json.

Run:
  python3 scripts/phase1_executor.py              # dry-run (default)
  python3 scripts/phase1_executor.py --live       # apply
  python3 scripts/phase1_executor.py --only 290115313  # one publisher
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

import core.ll_mgmt as llm

LOG_PATH = os.path.join(_REPO_ROOT, "logs", "pilot_2026-04.json")
RESULTS_PATH = os.path.join(_REPO_ROOT, "logs", "phase1_results_apr17.json")


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


def apply_demand_floor_plan(
    pub_id: int,
    pub_label: str,
    strategy: str,
    demand_changes: list,  # [(demand_id, label, new_floor, overwrite_existing), ...]
    dry_run: bool,
) -> dict:
    """
    For a single publisher, apply a planned set of demand-level floor changes.

    demand_changes entries:
      (demand_id, label_for_audit, new_floor, overwrite_existing)
      overwrite_existing=False  → only_if_none (never touch a pre-set floor)
      overwrite_existing=True   → always set, even if a floor already exists
    """
    result = {
        "action": "phase1_executor_apr17",
        "strategy": strategy,
        "publisher_id": pub_id,
        "publisher_label": pub_label,
        "timestamp": _now_iso(),
        "applied": False,
        "dry_run": dry_run,
        "changes": [],
        "skipped": [],
    }

    try:
        pub = llm.get_publisher(pub_id)
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
                demand_map[did] = v

    modified = False
    for demand_id, demand_label, new_floor, overwrite in demand_changes:
        v = demand_map.get(demand_id)
        if v is None:
            print(f"  ⚠  [{pub_name}] demand_id={demand_id} ({demand_label}) NOT FOUND — skip")
            result["skipped"].append({
                "demand_id": demand_id, "label": demand_label, "reason": "not_found",
            })
            continue

        old_floor = v.get("minBidFloor")
        status    = v.get("status")

        if status == 2:
            print(f"  ·  [{pub_name}] {demand_label} demand_id={demand_id} PAUSED — skip")
            result["skipped"].append({
                "demand_id": demand_id, "label": demand_label, "reason": "paused",
            })
            continue

        if not overwrite and old_floor is not None:
            print(f"  ·  [{pub_name}] {demand_label} demand_id={demand_id} "
                  f"already has floor={old_floor} — skip (only_if_none)")
            result["skipped"].append({
                "demand_id": demand_id, "label": demand_label,
                "reason": f"already_set_{old_floor}",
            })
            continue

        if old_floor == new_floor:
            print(f"  ·  [{pub_name}] {demand_label} demand_id={demand_id} "
                  f"floor already {old_floor} — no change")
            continue

        change = {
            "demand_id": demand_id,
            "demand_name": demand_label,
            "old_floor": old_floor,
            "new_floor": new_floor,
        }
        result["changes"].append(change)

        direction = "→" if old_floor is None else ("↑" if new_floor > (old_floor or 0) else "↓")
        tag = "DRY_RUN" if dry_run else "✓"
        print(f"  {tag}  [{pub_name}] {demand_label[:35]:<35} id={demand_id:<6} "
              f"minBidFloor {old_floor} {direction} {new_floor}")

        if not dry_run:
            v["minBidFloor"] = new_floor
            modified = True

    if not result["changes"]:
        return result

    if not dry_run and modified:
        try:
            llm._put(f"/v1/publishers/{pub_id}", pub)
            result["applied"] = True
            log_action(result)
        except Exception as e:
            print(f"  ✗ [{pub_name}] PUT failed: {e}")
            result["error"] = str(e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 CHANGE SPECIFICATION
# Every tuple is (demand_id, label, new_floor, overwrite_existing)
# ─────────────────────────────────────────────────────────────────────────────

PLAN = [
    # ─── P1.1a — Algorix Display and Video (id=290115313) ─────────────────────
    # 7d: $8,248 rev, 22.1% WR, $1.89 eCPM, all revenue from Pubmatic
    # Strategy: modestly raise Pubmatic floors (None→$0.80), activate Magnite
    # and Unruly with low floors to invite more competition
    {
        "publisher_id": 290115313,
        "publisher_label": "Algorix Display and Video",
        "strategy": "Activate + modest raise — 22% WR @ $1.89 eCPM",
        "demand_changes": [
            # Pubmatic — None → $0.80  (raise clearing price on dominant demand)
            (919, "Pubmatic 728x90 New EP",     0.80, False),
            (929, "Pubmatic 300x250 New EP",    0.80, False),
            (918, "Pubmatic 320x50 New EP",     0.80, False),
            # Magnite — None → $0.60  (activate so they start bidding)
            (529, "Magnite Algorix Display",                      0.60, False),
            (530, "Magnite Algorix Display oRTB 2.6",             0.60, False),
            (531, "Magnite Algorix Display Prebid Server",        0.60, False),
            (532, "Magnite Algorix Display PS oRTB 2.6",          0.60, False),
            # Unruly — None → $0.50
            (537, "Unruly Algorix",                               0.50, False),
            (538, "Unruly Algorix oRTB 2.6",                      0.50, False),
            (539, "Unruly Algorix Prebid Server",                 0.50, False),
            (540, "Unruly Algorix Prebid Server oRTB 2.6",        0.50, False),
        ],
    },

    # ─── P1.1b — Illumin Display & Video (id=290115268) ───────────────────────
    # 7d: $3,754 rev, 11.3% WR, $0.59 eCPM
    # Verve is 57% of revenue at $0.50 eCPM, Unruly 12% at $2.09 eCPM, Stirista 8% at $1.95
    # Strategy: don't touch Verve (biggest pool, lowest eCPM — could collapse).
    # Lift Unruly + Stirista + Pubmatic; leave Xandr $2 alone.
    {
        "publisher_id": 290115268,
        "publisher_label": "Illumin Display & Video",
        "strategy": "Activate high-eCPM secondaries — $2.09 Unruly, $1.95 Stirista",
        "demand_changes": [
            # Unruly — was 8 @ $0.2 → raise to $0.80 (eCPM $2.09, 21% WR)
            (8,   "Unruly Illumin",                      0.80, True),
            # Copy-Unruly (446) — activate to $0.60
            (446, "Copy Unruly Illumin 10%",             0.60, False),
            # Stirista — activate to $0.80 (67% WR, $1.95 eCPM)
            (449, "Stirista General Illumin",            0.80, False),
            (437, "Stirista Business Illumin",           0.80, False),
            (436, "Stirista Sports Illumin",             0.80, False),
            # Pubmatic — None → $0.40 ($0.50 eCPM on Verve means Pubmatic clearing is similar)
            (51,  "Pubmatic RON 300x250 PS",             0.40, False),
            (52,  "Pubmatic RON 728x90 PS",              0.40, False),
            (919, "Pubmatic 728x90 New EP",              0.40, False),
            (929, "Pubmatic 300x250 New EP",             0.40, False),
            (918, "Pubmatic 320x50 New EP",              0.40, False),
            # Magnite — set low floors to encourage bidding
            (41,  "Magnite Illumin Display PS",          0.40, False),
            (440, "Magnite Illumin Display PS (copy)",   0.40, False),
            (442, "Magnite Illumin Display 10%",         0.40, False),
            (678, "Magnite Illumin Display 10% West",    0.40, False),
            # Illumin-US was 0.5 → raise to 0.75 (keeps activity, pushes price)
            (788, "Illumin US",                          0.75, True),
        ],
    },

    # ─── P1.1c — BidMachine - In App Interstitial (id=290115333) ──────────────
    # 7d: $1,669 rev, 4.1% WR, $11.53 eCPM (!!) — highest eCPM in portfolio
    # Magnite $13.63 eCPM, Xandr $14.50 eCPM, LoopMe $3.19 at 23% WR
    # Strategy: aggressively floor Magnite+Xandr (they're already clearing high),
    # floor LoopMe to encourage scale, invite Stirista/Pubmatic to bid.
    {
        "publisher_id": 290115333,
        "publisher_label": "BidMachine - In App Interstitial",
        "strategy": "High-value floor activation — $11+ eCPM publisher",
        "demand_changes": [
            # Magnite — None → $3.00 (they clear $13.63, 5% WR, easy room)
            (604, "Magnite BidMachine In App",                  3.00, False),
            (605, "Magnite BidMachine In App oRTB 2.6",         3.00, False),
            (606, "Magnite BidMachine In App PS",               3.00, False),
            (607, "Magnite BidMachine In App PS oRTB 2.6",      3.00, False),
            (671, "Magnite BidMachine In App US West",          3.00, False),
            (672, "Magnite BidMachine In App oRTB 2.6 US West", 3.00, False),
            (673, "Magnite BidMachine In App PS US West",       3.00, False),
            (674, "Magnite BidMachine In App PS oRTB 2.6 West", 3.00, False),
            # Stirista — None → $2.00 (likely to scale OLV here)
            (682, "Stirista General BidMachine",        2.00, False),
            (683, "Stirista General OLV BidMachine",    2.00, False),
            # Pubmatic — None → $2.00 (let them compete for these high-value impressions)
            (45,  "Pubmatic RON 320x50 PS",             2.00, False),
            (46,  "Pubmatic RON 320x100 PS",            2.00, False),
            # Illumin-US was 0.5 → 1.50
            (788, "Illumin US",                         1.50, True),
        ],
    },

    # ─── P1.1d — BidMachine - In App D&V (id=290115319) ───────────────────────
    # 7d: $11,782 rev, 6.9% WR, $1.68 eCPM (biggest publisher)
    # NOT touched on apr16 Pubmatic side — stay conservative here
    # Strategy: activate Magnite + Illumin floors only, DO NOT touch Pubmatic
    # (give apr16 signal 48h to settle)
    {
        "publisher_id": 290115319,
        "publisher_label": "BidMachine - In App Display & Video",
        "strategy": "Pubmatic hedge: activate Magnite+Illumin only",
        "demand_changes": [
            # Magnite — None → $0.40 (currently 7.7% WR, $0.35 eCPM — tight)
            (604, "Magnite BidMachine In App",                  0.40, False),
            (605, "Magnite BidMachine In App oRTB 2.6",         0.40, False),
            (606, "Magnite BidMachine In App PS",               0.40, False),
            (607, "Magnite BidMachine In App PS oRTB 2.6",      0.40, False),
            (671, "Magnite BidMachine In App US West",          0.40, False),
            (672, "Magnite BidMachine In App oRTB 2.6 US West", 0.40, False),
            (673, "Magnite BidMachine In App PS US West",       0.40, False),
            (674, "Magnite BidMachine In App PS oRTB 2.6 West", 0.40, False),
            # Illumin-US: was 0.5 → 0.80 (16.7% WR, $2.40 eCPM — safely raise)
            (788, "Illumin US",                                  0.80, True),
        ],
    },
]


def main():
    p = argparse.ArgumentParser(description="PGAM Revenue Phase-1 Executor")
    p.add_argument("--live", action="store_true",
                   help="Apply changes (default: dry-run)")
    p.add_argument("--only", type=str, default=None,
                   help="Comma-separated publisher IDs to limit the run")
    args = p.parse_args()

    dry_run = not args.live
    only_ids = set(int(x) for x in args.only.split(",")) if args.only else None

    mode = "LIVE" if not dry_run else "DRY-RUN"
    print("=" * 72)
    print(f"  PHASE-1 EXECUTOR — {mode} — {_today()}")
    print(f"  target: lift combined $7.2k → $10k/day by 2026-05-01")
    print("=" * 72)

    results = []

    for step_idx, step in enumerate(PLAN, 1):
        pub_id = step["publisher_id"]
        if only_ids is not None and pub_id not in only_ids:
            continue

        print(f"\n[{step_idx}/{len(PLAN)}] {step['publisher_label']} (id={pub_id})")
        print(f"      strategy: {step['strategy']}")
        print(f"      changes planned: {len(step['demand_changes'])}")

        r = apply_demand_floor_plan(
            pub_id=pub_id,
            pub_label=step["publisher_label"],
            strategy=step["strategy"],
            demand_changes=step["demand_changes"],
            dry_run=dry_run,
        )
        results.append(r)

    total_changes = sum(len(r.get("changes", [])) for r in results)
    total_skipped = sum(len(r.get("skipped", [])) for r in results)
    errors = sum(1 for r in results if r.get("error"))

    print("\n" + "=" * 72)
    print(f"  SUMMARY:  {total_changes} changes planned across {len(results)} publishers")
    print(f"            {total_skipped} skipped (already-set / paused / not found)")
    if errors:
        print(f"            ⚠  {errors} publisher(s) had errors")
    if dry_run:
        print("\n  ⚡ DRY-RUN — nothing was changed.")
        print("     Re-run with --live to apply.")
    else:
        print("\n  ✓ LIVE — changes applied to LL. Monitor next 48h.")
    print("=" * 72)

    if not dry_run and results:
        os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Results → {RESULTS_PATH}\n")


if __name__ == "__main__":
    main()
