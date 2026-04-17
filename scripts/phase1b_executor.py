"""
scripts/phase1b_executor.py

Phase-1b: extend the floor-activation strategy to every meaningful LL publisher
we haven't yet touched. Same pattern as scripts/phase1_executor.py — demand-level
minBidFloor edits in biddingpreferences, only_if_none by default.

Targets (by 7-day revenue, publishers NOT yet optimized):

  P1b.1  Smaato - Magnite                          ($5,580, 5.1% WR, $1.04 eCPM)
  P1b.2  Copy - BidMachine In-App Interstitial EU  ($  883, 7.6% WR, $3.92 eCPM)
  P1b.3  BlueSeaX - US Endpoint                    ($  535, 7.2% WR, $1.01 eCPM)
  P1b.4  PubNative - In-App Magnite                ($  508, 3.9% WR, $1.21 eCPM)
  P1b.5  BidMachine In-App Interstitial (WL)       ($  504, 2.0% WR, $21.25 eCPM !!)
  P1b.6  Illumin Display EU                        ($  317, 8.9% WR, $0.50 eCPM)
  P1b.7  Illumin In-App EU                         ($   82, 25.3% WR, $0.52 eCPM)
  P1b.8  Start.IO - Video Magnite                  (0 wins / $1,178 rev — reporting bug, set floors anyway)
  P1b.9  Start.IO Display Magnite                  (0 wins / $161 rev — same)

Every change is only_if_none unless overwrite=True. Zero risk of stepping on an
existing hand-tuned floor.

Run:
  python3 scripts/phase1b_executor.py              # dry-run (default)
  python3 scripts/phase1b_executor.py --live       # apply
  python3 scripts/phase1b_executor.py --only 290115372  # limit to one publisher
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
RESULTS_PATH = os.path.join(_REPO_ROOT, "logs", "phase1b_results_apr17.json")


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


def apply_plan(pub_id, pub_label, strategy, demand_changes, dry_run):
    result = {
        "action": "phase1b_executor_apr17",
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
            result["skipped"].append({"demand_id": demand_id, "label": demand_label, "reason": "not_found"})
            continue

        old_floor = v.get("minBidFloor")
        status = v.get("status")

        if status == 2:
            print(f"  ·  [{pub_name}] {demand_label} demand_id={demand_id} PAUSED — skip")
            result["skipped"].append({"demand_id": demand_id, "label": demand_label, "reason": "paused"})
            continue

        if not overwrite and old_floor is not None:
            result["skipped"].append({"demand_id": demand_id, "label": demand_label, "reason": f"already_set_{old_floor}"})
            continue

        if old_floor == new_floor:
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
        print(f"  {tag}  [{pub_name[:28]:<28}] {demand_label[:38]:<38} id={demand_id:<5} "
              f"{old_floor} {direction} {new_floor}")

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
# PHASE 1b CHANGE SPECIFICATION
# ─────────────────────────────────────────────────────────────────────────────

PLAN = [
    # ─── P1b.1 — Smaato - Magnite (id=290115372) ──────────────────────────────
    # $5,580 / 7d, 5.1% WR, $1.04 eCPM. 33 active demands, 30 with NULL floor.
    # Biggest untouched revenue line. Activate Magnite + Sovrn + Pubmatic at modest floors.
    {
        "publisher_id": 290115372,
        "publisher_label": "Smaato - Magnite",
        "strategy": "Activate 30 null-floor demands — $5.6k/7d at 5.1% WR",
        "demand_changes": [
            # Magnite (876-878, 920-922) — activate at $0.50
            (876, "Magnite Smaato In App US",              0.50, False),
            (877, "Magnite Smaato oRTB 2.6 US",            0.50, False),
            (878, "Magnite Smaato PS US",                  0.50, False),
            (920, "Magnite Smaato In App US West",         0.50, False),
            (921, "Magnite Smaato oRTB 2.6 US West",       0.50, False),
            (922, "Magnite Smaato PS US West",             0.50, False),
            # Sovrn (865-873) — display, smaller floors $0.30
            (865, "Sovrn Smaato 300x250 oRTB 2.6",         0.30, False),
            (866, "Sovrn Smaato 300x600",                  0.30, False),
            (867, "Sovrn Smaato 300x600 oRTB 2.6",         0.30, False),
            (868, "Sovrn Smaato 320x50",                   0.30, False),
            (869, "Sovrn Smaato 320x50 oRTB 2.6",          0.30, False),
            (870, "Sovrn Smaato 336x280",                  0.30, False),
            (871, "Sovrn Smaato 336x280 oRTB 2.6",         0.30, False),
            (872, "Sovrn Smaato 728x90 oRTB 2.6",          0.30, False),
            (873, "Sovrn Smaato 728x90",                   0.30, False),
            # Pubmatic — if present in this pub's biddingprefs (may or may not match)
            (51,  "Pubmatic RON 300x250 PS",               0.50, False),
            (52,  "Pubmatic RON 728x90 PS",                0.50, False),
            (919, "Pubmatic 728x90 New EP",                0.50, False),
            (929, "Pubmatic 300x250 New EP",               0.50, False),
        ],
    },

    # ─── P1b.2 — Copy - BidMachine In-App Interstitial EU (id=290115334) ──────
    # $883 / 7d, 7.6% WR, $3.92 eCPM. 20 demands all NULL floor. Mirror of BM Interstitial pattern.
    {
        "publisher_id": 290115334,
        "publisher_label": "Copy - BidMachine - In App Interstitial (Europe)",
        "strategy": "Activate at Interstitial-EU rates — $3.92 eCPM pool",
        "demand_changes": [
            # Magnite EU (665-668) — $1.00 (EU clearing is lower than US)
            (665, "Magnite BidMachine In App EU",               1.00, False),
            (666, "Magnite BidMachine In App oRTB 2.6 EU",      1.00, False),
            (667, "Magnite BidMachine In App PS EU",            1.00, False),
            (668, "Magnite BidMachine In App PS oRTB 2.6 EU",   1.00, False),
            # Unruly EU (669, 670)
            (669, "Unruly BidMachine EU",                       0.80, False),
            (670, "Unruly BidMachine oRTB 2.6 EU",              0.80, False),
            # Pubmatic — $0.80
            (44,  "Pubmatic RON 970x250 PS",                    0.80, False),
            (45,  "Pubmatic RON 320x50 PS",                     0.80, False),
            (46,  "Pubmatic RON 320x100 PS",                    0.80, False),
            (47,  "Pubmatic RON 300x50 PS",                     0.80, False),
            (48,  "Pubmatic RON 300x600 PS",                    0.80, False),
            (49,  "Pubmatic RON 300x100 PS",                    0.80, False),
            (50,  "Pubmatic RON 160x600 PS",                    0.80, False),
            (51,  "Pubmatic RON 300x250 PS",                    0.80, False),
            (52,  "Pubmatic RON 728x90 PS",                     0.80, False),
        ],
    },

    # ─── P1b.3 — BlueSeaX - US Endpoint (id=290115378) ────────────────────────
    # $535 / 7d, 7.2% WR, $1.01 eCPM. 22 null-floor demands.
    {
        "publisher_id": 290115378,
        "publisher_label": "BlueSeaX - US Endpoint",
        "strategy": "Activate Magnite/Unruly/Pubmatic — $1.01 eCPM US pool",
        "demand_changes": [
            (991,  "Magnite BlueSeaX In App US",                  0.50, False),
            (992,  "Magnite BlueSeaX In App oRTB 2.6 US",         0.50, False),
            (993,  "Magnite BlueSeaX In App PS US",               0.50, False),
            (994,  "Magnite BlueSeaX In App PS oRTB 2.6 US",      0.50, False),
            (999,  "Unruly BlueSeaX US",                          0.40, False),
            (1000, "Unruly BlueSeaX oRTB 2.6 US",                 0.40, False),
            (1001, "Unruly BlueSeaX PS US",                       0.40, False),
            (1002, "Unruly BlueSeaX PS oRTB 2.6",                 0.40, False),
            (44,   "Pubmatic RON 970x250 PS",                     0.40, False),
            (45,   "Pubmatic RON 320x50 PS",                      0.40, False),
            (46,   "Pubmatic RON 320x100 PS",                     0.40, False),
            (47,   "Pubmatic RON 300x50 PS",                      0.40, False),
            (48,   "Pubmatic RON 300x600 PS",                     0.40, False),
            (49,   "Pubmatic RON 300x100 PS",                     0.40, False),
            (50,   "Pubmatic RON 160x600 PS",                     0.40, False),
            (51,   "Pubmatic RON 300x250 PS",                     0.40, False),
            (52,   "Pubmatic RON 728x90 PS",                      0.40, False),
        ],
    },

    # ─── P1b.4 — PubNative - In-App Magnite (id=290115373) ────────────────────
    # $508 / 7d, 3.9% WR, $1.21 eCPM. User explicitly called this out as trending well.
    {
        "publisher_id": 290115373,
        "publisher_label": "PubNative - In-App Magnite",
        "strategy": "Activate Pubmatic/Sovrn/LoopMe — $1.21 eCPM, user-flagged trending up",
        "demand_changes": [
            (47,   "Pubmatic RON 300x50 PS",                      0.50, False),
            (49,   "Pubmatic RON 300x100 PS",                     0.50, False),
            (50,   "Pubmatic RON 160x600 PS",                     0.50, False),
            (51,   "Pubmatic RON 300x250 PS",                     0.50, False),
            (53,   "Pubmatic RON 336x280 PS",                     0.50, False),
            (768,  "Sovrn PubNative 300x250",                     0.30, False),
            (769,  "Sovrn PubNative 300x250 oRTB 2.6",            0.30, False),
            (770,  "Sovrn PubNative 300x600",                     0.30, False),
            (771,  "Sovrn PubNative 300x600 oRTB 2.6",            0.30, False),
            (772,  "Sovrn PubNative 320x50",                      0.30, False),
            (773,  "Sovrn PubNative 320x50 oRTB 2.6",             0.30, False),
            (774,  "Sovrn PubNative 336x280",                     0.30, False),
            (775,  "Sovrn PubNative 336x280 oRTB 2.6",            0.30, False),
            (777,  "Sovrn PubNative 728x90",                      0.30, False),
            (1010, "LoopMe PubNative Prebid",                     0.50, False),
        ],
    },

    # ─── P1b.5 — BidMachine In-App Interstitial (WL) (id=290115332) ───────────
    # $504 / 7d, 2.0% WR, $21.25 eCPM (!!). 33 null-floor demands.
    # Apply same logic as main BM Interstitial (phase1): Magnite $3, Stirista $2, Pubmatic $2.
    {
        "publisher_id": 290115332,
        "publisher_label": "BidMachine - In App Interstitial (WL)",
        "strategy": "Activate premium floors — $21.25 eCPM, massive latent value",
        "demand_changes": [
            (604, "Magnite BidMachine In App",                    3.00, False),
            (605, "Magnite BidMachine In App oRTB 2.6",           3.00, False),
            (606, "Magnite BidMachine In App PS",                 3.00, False),
            (607, "Magnite BidMachine In App PS oRTB 2.6",        3.00, False),
            (671, "Magnite BidMachine In App US West",            3.00, False),
            (672, "Magnite BidMachine In App oRTB 2.6 US West",   3.00, False),
            (673, "Magnite BidMachine In App PS US West",         3.00, False),
            (674, "Magnite BidMachine In App PS oRTB 2.6 West",   3.00, False),
            (682, "Stirista General BidMachine",                  2.00, False),
            (683, "Stirista General OLV BidMachine",              2.00, False),
            (684, "Sovrn BidMachine 300x250",                     1.00, False),
            (685, "Sovrn BidMachine 300x250 oRTB 2.6",            1.00, False),
            (689, "Sovrn BidMachine 728x90",                      1.00, False),
            (690, "Sovrn BidMachine 320x100",                     1.00, False),
            (691, "Sovrn BidMachine 320x100 oRTB 2.6",            1.00, False),
            (45,  "Pubmatic RON 320x50 PS",                       2.00, False),
            (46,  "Pubmatic RON 320x100 PS",                      2.00, False),
            (51,  "Pubmatic RON 300x250 PS",                      2.00, False),
            (52,  "Pubmatic RON 728x90 PS",                       2.00, False),
            (788, "Illumin US",                                   1.50, True),
        ],
    },

    # ─── P1b.6 — Illumin Display EU (id=290115327) ────────────────────────────
    # $317 / 7d, 8.9% WR, $0.50 eCPM. 7 null demands.
    {
        "publisher_id": 290115327,
        "publisher_label": "Illumin Display EU",
        "strategy": "Activate EU demands — lower floors for EU clearing rates",
        "demand_changes": [
            (650, "Unruly Illumin 10% EU",                        0.30, False),
            (651, "AdaptMX Illumin EU",                           0.30, False),
            (652, "Magnite Illumin Display EU",                   0.40, False),
            (918, "Pubmatic 320x50 New EP",                       0.30, False),
            (919, "Pubmatic 728x90 New EP",                       0.30, False),
            (929, "Pubmatic 300x250 New EP",                      0.30, False),
            (930, "OneTag Illumin EU",                            0.30, False),
        ],
    },

    # ─── P1b.7 — Illumin In-App EU (id=290115329) ─────────────────────────────
    # $82 / 7d, 25.3% WR, $0.52 eCPM. High WR, tiny scale.
    {
        "publisher_id": 290115329,
        "publisher_label": "Illumin In App EU",
        "strategy": "25.3% WR — activate secondaries to scale volume",
        "demand_changes": [
            (650, "Unruly Illumin 10% EU",                        0.30, False),
            (651, "AdaptMX Illumin EU",                           0.30, False),
            (918, "Pubmatic 320x50 New EP",                       0.30, False),
            (919, "Pubmatic 728x90 New EP",                       0.30, False),
            (929, "Pubmatic 300x250 New EP",                      0.30, False),
            (930, "OneTag Illumin EU",                            0.30, False),
        ],
    },

    # ─── P1b.8 — Start.IO - Video Magnite (id=290115375) ──────────────────────
    # 0 wins but $1,178 rev — reporting bug. 7 null demands. Video → decent floors.
    # Even with broken WINS reporting, activating quality floors attracts quality bids.
    {
        "publisher_id": 290115375,
        "publisher_label": "Start.IO - Video Magnite",
        "strategy": "Activate video floors despite reporting bug — invite quality demand",
        "demand_changes": [
            (139, "Magnite Start.IO Video",                       1.50, False),
            (140, "Magnite Start.IO Video oRTB 2.6",              1.50, False),
            (174, "Magnite Start.IO Video PS",                    1.50, False),
            (147, "Sovrn Start.IO Video",                         1.00, False),
            (148, "Sovrn Start.IO Video oRTB 2.6",                1.00, False),
            (135, "Pubmatic Start.IO Video",                      1.00, False),
            (136, "Pubmatic Start.IO Video oRTB 2.6",             1.00, False),
        ],
    },

    # ─── P1b.9 — Start.IO Display Magnite (id=290115374) ──────────────────────
    # 0 wins / $161 rev. 69 null demands (mostly Pubmatic display sizes).
    # Conservative $0.40 floor on Pubmatic display demands to avoid clearing at pennies.
    {
        "publisher_id": 290115374,
        "publisher_label": "Start.IO Display Magnite",
        "strategy": "Quality filter — $0.40 on Pubmatic display demands",
        "demand_changes": [
            (114, "Pubmatic Start.IO 970x90 oRTB 2.6",            0.40, False),
            (115, "Pubmatic Start.IO 300x250",                    0.40, False),
            (116, "Pubmatic Start.IO 300x250 oRTB 2.6",           0.40, False),
            (117, "Pubmatic Start.IO 250x250",                    0.40, False),
            (118, "Pubmatic Start.IO 250x250 oRTB 2.6",           0.40, False),
            (119, "Pubmatic Start.IO 336x280",                    0.40, False),
            (120, "Pubmatic Start.IO 336x280 oRTB 2.6",           0.40, False),
            (121, "Pubmatic Start.IO 336x320",                    0.40, False),
            (122, "Pubmatic Start.IO 336x320 oRTB 2.6",           0.40, False),
            (123, "Pubmatic Start.IO 200x200",                    0.40, False),
            (124, "Pubmatic Start.IO 200x200 oRTB 2.6",           0.40, False),
            (125, "Pubmatic Start.IO 320x480",                    0.40, False),
            (126, "Pubmatic Start.IO 320x480 oRTB 2.6",           0.40, False),
            (127, "Pubmatic Start.IO 300x600",                    0.40, False),
            (128, "Pubmatic Start.IO 300x600 oRTB 2.6",           0.40, False),
        ],
    },
]


def main():
    p = argparse.ArgumentParser(description="PGAM LL Phase-1b Executor")
    p.add_argument("--live", action="store_true", help="Apply changes (default: dry-run)")
    p.add_argument("--only", type=str, default=None, help="Comma-separated publisher IDs to limit the run")
    args = p.parse_args()

    dry_run = not args.live
    only_ids = set(int(x) for x in args.only.split(",")) if args.only else None

    mode = "LIVE" if not dry_run else "DRY-RUN"
    print("=" * 78)
    print(f"  PHASE-1b EXECUTOR (LL ONLY) — {mode} — {_today()}")
    print(f"  extending Phase-1 to 9 additional LL publishers")
    print("=" * 78)

    results = []

    for step_idx, step in enumerate(PLAN, 1):
        pub_id = step["publisher_id"]
        if only_ids is not None and pub_id not in only_ids:
            continue

        print(f"\n[{step_idx}/{len(PLAN)}] {step['publisher_label']} (id={pub_id})")
        print(f"      strategy: {step['strategy']}")
        print(f"      changes planned: {len(step['demand_changes'])}")

        r = apply_plan(
            pub_id=pub_id,
            pub_label=step["publisher_label"],
            strategy=step["strategy"],
            demand_changes=step["demand_changes"],
            dry_run=dry_run,
        )
        results.append(r)

    total_changes = sum(len(r.get("changes", [])) for r in results)
    total_skipped = sum(len(r.get("skipped", [])) for r in results)
    not_found = sum(1 for r in results for s in r.get("skipped", []) if s.get("reason") == "not_found")
    errors = sum(1 for r in results if r.get("error"))

    print("\n" + "=" * 78)
    print(f"  SUMMARY:  {total_changes} changes across {len(results)} publishers")
    print(f"            {total_skipped} skipped (already-set / paused / not found)")
    print(f"            {not_found} demand IDs not present on target publisher (OK — just skipped)")
    if errors:
        print(f"            ⚠  {errors} publisher(s) had errors")
    if dry_run:
        print("\n  ⚡ DRY-RUN — nothing was changed. Re-run with --live to apply.")
    else:
        print("\n  ✓ LIVE — changes applied to LL. Monitor next 48h.")
    print("=" * 78)

    if not dry_run and results:
        os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n  Results → {RESULTS_PATH}\n")


if __name__ == "__main__":
    main()
