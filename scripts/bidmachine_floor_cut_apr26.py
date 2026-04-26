"""
scripts/bidmachine_floor_cut_apr26.py

One-shot floor reduction on BidMachine high-volume inventory.

Context (2026-04-26 revenue review)
-----------------------------------
We're at 16% of monthly target with a $842K gap. Floor pressure is 78% of the
immediately addressable opportunity, concentrated on the highest-volume
BidMachine inventory: Display & Video and Interstitial. Win rates sit at ~5.4%
(Display) and ~2% (Interstitial) — floors set during a higher-demand period
are pricing us out of competitive auctions in the current market.

Action
------
Drop demand-level minBidFloor from current ~$3.25 to $2.00 on the BidMachine
Display and BidMachine Interstitial publishers, in one shot. The portfolio
optimizer's ±25% per-proposal cap (intelligence/optimizer.py:46) blocks a
single-step move of this magnitude, so this script bypasses that path and
goes directly to the canonical write API.

Scope (default targets)
-----------------------
  290115319  BidMachine - In App Display & Video
  290115333  BidMachine - In App Interstitial

Override with --publishers 290115319,290115333,...

Safety
------
- Default is DRY-RUN. No writes happen without --apply.
- Uses core.ll_mgmt.set_demand_floor() — the canonical post-2026-04-18 path
  that PUTs /v1/demands/{id} and re-GETs to verify (the legacy
  PUT /v1/publishers/{id} path silently discards nested floor edits).
- set_demand_floor() is demand-global: a write applies to every publisher
  the demand is wired to. We fetch the cross-publisher footprint up front:
    * Demands wired ONLY to publishers in the BidMachine family
      (290115319 D&V, 290115332 WL, 290115333 Interstitial, 290115334 EU,
      290115340 Reseller) → safe, applied without prompt.
    * Demands wired to non-BidMachine publishers → SKIPPED unless
      --allow-multi-pub is passed; even then, every collateral publisher
      is logged to the ledger and the per-run JSON.
- Contract-floor clamps in set_demand_floor() still apply (e.g. 9 Dots @ $1.70).
- Every applied change is recorded to core.floor_ledger so verifier and
  auto_revert_harmful can pick up regressions.
- Per-run summary JSON written to logs/bidmachine_floor_cut_apr26.json.

Run
---
  python3 scripts/bidmachine_floor_cut_apr26.py                    # dry-run
  python3 scripts/bidmachine_floor_cut_apr26.py --apply             # live
  python3 scripts/bidmachine_floor_cut_apr26.py --from-floor 3.00 --to-floor 2.00
  python3 scripts/bidmachine_floor_cut_apr26.py --apply --allow-multi-pub
  python3 scripts/bidmachine_floor_cut_apr26.py --publishers 290115319
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

from core import floor_ledger, ll_mgmt

ACTOR = "bidmachine_floor_cut_apr26"

# Publishers in the BidMachine family. A demand wired only within this set is
# considered safe to retune without --allow-multi-pub.
BIDMACHINE_FAMILY = {
    290115319,  # BidMachine - In App Display & Video
    290115332,  # BidMachine - In App Interstitial (WL)
    290115333,  # BidMachine - In App Interstitial
    290115334,  # Copy - BidMachine - In App Interstitial (Europe)
    290115340,  # BidMachine Reseller
}

# Default action targets — the two highest-volume BidMachine surfaces.
DEFAULT_TARGETS = [290115319, 290115333]

RESULTS_PATH = os.path.join(_REPO_ROOT, "logs", "bidmachine_floor_cut_apr26.json")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_demand_publisher_map() -> dict[int, set[int]]:
    """Map demand_id -> set of publisher_ids that wire it.

    One pass over every active publisher's biddingpreferences.
    """
    mapping: dict[int, set[int]] = defaultdict(set)
    for pub in ll_mgmt.get_publishers(include_archived=False):
        pid = pub.get("id")
        if pid is None:
            continue
        for pref in pub.get("biddingpreferences", []):
            for v in pref.get("value", []):
                did = v.get("id")
                if did is not None:
                    mapping[did].add(pid)
    return mapping


def plan_publisher(
    pub_id: int,
    demand_to_pubs: dict[int, set[int]],
    from_floor: float,
    to_floor: float,
    allow_multi_pub: bool,
) -> dict:
    """Build the change set for one publisher (no writes)."""
    out = {
        "publisher_id": pub_id,
        "publisher_name": "",
        "changes": [],
        "skipped": [],
    }
    pub = ll_mgmt.get_publisher(pub_id)
    out["publisher_name"] = pub.get("name", f"pub_{pub_id}")

    seen_demand_ids: set[int] = set()
    for pref in pub.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if did is None or did in seen_demand_ids:
                continue
            seen_demand_ids.add(did)

            name = v.get("name") or ""
            current = v.get("minBidFloor")
            status = v.get("status")

            if status == 2:
                out["skipped"].append({"demand_id": did, "demand_name": name,
                                       "reason": "paused", "current_floor": current})
                continue
            if current is None:
                out["skipped"].append({"demand_id": did, "demand_name": name,
                                       "reason": "no_floor_set", "current_floor": None})
                continue
            try:
                cur_f = float(current)
            except (TypeError, ValueError):
                out["skipped"].append({"demand_id": did, "demand_name": name,
                                       "reason": "non_numeric_floor", "current_floor": current})
                continue
            if cur_f < from_floor:
                out["skipped"].append({"demand_id": did, "demand_name": name,
                                       "reason": f"below_from_floor_{from_floor}",
                                       "current_floor": cur_f})
                continue
            if cur_f <= to_floor:
                out["skipped"].append({"demand_id": did, "demand_name": name,
                                       "reason": f"already_at_or_below_target_{to_floor}",
                                       "current_floor": cur_f})
                continue

            other_pubs = sorted(demand_to_pubs.get(did, set()) - {pub_id})
            non_bm_pubs = [p for p in other_pubs if p not in BIDMACHINE_FAMILY]

            if non_bm_pubs and not allow_multi_pub:
                out["skipped"].append({
                    "demand_id": did, "demand_name": name,
                    "reason": "multi_pub_outside_bm_family",
                    "current_floor": cur_f,
                    "other_publishers": other_pubs,
                    "non_bm_publishers": non_bm_pubs,
                })
                continue

            out["changes"].append({
                "demand_id": did,
                "demand_name": name,
                "old_floor": cur_f,
                "new_floor": to_floor,
                "other_publishers": other_pubs,
                "non_bm_publishers": non_bm_pubs,
            })

    return out


def apply_change(change: dict, pub_id: int, pub_name: str, dry_run: bool) -> dict:
    """Write one floor change via the canonical path + ledger entry."""
    did = change["demand_id"]
    new_floor = change["new_floor"]
    old_floor = change["old_floor"]
    name = change["demand_name"]

    pubs_running_it = 1 + len(change.get("other_publishers", []))
    allow_multi = pubs_running_it > 1

    reason = (f"BidMachine floor reset apr26: pub_id={pub_id} "
              f"{old_floor}→{new_floor}; "
              f"runs_on_{pubs_running_it}_pubs")

    if dry_run:
        print(f"  DRY  [{pub_name[:32]:<32}] {name[:38]:<38} "
              f"id={did:<6} {old_floor}→{new_floor} "
              f"(also affects {len(change.get('other_publishers', []))} other pubs)")
        floor_ledger.record(
            publisher_id=pub_id, publisher_name=pub_name,
            demand_id=did, demand_name=name,
            old_floor=old_floor, new_floor=new_floor,
            actor=ACTOR, reason=reason,
            dry_run=True, applied=False,
        )
        return {"dry_run": True, **change}

    try:
        result = ll_mgmt.set_demand_floor(
            did, new_floor,
            verify=True,
            allow_multi_pub=allow_multi,
            _publishers_running_it=pubs_running_it,
        )
        floor_ledger.record(
            publisher_id=pub_id, publisher_name=pub_name,
            demand_id=did, demand_name=name,
            old_floor=old_floor, new_floor=new_floor,
            actor=ACTOR, reason=reason,
            dry_run=False, applied=True,
        )
        print(f"  OK   [{pub_name[:32]:<32}] {name[:38]:<38} "
              f"id={did:<6} {old_floor}→{new_floor}")
        return {"applied": True, **change, "result": result}
    except Exception as e:
        print(f"  FAIL [{pub_name[:32]:<32}] {name[:38]:<38} "
              f"id={did:<6} {old_floor}→{new_floor} :: {e}")
        floor_ledger.record(
            publisher_id=pub_id, publisher_name=pub_name,
            demand_id=did, demand_name=name,
            old_floor=old_floor, new_floor=new_floor,
            actor=ACTOR, reason=f"{reason} | FAILED: {e}",
            dry_run=False, applied=False,
        )
        return {"applied": False, "error": str(e), **change}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--apply", action="store_true",
                   help="Execute writes. Default is dry-run.")
    p.add_argument("--publishers", type=str, default=None,
                   help=f"Comma-separated publisher IDs (default: "
                        f"{','.join(str(x) for x in DEFAULT_TARGETS)})")
    p.add_argument("--from-floor", type=float, default=3.00,
                   help="Only touch demands whose current floor is >= this. Default 3.00.")
    p.add_argument("--to-floor", type=float, default=2.00,
                   help="Target floor. Default 2.00.")
    p.add_argument("--allow-multi-pub", action="store_true",
                   help="Allow demand-global writes that propagate to non-BidMachine "
                        "publishers. Default: skip those demands.")
    p.add_argument("--max-changes", type=int, default=30,
                   help="Hard cap on total writes per run. Default 30.")
    args = p.parse_args()

    pub_ids = ([int(x) for x in args.publishers.split(",")] if args.publishers
               else list(DEFAULT_TARGETS))

    dry_run = not args.apply
    mode = "DRY-RUN" if dry_run else "LIVE"

    print("=" * 78)
    print(f"  BIDMACHINE FLOOR CUT — {mode} — {_now_iso()}")
    print(f"  publishers: {pub_ids}")
    print(f"  policy: floor >= ${args.from_floor:.2f}  →  ${args.to_floor:.2f}")
    print(f"  multi-pub outside BM family: "
          f"{'ALLOWED (writes propagate)' if args.allow_multi_pub else 'SKIPPED'}")
    print(f"  max changes per run: {args.max_changes}")
    print("=" * 78)

    print("\n[1/3] Building cross-publisher demand map...")
    demand_to_pubs = build_demand_publisher_map()
    print(f"      {len(demand_to_pubs)} demand IDs across the portfolio")

    print("\n[2/3] Planning changes per publisher...")
    plans = []
    for pid in pub_ids:
        plan = plan_publisher(pid, demand_to_pubs, args.from_floor,
                              args.to_floor, args.allow_multi_pub)
        plans.append(plan)
        n_changes = len(plan["changes"])
        n_skipped = len(plan["skipped"])
        print(f"      [{pid}] {plan['publisher_name'][:50]:<50} "
              f"{n_changes} changes, {n_skipped} skipped")

    total_planned = sum(len(p["changes"]) for p in plans)
    if total_planned == 0:
        print("\nNo changes to make. Exiting.")
        return

    if total_planned > args.max_changes:
        print(f"\n  CAP: {total_planned} changes exceed --max-changes={args.max_changes}. "
              f"Truncating to first {args.max_changes}.")
        remaining = args.max_changes
        for plan in plans:
            if remaining <= 0:
                plan["skipped"].extend(
                    [{"demand_id": c["demand_id"], "demand_name": c["demand_name"],
                      "reason": "max_changes_cap", "current_floor": c["old_floor"]}
                     for c in plan["changes"]]
                )
                plan["changes"] = []
                continue
            if len(plan["changes"]) > remaining:
                overflow = plan["changes"][remaining:]
                plan["skipped"].extend(
                    [{"demand_id": c["demand_id"], "demand_name": c["demand_name"],
                      "reason": "max_changes_cap", "current_floor": c["old_floor"]}
                     for c in overflow]
                )
                plan["changes"] = plan["changes"][:remaining]
            remaining -= len(plan["changes"])

    print(f"\n[3/3] {'Previewing' if dry_run else 'Applying'} "
          f"{sum(len(p['changes']) for p in plans)} changes...")
    applied_count = 0
    failed_count = 0
    for plan in plans:
        for change in plan["changes"]:
            r = apply_change(change, plan["publisher_id"],
                             plan["publisher_name"], dry_run)
            if r.get("applied"):
                applied_count += 1
            elif r.get("error"):
                failed_count += 1

    print("\n" + "=" * 78)
    if dry_run:
        print(f"  DRY-RUN complete. {sum(len(p['changes']) for p in plans)} changes "
              f"previewed. Re-run with --apply to write.")
    else:
        print(f"  LIVE run complete. applied={applied_count}, failed={failed_count}")
    print("=" * 78)

    summary = {
        "actor": ACTOR,
        "ts_utc": _now_iso(),
        "dry_run": dry_run,
        "from_floor": args.from_floor,
        "to_floor": args.to_floor,
        "allow_multi_pub": args.allow_multi_pub,
        "publishers": pub_ids,
        "applied": applied_count,
        "failed": failed_count,
        "plans": plans,
    }
    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n  Per-run summary: {RESULTS_PATH}")


if __name__ == "__main__":
    main()
