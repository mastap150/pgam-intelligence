"""
scripts/aggressive_floor_lift.py

One-shot aggressive floor lift on extreme-headroom placements.

Separate from tb_floor_nudge (which does cautious +10% steps). This
script targets placements where the eCPM/floor ratio is ≥ AGGRESSIVE_X
— i.e. we're leaving ≥5x the floor on the table.

For those, lift the floor to TARGET_FLOOR_PCT × observed eCPM in one
shot. TB's is_optimal_price will then fine-tune from that new base.

Example:
  placement with eCPM $2.00 and floor $0.02 → ratio 100x
  → lift to 50% × $2.00 = $1.00 floor (vs +10% nudge = $0.022)

Rollback: every change logged to logs/aggressive_lift_log.json.
Reverse each row with its `before_floor`.

Safety
------
- Only acts when ratio ≥ AGGRESSIVE_X (big headroom).
- Only if impressions ≥ MIN_IMPS (not noise).
- Hard cap MAX_FLOOR (default $5.00).
- MAX_LIFTS_PER_RUN caps blast radius.
- Dry-run default.
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

AGGRESSIVE_X          = 5.0       # eCPM ≥ 5× floor
TARGET_FLOOR_PCT      = 0.50      # lift to 50% of observed eCPM
MIN_IMPS              = 5_000
MIN_ECPM              = 0.20
MAX_FLOOR             = 5.00
MIN_FLOOR             = 0.05
MAX_LIFTS_PER_RUN     = 20

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "logs", "aggressive_lift_log.json")


def _placement_stats(days: int = 14) -> dict[int, dict]:
    end   = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group", "total"), ("limit", 5000),
              ("attribute[]", "placement")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=300); r.raise_for_status()
    rows = r.json().get("data", r.json())
    out = {}
    for row in rows if isinstance(rows, list) else []:
        pid = row.get("placement_id")
        if pid is None: continue
        imps = row.get("impressions", 0) or 0
        spend= row.get("dsp_spend", 0.0) or 0.0
        out[int(pid)] = {
            "impressions": imps, "dsp_spend": spend,
            "ecpm": (spend * 1000.0 / imps) if imps else 0.0,
            "pub_rev": row.get("publisher_revenue", 0.0) or 0.0,
        }
    return out


def run(apply: bool = False, rollback: bool = False) -> dict:
    print(f"\n{'='*72}\n  Aggressive Floor Lift  "
          f"{'[ROLLBACK]' if rollback else '[APPLY]' if apply else '[DRY RUN]'}\n{'='*72}")

    if rollback:
        # Restore previous floors from log
        if not os.path.exists(LOG_FILE):
            print("  no log to roll back")
            return {}
        with open(LOG_FILE) as f: prior = json.load(f)
        # Most recent "before_floor" per placement
        latest: dict[int, float] = {}
        for a in prior:
            if a.get("applied"):
                latest[a["placement_id"]] = a["before_floor"]
        print(f"  rolling back {len(latest)} placements to pre-lift floors...")
        for pid, floor in latest.items():
            try:
                tbm.set_floor(pid, price=floor, dry_run=False)
            except Exception as e:
                print(f"  ✗ {pid}: {e}")
        return {"rolled_back": len(latest)}

    placements = tbm.list_all_placements_via_report(days=14, min_impressions=MIN_IMPS)
    stats = _placement_stats(14)

    candidates = []
    for p in placements:
        pid   = p["placement_id"]
        floor = float(p.get("price") or 0.0)
        if floor <= 0: continue
        s = stats.get(pid, {})
        imps = s.get("impressions", 0)
        ecpm = s.get("ecpm", 0.0)
        if imps < MIN_IMPS or ecpm < MIN_ECPM: continue
        ratio = ecpm / floor if floor else 0
        if ratio < AGGRESSIVE_X: continue
        target = min(MAX_FLOOR, max(MIN_FLOOR, round(ecpm * TARGET_FLOOR_PCT, 2)))
        if target <= floor: continue
        candidates.append({
            "placement_id":  pid, "title": p.get("title"),
            "inventory_id":  p.get("inventory_id"),
            "before_floor":  floor, "target_floor": target,
            "ecpm":          round(ecpm, 2), "ratio": round(ratio, 1),
            "impressions":   imps,
            "revenue":       round(s.get("pub_rev", 0), 2),
        })
    candidates.sort(key=lambda x: -x["revenue"])
    candidates = candidates[:MAX_LIFTS_PER_RUN]

    print(f"\n  {len(candidates)} aggressive-lift candidates (ratio ≥{AGGRESSIVE_X}×)")
    for c in candidates[:15]:
        print(f"    [{c['placement_id']}] {c['title'][:38]:<38}  "
              f"${c['before_floor']:.2f} → ${c['target_floor']:.2f}  "
              f"(eCPM ${c['ecpm']:.2f}, ratio {c['ratio']}×)  "
              f"rev=${c['revenue']} imps={c['impressions']:,}")

    actions = []
    lifted = 0
    for c in candidates:
        if not apply:
            actions.append({**c, "applied": False, "dry_run": True,
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            continue
        try:
            tbm.set_floor(c["placement_id"], price=c["target_floor"], dry_run=False)
            actions.append({**c, "applied": True,
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            lifted += 1
        except Exception as e:
            actions.append({**c, "applied": False, "error": str(e),
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            print(f"    ✗ {c['placement_id']}: {e}")

    prior = []
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(LOG_FILE, "w") as f:
        json.dump(prior, f, indent=2)

    print(f"\n  {'LIFTED' if apply else 'WOULD LIFT'}: {lifted or len(candidates)}  → {LOG_FILE}")

    try:
        from core.slack import post_message
        if candidates:
            tag = "🟢 LIVE" if apply else "🔍 DRY"
            lines = [f"📈 *Aggressive Floor Lift* {tag} — {len(candidates)} extreme-headroom placements"]
            for c in candidates[:6]:
                lines.append(f"  • [{c['placement_id']}] {c['title'][:28]}  "
                             f"${c['before_floor']:.2f}→${c['target_floor']:.2f}  "
                             f"(ratio {c['ratio']}×)")
            if apply: lines.append(f"\n✅ Applied {lifted}")
            post_message("\n".join(lines))
    except Exception: pass

    return {"candidates": candidates, "applied": lifted}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply",    action="store_true")
    ap.add_argument("--rollback", action="store_true")
    args = ap.parse_args()
    run(apply=args.apply, rollback=args.rollback)
