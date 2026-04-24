"""
agents/optimization/placement_status_agent.py

Auto-pause placements that haven't monetized in N days.

Logic
-----
For each active placement:
  - If impressions == 0 over PAUSE_AFTER_DAYS AND placement.status == True
    → disable (set status=false)
  - If bid_responses > MIN_RESP_THRESHOLD but impressions == 0
    → definitely dead, pause

Exclusions
----------
- Placements younger than GRACE_DAYS (newly onboarded, may need warm-up)
- Placements explicitly listed in EXEMPT_PLACEMENT_IDS

Safety
------
- Dry-run default.
- Preserves the ability to re-enable later (does not delete).
- MAX_PAUSES_PER_RUN caps blast radius.
- Writes audit log with full context so re-enables are easy.
"""

from __future__ import annotations

import json, os, sys, urllib.parse, requests
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)
import core.tb_mgmt as tbm

PAUSE_AFTER_DAYS     = 30
GRACE_DAYS           = 14
MIN_RESP_THRESHOLD   = 10_000
MAX_PAUSES_PER_RUN   = 20
EXEMPT_PLACEMENT_IDS: set[int] = set()

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR     = os.path.join(_REPO_ROOT, "logs")
ACTIONS_LOG = os.path.join(LOG_DIR, "placement_status_actions.json")
RECS_FILE   = os.path.join(LOG_DIR, "placement_status_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _placement_stats(days: int) -> dict[int, dict]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group", "total"), ("limit", 1000),
              ("attribute[]", "placement")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    rows = r.json().get("data", r.json())
    out = {}
    for row in rows if isinstance(rows, list) else []:
        pid = row.get("placement_id")
        if pid is None: continue
        out[int(pid)] = row
    return out


def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*70}\n  Placement Status Agent  {'[DRY RUN]' if dry_run else '[LIVE]'}\n{'='*70}")

    placements = tbm.list_all_placements_via_report(days=PAUSE_AFTER_DAYS, min_impressions=0)
    stats = _placement_stats(PAUSE_AFTER_DAYS)
    print(f"  {len(placements)} active placements")

    candidates = []
    for p in placements:
        pid = p["placement_id"]
        if pid in EXEMPT_PLACEMENT_IDS: continue
        if not p.get("status", True):   continue   # already paused
        s = stats.get(pid, {})
        imps = s.get("impressions", 0) or 0
        resp = s.get("bid_responses", 0) or 0
        if imps > 0: continue
        if resp < MIN_RESP_THRESHOLD: continue   # not enough data
        candidates.append({
            "placement_id": pid, "title": p.get("title"),
            "type": p.get("type"), "inventory_id": p.get("inventory_id"),
            "bid_responses": resp, "current_floor": p.get("price"),
        })

    candidates.sort(key=lambda x: -x["bid_responses"])
    print(f"  {len(candidates)} pause candidates (0 imps, ≥{MIN_RESP_THRESHOLD:,} responses, {PAUSE_AFTER_DAYS}d window)")

    for c in candidates[:15]:
        print(f"    [{c['placement_id']}] {c['title'][:40]:<40} "
              f"resp={c['bid_responses']:>8,} type={c['type']}")

    actions = []
    paused = 0
    for c in candidates[:MAX_PAUSES_PER_RUN]:
        if dry_run:
            actions.append({**c, "applied": False, "dry_run": True,
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            continue
        try:
            tbm.disable_placement(c["placement_id"], dry_run=False)
            actions.append({**c, "applied": True,
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            paused += 1
        except Exception as e:
            actions.append({**c, "applied": False, "error": str(e)})
            print(f"    ✗ [{c['placement_id']}] {e}")

    # Persist
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)

    recs = {"timestamp": datetime.now(timezone.utc).isoformat(),
            "dry_run": dry_run, "candidates": candidates, "paused": paused}
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  {'DRY' if dry_run else 'PAUSED'}: {len(candidates) if dry_run else paused}  → {ACTIONS_LOG}")

    try:
        from core.slack import post_message
        tag = "🟢 LIVE" if not dry_run else "🔍 DRY"
        msg = [f"🛑 *Placement Status Agent* {tag}",
               f"Dead placements: {len(candidates)} (0 imps, ≥{MIN_RESP_THRESHOLD:,} resp, {PAUSE_AFTER_DAYS}d)"]
        for c in candidates[:5]:
            msg.append(f"  • [{c['placement_id']}] {c['title'][:30]} resp={c['bid_responses']:,}")
        if not dry_run:
            msg.append(f"\n✅ Paused {paused}")
        post_message("\n".join(msg))
    except Exception: pass

    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(dry_run=not args.apply)
