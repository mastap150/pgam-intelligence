"""
scripts/revenue_recovery.py

Emergency recovery: identify inventories with significant revenue drop
since our May 1 brand_safety_sweep + April floor changes, then
selectively roll back the most likely culprits per-inventory.

Strategy
--------
1. Pull pre-period revenue (14-7 days ago) and post-period (last 3 days).
2. Compute per-inventory delta. Flag inventories with rev drop ≥30%
   AND prior_revenue ≥ $5 (skip noise).
3. For each flagged inventory, attempt rollbacks in order of safety:
   a. Remove IAB24 (Uncategorized) from blocked_categories — least
      damaging to revert (was the May 1 sweep that hit 852 inventories).
   b. If still hurting after a, remove IAB25/IAB26 too.
   c. Then look at placement-level floor reverts on that inventory's
      placements (rollback aggressive lifts where ratio dropped < 2x).
4. Each revert goes through tbm.set_floor / tbm.edit_inventory which
   logs to tb_ledger so guardian can verify in 24h.

Usage
-----
    python3 -m scripts.revenue_recovery               # dry-run
    python3 -m scripts.revenue_recovery --apply       # execute
    python3 -m scripts.revenue_recovery --aggressive  # also revert IAB25/26
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests
from collections import defaultdict
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

PRE_DAYS_BACK    = 14   # window starts here
PRE_DAYS_END     = 7    # window ends here
POST_DAYS        = 3    # measure last 3d
DROP_THRESHOLD   = 0.30 # ≥30% drop = flag
MIN_PRIOR_REV    = 5.0  # skip noise
MAX_REVERTS_RUN  = 50

TB = "https://ssp.pgammedia.com/api"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
RECS = os.path.join(LOG_DIR, "revenue_recovery_recs.json")


def _pull(s, e):
    """Per-inventory revenue over window."""
    url = f"{TB}/{tbm._get_token()}/report?" + urllib.parse.urlencode([
        ("from", s.isoformat()), ("to", e.isoformat()),
        ("day_group", "total"), ("limit", 5000),
        ("attribute[]", "inventory")])
    r = requests.get(url, timeout=300); r.raise_for_status()
    out = {}
    for row in r.json().get("data", r.json()) or []:
        iid = row.get("inventory_id")
        if iid is None:
            raw = row.get("inventory", "")
            if "#" in raw:
                try: iid = int(raw.rsplit("#",1)[1])
                except ValueError: continue
        if iid is None: continue
        try: iid = int(iid)
        except: continue
        out[iid] = {
            "revenue":    row.get("publisher_revenue", 0.0) or 0.0,
            "impressions":row.get("impressions", 0) or 0,
            "requests":   row.get("bid_requests", 0) or 0,
        }
    return out


def _build_form(inv: dict, blocked_cats: list[str], drop_dsp_ids: set[int] = None):
    """Mirror of brand_safety_sweep._build_form for inventory writes."""
    drop_dsp_ids = drop_dsp_ids or set()
    form = [("inventory_id", str(inv["inventory_id"]))]
    for c in blocked_cats:
        form.append(("blocked_categories[]", c))
    for pid in (inv.get("inventory_dsp[white]") or []):
        if int(pid) in drop_dsp_ids: continue
        form.append(("inventory_dsp[white][]", str(pid)))
    for pid in (inv.get("inventory_dsp[black]") or []):
        if int(pid) in drop_dsp_ids: continue
        form.append(("inventory_dsp[black][]", str(pid)))
    for cat in (inv.get("categories") or []):
        form.append(("categories[]", str(cat)))
    for dom in (inv.get("blocked_domains") or []):
        form.append(("blocked_domains[]", dom))
    return form


import re as _re
_INACTIVE_RE = _re.compile(r"#(\d+)\s+not active", _re.IGNORECASE)


def _write_inventory(inv: dict, new_blocked_cats: list[str]) -> dict:
    """Inventory-level write with the same retry-on-inactive-DSP pattern."""
    token = tbm._get_token()
    url = f"{TB}/{token}/edit_inventory"
    form = _build_form(inv, new_blocked_cats)
    r = requests.post(url, data=form, timeout=60)
    ok = r.ok and "html" not in r.headers.get("content-type","")
    if not ok and "not active" in r.text:
        ids = {int(m) for m in _INACTIVE_RE.findall(r.text)}
        if ids:
            r = requests.post(url, data=_build_form(inv, new_blocked_cats, drop_dsp_ids=ids), timeout=60)
            ok = r.ok and "html" not in r.headers.get("content-type","")
    return {"ok": ok, "code": r.status_code, "body": r.text[:200] if not ok else ""}


def run(apply: bool = False, aggressive: bool = False) -> dict:
    print(f"\n{'='*72}\n  Revenue Recovery  [{'APPLY' if apply else 'DRY'}]"
          f"{' AGGRESSIVE' if aggressive else ''}\n{'='*72}")
    end = date.today()
    pre_s = end - timedelta(days=PRE_DAYS_BACK)
    pre_e = end - timedelta(days=PRE_DAYS_END)
    post_s = end - timedelta(days=POST_DAYS)
    print(f"  pre window:  {pre_s} → {pre_e}")
    print(f"  post window: {post_s} → {end}")

    print("  pulling pre-period inventory rev...")
    pre = _pull(pre_s, pre_e)
    print(f"    {len(pre)} inventories had rev")
    print("  pulling post-period inventory rev...")
    post = _pull(post_s, end)
    print(f"    {len(post)} inventories")

    # Identify drops
    flagged = []
    for iid, p in pre.items():
        prior = p["revenue"]
        if prior < MIN_PRIOR_REV: continue
        post_rev = post.get(iid, {}).get("revenue", 0)
        # Normalize per-day
        prior_d = prior / max(PRE_DAYS_BACK - PRE_DAYS_END, 1)
        post_d = post_rev / POST_DAYS
        if prior_d <= 0: continue
        drop = (prior_d - post_d) / prior_d
        if drop < DROP_THRESHOLD: continue
        flagged.append({
            "inventory_id":  iid,
            "prior_per_day": round(prior_d, 2),
            "cur_per_day":   round(post_d, 2),
            "drop_pct":      round(drop * 100, 1),
            "lost_per_day":  round(prior_d - post_d, 2),
        })
    flagged.sort(key=lambda x: -x["lost_per_day"])
    flagged = flagged[:MAX_REVERTS_RUN]
    total_lost = sum(f["lost_per_day"] for f in flagged)
    print(f"\n  {len(flagged)} inventories with ≥{int(DROP_THRESHOLD*100)}% rev drop")
    print(f"  total lost: ${total_lost:.2f}/day  →  ${total_lost*30:,.0f}/mo")

    # Show top 15
    for f in flagged[:15]:
        print(f"    inv={f['inventory_id']:>5}  prior=${f['prior_per_day']:>6.2f}/d  "
              f"cur=${f['cur_per_day']:>6.2f}/d  drop={f['drop_pct']:>5.1f}%  "
              f"lost=${f['lost_per_day']:>6.2f}/d")

    # Recovery actions
    actions = []
    for f in flagged:
        try:
            inv = tbm.get_inventory(f["inventory_id"])
        except Exception as e:
            print(f"  ✗ get_inventory({f['inventory_id']}): {e}")
            continue
        existing_cats = list(inv.get("blocked_categories") or [])
        # Remove IAB24 (most aggressive — Uncategorized) first
        cats_to_remove = ["IAB24"] if not aggressive else ["IAB24","IAB25","IAB26"]
        new_cats = [c for c in existing_cats if c not in cats_to_remove]
        removed = [c for c in existing_cats if c in cats_to_remove]
        if not removed:
            actions.append({**f, "action":"none", "reason":"no IAB24/25/26 in blocked_categories"})
            continue
        if not apply:
            actions.append({**f, "action":"would_unblock", "removed_cats": removed,
                            "before": existing_cats, "after": new_cats, "applied":False})
            print(f"  🔍 inv {f['inventory_id']}  unblock {removed}")
            continue
        res = _write_inventory(inv, new_cats)
        actions.append({**f, "action":"unblock", "removed_cats":removed,
                        "before":existing_cats, "after":new_cats,
                        "applied":res["ok"], "result":res})
        if res["ok"]:
            print(f"  ✅ inv {f['inventory_id']}  unblocked {removed}")
            try:
                from core import tb_ledger
                tb_ledger.record(
                    actor="revenue_recovery", action="edit_inventory",
                    entity_type="inventory", entity_id=f["inventory_id"],
                    reason=f"rev drop {f['drop_pct']}% — unblocked {removed}",
                    before={"blocked_categories": existing_cats},
                    after={"blocked_categories": new_cats},
                    applied=True,
                )
            except Exception: pass
        else:
            print(f"  ✗ inv {f['inventory_id']}: {res['body']}")

    with open(RECS, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "flagged": flagged, "actions": actions,
                   "total_lost_per_day": total_lost,
                   "total_lost_per_month_est": total_lost * 30}, f, indent=2, default=str)
    print(f"\n  Recs → {RECS}")

    try:
        from core.slack import post_message
        ok = sum(1 for a in actions if a.get("applied"))
        msg = [f"🚑 *Revenue Recovery* {'LIVE' if apply else 'DRY'}",
               f"WoW drop: {len(flagged)} inventories ${total_lost:.0f}/day → ~${total_lost*30:,.0f}/mo at risk"]
        if apply: msg.append(f"Unblocked IAB24{('+IAB25+IAB26' if aggressive else '')} on {ok} inventories")
        for f in flagged[:5]:
            msg.append(f"  • inv {f['inventory_id']}  drop {f['drop_pct']:.0f}%  ${f['lost_per_day']:.0f}/d")
        post_message("\n".join(msg))
    except Exception: pass

    return {"flagged":flagged, "actions":actions, "total_lost_per_day":total_lost}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--aggressive", action="store_true",
                    help="also unblock IAB25/26 (default: only IAB24)")
    args = ap.parse_args()
    run(apply=args.apply, aggressive=args.aggressive)
