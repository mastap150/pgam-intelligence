"""
agents/optimization/size_gap_agent.py

Finds inventories missing high-demand ad sizes and recommends creation.

Core insight
------------
Some inventories serve only 300x250 but have heavy 728x90 demand
available. Every request that would have been a 728x90 response is
dropped. By creating the missing placement, we capture incremental
revenue with zero cost.

Logic
-----
1. Pull size × inventory report for the window.
2. Build a per-inventory map of {size: revenue}.
3. For each inventory, identify its TOP sizes (present + earning).
4. For each inventory, check: which high-demand sizes (computed
   account-wide) are ABSENT from that inventory?
5. Rank the gaps by expected revenue uplift:
     uplift ≈ account_avg_size_rpm × inventory_total_requests
6. Emits recommendations. Does NOT auto-create (needs config like
   format, mimes, position — too many knobs for dumb automation).

Safety: recommendations only — the create_placement_* call itself
requires judgement calls about format, min sizes, etc. that a human
should review before firing. Once approved, we can wire auto-create.
"""

from __future__ import annotations

import json, os, sys, urllib.parse, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)
import core.tb_mgmt as tbm

WINDOW_DAYS            = 14
MIN_INVENTORY_REQUESTS = 1_000_000    # only opine on inventories with scale
MIN_SIZE_REVENUE       = 50.0         # account-wide, a size must earn ≥$50
TOP_N_GAPS             = 50

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR   = os.path.join(_REPO_ROOT, "logs")
RECS_FILE = os.path.join(LOG_DIR, "size_gap_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _size_inv_report() -> list[dict]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group", "total"), ("limit", 10000),
              ("attribute[]", "size"), ("attribute[]", "inventory")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return r.json().get("data", r.json())


def run() -> dict:
    print(f"\n{'='*70}\n  Size Gap Agent\n{'='*70}")
    rows = _size_inv_report()
    print(f"  {len(rows)} rows")

    inv_sizes: dict[int, dict[str, dict]] = defaultdict(dict)
    inv_totals: dict[int, dict] = defaultdict(
        lambda: {"requests": 0, "impressions": 0, "revenue": 0.0})
    size_totals: dict[str, dict] = defaultdict(
        lambda: {"requests": 0, "impressions": 0, "revenue": 0.0})

    for r in rows:
        size = (r.get("size") or "").strip()
        inv  = r.get("inventory_id") or r.get("inventory")
        if not size or not inv: continue
        # inventory may come through as "Title #NN" — parse id
        if isinstance(inv, str) and "#" in inv:
            try: inv = int(inv.rsplit("#", 1)[1])
            except ValueError: continue
        try: inv = int(inv)
        except Exception: continue
        reqs = r.get("bid_requests", 0) or 0
        imps = r.get("impressions", 0) or 0
        rev  = r.get("publisher_revenue", 0.0) or 0.0
        inv_sizes[inv][size] = {"requests": reqs, "impressions": imps, "revenue": rev}
        inv_totals[inv]["requests"]    += reqs
        inv_totals[inv]["impressions"] += imps
        inv_totals[inv]["revenue"]     += rev
        size_totals[size]["requests"]    += reqs
        size_totals[size]["impressions"] += imps
        size_totals[size]["revenue"]     += rev

    # Account-wide rpm (revenue per million requests) per size
    size_rpm = {}
    for s, t in size_totals.items():
        if t["revenue"] < MIN_SIZE_REVENUE: continue
        if t["requests"] == 0: continue
        size_rpm[s] = t["revenue"] * 1_000_000.0 / t["requests"]
    top_sizes = sorted(size_rpm.items(), key=lambda x: -x[1])
    print(f"\n  Top 10 sizes by RPM (account-wide):")
    for s, r in top_sizes[:10]:
        print(f"    {s:<14} rpm=${r:>6.2f}  rev=${size_totals[s]['revenue']:>8.0f}")

    # For each inventory, find absent top sizes
    gaps = []
    for inv, sizes in inv_sizes.items():
        if inv_totals[inv]["requests"] < MIN_INVENTORY_REQUESTS: continue
        have = set(sizes.keys())
        for size, rpm in top_sizes:
            if size in have: continue
            # Expected uplift: account RPM × this inventory's request volume
            uplift = rpm * inv_totals[inv]["requests"] / 1_000_000.0
            if uplift < 10.0: continue   # skip trivial gaps
            gaps.append({
                "inventory_id":     inv,
                "missing_size":     size,
                "account_size_rpm": round(rpm, 2),
                "inv_total_requests": inv_totals[inv]["requests"],
                "inv_total_revenue":  round(inv_totals[inv]["revenue"], 2),
                "inv_size_count":     len(have),
                "estimated_monthly_uplift": round(uplift * (30.0 / WINDOW_DAYS), 2),
            })
    gaps.sort(key=lambda x: -x["estimated_monthly_uplift"])
    gaps = gaps[:TOP_N_GAPS]

    # Hydrate with inventory titles
    inv_titles = {}
    for g in gaps:
        if g["inventory_id"] in inv_titles: continue
        try:
            inv_titles[g["inventory_id"]] = tbm.get_inventory(g["inventory_id"]).get("title", "?")
        except Exception:
            inv_titles[g["inventory_id"]] = "?"
    for g in gaps:
        g["inventory_title"] = inv_titles.get(g["inventory_id"])

    print(f"\n  {len(gaps)} top size gaps (est. monthly uplift):")
    for g in gaps[:20]:
        print(f"    inv={g['inventory_id']:>5} {g['inventory_title'][:25]:<25} "
              f"missing={g['missing_size']:<12} "
              f"+${g['estimated_monthly_uplift']:.0f}/mo "
              f"(rpm ${g['account_size_rpm']:.2f}, {g['inv_total_requests']/1e6:.1f}M reqs)")

    with open(RECS_FILE, "w") as f:
        json.dump({
            "timestamp":      datetime.now(timezone.utc).isoformat(),
            "window_days":    WINDOW_DAYS,
            "top_sizes_rpm":  [{"size": s, "rpm": r, "revenue": size_totals[s]["revenue"]} for s, r in top_sizes[:30]],
            "gaps":           gaps,
        }, f, indent=2)

    try:
        from core.slack import post_message
        if gaps:
            total = sum(g["estimated_monthly_uplift"] for g in gaps[:10])
            lines = [f"📐 *Size Gap Agent* — top 10 gaps = est +${total:.0f}/mo uplift"]
            for g in gaps[:8]:
                lines.append(f"  • inv {g['inventory_id']} {g['inventory_title'][:22]}  "
                             f"add {g['missing_size']}  +${g['estimated_monthly_uplift']:.0f}/mo")
            post_message("\n".join(lines))
    except Exception: pass

    return {"gaps": gaps, "top_sizes": top_sizes}


if __name__ == "__main__":
    run()
