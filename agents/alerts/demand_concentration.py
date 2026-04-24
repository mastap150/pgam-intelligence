"""
agents/alerts/demand_concentration.py

Flags inventories where 1-2 DSPs carry the majority of revenue — a
concentration risk. If that single DSP drops out (budget cap, endpoint
broken, policy change), the inventory craters.

Logic
-----
Pull inventory × company_dsp revenue for 14 days. For each inventory:
  - HHI       = sum of market-share^2 per DSP
  - top1_pct  = top DSP's share of inventory revenue
  - top2_pct  = top 2 DSPs combined

Alert thresholds:
  🔴 CRITICAL  top1_pct ≥ 0.70   (single DSP controls ≥70%)
  🟠 WARNING   top2_pct ≥ 0.85   (top 2 control ≥85%)
  🟡 WATCH     HHI ≥ 3000 (moderate concentration)

Alerts only; no auto-action.
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

WINDOW_DAYS      = 14
MIN_INV_REVENUE  = 50.0
TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs")
RECS    = os.path.join(LOG_DIR, "demand_concentration_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def run() -> dict:
    print(f"\n{'='*70}\n  Demand Concentration Monitor\n{'='*70}")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=WINDOW_DAYS)
    params = [("from", start.isoformat()), ("to", end.isoformat()),
              ("day_group","total"),("limit",5000),
              ("attribute[]","inventory"),("attribute[]","company_dsp")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=300); r.raise_for_status()
    rows = r.json().get("data", r.json())
    print(f"  {len(rows)} rows")

    inv_dsp_rev: dict[int, dict[str,float]] = defaultdict(lambda: defaultdict(float))
    inv_titles: dict[int, str] = {}
    for row in rows if isinstance(rows, list) else []:
        inv = row.get("inventory_id")
        if inv is None:
            inv_raw = row.get("inventory","")
            if "#" in inv_raw:
                try: inv = int(inv_raw.rsplit("#",1)[1])
                except ValueError: continue
        if inv is None: continue
        try: inv = int(inv)
        except Exception: continue
        dsp = row.get("company_dsp","?")
        inv_dsp_rev[inv][dsp] += row.get("publisher_revenue", 0.0) or 0.0
        if row.get("inventory") and inv not in inv_titles:
            inv_titles[inv] = row.get("inventory")

    alerts = []
    for inv, dsps in inv_dsp_rev.items():
        total = sum(dsps.values())
        if total < MIN_INV_REVENUE: continue
        shares = sorted(dsps.items(), key=lambda x: -x[1])
        top1 = shares[0][1] / total
        top2 = sum(s[1] for s in shares[:2]) / total
        hhi  = sum((v/total * 100)**2 for v in dsps.values())
        sev = None
        if top1 >= 0.70:   sev = "🔴 CRITICAL"
        elif top2 >= 0.85: sev = "🟠 WARNING"
        elif hhi  >= 3000: sev = "🟡 WATCH"
        if sev:
            alerts.append({
                "inventory_id": inv, "title": inv_titles.get(inv, "?"),
                "total_revenue": round(total, 2),
                "top1_dsp":  shares[0][0], "top1_pct": round(top1*100, 1),
                "top2_dsps": [s[0] for s in shares[:2]],
                "top2_pct":  round(top2*100, 1),
                "hhi": round(hhi, 0), "severity": sev, "dsp_count": len(dsps),
            })
    alerts.sort(key=lambda a: (a["severity"], -a["total_revenue"]))

    print(f"\n  {len(alerts)} inventories flagged")
    for a in alerts[:20]:
        print(f"    {a['severity']}  inv={a['inventory_id']} {a['title'][:25]:<25}  "
              f"rev=${a['total_revenue']:,.0f}  top1={a['top1_pct']:.0f}% ({a['top1_dsp'][:30]})")

    with open(RECS, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "alerts": alerts}, f, indent=2)

    try:
        from core.slack import post_message
        if alerts:
            lines = [f"⚠️ *Demand Concentration* — {len(alerts)} inventories at risk"]
            for a in alerts[:8]:
                lines.append(f"  {a['severity']} inv {a['inventory_id']} {a['title'][:22]} "
                             f"rev ${a['total_revenue']:,.0f}  top1={a['top1_pct']:.0f}%")
            post_message("\n".join(lines))
    except Exception: pass
    return {"alerts": alerts}


if __name__ == "__main__":
    run()
