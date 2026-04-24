"""
agents/alerts/yield_compression.py

Detects placements where impressions hold stable but revenue is dropping
— i.e. eCPM compression. Common causes: DSP budgets cycling, partner
bid rate changes, competitive pressure.

Logic
-----
Compare last 7d vs prior 7d, per placement:
  - imps_delta_pct  : stability proxy
  - rev_delta_pct   : the bad news
  - ecpm_delta_pct  : the confirmation

Flag when:
  |imps_delta| ≤ STABILITY_BAND
  AND rev_delta_pct ≤ COMPRESSION_THRESHOLD
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

STABILITY_BAND          = 0.15
COMPRESSION_THRESHOLD   = -0.15
MIN_PRIOR_REVENUE       = 25.0

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs")
RECS    = os.path.join(LOG_DIR, "yield_compression_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _pull(start, end):
    params = [("from", start), ("to", end), ("day_group","total"),
              ("limit",5000), ("attribute[]","placement")]
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(params)
    r = requests.get(url, timeout=300); r.raise_for_status()
    out = {}
    rows = r.json().get("data", r.json())
    for row in rows if isinstance(rows, list) else []:
        pid = row.get("placement_id")
        if pid is None: continue
        imps = row.get("impressions", 0) or 0
        rev  = row.get("publisher_revenue", 0.0) or 0.0
        out[int(pid)] = {
            "impressions": imps, "revenue": rev,
            "ecpm": (row.get("dsp_spend", 0.0) * 1000.0 / imps) if imps else 0.0,
        }
    return out


def run() -> dict:
    print(f"\n{'='*70}\n  Yield Compression Detector\n{'='*70}")
    today = datetime.now(timezone.utc).date()
    cur  = _pull((today - timedelta(days=7)).isoformat(),  today.isoformat())
    prev = _pull((today - timedelta(days=14)).isoformat(), (today - timedelta(days=7)).isoformat())
    print(f"  cur={len(cur)} prior={len(prev)} placements")

    # Hydrate titles
    pmap = {p["placement_id"]: p for p in tbm.list_all_placements_via_report(days=14, min_impressions=0)}

    def pct(a, b): return (a - b) / b if b else 0

    alerts = []
    for pid, p in prev.items():
        if p["revenue"] < MIN_PRIOR_REVENUE: continue
        c = cur.get(pid, {"impressions":0,"revenue":0.0,"ecpm":0.0})
        imp_d = pct(c["impressions"], p["impressions"])
        rev_d = pct(c["revenue"],     p["revenue"])
        ecpm_d= pct(c["ecpm"],        p["ecpm"])
        if abs(imp_d) > STABILITY_BAND: continue       # imps changed — not compression
        if rev_d > COMPRESSION_THRESHOLD: continue     # revenue OK
        det = pmap.get(pid, {})
        alerts.append({
            "placement_id": pid, "title": det.get("title","?"),
            "inventory_id": det.get("inventory_id"),
            "prior_revenue": round(p["revenue"], 2),
            "cur_revenue":   round(c["revenue"], 2),
            "revenue_delta_pct": round(rev_d*100, 1),
            "imp_delta_pct":     round(imp_d*100, 1),
            "ecpm_delta_pct":    round(ecpm_d*100, 1),
            "prior_ecpm": round(p["ecpm"], 2),
            "cur_ecpm":   round(c["ecpm"], 2),
        })
    alerts.sort(key=lambda x: x["revenue_delta_pct"])

    print(f"\n  {len(alerts)} placements compressing (stable imps, rev down)")
    for a in alerts[:15]:
        print(f"    [{a['placement_id']}] {a['title'][:35]:<35}  "
              f"rev ${a['prior_revenue']}→${a['cur_revenue']} ({a['revenue_delta_pct']:+.0f}%)  "
              f"imps {a['imp_delta_pct']:+.0f}%  eCPM {a['ecpm_delta_pct']:+.0f}%")

    with open(RECS, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "alerts": alerts}, f, indent=2)

    try:
        from core.slack import post_message
        if alerts:
            lines = [f"📉 *Yield Compression* — {len(alerts)} placements "
                     f"(imps stable ±15%, rev ≤-15%)"]
            for a in alerts[:8]:
                lines.append(f"  • [{a['placement_id']}] {a['title'][:28]} "
                             f"${a['prior_revenue']}→${a['cur_revenue']} "
                             f"({a['revenue_delta_pct']:+.0f}%)  eCPM {a['ecpm_delta_pct']:+.0f}%")
            post_message("\n".join(lines))
    except Exception: pass
    return {"alerts": alerts}


if __name__ == "__main__":
    run()
