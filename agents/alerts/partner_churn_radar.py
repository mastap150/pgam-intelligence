"""
agents/alerts/partner_churn_radar.py

Early-warning system for revenue drops on SSP Company partners.

Logic
-----
Every day, pull publisher-level stats for:
  - Current 7d window
  - Prior 7d window (8–14 days ago)

For each partner, compute:
  req_delta_pct, imp_delta_pct, rev_delta_pct

Flag as:
  🔴 CRITICAL  — rev_delta_pct ≤ -40%  (revenue collapse)
  🟠 WARNING   — rev_delta_pct ≤ -20%  OR  req_delta_pct ≤ -30%
  🟡 WATCH     — rev_delta_pct ≤ -10%  AND revenue was ≥ $50 WoW prior

Posts to Slack daily. No auto-actions — alerts only.
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

MIN_PRIOR_REVENUE_USD = 50.0
CRITICAL_DROP = -0.40
WARNING_DROP  = -0.20
WATCH_DROP    = -0.10
REQ_WARNING_DROP = -0.30

TB_BASE = "https://ssp.pgammedia.com/api"
LOG_DIR  = os.path.join(_REPO_ROOT, "logs")
RECS     = os.path.join(LOG_DIR, "partner_churn_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


def _publisher_report(start: str, end: str) -> dict[str, dict]:
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode([
        ("from", start), ("to", end), ("day_group", "total"),
        ("limit", 500), ("attribute[]", "publisher")])
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    rows = r.json().get("data", r.json())
    out: dict[str, dict] = {}
    for row in rows if isinstance(rows, list) else []:
        n = row.get("publisher")
        if n: out[n] = row
    return out


def run() -> dict:
    print(f"\n{'='*70}\n  Partner Churn Radar\n{'='*70}")
    today = datetime.now(timezone.utc).date()
    cur_end  = today
    cur_start = today - timedelta(days=7)
    prior_end = cur_start
    prior_start = prior_end - timedelta(days=7)

    print(f"  current  {cur_start} → {cur_end}")
    print(f"  prior    {prior_start} → {prior_end}")

    cur = _publisher_report(cur_start.isoformat(), cur_end.isoformat())
    pri = _publisher_report(prior_start.isoformat(), prior_end.isoformat())

    def _delta(a, b):
        if not b: return None
        return (a - b) / b

    alerts = []
    for name, prow in pri.items():
        if (prow.get("publisher_revenue") or 0) < MIN_PRIOR_REVENUE_USD:
            continue
        crow = cur.get(name, {})
        rev_d = _delta(crow.get("publisher_revenue", 0) or 0, prow["publisher_revenue"])
        req_d = _delta(crow.get("bid_requests", 0) or 0, prow.get("bid_requests", 0) or 0)
        imp_d = _delta(crow.get("impressions", 0) or 0, prow.get("impressions", 0) or 0)
        severity = None
        if rev_d is not None and rev_d <= CRITICAL_DROP:
            severity = "🔴 CRITICAL"
        elif (rev_d is not None and rev_d <= WARNING_DROP) or \
             (req_d is not None and req_d <= REQ_WARNING_DROP):
            severity = "🟠 WARNING"
        elif rev_d is not None and rev_d <= WATCH_DROP:
            severity = "🟡 WATCH"
        if severity:
            alerts.append({
                "publisher": name, "severity": severity,
                "prior_rev": prow["publisher_revenue"],
                "cur_rev":   crow.get("publisher_revenue", 0) or 0,
                "rev_delta_pct": round((rev_d or 0) * 100, 1),
                "req_delta_pct": round((req_d or 0) * 100, 1),
                "imp_delta_pct": round((imp_d or 0) * 100, 1),
            })
    alerts.sort(key=lambda x: x["rev_delta_pct"])

    print(f"\n  {len(alerts)} partners flagged")
    for a in alerts[:20]:
        print(f"    {a['severity']}  {a['publisher'][:40]:<40} "
              f"rev ${a['prior_rev']:.0f}→${a['cur_rev']:.0f} "
              f"({a['rev_delta_pct']:+.1f}%)  "
              f"req {a['req_delta_pct']:+.1f}%  imp {a['imp_delta_pct']:+.1f}%")

    with open(RECS, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "alerts": alerts}, f, indent=2)

    try:
        from core.slack import post_message
        if not alerts:
            post_message("📡 *Partner Churn Radar* — all partners stable WoW ✅")
        else:
            lines = [f"📡 *Partner Churn Radar* — {len(alerts)} partners flagged"]
            for a in alerts[:10]:
                lines.append(
                    f"  {a['severity']} {a['publisher'][:32]}  "
                    f"rev ${a['prior_rev']:.0f}→${a['cur_rev']:.0f} "
                    f"({a['rev_delta_pct']:+.0f}%)  req {a['req_delta_pct']:+.0f}%"
                )
            post_message("\n".join(lines))
    except Exception: pass

    return {"alerts": alerts}


if __name__ == "__main__":
    run()
