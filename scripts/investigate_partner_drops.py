"""
scripts/investigate_partner_drops.py

Root-cause analysis for the 4 partners flagged by partner_churn_radar:
  Rough Maps #36, RevIQ #32, WeBlog #96, Aditude #60

For each partner, slice WoW deltas by:
  - inventory × placement     (which sites dropped?)
  - company_dsp               (which demand dried up?)
  - country                   (did a specific geo collapse?)

Outputs a ranked list of "the thing that changed" per partner.
"""

import os, sys, json, urllib.parse, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

PARTNERS = {
    "Rough Maps #36":         36,
    "RevIQ RevIQ #32":        32,
    "WeBlog WeBlog #96":      96,
    "Aditude Aditude #60":    60,
}

TB_BASE = "https://ssp.pgammedia.com/api"


def _report(start, end, attrs, publisher_name=None, limit=5000):
    """Single report pull; optionally filter in-memory to one publisher."""
    p = [("from", start), ("to", end), ("day_group", "total"), ("limit", limit)]
    attrs_full = list(attrs)
    if "publisher" not in attrs_full:
        attrs_full.append("publisher")
    for a in attrs_full: p.append(("attribute[]", a))
    url = f"{TB_BASE}/{tbm._get_token()}/report?" + urllib.parse.urlencode(p)
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    data = r.json()
    rows = data.get("data", data) if isinstance(data, dict) else data
    if publisher_name and isinstance(rows, list):
        rows = [x for x in rows if x.get("publisher") == publisher_name]
    return rows or []


def _windows():
    today = datetime.now(timezone.utc).date()
    cur   = (today - timedelta(days=7),  today)
    prior = (today - timedelta(days=14), today - timedelta(days=7))
    return cur, prior


def _by_key(rows, key):
    out = defaultdict(lambda: {"imps":0,"spend":0.0,"pub_rev":0.0,"reqs":0,"responses":0})
    for r in rows:
        k = r.get(key)
        if k is None: continue
        k = str(k)
        out[k]["imps"]      += r.get("impressions", 0) or 0
        out[k]["spend"]     += r.get("dsp_spend", 0.0) or 0.0
        out[k]["pub_rev"]   += r.get("publisher_revenue", 0.0) or 0.0
        out[k]["reqs"]      += r.get("bid_requests", 0) or 0
        out[k]["responses"] += r.get("bid_responses", 0) or 0
    return out


def _delta_rows(cur, prior, min_prior=50.0, metric="pub_rev"):
    """Return sorted deltas — biggest drops first."""
    out = []
    for k, p in prior.items():
        if p[metric] < min_prior: continue
        c = cur.get(k, {"imps":0,"spend":0.0,"pub_rev":0.0,"reqs":0,"responses":0})
        delta_abs = c[metric] - p[metric]
        delta_pct = delta_abs / p[metric] if p[metric] else 0
        out.append({
            "key": k,
            "prior_value": p[metric], "cur_value": c[metric],
            "delta_abs": delta_abs, "delta_pct": delta_pct * 100,
            "prior_imps": p["imps"], "cur_imps": c["imps"],
            "prior_reqs": p["reqs"], "cur_reqs": c["reqs"],
        })
    # Include NEW entries that appeared this week too
    for k, c in cur.items():
        if k in prior: continue
        if c[metric] > min_prior / 2:
            out.append({
                "key": k,
                "prior_value": 0, "cur_value": c[metric],
                "delta_abs": c[metric], "delta_pct": 9999.0,
                "prior_imps": 0, "cur_imps": c["imps"],
                "prior_reqs": 0, "cur_reqs": c["reqs"],
            })
    out.sort(key=lambda x: x["delta_abs"])   # most negative first
    return out


def investigate(partner_name: str, user_id: int):
    print(f"\n{'='*78}\n  {partner_name}  (user_id={user_id})\n{'='*78}")
    cur_win, prior_win = _windows()
    cur_s,  cur_e  = cur_win[0].isoformat(),  cur_win[1].isoformat()
    prior_s, prior_e = prior_win[0].isoformat(), prior_win[1].isoformat()

    # For each axis, pull the two windows filtered to this user
    findings = {}
    for axis in ["placement", "country", "company_dsp"]:
        try:
            cur_rows  = _report(cur_s,  cur_e,  [axis], publisher_name=partner_name)
            prior_rows= _report(prior_s, prior_e, [axis], publisher_name=partner_name)
        except Exception as e:
            print(f"  ✗ {axis}: {e}"); continue
        cur_k  = _by_key(cur_rows, axis)
        prior_k= _by_key(prior_rows, axis)
        deltas = _delta_rows(cur_k, prior_k, min_prior=20.0)
        findings[axis] = {
            "total_prior": sum(p["pub_rev"] for p in prior_k.values()),
            "total_cur":   sum(c["pub_rev"] for c in cur_k.values()),
            "deltas":      deltas,
        }

    # Summary totals
    if "placement" in findings:
        f = findings["placement"]
        print(f"\n  Publisher revenue:  prior=${f['total_prior']:.2f}  cur=${f['total_cur']:.2f}  "
              f"Δ=${f['total_cur']-f['total_prior']:+.2f}")

    # Top drops per axis
    for axis, title in [("placement","TOP DROPS BY PLACEMENT"),
                        ("country","TOP DROPS BY COUNTRY"),
                        ("company_dsp","TOP DROPS BY DSP ENDPOINT")]:
        if axis not in findings: continue
        print(f"\n  {title}")
        for d in findings[axis]["deltas"][:6]:
            p = d["prior_value"]; c = d["cur_value"]
            pct = f"{d['delta_pct']:+.0f}%" if d['delta_pct'] < 9000 else "NEW"
            marker = "  " if d["delta_abs"] >= 0 else "🔻"
            print(f"    {marker}{d['key'][:46]:<46}  "
                  f"${p:>7.0f} → ${c:>7.0f}  Δ${d['delta_abs']:>+7.0f}  {pct}")

    return findings


def main():
    all_findings = {}
    for name, uid in PARTNERS.items():
        try:
            all_findings[name] = investigate(name, uid)
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    # Write full report
    out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "logs", "partner_drop_investigation.json")
    with open(out, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "findings": all_findings}, f, indent=2, default=str)
    print(f"\n\n  Full report → {out}")


if __name__ == "__main__":
    main()
