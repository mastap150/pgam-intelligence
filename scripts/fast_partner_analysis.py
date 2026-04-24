"""
scripts/fast_partner_analysis.py

Single-shot partner drop analysis. Pulls one big report per axis
(cur+prior windows concatenated), then slices in memory. Much faster
than filtered per-partner pulls.
"""
import os, sys, json, urllib.parse, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

PARTNERS = ["Rough Maps #36", "RevIQ RevIQ #32", "WeBlog WeBlog #96", "Aditude Aditude #60"]
TB = "https://ssp.pgammedia.com/api"


def _pull(start, end, secondary_attr, limit=5000):
    token = tbm._get_token()
    params = [("from", start), ("to", end), ("day_group", "total"), ("limit", limit),
              ("attribute[]", "publisher"), ("attribute[]", secondary_attr)]
    url = f"{TB}/{token}/report?" + urllib.parse.urlencode(params)
    print(f"  pulling {secondary_attr} {start}→{end}...", flush=True)
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    data = r.json()
    rows = data.get("data", data) if isinstance(data, dict) else data
    print(f"    {len(rows)} rows", flush=True)
    return rows or []


def _pivot(rows, key):
    out = defaultdict(lambda: defaultdict(
        lambda: {"imps":0,"spend":0.0,"rev":0.0,"reqs":0,"responses":0}))
    for r in rows:
        pub = r.get("publisher")
        k   = r.get(key)
        if not pub or k is None: continue
        if pub not in PARTNERS: continue
        k = str(k)
        out[pub][k]["imps"]      += r.get("impressions", 0) or 0
        out[pub][k]["spend"]     += r.get("dsp_spend", 0.0) or 0.0
        out[pub][k]["rev"]       += r.get("publisher_revenue", 0.0) or 0.0
        out[pub][k]["reqs"]      += r.get("bid_requests", 0) or 0
        out[pub][k]["responses"] += r.get("bid_responses", 0) or 0
    return out


def _deltas(cur_pivot, prior_pivot, min_prior_rev=10.0):
    out = {}
    for pub in PARTNERS:
        cur   = cur_pivot.get(pub, {})
        prior = prior_pivot.get(pub, {})
        rows  = []
        keys  = set(cur) | set(prior)
        for k in keys:
            p = prior.get(k, {"imps":0,"spend":0.0,"rev":0.0,"reqs":0,"responses":0})
            c = cur.get(k,  {"imps":0,"spend":0.0,"rev":0.0,"reqs":0,"responses":0})
            if p["rev"] < min_prior_rev and c["rev"] < min_prior_rev: continue
            delta = c["rev"] - p["rev"]
            rows.append({
                "key": k, "prior_rev": round(p["rev"], 2),
                "cur_rev": round(c["rev"], 2), "delta": round(delta, 2),
                "delta_pct": round(delta / p["rev"] * 100, 1) if p["rev"] else 9999.0,
                "prior_imps": p["imps"], "cur_imps": c["imps"],
                "prior_reqs": p["reqs"], "cur_reqs": c["reqs"],
            })
        rows.sort(key=lambda x: x["delta"])
        out[pub] = rows
    return out


def main():
    today = datetime.now(timezone.utc).date()
    cur_s,  cur_e  = (today - timedelta(days=7)).isoformat(),  today.isoformat()
    prior_s, prior_e = (today - timedelta(days=14)).isoformat(), (today - timedelta(days=7)).isoformat()

    print(f"cur:   {cur_s} → {cur_e}")
    print(f"prior: {prior_s} → {prior_e}\n")

    results = {}
    for axis in ["placement", "country", "company_dsp"]:
        print(f"\n=== {axis.upper()} ===")
        cur_rows   = _pull(cur_s,  cur_e,  axis)
        prior_rows = _pull(prior_s, prior_e, axis)
        results[axis] = _deltas(_pivot(cur_rows, axis), _pivot(prior_rows, axis))

    out_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "logs", "partner_drop_analysis.json")
    with open(out_file, "w") as f:
        json.dump({"timestamp": datetime.now(timezone.utc).isoformat(),
                   "cur_window": [cur_s, cur_e], "prior_window": [prior_s, prior_e],
                   "results": results}, f, indent=2)
    print(f"\nFull → {out_file}\n")

    # Print per-partner summary
    for pub in PARTNERS:
        print(f"\n{'='*78}\n  {pub}\n{'='*78}")
        for axis in ["placement", "country", "company_dsp"]:
            rows = results[axis].get(pub, [])
            if not rows: continue
            drops = [r for r in rows if r["delta"] < -5][:6]
            if not drops: continue
            print(f"\n  TOP DROPS — {axis}")
            for r in drops:
                pct = f"{r['delta_pct']:+.0f}%" if abs(r['delta_pct']) < 9000 else "NEW"
                print(f"    {r['key'][:45]:<45}  "
                      f"${r['prior_rev']:>7.0f} → ${r['cur_rev']:>7.0f}  "
                      f"Δ${r['delta']:>+7.0f}  {pct:>6}  "
                      f"imps {r['prior_imps']:>8,}→{r['cur_imps']:>8,}")


if __name__ == "__main__":
    main()
