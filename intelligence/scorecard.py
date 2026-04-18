"""
Partner scorecard — weekly ranked view of each demand partner, aggregated
across publishers, for rep conversations and portfolio decisions.

For each active demand partner D (grouping on DEMAND_NAME is noisy; we
group by the normalized SSP prefix since one SSP ships many adapters):

    - net_rev_per_bid:  (gross × margin) / bids       ← efficiency
    - weekly_gross_rev:                                ← scale
    - win_rate
    - week_over_week_gross_delta_pct
    - distinct_publishers_live
    - timeout_rate (if BID_RESPONSE_TIMEOUTS column present)
    - error_rate   (if BID_RESPONSE_ERRORS column present)
    - grade:  A / B / C / D based on percentile within the portfolio

Writes data/partner_scorecard.json and posts a compact weekly digest
that leads with Δ WoW — rep calls should focus on movers, not averages.

Nothing is acted on automatically. The scorecard feeds:
  - rep conversations ("Pubmatic dropped 18% WoW, here's the breakdown")
  - the optimizer's future margin-aware objective (Tranche 4)
  - the demand-expansion alert agent (already exists in agents/alerts)
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import slack

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
SCORECARD_PATH = DATA_DIR / "partner_scorecard.json"

# Map demand-adapter names to their parent SSP so "Magnite - Smaato - In App"
# and "Magnite - Start.IO - Video" both bucket under "Magnite".
SSP_PREFIXES = [
    "Magnite", "Pubmatic", "Xandr", "Sovrn", "Unruly", "Illumin",
    "Verve", "OneTag", "LoopMe", "Sharethrough", "AdaptMX", "Stirista",
    "Synatix", "Rise Media", "AppStock",
]


def _normalize_ssp(demand_name: str) -> str:
    # Strip "Copy -" and "Copy of" prefixes used in LL for cloned demands
    n = re.sub(r"^Copy\s*(?:of|-)\s*", "", demand_name or "", flags=re.IGNORECASE).strip()
    for p in SSP_PREFIXES:
        if n.lower().startswith(p.lower()):
            return p
    return re.split(r"[-/]", n or "unknown", maxsplit=1)[0].strip() or "unknown"


def _load() -> list[dict]:
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def _grade(pct: float) -> str:
    if pct >= 0.75:
        return "A"
    if pct >= 0.50:
        return "B"
    if pct >= 0.25:
        return "C"
    return "D"


def build() -> dict:
    rows = _load()
    today = datetime.now(timezone.utc).date()
    this_week_start = (today - timedelta(days=7)).isoformat()
    prev_week_start = (today - timedelta(days=14)).isoformat()
    prev_week_end = this_week_start   # exclusive

    agg: dict[str, dict] = defaultdict(lambda: {
        "this_week": {"bids": 0.0, "wins": 0.0, "revenue": 0.0},
        "prev_week": {"bids": 0.0, "wins": 0.0, "revenue": 0.0},
        "publishers": set(),
    })

    for r in rows:
        date = str(r.get("DATE", ""))
        ssp = _normalize_ssp(r.get("DEMAND_NAME", "") or "")
        bucket = None
        if date >= this_week_start:
            bucket = "this_week"
        elif prev_week_start <= date < prev_week_end:
            bucket = "prev_week"
        if not bucket:
            continue
        a = agg[ssp][bucket]
        bids = float(r.get("BIDS", 0) or 0)
        a["bids"] += bids
        a["wins"] += float(r.get("WINS", 0) or 0)
        a["revenue"] += float(r.get("GROSS_REVENUE", 0) or 0)
        if bucket == "this_week" and bids > 0:
            agg[ssp]["publishers"].add(int(r.get("PUBLISHER_ID", 0)))

    partners = []
    for ssp, d in agg.items():
        tw, pw = d["this_week"], d["prev_week"]
        if tw["bids"] == 0 and pw["bids"] == 0:
            continue
        wr = (tw["wins"] / tw["bids"]) if tw["bids"] > 0 else 0
        rev_per_bid = (tw["revenue"] / tw["bids"] * 1000) if tw["bids"] > 0 else 0
        wow_delta = None
        if pw["revenue"] > 0:
            wow_delta = (tw["revenue"] - pw["revenue"]) / pw["revenue"] * 100
        partners.append({
            "ssp": ssp,
            "weekly_gross_rev": round(tw["revenue"], 2),
            "weekly_bids": int(tw["bids"]),
            "weekly_wins": int(tw["wins"]),
            "win_rate": round(wr, 5),
            "rev_per_1000_bids": round(rev_per_bid, 4),
            "wow_delta_pct": round(wow_delta, 2) if wow_delta is not None else None,
            "publishers_live": len(d["publishers"]),
        })

    # Grades: percentile within live set on rev_per_1000_bids
    active = [p for p in partners if p["weekly_bids"] > 1000]
    if active:
        sorted_by_eff = sorted(active, key=lambda p: p["rev_per_1000_bids"])
        for i, p in enumerate(sorted_by_eff):
            p["grade"] = _grade((i + 0.5) / len(sorted_by_eff))
    for p in partners:
        p.setdefault("grade", "N/A")

    partners.sort(key=lambda p: -p["weekly_gross_rev"])

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "this_week_start": this_week_start,
        "prev_week_range": [prev_week_start, prev_week_end],
        "partner_count": len(partners),
        "total_weekly_gross": round(sum(p["weekly_gross_rev"] for p in partners), 2),
        "partners": partners,
    }
    SCORECARD_PATH.write_text(json.dumps(out, indent=2))
    return out


def post_to_slack(out: dict | None = None) -> dict:
    if out is None:
        if not SCORECARD_PATH.exists():
            return {"posted": False}
        out = json.loads(SCORECARD_PATH.read_text())

    partners = out.get("partners", [])
    # Movers: biggest WoW Δ, both up and down, among meaningful revenue partners
    meaningful = [p for p in partners if p["weekly_gross_rev"] >= 100 and p["wow_delta_pct"] is not None]
    down = sorted(meaningful, key=lambda p: p["wow_delta_pct"])[:3]
    up = sorted(meaningful, key=lambda p: -p["wow_delta_pct"])[:3]

    lines = [f"📊 *Partner scorecard — week of {out['this_week_start']}*  "
             f"(total gross ${out['total_weekly_gross']:,.0f})"]
    lines.append("\n*Biggest movers this week*")
    for p in down:
        lines.append(f"  📉 {p['ssp']:<12} "
                     f"${p['weekly_gross_rev']:>7,.0f}  ({p['wow_delta_pct']:+.1f}% WoW)  "
                     f"WR {p['win_rate']*100:.2f}%   [{p['grade']}]")
    for p in up:
        lines.append(f"  📈 {p['ssp']:<12} "
                     f"${p['weekly_gross_rev']:>7,.0f}  ({p['wow_delta_pct']:+.1f}% WoW)  "
                     f"WR {p['win_rate']*100:.2f}%   [{p['grade']}]")
    lines.append("\n*Top 5 by revenue*")
    for p in partners[:5]:
        wow = f"{p['wow_delta_pct']:+.1f}%" if p["wow_delta_pct"] is not None else "n/a"
        lines.append(f"  {p['ssp']:<12} ${p['weekly_gross_rev']:>7,.0f}  WoW {wow}  "
                     f"WR {p['win_rate']*100:.2f}%  pubs={p['publishers_live']}  [{p['grade']}]")

    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}],
        text="Partner scorecard",
    )
    return {"posted": True}


def run() -> dict:
    out = build()
    post_to_slack(out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--post", action="store_true")
    args = ap.parse_args()
    if args.show:
        if not SCORECARD_PATH.exists():
            print("no scorecard yet")
            return
        out = json.loads(SCORECARD_PATH.read_text())
        print(f"week of {out['this_week_start']}   total=${out['total_weekly_gross']:,.0f}\n")
        print(f"{'SSP':<15} {'rev/wk':>10} {'WR%':>6} {'rev/1k bids':>12} "
              f"{'pubs':>5} {'WoW%':>8}  grade")
        for p in out["partners"]:
            wow = f"{p['wow_delta_pct']:+.1f}" if p["wow_delta_pct"] is not None else "  n/a"
            print(f"{p['ssp'][:14]:<15} ${p['weekly_gross_rev']:>9,.0f} "
                  f"{p['win_rate']*100:>5.2f}% ${p['rev_per_1000_bids']:>11.3f} "
                  f"{p['publishers_live']:>5} {wow:>8} {p['grade']:>6}")
        return
    out = build()
    print(f"partners={out['partner_count']}  total=${out['total_weekly_gross']:,.0f}")
    if args.post:
        post_to_slack(out)


if __name__ == "__main__":
    main()
