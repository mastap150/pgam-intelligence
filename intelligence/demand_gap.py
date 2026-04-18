"""
Demand-gap analysis — identify (publisher, demand) combinations we're NOT
running today that look highly likely to earn based on cross-publisher
analogues.

Method
------
For each active demand partner D, compute its 30d clearing eCPM per
publisher cluster. A cluster groups publishers by inferred trait
(display vs video, in-app vs web, US-heavy vs EU-heavy) derived from
publisher name + geo mix. Then for each (cluster, demand) pair where D
performs well (rev ≥ threshold, WR ≥ threshold) in ≥2 publishers, find
the publishers in that cluster where D is NOT currently running.
These are gap candidates — ranked by expected lift (D's median per-pub
revenue × 4 weeks).

Outputs ranked gaps to data/demand_gaps.json and posts top 5 to Slack.
No automatic action — human decides to add the demand via LL UI; once
activated, the liveness gate promotes it to 'treatment' and quarantine
takes over.

Limitations
-----------
* Cluster inference is heuristic (regex on pub name + geo share); not
  perfect but beats nothing. Tranche 4 can replace with a real embedding
  once we have enough labeled clusters.
* Revenue estimate is a simple transfer: "D earned $X on similar pub A,
  so it might earn $X on missing pub B". Assumes traffic quality is
  comparable within cluster. Call these estimates directional.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from core import slack

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
DAILY_GEO_PATH = DATA_DIR / "daily_pub_demand_country.json.gz"
GAPS_PATH = DATA_DIR / "demand_gaps.json"

LOOKBACK_DAYS = 30
MIN_REV_PER_PUB = 50.0
MIN_WR = 0.01
MIN_PUBS_PER_CLUSTER = 2
TOP_N_SLACK = 5


def _load(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as f:
        return json.load(f)


def _cluster_label(pub_name: str, us_share: float) -> str:
    """Heuristic publisher cluster label. Returns e.g. 'inapp_video_us'."""
    n = (pub_name or "").lower()
    fmt = "video" if any(k in n for k in ("video", "ctv", "vast", "olv")) else "display"
    if "in app" in n or "in-app" in n or "inapp" in n or "app" in n:
        surface = "inapp"
    else:
        surface = "web"
    geo = "us" if us_share >= 0.5 else ("eu" if us_share < 0.25 else "mixed")
    return f"{surface}_{fmt}_{geo}"


def build() -> dict:
    hourly = _load(HOURLY_PATH)
    geo = _load(DAILY_GEO_PATH)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    liveness_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat()

    # 1. Per-pub US share → cluster label
    pub_rev_total: dict[int, float] = defaultdict(float)
    pub_rev_us: dict[int, float] = defaultdict(float)
    pub_name: dict[int, str] = {}
    for r in geo:
        if str(r.get("DATE", "")) < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        if pid == 0:
            continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        pub_rev_total[pid] += rev
        if (r.get("COUNTRY") or "").upper() == "US":
            pub_rev_us[pid] += rev

    # 2. Per-(pub, demand) last-30d rev/wins/bids + liveness
    tuple_agg: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "bids": 0.0, "wins": 0.0, "revenue": 0.0, "recent_bids": 0.0,
        "pub_name": "", "demand_name": "",
    })
    for r in hourly:
        date = str(r.get("DATE", ""))
        if date < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        bids = float(r.get("BIDS", 0) or 0)
        t = tuple_agg[(pid, did)]
        t["bids"] += bids
        t["wins"] += float(r.get("WINS", 0) or 0)
        t["revenue"] += float(r.get("GROSS_REVENUE", 0) or 0)
        t["pub_name"] = r.get("PUBLISHER_NAME", "") or t["pub_name"]
        t["demand_name"] = r.get("DEMAND_NAME", "") or t["demand_name"]
        if date >= liveness_cutoff:
            t["recent_bids"] += bids
        pub_name[pid] = r.get("PUBLISHER_NAME", "") or pub_name.get(pid, "")

    # 3. Cluster each publisher
    pub_cluster: dict[int, str] = {}
    for pid, name in pub_name.items():
        us_share = (pub_rev_us[pid] / pub_rev_total[pid]) if pub_rev_total[pid] > 0 else 0
        pub_cluster[pid] = _cluster_label(name, us_share)

    # 4. For each (cluster, demand), find pubs where D is running well
    cluster_demand_pubs: dict[tuple[str, int], list[dict]] = defaultdict(list)
    demand_name_by_id: dict[int, str] = {}
    for (pid, did), agg in tuple_agg.items():
        if agg["recent_bids"] <= 0:
            continue  # demand not live on this pub — skip as "running well" case
        if agg["bids"] < 1000:
            continue
        wr = agg["wins"] / agg["bids"] if agg["bids"] > 0 else 0
        if agg["revenue"] < MIN_REV_PER_PUB or wr < MIN_WR:
            continue
        cluster = pub_cluster.get(pid)
        if not cluster:
            continue
        cluster_demand_pubs[(cluster, did)].append({
            "publisher_id": pid,
            "publisher_name": agg["pub_name"],
            "revenue_30d": agg["revenue"],
            "win_rate": wr,
        })
        demand_name_by_id[did] = agg["demand_name"] or demand_name_by_id.get(did, "")

    # 5. For each cluster-demand with ≥ MIN_PUBS_PER_CLUSTER running pubs,
    #    find pubs in same cluster where demand is NOT wired.
    pubs_by_cluster: dict[str, set[int]] = defaultdict(set)
    for pid, c in pub_cluster.items():
        if pub_rev_total[pid] >= 100:  # only real publishers
            pubs_by_cluster[c].add(pid)

    active_by_demand: dict[int, set[int]] = defaultdict(set)
    for (pid, did), agg in tuple_agg.items():
        if agg["recent_bids"] > 0:
            active_by_demand[did].add(pid)

    gaps: list[dict] = []
    for (cluster, did), running_pubs in cluster_demand_pubs.items():
        if len(running_pubs) < MIN_PUBS_PER_CLUSTER:
            continue
        median_rev = median(p["revenue_30d"] for p in running_pubs)
        median_wr = median(p["win_rate"] for p in running_pubs)
        running_ids = {p["publisher_id"] for p in running_pubs}
        for pid in pubs_by_cluster[cluster] - running_ids - active_by_demand[did]:
            # gap: this pub is in the cluster but demand isn't wired in.
            # Estimate lift by applying cluster-median rev scaled by this pub's
            # size relative to the running peers.
            running_med_pub_size = median(pub_rev_total[p["publisher_id"]] for p in running_pubs)
            size_ratio = (pub_rev_total[pid] / running_med_pub_size) if running_med_pub_size > 0 else 0
            est_rev_30d = median_rev * min(size_ratio, 3.0)  # cap at 3× to avoid blowups
            gaps.append({
                "publisher_id": pid,
                "publisher_name": pub_name.get(pid, ""),
                "publisher_30d_rev": round(pub_rev_total[pid], 2),
                "demand_id": did,
                "demand_name": demand_name_by_id.get(did, ""),
                "cluster": cluster,
                "running_in_n_pubs": len(running_pubs),
                "peer_median_revenue_30d": round(median_rev, 2),
                "peer_median_win_rate": round(median_wr, 4),
                "est_lift_30d": round(est_rev_30d, 2),
            })

    gaps.sort(key=lambda g: -g["est_lift_30d"])

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "n_gaps": len(gaps),
        "total_est_lift_30d": round(sum(g["est_lift_30d"] for g in gaps), 2),
        "gaps": gaps[:100],    # cap the write
    }
    GAPS_PATH.write_text(json.dumps(out, indent=2))
    return out


def post_to_slack(out: dict | None = None) -> dict:
    if out is None:
        if not GAPS_PATH.exists():
            return {"posted": False}
        out = json.loads(GAPS_PATH.read_text())

    gaps = out.get("gaps", [])
    if not gaps:
        slack.send_text("🧩 *Demand-gap scan*: no new wiring opportunities this week.")
        return {"posted": True, "note": "no gaps"}

    top = gaps[:TOP_N_SLACK]
    lines = [f"🧩 *Demand-gap scan* — {out['n_gaps']} missing (pub × demand) wirings, "
             f"total est. +${out['total_est_lift_30d']:,.0f}/30d"]
    lines.append("\n*Top candidates:*")
    for g in top:
        lines.append(
            f"  • Wire *{g['demand_name'][:30]}* into _{g['publisher_name'][:25]}_ "
            f"(cluster `{g['cluster']}`, runs on {g['running_in_n_pubs']} peers at "
            f"${g['peer_median_revenue_30d']:.0f}/30d, {g['peer_median_win_rate']*100:.1f}% WR) — "
            f"est. +${g['est_lift_30d']:.0f}/30d"
        )
    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}],
        text="Demand-gap scan",
    )
    return {"posted": True, "top_n": len(top)}


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
        if not GAPS_PATH.exists():
            print("no gaps file yet")
            return
        out = json.loads(GAPS_PATH.read_text())
        print(f"n_gaps={out['n_gaps']}  total_est_lift_30d=${out['total_est_lift_30d']:,.0f}")
        for g in out["gaps"][:25]:
            print(f"  +${g['est_lift_30d']:>6,.0f}  wire {g['demand_name'][:28]:<30} into "
                  f"{g['publisher_name'][:25]:<27}  [{g['cluster']}]  "
                  f"peers={g['running_in_n_pubs']} wr={g['peer_median_win_rate']*100:.1f}%")
        return
    out = build()
    print(f"n_gaps={out['n_gaps']}  total_est_lift_30d=${out['total_est_lift_30d']:,.0f}")
    if args.post:
        post_to_slack(out)


if __name__ == "__main__":
    main()
