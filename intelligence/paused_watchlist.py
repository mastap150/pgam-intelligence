"""
Paused-partner watchlist — weekly resurrect/prune signal.

Some (publisher, demand) tuples go inactive because a human or an earlier
optimizer decided the partner wasn't earning its keep. Market conditions
change: a paused partner's pre-pause clearing eCPM may now be competitive
against the partners actually running on the same publisher.

This module:
  1. Finds every 'inactive' tuple from the holdout assignment.
  2. For each, pulls its last 30d of pre-pause eCPM samples (when BIDS > 0
     and WINS > 0).
  3. Compares its historical clear eCPM distribution to the CURRENT median
     clear eCPM of the active demands on the SAME publisher.
  4. Emits a ranked resurrect list:
       strong_resurrect  →  paused partner's p40 exceeds active-peer median
       watch             →  paused partner's p40 exceeds active-peer p25
       prune             →  paused partner's historical eCPM well below peers
                            (safe to archive permanently / notify rep)

Writes data/paused_watchlist.json and (if a Slack webhook is configured)
posts a weekly digest for human review.

Nothing is auto-resurrected. The optimizer still refuses to write to
inactive tuples — this module only surfaces candidates for a human +
rep conversation. If a resurrect is approved, the human unpauses via
LL UI, the liveness gate naturally promotes the tuple back to treatment,
and quarantine takes over from there.
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from core import slack

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
HOLDOUT_DETAIL_PATH = DATA_DIR / "holdout_tuples_detail.json"
WATCHLIST_PATH = DATA_DIR / "paused_watchlist.json"

MIN_HISTORICAL_WINS = 100     # need real pre-pause data
LOOKBACK_DAYS = 30


def _load_hourly() -> list[dict]:
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def build() -> dict:
    """Produce ranked resurrect candidates + pruning list."""
    if not HOLDOUT_DETAIL_PATH.exists():
        return {"error": "run intelligence.holdout --tuples first"}
    tuples = json.loads(HOLDOUT_DETAIL_PATH.read_text())
    inactive = [t for t in tuples if t["group"] == "inactive"]
    if not inactive:
        WATCHLIST_PATH.write_text(json.dumps(
            {"generated_utc": datetime.now(timezone.utc).isoformat(),
             "candidates": []}, indent=2))
        return {"inactive_count": 0, "candidates": 0}

    rows = _load_hourly()
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
    liveness_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat()

    # Active peers per publisher: clear eCPM samples grouped by pub,
    # taken only from (pub, demand) tuples currently live (recent BIDS > 0).
    active_peer_samples: dict[int, list[float]] = defaultdict(list)
    # Paused tuple eCPM samples — last 30d across the full window
    paused_samples: dict[tuple[int, int], list[float]] = defaultdict(list)
    inactive_set = {(t["publisher_id"], t["demand_id"]) for t in inactive}
    recent_bids_by_tuple: dict[tuple[int, int], float] = defaultdict(float)

    for r in rows:
        date = str(r.get("DATE", ""))
        if date < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        wins = float(r.get("WINS", 0) or 0)
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        bids = float(r.get("BIDS", 0) or 0)
        if date >= liveness_cutoff:
            recent_bids_by_tuple[(pid, did)] += bids

        if wins <= 0 or rev <= 0:
            continue
        ecpm = rev / wins * 1000.0

        if (pid, did) in inactive_set:
            paused_samples[(pid, did)].append(ecpm)
        # else: potentially an active peer — include it below if it's actually live now

    # Compute active-peer clearing-eCPM distribution per publisher
    # (using last 30d eCPM for any currently-live (pub, demand) tuple on that pub)
    for r in rows:
        date = str(r.get("DATE", ""))
        if date < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        if recent_bids_by_tuple.get((pid, did), 0) <= 0:
            continue  # not currently live — skip
        wins = float(r.get("WINS", 0) or 0)
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        if wins <= 0 or rev <= 0:
            continue
        active_peer_samples[pid].append(rev / wins * 1000.0)

    candidates = []
    for t in inactive:
        key = (t["publisher_id"], t["demand_id"])
        samples = paused_samples.get(key, [])
        if len(samples) < MIN_HISTORICAL_WINS // 5:  # need ≥20 hourly eCPM pts
            verdict = "insufficient_history"
            peer_info = {}
            paused_p40 = None
        else:
            peers = active_peer_samples.get(t["publisher_id"], [])
            if not peers:
                verdict = "no_active_peers"   # publisher itself inactive
                peer_info = {}
            else:
                paused_p40 = _percentile(samples, 0.40)
                peer_median = median(peers)
                peer_p25 = _percentile(peers, 0.25)
                peer_info = {
                    "peer_median_ecpm": round(peer_median, 4),
                    "peer_p25_ecpm": round(peer_p25, 4),
                    "peer_sample_size": len(peers),
                    "paused_p40_ecpm": round(paused_p40, 4),
                }
                if paused_p40 >= peer_median:
                    verdict = "strong_resurrect"
                elif paused_p40 >= peer_p25:
                    verdict = "watch"
                else:
                    verdict = "prune"

        candidates.append({
            "publisher_id": t["publisher_id"],
            "publisher_name": t["publisher_name"],
            "demand_id": t["demand_id"],
            "demand_name": t["demand_name"],
            "revenue_30d_pre_pause": t["revenue_30d"],
            "bids_30d_pre_pause": t["bids_30d"],
            "ecpm_samples": len(samples),
            "verdict": verdict,
            **peer_info,
        })

    order = {"strong_resurrect": 0, "watch": 1, "insufficient_history": 2,
             "no_active_peers": 3, "prune": 4}
    candidates.sort(key=lambda c: (order.get(c["verdict"], 99), -c["revenue_30d_pre_pause"]))

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "inactive_count": len(inactive),
        "verdicts": {v: sum(1 for c in candidates if c["verdict"] == v)
                     for v in ["strong_resurrect", "watch", "prune",
                               "insufficient_history", "no_active_peers"]},
        "candidates": candidates,
    }
    WATCHLIST_PATH.write_text(json.dumps(out, indent=2))
    return out


def post_to_slack(out: dict | None = None) -> dict:
    if out is None:
        if not WATCHLIST_PATH.exists():
            return {"posted": False, "reason": "no watchlist"}
        out = json.loads(WATCHLIST_PATH.read_text())

    resurrect = [c for c in out["candidates"] if c["verdict"] == "strong_resurrect"]
    prune = [c for c in out["candidates"] if c["verdict"] == "prune"]
    if not resurrect and not prune:
        slack.send_text("🔕 *Paused-partner watchlist*: no strong signals this week.")
        return {"posted": True, "note": "no signals"}

    lines = [f"🗂 *Paused-partner watchlist* — {len(resurrect)} to resurrect, {len(prune)} to prune"]
    if resurrect:
        lines.append("\n*Resurrect candidates* (paused p40 ≥ peer median on same pub):")
        for c in resurrect[:8]:
            lines.append(
                f"  • *{c['publisher_name'][:25]}* / _{c['demand_name'][:30]}_ — "
                f"paused p40 ${c['paused_p40_ecpm']:.2f} vs peer median ${c['peer_median_ecpm']:.2f}  "
                f"(30d pre-pause rev ${c['revenue_30d_pre_pause']:,.0f})"
            )
    if prune:
        lines.append("\n*Prune candidates* (historical eCPM well below peers):")
        for c in prune[:8]:
            lines.append(
                f"  • *{c['publisher_name'][:25]}* / _{c['demand_name'][:30]}_ — "
                f"paused p40 ${c['paused_p40_ecpm']:.2f} < peer p25 ${c['peer_p25_ecpm']:.2f}"
            )
    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}],
        text="Paused-partner watchlist",
    )
    return {"posted": True, "resurrect": len(resurrect), "prune": len(prune)}


def run() -> dict:
    out = build()
    if "error" in out:
        return out
    post_to_slack(out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--post", action="store_true")
    args = ap.parse_args()
    if args.show:
        if not WATCHLIST_PATH.exists():
            print("no watchlist yet")
            return
        out = json.loads(WATCHLIST_PATH.read_text())
        print(f"inactive={out['inactive_count']}  verdicts={out['verdicts']}")
        for c in out["candidates"][:20]:
            tag = c["verdict"]
            extra = ""
            if "paused_p40_ecpm" in c:
                extra = f" p40=${c['paused_p40_ecpm']:.2f} peer_med=${c.get('peer_median_ecpm', 0):.2f}"
            print(f"  [{tag:<22}] {c['publisher_name'][:25]:<27} / {c['demand_name'][:35]:<37} "
                  f"rev30d=${c['revenue_30d_pre_pause']:>6,.0f}{extra}")
        return
    out = build()
    print(json.dumps({k: v for k, v in out.items() if k != "candidates"}, indent=2))
    if args.post:
        post_to_slack(out)


if __name__ == "__main__":
    main()
