"""
New-partner quarantine harness — 14-day trial phase for any (publisher,
demand) tuple that's newly wired into the waterfall.

Lifecycle
---------
  NEW         first seen in the hourly data with BIDS > 0
   │
   ▼
  QUARANTINE  on-boarded; optimizer may NOT change its floor; watchdog
              monitors for obvious misfires; 14 days
   │
   ▼
  GRADUATED   scorecard beats trial bar (net_rev_per_1000_bids ≥
              portfolio p50) → promoted; optimizer fully in charge
   ⇘
    FAILED    misses bar after 14 days → pause recommendation emitted to
              Slack; tuple sits unpaused until a human decides
              (we don't auto-disable — safer to keep the signal alive
              until rep is consulted)

The "first seen" pointer is persisted in data/quarantine_state.json so
we don't re-quarantine tuples that have always existed. On the very
first run, every currently-live tuple is seeded as already-graduated
(they're clearly past probation).

Gates
-----
    is_in_quarantine(pub_id, demand_id)  →  bool
    optimizer.py calls this alongside holdout.is_tuple_held_out — any
    True result blocks a proposal from being generated or applied.
"""
from __future__ import annotations

import argparse
import gzip
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core import slack
from intelligence import price_response

DATA_DIR = Path(__file__).parent.parent / "data"
STATE_PATH = DATA_DIR / "quarantine_state.json"

QUARANTINE_DAYS = 14
TRIAL_BAR_PERCENTILE = 0.50        # must beat median net_rev_per_bid


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"initialized": False, "first_seen": {},  # key=f"{pid}:{did}" → ISO date
                "status": {}}                            # key → 'quarantine'|'graduated'|'failed'
    return json.loads(STATE_PATH.read_text())


def _save_state(s: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(s, indent=2))


def is_in_quarantine(publisher_id: int, demand_id: int) -> bool:
    s = _load_state()
    key = f"{publisher_id}:{demand_id}"
    return s.get("status", {}).get(key) == "quarantine"


def _scan_live_tuples() -> dict[tuple[int, int], dict]:
    """Return per-live-tuple 14d aggregate stats (bids, wins, revenue) for
    partners with BIDS>0 in last 7d."""
    hourly_path = DATA_DIR / "hourly_pub_demand.json.gz"
    if not hourly_path.exists():
        return {}
    with gzip.open(hourly_path, "rt") as f:
        rows = json.load(f)
    cutoff = (_today() - timedelta(days=14)).isoformat()
    liveness_cutoff = (_today() - timedelta(days=7)).isoformat()
    agg: dict[tuple[int, int], dict] = {}
    for r in rows:
        date_str = str(r.get("DATE", ""))
        if date_str < cutoff:
            continue
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        bids = float(r.get("BIDS", 0) or 0)
        a = agg.setdefault((pid, did), {"bids": 0.0, "wins": 0.0, "revenue": 0.0,
                                         "recent_bids": 0.0,
                                         "pub_name": "", "demand_name": ""})
        a["bids"] += bids
        a["wins"] += float(r.get("WINS", 0) or 0)
        a["revenue"] += float(r.get("GROSS_REVENUE", 0) or 0)
        if date_str >= liveness_cutoff:
            a["recent_bids"] += bids
        a["pub_name"] = r.get("PUBLISHER_NAME", "") or a["pub_name"]
        a["demand_name"] = r.get("DEMAND_NAME", "") or a["demand_name"]
    return {k: v for k, v in agg.items() if v["recent_bids"] > 0}


def _trial_bar(live_agg: dict) -> float:
    """The bar: portfolio median of 14d net-rev-per-1000-bids among
    currently-live tuples. Simple baseline — Tranche 4 can make this
    cluster-specific."""
    rpms = []
    for a in live_agg.values():
        if a["bids"] < 1000:
            continue
        rpms.append(a["revenue"] / a["bids"] * 1000)
    if not rpms:
        return 0.0
    rpms.sort()
    return rpms[int(len(rpms) * TRIAL_BAR_PERCENTILE)]


def _eval_quarantined(key, first_seen_iso, live_agg, bar):
    """Return new status: 'quarantine' | 'graduated' | 'failed'."""
    days_in = (_today() - date.fromisoformat(first_seen_iso)).days
    if days_in < QUARANTINE_DAYS:
        return "quarantine"
    pid, did = map(int, key.split(":"))
    a = live_agg.get((pid, did))
    if not a or a["bids"] < 500:
        # Didn't even get enough volume during trial — mark failed; rep
        # conversation will decide next steps.
        return "failed"
    rpm = a["revenue"] / a["bids"] * 1000
    return "graduated" if rpm >= bar else "failed"


def run() -> dict:
    s = _load_state()
    live = _scan_live_tuples()
    bar = _trial_bar(live)

    # First-run seeding: treat everything already live as graduated (they're
    # past any reasonable probation).
    if not s["initialized"]:
        for (pid, did) in live.keys():
            key = f"{pid}:{did}"
            s["first_seen"][key] = _today().isoformat()
            s["status"][key] = "graduated"
        s["initialized"] = True
        s["seeded_utc"] = datetime.now(timezone.utc).isoformat()
        _save_state(s)
        return {"seeded": len(live), "bar": round(bar, 4), "first_run": True}

    newly_quarantined, promoted, failed = [], [], []
    for (pid, did) in live.keys():
        key = f"{pid}:{did}"
        if key not in s["first_seen"]:
            s["first_seen"][key] = _today().isoformat()
            s["status"][key] = "quarantine"
            newly_quarantined.append({"key": key,
                                       "pub_name": live[(pid, did)]["pub_name"],
                                       "demand_name": live[(pid, did)]["demand_name"]})
            continue
        if s["status"].get(key) != "quarantine":
            continue
        new_status = _eval_quarantined(key, s["first_seen"][key], live, bar)
        if new_status != "quarantine":
            s["status"][key] = new_status
            (promoted if new_status == "graduated" else failed).append({
                "key": key,
                "pub_name": live[(pid, did)]["pub_name"],
                "demand_name": live[(pid, did)]["demand_name"],
                "trial_rpm": round(live[(pid, did)]["revenue"] / max(live[(pid, did)]["bids"], 1) * 1000, 4),
                "bar": round(bar, 4),
            })

    _save_state(s)

    # Slack digest if anything interesting happened
    if newly_quarantined or promoted or failed:
        _post_digest(newly_quarantined, promoted, failed, bar)

    return {
        "live_tuples": len(live),
        "trial_bar_rpm": round(bar, 4),
        "newly_quarantined": newly_quarantined,
        "promoted": promoted,
        "failed": failed,
        "status_counts": _count_status(s),
    }


def _count_status(s: dict) -> dict:
    from collections import Counter
    return dict(Counter(s.get("status", {}).values()))


def _post_digest(new, promoted, failed, bar):
    lines = [f"🧪 *Quarantine update* — trial bar: ${bar:.3f} rev/1k bids"]
    if new:
        lines.append(f"\n*Newly quarantined* ({len(new)}):")
        for c in new[:5]:
            lines.append(f"  🔬 {c['pub_name'][:22]} / {c['demand_name'][:30]}")
    if promoted:
        lines.append(f"\n*Graduated* ({len(promoted)}):")
        for c in promoted[:5]:
            lines.append(f"  ✅ {c['pub_name'][:22]} / {c['demand_name'][:30]}  "
                         f"(rpm ${c['trial_rpm']:.3f})")
    if failed:
        lines.append(f"\n*Failed trial — rep conversation suggested* ({len(failed)}):")
        for c in failed[:5]:
            lines.append(f"  ⚠️  {c['pub_name'][:22]} / {c['demand_name'][:30]}  "
                         f"(rpm ${c['trial_rpm']:.3f} < bar ${c['bar']:.3f})")
    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}],
        text="Quarantine update",
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    if args.show:
        s = _load_state()
        print(f"initialized={s.get('initialized')}  status counts={_count_status(s)}")
        return
    out = run()
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
