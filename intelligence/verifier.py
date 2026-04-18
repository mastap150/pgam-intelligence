"""
Post-write verifier — re-reads the LL management API after floor changes
to confirm they actually persisted. The Apr-17 post-mortem found phase1b
entries claiming `applied=true` but live state was `minBidFloor=null`
(either the PUT failed silently or something reverted them). We don't
want to plan optimizer actions on top of a ledger that doesn't reflect
reality.

For each recent ledger entry with applied=true AND dry_run=false, fetch
the publisher and compare the recorded new_floor vs the current live
minBidFloor on the demand adapter.

Outcomes written to data/verification_log.jsonl.gz:
    - "ok"         live matches new_floor (within 0.001)
    - "drifted"    live differs from new_floor (another write happened after)
    - "reverted"   live == old_floor  (common form of drift)
    - "missing"    demand no longer present on publisher
    - "error"      fetch failed

Usage:
    python -m intelligence.verifier --window-hours 48
    python -m intelligence.verifier --show         # last verification summary
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, ll_mgmt

DATA_DIR = Path(__file__).parent.parent / "data"
LOG_PATH = DATA_DIR / "verification_log.jsonl.gz"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _append(row: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    mode = "at" if LOG_PATH.exists() else "wt"
    with gzip.open(LOG_PATH, mode) as f:
        f.write(json.dumps(row) + "\n")


def _read_log() -> list[dict]:
    if not LOG_PATH.exists():
        return []
    out = []
    with gzip.open(LOG_PATH, "rt") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _floor_equal(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) < 0.001
    except (TypeError, ValueError):
        return False


def verify(window_hours: int = 48) -> dict:
    """Re-read every applied ledger entry within the last `window_hours` and
    compare to live state. Group by publisher so we only fetch each pub once."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    entries = [
        r for r in floor_ledger.read_all()
        if r["applied"] and not r["dry_run"] and r["ts_utc"] >= cutoff
    ]
    if not entries:
        return {"verified": 0, "window_hours": window_hours, "note": "no recent applied entries"}

    # For each (pub, demand), keep only the LATEST ledger entry within window
    # — we only care if the most-recent intended state is live.
    latest: dict[tuple[int, int], dict] = {}
    for e in entries:
        k = (e["publisher_id"], e["demand_id"])
        if k not in latest or e["ts_utc"] > latest[k]["ts_utc"]:
            latest[k] = e

    # Fetch each publisher once
    by_pub: dict[int, list[dict]] = defaultdict(list)
    for k, e in latest.items():
        by_pub[k[0]].append(e)

    outcomes = Counter()
    details = []
    for pub_id, pub_entries in by_pub.items():
        try:
            pub = ll_mgmt.get_publisher(pub_id)
        except Exception as exc:
            for e in pub_entries:
                d = {
                    "ts_utc": _now_iso(),
                    "ledger_id": e["id"],
                    "publisher_id": pub_id,
                    "demand_id": e["demand_id"],
                    "expected": e["new_floor"],
                    "actual": None,
                    "outcome": "error",
                    "error": str(exc),
                }
                _append(d)
                details.append(d)
                outcomes["error"] += 1
            continue

        live_map = {}
        for pref in pub.get("biddingpreferences", []):
            for v in pref.get("value", []):
                did = v.get("id")
                if did is not None:
                    live_map[int(did)] = v

        for e in pub_entries:
            live = live_map.get(e["demand_id"])
            if live is None:
                outcome = "missing"
                actual = None
            else:
                actual = live.get("minBidFloor")
                if _floor_equal(actual, e["new_floor"]):
                    outcome = "ok"
                elif _floor_equal(actual, e["old_floor"]):
                    outcome = "reverted"
                else:
                    outcome = "drifted"

            d = {
                "ts_utc": _now_iso(),
                "ledger_id": e["id"],
                "publisher_id": pub_id,
                "publisher_name": e.get("publisher_name", ""),
                "demand_id": e["demand_id"],
                "demand_name": e.get("demand_name", ""),
                "actor": e.get("actor", ""),
                "expected": e["new_floor"],
                "actual": actual,
                "prior": e["old_floor"],
                "outcome": outcome,
            }
            _append(d)
            details.append(d)
            outcomes[outcome] += 1

    return {
        "verified": sum(outcomes.values()),
        "window_hours": window_hours,
        "outcomes": dict(outcomes),
        "issues": [d for d in details if d["outcome"] in ("reverted", "drifted", "missing", "error")],
    }


def run() -> dict:
    """Scheduler entry point — verify last 48h of writes."""
    return verify(window_hours=48)


def show_recent(limit: int = 20) -> None:
    rows = _read_log()[-limit:]
    if not rows:
        print("no verification records yet")
        return
    print(f"{'ts':<20} {'pub':>10} {'demand':>6} {'expected':>9} {'actual':>9} outcome")
    for r in rows:
        print(f"{r['ts_utc'][:19]:<20} {r['publisher_id']:>10} {r['demand_id']:>6} "
              f"{str(r.get('expected')):>9} {str(r.get('actual')):>9} {r['outcome']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=48)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    if args.show:
        show_recent()
        return
    out = verify(window_hours=args.window_hours)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
