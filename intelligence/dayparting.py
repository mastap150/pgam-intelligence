"""
Dayparting layer — per-hour floor schedule for high-variance demand_ids.

Background
----------
The LL floor-write endpoint PUT /v1/demands/{id} sets a single global floor
per demand — no native dayparting field exists on the demand object or the
publisher-side biddingpref entry (confirmed 2026-04-18 against demand 692).

We emulate dayparting via a rotating writer: every hour, re-PUT the floor
appropriate for the current UTC hour-of-week. Source of truth is the p40
clear-eCPM-by-hour table from ``intelligence.bid_landscape.build_percentiles``
(data/hourly_percentiles.json.gz), aggregated from per-(pub, demand, how) up
to per-(demand, how) by revenue-weighted average (mirrors the demand-global
PUT semantics).

Safety posture
--------------
- Gated by env flag PGAM_DAYPARTING_ENABLED=1. Default off.
- LL_DRY_RUN=true still wins (via ll_mgmt.set_demand_floor).
- High-variance filter: only daypart demands where
  max_hour_p40 / min_hour_p40 >= VARIANCE_RATIO_THRESHOLD and
  portfolio weekly revenue >= MIN_WEEKLY_REV.
- Per-write ±25 % clip vs current live floor (same as optimizer).
- Quiet-hour threshold: skip write if delta < 5 % of current.
- Dayparting-specific cooldown (default 45 min) — short enough to allow
  hourly rotation but long enough to absorb scheduler jitter.
- Holdout control / quarantine on ANY pub running the demand aborts the
  write (same rule as portfolio_optimizer._any_pub_gated).
- Ledger writes tagged actor="dayparting" so the main optimizer's 24 h
  cooldown can exclude them (see _recent_ledger_ts patch in optimizer.py
  and portfolio_optimizer._in_cooldown).

Usage
-----
    python -m intelligence.dayparting --build      # build candidate table
    python -m intelligence.dayparting --show 692   # show schedule for a demand
    python -m intelligence.dayparting --rotate     # run one rotator tick
    python -m intelligence.dayparting              # build + rotate (scheduler)
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger, ll_mgmt
from intelligence import holdout, quarantine

DATA_DIR = Path(__file__).parent.parent / "data"
PERCENTILES_PATH = DATA_DIR / "hourly_percentiles.json.gz"
COUNTRY_PATH = DATA_DIR / "daily_pub_demand_country.json.gz"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
CANDIDATES_PATH = DATA_DIR / "dayparting_candidates.json"
SCHEDULE_PATH = DATA_DIR / "dayparting_floors.json"

ACTOR = "dayparting"
ENV_FLAG = "PGAM_DAYPARTING_ENABLED"

# High-variance filter
VARIANCE_RATIO_THRESHOLD = 1.5   # max_p40 / min_p40 >= this to be eligible
MIN_WEEKLY_REV = 100.0
MIN_HOURS_WITH_DATA = 12         # need at least half the week-hours populated

# Write-time safety
MAX_CHANGE_FRACTION = 0.25       # ±25 % from current live floor
QUIET_HOUR_THRESHOLD = 0.05      # skip if |Δ| < 5 % of current
DAYPARTING_COOLDOWN_MIN = 45     # min gap between dayparting writes on same demand

# Country → UTC offset (hours). Small map — revenue is very concentrated
# in US/UK/DE for this book. Anything missing falls back to 0.
COUNTRY_UTC_OFFSET = {
    "US": -5, "CA": -5, "MX": -6, "BR": -3,
    "GB": 0, "IE": 0, "PT": 0,
    "DE": 1, "FR": 1, "ES": 1, "IT": 1, "NL": 1, "PL": 1, "SE": 1,
    "TR": 3, "RU": 3, "SA": 3, "AE": 4,
    "IN": 5, "CN": 8, "HK": 8, "SG": 8, "JP": 9, "KR": 9,
    "AU": 10, "NZ": 12,
}


# ────────────────────────────────────────────────────────────────────────────
# Data loading
# ────────────────────────────────────────────────────────────────────────────

def _read_gz_json(path: Path) -> list[dict]:
    with gzip.open(path, "rt") as f:
        return json.load(f)


def _dominant_tz_offset(demand_id: int) -> tuple[int, str]:
    """Revenue-weighted dominant country for a demand_id → (utc_offset_hours, country_code)."""
    if not COUNTRY_PATH.exists():
        return 0, ""
    by_country: dict[str, float] = defaultdict(float)
    for r in _read_gz_json(COUNTRY_PATH):
        if int(r.get("DEMAND_ID", 0)) != demand_id:
            continue
        c = r.get("COUNTRY", "")
        by_country[c] += float(r.get("GROSS_REVENUE", 0) or 0)
    if not by_country:
        return 0, ""
    top_country = max(by_country, key=by_country.get)
    return COUNTRY_UTC_OFFSET.get(top_country.upper(), 0), top_country


def _pubs_per_demand(lookback_days: int = 7) -> dict[int, set[int]]:
    """Pubs with bids>0 in the last N days, per demand_id."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    out: dict[int, set[int]] = defaultdict(set)
    if not HOURLY_PATH.exists():
        return out
    for r in _read_gz_json(HOURLY_PATH):
        if str(r.get("DATE", "")) < cutoff:
            continue
        if float(r.get("BIDS", 0) or 0) <= 0:
            continue
        did = int(r.get("DEMAND_ID", 0))
        pid = int(r.get("PUBLISHER_ID", 0))
        if did and pid:
            out[did].add(pid)
    return out


def _weekly_rev_per_demand(lookback_days: int = 7) -> dict[int, float]:
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    rev: dict[int, float] = defaultdict(float)
    if not HOURLY_PATH.exists():
        return rev
    for r in _read_gz_json(HOURLY_PATH):
        if str(r.get("DATE", "")) < cutoff:
            continue
        did = int(r.get("DEMAND_ID", 0))
        if did:
            rev[did] += float(r.get("GROSS_REVENUE", 0) or 0)
    return {d: v * (7.0 / lookback_days) for d, v in rev.items()}


# ────────────────────────────────────────────────────────────────────────────
# Candidate table — which demands are worth dayparting?
# ────────────────────────────────────────────────────────────────────────────

def build_candidates() -> dict:
    """Aggregate hourly_percentiles from per-(pub, demand, how) up to
    per-(demand, how) by revenue-weighted p40. Tag high-variance demands
    as dayparting candidates and write the schedule table."""
    if not PERCENTILES_PATH.exists():
        raise RuntimeError(
            "hourly_percentiles.json.gz missing — "
            "run `python -m intelligence.bid_landscape` first."
        )

    rows = _read_gz_json(PERCENTILES_PATH)
    # Per-(pub, demand) weekly revenue as weight source.
    pub_rev = defaultdict(float)
    for r in _read_gz_json(HOURLY_PATH):
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=14)).isoformat()
        if str(r.get("DATE", "")) < cutoff:
            continue
        k = (int(r.get("PUBLISHER_ID", 0)), int(r.get("DEMAND_ID", 0)))
        pub_rev[k] += float(r.get("GROSS_REVENUE", 0) or 0)

    # (demand_id, hour_of_week) → weighted p40
    num: dict[tuple[int, int], float] = defaultdict(float)
    den: dict[tuple[int, int], float] = defaultdict(float)
    names: dict[int, str] = {}
    for r in rows:
        did = r["demand_id"]
        pid = r["publisher_id"]
        how = r["hour_of_week"]
        w = pub_rev.get((pid, did), 0.0)
        if w <= 0:
            # fall back to 1.0 so pubs with fresh data but outside lookback still count
            w = 1.0
        num[(did, how)] += r["p40"] * w
        den[(did, how)] += w
        names.setdefault(did, r.get("demand_name", ""))

    # Build per-demand hour_of_week → floor map
    weekly_rev = _weekly_rev_per_demand(lookback_days=7)
    pubs_map = _pubs_per_demand(lookback_days=7)
    schedule: dict[int, dict] = {}
    candidates: list[dict] = []

    per_demand_hours: dict[int, dict[int, float]] = defaultdict(dict)
    for (did, how), n in num.items():
        d = den[(did, how)]
        if d > 0:
            per_demand_hours[did][how] = round(n / d, 4)

    for did, hours in per_demand_hours.items():
        vals = [v for v in hours.values() if v > 0]
        if len(hours) < MIN_HOURS_WITH_DATA or len(vals) < MIN_HOURS_WITH_DATA:
            continue
        mx, mn = max(vals), min(vals)
        if mn <= 0:
            continue
        ratio = mx / mn
        rev = weekly_rev.get(did, 0.0)
        tz_off, tz_country = _dominant_tz_offset(did)
        # local-hour peak is the one the sales story hangs off
        peak_how = max(hours, key=hours.get)
        peak_utc_hod = peak_how % 24
        peak_local_hod = (peak_utc_hod + tz_off) % 24

        entry = {
            "demand_id": did,
            "demand_name": names.get(did, ""),
            "weekly_rev": round(rev, 2),
            "n_pubs": len(pubs_map.get(did, set())),
            "dominant_country": tz_country,
            "utc_offset_hours": tz_off,
            "hours_with_data": len(vals),
            "p40_min": round(mn, 4),
            "p40_max": round(mx, 4),
            "variance_ratio": round(ratio, 3),
            "peak_hour_utc": peak_utc_hod,
            "peak_hour_local": peak_local_hod,
            "eligible": ratio >= VARIANCE_RATIO_THRESHOLD and rev >= MIN_WEEKLY_REV,
        }
        candidates.append(entry)

        if entry["eligible"]:
            schedule[did] = {
                "demand_name": entry["demand_name"],
                "n_pubs": entry["n_pubs"],
                "dominant_country": tz_country,
                "utc_offset_hours": tz_off,
                "hour_of_week_floors": {str(how): hours[how] for how in sorted(hours)},
            }

    candidates.sort(key=lambda e: (-e["variance_ratio"], -e["weekly_rev"]))
    CANDIDATES_PATH.write_text(json.dumps({
        "generated_utc": _now_iso(),
        "variance_ratio_threshold": VARIANCE_RATIO_THRESHOLD,
        "min_weekly_rev": MIN_WEEKLY_REV,
        "eligible_count": sum(1 for c in candidates if c["eligible"]),
        "total_demands_examined": len(candidates),
        "candidates": candidates,
    }, indent=2))
    SCHEDULE_PATH.write_text(json.dumps(schedule, indent=2))
    return {
        "candidates": len(candidates),
        "eligible": len(schedule),
        "schedule_path": str(SCHEDULE_PATH),
    }


# ────────────────────────────────────────────────────────────────────────────
# Rotator — hourly writer
# ────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _current_hour_of_week() -> int:
    now = datetime.now(timezone.utc)
    return now.weekday() * 24 + now.hour


def _in_dayparting_cooldown(demand_id: int) -> bool:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=DAYPARTING_COOLDOWN_MIN)
    for r in floor_ledger.read_all():
        if r.get("demand_id") != demand_id or not r.get("applied") or r.get("dry_run"):
            continue
        if r.get("actor") != ACTOR:
            continue
        last = datetime.fromisoformat(r["ts_utc"].replace("Z", "+00:00"))
        if last >= cutoff:
            return True
    return False


def _any_pub_gated(demand_id: int, pub_ids: set[int]) -> str | None:
    for pid in pub_ids:
        if holdout.is_tuple_held_out(pid, demand_id):
            group = holdout.assign_tuple(pid, demand_id)
            if group == "inactive":
                continue
            return f"pub {pid} in holdout:{group}"
        if quarantine.is_in_quarantine(pid, demand_id):
            return f"pub {pid} in quarantine"
    return None


def _clip(current: float | None, target: float) -> float:
    if current is None or current <= 0:
        return round(target, 4)
    lo = current * (1 - MAX_CHANGE_FRACTION)
    hi = current * (1 + MAX_CHANGE_FRACTION)
    return round(max(lo, min(hi, target)), 4)


def _is_quiet(current: float | None, target: float) -> bool:
    base = max(current or 0.0, 0.05)
    return abs((target - (current or 0.0)) / base) < QUIET_HOUR_THRESHOLD


def rotate(*, dry_run: bool | None = None) -> dict:
    """One rotator tick: read schedule, apply per-demand floor for current hour."""
    enabled = os.environ.get(ENV_FLAG, "0") == "1"
    if not enabled:
        return {"skipped": True, "reason": f"{ENV_FLAG} != 1 (default off)"}

    if not SCHEDULE_PATH.exists():
        return {"skipped": True, "reason": "no schedule — run --build first"}

    schedule = json.loads(SCHEDULE_PATH.read_text())
    how = _current_hour_of_week()
    pubs_map = _pubs_per_demand(lookback_days=7)

    results = []
    for did_str, entry in schedule.items():
        did = int(did_str)
        outcome: dict = {"demand_id": did, "demand_name": entry.get("demand_name", "")}

        target = entry["hour_of_week_floors"].get(str(how))
        if target is None:
            outcome["verdict"] = "no_data_for_hour"
            results.append(outcome)
            continue
        outcome["target_raw"] = target

        if _in_dayparting_cooldown(did):
            outcome["verdict"] = "cooldown"
            results.append(outcome)
            continue

        pub_ids = pubs_map.get(did, set())
        gate = _any_pub_gated(did, pub_ids)
        if gate:
            outcome["verdict"] = "gated"
            outcome["detail"] = gate
            results.append(outcome)
            continue

        try:
            live = ll_mgmt._get(f"/v1/demands/{did}").get("minBidFloor")
        except Exception as e:
            outcome["verdict"] = "live_fetch_failed"
            outcome["detail"] = str(e)
            results.append(outcome)
            continue

        live_f = float(live) if live is not None else None
        clipped = _clip(live_f, target)
        outcome["live_floor"] = live_f
        outcome["target_clipped"] = clipped

        if _is_quiet(live_f, clipped):
            outcome["verdict"] = "quiet"
            results.append(outcome)
            continue

        try:
            resp = ll_mgmt.set_demand_floor(
                did, clipped,
                verify=True,
                allow_multi_pub=True,
                _publishers_running_it=len(pub_ids),
                dry_run=bool(dry_run) if dry_run is not None else False,
            )
            outcome["verdict"] = "written"
            outcome["resp"] = resp
            floor_ledger.record(
                publisher_id=0,
                publisher_name=f"[dayparting: {len(pub_ids)} pubs]",
                demand_id=did,
                demand_name=entry.get("demand_name", ""),
                old_floor=live_f,
                new_floor=clipped,
                actor=ACTOR,
                reason=f"hour_of_week={how} target={target} clip±{int(MAX_CHANGE_FRACTION*100)}%",
                dry_run=bool(dry_run) if dry_run is not None else False,
                applied=True,
            )
        except Exception as e:
            outcome["verdict"] = "write_failed"
            outcome["detail"] = str(e)

        results.append(outcome)

    return {
        "ran_at": _now_iso(),
        "hour_of_week": how,
        "enabled": True,
        "demands_in_schedule": len(schedule),
        "results": results,
        "written": sum(1 for r in results if r.get("verdict") == "written"),
    }


# ────────────────────────────────────────────────────────────────────────────
# CLI / scheduler entry
# ────────────────────────────────────────────────────────────────────────────

def run() -> dict:
    """Scheduler entry: build table if stale, then rotate.

    Build is cheap (~seconds) and idempotent — run it every tick so the
    schedule reflects the latest percentile data. Rotate only writes if
    PGAM_DAYPARTING_ENABLED=1.

    Self-bootstrap: on a fresh Render deploy the data/ dir is wiped, so
    hourly_percentiles.json.gz may not exist yet. If the percentiles
    file is missing but the hourly collector file is present, rebuild
    percentiles on the fly. If both are missing the collector hasn't
    run yet — skip this tick and try again next hour."""
    if not PERCENTILES_PATH.exists():
        if not HOURLY_PATH.exists():
            return {"skipped": True, "reason": "hourly_pub_demand.json.gz missing — "
                    "ml_collector has not run yet; will retry next tick"}
        print("[dayparting] hourly_percentiles.json.gz missing — rebuilding from bid_landscape")
        from intelligence import bid_landscape
        bid_landscape.build_percentiles()

    try:
        build_stats = build_candidates()
    except RuntimeError as e:
        return {"skipped": True, "reason": str(e)}
    rotate_stats = rotate()
    return {"build": build_stats, "rotate": rotate_stats}


def show_schedule(demand_id: int) -> None:
    if not SCHEDULE_PATH.exists():
        print("No schedule. Run --build first.")
        return
    schedule = json.loads(SCHEDULE_PATH.read_text())
    entry = schedule.get(str(demand_id))
    if not entry:
        print(f"demand_id={demand_id} is not a dayparting candidate.")
        # show candidate row anyway if present
        if CANDIDATES_PATH.exists():
            cands = json.loads(CANDIDATES_PATH.read_text()).get("candidates", [])
            for c in cands:
                if c["demand_id"] == demand_id:
                    print(f"Variance ratio {c['variance_ratio']} (threshold {VARIANCE_RATIO_THRESHOLD}), "
                          f"weekly rev ${c['weekly_rev']} (min ${MIN_WEEKLY_REV}).")
                    break
        return
    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    print(f"\n{entry['demand_name']} — {entry['n_pubs']} pubs, "
          f"dominant={entry['dominant_country']} (UTC{entry['utc_offset_hours']:+d})")
    print(f"{'DOW':<4} {'Hr':>3} {'floor':>8}  {'local':>5}")
    for how_str, floor in sorted(entry["hour_of_week_floors"].items(), key=lambda kv: int(kv[0])):
        how = int(how_str)
        local = (how % 24 + entry["utc_offset_hours"]) % 24
        print(f"{dows[how // 24]:<4} {how % 24:>3}  ${floor:>6.3f}  {local:>5}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true", help="rebuild candidate + schedule tables only")
    ap.add_argument("--rotate", action="store_true", help="run one rotator tick")
    ap.add_argument("--dry-run", action="store_true", help="rotate in dry-run mode")
    ap.add_argument("--show", type=int, metavar="DEMAND_ID",
                    help="print the schedule for a specific demand_id")
    args = ap.parse_args()

    if args.show is not None:
        show_schedule(args.show)
        return
    if args.build:
        print(json.dumps(build_candidates(), indent=2))
        return
    if args.rotate:
        print(json.dumps(rotate(dry_run=args.dry_run), indent=2, default=str))
        return

    # Default — scheduler-style: build then rotate
    print(json.dumps(run(), indent=2, default=str))


if __name__ == "__main__":
    main()
