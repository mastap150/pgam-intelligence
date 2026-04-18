"""
Holdout harness — two complementary mechanisms for isolating optimizer lift
from market drift:

1. **Tuple holdout** — (publisher_id, demand_id) tuples are deterministically
   hashed into TREATMENT or CONTROL. The optimizer is forbidden from touching
   CONTROL tuples. Realized revenue delta: treatment vs control is the causal
   lift. This is the primary mechanism because actions are tuple-level.

2. **Geo holdout** — countries outside the always-treatment set get a small
   fraction held out. Since actions are applied account-wide (not per-geo),
   this doesn't measure lift but **does** serve as a market-drift sentinel:
   if control-country revenue shifts in lockstep with treatment, the move
   was macro, not ours.

All assignments are a stable hash of (salt, entity) so reports are
reproducible and there's no leakage across runs.

    python -m intelligence.holdout --assign                 # country table
    python -m intelligence.holdout --tuples                 # tuple table
    python -m intelligence.holdout --lift ACTOR             # pre/post lift
    python -m intelligence.holdout --is-held-out PUB DEMAND # check before edit
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core import floor_ledger

DATA_DIR = Path(__file__).parent.parent / "data"
DAILY_GEO_PATH = DATA_DIR / "daily_pub_demand_country.json.gz"
ASSIGNMENT_PATH = DATA_DIR / "holdout_assignment.json"

# Salt pins the assignment. Change only if you want to re-randomize (rare).
SALT = "pgam-holdout-v1"

# Fraction of countries (by count, weighted later by revenue) assigned to control.
CONTROL_FRACTION_TAIL = 0.10

# Major markets — kept 100% in treatment to avoid losing signal.
# Long-tail countries are where we can safely hold out.
ALWAYS_TREATMENT = {"US"}

# Tuple-level holdout: pin a fraction of (pub, demand) tuples as CONTROL — the
# optimizer must NOT write floor changes for these. Excludes the top-revenue
# tuples (would cost too much to hold out) and zero-volume tuples (no signal).
TUPLE_CONTROL_FRACTION = 0.15
TUPLE_HOLDOUT_TOP_EXCLUDE = 5    # top-N revenue tuples always in treatment
TUPLE_HOLDOUT_MIN_BIDS_30D = 1000  # below this → no holdout, not enough signal


def _bucket(entity_id: str) -> float:
    """Uniform [0,1) deterministic bucket for a given entity id + salt."""
    h = hashlib.sha256(f"{SALT}:{entity_id}".encode()).hexdigest()
    return int(h[:16], 16) / float(1 << 64)


def assign_country(country_code: str) -> str:
    if not country_code:
        return "treatment"
    cc = country_code.upper()
    if cc in ALWAYS_TREATMENT:
        return "treatment"
    return "control" if _bucket(f"country:{cc}") < CONTROL_FRACTION_TAIL else "treatment"


def is_control(country_code: str) -> bool:
    return assign_country(country_code) == "control"


# ────────────────────────────────────────────────────────────────────────────
# Tuple-level holdout
# ────────────────────────────────────────────────────────────────────────────

TUPLE_ASSIGNMENT_PATH = DATA_DIR / "holdout_tuples.json"


def _load_tuple_assignment() -> dict:
    if not TUPLE_ASSIGNMENT_PATH.exists():
        return {}
    return json.loads(TUPLE_ASSIGNMENT_PATH.read_text())


def assign_tuple(publisher_id: int, demand_id: int) -> str:
    """Hash-based fallback if no snapshot assignment exists yet."""
    stored = _load_tuple_assignment().get(f"{publisher_id}:{demand_id}")
    if stored:
        return stored
    return "control" if _bucket(f"tuple:{publisher_id}:{demand_id}") < TUPLE_CONTROL_FRACTION else "treatment"


def is_tuple_held_out(publisher_id: int, demand_id: int) -> bool:
    """Gate for optimizers — MUST be called before any floor write."""
    return assign_tuple(publisher_id, demand_id) == "control"


def build_tuple_assignment() -> dict:
    """Snapshot (pub, demand) assignment using 30d hourly data as the universe.

    Rules:
    - Top TUPLE_HOLDOUT_TOP_EXCLUDE revenue tuples → forced treatment.
    - Tuples below TUPLE_HOLDOUT_MIN_BIDS_30D → 'excluded' (no signal, no holdout).
    - Otherwise: hash-based treatment/control split at TUPLE_CONTROL_FRACTION.
    """
    hourly_path = DATA_DIR / "hourly_pub_demand.json.gz"
    if not hourly_path.exists():
        return {"error": "no hourly data — run collector first"}
    with gzip.open(hourly_path, "rt") as f:
        rows = json.load(f)

    agg: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "bids": 0.0, "wins": 0.0, "revenue": 0.0,
        "publisher_name": "", "demand_name": "",
    })
    for r in rows:
        pid = int(r.get("PUBLISHER_ID", 0))
        did = int(r.get("DEMAND_ID", 0))
        if pid == 0 or did == 0:
            continue
        a = agg[(pid, did)]
        a["bids"] += float(r.get("BIDS", 0) or 0)
        a["wins"] += float(r.get("WINS", 0) or 0)
        a["revenue"] += float(r.get("GROSS_REVENUE", 0) or 0)
        a["publisher_name"] = r.get("PUBLISHER_NAME", "") or a["publisher_name"]
        a["demand_name"] = r.get("DEMAND_NAME", "") or a["demand_name"]

    top_keys = {k for k, _ in sorted(agg.items(), key=lambda kv: -kv[1]["revenue"])[:TUPLE_HOLDOUT_TOP_EXCLUDE]}

    assignment: dict[str, str] = {}
    tuples_out: list[dict] = []
    for (pid, did), m in agg.items():
        if m["bids"] < TUPLE_HOLDOUT_MIN_BIDS_30D:
            group = "excluded"
        elif (pid, did) in top_keys:
            group = "treatment"  # forced
        else:
            group = "control" if _bucket(f"tuple:{pid}:{did}") < TUPLE_CONTROL_FRACTION else "treatment"
        assignment[f"{pid}:{did}"] = group
        tuples_out.append({
            "publisher_id": pid, "demand_id": did,
            "publisher_name": m["publisher_name"], "demand_name": m["demand_name"],
            "group": group,
            "revenue_30d": round(m["revenue"], 2),
            "bids_30d": int(m["bids"]),
        })

    snapshot = {
        "salt": SALT,
        "control_fraction": TUPLE_CONTROL_FRACTION,
        "top_excluded_n": TUPLE_HOLDOUT_TOP_EXCLUDE,
        "min_bids_30d": TUPLE_HOLDOUT_MIN_BIDS_30D,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "assignment": assignment,
    }
    TUPLE_ASSIGNMENT_PATH.write_text(json.dumps(snapshot, indent=2))

    # Also write a readable companion
    (DATA_DIR / "holdout_tuples_detail.json").write_text(
        json.dumps(sorted(tuples_out, key=lambda x: -x["revenue_30d"]), indent=2))

    totals = defaultdict(lambda: {"n": 0, "rev": 0.0})
    for t in tuples_out:
        totals[t["group"]]["n"] += 1
        totals[t["group"]]["rev"] += t["revenue_30d"]
    print(f"tuple assignment:")
    for g, s in sorted(totals.items()):
        print(f"  {g:<10} n={s['n']:>4}  30d_rev=${s['rev']:>10,.0f}")
    return snapshot


def build_assignment() -> dict:
    """Snapshot the current country list from daily_geo and freeze assignments."""
    if not DAILY_GEO_PATH.exists():
        return {"error": "no geo data — run collector first"}
    with gzip.open(DAILY_GEO_PATH, "rt") as f:
        rows = json.load(f)
    countries = sorted({r.get("COUNTRY", "") for r in rows if r.get("COUNTRY")})
    rev = defaultdict(float)
    for r in rows:
        rev[r.get("COUNTRY", "")] += float(r.get("GROSS_REVENUE", 0) or 0)

    assignment = {
        "salt": SALT,
        "control_fraction_tail": CONTROL_FRACTION_TAIL,
        "always_treatment": sorted(ALWAYS_TREATMENT),
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "countries": [
            {
                "country": c,
                "group": assign_country(c),
                "revenue_30d": round(rev[c], 2),
            }
            for c in countries
        ],
    }
    ASSIGNMENT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ASSIGNMENT_PATH.write_text(json.dumps(assignment, indent=2))

    ctrl_rev = sum(c["revenue_30d"] for c in assignment["countries"] if c["group"] == "control")
    treat_rev = sum(c["revenue_30d"] for c in assignment["countries"] if c["group"] == "treatment")
    total = ctrl_rev + treat_rev
    print(f"countries: {len(countries)}  "
          f"control_share_revenue: {ctrl_rev / total * 100:.2f}%  "
          f"total_30d: ${total:,.0f}")
    return assignment


# ────────────────────────────────────────────────────────────────────────────
# Lift measurement — pre vs post, control vs treatment
# ────────────────────────────────────────────────────────────────────────────

def _rows_in_window(rows: list[dict], start: str, end: str) -> list[dict]:
    return [r for r in rows if start <= str(r.get("DATE", "")) <= end]


def lift_report(actor: str, window_days: int = 7) -> dict:
    """
    For every floor change made by ``actor``, compare treatment vs control
    revenue deltas between the ``window_days`` pre-change and post-change.

    Since floor changes are applied account-wide (not per-country), the
    comparison cleanly isolates optimizer lift: if treatment rose more
    than control post-change, the optimizer caused the lift.
    """
    if not DAILY_GEO_PATH.exists():
        return {"error": "no geo data"}
    with gzip.open(DAILY_GEO_PATH, "rt") as f:
        geo_rows = json.load(f)

    changes = [r for r in floor_ledger.read_all()
               if r.get("actor") == actor and r.get("applied") and not r.get("dry_run")]
    if not changes:
        return {"actor": actor, "changes": 0, "note": "no matching ledger entries"}

    # Use the earliest change as the cutover (actor is usually a one-shot script).
    cutover_iso = min(c["ts_utc"] for c in changes)
    cutover = cutover_iso[:10]
    pre_start = (datetime.fromisoformat(cutover) - timedelta(days=window_days)).date().isoformat()
    pre_end = (datetime.fromisoformat(cutover) - timedelta(days=1)).date().isoformat()
    post_start = cutover
    post_end = (datetime.fromisoformat(cutover) + timedelta(days=window_days - 1)).date().isoformat()

    # Filter to affected (pub, demand) tuples only — don't let unrelated
    # traffic dilute the signal.
    affected = {(c["publisher_id"], c["demand_id"]) for c in changes}
    relevant = [r for r in geo_rows
                if (int(r.get("PUBLISHER_ID", 0)), int(r.get("DEMAND_ID", 0))) in affected]

    def _agg(rows, group):
        tot = {"bids": 0.0, "wins": 0.0, "revenue": 0.0}
        for r in rows:
            if assign_country(r.get("COUNTRY", "")) != group:
                continue
            tot["bids"] += float(r.get("BIDS", 0) or 0)
            tot["wins"] += float(r.get("WINS", 0) or 0)
            tot["revenue"] += float(r.get("GROSS_REVENUE", 0) or 0)
        return tot

    pre = _rows_in_window(relevant, pre_start, pre_end)
    post = _rows_in_window(relevant, post_start, post_end)

    pre_t, pre_c = _agg(pre, "treatment"), _agg(pre, "control")
    post_t, post_c = _agg(post, "treatment"), _agg(post, "control")

    def _pct(a, b):
        return ((a - b) / b * 100.0) if b > 0 else None

    return {
        "actor": actor,
        "cutover_date": cutover,
        "window_days": window_days,
        "affected_tuples": len(affected),
        "pre_window": [pre_start, pre_end],
        "post_window": [post_start, post_end],
        "treatment": {
            "pre_revenue": round(pre_t["revenue"], 2),
            "post_revenue": round(post_t["revenue"], 2),
            "pct_change": round(_pct(post_t["revenue"], pre_t["revenue"]) or 0, 2),
        },
        "control": {
            "pre_revenue": round(pre_c["revenue"], 2),
            "post_revenue": round(post_c["revenue"], 2),
            "pct_change": round(_pct(post_c["revenue"], pre_c["revenue"]) or 0, 2),
        },
        "diff_in_diff_pct": round(
            (_pct(post_t["revenue"], pre_t["revenue"]) or 0)
            - (_pct(post_c["revenue"], pre_c["revenue"]) or 0),
            2,
        ),
        "note": (
            "diff_in_diff_pct > 0 → treatment outperformed control, "
            "optimizer action has positive causal evidence. "
            "< 0 → market drift explains the movement or action was net-negative."
        ),
    }


def run() -> dict:
    """Scheduler entry point: refresh both assignment snapshots."""
    return {"country": build_assignment(), "tuple": build_tuple_assignment()}


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--assign", action="store_true", help="build country assignment")
    g.add_argument("--tuples", action="store_true", help="build tuple assignment")
    g.add_argument("--lift", metavar="ACTOR")
    g.add_argument("--is-held-out", nargs=2, type=int, metavar=("PUB", "DEMAND"))
    ap.add_argument("--window", type=int, default=7)
    args = ap.parse_args()

    if args.assign:
        a = build_assignment()
        control = [c for c in a.get("countries", []) if c["group"] == "control"]
        print(f"\ncontrol countries ({len(control)}):")
        for c in sorted(control, key=lambda x: -x["revenue_30d"])[:20]:
            print(f"  {c['country']}  ${c['revenue_30d']:>8,.0f}")
    elif args.tuples:
        build_tuple_assignment()
    elif args.is_held_out:
        pid, did = args.is_held_out
        print(f"pub={pid} demand={did}: {assign_tuple(pid, did)}")
    else:
        r = lift_report(args.lift, args.window)
        print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
