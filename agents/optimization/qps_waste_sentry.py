#!/usr/bin/env python3
"""Cut QPS waste on a fixed cadence, because the platform cannot shape traffic.

The problem this exists for
---------------------------
Teqblaze does not shape traffic. We cannot send a demand source 30% of what it
gets today — the controls are binary or near-binary: a hard `qps_limit`, a
source `status` toggle, a routing change, a geo blacklist. So waste cannot be
gradually tuned away. It has to be *cut*, and cutting is only safe if the
decision follows a rule with a stated observation window rather than being made
in the moment.

The metric
----------
`GPM` — gross revenue per million bid requests. It is the honest efficiency
measure here because request volume is the scarce resource: every request costs
QPS capacity whether or not it ever returns a dollar.

    GPM = gross_revenue / (bid_requests / 1_000_000)

Measured over the 30 days to 2026-08-18 the marketplace ran ~405 billion bid
requests for $357.5k, a blended **$0.88 per million**. Individual setups ranged
from $13.70/M (Advetisi - Zmaticoo) to $0.005/M (BidFuse CTV AdPrime) — a
2,800× spread. The bottom band, 41.5 billion requests, returned $2,407: about
10% of all QPS for 0.67% of revenue.

The rule
--------
Three bands against the blended baseline, each requiring the condition to hold
for the whole observation window — not on any single day:

    CUT    GPM < 10% of blended  AND  gross < $100      → disable the setup
    CAP    GPM < 25% of blended  AND  gross >= $100     → hard qps_limit, re-measure
    WATCH  GPM < 50% of blended                         → report only

Safeguards, all deliberate:

* **Grace period.** A setup younger than `GRACE_DAYS` is never cut. New
  integrations ramp, wait on seat approval, and look like waste before they work.
* **Blast radius.** At most `MAX_ACTIONS_PER_RUN` actions, and never more than
  `MAX_QPS_SHARE_PER_RUN` of total request volume in a single run. A rule that
  can cut everything at once is a rule that will, on the day the data is wrong.
* **Never-cut list.** Contract commitments and the existing partner freeze
  (`core/partner_freeze.py`) win over this rule, always.
* **Quarterly re-test.** Anything cut is re-enabled for a 7-day re-test once a
  quarter unless explicitly marked dead. Otherwise one bad fortnight
  permanently removes a seasonal partner.
* **Ledger.** Every proposal and every action is recorded with its evidence, so
  a revenue move afterwards can be attributed rather than guessed at.

**There is no write path in this module.** Not a gated one, not a flag. It
measures, classifies, and prints what it would recommend; a person makes the
change on the platform. That is the posture until the promotion gate in
`docs/optimization-cadence.md` §3.5 is met — and since Teqblaze cannot shape
traffic, every action this rule proposes is irreversible-in-effect within the
cadence, which is exactly the kind of change that should not start automated.

Usage
-----
    python3 -m agents.optimization.qps_waste_sentry
    python3 -m agents.optimization.qps_waste_sentry --json proposals.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

ACTOR = "qps_waste_sentry"
_LOG = "[qps_waste_sentry]"

# ── Rule parameters ─────────────────────────────────────────────────────────
# Env-overridable so the cadence can be tuned without a code change, but the
# defaults are the ones argued for in docs/optimization-cadence.md.
OBSERVE_DAYS   = int(os.getenv("PGAM_QPS_OBSERVE_DAYS", "14"))
GRACE_DAYS     = int(os.getenv("PGAM_QPS_GRACE_DAYS", "21"))

CUT_BAND       = float(os.getenv("PGAM_QPS_CUT_BAND", "0.10"))   # of blended GPM
CAP_BAND       = float(os.getenv("PGAM_QPS_CAP_BAND", "0.25"))
WATCH_BAND     = float(os.getenv("PGAM_QPS_WATCH_BAND", "0.50"))

CUT_MAX_GROSS  = float(os.getenv("PGAM_QPS_CUT_MAX_GROSS", "100"))  # $ in window
MIN_REQUESTS   = int(float(os.getenv("PGAM_QPS_MIN_REQUESTS", "50e6")))

MAX_ACTIONS_PER_RUN  = int(os.getenv("PGAM_QPS_MAX_ACTIONS", "5"))
MAX_QPS_SHARE_PER_RUN = float(os.getenv("PGAM_QPS_MAX_SHARE", "0.15"))
CAP_TO_FRACTION      = float(os.getenv("PGAM_QPS_CAP_FRACTION", "0.25"))

# Setups this rule must never touch, whatever the numbers say. Populate from
# contract commitments. The partner freeze list is consulted separately.
NEVER_CUT: set[str] = set(
    n.strip() for n in os.getenv("PGAM_QPS_NEVER_CUT", "").split(",") if n.strip()
)


def _frozen_names() -> set[str]:
    """Names under the existing cross-platform partner freeze."""
    try:
        from core import partner_freeze
        for attr in ("frozen_names", "frozen_partners", "all_frozen"):
            fn = getattr(partner_freeze, attr, None)
            if callable(fn):
                return {str(x).lower() for x in (fn() or [])}
        names = getattr(partner_freeze, "FROZEN", None) or getattr(partner_freeze, "FREEZE_LIST", None)
        if names:
            return {str(x).lower() for x in names}
    except Exception as exc:
        print(f"{_LOG} WARN: could not read partner_freeze ({exc}); "
              f"treating freeze list as EMPTY. Verify before applying.",
              file=sys.stderr)
    return set()


# ── Measurement ─────────────────────────────────────────────────────────────

_SQL = """
SELECT {label} AS name,
       sum(bids)::bigint            AS requests,
       sum(impressions)::bigint     AS imps,
       sum(gross_revenue)::numeric  AS gross,
       count(DISTINCT report_date)  AS active_days,
       min(report_date)             AS first_seen
FROM pgam_direct.{table}
WHERE report_date BETWEEN %(df)s AND %(dt)s
GROUP BY {label}
HAVING sum(bids) > 0
"""

_FIRST_SEEN = """
SELECT {label} AS name, min(report_date)
FROM pgam_direct.{table}
GROUP BY {label}
"""


def measure(conn, table: str, label: str, df: str, dt: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_FIRST_SEEN.format(table=table, label=label))
        ever = {r[0]: r[1] for r in cur.fetchall()}
        cur.execute(_SQL.format(table=table, label=label), {"df": df, "dt": dt})
        rows = cur.fetchall()

    out = []
    for name, requests, imps, gross, active_days, _ in rows:
        requests = int(requests or 0)
        gross = float(gross or 0)
        if requests < MIN_REQUESTS:
            continue  # too small to matter either way
        out.append({
            "name": name,
            "requests": requests,
            "impressions": int(imps or 0),
            "gross": round(gross, 2),
            "gpm": round(gross / (requests / 1_000_000), 4) if requests else 0.0,
            "active_days": int(active_days or 0),
            "first_seen": str(ever.get(name) or ""),
        })
    return out


def classify(rows: list[dict], blended_gpm: float, today: date) -> list[dict]:
    """Attach a band and a reason to each row. Pure function — easy to test."""
    frozen = _frozen_names()
    cut_at, cap_at, watch_at = (blended_gpm * CUT_BAND,
                                blended_gpm * CAP_BAND,
                                blended_gpm * WATCH_BAND)
    for r in rows:
        gpm, name = r["gpm"], r["name"]
        r["band"] = "ok"
        r["reason"] = ""

        if gpm >= watch_at:
            continue

        # Grace: too young to judge.
        age = None
        if r["first_seen"]:
            try:
                age = (today - date.fromisoformat(r["first_seen"])).days
            except ValueError:
                age = None
        if age is not None and age < GRACE_DAYS:
            r["band"] = "grace"
            r["reason"] = f"only {age}d old, grace period is {GRACE_DAYS}d"
            continue

        # Protected: contract or freeze list wins over the numbers.
        if name in NEVER_CUT or str(name).lower() in frozen:
            r["band"] = "protected"
            r["reason"] = "on the never-cut or partner-freeze list"
            continue

        # Partial coverage in the window means we may be looking at a pause,
        # not at waste. Report, do not act.
        if r["active_days"] < OBSERVE_DAYS:
            r["band"] = "watch"
            r["reason"] = (f"only {r['active_days']}/{OBSERVE_DAYS} active days — "
                           f"could be a pause rather than waste")
            continue

        if gpm < cut_at and r["gross"] < CUT_MAX_GROSS:
            r["band"] = "cut"
            r["reason"] = (f"${gpm:.4f}/M is under {CUT_BAND:.0%} of blended "
                           f"${blended_gpm:.4f}/M, on ${r['gross']:.2f} gross")
        elif gpm < cap_at:
            r["band"] = "cap"
            r["reason"] = (f"${gpm:.4f}/M is under {CAP_BAND:.0%} of blended, "
                           f"but ${r['gross']:.2f} gross is worth keeping — cap it")
        else:
            r["band"] = "watch"
            r["reason"] = f"${gpm:.4f}/M is under {WATCH_BAND:.0%} of blended"
    return rows


def enforce_blast_radius(rows: list[dict], total_requests: int) -> list[dict]:
    """Trim the action list to the per-run caps, worst offender first."""
    actionable = [r for r in rows if r["band"] in ("cut", "cap")]
    actionable.sort(key=lambda r: (r["gpm"], -r["requests"]))

    kept, share = [], 0.0
    for r in actionable:
        if len(kept) >= MAX_ACTIONS_PER_RUN:
            r["band"] = "deferred"
            r["reason"] = f"{r['reason']} — deferred, {MAX_ACTIONS_PER_RUN}-action cap reached"
            continue
        r_share = r["requests"] / total_requests if total_requests else 0
        if share + r_share > MAX_QPS_SHARE_PER_RUN:
            r["band"] = "deferred"
            r["reason"] = (f"{r['reason']} — deferred, would exceed the "
                           f"{MAX_QPS_SHARE_PER_RUN:.0%} per-run QPS cap")
            continue
        share += r_share
        kept.append(r)
    return kept


# ── Reporting ───────────────────────────────────────────────────────────────

def report(dimension: str, rows: list[dict], blended: float,
           actions: list[dict], total_requests: int) -> None:
    print(f"\n{'═' * 78}\n{dimension}\n{'═' * 78}")
    print(f"  blended baseline ${blended:.4f} per million requests")
    print(f"  bands: cut <${blended * CUT_BAND:.4f}  cap <${blended * CAP_BAND:.4f}  "
          f"watch <${blended * WATCH_BAND:.4f}")

    by_band: dict[str, list[dict]] = {}
    for r in rows:
        by_band.setdefault(r["band"], []).append(r)

    for band in ("cut", "cap", "deferred", "watch", "grace", "protected"):
        group = by_band.get(band, [])
        if not group:
            continue
        group.sort(key=lambda r: r["gpm"])
        req = sum(r["requests"] for r in group)
        gross = sum(r["gross"] for r in group)
        pct = 100 * req / total_requests if total_requests else 0
        print(f"\n  ── {band.upper()} — {len(group)} setup(s), "
              f"{req / 1e9:,.2f}B requests ({pct:,.1f}% of QPS), ${gross:,.2f} gross")
        for r in group[:20]:
            print(f"     {r['name'][:44]:44}  ${r['gpm']:>9.4f}/M  "
                  f"{r['requests'] / 1e9:>7.2f}B  ${r['gross']:>10,.2f}")
            print(f"       └ {r['reason']}")
        if len(group) > 20:
            print(f"     … {len(group) - 20} more")

    if actions:
        freed = sum(r["requests"] for r in actions)
        risked = sum(r["gross"] for r in actions)
        print(f"\n  ► THIS RUN WOULD ACT ON {len(actions)} setup(s):")
        print(f"     frees  {freed / 1e9:,.2f}B requests "
              f"({100 * freed / total_requests if total_requests else 0:,.1f}% of QPS)")
        print(f"     risks  ${risked:,.2f} gross over the window "
              f"({'trivial' if risked < 500 else 'material — review individually'})")
    else:
        print("\n  ► no setup meets the action bands this run")


def record_proposals(actions: list[dict], dimension: str, blended: float,
                     window: tuple[str, str]) -> int:
    """Append each proposal to the TB ledger.

    Claimed in this module's docstring and in docs/optimization-cadence.md
    before it was implemented — that gap is why this exists as its own
    function with its own test rather than a line buried in main().

    Entries are written with `applied=False`, because nothing here executes:
    the record says "this was recommended, on this evidence, on this date".
    When someone cuts a source by hand afterwards, the ledger is what lets a
    later revenue move be attributed to the decision instead of guessed at.
    """
    if not actions:
        return 0
    try:
        from core import tb_ledger
    except Exception as exc:
        print(f"{_LOG} WARN: ledger unavailable ({exc}) — proposals NOT recorded. "
              f"A later revenue move will not be attributable.", file=sys.stderr)
        return 0

    run_id = f"{ACTOR}:{date.today().isoformat()}"
    written = 0
    for r in actions:
        try:
            tb_ledger.record(
                actor=ACTOR,
                action=f"propose_{r['band']}",
                entity_type="demand_source" if "DEMAND" in dimension else "supply_setup",
                entity_id=r["name"],
                reason=r["reason"],
                before={"requests": r["requests"], "gross": r["gross"],
                        "gpm": r["gpm"], "impressions": r["impressions"]},
                after={},                     # nothing changed
                applied=False,                # explicitly: this is a proposal
                dry_run=False,                # not a dry run either — a recommendation
                run_id=run_id,
                extra={"blended_gpm": round(blended, 4),
                       "window": {"from": window[0], "to": window[1]},
                       "observe_days": OBSERVE_DAYS,
                       "note": "proposal only; this module cannot write to any platform"},
            )
            written += 1
        except Exception as exc:
            print(f"{_LOG} WARN: could not record {r['name']}: {exc}", file=sys.stderr)
    return written


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=OBSERVE_DAYS)
    ap.add_argument("--json", help="write the proposal set to this path")
    a = ap.parse_args()

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("No Neon DSN set.", file=sys.stderr)
        return 2

    end = date.today() - timedelta(days=1)
    df = (end - timedelta(days=a.days - 1)).isoformat()
    dt = end.isoformat()

    from core.neon import connect

    print(f"{_LOG} QPS waste sweep")
    print(f"  window        {df} → {dt}  ({a.days}d observation)")
    print(f"  metric        gross $ per million bid requests (GPM)")
    print(f"  mode          PROPOSE ONLY — this module cannot write")
    print(f"  why cut, not throttle: Teqblaze does not shape traffic, so the")
    print(f"  only controls are binary. See the module docstring.")

    payload = {"window": {"from": df, "to": dt}, "dimensions": {}}
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")

        for dim, table, label in (
            ("DEMAND SOURCES", "tb_daily_demand_revenue", "demand_name"),
            ("SUPPLY SETUPS",  "tb_daily_publisher_revenue", "publisher_name"),
        ):
            rows = measure(conn, table, label, df, dt)
            if not rows:
                print(f"\n{dim}: no rows above the {MIN_REQUESTS:,}-request floor")
                continue
            total_req = sum(r["requests"] for r in rows)
            total_gross = sum(r["gross"] for r in rows)
            blended = total_gross / (total_req / 1_000_000) if total_req else 0.0

            rows = classify(rows, blended, end)
            actions = enforce_blast_radius(rows, total_req)
            report(dim, rows, blended, actions, total_req)

            n = record_proposals(actions, dim, blended, (df, dt))
            if n:
                print(f"\n  {n} proposal(s) recorded to the ledger "
                      f"(applied=False — nothing was changed)")

            payload["dimensions"][dim] = {
                "blended_gpm": round(blended, 4),
                "total_requests": total_req,
                "total_gross": round(total_gross, 2),
                "rows": rows,
                "actions": [r["name"] for r in actions],
            }
    finally:
        conn.close()

    if a.json:
        with open(a.json, "w") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"\n{_LOG} proposals written to {a.json}")

    print(f"\n{_LOG} Proposals above are the deliverable. Nothing was changed.")
    print(f"{_LOG} Apply them by hand on the platform after review; the promotion")
    print(f"{_LOG} gate for automating this is docs/optimization-cadence.md §3.5.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
