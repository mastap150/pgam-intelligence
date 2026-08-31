#!/usr/bin/env python3
"""
Disable the supply and demand sources `tbx_trim.py` flags as waste.

The companion write path to the read-only trim report. Everything here is
gated three ways: dry run by default, `--apply` to mean it, and
`TBX_ALLOW_WRITES=1` at the environment level (enforced inside
`core.tbx_mgmt`, not here — this script cannot bypass it).

Why it re-measures instead of taking a list
-------------------------------------------
The obvious design is "hand it the ids from the report". This does not do
that, and the reason is the failure mode it prevents: a cut list is a
snapshot of a seven-day window, and a partner that resumed yesterday looks
identical in that snapshot to one that is still dark. Applying a
three-day-old list would switch off traffic that had already come back, with
no record that the decision was made against stale evidence.

So `--apply` re-runs the same assessment against live data first and acts
only on what is *still* flagged, printing anything that has dropped off the
list since. The ids you pass are a filter on that fresh assessment, never a
substitute for it.

Safety rails
------------
* `--max-revenue-day` (default $1.00) — an absolute refusal, not a warning.
  Nothing earning above it is touched whatever bucket it is in, so a
  mistyped `--include` cannot take out a real earner.
* `--buckets` is required. There is no "cut everything flagged" default,
  because DEAD and HUNGRY are completely different decisions: DEAD earned
  nothing for seven days, HUNGRY is a live partner with a bad ratio.
* `--exclude` for entities that need a conversation before a switch. The
  Illumin RON cluster and Dexerto Display are both in this category — see
  `EXCLUDE_BY_DEFAULT`.
* Demand writes honour `core.partner_freeze` (inside `set_demand_source_status`).

Reverting
---------
Every applied run writes a ledger. `--revert LEDGER` re-enables exactly what
that run disabled, and nothing else. This is the only supported undo: there
is no bulk "turn everything back on", deliberately.

Usage
-----
    # see what would happen — no credentials needed beyond read access
    python3 scripts/tbx_cut.py --buckets DEAD

    # the eight sources that produced zero impressions in seven days
    python3 scripts/tbx_cut.py --buckets DEAD --apply

    # sub-dollar demand partners, excluding the Illumin cluster
    python3 scripts/tbx_cut.py --side demand --buckets NO-WIN --apply

    # undo
    python3 scripts/tbx_cut.py --revert cut-ledger-20260831T130000Z.json

Exit codes:
    0  nothing to do, or the run succeeded
    1  at least one write was refused or failed
    2  credentials absent, or the request was rejected as unsafe
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

# Entities that must not be switched off by a batch job, with the reason.
# These are not "risky" in the revenue sense — they are decisions that belong
# to a person, and a script that quietly included them would be making that
# decision on their behalf.
EXCLUDE_BY_DEFAULT: dict[int, str] = {
    # Ten demand endpoints named RON / copy1 / copy2 with identical zero-win
    # behaviour read as one duplicated configuration, not ten dead partners.
    # If they are meant to be bidding, the fault is upstream of this decision.
    1549: "Illumin RON cluster — confirm with Illumin first",
    2179: "Illumin RON cluster — confirm with Illumin first",
    2178: "Illumin RON cluster — confirm with Illumin first",
    1553: "Illumin RON cluster — confirm with Illumin first",
    2311: "Illumin RON cluster — confirm with Illumin first",
    1826: "Illumin RON cluster — confirm with Illumin first",
    1830: "Illumin RON cluster — confirm with Illumin first",
    959:  "Illumin RON cluster — confirm with Illumin first",
    831:  "Illumin RON cluster — confirm with Illumin first",
    # A named publisher relationship still earning, however badly.
    6: "Dexerto Display — named publisher, still earning; needs a decision",
}

SUPPLY_BUCKETS = ("DEAD", "NEAR-DEAD", "LOSS", "HUNGRY")
DEMAND_BUCKETS = ("NO-WIN", "TIMEOUT")


def ledger_path() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"cut-ledger-{stamp}.json"


def assess(args) -> tuple[list[dict], list[dict], int, int]:
    """Fresh supply and demand assessments over the window."""
    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Re-measuring {start} → {end} ({args.days} settled days)\n")

    supply, demand = [], []
    supply_days = demand_days = 0
    if args.side in ("supply", "both"):
        rows, supply_days = trim.pull_supply(start, args.days)
        supply, _ = trim.assess_supply(rows, max(supply_days, 1), args)
    if args.side in ("demand", "both"):
        rows, demand_days = trim.pull_demand(start, args.days, trim.DEMAND_METRICS)
        demand = trim.assess_demand(rows, max(demand_days, 1), args)
    return supply, demand, supply_days, demand_days


def select(assessed: list[dict], kind: str, args) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Split the assessment into (targets, skipped-with-reason)."""
    revenue_key = "gross_day" if kind == "supply_source" else "spend_day"
    wanted = set(args.buckets)
    targets, skipped = [], []

    for entry in assessed:
        if entry.get("bucket") not in wanted:
            continue
        eid = entry.get("id")
        if eid is None:
            skipped.append((entry, "no id could be parsed from the name"))
            continue
        if args.include and eid not in args.include:
            continue
        if eid in args.exclude:
            skipped.append((entry, args.exclude[eid]))
            continue
        earning = entry.get(revenue_key, 0.0)
        if earning > args.max_revenue_day:
            skipped.append((entry,
                            f"earns {trim.money(earning)}/day, above the "
                            f"{trim.money(args.max_revenue_day)} rail"))
            continue
        entry["_revenue_day"] = earning
        entry["_kind"] = kind
        targets.append(entry)

    targets.sort(key=lambda e: -e["requests_day"])
    return targets, skipped


def render(targets: list[dict], skipped: list[tuple[dict, str]], kind: str) -> None:
    label = "supply" if kind == "supply_source" else "demand"
    print(f"\n── {label} ──────────────────────────────────────────")
    if not targets:
        print("  nothing selected")
    for entry in targets:
        print(f"  {entry['bucket']:<10} {entry['requests_day']:>15,.0f} req/day  "
              f"{trim.money(entry['_revenue_day']):>8}/day  "
              f"{entry['name']} #{entry['id']}")
    if targets:
        req = sum(e["requests_day"] for e in targets)
        rev = sum(e["_revenue_day"] for e in targets)
        print(f"\n  {len(targets)} to disable: {req:,.0f} requests/day, "
              f"giving up {trim.money(rev)}/day")
    if skipped:
        print(f"\n  {len(skipped)} skipped:")
        for entry, why in skipped:
            name = entry.get("name", "?")
            eid = entry.get("id")
            print(f"    · {name}{f' #{eid}' if eid else ''} — {why}")


def apply_cuts(targets: list[dict], args) -> tuple[list[dict], int]:
    """Disable each target. Returns (ledger entries, failure count)."""
    entries, failures = [], 0
    for entry in targets:
        kind, eid = entry["_kind"], entry["id"]
        reason = (f"tbx_cut {entry['bucket']}: {entry['requests_day']:,.0f} req/day "
                  f"for {trim.money(entry['_revenue_day'])}/day over {args.days}d")
        try:
            if kind == "supply_source":
                result = tbm.set_supply_source_status(
                    eid, False, actor=args.actor, reason=reason,
                    dry_run=not args.apply)
            else:
                result = tbm.set_demand_source_status(
                    eid, False, actor=args.actor, reason=reason,
                    dry_run=not args.apply, demand_name=entry["name"])
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {kind} {eid} ({entry['name']}): {exc}", file=sys.stderr)
            failures += 1
            continue

        if args.apply and not result.get("applied"):
            why = result.get("refused", "unknown")
            print(f"  ✗ {kind} {eid} ({entry['name']}) refused: {why}",
                  file=sys.stderr)
            failures += 1
            continue

        entries.append({
            "kind": kind, "id": eid, "name": entry["name"],
            "bucket": entry["bucket"],
            "requests_day": entry["requests_day"],
            "revenue_day": entry["_revenue_day"],
            "applied": bool(result.get("applied")),
        })
    return entries, failures


def revert(path: str, args) -> int:
    """Re-enable exactly what one ledger recorded disabling."""
    with open(path) as handle:
        ledger = json.load(handle)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0

    print(f"Re-enabling {len(entries)} entities from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for entry in entries:
        reason = f"revert of {os.path.basename(path)}"
        try:
            if entry["kind"] == "supply_source":
                result = tbm.set_supply_source_status(
                    entry["id"], True, actor=args.actor, reason=reason,
                    dry_run=not args.apply)
            else:
                result = tbm.set_demand_source_status(
                    entry["id"], True, actor=args.actor, reason=reason,
                    dry_run=not args.apply, demand_name=entry.get("name"))
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {entry['kind']} {entry['id']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ {entry['kind']} {entry['id']} refused: "
                  f"{result.get('refused', 'unknown')}", file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ {entry['kind']} {entry['id']}  {entry['name']}")
    return 1 if failures else 0


def parse_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    return {int(part) for part in raw.replace(",", " ").split()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Disable the sources tbx_trim flags as waste.")
    parser.add_argument("--side", choices=("supply", "demand", "both"),
                        default="both")
    parser.add_argument("--buckets", default="",
                        help="comma-separated, REQUIRED: "
                             f"{'/'.join(SUPPLY_BUCKETS + DEMAND_BUCKETS)}. "
                             "There is no cut-everything default — DEAD and "
                             "HUNGRY are different decisions.")
    parser.add_argument("--days", type=int, default=7,
                        help="settled days to re-measure over (default 7)")
    parser.add_argument("--max-revenue-day", type=float, default=1.0,
                        help="absolute refusal above this $/day (default 1.0)")
    parser.add_argument("--min-requests-day", type=float, default=10_000)
    parser.add_argument("--min-revenue-day", type=float, default=1.0,
                        help="NEAR-DEAD threshold passed to the assessor")
    parser.add_argument("--hungry-multiple", type=float, default=5.0)
    parser.add_argument("--timeout-pct", type=float, default=20.0)
    parser.add_argument("--include", default="",
                        help="only these ids, filtered against the fresh "
                             "assessment — never a substitute for it")
    parser.add_argument("--also-exclude", default="",
                        help="ids to skip on top of the built-in list")
    parser.add_argument("--clear-default-excludes", action="store_true",
                        help="drop the built-in exclusions. Each one names a "
                             "conversation that should happen first; use this "
                             "only once those have.")
    parser.add_argument("--apply", action="store_true",
                        help="actually write. Also needs TBX_ALLOW_WRITES=1, "
                             "which core.tbx_mgmt enforces independently.")
    parser.add_argument("--revert", metavar="LEDGER",
                        help="re-enable what one ledger file disabled")
    parser.add_argument("--actor", default="tbx_cut")
    parser.add_argument("--ledger", default=None,
                        help="ledger path (default cut-ledger-<stamp>.json)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to do.",
              file=sys.stderr)
        return 2

    args.exclude = dict(EXCLUDE_BY_DEFAULT)
    if args.clear_default_excludes:
        args.exclude = {}
    for eid in parse_ids(args.also_exclude):
        args.exclude[eid] = "excluded on the command line"
    args.include = parse_ids(args.include)

    if args.revert:
        return revert(args.revert, args)

    args.buckets = [b.strip().upper() for b in args.buckets.split(",") if b.strip()]
    if not args.buckets:
        print("--buckets is required. Pick from "
              f"{', '.join(SUPPLY_BUCKETS + DEMAND_BUCKETS)}.\n"
              "There is deliberately no default: DEAD earned nothing for the "
              "whole window, HUNGRY is a live partner with a bad ratio, and "
              "those are not the same decision.", file=sys.stderr)
        return 2
    unknown = [b for b in args.buckets if b not in SUPPLY_BUCKETS + DEMAND_BUCKETS]
    if unknown:
        print(f"unknown bucket(s) {unknown}", file=sys.stderr)
        return 2

    if args.apply and not tbm.writes_enabled():
        print("--apply was passed but TBX_ALLOW_WRITES is not 1.\n"
              "Nothing will be written. Set it in the Render or Actions "
              "environment; this script cannot and must not bypass it.",
              file=sys.stderr)
        return 2

    supply, demand, supply_days, demand_days = assess(args)
    if args.side in ("supply", "both") and supply_days == 0:
        print("no supply data came back — refusing to act on an empty read.",
              file=sys.stderr)
        return 2
    if args.side in ("demand", "both") and demand_days == 0:
        print("no demand data came back — refusing to act on an empty read.",
              file=sys.stderr)
        return 2

    targets: list[dict] = []
    for assessed, kind in ((supply, "supply_source"), (demand, "demand_source")):
        if not assessed:
            continue
        picked, skipped = select(assessed, kind, args)
        render(picked, skipped, kind)
        targets.extend(picked)

    if not targets:
        print("\nNothing to disable.")
        return 0

    print(f"\n{'APPLYING' if args.apply else 'DRY RUN — nothing will be written'}"
          f" · {len(targets)} entities\n")
    entries, failures = apply_cuts(targets, args)

    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as handle:
            json.dump({
                "created": datetime.now(timezone.utc).isoformat(),
                "actor": args.actor,
                "buckets": args.buckets,
                "days": args.days,
                "entries": entries,
            }, handle, indent=2)
        print(f"\nLedger: {path}")
        print(f"Undo with:  python3 scripts/tbx_cut.py --revert {path} --apply")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
