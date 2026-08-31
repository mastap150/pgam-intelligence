#!/usr/bin/env python3
"""
Blacklist the DSP × country pairs that take outbound QPS and never buy.

The write path for `scripts/tbx_geo_waste.py`, in the same shape as
`scripts/tbx_cut.py` is the write path for `tbx_trim.py`: the report proposes,
this decides, and it **re-measures rather than replaying the report's list**.
That is not ceremony — on the supply cut of 2026-08-31 the re-measure caught
three sources that had gone dark between the report and the run. A pair that
started trading yesterday must not be blacklisted because a report from last
week said it never did.

Why this lever rather than disabling something
----------------------------------------------
A blacklist is per-DSP. Removing Brazil from a buyer that has never bought a
Brazilian impression cannot reduce that buyer's revenue and cannot touch any
other buyer: the same supply keeps flowing to everyone else, and the DSP keeps
every country it does buy. That is a far smaller blast radius than switching
off a source, which is why the bar here can be lower than `tbx_cut`'s.

What it refuses to do
---------------------
1. **A pair with any impressions at all is never blacklisted** by default
   (`--max-imps-day 0`). "Zero spend" is not the same claim as "never trades":
   a pair with 70 impressions and $0.40/day is trading, badly, and the lever
   for that is a floor (`tbx_demand_geo_floor`), not a block.
2. **`--max-spend-day` is an absolute rail** and beats an explicit `--include`,
   exactly as `tbx_cut`'s revenue rail does.
3. **A DSP that buys nothing anywhere is skipped**, not blacklisted country by
   country. A buyer with zero spend across the whole window is a dead
   integration, and the honest fix is `tbx_cut.py --side demand --buckets
   NO-WIN`. Blacklisting its geos one at a time would bury a source-level
   problem under a hundred country-level edits.
4. **`--min-requests-day` sets a floor on volume**, so the run is about cost
   that is actually being paid, not a tail of pairs sending nine requests.

Merge, not replace
------------------
`geo_settings.blacklist` replaces wholesale on the wire, and a DSP's existing
blacklist is a standing trading rule somebody set by hand. `tbx_mgmt.
set_demand_geo_blacklist` therefore merges by default, and this script relies
on that: it only ever adds. The ledger records `blacklist_before` per DSP, so
a revert restores the exact prior list rather than "everything except what we
added".

Usage
-----
    # what would happen — read-only, no gate needed
    python3 scripts/tbx_geo_cut.py

    # apply it
    python3 scripts/tbx_geo_cut.py --apply

    # one buyer only
    python3 scripts/tbx_geo_cut.py --include 501 --apply

    # undo
    python3 scripts/tbx_geo_cut.py --revert geo-ledger-20260831T190000Z.json --apply

Writes need `--apply` **and** `TBX_ALLOW_WRITES=1`; `core.tbx_mgmt` enforces
the second one independently of anything here.

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

from core import tbx_api as tbx              # noqa: E402
from core import tbx_mgmt as tbm             # noqa: E402
from scripts import tbx_trim as trim         # noqa: E402
from scripts import tbx_geo_waste as geo     # noqa: E402

_HDR = "=" * 78

# Buyers a batch job must not quietly re-scope. Same intent as tbx_cut's
# EXCLUDE_BY_DEFAULT: not "risky", but decisions that belong to a person.
EXCLUDE_BY_DEFAULT: dict[int, str] = {}

# A country label the report could not attribute. Blacklisting it is not even
# well-defined, and it is where a reporting gap would show up first.
UNATTRIBUTED = {"(none)", "", "unknown", "n/a"}


def ledger_path() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"geo-ledger-{stamp}.json"


def measure(args) -> tuple[list[dict], int]:
    """Fresh demand_source × country assessment over the window."""
    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Re-measuring {start} → {end} ({args.days} settled days)\n")
    rows, days_with_data = geo.pull_pairs(start, args.days)
    if days_with_data == 0:
        return [], 0
    return geo.summarise(rows, "demand_source", days_with_data), days_with_data


def select(pairs: list[dict], args) -> tuple[dict[int, dict], list[tuple[dict, str]]]:
    """Split the assessment into per-DSP targets and skipped-with-reason.

    Returns `{demand_id: {"name":…, "countries":[…], "pairs":[…]}}`.
    """
    # Rail 3: a buyer with no spend anywhere is a source-level problem.
    spend_by_dsp: dict[int, float] = {}
    for pair in pairs:
        if pair["id"] is not None:
            spend_by_dsp[pair["id"]] = spend_by_dsp.get(pair["id"], 0.0) + pair["spend_day"]

    targets: dict[int, dict] = {}
    skipped: list[tuple[dict, str]] = []

    for pair in pairs:
        did, country = pair["id"], pair["country"]

        if did is None:
            skipped.append((pair, "no demand id could be parsed from the name"))
            continue
        if not country or str(country).strip().lower() in UNATTRIBUTED:
            skipped.append((pair, "country is unattributed — not a blacklistable value"))
            continue
        if pair["requests_day"] < args.min_requests_day:
            continue                      # below the volume bar; no opinion
        if args.include and did not in args.include:
            continue
        if did in args.exclude:
            skipped.append((pair, args.exclude[did]))
            continue

        if pair["spend_day"] > args.max_spend_day:
            skipped.append((pair, f"spends {trim.money(pair['spend_day'])}/day, "
                                  f"above the {trim.money(args.max_spend_day)} rail"))
            continue
        if pair["imps_day"] > args.max_imps_day:
            skipped.append((pair, f"{pair['imps_day']:,.0f} imps/day — it trades, "
                                  f"so the lever is a floor, not a block"))
            continue
        if spend_by_dsp.get(did, 0.0) <= args.dead_dsp_spend_day:
            skipped.append((pair, "this buyer spends nothing in ANY country — "
                                  "use tbx_cut --side demand --buckets NO-WIN"))
            continue

        bucket = targets.setdefault(did, {
            "id": did,
            # summarise() labels a pair "Name #NNNN — CC". Strip the country,
            # then the id, so the display does not print the id twice.
            "name": trim.split_name_id(pair["name"].rsplit(" — ", 1)[0])[0],
            "countries": [],
            "pairs": [],
        })
        bucket["countries"].append(country)
        bucket["pairs"].append(pair)

    for bucket in targets.values():
        bucket["countries"] = sorted(set(bucket["countries"]))
        bucket["pairs"].sort(key=lambda p: -p["requests_day"])
        bucket["requests_day"] = sum(p["requests_day"] for p in bucket["pairs"])
        bucket["spend_day"] = sum(p["spend_day"] for p in bucket["pairs"])
    return targets, skipped


def render(targets: dict[int, dict], skipped: list[tuple[dict, str]]) -> None:
    print(f"\n{_HDR}\nPairs to blacklist\n{_HDR}")
    if not targets:
        print("  nothing selected")
    for bucket in sorted(targets.values(), key=lambda b: -b["requests_day"]):
        print(f"\n  {bucket['name']} #{bucket['id']} — "
              f"{bucket['requests_day']:,.0f} req/day across "
              f"{len(bucket['countries'])} country(ies), "
              f"{trim.money(bucket['spend_day'])}/day at stake")
        for pair in bucket["pairs"][:12]:
            print(f"    · {pair['country']:<6} {pair['requests_day']:>13,.0f} req/day  "
                  f"{pair['imps_day']:>8,.0f} imps/day  {trim.money(pair['spend_day'])}/day")
        if len(bucket["pairs"]) > 12:
            print(f"    … and {len(bucket['pairs']) - 12} more")

    if targets:
        req = sum(b["requests_day"] for b in targets.values())
        spend = sum(b["spend_day"] for b in targets.values())
        pairs = sum(len(b["pairs"]) for b in targets.values())
        print(f"\n  {len(targets)} buyer(s), {pairs} pair(s): {req:,.0f} requests/day "
              f"removed, giving up {trim.money(spend)}/day.")

    if skipped:
        print(f"\n  {len(skipped)} pair(s) skipped:")
        # Collapse by reason — a hundred identical lines is not a report.
        by_reason: dict[str, list[dict]] = {}
        for pair, why in skipped:
            by_reason.setdefault(why, []).append(pair)
        for why, items in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
            print(f"    · {len(items)}× {why}")
            for pair in sorted(items, key=lambda p: -p["requests_day"])[:5]:
                print(f"        {pair['name']} ({pair['requests_day']:,.0f} req/day)")


def apply_blacklists(targets: dict[int, dict], args) -> tuple[list[dict], int]:
    """One `set_demand_geo_blacklist` per buyer. Returns (ledger, failures)."""
    entries, failures = [], 0
    for bucket in sorted(targets.values(), key=lambda b: -b["requests_day"]):
        did, countries = bucket["id"], bucket["countries"]

        country_ids = tbx.country_ids(countries)
        if len(country_ids) != len(countries):
            # country_ids() warns on what it could not resolve. Writing a
            # partial list would silently block a different set than the one
            # reported, so refuse the buyer rather than guess.
            print(f"  ✗ {bucket['name']} #{did}: only {len(country_ids)}/"
                  f"{len(countries)} countries resolved to platform ids — "
                  f"skipping this buyer entirely", file=sys.stderr)
            failures += 1
            continue

        reason = (f"tbx_geo_cut: {len(countries)} countries with "
                  f"{bucket['requests_day']:,.0f} req/day and "
                  f"{trim.money(bucket['spend_day'])}/day over {args.days}d")
        try:
            result = tbm.set_demand_geo_blacklist(
                did, country_ids, actor=args.actor, reason=reason,
                dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {did} ({bucket['name']}): {exc}", file=sys.stderr)
            failures += 1
            continue

        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {did} ({bucket['name']}) refused: "
                  f"{result.get('refused', 'unknown')}", file=sys.stderr)
            failures += 1
            continue

        entries.append({
            "kind": "demand_geo_blacklist",
            "id": did,
            "name": bucket["name"],
            "countries": countries,
            "country_ids": country_ids,
            "requests_day": bucket["requests_day"],
            "spend_day": bucket["spend_day"],
            # The prior list is the only thing that makes a revert exact.
            "blacklist_before": result.get("blacklist_before", []),
            "blacklist_after": result.get("blacklist_after", []),
            "added": result.get("added", []),
            "applied": bool(result.get("applied")),
        })
        verb = "blacklisted" if args.apply else "would blacklist"
        print(f"  ✓ {verb} {len(result.get('added', country_ids))} "
              f"country(ies) on {bucket['name']} #{did}")
    return entries, failures


def revert(path: str, args) -> int:
    """Restore each buyer's blacklist to exactly what it was before."""
    with open(path) as handle:
        ledger = json.load(handle)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0

    print(f"Restoring {len(entries)} blacklist(s) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for entry in entries:
        before = entry.get("blacklist_before", [])
        try:
            # replace=True, because restoring means dropping what we added.
            result = tbm.set_demand_geo_blacklist(
                entry["id"], before, replace=True, actor=args.actor,
                reason=f"revert of {os.path.basename(path)}",
                dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {entry['id']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {entry['id']} refused: "
                  f"{result.get('refused', 'unknown')}", file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ demand {entry['id']}  {entry.get('name', '?')} "
                  f"→ {len(before)} country(ies)")
    return 1 if failures else 0


def parse_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    return {int(part) for part in raw.replace(",", " ").split()}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Blacklist the DSP × country pairs that never trade.")
    parser.add_argument("--days", type=int, default=7,
                        help="settled days to re-measure over (default 7)")
    parser.add_argument("--min-requests-day", type=float, default=100_000,
                        help="only pairs above this outbound volume "
                             "(default 1e5 — the pair grain is much finer "
                             "than the country grain)")
    parser.add_argument("--max-spend-day", type=float, default=0.10,
                        help="absolute refusal above this $/day (default 0.10)")
    parser.add_argument("--max-imps-day", type=float, default=0.0,
                        help="absolute refusal above this imps/day. The "
                             "default of 0 means a pair that trades at all is "
                             "never blacklisted — that case wants a floor.")
    parser.add_argument("--dead-dsp-spend-day", type=float, default=0.0,
                        help="skip buyers whose TOTAL spend across all "
                             "countries is at or below this; they are a "
                             "source-level decision, not a geo one")
    parser.add_argument("--include", default="",
                        help="only these demand ids, filtered against the "
                             "fresh measurement — never a substitute for it")
    parser.add_argument("--also-exclude", default="",
                        help="demand ids to skip on top of the built-in list")
    parser.add_argument("--apply", action="store_true",
                        help="actually write. Also needs TBX_ALLOW_WRITES=1, "
                             "which core.tbx_mgmt enforces independently.")
    parser.add_argument("--revert", metavar="LEDGER",
                        help="restore the blacklists one ledger file changed")
    parser.add_argument("--actor", default="tbx_geo_cut")
    parser.add_argument("--ledger", default=None,
                        help="ledger path (default geo-ledger-<stamp>.json)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.exclude = dict(EXCLUDE_BY_DEFAULT)
    args.exclude.update({eid: "excluded on the command line"
                         for eid in parse_ids(args.also_exclude)})
    args.include = parse_ids(args.include)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to do.",
              file=sys.stderr)
        return 2

    if args.revert:
        return revert(args.revert, args)

    if args.max_imps_day > 0 or args.max_spend_day > 1.0:
        print(f"::warning::rails relaxed — max {args.max_imps_day:,.0f} imps/day, "
              f"{trim.money(args.max_spend_day)}/day. Pairs that trade can now "
              f"be blocked.")

    pairs, days_with_data = measure(args)
    if days_with_data == 0:
        print("no data came back — refusing to act on an empty read.",
              file=sys.stderr)
        return 2
    print(f"  {len(pairs)} pair(s) over {days_with_data}/{args.days} days")

    targets, skipped = select(pairs, args)
    render(targets, skipped)

    if not targets:
        print("\nNothing to blacklist.")
        return 0

    if not args.apply:
        print(f"\n{_HDR}\nDRY RUN — nothing was written. Re-run with --apply "
              f"(and TBX_ALLOW_WRITES=1).\n{_HDR}")

    print()
    entries, failures = apply_blacklists(targets, args)

    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as handle:
            json.dump({
                "created": datetime.now(timezone.utc).isoformat(),
                "actor": args.actor,
                "days": args.days,
                "entries": entries,
            }, handle, indent=2)
        print(f"\nLedger: {path}")
        print(f"Undo with: python3 scripts/tbx_geo_cut.py --revert {path} --apply")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
