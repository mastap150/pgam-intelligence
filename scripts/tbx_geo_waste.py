#!/usr/bin/env python3
"""
Which countries cost outbound QPS and never buy?

The gap `tbx_demand_geo_floor` leaves. That agent tunes the *price* on
demand_source × country pairs that already trade — it asks "is this floor
right". This asks the prior question nobody has: **which pairs should not be
in the auction at all**.

They are different levers with different fixes:

    geo floor   pair trades, the price is wrong        -> raise the floor
    geo waste   pair never trades, the request is cost -> blacklist the country

A pair sending millions of bid requests a day to a DSP that has never bought
a single impression in that country is pure outbound cost. `set_demand_geo_
blacklist` removes it in one call and nothing else changes: the DSP keeps
every country it does buy, and the supply keeps flowing to every other DSP.

Two views, because they answer different questions
--------------------------------------------------
`--view country` sizes the prize. One row per country across the whole book:
requests, impressions, spend. This is where the long tail shows up — the
classic pattern is a third of request volume in geos that produce ~2% of
revenue.

`--view pairs` is the actionable one. demand_source × country, listing pairs
that take real request volume and return nothing. That grain is much larger,
so it is capped by `--min-requests-day` and `--top`.

Why blacklisting is the safe direction here
-------------------------------------------
It is per-DSP, not global. Removing Brazil from one buyer that never bought
Brazil cannot reduce revenue from that buyer, and cannot touch any other
buyer. That is a materially smaller blast radius than a supply-side change,
and the same reasoning `tbx_demand_geo_floor` records for preferring the
demand side.

What it does NOT do
-------------------
It never writes. The blacklist call is named in the output so a human runs
it deliberately, the same propose-first shape as the geo floor agent.

Read-only.

Exit codes:
    0  nothing wasteful found
    1  at least one wasteful country or pair
    2  credentials absent
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

_HDR = "=" * 78

# requests_sum is the outbound counter — what we send to DSPs — which is
# exactly the cost this report is about. imps_sum and dsp_price_sum say
# whether any of it came back.
GEO_METRICS = ["requests_sum", "imps_sum", "dsp_price_sum"]


def summarise(rows: list[dict], key: str, days: int) -> list[dict]:
    out = []
    for row in rows:
        # Pair rows arrive pre-split with their own name/id, because the
        # composite label ("Some DSP #501 — BR") puts the id mid-string where
        # split_name_id cannot see it — it only matches a TRAILING #NNNN.
        # Losing the id there would leave the report telling a reader to call
        # set_demand_geo_blacklist without giving them the argument.
        if "demand_id" in row:
            name, eid = row["label"], row["demand_id"]
        else:
            raw = row.get(key) or ""
            name, eid = (raw, None)
        requests = trim.num(row, "requests_sum")
        imps = trim.num(row, "imps_sum")
        spend = trim.num(row, "dsp_price_sum")
        out.append({
            "name": name, "id": eid,
            "country": row.get("country") or (name if key == "country" else None),
            "requests_day": requests / days,
            "imps_day": imps / days,
            "spend_day": spend / days,
            "requests": requests, "imps": imps, "spend": spend,
            "per_dollar": trim.per_dollar(requests, spend),
        })
    out.sort(key=lambda r: -r["requests_day"])
    return out


def render_country(rows: list[dict], args) -> list[dict]:
    total_req = sum(r["requests_day"] for r in rows)
    total_spend = sum(r["spend_day"] for r in rows)

    print(f"\n{_HDR}\nBy country — where the request volume goes\n{_HDR}")
    print(f"  book: {total_req:,.0f} requests/day, {trim.money(total_spend)}/day spend\n")
    print(f"  {'requests/day':>15} {'% req':>7} {'$/day':>10} {'% $':>6}  country")
    print(f"  {'-'*15:>15} {'-'*7:>7} {'-'*10:>10} {'-'*6:>6}  {'-'*24}")
    for r in rows[:args.top]:
        pct_req = (r["requests_day"] / total_req * 100) if total_req else 0
        pct_spend = (r["spend_day"] / total_spend * 100) if total_spend else 0
        print(f"  {r['requests_day']:>15,.0f} {pct_req:>6.1f}% "
              f"{trim.money(r['spend_day']):>10} {pct_spend:>5.1f}%  {r['name'] or '(none)'}")

    # The prize: countries carrying real volume for essentially no money.
    waste = [r for r in rows
             if r["requests_day"] >= args.min_requests_day
             and r["spend_day"] < args.max_spend_day]
    if waste:
        wr = sum(r["requests_day"] for r in waste)
        ws = sum(r["spend_day"] for r in waste)
        print(f"\n  {len(waste)} country(ies) above {args.min_requests_day:,.0f} "
              f"requests/day earning under {trim.money(args.max_spend_day)}/day:")
        for r in waste[:args.top]:
            print(f"    · {r['name'] or '(none)'}: {r['requests_day']:,.0f} req/day, "
                  f"{trim.money(r['spend_day'])}/day, {r['imps_day']:,.0f} imps/day")
        print(f"\n  Together {wr:,.0f} requests/day ({wr/total_req*100:.1f}% of the "
              f"book's outbound) for {trim.money(ws)}/day "
              f"({ws/total_spend*100 if total_spend else 0:.2f}% of spend).")
    else:
        print(f"\n  No country carries {args.min_requests_day:,.0f}+ requests/day "
              f"for under {trim.money(args.max_spend_day)}/day.")
    return waste


def render_pairs(rows: list[dict], args) -> list[dict]:
    """demand_source × country, the grain a blacklist is actually written at."""
    waste = [r for r in rows
             if r["requests_day"] >= args.min_requests_day
             and r["spend_day"] < args.max_spend_day]
    waste.sort(key=lambda r: -r["requests_day"])

    print(f"\n{_HDR}\nDSP × country pairs that never trade\n{_HDR}")
    if not waste:
        print("  none above the thresholds")
        return []

    print(f"  {'requests/day':>15} {'imps/day':>10} {'$/day':>9}  pair")
    print(f"  {'-'*15:>15} {'-'*10:>10} {'-'*9:>9}  {'-'*40}")
    for r in waste[:args.top]:
        print(f"  {r['requests_day']:>15,.0f} {r['imps_day']:>10,.0f} "
              f"{trim.money(r['spend_day']):>9}  {r['name']}")

    total = sum(r["requests_day"] for r in waste)
    lost = sum(r["spend_day"] for r in waste)
    print(f"\n  {len(waste)} pair(s): {total:,.0f} requests/day for "
          f"{trim.money(lost)}/day.")
    print(f"\n  These are per-DSP. Removing a country from a buyer that has "
          f"never bought it\n  cannot reduce that buyer's revenue and cannot "
          f"touch any other buyer.")

    # Group by DSP so the reader gets one call per buyer rather than one per
    # pair — which is also how set_demand_geo_blacklist takes its argument.
    by_dsp: dict[int, list[str]] = {}
    for r in waste:
        if r["id"] is not None:
            by_dsp.setdefault(r["id"], []).append(r["country"])
    if by_dsp:
        # Codes, not platform ids — set_demand_geo_blacklist takes the numeric
        # ids that tbx_api.country_ids() resolves these to. scripts/
        # tbx_geo_cut.py does that resolution, re-measures first, and refuses
        # a buyer whose countries do not all resolve.
        print(f"\n  One call per buyer (country CODES; resolve with "
              f"tbx_api.country_ids):")
        for did, countries in sorted(by_dsp.items()):
            print(f"    set_demand_geo_blacklist({did}, "
                  f"country_ids({sorted(set(countries))}))")
        print(f"\n  To apply, with a fresh measurement and a revertible "
              f"ledger:\n    python3 scripts/tbx_geo_cut.py --apply")
    return waste


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Countries and DSP×country pairs that cost QPS and never buy.")
    p.add_argument("--view", choices=("country", "pairs"), default="country",
                   help="country sizes the prize; pairs is the actionable grain")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--min-requests-day", type=float, default=1_000_000,
                   help="only consider rows above this outbound volume "
                        "(default 1e6; use a lower value for --view pairs)")
    p.add_argument("--max-spend-day", type=float, default=1.0,
                   help="'never buys' threshold in $/day (default 1.0)")
    p.add_argument("--top", type=int, default=30)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Geo waste ({args.view}) — {start} → {end} ({args.days} settled days)")

    if args.view == "country":
        grain, key = "country", "country"
    else:
        # One attribute string the report groups by; split_name_id then
        # recovers the demand id from the "Name #NNNN — CC" shape.
        grain, key = "demand_source", "demand_source"

    if args.view == "country":
        rows, days_with_data = trim.pull_daily(grain, start, args.days, GEO_METRICS)
    else:
        rows, days_with_data = pull_pairs(start, args.days)

    if days_with_data == 0:
        print("no data came back — not reporting on an empty read.", file=sys.stderr)
        return 2

    summarised = summarise(rows, key, days_with_data)
    print(f"  {len(summarised)} row(s) over {days_with_data}/{args.days} days")
    if days_with_data < args.days:
        print(f"  ⚠ partial window — every per-day figure below is divided by "
              f"{days_with_data}, not {args.days}. A pair that only traded on a "
              f"missing day looks dead here.")

    flagged = (render_country(summarised, args) if args.view == "country"
               else render_pairs(summarised, args))

    print(f"\n  Nothing was changed. This report never writes.")
    return 1 if flagged else 0


def pull_pairs(start: date, days: int) -> tuple[list[dict], int]:
    """demand_source × country, one request per day.

    Kept separate from `trim.pull_daily` because the key is a composite: the
    aggregation has to be per (demand, country), not per demand, or every
    country collapses into one row and the whole point is lost.
    """
    totals: dict[tuple, dict[str, float]] = {}
    days_with_data = 0
    failed: list[str] = []
    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        try:
            rows, _ = tbx.report(day, day,
                                 attributes=["date", "demand_source", "country"],
                                 metrics=GEO_METRICS)
        except tbx.TbxError as exc:
            # One day timing out must not throw away the days that answered.
            # This grain is the heaviest query the repo makes and read
            # timeouts are routine here (§5.9) — on 2026-08-31 three good days
            # were lost to the fourth one failing. Every per-day figure is
            # already divided by the days that answered, so a short window is
            # a smaller measurement, not a wrong one. The caller decides
            # whether the coverage is enough to act on.
            print(f"    {day}: FAILED — {exc}", file=sys.stderr, flush=True)
            failed.append(day)
            continue
        kept = 0
        for row in rows:
            if str(row.get("date") or "")[:10] != day:
                continue
            dname, did = trim.split_name_id(row.get("demand_source") or "")
            country = row.get("country") or "(none)"
            key = (did, dname, country)
            bucket = totals.setdefault(key, {m: 0.0 for m in GEO_METRICS})
            for metric in GEO_METRICS:
                bucket[metric] += trim.num(row, metric)
            kept += 1
        print(f"    {day}: {kept} pair rows", flush=True)
        if kept:
            days_with_data += 1
    if failed:
        print(f"\n  {len(failed)}/{days} day(s) did not answer: {', '.join(failed)}",
              flush=True)
    out = []
    for (did, dname, country), vals in totals.items():
        out.append({
            "demand_id": did,
            "country": country,
            "label": f"{dname} #{did} — {country}" if did else f"{dname} — {country}",
            **vals,
        })
    return out, days_with_data


if __name__ == "__main__":
    # Exit 1 means "found waste" and callers treat it as success, so an
    # unhandled TbxError must NOT also exit 1 — Python's default for an
    # uncaught exception. On 2026-08-31 that collision let a crashed pairs
    # run report success and produce nothing.
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
