#!/usr/bin/env python3
"""
What traffic is worth cutting, and where is the margin going?

Two questions that are usually asked separately but share one report, because
the answer to both is a ranking of supply and demand sources by what they cost
against what they return.

Read-only. Imports nothing from the write path and never sets
TBX_ALLOW_WRITES; every finding is a recommendation for a human to action in
the dashboard: a SUPPLY source's margin_type/margin_min/margin_max are
read-only over this API (`docs/teqblaze-new-platform.md` §6.1). Note the
qualifier — a DEMAND source's margin fields ARE writable, via
`set_demand_economics`; only the supply side is locked.

What "cost" means here, and what it does not
--------------------------------------------
This report deliberately does NOT put a dollar figure on a bid request. PGAM
has no per-request infrastructure cost in any system this can read, and an
invented one would make every ranking below look authoritative when it is
really a guess with a multiplier on it.

So waste is expressed in the unit it is actually measured in: **requests per
revenue dollar**, ranked against the book's own median. A source needing 40x
the median number of requests to produce a dollar is a real finding whatever
a request costs, and it stays a finding if the cost per request changes.

Every cut candidate is reported with BOTH sides of the trade:

    what you stop paying for   — requests/day, the QPS and scanner load
    what you give up           — revenue/day, the actual money lost

A cut is only obvious when the second number is ~0. When it is not, the report
says so rather than recommending it, because "cut the low-margin source" is
how a book shrinks into profitability and then out of it.

Buckets
-------
supply side
  DEAD        requests but zero impressions over the whole window. Pure cost.
  NEAR-DEAD   below --min-revenue-day per day. Costs real QPS for pennies.
  LOSS        payout exceeds what the DSP paid: every impression loses money.
  HUNGRY      requests-per-dollar above --hungry-multiple x the book median.

demand side
  NO-WIN      we send bid requests, they never win. Pure outbound cost.
  TIMEOUT     above --timeout-pct of requests time out — they slow every
              auction they are in and cannot pay for the ones they lose.

margin
  Realised take rate per supply source, ranked, with the configured
  margin_type / margin_min / margin_max read back per source so the report
  can say whether the realised number is even inside the band that was set.
  That comparison is the point: a source realising 4% under a 5-30% range is
  not a pricing outcome, it is a configuration that is not doing what it says.

Exit codes:
    0  nothing flagged
    1  at least one cut candidate or out-of-band margin found
    2  credentials absent
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from statistics import median

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402

# Same US/Eastern close as tbx_take_rate: a day is not final until ~04:00 or
# 05:00 UTC depending on DST, and reading earlier understates it.
ET_CLOSE_LAG_HOURS = 5

_TRAILING_ID = re.compile(r"\s*#(-?\d+)\s*$")
_HDR = "=" * 78


def split_name_id(raw: str) -> tuple[str, int | None]:
    """('Advetisi - Zmaticoo #264') -> ('Advetisi - Zmaticoo', 264).

    The vendor appends the entity id to the display name rather than sending
    it as a field — the same convention the ETL parses. Only a TRAILING
    #NNNN counts, so a '#' inside a partner's own name is left alone.
    """
    text = (raw or "").strip()
    m = _TRAILING_ID.search(text)
    if not m:
        return text, None
    return text[:m.start()].strip(), int(m.group(1))


def latest_settled(now_utc: datetime) -> date:
    """The most recent day whose US/Eastern close has passed."""
    return (now_utc - timedelta(hours=ET_CLOSE_LAG_HOURS)).date() - timedelta(days=1)


def num(row: dict, key: str) -> float:
    """A metric as a float. The platform returns these as strings."""
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def money(value: float) -> str:
    return f"${value:,.2f}"


def per_dollar(requests: float, revenue: float) -> float | None:
    """Requests needed per revenue dollar. None when there is no revenue.

    None is not zero and must not sort as zero — a source with no revenue at
    all is the worst case, not the best, so callers handle it explicitly
    rather than letting a 0.0 sentinel float it to the top of an ascending
    sort.
    """
    if revenue <= 0:
        return None
    return requests / revenue


# ---------------------------------------------------------------------------
# Pulls
# ---------------------------------------------------------------------------

# Counts and money only. Every rate metric the platform offers is derivable
# from these (fill rate is imps/requests), and a report asking for computed
# rates is markedly slower to come back — the first live run spent >25 minutes
# on two calls. Ask for the raw numbers and do the division here.
SUPPLY_METRICS = [
    "ssp_requests_sum", "imps_sum",
    "dsp_price_sum", "ssp_price_sum",
]

# `timeout_rate` is the one rate that cannot be derived from counts, so it
# stays — but it is the sole reason the demand call is the expensive one, and
# --no-timeout drops it when the supply half is all that is wanted.
DEMAND_METRICS = [
    "requests_sum", "responses_sum", "wins_sum", "imps_sum",
    "dsp_price_sum", "timeout_rate",
]

# Metrics that are ratios, not counts. Summing a rate across seven days gives
# a number up to 700% and would put every demand partner in the TIMEOUT
# bucket, so these are averaged instead — weighted by RATE_WEIGHT, because a
# day on which a partner saw 12 requests should not count as much as a day it
# saw twelve million.
RATE_METRICS = frozenset({"timeout_rate"})
RATE_WEIGHT = "requests_sum"


def pull_daily(grain: str, start: date, days: int,
               metrics: list[str]) -> tuple[list[dict], int]:
    """Sum `metrics` per entity over `days`, ONE DAY PER REQUEST.

    Not an optimisation — a correctness requirement, and the same conclusion
    `tbx_revenue_etl` reached independently (its CHUNK_DAYS = 1). A multi-day
    request is answered 200 with only the most recent ~5 days in it: no error,
    no flag. Asked for 7 days in one call, this report would have divided a
    5-day total by 7 and understated every source by ~30% while looking
    entirely healthy.

    It is also the faster shape in practice. The first live run asked for the
    whole window at once and was still going after 34 minutes; the probe's
    single-day calls return in under a second.

    `date` is requested and checked on every row rather than trusted, because
    a single-day request is only known to be single-day if the rows say so.

    Returns (rows, days_with_data). The second is the divisor — dividing by a
    nominal 7 when two days returned nothing is the same understatement in a
    different place.
    """
    totals: dict[str, dict[str, float]] = {}
    # Numerator and denominator for each rate metric, kept apart from the
    # counts so the weighted average can be finished once at the end.
    rate_acc: dict[str, dict[str, float]] = {}
    rate_weight: dict[str, float] = {}
    days_with_data = 0
    started = time.monotonic()

    counts = [m for m in metrics if m not in RATE_METRICS]
    rates = [m for m in metrics if m in RATE_METRICS]

    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        # Per-day timing, flushed. Two runs were cancelled at 34 and 48
        # minutes having printed nothing at all, so there was no way to tell
        # a slow call from a hung one, or to know which grain was to blame.
        # A line per day costs nothing and makes the next slow run legible.
        day_started = time.monotonic()
        rows, _ = tbx.report(day, day, attributes=["date", grain],
                             metrics=metrics)
        print(f"    {day}: {len(rows)} rows in "
              f"{time.monotonic() - day_started:.1f}s", flush=True)
        kept = 0
        for row in rows:
            if str(row.get("date") or "")[:10] != day:
                continue                      # off-window row; the ETL's rule
            key = row.get(grain) or ""
            bucket = totals.setdefault(key, {m: 0.0 for m in counts})
            for metric in counts:
                bucket[metric] += num(row, metric)
            if rates:
                weight = num(row, RATE_WEIGHT)
                acc = rate_acc.setdefault(key, {m: 0.0 for m in rates})
                for metric in rates:
                    acc[metric] += num(row, metric) * weight
                rate_weight[key] = rate_weight.get(key, 0.0) + weight
            kept += 1
        if kept:
            days_with_data += 1

    out = []
    for key, vals in totals.items():
        row = {grain: key, **vals}
        weight = rate_weight.get(key, 0.0)
        for metric in rates:
            # No weight means the rate was never observed against any
            # traffic. Leave it out entirely rather than writing 0.0 — the
            # assessor reads a missing key as "not measured", which is what
            # this is.
            if weight > 0:
                row[metric] = rate_acc[key][metric] / weight
        out.append(row)

    elapsed = time.monotonic() - started
    print(f"  ✓ {grain}: {len(out)} entities over {days_with_data}/{days} "
          f"days in {elapsed:.1f}s", flush=True)
    return out, days_with_data


def pull_supply(start: date, days: int) -> tuple[list[dict], int]:
    print("  → supply, one call per day ...", flush=True)
    return pull_daily("supply_source", start, days, SUPPLY_METRICS)


def pull_demand(start: date, days: int,
                metrics: list[str]) -> tuple[list[dict], int]:
    print("  → demand, one call per day ...", flush=True)
    return pull_daily("demand_source", start, days, metrics)


# ---------------------------------------------------------------------------
# Assessment
# ---------------------------------------------------------------------------

def assess_supply(rows: list[dict], days: int, args) -> tuple[list[dict], float | None]:
    """Per supply source: the trade, the bucket, and the book's median rate.

    Returns (assessed, median_requests_per_dollar). The median is computed
    over earning sources only — including the zero-revenue ones would drag
    it toward a number no healthy source is near, and it is the yardstick
    the HUNGRY bucket is measured against.
    """
    assessed = []
    for row in rows:
        name, sid = split_name_id(row.get("supply_source", ""))
        requests = num(row, "ssp_requests_sum")
        imps = num(row, "imps_sum")
        gross = num(row, "dsp_price_sum")
        payout = num(row, "ssp_price_sum")
        profit = gross - payout
        assessed.append({
            "name": name, "id": sid,
            "requests_day": requests / days,
            "imps_day": imps / days,
            "gross_day": gross / days,
            "profit_day": profit / days,
            "take_rate": (profit / gross * 100.0) if gross > 0 else None,
            "fill_rate": (imps / requests * 100.0) if requests else 0.0,
            "per_dollar": per_dollar(requests, gross),
            "requests": requests, "gross": gross, "imps": imps,
        })

    earning = [a["per_dollar"] for a in assessed if a["per_dollar"] is not None]
    book_median = median(earning) if earning else None

    for a in assessed:
        # Order matters: the first bucket that fits wins, and they are
        # ordered by how unambiguous the cut is. A source that is both DEAD
        # and HUNGRY should read as DEAD — that is the stronger statement.
        if a["requests_day"] < args.min_requests_day:
            a["bucket"] = None          # too small to have an opinion about
        elif a["imps"] == 0:
            a["bucket"] = "DEAD"
        elif a["gross_day"] < args.min_revenue_day:
            a["bucket"] = "NEAR-DEAD"
        elif a["profit_day"] <= 0:
            a["bucket"] = "LOSS"
        elif (book_median and a["per_dollar"]
              and a["per_dollar"] > book_median * args.hungry_multiple):
            a["bucket"] = "HUNGRY"
        else:
            a["bucket"] = None
    return assessed, book_median


def assess_demand(rows: list[dict], days: int, args) -> list[dict]:
    """Per demand source: outbound cost against what it actually buys."""
    assessed = []
    for row in rows:
        name, did = split_name_id(row.get("demand_source", ""))
        requests = num(row, "requests_sum")
        wins = num(row, "wins_sum")
        imps = num(row, "imps_sum")
        spend = num(row, "dsp_price_sum")
        # None, not 0.0, when the metric was not requested — a missing
        # rate is "not measured", and scoring it as 0 would silently clear
        # every partner of the TIMEOUT verdict.
        timeout = num(row, "timeout_rate") if "timeout_rate" in row else None
        entry = {
            "name": name, "id": did,
            "requests_day": requests / days,
            "wins_day": wins / days,
            "imps_day": imps / days,
            "spend_day": spend / days,
            "timeout_rate": timeout,
            "per_dollar": per_dollar(requests, spend),
            "requests": requests, "spend": spend, "wins": wins,
        }
        if entry["requests_day"] < args.min_requests_day:
            entry["bucket"] = None
        elif wins == 0:
            entry["bucket"] = "NO-WIN"
        elif timeout is not None and timeout >= args.timeout_pct:
            entry["bucket"] = "TIMEOUT"
        else:
            entry["bucket"] = None
        assessed.append(entry)
    return assessed


def read_margin_config(ids: list[int]) -> dict[int, dict]:
    """Configured margin band per supply source, by id.

    One GET each, so the caller caps how many. Any source that fails to read
    is simply absent from the result — a missing config must not be reported
    as a zero band, which would make every source look out of range.
    """
    out: dict[int, dict] = {}
    for sid in ids:
        try:
            entity = tbm.get_supply_source(sid) or {}
        except Exception as exc:                      # noqa: BLE001
            print(f"  ! could not read config for supply {sid}: {exc}",
                  file=sys.stderr)
            continue
        if not entity:
            continue
        source = entity.get("source") or {}
        out[sid] = {
            "margin_type": entity.get("margin_type"),
            "margin_min": entity.get("margin_min"),
            "margin_max": entity.get("margin_max"),
            "is_dynamic_margin": source.get("is_dynamic_margin"),
            "dynamic_margin": source.get("dynamic_margin"),
            "is_smart_floor": source.get("is_smart_floor"),
            "floor_price": source.get("floor_price"),
        }
    return out


def band_verdict(realised: float | None, cfg: dict) -> str:
    """Is the realised take rate inside the band the source is configured for?

    `fixed` uses margin_min as the single number and ignores margin_max,
    which the platform returns as 0 — reading that as an upper bound would
    put every fixed source permanently "above band".
    """
    if realised is None:
        return "no revenue"
    kind = (cfg.get("margin_type") or "").lower()
    try:
        low = float(cfg.get("margin_min") or 0)
        high = float(cfg.get("margin_max") or 0)
    except (TypeError, ValueError):
        return "unreadable band"

    if kind == "fixed":
        # A fixed margin should land on its number. Realised is blended over
        # the window, so allow a point of slack before calling it wrong.
        if abs(realised - low) <= 1.0:
            return f"on target ({low:g}% fixed)"
        return f"OFF TARGET — configured {low:g}% fixed, realising {realised:.1f}%"
    if kind in ("range", "adaptive"):
        if realised < low:
            return f"BELOW BAND — {low:g}-{high:g}% configured, realising {realised:.1f}%"
        if high and realised > high:
            return f"above band — {low:g}-{high:g}% configured, realising {realised:.1f}%"
        return f"in band ({low:g}-{high:g}%)"
    return f"unknown margin_type {kind!r}"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def section(title: str) -> None:
    print(f"\n{_HDR}\n{title}\n{_HDR}")


def render_cuts(assessed: list[dict], side: str) -> list[dict]:
    """Print the cut candidates for one side and return them."""
    flagged = [a for a in assessed if a.get("bucket")]
    if not flagged:
        print(f"  nothing flagged on the {side} side")
        return []

    revenue_key = "gross_day" if side == "supply" else "spend_day"
    order = {"DEAD": 0, "NEAR-DEAD": 1, "LOSS": 2, "HUNGRY": 3,
             "NO-WIN": 0, "TIMEOUT": 1}
    flagged.sort(key=lambda a: (order.get(a["bucket"], 9), -a["requests_day"]))

    print(f"  {'bucket':<10} {'requests/day':>13} {'$/day':>10}  source")
    print(f"  {'-' * 10} {'-' * 13:>13} {'-' * 10:>10}  {'-' * 34}")
    for a in flagged:
        label = a["name"][:34] + (f" #{a['id']}" if a["id"] else "")
        print(f"  {a['bucket']:<10} {a['requests_day']:>13,.0f} "
              f"{money(a[revenue_key]):>10}  {label}")

    total_requests = sum(a["requests_day"] for a in flagged)
    total_revenue = sum(a[revenue_key] for a in flagged)
    print(f"\n  {len(flagged)} flagged: {total_requests:,.0f} requests/day "
          f"against {money(total_revenue)}/day.")

    free = [a for a in flagged if a[revenue_key] < 1.0]
    if free:
        free_requests = sum(a["requests_day"] for a in free)
        print(f"  Of those, {len(free)} earn under $1/day and account for "
              f"{free_requests:,.0f} requests/day — the unambiguous cuts.")
    return flagged


def render_margins(assessed: list[dict], config: dict[int, dict], top: int) -> list[dict]:
    """Realised take rate against configured band, biggest sources first."""
    earning = [a for a in assessed if a["gross_day"] > 0]
    earning.sort(key=lambda a: -a["gross_day"])
    shown = earning[:top]

    out_of_band = []
    print(f"  {'$/day':>10} {'take':>7}  {'configured':<26} source")
    print(f"  {'-' * 10:>10} {'-' * 7:>7}  {'-' * 26:<26} {'-' * 30}")
    for a in shown:
        cfg = config.get(a["id"] or -1)
        if cfg:
            verdict = band_verdict(a["take_rate"], cfg)
            band = f"{cfg.get('margin_type') or '?'} " \
                   f"{cfg.get('margin_min')}-{cfg.get('margin_max')}"
            if verdict.startswith(("BELOW", "OFF", "above")):
                out_of_band.append((a, cfg, verdict))
        else:
            band = "(not read)"
        take = f"{a['take_rate']:.1f}%" if a["take_rate"] is not None else "—"
        label = a["name"][:30] + (f" #{a['id']}" if a["id"] else "")
        print(f"  {money(a['gross_day']):>10} {take:>7}  {band:<26} {label}")

    if out_of_band:
        print(f"\n  {len(out_of_band)} realising outside the band they are set to:")
        for a, cfg, verdict in out_of_band:
            label = a["name"] + (f" #{a['id']}" if a["id"] else "")
            print(f"    • {label}: {verdict}")
            print(f"      {money(a['gross_day'])}/day gross, "
                  f"{money(a['profit_day'])}/day profit")
            if cfg.get("is_smart_floor"):
                print(f"      is_smart_floor is ON — the platform's optimiser "
                      f"owns this floor, so a PGAM floor agent must not.")
    else:
        print("\n  every source read is realising inside its configured band")
    return [a for a, _, _ in out_of_band]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Traffic worth cutting and margins worth reviewing, on TBX.")
    parser.add_argument("--days", type=int, default=7,
                        help="settled days to assess (default 7)")
    parser.add_argument("--min-requests-day", type=float, default=10_000,
                        help="ignore sources below this many requests/day "
                             "(default 10000) — too small to be worth a verdict")
    parser.add_argument("--min-revenue-day", type=float, default=1.0,
                        help="NEAR-DEAD threshold in $/day (default 1.0)")
    parser.add_argument("--hungry-multiple", type=float, default=5.0,
                        help="HUNGRY at this multiple of the book's median "
                             "requests-per-dollar (default 5)")
    parser.add_argument("--timeout-pct", type=float, default=20.0,
                        help="TIMEOUT at this timeout_rate or above (default 20)")
    parser.add_argument("--config-top", type=int, default=20,
                        help="read the margin config for this many top "
                             "sources by revenue (one GET each, default 20)")
    parser.add_argument("--no-timeout", action="store_true",
                        help="drop timeout_rate from the demand pull. It is "
                             "the one rate that cannot be derived from counts "
                             "and the slowest metric to come back; without it "
                             "the TIMEOUT bucket is not assessed at all")
    parser.add_argument("--no-config", action="store_true",
                        help="skip the per-source config reads entirely")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    end = latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    df, dt = start.isoformat(), end.isoformat()
    days = args.days

    print(f"TBX trim report — {df} to {dt} ({days} settled days)")
    print(f"host {os.getenv('TBX_BASE_URL', 'https://api.pgammedia.com')}")

    section("SUPPLY — what we pay to receive")
    supply_rows, supply_days = pull_supply(start, days)
    supply, book_median = assess_supply(supply_rows, max(supply_days, 1), args)
    if book_median:
        print(f"  book median: {book_median:,.0f} requests per revenue dollar")
        print(f"  HUNGRY is above {book_median * args.hungry_multiple:,.0f}\n")
    supply_flagged = render_cuts(supply, "supply")

    section("DEMAND — what we pay to send")
    demand_metrics = [m for m in DEMAND_METRICS
                      if not (args.no_timeout and m == "timeout_rate")]
    if args.no_timeout:
        print("  --no-timeout: the TIMEOUT bucket is NOT assessed this run")
    demand_rows, demand_days = pull_demand(start, days, demand_metrics)
    demand = assess_demand(demand_rows, max(demand_days, 1), args)
    demand_flagged = render_cuts(demand, "demand")

    section("MARGIN — realised take rate against configured band")
    config: dict[int, dict] = {}
    if not args.no_config:
        earning = sorted((a for a in supply if a["gross_day"] > 0),
                         key=lambda a: -a["gross_day"])[:args.config_top]
        ids = [a["id"] for a in earning if a["id"]]
        print(f"  reading margin config for {len(ids)} sources...\n")
        config = read_margin_config(ids)
    off_band = render_margins(supply, config, args.config_top)

    section("SUMMARY")
    total_gross = sum(a["gross_day"] for a in supply)
    total_profit = sum(a["profit_day"] for a in supply)
    blended = (total_profit / total_gross * 100.0) if total_gross else 0.0
    print(f"  book: {money(total_gross)}/day gross, {money(total_profit)}/day "
          f"profit, {blended:.1f}% blended take rate")
    print(f"  supply cut candidates: {len(supply_flagged)}")
    print(f"  demand cut candidates: {len(demand_flagged)}")
    print(f"  sources realising outside their band: {len(off_band)}")
    print("\n  Nothing here was changed. A supply source's margin is read-only "
          "over this API (§6.1),\n  so the margin actions above are dashboard "
          "changes. Demand-source margin IS writable.")

    return 1 if (supply_flagged or demand_flagged or off_band) else 0


if __name__ == "__main__":
    sys.exit(main())
