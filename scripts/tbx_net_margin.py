#!/usr/bin/env python3
"""
What PGAM actually keeps, after Teqblaze's fee.

The gap this closes
-------------------
Every margin tool in this repo reports the **take rate**:

    take = (dsp_price_sum - ssp_price_sum) / dsp_price_sum

That is the spread between what the buyer paid and what the publisher was
paid. It is not what PGAM keeps, because Teqblaze bills a percentage of
gross on top. `tbx_take_rate.py` and `tbx_margin_sentry.py` both stop at the
take rate, so every margin number anyone has looked at so far is overstated
by the whole fee.

The arithmetic, and why the fee base matters so much
----------------------------------------------------
With the fee charged on GROSS (the default, and how PGAM is billed):

    gross   = dsp_price_sum          what the buyer paid
    payout  = ssp_price_sum          what the publisher was paid
    margin  = gross - payout         the spread, i.e. the take
    fee     = fee_pct * gross        Teqblaze
    net     = margin - fee           what PGAM keeps

    net%    = margin/gross - fee_pct = take% - fee_pct

So on a gross base the fee comes straight off the take rate in percentage
points, and two things follow that are easy to miss:

  * **A source whose take rate is below the fee loses money on every
    impression.** Not "earns little" — costs money. At an 8% fee a 5.3%
    take rate is -2.7% net.

  * **The fee's bite is a fraction of the take, not of the gross.**
    retention = net/margin = 1 - fee_pct/take%. At a 30% take rate the fee
    costs 27% of the margin; at 12% it costs 67%; at 10% it costs 80%. The
    thinner the spread, the more of it the fee takes — which is the opposite
    of the intuition that a flat percentage hurts everyone equally.

If the fee is instead charged on the margin, pass `--fee-base margin`:
net = margin * (1 - fee_pct), retention is a flat (1 - fee_pct), and nothing
can be pushed underwater by the fee alone. The two bases give very different
answers on thin sources, so the flag is explicit rather than assumed.

Settlement
----------
A day is not final until its US/Eastern close has passed (§ET_CLOSE_LAG).
Days after that are reported and clearly marked PARTIAL, but the per-source
aggregate and the recommendation are computed from settled days only —
a half-counted day understates gross and can invent a margin problem that
the full day does not have. When every requested day is partial, the report
says so at the top and in the recommendation rather than quietly proceeding.

Read-only. It imports no write path.

Usage
-----
    # the default: the last settled day
    python3 scripts/tbx_net_margin.py

    # named days, at the real fee
    python3 scripts/tbx_net_margin.py --date 2026-09-01 --date 2026-09-02 \
        --fee-pct 8

    # what would it take to clear 10% net?
    python3 scripts/tbx_net_margin.py --lookback 7 --target-net 10

Exit codes:
    0  nothing is underwater
    1  at least one source nets negative after the fee
    2  credentials absent, or nothing could be measured
    3  the platform was unreachable
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

_HDR = "=" * 78

# Money only. ssp_requests_sum is the heaviest counter in the system (§5.9)
# and nothing here needs it.
METRICS = ["imps_sum", "dsp_price_sum", "ssp_price_sum"]


def fee_on(gross: float, margin: float, fee_pct: float, base: str) -> float:
    """Teqblaze's cut, in dollars."""
    return (gross if base == "gross" else margin) * fee_pct / 100.0


def net_rate(take_pct: float | None, fee_pct: float, base: str) -> float | None:
    """Net margin as a percentage of gross."""
    if take_pct is None:
        return None
    if base == "gross":
        return take_pct - fee_pct
    return take_pct * (1.0 - fee_pct / 100.0)


def retention(take_pct: float | None, fee_pct: float, base: str) -> float | None:
    """How much of the spread survives the fee, as a percentage of it."""
    if take_pct is None or take_pct == 0:
        return None
    if base == "gross":
        return (1.0 - fee_pct / take_pct) * 100.0
    return 100.0 - fee_pct


def take_of(gross: float, payout: float) -> float | None:
    return ((gross - payout) / gross * 100.0) if gross > 0 else None


def pull_day(day: str) -> dict[str, dict] | None:
    """{source key: {name, id, gross, payout, imps}} for one day, or None.

    One day per request (§5.10: a multi-day request is answered 200 with only
    the most recent ~5 days in it). A day that raises returns None, which is
    reported as unmeasured rather than folded in as a day of zero revenue.
    """
    try:
        rows, _ = tbx.report(day, day, attributes=["date", "supply_source"],
                             metrics=METRICS)
    except tbx.TbxError as exc:
        print(f"    {day}: FAILED — {exc}", file=sys.stderr, flush=True)
        return None

    out: dict[str, dict] = {}
    for row in rows:
        if str(row.get("date") or "")[:10] != day:
            continue
        name, sid = trim.split_name_id(row.get("supply_source") or "")
        key = f"{name}#{sid}" if sid is not None else name
        entry = out.setdefault(key, {"name": name, "id": sid,
                                     "gross": 0.0, "payout": 0.0, "imps": 0.0})
        entry["gross"] += trim.num(row, "dsp_price_sum")
        entry["payout"] += trim.num(row, "ssp_price_sum")
        entry["imps"] += trim.num(row, "imps_sum")
    print(f"    {day}: {len(out)} supply source(s), "
          f"{trim.money(sum(e['gross'] for e in out.values()))} gross",
          flush=True)
    return out


def day_line(day: str, sources: dict[str, dict], args, settled: bool) -> dict:
    gross = sum(e["gross"] for e in sources.values())
    payout = sum(e["payout"] for e in sources.values())
    margin = gross - payout
    take = take_of(gross, payout)
    fee = fee_on(gross, margin, args.fee_pct, args.fee_base)
    return {
        "day": day, "settled": settled,
        "gross": gross, "payout": payout, "margin": margin,
        "take_pct": take, "fee": fee, "net": margin - fee,
        "net_pct": net_rate(take, args.fee_pct, args.fee_base),
        "retention_pct": retention(take, args.fee_pct, args.fee_base),
        "imps": sum(e["imps"] for e in sources.values()),
    }


def render_days(lines: list[dict], args) -> None:
    print(f"\n{_HDR}\nWhat PGAM keeps, per day — "
          f"{args.fee_pct:g}% Teqblaze fee on {args.fee_base}\n{_HDR}")
    print(f"  {'day':<12} {'gross':>11} {'payout':>11} {'take':>7} "
          f"{'TB fee':>10} {'net':>11} {'net%':>7} {'kept':>6}")
    print(f"  {'-'*12} {'-'*11} {'-'*11} {'-'*7} {'-'*10} {'-'*11} "
          f"{'-'*7} {'-'*6}")
    for row in lines:
        flag = "" if row["settled"] else "  ← PARTIAL"
        take = f"{row['take_pct']:.1f}%" if row["take_pct"] is not None else "  n/a"
        netp = f"{row['net_pct']:.1f}%" if row["net_pct"] is not None else "  n/a"
        keep = f"{row['retention_pct']:.0f}%" if row["retention_pct"] is not None else " n/a"
        print(f"  {row['day']:<12} {row['gross']:>11,.2f} {row['payout']:>11,.2f} "
              f"{take:>7} {row['fee']:>10,.2f} {row['net']:>11,.2f} "
              f"{netp:>7} {keep:>6}{flag}")
    print("\n  'kept' is how much of the spread survives the fee "
          "(net / take).")


def aggregate(days: list[dict[str, dict]]) -> dict[str, dict]:
    """Sum the per-day source maps into one."""
    out: dict[str, dict] = {}
    for sources in days:
        for key, entry in sources.items():
            agg = out.setdefault(key, {"name": entry["name"], "id": entry["id"],
                                       "gross": 0.0, "payout": 0.0, "imps": 0.0})
            agg["gross"] += entry["gross"]
            agg["payout"] += entry["payout"]
            agg["imps"] += entry["imps"]
    return out


def assess(sources: dict[str, dict], n_days: int, args) -> list[dict]:
    out = []
    for entry in sources.values():
        gross = entry["gross"]
        if gross < args.min_gross:
            continue
        payout = entry["payout"]
        margin = gross - payout
        take = take_of(gross, payout)
        fee = fee_on(gross, margin, args.fee_pct, args.fee_base)
        net = margin - fee
        out.append({
            "name": entry["name"], "id": entry["id"],
            "gross_day": gross / n_days,
            "margin_day": margin / n_days,
            "fee_day": fee / n_days,
            "net_day": net / n_days,
            "take_pct": take,
            "net_pct": net_rate(take, args.fee_pct, args.fee_base),
            "retention_pct": retention(take, args.fee_pct, args.fee_base),
        })
    out.sort(key=lambda r: r["net_day"])
    return out


def render_sources(rows: list[dict], args, n_days: int, settled: bool) -> list[dict]:
    label = "settled" if settled else "PARTIAL — not final"
    print(f"\n{_HDR}\nBy supply source, per day across {n_days} {label} day(s)"
          f"\n{_HDR}")
    print(f"  {'source':<38} {'gross/d':>10} {'take':>7} {'net%':>7} "
          f"{'net $/d':>10}")
    print(f"  {'-'*38} {'-'*10} {'-'*7} {'-'*7} {'-'*10}")
    for row in rows:
        name = f"{row['name']} #{row['id']}" if row["id"] is not None else row["name"]
        take = f"{row['take_pct']:.1f}%" if row["take_pct"] is not None else "  n/a"
        netp = f"{row['net_pct']:.1f}%" if row["net_pct"] is not None else "  n/a"
        print(f"  {name[:38]:<38} {row['gross_day']:>10,.2f} {take:>7} "
              f"{netp:>7} {row['net_day']:>10,.2f}")

    underwater = [r for r in rows if r["net_day"] < 0]
    if underwater:
        lost = sum(r["net_day"] for r in underwater)
        print(f"\n  ⚠ {len(underwater)} source(s) net NEGATIVE after the fee — "
              f"{trim.money(lost)}/day.")
        if args.fee_base == "gross":
            print(f"    Every one has a take rate below the {args.fee_pct:g}% "
                  f"fee, so each impression costs money.")
    return underwater


def render_adjustment(rows: list[dict], args, settled: bool) -> None:
    print(f"\n{_HDR}\nWhat it would take to clear {args.target_net:g}% net"
          f"\n{_HDR}")
    if not settled:
        print("  ⚠ Computed from PARTIAL days — treat as directional only.")
    if args.fee_base == "gross":
        need = args.target_net + args.fee_pct
        print(f"  On a gross-based fee, net% = take% - {args.fee_pct:g}, so "
              f"clearing {args.target_net:g}% net\n  requires a "
              f"{need:.1f}% take rate. Below that the source cannot get there "
              f"on volume.\n")
    else:
        need = args.target_net / (1.0 - args.fee_pct / 100.0)
        print(f"  On a margin-based fee, clearing {args.target_net:g}% net "
              f"requires a {need:.1f}% take rate.\n")

    short = [r for r in rows if r["net_pct"] is not None
             and r["net_pct"] < args.target_net]
    if not short:
        print(f"  Every source measured is already at or above "
              f"{args.target_net:g}% net.")
        return

    print(f"  {'source':<38} {'take':>7} {'need':>7} {'gap pp':>7} "
          f"{'$/day to close':>15}")
    print(f"  {'-'*38} {'-'*7} {'-'*7} {'-'*7} {'-'*15}")
    total = 0.0
    for row in sorted(short, key=lambda r: -(need - (r["take_pct"] or 0))
                      * r["gross_day"]):
        gap = need - (row["take_pct"] or 0.0)
        dollars = gap / 100.0 * row["gross_day"]
        total += dollars
        name = f"{row['name']} #{row['id']}" if row["id"] is not None else row["name"]
        print(f"  {name[:38]:<38} {row['take_pct'] or 0:>6.1f}% {need:>6.1f}% "
              f"{gap:>6.1f} {dollars:>15,.2f}")
    print(f"\n  Closing all of them is {trim.money(total)}/day, taken out of "
          f"payout or added\n  to gross. On this platform the lever is the "
          f"floor: raising a floor lifts gross,\n  lowering the revenue share "
          f"cuts payout. Both move the same number.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Realised margin after the Teqblaze fee.")
    p.add_argument("--date", action="append", default=[],
                   help="a specific day (YYYY-MM-DD). Repeatable.")
    p.add_argument("--lookback", type=int, default=1,
                   help="days back from the last settled day, when --date is "
                        "not given (default 1)")
    p.add_argument("--fee-pct", type=float, default=8.0,
                   help="Teqblaze's fee as a percentage (default 8)")
    p.add_argument("--fee-base", choices=("gross", "margin"), default="gross",
                   help="what the fee is charged on (default gross)")
    p.add_argument("--target-net", type=float, default=10.0,
                   help="net margin %% to size the adjustment against")
    p.add_argument("--min-gross", type=float, default=1.0,
                   help="ignore sources below this gross across the window")
    p.add_argument("--json", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    now = datetime.now(timezone.utc)
    last_settled = trim.latest_settled(now)

    if args.date:
        wanted = sorted(set(args.date))
    else:
        wanted = [(last_settled - timedelta(days=i)).isoformat()
                  for i in range(args.lookback - 1, -1, -1)]

    print(f"Last settled day is {last_settled} "
          f"(US/Eastern close + {trim.ET_CLOSE_LAG_HOURS}h lag).")
    partial = [d for d in wanted if d > last_settled.isoformat()]
    if partial:
        print(f"⚠ {', '.join(partial)} {'is' if len(partial) == 1 else 'are'} "
              f"NOT settled yet — reported below and marked PARTIAL.")
    print(f"Measuring {', '.join(wanted)}\n")

    per_day: dict[str, dict[str, dict]] = {}
    for day in wanted:
        sources = pull_day(day)
        if sources is not None:
            per_day[day] = sources

    if not per_day:
        print("\n::error::no day could be measured. Refusing to report.",
              file=sys.stderr)
        return 2

    lines = [day_line(d, s, args, d <= last_settled.isoformat())
             for d, s in sorted(per_day.items())]
    render_days(lines, args)

    settled_days = [d for d in per_day if d <= last_settled.isoformat()]
    use = settled_days or list(per_day)
    on_settled = bool(settled_days)
    if not on_settled:
        print("\n  ⚠ No settled day in the requested range. Everything below "
              "is computed from\n    partial days and will move as they "
              "close.")

    rows = assess(aggregate([per_day[d] for d in use]), len(use), args)
    underwater = render_sources(rows, args, len(use), on_settled)
    render_adjustment(rows, args, on_settled)

    if args.json:
        print("\n" + json.dumps({"days": lines, "sources": rows}, indent=2))
    return 1 if underwater else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
