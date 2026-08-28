#!/usr/bin/env python3
"""
Which supply sources changed their take rate, and when?

Margin fell from ~30.6% on the legacy host to 20–24% on TBX and stayed there
(`docs/tb-data-workflow-integration.md` §12). It accounts for 22% of the
profit drop — ~$363/day — and it is the component that does NOT behave like
market conditions: 2026-08-22 had the best CPM of the period and the second
worst margin. A take rate that moves independently of what inventory sells
for points at revenue-share configuration.

Whole-book margin cannot say whether that is every publisher giving up a
little or a few giving up a lot, and those need completely different
responses — one is a platform default, the other is a handful of settings.
This splits it per supply source.

What it flags
-------------
A source is FLAGGED when its margin on the latest settled day deviates from
its own trailing median by more than `--threshold` percentage points. Its
own median, not the book's: a source that has always run at 12% is not a
problem, and one that ran at 35% for a fortnight and is now at 25% is,
even though 25% is above the book average. Comparing everything to one
number would report the first and miss the second.

Percentage POINTS, not percent. A margin going 30% → 24% is a 6-point drop
and a 20% relative one; the point difference is what the money follows and
what an operator can reason about.

Why not standard deviations
---------------------------
A source with a very steady margin has a tiny σ, so a 1-point wobble reads
as a 5σ event and the report fills with noise from the best-behaved
publishers. Points are the unit the question is actually asked in.

Guards against the two ways this report lies
--------------------------------------------
1. **Thin days.** A source with $3 of revenue can swing 40 points on
   rounding. `--min-revenue` drops days below a floor from both the median
   and the comparison, rather than letting them set a baseline nothing can
   match.
2. **Unsettled days.** The latest day is only used once its US/Eastern close
   has passed — a part-day's margin is not the day's margin. Same trap §12
   records against 2026-08-24, where an early read was 24% low.

Read-only: `SET TRANSACTION READ ONLY`, no platform call.

Exit codes:
    0  nothing flagged
    1  at least one source moved (so a scheduled run is visibly red)
    2  could not run

Usage:
    python3 scripts/tbx_take_rate.py
    python3 scripts/tbx_take_rate.py --lookback 14 --threshold 3
    python3 scripts/tbx_take_rate.py --date 2026-08-24 --json
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

TABLE = "tbx_daily_supply_revenue"

# US/Eastern is what TBX_TIMEZONE is set to everywhere in this repo, so a day
# closes at 04:00 or 05:00 UTC depending on DST. Five hours is the safe side
# of both.
ET_CLOSE_LAG_HOURS = 5

_HDR = "=" * 78


def _q(conn, sql: str, params=None) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def margin(gross: float, payout: float) -> float | None:
    """Take rate as a percentage of gross. None when there is no gross."""
    if not gross:
        return None
    return (gross - payout) / gross * 100.0


def latest_settled(now_utc: datetime) -> date:
    """The most recent day whose US/Eastern close has passed."""
    return (now_utc - timedelta(hours=ET_CLOSE_LAG_HOURS)).date() - timedelta(days=1)


def assess(series: dict[str, list[tuple[date, float, float]]], target: date,
           threshold: float, min_revenue: float) -> list[dict]:
    """Per source: margin on `target` against its own trailing median.

    `series` is {name: [(day, gross, payout)]} covering the lookback AND the
    target day. Days below `min_revenue` are dropped entirely — they cannot
    set a baseline and they cannot trip one.
    """
    out = []
    for name, rows in series.items():
        usable = [(d, g, p) for d, g, p in rows if g >= min_revenue]
        today = [(d, g, p) for d, g, p in usable if d == target]
        history = [(d, g, p) for d, g, p in usable if d < target]
        if not today or len(history) < 3:
            # Fewer than three comparable days is not a trend, it is a guess.
            continue

        _, g_now, p_now = today[0]
        m_now = margin(g_now, p_now)
        hist_margins = [m for m in (margin(g, p) for _, g, p in history)
                        if m is not None]
        if m_now is None or not hist_margins:
            continue

        med = statistics.median(hist_margins)
        delta = m_now - med
        out.append({
            "name": name,
            "margin_now": m_now,
            "margin_median": med,
            "delta_points": delta,
            "gross_now": g_now,
            "payout_now": p_now,
            "history_days": len(hist_margins),
            # What the deviation is worth per day at today's gross. A 10-point
            # slip on $40 is a rounding artifact; on $4,000 it is the finding.
            "profit_delta_usd": g_now * delta / 100.0,
            "flagged": abs(delta) >= threshold,
        })
    out.sort(key=lambda r: r["profit_delta_usd"])
    return out


def across_cutover(conn, cutover: date, days: int,
                   min_revenue: float) -> list[dict]:
    """Each source's margin before the cutover against after it.

    This is the comparison the sentry above structurally cannot make. It
    compares TBX to TBX, so it can only see drift once a TBX baseline exists
    — and the ten points §12 is about went AT the cutover, where the baseline
    is on the other host. Per-source margins have been stable since; the
    step is in the step.

    Joined on the suffix-stripped name, for the reason recorded in
    `tbx_supply_gap`: the legacy report appends the entity id to the display
    name ('Foo #190') and stores that whole string in its id column too, so
    the bare name is the only workable key.
    """
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from tbx_supply_gap import split_name_id

    before = [cutover - timedelta(days=n) for n in range(days, 0, -1)]
    after = [cutover + timedelta(days=n) for n in range(days)]

    def side(table, name_col, window):
        agg: dict[str, list[float]] = {}
        for name, gross, payout in _q(conn, f"""
            SELECT {name_col}, sum(gross_revenue), sum(pub_payout)
              FROM pgam_direct.{table}
             WHERE report_date = ANY(%(d)s) AND {name_col} IS NOT NULL
             GROUP BY 1
        """, {"d": window}):
            key, _ = split_name_id(str(name))
            e = agg.setdefault(key, [0.0, 0.0, str(name)])
            e[0] += float(gross or 0)
            e[1] += float(payout or 0)
        return agg

    pre = side("tb_daily_publisher_revenue", "publisher_name", before)
    post = side(TABLE, "supply_name", after)

    out = []
    for key, (g_pre, p_pre, disp) in pre.items():
        if key not in post or g_pre < min_revenue:
            continue
        g_post, p_post, _ = post[key]
        if g_post < min_revenue:
            continue
        m_pre, m_post = margin(g_pre, p_pre), margin(g_post, p_post)
        if m_pre is None or m_post is None:
            continue
        delta = m_post - m_pre
        out.append({
            "name": disp, "margin_before": m_pre, "margin_after": m_post,
            "delta_points": delta,
            "gross_after_per_day": g_post / len(after),
            # What the margin change alone costs per day, holding the
            # post-cutover gross fixed. Isolates the take rate from the
            # volume and price effects §12 measures separately.
            "profit_delta_usd": (g_post / len(after)) * delta / 100.0,
        })
    out.sort(key=lambda r: r["profit_delta_usd"])
    return out


def render_cutover(rows: list[dict], cutover: date, days: int) -> None:
    print(_HDR)
    print(f"TAKE RATE ACROSS THE CUTOVER — {days}d either side of {cutover}")
    print(_HDR)
    print("  legacy margin vs TBX margin, same source, joined on name\n")
    if not rows:
        print("  No source clears the revenue floor on both sides.")
        return
    print(f"  {'supply source':<34} {'before':>8} {'after':>8} "
          f"{'Δ pts':>8} {'$/day':>10}")
    print(f"  {'-' * 34} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 10}")
    for r in rows:
        print(f"  {str(r['name'])[:34]:<34} "
              f"{r['margin_before']:>7.1f}% {r['margin_after']:>7.1f}% "
              f"{r['delta_points']:>+8.1f} {r['profit_delta_usd']:>+10.2f}")
    total = sum(r["profit_delta_usd"] for r in rows)
    print(f"\n  margin effect across matched sources: ${total:>+10,.2f}/day")
    print(_HDR)


def render(rows: list[dict], target: date, lookback: int,
           threshold: float) -> None:
    flagged = [r for r in rows if r["flagged"]]
    print(_HDR)
    print(f"TAKE RATE — {target}, against each source's own "
          f"{lookback}-day median")
    print(_HDR)
    print(f"  flagged past ±{threshold:.0f} percentage points   "
          f"{len(rows)} source(s) with enough history\n")

    if not flagged:
        print("  Nothing moved. Every source is within "
              f"{threshold:.0f} points of its own median.\n")
    else:
        print(f"  {'supply source':<34} {'median':>8} {'now':>8} "
              f"{'Δ pts':>8} {'$/day':>10}")
        print(f"  {'-' * 34} {'-' * 8} {'-' * 8} {'-' * 8} {'-' * 10}")
        for r in flagged:
            print(f"  {str(r['name'])[:34]:<34} "
                  f"{r['margin_median']:>7.1f}% {r['margin_now']:>7.1f}% "
                  f"{r['delta_points']:>+8.1f} {r['profit_delta_usd']:>+10.2f}")

        worse = sum(r["profit_delta_usd"] for r in flagged
                    if r["profit_delta_usd"] < 0)
        better = sum(r["profit_delta_usd"] for r in flagged
                     if r["profit_delta_usd"] > 0)
        print()
        print(f"  margin lost to the sources that fell : ${worse:>10,.2f}/day")
        print(f"  margin gained by those that rose     : ${better:>+10,.2f}/day")
        print(f"  net                                  : "
              f"${worse + better:>+10,.2f}/day")

    book_now = sum(r["gross_now"] for r in rows)
    book_profit = sum(r["gross_now"] - r["payout_now"] for r in rows)
    if book_now:
        # NOT the whole book: only sources that cleared the revenue floor and
        # had enough history to assess. Saying "whole book" here would invite
        # a comparison against the day's real gross and quietly lose the
        # difference.
        print(f"\n  assessed sources on {target}: "
              f"{book_profit / book_now * 100:.1f}% on ${book_now:,.2f} gross "
              f"({len(rows)} of the book)")
    print(_HDR)
    if flagged:
        print("\n  A take rate that moves while price and volume do not is a")
        print("  revenue-share setting, not the market. Check these sources'")
        print("  publisher revenue-share configuration on api.pgammedia.com.")


def to_slack(rows: list[dict], target: date, threshold: float,
             mode: str, cutover: str | None) -> None:
    """Post the finding to the revenue channel.

    This report has run daily since 2026-08-25 and written to a workflow log
    nobody opens, while the take rate it measures became the largest single
    component of the profit gap (§12/§13). A number that costs ~$744/day
    belongs where the revenue alert already goes.

    Posts only when something is flagged. A daily "nothing moved" is how a
    channel teaches people to ignore it.
    """
    flagged = [r for r in rows if r.get("flagged") or
               (mode == "cutover" and r["delta_points"] <= -threshold)]
    if not flagged:
        print("[slack] nothing flagged — not posting.")
        return

    worse = [r for r in flagged if r["profit_delta_usd"] < 0]
    total = sum(r["profit_delta_usd"] for r in worse)

    if mode == "cutover":
        head = (f"*Take rate since the {cutover} cutover* — "
                f"{len(worse)} source(s) below their legacy rate")
        col = "before → after"
    else:
        head = (f"*Take rate moved on {target}* — {len(flagged)} source(s) "
                f"past ±{threshold:.0f} points vs their own median")
        col = "median → now"

    lines = [f"{'source':<32} {col:>16} {'Δ pts':>7} {'$/day':>9}",
             "-" * 68]
    for r in sorted(flagged, key=lambda r: r["profit_delta_usd"])[:12]:
        a = r.get("margin_before", r.get("margin_median"))
        b = r.get("margin_after", r.get("margin_now"))
        lines.append(f"{str(r['name'])[:32]:<32} "
                     f"{a:>7.1f}% →{b:>6.1f}% "
                     f"{r['delta_points']:>+7.1f} "
                     f"{r['profit_delta_usd']:>+9.2f}")

    blocks = [
        {"type": "section",
         "text": {"type": "mrkdwn", "text": head}},
        {"type": "section",
         "text": {"type": "mrkdwn", "text": "```\n" + "\n".join(lines) + "\n```"}},
        {"type": "context",
         "elements": [{"type": "mrkdwn",
                       "text": f"*{total:+,.2f}/day* from the sources that fell. "
                               f"A take rate that moves while price and volume do not "
                               f"is a revenue-share setting — check these on "
                               f"api.pgammedia.com."}]},
    ]

    from core.slack import send_blocks
    send_blocks(blocks, text=head.replace("*", ""))
    print(f"[slack] posted {len(flagged)} flagged source(s).")


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lookback", type=int, default=14,
                   help="trailing days for each source's median (default 14)")
    p.add_argument("--threshold", type=float, default=3.0,
                   help="percentage-point deviation that flags (default 3)")
    p.add_argument("--min-revenue", type=float, default=25.0,
                   help="ignore days below this gross (default 25)")
    p.add_argument("--date", help="settled day to assess (default: latest)")
    p.add_argument("--vs-legacy", metavar="CUTOVER",
                   help="instead of drift, compare each source's margin "
                        "before and after this cutover date — the step the "
                        "trailing-median mode structurally cannot see")
    p.add_argument("--slack", action="store_true",
                   help="post the finding to SLACK_WEBHOOK (only when "
                        "something is flagged)")
    p.add_argument("--json", action="store_true")
    args = p.parse_args()

    dsn = (os.environ.get("PGAM_DIRECT_DATABASE_URL")
           or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        print("Set PGAM_DIRECT_DATABASE_URL (or DATABASE_URL).", file=sys.stderr)
        return 2

    if args.date:
        try:
            target = date.fromisoformat(args.date)
        except ValueError:
            print(f"--date {args.date!r} is not YYYY-MM-DD", file=sys.stderr)
            return 2
    else:
        target = latest_settled(datetime.utcnow())

    days = [target - timedelta(days=n) for n in range(args.lookback, -1, -1)]

    import psycopg2
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")

        if args.vs_legacy:
            try:
                cut = date.fromisoformat(args.vs_legacy)
            except ValueError:
                print(f"--vs-legacy {args.vs_legacy!r} is not YYYY-MM-DD",
                      file=sys.stderr)
                return 2
            xrows = across_cutover(conn, cut, 4, args.min_revenue)
            if args.json:
                print(json.dumps(xrows, indent=2, default=str))
            else:
                render_cutover(xrows, cut, 4)
            if args.slack:
                to_slack(xrows, cut, args.threshold, "cutover", args.vs_legacy)
            return 1 if any(r["delta_points"] <= -args.threshold
                            for r in xrows) else 0

        rows = _q(conn, f"""
            SELECT supply_name, report_date,
                   sum(gross_revenue), sum(pub_payout)
              FROM pgam_direct.{TABLE}
             WHERE report_date = ANY(%(days)s) AND supply_name IS NOT NULL
             GROUP BY 1, 2
        """, {"days": days})
    finally:
        conn.close()

    series: dict[str, list[tuple[date, float, float]]] = {}
    for name, d, gross, payout in rows:
        series.setdefault(str(name), []).append(
            (d, float(gross or 0), float(payout or 0)))

    assessed = assess(series, target, args.threshold, args.min_revenue)

    if args.json:
        print(json.dumps({"date": target.isoformat(),
                          "threshold_points": args.threshold,
                          "sources": assessed}, indent=2, default=str))
    else:
        render(assessed, target, args.lookback, args.threshold)

    if args.slack:
        to_slack(assessed, target, args.threshold, "drift", None)

    return 1 if any(r["flagged"] for r in assessed) else 0


if __name__ == "__main__":
    raise SystemExit(main())
