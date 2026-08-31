#!/usr/bin/env python3
"""
Do the zero-win demand endpoints ever win?

A narrow watch on one open question. The 2026-08-31 audit found nine Illumin
RON demand endpoints receiving **872 million bid requests a day between them
with zero wins and zero spend** across a seven-day window. That is the largest
single cut available, and it is blocked on an ambiguity the numbers alone
cannot settle:

  * If they are genuinely dormant, switching them off costs nothing.
  * If they are *meant* to be bidding, the fault is upstream — in their
    endpoint configuration, a seat mapping, a QPS cap — and switching them
    off buries the bug instead of fixing it.

Their names are the reason to doubt the first reading: `RON`, `RON copy1`,
`RON copy2`, `Endpoint3 - RON`, `Endpoint3 - RON copy1`. That plus identical
zero-win behaviour reads like one duplicated configuration, not nine partners
independently going quiet.

So this watches rather than guesses. A single win from any of them is a state
change worth knowing about the same day: it proves the endpoint is live and
moves the question from "cut them" to "why is only one working". A long enough
run of zero settles the cut on evidence.

It reports per-day rather than as a window total, because "0 wins over 14
days" and "0 wins for 13 days then 40,000 yesterday" have the same total and
opposite meanings.

Read-only. No write path is imported.

Exit codes:
    0  every watched endpoint is still at zero wins
    1  at least one won — investigate before cutting
    2  credentials absent, or none of the watched ids appeared at all
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

_HDR = "=" * 78

# The cluster the audit surfaced. Kept here rather than in a config file so
# the reason travels with the list; `--ids` overrides it entirely.
DEFAULT_WATCH: dict[int, str] = {
    1549: "Illumin Endpoint3 - RON",
    2179: "Illumin - RON copy1",
    2178: "Illumin Endpoint3 - RON copy1",
    1553: "Illumin - RON",
    2311: "Illumin - RON copy2",
    1826: "Illumin Endpoint3 - RON Adapex",
    1830: "Illumin - RON Adapex",
    959:  "Illumin - Adapex Display",
    831:  "Illumin - Aditude Display",
}

WATCH_METRICS = ["requests_sum", "responses_sum", "wins_sum", "imps_sum",
                 "dsp_price_sum"]


def pull_per_day(start: date, days: int,
                 watch: set[int]) -> tuple[dict[int, list[dict]], list[str]]:
    """One request per day; per-day rows for the watched ids only.

    Single-day requests for the same reason every other TBX reader uses them:
    a multi-day window comes back truncated to roughly the most recent five
    days with no error (§5.10). Here that would silently shorten the very
    streak the report exists to measure.
    """
    series: dict[int, list[dict]] = {eid: [] for eid in watch}
    missing_days: list[str] = []

    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        rows, _ = tbx.report(day, day, attributes=["date", "demand_source"],
                             metrics=WATCH_METRICS)
        seen = 0
        for row in rows:
            if str(row.get("date") or "")[:10] != day:
                continue
            _, did = trim.split_name_id(row.get("demand_source", ""))
            if did not in watch:
                continue
            series[did].append({
                "day": day,
                "requests": trim.num(row, "requests_sum"),
                "wins": trim.num(row, "wins_sum"),
                "imps": trim.num(row, "imps_sum"),
                "spend": trim.num(row, "dsp_price_sum"),
            })
            seen += 1
        print(f"    {day}: {seen} watched endpoint(s) present", flush=True)
        if seen == 0:
            missing_days.append(day)
    return series, missing_days


def assess(series: dict[int, list[dict]], names: dict[int, str]) -> list[dict]:
    out = []
    for eid, rows in series.items():
        wins = sum(r["wins"] for r in rows)
        requests = sum(r["requests"] for r in rows)
        spend = sum(r["spend"] for r in rows)
        winning_days = [r["day"] for r in rows if r["wins"] > 0]
        out.append({
            "id": eid,
            "name": names.get(eid, f"#{eid}"),
            "days_present": len(rows),
            "requests": requests,
            "wins": wins,
            "spend": spend,
            "winning_days": winning_days,
            "woke": bool(winning_days),
        })
    out.sort(key=lambda e: (-e["wins"], -e["requests"]))
    return out


def render(assessed: list[dict], days: int, missing_days: list[str]) -> list[dict]:
    woke = [a for a in assessed if a["woke"]]
    absent = [a for a in assessed if a["days_present"] == 0]

    print(f"\n{_HDR}\nWatched endpoints over {days} settled days\n{_HDR}")
    print(f"  {'wins':>8} {'requests':>16} {'$ spend':>10} {'days':>5}  endpoint")
    print(f"  {'-'*8:>8} {'-'*16:>16} {'-'*10:>10} {'-'*5:>5}  {'-'*34}")
    for a in assessed:
        print(f"  {a['wins']:>8,.0f} {a['requests']:>16,.0f} "
              f"{trim.money(a['spend']):>10} {a['days_present']:>5}  "
              f"{a['name']} #{a['id']}")

    if absent:
        # Not the same as zero wins: the platform drops all-zero rows, so an
        # endpoint absent from every day may have been switched off already.
        print(f"\n  {len(absent)} endpoint(s) returned no rows at all — they may "
              f"already be off:")
        for a in absent:
            print(f"    · {a['name']} #{a['id']}")

    if missing_days:
        print(f"\n  {len(missing_days)} day(s) returned no watched endpoints: "
              f"{', '.join(missing_days)}")

    if woke:
        print(f"\n  ⚠︎  {len(woke)} endpoint(s) WON during the window — "
              f"do not cut these until it is understood:")
        for a in woke:
            print(f"    · {a['name']} #{a['id']}: {a['wins']:,.0f} wins on "
                  f"{', '.join(a['winning_days'])}, {trim.money(a['spend'])} spend")
    else:
        active = [a for a in assessed if a["days_present"] > 0]
        print(f"\n  ✓  zero wins across all {len(active)} active endpoint(s) for "
              f"{days} consecutive settled days.")
        print(f"     Combined: {sum(a['requests'] for a in active):,.0f} requests "
              f"for {trim.money(sum(a['spend'] for a in active))}.")
        print(f"     The longer this holds, the better evidenced the cut.")
    return woke


def to_slack(woke: list[dict], assessed: list[dict], days: int) -> None:
    """Post only on a state change — an endpoint that won.

    The steady state here is silence for weeks. A daily "still zero" would
    train the channel to skip the one message that matters.
    """
    if not woke:
        print("[slack] still zero wins — not posting.")
        return

    head = (f"*Zero-win demand endpoint started bidding* — {len(woke)} of "
            f"{len(assessed)} watched endpoint(s) won in the last {days} days")
    lines = [
        f"• *{a['name']}* #{a['id']} — {a['wins']:,.0f} wins, "
        f"{trim.money(a['spend'])} spend, on {', '.join(a['winning_days'])}"
        for a in woke
    ]
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": head}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            "These were on the cut list as dormant. A win means the endpoint "
            "is live — treat the rest of the cluster as a configuration "
            "question, not a dead partner, and do not cut it."}]},
    ]
    from core.slack import send_blocks
    send_blocks(blocks, text=head.replace("*", ""))
    print(f"[slack] posted {len(woke)} endpoint(s) that woke up.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Watch demand endpoints that have never won an auction.")
    p.add_argument("--days", type=int, default=14,
                   help="settled days to check, one request each (default 14)")
    p.add_argument("--ids", default="",
                   help="comma-separated demand source ids, replacing the "
                        "built-in Illumin RON cluster")
    p.add_argument("--slack", action="store_true",
                   help="post when a watched endpoint wins")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    if args.ids:
        watch_ids = {int(x) for x in args.ids.replace(",", " ").split()}
        names = {eid: f"demand #{eid}" for eid in watch_ids}
    else:
        watch_ids = set(DEFAULT_WATCH)
        names = dict(DEFAULT_WATCH)

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"No-win watch — {start} → {end} ({args.days} settled days), "
          f"{len(watch_ids)} endpoint(s)\n")

    series, missing_days = pull_per_day(start, args.days, watch_ids)
    assessed = assess(series, names)

    if all(a["days_present"] == 0 for a in assessed):
        print("\nNone of the watched ids appeared on any day. Either they are "
              "already disabled or the ids are wrong — not reporting a clean "
              "streak from an empty read.", file=sys.stderr)
        return 2

    woke = render(assessed, args.days, missing_days)
    if args.slack:
        to_slack(woke, assessed, args.days)
    return 1 if woke else 0


if __name__ == "__main__":
    sys.exit(main())
