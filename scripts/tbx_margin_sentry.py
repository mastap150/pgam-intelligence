#!/usr/bin/env python3
"""
Is each supply source realising the margin it is configured for?

A different question from `tbx_take_rate.py`, and the two are complementary:

    tbx_take_rate    has this source drifted from its OWN trailing median?
    tbx_margin_sentry is this source realising what it was CONFIGURED to?

The first is relative and catches movement. The second is absolute and
catches a source that has been quietly wrong the whole time — which the
drift sentry structurally cannot see, because a steady 7.6% under a 2–30%
band has no drift in it at all. Supply source 194 sat on a flat 2% fixed
margin for weeks and neither the take-rate sentry nor the P&L noticed.

Why it alerts on BELOW band only, by default
--------------------------------------------
The first live reading (2026-08-31, run 33394167432) found eight sources
outside their band and **all eight were above it**, never below — including
four `fixed 10%` sources landing at 14.0, 15.2, 15.3 and 15.8%. Eight
independent misconfigurations do not produce a pattern that tidy.

The likelier reading is that realised take rate, computed as
`(dsp_price_sum - ssp_price_sum) / dsp_price_sum`, includes a spread the
`margin_type` setting does not govern — a platform fee, or smart-floor spread
landing on top of the configured margin. That is a question for Teqblaze
(`docs/teqblaze-new-platform.md` §6.1), not eight settings to change.

So a sentry that fired on "outside the band" would post the same eight rows
every single day, and a channel learns to ignore that within a week. Below
band is the direction that costs money and the direction the above-band
pattern does not explain. `--include-above` opts back in once Teqblaze has
answered.

The dollar figure
-----------------
A below-band source is reported with what the gap is worth per day:

    (margin_min - realised) / 100 * gross_per_day

That is the profit not being taken at the configured floor, on today's
volume. It is an upper bound on the fix, not a promise: raising a take rate
reduces what the supply partner receives and can move their volume
elsewhere.

Read-only. No write path is imported.

Exit codes:
    0  nothing below band
    1  at least one source below its configured floor
    2  credentials absent
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

_HDR = "=" * 78


def floor_for(cfg: dict) -> float | None:
    """The configured margin floor, or None when there is not one to compare.

    `fixed` uses margin_min as its single number — margin_max comes back 0 on
    a fixed source and is not a ceiling. `range` and `adaptive` use margin_min
    as the bottom of the band. An unknown margin_type yields None rather than
    a guess: a sentry that invents a floor produces alerts nobody can act on.
    """
    kind = (cfg.get("margin_type") or "").lower()
    if kind not in ("fixed", "range", "adaptive"):
        return None
    try:
        return float(cfg.get("margin_min") or 0)
    except (TypeError, ValueError):
        return None


def ceiling_for(cfg: dict) -> float | None:
    """The configured ceiling, or None when the type does not have one."""
    kind = (cfg.get("margin_type") or "").lower()
    if kind not in ("range", "adaptive"):
        return None
    try:
        high = float(cfg.get("margin_max") or 0)
    except (TypeError, ValueError):
        return None
    return high or None


def assess(sources: list[dict], config: dict[int, dict],
           tolerance: float, min_gross_day: float) -> list[dict]:
    """One verdict per source that has both a take rate and a readable band."""
    out = []
    for src in sources:
        sid = src.get("id")
        realised = src.get("take_rate")
        gross = src.get("gross_day", 0.0)
        if sid is None or realised is None:
            continue
        # A source turning over a few dollars a day can swing many points on
        # rounding, and a sentry that reports those is mostly reporting noise.
        if gross < min_gross_day:
            continue
        cfg = config.get(sid)
        if not cfg:
            continue
        low = floor_for(cfg)
        if low is None:
            continue

        high = ceiling_for(cfg)
        gap = low - realised
        verdict = None
        if gap > tolerance:
            verdict = "BELOW"
        elif high is not None and realised - high > tolerance:
            verdict = "ABOVE"

        out.append({
            "id": sid,
            "name": src.get("name", ""),
            "realised": realised,
            "floor": low,
            "ceiling": high,
            "margin_type": cfg.get("margin_type"),
            "gross_day": gross,
            "profit_day": src.get("profit_day", 0.0),
            # Only meaningful for BELOW; kept at 0.0 otherwise so callers
            # never sum an "above band" row into a shortfall total.
            "shortfall_day": (gap / 100.0 * gross) if verdict == "BELOW" else 0.0,
            "verdict": verdict,
            "is_smart_floor": cfg.get("is_smart_floor"),
        })
    return out


def render(rows: list[dict], include_above: bool) -> tuple[list[dict], list[dict]]:
    below = [r for r in rows if r["verdict"] == "BELOW"]
    above = [r for r in rows if r["verdict"] == "ABOVE"]
    below.sort(key=lambda r: -r["shortfall_day"])
    above.sort(key=lambda r: -r["gross_day"])

    print(f"\n{_HDR}\nBELOW the configured floor\n{_HDR}")
    if not below:
        print("  none — every assessed source is at or above its configured floor")
    else:
        print(f"  {'$/day':>10} {'realised':>9} {'floor':>7} {'gap $/day':>10}  source")
        print(f"  {'-'*10:>10} {'-'*9:>9} {'-'*7:>7} {'-'*10:>10}  {'-'*34}")
        for r in below:
            print(f"  {trim.money(r['gross_day']):>10} {r['realised']:>8.1f}% "
                  f"{r['floor']:>6.0f}% {trim.money(r['shortfall_day']):>10}  "
                  f"{r['name']} #{r['id']}")
        print(f"\n  {len(below)} below floor, "
              f"{trim.money(sum(r['shortfall_day'] for r in below))}/day not taken "
              f"at the configured rate")

    print(f"\n{_HDR}\nABOVE the configured ceiling — informational\n{_HDR}")
    print(f"  {len(above)} source(s).")
    if above and include_above:
        for r in above:
            print(f"  {trim.money(r['gross_day']):>10} {r['realised']:>8.1f}% "
                  f"ceiling {r['ceiling']:>3.0f}%  {r['name']} #{r['id']}")
    if above:
        print("  Not alerted on by default: the first reading found every "
              "out-of-band source above,\n  never below, which points at the "
              "take-rate definition rather than at the settings.\n"
              "  See docs/teqblaze-new-platform.md §6.1.")
    return below, above


def to_slack(below: list[dict], above: list[dict], days: int) -> None:
    """Post only when something is below its floor.

    A daily "nothing to report" is how a channel teaches people to skip it,
    which is the same reasoning tbx_take_rate.to_slack records.
    """
    if not below:
        print("[slack] nothing below floor — not posting.")
        return

    total = sum(r["shortfall_day"] for r in below)
    head = (f"*Margin below configured floor* — {len(below)} supply source(s), "
            f"{trim.money(total)}/day not taken at the configured rate "
            f"({days}d average)")

    lines = []
    for r in below[:15]:
        smart = "  ⚠︎ smart floor on" if r.get("is_smart_floor") else ""
        lines.append(
            f"• *{r['name']}* #{r['id']} — realising {r['realised']:.1f}% "
            f"against a {r['margin_type']} floor of {r['floor']:.0f}%\n"
            f"   {trim.money(r['gross_day'])}/day gross · "
            f"gap worth {trim.money(r['shortfall_day'])}/day{smart}")
    if len(below) > 15:
        lines.append(f"_…and {len(below) - 15} more._")

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": head}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text":
            "Margin fields are *not writable over the API* — these are "
            "dashboard changes. Raising a take rate reduces the supply "
            "partner's share, so the figure is an upper bound, not a "
            "forecast."}]},
    ]
    if above:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
            f"{len(above)} source(s) are *above* their ceiling and are not "
            f"alerted on — see §6.1."}]})

    from core.slack import send_blocks
    send_blocks(blocks, text=head.replace("*", ""))
    print(f"[slack] posted {len(below)} source(s) below floor.")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Alert when a supply source realises less than its "
                    "configured margin floor.")
    p.add_argument("--days", type=int, default=7,
                   help="settled days to average over (default 7)")
    p.add_argument("--tolerance", type=float, default=1.0,
                   help="percentage points of slack before flagging "
                        "(default 1.0) — a blended average will not land "
                        "exactly on a fixed number")
    p.add_argument("--min-gross-day", type=float, default=10.0,
                   help="ignore sources below this $/day (default 10) — a "
                        "few dollars a day swings many points on rounding")
    p.add_argument("--top", type=int, default=40,
                   help="how many sources by revenue to read config for "
                        "(one GET each, default 40)")
    p.add_argument("--include-above", action="store_true",
                   help="also list sources above their ceiling. Off by "
                        "default; see the module docstring.")
    p.add_argument("--slack", action="store_true",
                   help="post to the revenue channel when something is below")
    # Passed through to trim.assess_supply, which needs them on the namespace.
    p.add_argument("--min-requests-day", type=float, default=0.0)
    p.add_argument("--min-revenue-day", type=float, default=1.0)
    p.add_argument("--hungry-multiple", type=float, default=5.0)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Margin sentry — {start} → {end} ({args.days} settled days)")

    rows, days_with_data = trim.pull_supply(start, args.days)
    if days_with_data == 0:
        print("no supply data came back — not reporting on an empty read.",
              file=sys.stderr)
        return 2
    sources, _ = trim.assess_supply(rows, days_with_data, args)

    earning = sorted((s for s in sources if s["gross_day"] >= args.min_gross_day),
                     key=lambda s: -s["gross_day"])[:args.top]
    ids = [s["id"] for s in earning if s["id"]]
    print(f"  reading margin config for {len(ids)} sources...")
    config = trim.read_margin_config(ids)

    assessed = assess(sources, config, args.tolerance, args.min_gross_day)
    print(f"  {len(assessed)} source(s) had both a take rate and a readable band")
    below, above = render(assessed, args.include_above)

    if args.slack:
        to_slack(below, above, args.days)

    return 1 if below else 0


if __name__ == "__main__":
    sys.exit(main())
