"""
hasib_month_report.py — one calendar month of AI-vs-Hasib cohort performance,
delivered to Slack.

WHY THIS EXISTS
───────────────
`msn_lane_performance.py` answers "did the 2026-07-21 optimization bundle
work?" (two arbitrary windows, printed to a terminal) and
`hasib_trigger_check.py` answers "does the step-down trigger fire?" (ISO
weeks, printed to a terminal). Neither answers the question that actually
gets asked out loud every month — *how many articles did Hasib write last
month, and how did his MSN views compare to the AI rotation's?* — and both
assume a human is sitting at a laptop with `.env` loaded.

This script answers that one question for a named month and pushes the
answer to Slack, so nobody has to open Neon, MSN Partner Hub, or a laptop
to get it. Cohort definitions are imported from `msn_lane_performance` so
the bylines cannot drift between the two reports.

WHY IT DOES NOT PRINT THE NUMBERS
─────────────────────────────────
This repository is PUBLIC, so GitHub Actions logs for it are world-readable.
Cohort page views, revenue estimates and a named contractor's per-article
performance do not belong in a public log. The default output is therefore
Slack only; stdout gets a delivery confirmation and nothing else. `--stdout`
exists for local debugging on a laptop and must not be wired into CI.

USAGE
─────
  # Local (laptop, .env present)
  export $(grep -E '^(PGAM_DIRECT_DATABASE_URL|BOXINGNEWS_DATABASE_URL|SLACK_WEBHOOK)=' \\
    ~/Desktop/pgam-intelligence/.env | xargs)
  python3 -m scripts.hasib_month_report --month 2026-08
  python3 -m scripts.hasib_month_report --month 2026-08 --stdout   # debug only

  # Cloud: .github/workflows/hasib-cohort-report.yml (secrets, not a prompt).
  # NEVER paste a connection string into a Routine prompt — list_triggers
  # echoes prompts back in full. See docs/security/ for the 2026-08-25
  # incident where exactly that happened to both of these DSNs.

CAVEATS
───────
- Peak PVs are MAX(read_count) across puller snapshots. Articles need ~5
  days on MSN to reach their peak, so a month is only trustworthy from the
  5th of the following month. The report says so inline when the window is
  too fresh, rather than leaving the reader to remember.
- Est. revenue is $4 CPM × 1.6 payout multiplier, the same proxy the
  trigger check uses. Reconcile against msn_earning_monthly once MSN posts
  the official monthly row (2-6 weeks after month end).
- "Articles" counts published rows in the boxingnews DB, which is what
  Hasib is paid per. "On MSN" counts the subset MSN actually ingested,
  and avg PV/MSN article is the metric the step-down trigger fires on.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, date, datetime, timedelta

from scripts.msn_lane_performance import (
    CPM_USD,
    Cohort,
    load_msn_peaks,
    scan_window,
)

# msn_earning_monthly has run ~60% above the $4-CPM puller estimate. Same
# multiplier hasib_trigger_check.py applies, kept in step deliberately.
PAYOUT_MULTIPLIER = 1.6

# Days an article needs on MSN before its peak read_count has settled.
PEAK_SETTLE_DAYS = 5


def month_bounds(month: str) -> tuple[date, date]:
    """'2026-08' → (2026-08-01, 2026-09-01). End is exclusive."""
    try:
        first = datetime.strptime(month, "%Y-%m").date()
    except ValueError as exc:
        raise SystemExit(f"--month must look like 2026-08, got {month!r}") from exc
    nxt = date(first.year + 1, 1, 1) if first.month == 12 else date(first.year, first.month + 1, 1)
    return first, nxt


def previous_month(month: str) -> str:
    first, _ = month_bounds(month)
    prev_end = first - timedelta(days=1)
    return f"{prev_end.year:04d}-{prev_end.month:02d}"


def default_month(today: date) -> str:
    """Last fully-settled month: the previous month, or the one before it
    if we are inside the peak-settling window at the start of a month."""
    first_of_this = today.replace(day=1)
    candidate_end = first_of_this
    if (today - candidate_end).days < PEAK_SETTLE_DAYS:
        candidate_end = (first_of_this - timedelta(days=1)).replace(day=1)
    prev_end = candidate_end - timedelta(days=1)
    return f"{prev_end.year:04d}-{prev_end.month:02d}"


def est_payout(cohort: Cohort) -> float:
    return cohort.total_pvs * CPM_USD / 1000.0 * PAYOUT_MULTIPLIER


def _delta(now: float, before: float) -> str:
    if not before:
        return "n/a" if not now else "new"
    pct = 100.0 * (now - before) / before
    return f"{pct:+.0f}%"


def format_report(
    month: str,
    cur: dict[str, Cohort],
    prev_month: str,
    prev: dict[str, Cohort],
    stale_days: int | None,
) -> str:
    ai, hasib = cur["rotation"], cur["hasib"]
    p_ai, p_hasib = prev["rotation"], prev["hasib"]

    lines: list[str] = [f"*MSN cohorts — {month}*  (AI rotation vs Hasib)", ""]

    if stale_days is not None:
        lines += [
            f":warning: Window closed {stale_days}d ago; peaks need "
            f"~{PEAK_SETTLE_DAYS}d to settle, so these numbers UNDER-count. "
            f"Re-run after {PEAK_SETTLE_DAYS - stale_days}d for the final read.",
            "",
        ]

    lines += [
        "```",
        f"{'':<22}{'AI rotation':>14}{'Hasib':>14}",
        f"{'articles published':<22}{ai.articles:>14,}{hasib.articles:>14,}",
        f"{'ingested by MSN':<22}{ai.on_msn:>14,}{hasib.on_msn:>14,}",
        f"{'ingest %':<22}{ai.ingest_pct:>13.0f}%{hasib.ingest_pct:>13.0f}%",
        f"{'total MSN views':<22}{ai.total_pvs:>14,}{hasib.total_pvs:>14,}",
        f"{'avg views / ingested':<22}{ai.avg_per_msn:>14,.0f}{hasib.avg_per_msn:>14,.0f}",
        f"{'best article':<22}{ai.max_pv:>14,}{hasib.max_pv:>14,}",
        f"{'articles >=1K views':<22}{ai.ge_1k:>14,}{hasib.ge_1k:>14,}",
        f"{'est. payout':<22}{est_payout(ai):>13,.0f}${est_payout(hasib):>13,.0f}$",
        "```",
        "",
        f"*Views:* AI {ai.total_pvs:,} vs Hasib {hasib.total_pvs:,} "
        f"— AI is {(ai.total_pvs / hasib.total_pvs):.1f}x on total views."
        if hasib.total_pvs
        else f"*Views:* AI {ai.total_pvs:,} vs Hasib 0.",
        f"*Per ingested article* (the metric the trigger fires on): "
        f"AI {ai.avg_per_msn:,.0f} vs Hasib {hasib.avg_per_msn:,.0f}.",
        "",
        f"vs {prev_month}: AI articles {_delta(ai.articles, p_ai.articles)}, "
        f"AI views {_delta(ai.total_pvs, p_ai.total_pvs)} | "
        f"Hasib articles {_delta(hasib.articles, p_hasib.articles)}, "
        f"Hasib views {_delta(hasib.total_pvs, p_hasib.total_pvs)}",
        "",
        f"_Hasib cost at $11/article: ${11 * hasib.articles:,}. "
        f"Est. payout is $4 CPM x{PAYOUT_MULTIPLIER} — reconcile against "
        f"msn_earning_monthly when MSN posts {month}. "
        f"Step-down verdict: `hasib_trigger_check.py`._",
    ]
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    today = datetime.now(UTC).date()
    ap.add_argument(
        "--month",
        default=default_month(today),
        help="Month to report, YYYY-MM (default: last settled month)",
    )
    ap.add_argument(
        "--stdout",
        action="store_true",
        help="Also print the report locally. DEBUG ONLY — this repo is public, "
             "so CI logs are world-readable. Never set this in a workflow.",
    )
    ap.add_argument(
        "--no-slack",
        action="store_true",
        help="Skip Slack delivery (implies --stdout; local debugging only).",
    )
    args = ap.parse_args()

    pgam_dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL")
    bn_dsn = os.environ.get("BOXINGNEWS_DATABASE_URL")
    missing = [
        name
        for name, val in (
            ("PGAM_DIRECT_DATABASE_URL", pgam_dsn),
            ("BOXINGNEWS_DATABASE_URL", bn_dsn),
        )
        if not val
    ]
    if missing:
        raise SystemExit(
            f"Missing required env var(s): {', '.join(missing)}\n"
            "  Local:  export $(grep -E '^(PGAM_DIRECT_DATABASE_URL|"
            "BOXINGNEWS_DATABASE_URL)=' .env | xargs)\n"
            "  Cloud:  GitHub Actions secrets, via "
            ".github/workflows/hasib-cohort-report.yml\n"
            "  Never paste a connection string into a Routine prompt — "
            "list_triggers echoes prompts back in full."
        )

    month = args.month
    prev_m = previous_month(month)
    cur_from, cur_to = month_bounds(month)
    prev_from, prev_to = month_bounds(prev_m)

    closed_days = (datetime.now(UTC).date() - cur_to).days
    stale = closed_days if 0 <= closed_days < PEAK_SETTLE_DAYS else None
    if closed_days < 0:
        print(f"[warn] {month} has not ended yet — reporting partial month.")

    peaks = load_msn_peaks(pgam_dsn)
    cur = scan_window(bn_dsn, peaks, cur_from, cur_to)
    prev = scan_window(bn_dsn, peaks, prev_from, prev_to)

    report = format_report(month, cur, prev_m, prev, stale)

    if args.stdout or args.no_slack:
        print(report)

    if args.no_slack:
        return

    from core.slack import send_text

    if not os.environ.get("SLACK_WEBHOOK"):
        raise SystemExit(
            "SLACK_WEBHOOK not set — report built but nowhere to deliver it. "
            "Set the secret, or pass --no-slack locally."
        )
    send_text(report)
    # Deliberately no numbers here: public repo, public Actions logs.
    print(f"delivered {month} cohort report to Slack ({len(report)} chars)")


if __name__ == "__main__":
    sys.exit(main())
