"""
hasib_trigger_check.py — Weekly monitor for the Hasib-reduction trigger.

BACKGROUND
──────────
Priyesh pays Hasib $11/article × 5/day (~$1,650/mo). AI content already
outputs ~8x his volume and now beats him on TOTAL MSN views. The plan:
keep him at 5/day until the AI side clears defensible per-article
thresholds, then step him down.

Trigger rule (agreed 2026-07-27):
  KEEP_5     — default
  CUT_TO_3   — AI avg reads/ingested ≥ 90 for 2 consecutive weeks
                AND AI weekly MSN est. revenue ≥ $350
  CUT_TO_2   — AI avg reads/ingested ≥ 120 for 2 consecutive weeks

WHAT THIS PRINTS
────────────────
The last N ISO weeks of cohort performance:
  week | ai_articles ai_on_msn ai_avg_pv ai_est_rev | hasib_articles hasib_avg_pv hasib_est_rev

Then evaluates the last 2 COMPLETED weeks against the trigger thresholds
and prints a single-line verdict.

USAGE
─────
Two connection strings are required, and they are read from the
environment — never passed as arguments, and never written into a
command line, a routine prompt, or this file.

  Local (Priyesh's machine), from the gitignored .env:

    export $(grep -E '^(PGAM_DIRECT_DATABASE_URL|BOXINGNEWS_DATABASE_URL)=' \\
      ~/Desktop/pgam-intelligence/.env | xargs)
    python3 scripts/hasib_trigger_check.py            # last 6 weeks
    python3 scripts/hasib_trigger_check.py --weeks 12 # deeper history

  Cloud session / weekly Routine:

    Both variables are injected by the environment at session start.
    Just run the script — there is nothing to export.

      python3 scripts/hasib_trigger_check.py --weeks 6

WHY THAT DISTINCTION IS SPELLED OUT
───────────────────────────────────
This block used to give only the local recipe, which reads a path that
exists on exactly one laptop. A cloud Routine cannot satisfy it, so
whoever set up the weekly monitor did the thing the instructions left
them no alternative to: pasted both live connection strings, passwords
included, into the Routine's prompt. Routine prompts are stored
server-side and echoed back in full by `list_triggers`, so both
credentials sat readable in the control plane from 2026-07-27 until it
was caught on 2026-08-25.

See docs/security/2026-08-25-routine-credential-exposure.md. The fix
is not only to remove them — a stored secret has to be rotated to be
gone — and this script wants a **read-only** role, since it issues two
SELECTs and writes nothing.

CAVEATS
───────
- Peak PVs are MAX(read_count) across our puller snapshots. Articles
  <5 days old under-count; the trigger check should be run at least 5
  days after a week closes for the numbers to be trustworthy.
- $4 base CPM × 1.6 real-payout multiplier is a proxy for MSN cash;
  reconcile against msn_earning_monthly when official payouts post.
- The trigger does NOT auto-execute — it's a monitor. Cutting Hasib's
  contract is a manual step Priyesh takes.
"""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlparse

import psycopg

ROTATION_BYLINES = {
    "Tom Rashid",
    "Aaron Clarke",
    "Dan O'Keefe",
    "James Wright",
    "Priya Shah",
    "Sarah Mitchell",
    "Editorial",
}
HASIB_BYLINES = {
    "Boxing News Staff",
    "MMA News Staff",
    " MMA News Staff",
    "MMA News",
}

CPM_USD = 4.0
PAYOUT_MULTIPLIER = 1.6  # msn_earning_monthly runs ~60% above puller estimate

CUT_TO_3_AVG_PV = 90
CUT_TO_3_MIN_REV = 350.0
CUT_TO_2_AVG_PV = 120


@dataclass
class WeekBucket:
    label: str
    frm: date
    to: date
    ai_articles: int = 0
    ai_on_msn: int = 0
    ai_pvs: int = 0
    hasib_articles: int = 0
    hasib_on_msn: int = 0
    hasib_pvs: int = 0
    ai_peaks: list[int] = field(default_factory=list)
    hasib_peaks: list[int] = field(default_factory=list)

    @property
    def ai_avg_pv(self) -> float:
        return (sum(self.ai_peaks) / len(self.ai_peaks)) if self.ai_peaks else 0.0

    @property
    def hasib_avg_pv(self) -> float:
        return (sum(self.hasib_peaks) / len(self.hasib_peaks)) if self.hasib_peaks else 0.0

    @property
    def ai_est_rev(self) -> float:
        return self.ai_pvs * (CPM_USD / 1000.0) * PAYOUT_MULTIPLIER

    @property
    def hasib_est_rev(self) -> float:
        return self.hasib_pvs * (CPM_USD / 1000.0) * PAYOUT_MULTIPLIER


def slug_of(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else None


def load_msn_peaks(pgam_dsn: str) -> dict[str, int]:
    peaks: dict[str, int] = {}
    with psycopg.connect(pgam_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.canonical_url, p.peak_read_count
              FROM pgam_direct.msn_article_peak p
              JOIN pgam_direct.msn_article_meta m USING (doc_id)
             WHERE m.canonical_url IS NOT NULL
            """
        )
        for url, peak in cur.fetchall():
            slug = slug_of(url)
            if slug:
                peaks[slug] = max(peaks.get(slug, 0), int(peak or 0))
    return peaks


def cohort_for(byline: str) -> str | None:
    if byline in ROTATION_BYLINES:
        return "ai"
    if byline in HASIB_BYLINES or byline.strip().lower() == "hasib":
        return "hasib"
    return None


def scan_weeks(
    bn_dsn: str,
    peaks_by_slug: dict[str, int],
    weeks: list[tuple[date, date]],
) -> list[WeekBucket]:
    """weeks: list of (frm, to_exclusive) date pairs, oldest first."""
    buckets = [WeekBucket(label=f"{f.isoformat()} → {(t - timedelta(days=1)).isoformat()}",
                           frm=f, to=t) for f, t in weeks]

    with psycopg.connect(bn_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT slug,
                   COALESCE(NULLIF(TRIM(author_name),''),'') AS author,
                   published_at::date AS pub_date
              FROM articles
             WHERE COALESCE(status,'published') = 'published'
               AND published_at >= %s
               AND published_at <  %s
            """,
            (weeks[0][0], weeks[-1][1]),
        )
        for slug, author, pub_date in cur.fetchall():
            coh = cohort_for(author)
            if not coh:
                continue
            for b in buckets:
                if b.frm <= pub_date < b.to:
                    if coh == "ai":
                        b.ai_articles += 1
                    else:
                        b.hasib_articles += 1
                    peak = peaks_by_slug.get(slug)
                    if peak is not None and peak > 0:
                        if coh == "ai":
                            b.ai_on_msn += 1
                            b.ai_pvs += peak
                            b.ai_peaks.append(peak)
                        else:
                            b.hasib_on_msn += 1
                            b.hasib_pvs += peak
                            b.hasib_peaks.append(peak)
                    break
    return buckets


def iso_week_windows(weeks_back: int, ref: date | None = None) -> list[tuple[date, date]]:
    """Return `weeks_back` ISO-week windows ending with the most recently
    COMPLETED week (Mon-Sun), oldest first. Excludes the in-progress week."""
    today = ref or datetime.now(UTC).date()
    # Monday of the current in-progress week
    this_mon = today - timedelta(days=today.weekday())
    windows: list[tuple[date, date]] = []
    for k in range(weeks_back, 0, -1):
        frm = this_mon - timedelta(days=7 * k)
        to = frm + timedelta(days=7)
        windows.append((frm, to))
    return windows


def evaluate_trigger(buckets: list[WeekBucket]) -> tuple[str, str]:
    """Look at the last 2 completed weeks. Returns (verdict, reason)."""
    if len(buckets) < 2:
        return "KEEP_5", "not enough weekly history yet"
    last_two = buckets[-2:]
    ai_avgs = [b.ai_avg_pv for b in last_two]
    ai_revs = [b.ai_est_rev for b in last_two]

    if all(a >= CUT_TO_2_AVG_PV for a in ai_avgs):
        return "CUT_TO_2", (
            f"AI avg reads/ingested ≥ {CUT_TO_2_AVG_PV} for 2 consecutive weeks "
            f"({ai_avgs[0]:.0f}, {ai_avgs[1]:.0f})"
        )
    if all(a >= CUT_TO_3_AVG_PV for a in ai_avgs) and all(r >= CUT_TO_3_MIN_REV for r in ai_revs):
        return "CUT_TO_3", (
            f"AI avg reads/ingested ≥ {CUT_TO_3_AVG_PV} for 2 consecutive weeks "
            f"({ai_avgs[0]:.0f}, {ai_avgs[1]:.0f}) and AI weekly est. rev ≥ ${CUT_TO_3_MIN_REV:.0f} "
            f"(${ai_revs[0]:.0f}, ${ai_revs[1]:.0f})"
        )
    # Not triggered — explain the gap
    return "KEEP_5", (
        f"last 2 weeks AI avg reads/ingested = ({ai_avgs[0]:.0f}, {ai_avgs[1]:.0f}), "
        f"AI est. rev = (${ai_revs[0]:.0f}, ${ai_revs[1]:.0f}). "
        f"Need ≥ {CUT_TO_3_AVG_PV} & ≥ ${CUT_TO_3_MIN_REV:.0f} for CUT_TO_3."
    )


def print_table(buckets: list[WeekBucket]) -> None:
    hdr = (
        f'{"week":<27} '
        f'{"ai_arts":>8} {"ai_msn":>7} {"ai_avg":>7} {"ai_rev":>9}   '
        f'{"hasib_arts":>10} {"hasib_msn":>10} {"hasib_avg":>10} {"hasib_rev":>10}'
    )
    print(hdr)
    print("-" * len(hdr))
    for b in buckets:
        print(
            f'{b.label:<27} '
            f'{b.ai_articles:>8} {b.ai_on_msn:>7} {b.ai_avg_pv:>7.0f} ${b.ai_est_rev:>7.0f}   '
            f'{b.hasib_articles:>10} {b.hasib_on_msn:>10} {b.hasib_avg_pv:>10.0f} ${b.hasib_est_rev:>8.0f}'
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--weeks", type=int, default=6, help="Number of completed ISO weeks to show (default 6)")
    args = ap.parse_args()

    pgam_dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL")
    bn_dsn = os.environ.get("BOXINGNEWS_DATABASE_URL")
    if not pgam_dsn or not bn_dsn:
        missing = [
            name
            for name, value in (
                ("PGAM_DIRECT_DATABASE_URL", pgam_dsn),
                ("BOXINGNEWS_DATABASE_URL", bn_dsn),
            )
            if not value
        ]
        # Name both paths. An error that only describes one machine's
        # .env file is what pushed the last person to inline the
        # credentials somewhere they could be read — see the module
        # docstring.
        raise SystemExit(
            f"Missing required env var(s): {', '.join(missing)}\n"
            "\n"
            "  Local:  export $(grep -E "
            "'^(PGAM_DIRECT_DATABASE_URL|BOXINGNEWS_DATABASE_URL)=' \\\n"
            "            ~/Desktop/pgam-intelligence/.env | xargs)\n"
            "\n"
            "  Cloud:  set them on the environment (claude.ai/code -> environment\n"
            "          selector -> gear -> Environment variables), then start a NEW\n"
            "          session; values are copied once at startup.\n"
            "\n"
            "Never paste a connection string into a Routine prompt or a shell\n"
            "command. Routine prompts are stored server-side and echoed back by\n"
            "list_triggers. See docs/security/2026-08-25-routine-credential-exposure.md"
        )

    peaks = load_msn_peaks(pgam_dsn)
    windows = iso_week_windows(args.weeks)
    buckets = scan_weeks(bn_dsn, peaks, windows)

    print_table(buckets)
    verdict, reason = evaluate_trigger(buckets)
    print()
    print(f"TRIGGER: {verdict}")
    print(f"  {reason}")
    print()
    print(
        "Rule: CUT_TO_3 when AI avg PV ≥ 90 for 2 consecutive weeks AND AI est. rev ≥ $350/wk. "
        "CUT_TO_2 at ≥ 120."
    )


if __name__ == "__main__":
    main()
