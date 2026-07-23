#!/usr/bin/env python3
"""
scripts/msn_lane_performance.py

Compare MSN performance for the boxingnews rotation lane across two date
windows. Meant for measuring the impact of the 2026-07-21 optimization
bundle (formulaic-cron kill + strategy-bias wiring + 06:00-07:30 UTC
publish concentration + daily cap).

USAGE
─────
  # Default: compare 30 days before deploy vs deploy → now
  python3 scripts/msn_lane_performance.py

  # Custom windows (all args optional; defaults derived from --deploy)
  python3 scripts/msn_lane_performance.py \\
      --pre-from 2026-06-21 --pre-to 2026-07-21 \\
      --post-from 2026-07-21 --post-to 2026-08-04

  # Move the deploy anchor if you re-baseline
  python3 scripts/msn_lane_performance.py --deploy 2026-07-21

WHAT IT MEASURES
────────────────
Two windows, three cohorts each:
  1. Rotation lane   — Tom Rashid / Aaron Clarke / Dan O'Keefe /
                        James Wright / Priya Shah / Sarah Mitchell / Editorial
                       (These are AI-content bylines assigned by
                        pickAuthorForContent in the boxingnews repo.)
  2. Hasib (Sanity)  — "Boxing News Staff" / "MMA News Staff" bylines
                       (Confirmed by Priyesh 2026-07-21 — see memory
                        boxingnews_hasib_pipeline_gap.md.)
  3. Fighter-angle   — articles tagged 'fighter-angle' (already reads
                        strategy, kept for reference).

For each cohort in each window:
  - articles published
  - articles that made it onto MSN (canonical_url resolved)
  - MSN ingest %
  - total peak PVs
  - avg peak PVs / MSN article  ← the KEY metric for the optimization
  - PVs ≥1K, ≥5K, ≥10K
  - est. revenue ($4 CPM — multiply by ~1.6 for real payout, see
    memory boxingnews_msn_actual_cpm.md)

The output shows a WoW-style before/after side-by-side plus a delta
row so it's easy to read at a glance.

CAVEATS
───────
- Peak PVs are the MAX read_count across MSN's rolling 24h window that
  our puller snapshotted. Articles published very recently haven't fully
  aged into their MSN peak — for post-window articles <5 days old the
  peak is under-counted. Recommend running this AFTER post-window
  articles have had 5+ days on MSN before drawing final conclusions.
- Real payout runs ~1.6× the $4-CPM estimate. Use msn_earning_monthly
  once June-July payouts post if you want authoritative numbers.
"""
from __future__ import annotations

import argparse
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import psycopg  # psycopg3

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
    " MMA News Staff",  # observed whitespace variant in the wild
    "MMA News",
}

CPM_USD = 4.0  # underestimates real payout by ~60% — see memory


@dataclass
class Cohort:
    label: str
    articles: int = 0
    on_msn: int = 0
    total_pvs: int = 0
    max_pv: int = 0
    ge_1k: int = 0
    ge_5k: int = 0
    ge_10k: int = 0
    peaks: list[int] = field(default_factory=list)

    @property
    def ingest_pct(self) -> float:
        return 100.0 * self.on_msn / self.articles if self.articles else 0.0

    @property
    def avg_per_msn(self) -> float:
        return self.total_pvs / self.on_msn if self.on_msn else 0.0

    @property
    def avg_per_article(self) -> float:
        return self.total_pvs / self.articles if self.articles else 0.0

    @property
    def est_usd(self) -> float:
        return self.total_pvs * CPM_USD / 1000.0


def slug_of(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] if path else None


def load_msn_peaks(pgam_dsn: str) -> dict[str, int]:
    """canonical_url slug → peak_read_count across all snapshot history."""
    peaks: dict[str, int] = {}
    with psycopg.connect(pgam_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.canonical_url, MAX(s.read_count) AS peak
              FROM pgam_direct.msn_article_meta m
              JOIN pgam_direct.msn_article_snapshots s USING (doc_id)
             WHERE m.canonical_url IS NOT NULL
             GROUP BY m.canonical_url
            """
        )
        for url, peak in cur.fetchall():
            slug = slug_of(url)
            if slug:
                peaks[slug] = int(peak or 0)
    return peaks


def cohort_for(byline: str, tags: list[str] | None) -> str | None:
    """Bucket an article into rotation | hasib | fighter-angle | None."""
    tag_set = {t.lower() for t in (tags or [])}
    if "fighter-angle" in tag_set:
        return "fighter-angle"
    if byline in ROTATION_BYLINES:
        return "rotation"
    if byline in HASIB_BYLINES:
        return "hasib"
    return None


def scan_window(
    bn_dsn: str,
    peaks_by_slug: dict[str, int],
    frm: date,
    to: date,
) -> dict[str, Cohort]:
    """Return {cohort_label: Cohort} for the [frm, to) window."""
    cohorts = {
        "rotation": Cohort("rotation (Tom Rashid et al.)"),
        "hasib": Cohort("Hasib (Boxing News Staff / MMA News Staff)"),
        "fighter-angle": Cohort("fighter-angle lane"),
    }
    with psycopg.connect(bn_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT slug, COALESCE(NULLIF(TRIM(author_name),''),'') AS author, tag_names
              FROM articles
             WHERE COALESCE(status,'published') = 'published'
               AND published_at >= %s
               AND published_at <  %s
            """,
            (frm, to),
        )
        for slug, author, tags in cur.fetchall():
            label = cohort_for(author, tags)
            if not label:
                continue
            c = cohorts[label]
            c.articles += 1
            peak = peaks_by_slug.get(slug)
            if peak is not None:
                c.on_msn += 1
                c.total_pvs += peak
                c.max_pv = max(c.max_pv, peak)
                if peak >= 1000:
                    c.ge_1k += 1
                if peak >= 5000:
                    c.ge_5k += 1
                if peak >= 10000:
                    c.ge_10k += 1
                c.peaks.append(peak)
    return cohorts


def fmt(v: Any, kind: str = "int") -> str:
    if v is None:
        return "-"
    if kind == "int":
        return f"{int(v):,}"
    if kind == "float":
        return f"{float(v):,.0f}"
    if kind == "pct":
        return f"{float(v):.0f}%"
    if kind == "usd":
        return f"${float(v):,.2f}"
    return str(v)


def print_side_by_side(pre: dict[str, Cohort], post: dict[str, Cohort],
                        pre_days: int, post_days: int) -> None:
    metrics = [
        ("articles",         "articles",           "int"),
        ("on_msn",           "on MSN",             "int"),
        ("ingest_pct",       "ingest %",           "pct"),
        ("total_pvs",        "total PVs",          "int"),
        ("avg_per_msn",      "avg PV / MSN art",   "float"),
        ("avg_per_article",  "avg PV / all art",   "float"),
        ("max_pv",           "max PV",             "int"),
        ("ge_1k",            "articles ≥1K",       "int"),
        ("ge_5k",            "articles ≥5K",       "int"),
        ("ge_10k",           "articles ≥10K",      "int"),
        ("est_usd",          "est $ ($4 CPM)",     "usd"),
    ]
    for label in ("rotation", "hasib", "fighter-angle"):
        p_pre = pre[label]
        p_post = post[label]
        print(f"\n=== {p_pre.label} ===")
        print(f"  window PRE:  {pre_days:>3}d   window POST: {post_days:>3}d")
        header = f"  {'metric':<20} {'PRE':>15} {'POST':>15} {'Δ':>12}   {'PRE/day':>10} {'POST/day':>10}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for attr, disp, kind in metrics:
            pre_v = getattr(p_pre, attr)
            post_v = getattr(p_post, attr)
            delta_raw = post_v - pre_v
            if kind == "int":
                delta = f"{int(delta_raw):+,}"
            elif kind == "float":
                delta = f"{float(delta_raw):+,.0f}"
            elif kind == "pct":
                delta = f"{float(delta_raw):+.0f}pp"
            elif kind == "usd":
                delta = f"${float(delta_raw):+,.2f}"
            else:
                delta = "-"
            # Per-day only meaningful for count-ish metrics
            if attr in ("articles", "on_msn", "total_pvs", "ge_1k", "ge_5k", "ge_10k", "est_usd"):
                pre_day = fmt(pre_v / pre_days if pre_days else 0, "float" if kind == "int" else kind)
                post_day = fmt(post_v / post_days if post_days else 0, "float" if kind == "int" else kind)
            else:
                pre_day = post_day = ""
            print(f"  {disp:<20} {fmt(pre_v, kind):>15} {fmt(post_v, kind):>15} {delta:>12}   {pre_day:>10} {post_day:>10}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--deploy", type=lambda s: datetime.fromisoformat(s).date(),
                     default=date(2026, 7, 21),
                     help="Deploy anchor (default 2026-07-21). PRE window = 30 days before this; POST window = this → now.")
    ap.add_argument("--pre-from", type=lambda s: datetime.fromisoformat(s).date())
    ap.add_argument("--pre-to",   type=lambda s: datetime.fromisoformat(s).date())
    ap.add_argument("--post-from", type=lambda s: datetime.fromisoformat(s).date())
    ap.add_argument("--post-to",   type=lambda s: datetime.fromisoformat(s).date())
    args = ap.parse_args()

    deploy = args.deploy
    today = datetime.now(UTC).date()
    pre_from  = args.pre_from  or (deploy - timedelta(days=30))
    pre_to    = args.pre_to    or deploy
    post_from = args.post_from or deploy
    post_to   = args.post_to   or today

    pgam_dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL")
    bn_dsn   = os.environ.get("BOXINGNEWS_DATABASE_URL")
    if not pgam_dsn or not bn_dsn:
        raise SystemExit("PGAM_DIRECT_DATABASE_URL and BOXINGNEWS_DATABASE_URL env vars are required.\n"
                          "  export $(grep -E '^(PGAM_DIRECT_DATABASE_URL|BOXINGNEWS_DATABASE_URL)=' ~/Desktop/pgam-intelligence/.env | xargs)")

    print(f"Deploy anchor: {deploy}")
    print(f"PRE  window:  {pre_from} → {pre_to}   ({(pre_to - pre_from).days} days)")
    print(f"POST window:  {post_from} → {post_to}   ({(post_to - post_from).days} days)")
    print()

    peaks = load_msn_peaks(pgam_dsn)
    print(f"Loaded {len(peaks)} MSN doc→peak mappings.\n")

    pre_days = max(1, (pre_to - pre_from).days)
    post_days = max(1, (post_to - post_from).days)
    pre  = scan_window(bn_dsn, peaks, pre_from, pre_to)
    post = scan_window(bn_dsn, peaks, post_from, post_to)

    print_side_by_side(pre, post, pre_days, post_days)

    print("\nNotes:")
    print("  - Post-window articles <5 days old have UNDERCOUNTED peak PVs")
    print("    (MSN's 24h rolling window hasn't landed yet). Re-run 5+ days")
    print("    after any post_to date for the settled comparison.")
    print("  - Est $ uses $4 CPM (puller default). Multiply by ~1.6 for real")
    print("    payout — see memory boxingnews_msn_actual_cpm.md.")


if __name__ == "__main__":
    main()
