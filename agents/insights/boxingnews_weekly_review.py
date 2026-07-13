"""
agents/insights/boxingnews_weekly_review.py
──────────────────────────────────────────────────────────────────────────────
Monday-morning weekly content-strategy postmortem for BoxingNews.com.

WHY THIS EXISTS
───────────────
MSN pays BoxingNews $4 CPM on article reads. Performance is brutally
power-law: in any 7-day window the top 5 articles drive ~50% of all
reads while the bottom 90% earn under $1 combined. The single biggest
lever is being *deliberate* about what we produce next week given what
we learned this week — but that learning was previously a manual chore
no-one had time to do.

This agent closes the loop:

  1. Pull the prior 7 days of MSN per-article performance from
     pgam_direct.msn_article_peak + msn_article_meta + msn_article_snapshots.
  2. Pull the matching boxingnews articles (canonical title, msn_title,
     tags, msn_title_variants.pattern, published_at).
  3. Compute the postmortem segmentation:
        - by topic (tag)
        - by headline pattern (P1..P6 from the tuner taxonomy)
        - by origin lane (breaking vs trending vs programmatic)
        - by origin source (subreddit / Twitter handle / RSS feed)
        - by day-of-week / time-since-event
  4. Ask Claude (Opus tier — this is the high-judgment call of the
     week) to produce two artifacts in one shot:
        a. report_md  — the human briefing emailed to Priyesh
        b. strategy   — the machine-readable JSON consumed by the
                        boxingnews codebase to bias next week's output
  5. UPSERT one row into pgam_direct.msn_weekly_review.
  6. Email + Slack the report.

The boxingnews repo's headline-tuner reads strategy.winning_patterns,
hot_topics, hot_fighters and avoid_phrases on every cron tick via
src/lib/msn/strategy.ts — that's how the loop self-refines without
manual intervention.

SAFETY POSTURE
──────────────
The agent NEVER writes back to the boxingnews DB. It writes only to
its own table in pgam_direct. If the agent crashes or produces a bad
strategy, the worst case is the tuner reads an empty strategy block
and falls back to the static prompt — no regression vs the pre-loop
behaviour.

SCHEDULING
──────────
Registered in scheduler.py for Monday 09:30 ET. The 09:30 anchor is
late enough that the prior week's Friday-Sunday MSN reads have settled
in our snapshots (peak_read_count is the MAX across the rolling 24h
window) and early enough to feed Monday's editorial planning.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import pytz

from core.neon import connect as connect_pgam_direct
from core.boxingnews_db import connect as connect_boxingnews
from core.slack import send_text

# Optional Claude — fall back to a deterministic minimal report when
# unavailable so the agent never silently fails on a bad API key.
try:
    import anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False


ET = pytz.timezone("America/New_York")
PARTNER_ID = "AA1lKiff"          # BoxingNews on MSN Partner Hub
MSN_CPM_USD = 4.00               # MSN pays $4 per 1000 reads
RECIPIENT = os.environ.get("BOXINGNEWS_REVIEW_RECIPIENT", "priyesh@pgammedia.com")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "noreply@pgammedia.com")
MODEL = os.environ.get("BOXINGNEWS_REVIEW_MODEL", "claude-opus-4-7")


# ───────────────────────────────────────────────────────────────────────────
# Public entrypoint (scheduler calls this)
# ───────────────────────────────────────────────────────────────────────────

def boxingnews_weekly_review() -> None:
    """Run the weekly postmortem. Idempotent — same-week reruns UPSERT.

    Defines "the week under review" as the seven full calendar days
    ENDING yesterday in ET. So a Monday morning run reviews
    Mon-Sun (one full Mon→Sun cycle).
    """
    today_et = datetime.now(ET).date()
    period_end = today_et - timedelta(days=1)
    period_start = period_end - timedelta(days=6)
    iso_week = _iso_week_label(period_end)

    prev_period_end = period_start - timedelta(days=1)
    prev_period_start = prev_period_end - timedelta(days=6)

    print(f"[boxingnews_weekly_review] period {period_start} → {period_end} (iso {iso_week})")

    msn_rows = _pull_msn_rows(period_start, period_end)
    prev_msn_rows = _pull_msn_rows(prev_period_start, prev_period_end)
    if not msn_rows:
        print("[boxingnews_weekly_review] no MSN data for the period — skipping")
        return

    article_rows = _pull_boxingnews_articles([r["canonical_url"] for r in msn_rows if r["canonical_url"]])

    joined = _join_msn_and_articles(msn_rows, article_rows)
    segmentation = _segment(joined)

    summary_stats = _summary_stats(msn_rows, prev_msn_rows)

    payload_for_claude = _build_claude_payload(
        period_start, period_end, iso_week,
        summary_stats, segmentation, joined,
    )

    if _ANTHROPIC_AVAILABLE and os.environ.get("ANTHROPIC_API_KEY"):
        report_md, strategy = _ask_claude(payload_for_claude)
    else:
        print("[boxingnews_weekly_review] anthropic unavailable — using fallback report")
        report_md, strategy = _fallback_report(payload_for_claude)

    _upsert_weekly_review(
        iso_week=iso_week,
        period_start=period_start,
        period_end=period_end,
        stats=summary_stats,
        report_md=report_md,
        strategy=strategy,
        top_article=segmentation["top_article"],
    )

    _deliver_report(
        iso_week=iso_week,
        period_start=period_start,
        period_end=period_end,
        stats=summary_stats,
        report_md=report_md,
    )

    print(f"[boxingnews_weekly_review] complete — reads={summary_stats['reads_total']}, "
          f"articles={summary_stats['articles_indexed']}, revenue=${summary_stats['revenue_usd']:.2f}")


# ───────────────────────────────────────────────────────────────────────────
# Data pulls
# ───────────────────────────────────────────────────────────────────────────

def _pull_msn_rows(period_start: date, period_end: date) -> list[dict[str, Any]]:
    """One row per MSN article first-seen in the window, with peak reads
    and the canonical boxingnews URL when the resolver matched it."""
    with connect_pgam_direct() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.doc_id,
                   p.peak_read_count,
                   p.first_seen_at,
                   m.canonical_url,
                   m.msn_title_first,
                   m.msn_url
              FROM pgam_direct.msn_article_peak p
              LEFT JOIN pgam_direct.msn_article_meta m USING (doc_id)
             WHERE p.partner_id = %s
               AND p.first_seen_at::date BETWEEN %s AND %s
             ORDER BY p.peak_read_count DESC
            """,
            (PARTNER_ID, period_start, period_end),
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _pull_boxingnews_articles(canonical_urls: list[str]) -> dict[str, dict[str, Any]]:
    """Look up boxingnews-side article rows by canonical URL.
    Returns dict keyed on the URL — same key MSN-side rows use."""
    if not canonical_urls:
        return {}

    # boxingnews stores canonical URLs as boxingnews.com/news/{slug}/ — the
    # MSN-side resolver lands them in m.canonical_url verbatim, so we can
    # match on slug derived from URL.
    slugs = []
    url_for_slug: dict[str, str] = {}
    for u in canonical_urls:
        if not u:
            continue
        # Strip protocol+host, then any leading /news/ or /, then trailing /
        path = u.split("://", 1)[-1].split("/", 1)[-1]
        s = path.replace("news/", "", 1).strip("/")
        if not s:
            continue
        slugs.append(s)
        url_for_slug[s] = u

    out: dict[str, dict[str, Any]] = {}
    if not slugs:
        return out

    with connect_boxingnews() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT slug,
                   title,
                   msn_title,
                   msn_title_variants,
                   msn_status,
                   published_at,
                   tag_names,
                   category_names,
                   sport,
                   sanity_id
              FROM articles
             WHERE slug = ANY(%s)
            """,
            (slugs,),
        )
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            url = url_for_slug.get(row["slug"])
            if url:
                out[url] = row
    return out


# ───────────────────────────────────────────────────────────────────────────
# Join + segment
# ───────────────────────────────────────────────────────────────────────────

def _join_msn_and_articles(
    msn_rows: list[dict[str, Any]],
    article_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Produce one row per MSN doc with whatever boxingnews context we
    could resolve. Unmatched docs still flow through with article=None
    so the postmortem still sees their reads."""
    joined = []
    for m in msn_rows:
        a = article_rows.get(m.get("canonical_url") or "")
        tags = a["tag_names"] if a and a.get("tag_names") else []
        pattern = _detect_pattern(a)
        lane = _detect_lane(tags)
        origin_sources = _detect_origin_sources(a)
        joined.append({
            "doc_id":         m["doc_id"],
            "reads":          int(m["peak_read_count"] or 0),
            "first_seen_at":  m["first_seen_at"],
            "msn_title":      m.get("msn_title_first") or (a.get("msn_title") if a else None) or (a.get("title") if a else None) or "",
            "canonical_url":  m.get("canonical_url"),
            "tags":           tags,
            "sport":          (a.get("sport") if a else None) or "boxing",
            "pattern":        pattern,
            "lane":           lane,
            "origin_sources": origin_sources,
            "published_at":   a["published_at"] if a else None,
            "matched":        a is not None,
        })
    return joined


def _detect_pattern(article: dict[str, Any] | None) -> str:
    """Bucket the article by tuner pattern.

    - "P1".."P6": tuner picked a variant, headline exact-matches
    - "P-skip":   tuner deliberately skipped this article (msn_status='skipped').
                  Skipped rows store {"reason": ...} in msn_title_variants
                  with no variants array — before this bucket existed they
                  all fell to "P?" and contaminated the tuner-pattern signal.
    - "P?":       genuinely unknown (pre-tuner article, manual override,
                  or tuner state anomaly). Post-fix this should be small.
    """
    if not article:
        return "P?"
    if article.get("msn_status") == "skipped":
        return "P-skip"
    if not article.get("msn_title_variants"):
        return "P?"
    raw = article["msn_title_variants"]
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return "P?"
    if isinstance(raw, dict):
        variants = raw.get("variants") or []
        for v in variants:
            if isinstance(v, dict) and v.get("headline") == article.get("msn_title"):
                p = (v.get("pattern") or "").strip()
                return p or "P?"
        # No exact match — fallback to the first variant's pattern (the
        # tuner stores picked_index implicitly = 0 when no rotation
        # has happened).
        if variants and isinstance(variants[0], dict):
            return (variants[0].get("pattern") or "P?").strip() or "P?"
    return "P?"


def _detect_lane(tags: list[str]) -> str:
    """Map a tag set back to the ingest lane that produced the article.

    Provenance tags (`breaking-news`, `trending-now`) were introduced
    2026-06-07 by the breaking + trending ingest routes. Historical
    articles won't carry them, so we ALSO fall back to content-tag
    fingerprinting (press conference / p4p / on-this-day / weekend
    roundup / live blog) to attribute the programmatic generators —
    those have been writing the same content-tag shapes since they
    launched."""
    tag_set = {t.lower() for t in tags}

    if "fighter-angle" in tag_set:
        return "fighter-angle"
    if "breaking-news" in tag_set:
        return "breaking"
    if "trending-now" in tag_set or "trending" in tag_set:
        return "trending"

    # Programmatic generators — match by their characteristic content tags.
    # Both space- and dash-separated variants seen in the wild (press
    # conference vs press-conference).
    PROGRAMMATIC = {
        "press conference", "press-conference",
        "weekend roundup", "weekend-roundup",
        "division state", "division-state",
        "p4p", "pound-for-pound", "pound for pound",
        "live blog", "live-blog",
        "on this day", "on-this-day",
    }
    if tag_set & PROGRAMMATIC:
        return "programmatic"
    if any(t.startswith("on-this-day") or t.startswith("p4p") for t in tag_set):
        return "programmatic"

    return "editorial"  # Sanity-authored, AI-extracted from queue, or pre-provenance-tagging legacy


def _detect_origin_sources(article: dict[str, Any] | None) -> list[str]:
    """Best-effort attribution. Currently we don't persist origin-source
    per-article in boxingnews; we infer from tags and lane. The weekly
    review's source attribution gets noticeably sharper once
    `articles.origin_source` is added (TODO)."""
    if not article:
        return []
    tags = article.get("tag_names") or []
    out: list[str] = []
    if "breaking-news" in tags: out.append("breaking-lane")
    if "trending-now" in tags or "trending" in tags: out.append("trending-lane")
    return out


def _segment(joined: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the segmentation Claude needs to write the postmortem."""

    by_pattern: dict[str, list[int]] = defaultdict(list)
    by_lane: dict[str, list[int]] = defaultdict(list)
    by_tag: dict[str, list[int]] = defaultdict(list)
    by_dow: dict[str, list[int]] = defaultdict(list)

    for row in joined:
        by_pattern[row["pattern"]].append(row["reads"])
        by_lane[row["lane"]].append(row["reads"])
        for t in (row.get("tags") or []):
            by_tag[t].append(row["reads"])
        if row.get("published_at"):
            dow = row["published_at"].strftime("%a")
            by_dow[dow].append(row["reads"])

    def _agg(buckets: dict[str, list[int]]) -> list[dict[str, Any]]:
        out = []
        for k, reads in buckets.items():
            if not reads:
                continue
            out.append({
                "key": k,
                "articles": len(reads),
                "reads_total": sum(reads),
                "reads_avg": round(sum(reads) / len(reads), 1),
                "reads_max": max(reads),
            })
        out.sort(key=lambda r: r["reads_total"], reverse=True)
        return out

    top_articles = sorted(joined, key=lambda r: r["reads"], reverse=True)[:15]
    top_article = top_articles[0] if top_articles else None

    return {
        "by_pattern":    _agg(by_pattern),
        "by_lane":       _agg(by_lane),
        "by_tag":        _agg(by_tag)[:25],         # cap — long tail is noise
        "by_day_of_week":_agg(by_dow),
        "top_articles":  top_articles,
        "top_article":   top_article,
    }


def _summary_stats(rows: list[dict[str, Any]], prev_rows: list[dict[str, Any]]) -> dict[str, Any]:
    reads = sum(int(r["peak_read_count"] or 0) for r in rows)
    prev_reads = sum(int(r["peak_read_count"] or 0) for r in prev_rows) if prev_rows else 0
    return {
        "articles_indexed":  len(rows),
        "reads_total":       reads,
        "reads_prev_week":   prev_reads,
        "wow_delta_pct":     round(((reads - prev_reads) / prev_reads) * 100, 1) if prev_reads else None,
        "revenue_usd":       reads / 1000.0 * MSN_CPM_USD,
        "revenue_usd_cents": int(round(reads / 1000.0 * MSN_CPM_USD * 100)),
        "avg_reads":         round(reads / len(rows), 1) if rows else 0,
        "median_cpm_eligible": _median([int(r["peak_read_count"] or 0) for r in rows]),
    }


def _median(values: list[int]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2 == 1:
        return float(s[n // 2])
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


# ───────────────────────────────────────────────────────────────────────────
# Claude — produce report_md + strategy JSON in one call
# ───────────────────────────────────────────────────────────────────────────

def _build_claude_payload(
    period_start: date,
    period_end: date,
    iso_week: str,
    stats: dict[str, Any],
    segmentation: dict[str, Any],
    joined: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compact JSON-friendly payload — keeps the prompt under ~2K tokens
    even on heavy weeks."""
    return {
        "period": {
            "iso_week":   iso_week,
            "start":      str(period_start),
            "end":        str(period_end),
        },
        "stats": {
            "articles":        stats["articles_indexed"],
            "reads_total":     stats["reads_total"],
            "reads_prev_week": stats["reads_prev_week"],
            "wow_delta_pct":   stats["wow_delta_pct"],
            "revenue_usd":     round(stats["revenue_usd"], 2),
            "avg_reads":       stats["avg_reads"],
            "median_reads":    stats["median_cpm_eligible"],
        },
        "top_articles": [
            {
                "title":    a["msn_title"],
                "reads":    a["reads"],
                "pattern":  a["pattern"],
                "lane":     a["lane"],
                "tags":     a["tags"][:6],
                "sport":    a["sport"],
            }
            for a in segmentation["top_articles"][:10]
        ],
        "by_pattern":      segmentation["by_pattern"],
        "by_lane":         segmentation["by_lane"],
        "by_tag":          segmentation["by_tag"],
        "by_day_of_week":  segmentation["by_day_of_week"],
    }


_SYSTEM_PROMPT = """You are the content-strategy analyst for BoxingNews.com,
a UK boxing/MMA news site that syndicates to MSN Start. MSN pays $4 CPM
on article reads — page views are the only revenue lever that matters.

Your weekly job:
1. Read the prior-7-day MSN performance data.
2. Produce TWO outputs in one response:

   A. A short Markdown report (Slack-ready, <500 words) for the founder
      Priyesh. Cover:
        - Headline number: total reads, revenue, week-on-week delta.
        - The single biggest win and what made it work.
        - The single biggest miss / wasted publish lane.
        - 3 concrete recommendations for next week.
        - Tone: terse, opinionated, no AI-cadence hedges, no "in
          conclusion", no triplets, no emojis. Sound like a sharp
          analyst writing a brief, not a chatbot.

   B. A strategy JSON object the production system can read directly.
      Schema (every key required, even if empty list):
        {
          "hot_topics":         [string, ...]   // 5-12 tag-shaped topics that overperformed
          "hot_fighters":       [string, ...]   // 5-12 proper-name fighters that overperformed
          "winning_patterns":   ["P1"|"P2"|"P3"|"P4"|"P5"|"P6", ...]
          "hot_sources":        [string, ...]   // origin lanes / publisher sources that overperformed
          "dud_sources":        [string, ...]   // origin lanes / sources to deprioritize
          "avoid_phrases":      [string, ...]   // 0-10 phrases observed to depress PVs
          "notes":              string          // 1-2 sentence free-form note for the tuner
        }

      Notes on the strategy fields:
        - hot_topics MUST be drawn from the `by_tag` list the data
          provides — do not invent new tags.
        - hot_fighters should be extracted from top_articles titles.
        - winning_patterns picked from the `by_pattern` ranking. A
          pattern only counts if avg_reads exceeds the overall median
          by ≥30%.
        - Be honest about dud_sources: if a lane published a lot of
          articles but produced <30% of the cohort's median reads,
          flag it. Empty list is fine if the data is too sparse.

OUTPUT FORMAT — strict:
First emit the Markdown report wrapped in <report>...</report>.
Then emit the strategy JSON wrapped in <strategy>...</strategy>.
No prose outside those tags."""


def _ask_claude(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    user_msg = (
        "Here is the prior-7-day MSN performance data for BoxingNews.com.\n\n"
        f"```json\n{json.dumps(payload, indent=2, default=str)}\n```\n\n"
        "Produce the weekly report and strategy JSON per the system prompt."
    )
    # Note: temperature is deprecated on claude-sonnet-4-5+/opus-4-7+ —
    # newer models use their own deterministic decoding tuned for these
    # judgement-style prompts. Omit it entirely so we work across the
    # whole 4.x family.
    resp = client.messages.create(
        model=MODEL,
        max_tokens=2000,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
    return _parse_report_and_strategy(text)


def _parse_report_and_strategy(text: str) -> tuple[str, dict[str, Any]]:
    report = _extract_tag(text, "report") or text.strip()
    strategy_raw = _extract_tag(text, "strategy") or "{}"
    try:
        strategy = json.loads(strategy_raw)
    except json.JSONDecodeError:
        # Try to find the first {...} block
        start = strategy_raw.find("{")
        end = strategy_raw.rfind("}")
        if start >= 0 and end > start:
            try:
                strategy = json.loads(strategy_raw[start:end + 1])
            except json.JSONDecodeError:
                strategy = {}
        else:
            strategy = {}
    return report, _normalize_strategy(strategy)


def _extract_tag(text: str, tag: str) -> str | None:
    start_marker = f"<{tag}>"
    end_marker = f"</{tag}>"
    s = text.find(start_marker)
    e = text.find(end_marker)
    if s < 0 or e < 0 or e < s:
        return None
    return text[s + len(start_marker):e].strip()


def _normalize_strategy(s: dict[str, Any]) -> dict[str, Any]:
    """Ensure every expected key exists and is the right type — the
    boxingnews-side consumer treats missing keys as empty arrays, so
    this isn't strictly required, but keeping the wire format clean
    makes downstream debugging much easier."""
    def _arr(v) -> list[str]:
        return [str(x) for x in v if isinstance(x, (str, int))][:50] if isinstance(v, list) else []
    return {
        "hot_topics":       _arr(s.get("hot_topics")),
        "hot_fighters":     _arr(s.get("hot_fighters")),
        "winning_patterns": _arr(s.get("winning_patterns")),
        "hot_sources":      _arr(s.get("hot_sources")),
        "dud_sources":      _arr(s.get("dud_sources")),
        "avoid_phrases":    _arr(s.get("avoid_phrases")),
        "notes":            str(s.get("notes") or "")[:1000],
    }


def _fallback_report(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """When Anthropic isn't reachable we still want a row to land so
    the cadence doesn't break. Produce a stripped-down deterministic
    summary."""
    stats = payload["stats"]
    top = payload["top_articles"][:5]
    lines = [
        f"# BoxingNews — week of {payload['period']['start']} to {payload['period']['end']}",
        "",
        f"- **Reads**: {stats['reads_total']:,} ({stats['articles']} articles, avg {stats['avg_reads']})",
        f"- **Revenue**: ${stats['revenue_usd']:.2f}",
    ]
    if stats.get("wow_delta_pct") is not None:
        lines.append(f"- **WoW change**: {stats['wow_delta_pct']:+.1f}%")
    lines.append("")
    lines.append("## Top 5 articles")
    for a in top:
        lines.append(f"- {a['title']}  — {a['reads']:,} reads  [{a['pattern']}, {a['lane']}]")
    report = "\n".join(lines)

    # Best-effort strategy from the segmentation data alone.
    cohort_median = stats.get("median_reads", 0) or 0
    # Match the LLM-path schema: winning_patterns is P1..P6 only. P-skip and
    # P? are aggregation buckets, not tuner patterns the headline generator
    # can act on.
    _PICKABLE = {"P1", "P2", "P3", "P4", "P5", "P6"}
    winning_patterns = [
        b["key"] for b in payload["by_pattern"]
        if b["key"] in _PICKABLE and cohort_median and b["reads_avg"] >= cohort_median * 1.3
    ][:6]
    hot_topics = [b["key"] for b in payload["by_tag"][:8]]
    dud_lanes = [
        b["key"] for b in payload["by_lane"]
        if cohort_median and b["reads_avg"] < cohort_median * 0.3 and b["articles"] >= 5
    ]
    return report, _normalize_strategy({
        "hot_topics":       hot_topics,
        "hot_fighters":     [],
        "winning_patterns": winning_patterns,
        "hot_sources":      [],
        "dud_sources":      dud_lanes,
        "avoid_phrases":    [],
        "notes":            "Auto-generated fallback (Anthropic unavailable). Re-run for a richer review.",
    })


# ───────────────────────────────────────────────────────────────────────────
# Persistence
# ───────────────────────────────────────────────────────────────────────────

def _upsert_weekly_review(
    *,
    iso_week: str,
    period_start: date,
    period_end: date,
    stats: dict[str, Any],
    report_md: str,
    strategy: dict[str, Any],
    top_article: dict[str, Any] | None,
) -> None:
    with connect_pgam_direct() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pgam_direct.msn_weekly_review (
                iso_week, period_start, period_end,
                reads_total, reads_prev_week, articles_indexed, revenue_usd_cents,
                top_article_doc_id, top_article_reads,
                report_md, strategy
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (iso_week) DO UPDATE SET
                period_start       = EXCLUDED.period_start,
                period_end         = EXCLUDED.period_end,
                reads_total        = EXCLUDED.reads_total,
                reads_prev_week    = EXCLUDED.reads_prev_week,
                articles_indexed   = EXCLUDED.articles_indexed,
                revenue_usd_cents  = EXCLUDED.revenue_usd_cents,
                top_article_doc_id = EXCLUDED.top_article_doc_id,
                top_article_reads  = EXCLUDED.top_article_reads,
                report_md          = EXCLUDED.report_md,
                strategy           = EXCLUDED.strategy,
                generated_at       = NOW()
            """,
            (
                iso_week, period_start, period_end,
                stats["reads_total"], stats["reads_prev_week"] or None,
                stats["articles_indexed"], stats["revenue_usd_cents"],
                top_article["doc_id"] if top_article else None,
                top_article["reads"] if top_article else None,
                report_md, json.dumps(strategy),
            ),
        )
        conn.commit()


# ───────────────────────────────────────────────────────────────────────────
# Delivery — Slack + SendGrid email
# ───────────────────────────────────────────────────────────────────────────

def _deliver_report(
    *,
    iso_week: str,
    period_start: date,
    period_end: date,
    stats: dict[str, Any],
    report_md: str,
) -> None:
    subject = f"BoxingNews weekly review — {period_start} to {period_end}"

    # Slack's mrkdwn renders **bold** literally; it wants *bold*. Convert
    # the Markdown source so the message looks right in Slack without
    # corrupting the persisted report_md (which still has standard MD
    # for the email path and the admin dashboard).
    slack_body = _markdown_to_slack_mrkdwn(report_md)
    try:
        send_text(f"*{subject}*\n\n{slack_body}")
    except Exception as exc:
        print(f"[boxingnews_weekly_review] Slack delivery failed (non-fatal): {exc}")

    html = _markdown_to_html(report_md, subject)
    sendgrid_key = os.environ.get("SENDGRID_KEY", "")
    if sendgrid_key and EMAIL_FROM and RECIPIENT:
        ok = _send_email_html(html, subject, sendgrid_key, EMAIL_FROM, RECIPIENT)
        print(f"[boxingnews_weekly_review] Email send ok={ok} → {RECIPIENT}")
    else:
        print("[boxingnews_weekly_review] SendGrid not configured — skipping email")


def _markdown_to_slack_mrkdwn(md: str) -> str:
    """Convert a standard-Markdown report to Slack's mrkdwn dialect.
    Specifically: **bold** → *bold*, and # / ## headings → *Heading*."""
    import re
    out: list[str] = []
    for line in md.split("\n"):
        if line.startswith("## "):
            out.append(f"*{line[3:].strip()}*")
        elif line.startswith("# "):
            out.append(f"*{line[2:].strip()}*")
        else:
            # **bold** → *bold*
            out.append(re.sub(r"\*\*(.+?)\*\*", r"*\1*", line))
    return "\n".join(out)


def _markdown_to_html(md: str, subject: str) -> str:
    """Very minimal MD→HTML — we don't need a full parser, the report
    structure is bullet lists + h1/h2. Anything more elaborate goes
    through Slack's native Markdown rendering, not the email path."""
    lines = md.split("\n")
    out: list[str] = []
    in_list = False
    for line in lines:
        if line.startswith("# "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h1>{_html_escape(line[2:].strip())}</h1>")
        elif line.startswith("## "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h2>{_html_escape(line[3:].strip())}</h2>")
        elif line.startswith("- "):
            if not in_list: out.append("<ul>"); in_list = True
            out.append(f"<li>{_inline_html(line[2:].strip())}</li>")
        elif not line.strip():
            if in_list: out.append("</ul>"); in_list = False
            out.append("")
        else:
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<p>{_inline_html(line)}</p>")
    if in_list: out.append("</ul>")
    body = "\n".join(out)
    return f"<html><body style='font-family:system-ui,sans-serif;max-width:680px;margin:24px auto;color:#111'><h2>{_html_escape(subject)}</h2>{body}</body></html>"


def _inline_html(s: str) -> str:
    # Render **bold** + plain link [text](url) — that's all the report uses.
    import re
    s = _html_escape(s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    s = re.sub(r"\[([^\]]+)\]\((https?://[^\)]+)\)", r'<a href="\2">\1</a>', s)
    return s


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _send_email_html(html: str, subject: str, sendgrid_key: str, sender: str, to: str) -> bool:
    payload = {
        "personalizations": [{"to": [{"email": to}]}],
        "from":    {"email": sender},
        "subject": subject,
        "content": [{"type": "text/html", "value": html}],
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {sendgrid_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.getcode() in (200, 202)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"[boxingnews_weekly_review] SendGrid error {exc.code}: {body[:300]}")
        return False
    except Exception as exc:
        print(f"[boxingnews_weekly_review] SendGrid send failed: {exc}")
        return False


# ───────────────────────────────────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────────────────────────────────

def _iso_week_label(d: date) -> str:
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


# Module-callable shim so scheduler.py's _import helper (which looks for
# a top-level `run`) can wire this in directly.
def run() -> None:
    boxingnews_weekly_review()


if __name__ == "__main__":
    run()
