"""
healthnation_affiliate_audit.py — close the open items in the HealthNation
Amazon Associates review.

BACKGROUND
──────────
docs/healthnation-amazon-affiliate-review-2026-08-30.md audited the affiliate
layer in mastap150/healthnation-web after an Amazon payments notice
(store healthnation2-20, $0.55 for 06/2026). The audit was done from a cloud
session with no DATABASE_URL and no egress to healthnation.com, so four
things were inferred rather than observed:

  1. how much content is actually published
  2. how many documents carry amazon.com links (the tagger's real surface)
  3. whether AMAZON_PARTNER_TAG is set in Vercel prod
  4. actual affiliate_clicks volume, and the position breakdown

This script settles all four. (1), (2) and (4) come from SQL; (3) needs a
live fetch, so it lives behind --check-live.

The two facts that make the output readable:

  · tagAmazonLinks() runs on /[hub]/[slug] and /best/[slug] only. It does
    NOT run on /reviews/[slug], so Amazon links in a product review body go
    out untagged and earn nothing.
  · Auto-tagged inline links go straight to amazon.com and never touch the
    /go bouncer, so affiliate_clicks UNDERCOUNTS real outbound clicks. It
    sees the deliberate product CTAs and nothing else.

WHAT THIS PRINTS
────────────────
Ten sections, matching docs/healthnation-affiliate-audit-queries.sql:
  Q1  content inventory by status
  Q2  amazon link surface by doc type and domain  (non-.com rows = dead clicks)
  Q3  untagged amazon links in product reviews    (pure leakage)
  Q4  published articles with amazon links        (the disclosure gap)
  Q5  has_affiliate_links disagreeing with reality
  Q6  product catalog by affiliate network        (skimlinks rows earn $0)
  Q7  click volume by network and month
  Q8  clicks by on-page position
  Q9  top products by clicks, 90d                 (who to chase for direct deals)
  Q10 top source pages by clicks, 90d

Then a short VERDICT block naming the specific next actions the numbers
support.

USAGE
─────
  export HEALTHNATION_DATABASE_URL=...   # healthnation-web .env.local DATABASE_URL
  python3 scripts/healthnation_affiliate_audit.py
  python3 scripts/healthnation_affiliate_audit.py --check-live
  python3 scripts/healthnation_affiliate_audit.py --only Q4 Q6
  python3 scripts/healthnation_affiliate_audit.py --json > audit.json

CAVEATS
───────
- Read-only. Nothing here writes to healthnation.
- Q2–Q5 regex the stored HTML. A link built by JS at runtime, or an Amazon
  shortlink (amzn.to), will not match — amzn.to is worth grepping separately
  if the counts look implausibly low.
- affiliate_clicks logs the click, not the conversion. It cannot tell you
  revenue; it tells you where the intent is. Amazon earnings still have to be
  read out of the Associates dashboard.
- --check-live proves the tag is live on ONE rendered page. That is enough to
  confirm the env var is set in prod, which is all item (3) asks.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import healthnation_db  # noqa: E402

SITE_URL = os.environ.get("HEALTHNATION_SITE_URL", "https://healthnation.com")

# Section id → (heading, what the reader should take from it, SQL).
QUERIES: dict[str, tuple[str, str, str]] = {
    "Q1": (
        "Content inventory",
        "How much is actually published.",
        """
        SELECT 'articles' AS kind, status, count(*) AS n
          FROM healthnation.articles GROUP BY 1, 2
        UNION ALL
        SELECT 'buyer_guides', status, count(*)
          FROM healthnation.buyer_guides GROUP BY 1, 2
        UNION ALL
        SELECT 'product_reviews', status, count(*)
          FROM healthnation.product_reviews GROUP BY 1, 2
        ORDER BY 1, 2
        """,
    ),
    "Q2": (
        "Amazon link surface",
        "Every row whose host is not exactly amazon.com is a dead click — "
        "the tagger stamps the US tag on all 18 matched TLDs.",
        r"""
        WITH docs AS (
          SELECT 'article' AS kind, slug, status, content_html AS html
            FROM healthnation.articles
          UNION ALL
          SELECT 'buyer_guide', slug, status,
                 coalesce(intro_html, '') || ' ' || coalesce(body_html, '')
            FROM healthnation.buyer_guides
          UNION ALL
          SELECT 'product_review', slug, status,
                 coalesce(summary_html, '') || ' ' || coalesce(body_html, '')
            FROM healthnation.product_reviews
        ),
        links AS (
          SELECT d.kind, d.slug, d.status, lower(m[1]) AS host
            FROM docs d,
                 LATERAL regexp_matches(
                   d.html, 'https?://(?:www\.)?(amazon\.[a-z.]{2,9})/', 'gi'
                 ) AS m
        )
        SELECT kind, status, host,
               count(*) AS link_count, count(DISTINCT slug) AS docs
          FROM links GROUP BY 1, 2, 3 ORDER BY link_count DESC
        """,
    ),
    "Q3": (
        "Untagged Amazon links in product reviews",
        "/reviews/[slug] never calls tagAmazonLinks — these leave the site "
        "with no Associate tag at all.",
        r"""
        SELECT r.slug, r.status, count(*) AS untagged_amazon_links
          FROM healthnation.product_reviews r,
               LATERAL regexp_matches(
                 coalesce(r.summary_html, '') || ' ' || coalesce(r.body_html, ''),
                 'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
               ) AS m
         GROUP BY 1, 2 ORDER BY untagged_amazon_links DESC
        """,
    ),
    "Q4": (
        "Disclosure gap — published articles carrying Amazon links",
        "/[hub]/[slug] runs the tagger but renders no disclosure block. "
        "Each row is a live Operating Agreement exposure.",
        r"""
        SELECT a.hub, a.slug, a.published_at::date AS published,
               a.has_affiliate_links AS flagged, count(*) AS amazon_links
          FROM healthnation.articles a,
               LATERAL regexp_matches(
                 a.content_html, 'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
               ) AS m
         WHERE a.status = 'published'
         GROUP BY 1, 2, 3, 4 ORDER BY amazon_links DESC
        """,
    ),
    "Q5": (
        "has_affiliate_links disagreeing with reality",
        "Rows carry Amazon links but claim they don't — any disclosure logic "
        "keyed on the flag would skip exactly the wrong pages.",
        r"""
        SELECT a.hub, a.slug, a.status, count(*) AS amazon_links
          FROM healthnation.articles a,
               LATERAL regexp_matches(
                 a.content_html, 'https?://(?:www\.)?amazon\.[a-z.]{2,9}/', 'gi'
               ) AS m
         WHERE a.has_affiliate_links = false
         GROUP BY 1, 2, 3 ORDER BY amazon_links DESC
        """,
    ),
    "Q6": (
        "Product catalog by affiliate network",
        "Rows on a network with no live integration are 'Check current price' "
        "buttons earning $0. Skimlinks is absent from healthnation-web/src.",
        """
        SELECT coalesce(affiliate_network, '(null)') AS network, status,
               count(*) AS products,
               count(*) FILTER (WHERE affiliate_url IS NULL) AS no_url,
               count(*) FILTER (WHERE affiliate_url NOT ILIKE '%?%')
                 AS url_no_query_params
          FROM healthnation.products
         GROUP BY 1, 2 ORDER BY products DESC
        """,
    ),
    "Q7": (
        "Click volume by network and month",
        "Undercounts real outbound clicks — inline Amazon links bypass the "
        "bouncer entirely.",
        """
        SELECT date_trunc('month', created_at)::date AS month,
               coalesce(affiliate_network, '(null)') AS network,
               count(*) AS clicks, count(DISTINCT session_id) AS sessions
          FROM healthnation.affiliate_clicks
         GROUP BY 1, 2 ORDER BY 1 DESC, clicks DESC
        """,
    ),
    "Q8": (
        "Clicks by on-page position",
        "The dimension the bouncer was built for. This is what should drive "
        "placement decisions.",
        """
        SELECT coalesce(position_on_page, '(null)') AS position,
               count(*) AS clicks, count(DISTINCT product_slug) AS products,
               round(100.0 * count(*) / nullif(sum(count(*)) OVER (), 0), 1) AS pct
          FROM healthnation.affiliate_clicks
         GROUP BY 1 ORDER BY clicks DESC
        """,
    ),
    "Q9": (
        "Top products by clicks (90d)",
        "The list that decides which brands are worth a direct "
        "Impact/ShareASale application. Chase the top 5, ignore the tail.",
        """
        SELECT c.product_slug, p.brand,
               coalesce(c.affiliate_network, '(null)') AS network,
               count(*) AS clicks_90d,
               count(DISTINCT c.session_id) AS sessions,
               count(DISTINCT c.source_url) AS source_pages
          FROM healthnation.affiliate_clicks c
          LEFT JOIN healthnation.products p ON p.slug = c.product_slug
         WHERE c.created_at >= now() - interval '90 days'
         GROUP BY 1, 2, 3 ORDER BY clicks_90d DESC LIMIT 25
        """,
    ),
    "Q10": (
        "Top source pages by clicks (90d)",
        "Pair with Q4 — a page that drives clicks AND lacks a disclosure is "
        "the first one to fix.",
        """
        SELECT coalesce(source_url, '(direct)') AS source_page,
               count(*) AS clicks, count(DISTINCT product_slug) AS products
          FROM healthnation.affiliate_clicks
         WHERE created_at >= now() - interval '90 days'
         GROUP BY 1 ORDER BY clicks DESC LIMIT 25
        """,
    ),
}


def run(conn, sql: str) -> tuple[list[str], list[tuple]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d.name for d in cur.description] if cur.description else []
        return cols, cur.fetchall()


def print_table(cols: list[str], rows: list[tuple], indent: str = "  ") -> None:
    if not rows:
        print(f"{indent}(no rows)")
        return
    cells = [[("" if v is None else str(v)) for v in r] for r in rows]
    widths = [
        max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)
    ]
    print(indent + "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols)))
    print(indent + "  ".join("─" * w for w in widths))
    for row in cells:
        print(indent + "  ".join(v.ljust(widths[i]) for i, v in enumerate(row)))


def check_live(conn) -> dict:
    """Fetch one rendered page and look for the Associate tag on an Amazon
    link. Proves AMAZON_PARTNER_TAG is set in Vercel prod (open item 3)."""
    import requests

    with conn.cursor() as cur:
        cur.execute(
            r"""
            SELECT a.hub || '/' || a.slug AS path
              FROM healthnation.articles a
             WHERE a.status = 'published'
               AND a.content_html ~* 'https?://(www\.)?amazon\.[a-z.]{2,9}/'
             ORDER BY a.published_at DESC NULLS LAST
             LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            cur.execute(
                r"""
                SELECT 'best/' || g.slug
                  FROM healthnation.buyer_guides g
                 WHERE g.status = 'published'
                   AND (coalesce(g.intro_html, '') || coalesce(g.body_html, ''))
                       ~* 'https?://(www\.)?amazon\.[a-z.]{2,9}/'
                 ORDER BY g.published_at DESC NULLS LAST
                 LIMIT 1
                """
            )
            row = cur.fetchone()

    if not row:
        return {
            "verdict": "inconclusive",
            "detail": "No published article or guide contains an Amazon link, "
                      "so there is nothing for the tagger to stamp. That alone "
                      "explains a $0.55 month.",
        }

    url = f"{SITE_URL.rstrip('/')}/{row[0]}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:  # network, DNS, 4xx/5xx
        return {"verdict": "error", "url": url, "detail": f"fetch failed: {exc}"}

    anchors = re.findall(
        r'href="(https?://(?:www\.)?amazon\.[a-z.]{2,9}/[^"]*)"', resp.text, re.I
    )
    tagged = [a for a in anchors if "tag=" in a]
    tags = sorted({m.group(1) for a in tagged if (m := re.search(r"tag=([^&\"]+)", a))})

    if not anchors:
        verdict = "inconclusive"
        detail = ("Page rendered but no Amazon anchors survived into the HTML. "
                  "Check whether the stored link is inside a code block or is "
                  "built client-side.")
    elif tagged:
        verdict = "tag is live in prod"
        detail = f"{len(tagged)}/{len(anchors)} Amazon anchors carry a tag: {', '.join(tags)}"
    else:
        verdict = "TAG MISSING IN PROD"
        detail = (f"{len(anchors)} Amazon anchors, none tagged. "
                  "AMAZON_PARTNER_TAG is almost certainly unset in Vercel — "
                  "tagAmazonLinks() no-ops silently when it is.")

    return {"verdict": verdict, "url": url, "anchors": len(anchors),
            "tagged": len(tagged), "tags": tags, "detail": detail}


def plural(n: int, singular: str, suffix: str = "s") -> str:
    """'1 link' / '2 links' — the verdict block is read by a human."""
    return f"{n} {singular}{'' if n == 1 else suffix}"


def verdict(results: dict) -> list[str]:
    """Turn the numbers into the specific next actions they support."""
    out: list[str] = []

    def rows(qid: str) -> list[tuple]:
        return results.get(qid, {}).get("rows", [])

    # Q2 — international Amazon domains are dead clicks.
    intl = [r for r in rows("Q2") if r[2] != "amazon.com"]
    if intl:
        n = sum(r[3] for r in intl)
        hosts = ", ".join(sorted({r[2] for r in intl}))
        out.append(f"{plural(n, 'Amazon link')} point at non-US domains ({hosts}). "
                   f"These earn nothing today — enroll in OneLink, or restrict "
                   f"AMAZON_HOST in src/lib/amazon-tag.ts to amazon.com.")

    # Q3 — untagged review links.
    if rows("Q3"):
        n = sum(r[2] for r in rows("Q3"))
        out.append(f"{plural(n, 'Amazon link')} in product reviews, which never run "
                   f"the tagger — those render untagged. Add tagAmazonLinks() to "
                   f"/reviews/[slug]: smallest possible diff, immediate effect.")

    # Q4 — disclosure gap.
    if rows("Q4"):
        n = len(rows("Q4"))
        out.append(f"{plural(n, 'published article')} carry Amazon links with no "
                   f"disclosure block. This is the compliance item — reuse the "
                   f"markup from best/[slug]/page.tsx:162.")

    # Q6 — catalog earning nothing.
    dead = [r for r in rows("Q6") if r[0] in ("skimlinks", "(null)") and r[1] == "active"]
    if dead:
        n = sum(r[2] for r in dead)
        out.append(f"{plural(n, 'active product')} route to a network with no live "
                   f"integration. Every 'Check current price' button on those "
                   f"earns $0. This is the biggest number on the page.")

    # Q7/Q9 — is there enough traffic to act on? Guarded on Q7 actually
    # having run: under --only, an absent section is unknown, not zero.
    total_clicks = sum(r[2] for r in rows("Q7"))
    if "Q7" not in results:
        pass
    elif total_clicks == 0:
        out.append("Zero clicks logged through /go. Either the catalog gets no "
                   "traffic yet, or nothing links to /reviews and /best. Fix "
                   "distribution before optimising networks.")
    elif rows("Q9"):
        top = [r[0] for r in rows("Q9")[:5]]
        out.append(f"{plural(total_clicks, 'click')} logged. Top products by intent: "
                   f"{', '.join(top)} — these are the direct-deal applications "
                   f"worth filing.")

    if not out:
        out.append("Nothing actionable surfaced. Worth confirming the DSN points "
                   "at prod and not a local/empty database.")

    skipped = [q for q in QUERIES if q not in results]
    if skipped:
        out.append(f"(Partial run — {', '.join(skipped)} not executed, so this "
                   f"verdict is drawn from {len(results)} of {len(QUERIES)} "
                   f"sections.)")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--only", nargs="+", metavar="QID",
                    help="run only these sections, e.g. --only Q4 Q6")
    ap.add_argument("--check-live", action="store_true",
                    help="also fetch a rendered page to confirm the Associate "
                         "tag is live in prod")
    ap.add_argument("--json", action="store_true",
                    help="emit machine-readable JSON instead of tables")
    args = ap.parse_args()

    wanted = [q.upper() for q in args.only] if args.only else list(QUERIES)
    unknown = [q for q in wanted if q not in QUERIES]
    if unknown:
        print(f"unknown section(s): {', '.join(unknown)}. "
              f"Known: {', '.join(QUERIES)}", file=sys.stderr)
        return 2

    results: dict = {}
    try:
        conn_ctx = healthnation_db.connect()
    except RuntimeError as exc:          # DSN not configured — config, not a crash
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:             # bad DSN, host unreachable, auth refused
        print(f"error: could not connect to the healthnation DB: {exc}",
              file=sys.stderr)
        return 2

    with conn_ctx as conn:
        for qid in wanted:
            heading, note, sql = QUERIES[qid]
            try:
                cols, rws = run(conn, sql)
            except Exception as exc:
                results[qid] = {"heading": heading, "error": str(exc)}
                continue
            results[qid] = {"heading": heading, "note": note,
                            "columns": cols, "rows": rws}

        live = check_live(conn) if args.check_live else None

    if args.json:
        payload = {
            qid: {k: (v if k != "rows" else [list(map(_jsonable, r)) for r in v])
                  for k, v in body.items()}
            for qid, body in results.items()
        }
        if live is not None:
            payload["live_check"] = live
        payload["verdict"] = verdict(results)
        print(json.dumps(payload, indent=2, default=str))
        return 0

    print("\nHEALTHNATION AFFILIATE AUDIT")
    print("=" * 72)
    for qid in wanted:
        body = results[qid]
        print(f"\n{qid}  {body['heading']}")
        if "error" in body:
            print(f"  ERROR: {body['error']}")
            continue
        print(f"  ↳ {body['note']}")
        print()
        print_table(body["columns"], body["rows"])

    if live is not None:
        print("\nLIVE CHECK — is AMAZON_PARTNER_TAG set in Vercel prod?")
        print("=" * 72)
        for k in ("url", "anchors", "tagged", "tags"):
            if k in live:
                print(f"  {k}: {live[k]}")
        print(f"  VERDICT: {live['verdict']}")
        print(f"  {live['detail']}")

    print("\nVERDICT — what the numbers support")
    print("=" * 72)
    for line in verdict(results):
        print(f"  · {line}")
    print()
    return 0


def _jsonable(v):
    return v if isinstance(v, (str, int, float, bool, type(None))) else str(v)


if __name__ == "__main__":
    raise SystemExit(main())
