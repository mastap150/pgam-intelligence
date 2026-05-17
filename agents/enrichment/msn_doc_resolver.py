"""
agents/enrichment/msn_doc_resolver.py

Lazy resolver for MSN docID → boxingnews.com canonical URL + thumbnail.

The realtime ETL (agents.etl.msn_insights_etl) inserts a row into
pgam_direct.msn_article_meta with resolve_status='pending' the first
time it sees a docID. This agent walks the pending queue, fetches the
public MSN article page, and tries to extract:

  1. msn_url        — the actual MSN URL we landed on (after redirects)
  2. canonical_url  — the source boxingnews.com URL the MSN page links
                      to. MSN syndication pages typically embed this
                      either as an anchor in the article body, in a
                      "View original" CTA, or in JSON-LD metadata.
  3. thumbnail_url  — og:image from the MSN page (works ~always)
  4. canonical_title — og:title from the MSN page

This runs out-of-band on a slower cadence (default: every 30 min,
batch of up to MAX_BATCH docIDs per run) so we never hammer MSN. New
docIDs typically have meta resolved within the hour they're discovered.

We try multiple URL patterns because MSN's public URL scheme requires
a category slug + article slug + `ar-{docID}`; we don't know the slugs
upfront, so we use the share-URL form which redirects to the canonical.

resolve_status state machine:
  pending  → ok       — got everything we wanted
  pending  → failed   — fetched the page but couldn't find a boxingnews link
  pending  → gone     — MSN returned 404; article delisted, don't retry
  failed   → ok       — retry succeeded (e.g. page now has the canonical link)

We cap resolve_attempts at MAX_ATTEMPTS so a permanently broken docID
doesn't keep hogging the worker.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import traceback
from typing import Any, Optional

import requests

from core.neon import connect

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# MSN public URLs follow the pattern:
#   https://www.msn.com/{locale}/{vertical}/{subcategory}/{slug}/ar-{docID}
# We don't know the vertical/subcategory/slug, so we try the "topic"
# fallback URL which MSN serves and 301s to the canonical article.
_URL_CANDIDATES = (
    "https://www.msn.com/en-us/news/other/article/ar-{doc_id}",
    "https://www.msn.com/en-us/news/article/ar-{doc_id}",
    "https://www.msn.com/en-us/article/ar-{doc_id}",
)

MAX_BATCH = 50
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SEC = 15
INTER_REQUEST_SLEEP_SEC = 0.7  # gentle on MSN; ~85 docs/min ceiling

# Browser-ish UA. MSN sometimes serves a thin "preview" page to bot UAs
# that strips the syndication source link.
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

# Permissive boxingnews URL pattern. MSN sometimes encodes the source
# URL with HTML entities or wraps it in a tracking redirect; matching
# the host substring catches both.
_BOXINGNEWS_HREF_RE = re.compile(
    r'href=["\']([^"\']*boxingnews\.com[^"\']*)["\']',
    re.IGNORECASE,
)
_BOXINGNEWS_PLAIN_RE = re.compile(
    r'https?://(?:www\.)?boxingnews\.com/[A-Za-z0-9\-/_?=&%.]+',
    re.IGNORECASE,
)

# OG / Twitter meta scrape — cheap, regex-based to avoid pulling bs4
# unless we really need it. MSN's HTML is rendered server-side enough
# for og:image and og:title to be in the initial response.
_META_RE_TEMPLATE = (
    r'<meta\s+(?:property|name)=["\']{key}["\']\s+content=["\']([^"\']+)["\']'
)


def _meta(html: str, key: str) -> Optional[str]:
    match = re.search(_META_RE_TEMPLATE.format(key=re.escape(key)), html, re.IGNORECASE)
    return match.group(1) if match else None


def _find_canonical_boxingnews_url(html: str) -> Optional[str]:
    """Find the boxingnews.com URL the MSN page links back to.

    MSN renders syndicated articles with a "View original" anchor and
    typically a source attribution somewhere in the body. We try the
    anchor href first (more reliable), fall back to any plain URL match.
    Returns None if no boxingnews.com reference is in the HTML.
    """
    m = _BOXINGNEWS_HREF_RE.search(html)
    if m:
        return _clean_url(m.group(1))
    m = _BOXINGNEWS_PLAIN_RE.search(html)
    if m:
        return _clean_url(m.group(0))
    return None


def _clean_url(url: str) -> str:
    """Strip MSN tracking params and HTML entities."""
    url = url.replace("&amp;", "&").strip()
    # Strip MSN's outbound tracking — boxingnews URLs sometimes get
    # wrapped in a redirector like /redir?to={url}. If we can extract
    # the inner URL, use it.
    if "boxingnews.com" not in url:
        return url
    # Drop common MSN tracking params (ocid, cvid, ei) but keep meaningful
    # query strings (article queries, UTMs from BN itself).
    for param in ("ocid=", "cvid=", "ei=", "rwndsearch="):
        if param in url:
            url = re.sub(rf"[?&]{param}[^&]*", "", url)
    # Drop trailing punctuation that's commonly mis-matched
    return url.rstrip(").,;'\"")


# ---------------------------------------------------------------------------
# Queue queries
# ---------------------------------------------------------------------------

_QUEUE_SQL = """
SELECT doc_id, partner_id, resolve_status, resolve_attempts
  FROM pgam_direct.msn_article_meta
 WHERE resolve_status = 'pending'
    OR (resolve_status = 'failed' AND resolve_attempts < %(max_attempts)s)
 ORDER BY first_seen_at ASC
 LIMIT %(limit)s;
"""

_OK_SQL = """
UPDATE pgam_direct.msn_article_meta
   SET msn_url           = %(msn_url)s,
       canonical_url     = %(canonical_url)s,
       thumbnail_url     = %(thumbnail_url)s,
       canonical_title   = %(canonical_title)s,
       last_resolved_at  = now(),
       resolve_attempts  = resolve_attempts + 1,
       resolve_status    = 'ok',
       resolve_error     = NULL
 WHERE doc_id = %(doc_id)s;
"""

_FAIL_SQL = """
UPDATE pgam_direct.msn_article_meta
   SET msn_url           = COALESCE(%(msn_url)s, msn_url),
       thumbnail_url     = COALESCE(%(thumbnail_url)s, thumbnail_url),
       canonical_title   = COALESCE(%(canonical_title)s, canonical_title),
       last_resolved_at  = now(),
       resolve_attempts  = resolve_attempts + 1,
       resolve_status    = %(status)s,
       resolve_error     = %(error)s
 WHERE doc_id = %(doc_id)s;
"""


def _fetch_doc(doc_id: str, session: requests.Session) -> dict[str, Any]:
    """Fetch a docID's MSN page, return a dict of what we parsed.

    Returns:
      {
        "status":         'ok' | 'failed' | 'gone',
        "msn_url":        str | None,
        "canonical_url":  str | None,
        "thumbnail_url":  str | None,
        "canonical_title": str | None,
        "error":          str | None,
      }
    """
    last_resp: Optional[requests.Response] = None
    for tmpl in _URL_CANDIDATES:
        url = tmpl.format(doc_id=doc_id)
        try:
            resp = session.get(
                url,
                timeout=REQUEST_TIMEOUT_SEC,
                allow_redirects=True,
                headers={"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"},
            )
        except requests.RequestException as exc:
            return {
                "status": "failed",
                "msn_url": None,
                "canonical_url": None,
                "thumbnail_url": None,
                "canonical_title": None,
                "error": f"request error: {exc}",
            }
        last_resp = resp
        if resp.status_code == 404:
            continue  # try next candidate
        if resp.status_code >= 500:
            return {
                "status": "failed",
                "msn_url": resp.url,
                "canonical_url": None,
                "thumbnail_url": None,
                "canonical_title": None,
                "error": f"HTTP {resp.status_code}",
            }
        if 200 <= resp.status_code < 300:
            break
    else:
        # All candidates returned 404 → article is genuinely gone.
        return {
            "status": "gone",
            "msn_url": last_resp.url if last_resp is not None else None,
            "canonical_url": None,
            "thumbnail_url": None,
            "canonical_title": None,
            "error": "all url candidates returned 404",
        }

    assert last_resp is not None
    html = last_resp.text
    canonical_url   = _find_canonical_boxingnews_url(html)
    thumbnail_url   = _meta(html, "og:image")
    canonical_title = _meta(html, "og:title")

    if canonical_url:
        return {
            "status": "ok",
            "msn_url": last_resp.url,
            "canonical_url": canonical_url,
            "thumbnail_url": thumbnail_url,
            "canonical_title": canonical_title,
            "error": None,
        }
    return {
        "status": "failed",
        "msn_url": last_resp.url,
        "canonical_url": None,
        "thumbnail_url": thumbnail_url,
        "canonical_title": canonical_title,
        "error": "no boxingnews.com link in page body",
    }


def run(
    *,
    batch_size: int = MAX_BATCH,
    max_attempts: int = MAX_ATTEMPTS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Resolve up to `batch_size` pending docIDs in this run.

    Returns a status dict like:
        {"ok": True, "attempted": 30, "ok_count": 27, "failed": 2, "gone": 1}
    """
    t0 = time.perf_counter()
    attempted = 0
    ok_count = 0
    failed_count = 0
    gone_count = 0
    err: Optional[str] = None

    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_QUEUE_SQL, {"limit": batch_size, "max_attempts": max_attempts})
                queue = cur.fetchall()

        if not queue:
            elapsed = round(time.perf_counter() - t0, 2)
            print("[msn_doc_resolver] no pending docs — queue empty")
            return {"ok": True, "attempted": 0, "elapsed_seconds": elapsed}

        print(f"[msn_doc_resolver] resolving {len(queue)} docs (dry_run={dry_run})")
        session = requests.Session()

        for doc_id, partner_id, _status, _attempts in queue:
            attempted += 1
            try:
                result = _fetch_doc(doc_id, session)
            except Exception as exc:  # noqa: BLE001
                result = {
                    "status": "failed",
                    "msn_url": None,
                    "canonical_url": None,
                    "thumbnail_url": None,
                    "canonical_title": None,
                    "error": f"unexpected exception: {exc}",
                }
                traceback.print_exc()

            print(
                f"[msn_doc_resolver]   {doc_id} -> {result['status']:6s} "
                f"canon={(result.get('canonical_url') or '-')[:80]}"
            )

            if dry_run:
                pass
            elif result["status"] == "ok":
                ok_count += 1
                with connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(_OK_SQL, {
                            "doc_id":          doc_id,
                            "msn_url":         result["msn_url"],
                            "canonical_url":   result["canonical_url"],
                            "thumbnail_url":   result["thumbnail_url"],
                            "canonical_title": result["canonical_title"],
                        })
                    conn.commit()
            else:
                if result["status"] == "gone":
                    gone_count += 1
                else:
                    failed_count += 1
                with connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(_FAIL_SQL, {
                            "doc_id":          doc_id,
                            "msn_url":         result["msn_url"],
                            "thumbnail_url":   result["thumbnail_url"],
                            "canonical_title": result["canonical_title"],
                            "status":          result["status"],
                            "error":           (result["error"] or "")[:500],
                        })
                    conn.commit()

            time.sleep(INTER_REQUEST_SLEEP_SEC)

    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        print(f"[msn_doc_resolver] ✗ {err}")
        traceback.print_exc()

    elapsed = round(time.perf_counter() - t0, 2)
    summary = {
        "ok":               err is None,
        "attempted":        attempted,
        "ok_count":         ok_count,
        "failed":           failed_count,
        "gone":             gone_count,
        "elapsed_seconds":  elapsed,
        "error":            err,
    }
    print(f"[msn_doc_resolver] done: {summary}")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resolve MSN docIDs to boxingnews canonical URLs")
    parser.add_argument("--batch", type=int, default=MAX_BATCH,
                        help=f"Max docs to resolve this run (default {MAX_BATCH})")
    parser.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS,
                        help=f"Skip docs already retried this many times (default {MAX_ATTEMPTS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and parse, but don't write Neon updates")
    args = parser.parse_args()
    result = run(
        batch_size=args.batch,
        max_attempts=args.max_attempts,
        dry_run=args.dry_run,
    )
    sys.exit(0 if result["ok"] else 1)
