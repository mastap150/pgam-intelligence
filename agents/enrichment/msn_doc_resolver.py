"""
agents/enrichment/msn_doc_resolver.py

Lazy resolver for MSN docID → boxingnews.com canonical URL + thumbnail.

The realtime ETL (agents.etl.msn_insights_etl) inserts a row into
pgam_direct.msn_article_meta with resolve_status='pending' the first
time it sees a docID. This agent walks the pending queue, calls MSN's
content API, and extracts:

  1. msn_url         — the MSN content API URL we hit
  2. canonical_url   — `sourceHref` from the JSON payload, which is the
                       boxingnews.com URL the MSN syndication is sourced
                       from (1:1 with our published article)
  3. thumbnail_url   — `imageResources[0].url`
  4. canonical_title — `title`

Previously this resolver scraped MSN's www.msn.com HTML pages for an
anchor pointing back to boxingnews.com. As of mid-2026 those pages are
a client-side SPA shell (`<title>MSN</title>`, empty `<div id="root">`,
no og:* tags) and contain no boxingnews reference at all — so every
docID was bucketing into resolve_status='failed' with
"no boxingnews.com link in page body". The fix is to hit MSN's content
API directly:

    GET https://assets.msn.com/content/view/v2/Detail/en-us/{docID}

which still serves the full article JSON server-side, including
sourceHref. We dropped the HTML candidates and regex scrapers entirely.

This runs out-of-band on a slower cadence (default: every 30 min,
batch of up to MAX_BATCH docIDs per run) so we never hammer MSN. New
docIDs typically have meta resolved within the hour they're discovered.

resolve_status state machine:
  pending  → ok       — got everything we wanted
  pending  → failed   — JSON had no sourceHref / wrong host
  pending  → gone     — MSN returned 404/410; article delisted, don't retry
  failed   → ok       — retry succeeded (e.g. transient API blip)

We cap resolve_attempts at MAX_ATTEMPTS so a permanently broken docID
doesn't keep hogging the worker.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from typing import Any, Optional

import requests

from core.neon import connect

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# MSN's content API. Serves the full article JSON for a given docID
# server-side (the public www.msn.com page is a client-rendered shell
# as of mid-2026 and contains no usable metadata). Note: this endpoint
# 404s on HEAD but 200s on GET — always use GET.
_JSON_API = "https://assets.msn.com/content/view/v2/Detail/en-us/{doc_id}"

# Host substring we expect inside sourceHref. Defensive: if MSN ever
# syndicates a non-boxingnews article into a docID that landed in our
# meta table, we'd rather flag it 'failed' than write a foreign URL.
_EXPECTED_HOST = "boxingnews.com"

MAX_BATCH = 50
MAX_ATTEMPTS = 5
REQUEST_TIMEOUT_SEC = 15
INTER_REQUEST_SLEEP_SEC = 0.7  # gentle on MSN; ~85 docs/min ceiling

# Browser-ish UA. The content API doesn't strictly require this, but
# it costs nothing and keeps us anonymous-looking in MSN's logs.
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)


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


def _first_image_url(payload: dict[str, Any]) -> Optional[str]:
    """Return the highest-quality image URL from the JSON payload, if any."""
    images = payload.get("imageResources") or []
    if not images:
        return None
    # MSN typically returns one item; if multiple, prefer the one with
    # the largest area as the hero thumbnail.
    def _area(img: dict[str, Any]) -> int:
        try:
            return int(img.get("width", 0)) * int(img.get("height", 0))
        except (TypeError, ValueError):
            return 0
    best = max(images, key=_area)
    url = best.get("url")
    return url or None


def _fetch_doc(doc_id: str, session: requests.Session) -> dict[str, Any]:
    """Fetch a docID's metadata from MSN's content API.

    Returns:
      {
        "status":         'ok' | 'failed' | 'gone',
        "msn_url":        str | None,   # the API URL we hit
        "canonical_url":  str | None,   # sourceHref from JSON
        "thumbnail_url":  str | None,
        "canonical_title": str | None,
        "error":          str | None,
      }
    """
    url = _JSON_API.format(doc_id=doc_id)
    try:
        resp = session.get(
            url,
            timeout=REQUEST_TIMEOUT_SEC,
            allow_redirects=True,
            headers={"User-Agent": _UA, "Accept": "application/json"},
        )
    except requests.RequestException as exc:
        return {
            "status": "failed",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": None,
            "canonical_title": None,
            "error": f"request error: {exc}",
        }

    # 404 / 410 → article is delisted; don't retry.
    if resp.status_code in (404, 410):
        return {
            "status": "gone",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": None,
            "canonical_title": None,
            "error": f"HTTP {resp.status_code}",
        }
    if resp.status_code >= 400:
        return {
            "status": "failed",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": None,
            "canonical_title": None,
            "error": f"HTTP {resp.status_code}",
        }

    try:
        payload = resp.json()
    except ValueError as exc:
        return {
            "status": "failed",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": None,
            "canonical_title": None,
            "error": f"json decode error: {exc}",
        }

    canonical_url = (payload.get("sourceHref") or "").strip() or None
    canonical_title = payload.get("title")
    thumbnail_url = _first_image_url(payload)

    if not canonical_url:
        return {
            "status": "failed",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": thumbnail_url,
            "canonical_title": canonical_title,
            "error": "no sourceHref in MSN payload",
        }
    if _EXPECTED_HOST not in canonical_url:
        return {
            "status": "failed",
            "msn_url": url,
            "canonical_url": None,
            "thumbnail_url": thumbnail_url,
            "canonical_title": canonical_title,
            "error": f"sourceHref host mismatch: {canonical_url[:120]}",
        }

    return {
        "status": "ok",
        "msn_url": url,
        "canonical_url": canonical_url,
        "thumbnail_url": thumbnail_url,
        "canonical_title": canonical_title,
        "error": None,
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
