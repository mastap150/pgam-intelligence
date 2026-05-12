"""
agents/enrichment/app_name_enrichment.py

Resolves bundle IDs from LL/TB inventory tables into readable app
names via iTunes Search API.

Bundle ID formats:
  - Numeric  (e.g. "6759081967")   → iOS App Store IDs
  - Reverse-DNS (e.g. "com.x.y")    → Android packages OR iOS bundle IDs

iTunes Search API handles both:
  https://itunes.apple.com/lookup?id={numeric}
  https://itunes.apple.com/lookup?bundleId={reverse_dns}

It's free, no auth required, and unofficially rate-limited to ~20
req/min. We respect that with a 3.5s sleep between calls.

Strategy:
  1. Pull top N bundles by trailing-30d revenue from
     ll_daily_publisher_bundle_demand. (N defaults to 500.)
  2. For each bundle that doesn't have fresh metadata
     (last_fetched < 30 days ago OR app_name IS NULL), call iTunes.
  3. UPSERT into pgam_direct.app_metadata. Track 404s as
     fetch_attempts increments so we don't hammer a missing app.

Reverse-DNS bundles that iTunes can't find are likely Android-only.
v1 just marks platform='unknown'. Future work: Google Play scraping
via the `google-play-scraper` Node package or playwright.

Schedule: daily 04:30 ET via scheduler.py — once a day is plenty
since app names don't churn.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Optional

from core.neon import connect

ITUNES_LOOKUP = "https://itunes.apple.com/lookup"
TOP_N_DEFAULT = 500
FRESH_DAYS = 30
RATE_LIMIT_SECONDS = 3.5
USER_AGENT = "PGAM-Intelligence app-name-enrichment/1.0"

# ---------------------------------------------------------------------------
# Bundle classification — numeric vs reverse-DNS. Empty / unknown
# shapes get skipped.
# ---------------------------------------------------------------------------

_NUMERIC = re.compile(r"^\d{6,12}$")
_REVERSE_DNS = re.compile(r"^[a-z][\w.\-]*(\.[a-z][\w.\-]*)+$", re.IGNORECASE)


def _classify(bundle: str) -> str:
    if not bundle:
        return "unknown"
    if _NUMERIC.match(bundle):
        return "ios_numeric"
    if _REVERSE_DNS.match(bundle):
        return "reverse_dns"
    return "unknown"


# ---------------------------------------------------------------------------
# iTunes Search API
# ---------------------------------------------------------------------------

def _itunes_lookup(bundle: str, kind: str) -> Optional[dict]:
    """Returns iTunes result dict, or None if not found / failed."""
    if kind == "ios_numeric":
        url = f"{ITUNES_LOOKUP}?{urllib.parse.urlencode({'id': bundle})}"
    elif kind == "reverse_dns":
        url = f"{ITUNES_LOOKUP}?{urllib.parse.urlencode({'bundleId': bundle})}"
    else:
        return None
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = json.loads(r.read().decode())
    except Exception as exc:
        print(f"[app_name_enrichment] iTunes error {bundle}: {exc}", flush=True)
        return None
    results = body.get("results") or []
    if not results:
        return None
    return results[0]


# ---------------------------------------------------------------------------
# Top-N bundles to resolve
# ---------------------------------------------------------------------------

def _fetch_top_bundles(conn, top_n: int, fresh_days: int) -> list[str]:
    """Bundle IDs ordered by trailing 30d revenue, excluding ones we
    already have fresh metadata for."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.bundle
              FROM (
                SELECT bundle, SUM(gross_revenue) AS gross
                  FROM pgam_direct.ll_daily_publisher_bundle_demand
                 WHERE report_date >= CURRENT_DATE - 30
                 GROUP BY bundle
                 HAVING SUM(gross_revenue) > 0
                 ORDER BY SUM(gross_revenue) DESC
                 LIMIT %s
              ) AS b
              LEFT JOIN pgam_direct.app_metadata m ON m.bundle_id = b.bundle
             WHERE m.bundle_id IS NULL
                OR m.last_fetched < now() - (INTERVAL '1 day' * %s)
                OR m.app_name IS NULL
            """,
            (top_n, fresh_days),
        )
        return [r[0] for r in cur.fetchall() if r[0]]


# ---------------------------------------------------------------------------
# UPSERT
# ---------------------------------------------------------------------------

_UPSERT = """
INSERT INTO pgam_direct.app_metadata
    (bundle_id, app_name, developer, platform, genre, icon_url, store_url,
     source, last_fetched, last_resolved, fetch_attempts, updated_at)
VALUES (%(bundle_id)s, %(app_name)s, %(developer)s, %(platform)s, %(genre)s,
        %(icon_url)s, %(store_url)s, %(source)s, now(), %(last_resolved)s, 1, now())
ON CONFLICT (bundle_id) DO UPDATE
   SET app_name      = COALESCE(EXCLUDED.app_name,      pgam_direct.app_metadata.app_name),
       developer     = COALESCE(EXCLUDED.developer,     pgam_direct.app_metadata.developer),
       platform      = COALESCE(EXCLUDED.platform,      pgam_direct.app_metadata.platform),
       genre         = COALESCE(EXCLUDED.genre,         pgam_direct.app_metadata.genre),
       icon_url      = COALESCE(EXCLUDED.icon_url,      pgam_direct.app_metadata.icon_url),
       store_url     = COALESCE(EXCLUDED.store_url,     pgam_direct.app_metadata.store_url),
       source        = EXCLUDED.source,
       last_fetched  = now(),
       last_resolved = COALESCE(EXCLUDED.last_resolved, pgam_direct.app_metadata.last_resolved),
       fetch_attempts = pgam_direct.app_metadata.fetch_attempts + 1,
       updated_at    = now()
"""


def _normalise_itunes(bundle: str, result: dict) -> dict:
    """Pull the fields we care about out of an iTunes Search result."""
    return {
        "bundle_id": bundle,
        "app_name":  (result.get("trackName") or "").strip() or None,
        "developer": (result.get("artistName") or "").strip() or None,
        "platform":  "ios",
        "genre":     (result.get("primaryGenreName") or "").strip() or None,
        "icon_url":  result.get("artworkUrl100"),
        "store_url": result.get("trackViewUrl"),
        "source":    "itunes",
        "last_resolved": "now()",  # set via SQL literal below
    }


def _miss(bundle: str, kind: str) -> dict:
    """Record a miss so we don't re-hit iTunes on every run."""
    return {
        "bundle_id": bundle,
        "app_name":  None,
        "developer": None,
        "platform":  "unknown" if kind == "unknown" else ("android" if kind == "reverse_dns" else "ios"),
        "genre":     None,
        "icon_url":  None,
        "store_url": None,
        "source":    "unknown",
        "last_resolved": None,
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def run(top_n: int = TOP_N_DEFAULT, fresh_days: int = FRESH_DAYS,
        max_calls: int | None = None) -> dict:
    started = time.time()
    with connect() as conn:
        bundles = _fetch_top_bundles(conn, top_n, fresh_days)
        if max_calls:
            bundles = bundles[:max_calls]
        if not bundles:
            print("[app_name_enrichment] nothing to fetch — all top bundles have fresh metadata", flush=True)
            return {"ok": True, "resolved": 0, "missed": 0, "skipped": 0}

        print(f"[app_name_enrichment] resolving {len(bundles)} bundles "
              f"(top-N={top_n}, max_calls={max_calls})", flush=True)

        resolved = 0
        missed = 0
        records: list[dict] = []
        for i, bundle in enumerate(bundles, 1):
            kind = _classify(bundle)
            if kind == "unknown":
                records.append(_miss(bundle, kind))
                missed += 1
                continue
            result = _itunes_lookup(bundle, kind)
            if result and result.get("trackName"):
                records.append(_normalise_itunes(bundle, result))
                resolved += 1
            else:
                records.append(_miss(bundle, kind))
                missed += 1
            if i % 25 == 0:
                print(f"[app_name_enrichment]   {i}/{len(bundles)} (resolved={resolved}, missed={missed})", flush=True)
            time.sleep(RATE_LIMIT_SECONDS)

        # UPSERT — we can't pass NOW() through psycopg params so do
        # rows one at a time but using a parameterised query that
        # handles last_resolved=NULL vs now() via COALESCE on the
        # `now()` SQL literal at the column default.
        with conn.cursor() as cur:
            for rec in records:
                # Convert sentinel "now()" string to real None vs let
                # the SQL default fire. Keep it simple: send None when
                # we didn't resolve, and a literal timestamp via SQL
                # when we did.
                params = dict(rec)
                params["last_resolved"] = None if rec["last_resolved"] is None else None
                # We can't easily mix literal now() with parameter
                # binding. Two-pass: when resolved, separately UPDATE
                # last_resolved=now() right after the UPSERT.
                cur.execute(_UPSERT, params)
                if rec["source"] == "itunes" and rec["app_name"]:
                    cur.execute(
                        "UPDATE pgam_direct.app_metadata SET last_resolved = now() WHERE bundle_id = %s",
                        (rec["bundle_id"],),
                    )
        conn.commit()

    elapsed = round(time.time() - started, 1)
    print(f"[app_name_enrichment] DONE — resolved {resolved}, missed {missed} in {elapsed}s", flush=True)
    return {"ok": True, "resolved": resolved, "missed": missed, "elapsed_s": elapsed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=TOP_N_DEFAULT,
                        help="How many top bundles to consider (default 500)")
    parser.add_argument("--max-calls", type=int, default=None,
                        help="Cap iTunes calls this run (for testing)")
    parser.add_argument("--fresh-days", type=int, default=FRESH_DAYS,
                        help="Skip bundles fetched within this many days")
    args = parser.parse_args()
    result = run(top_n=args.top_n, fresh_days=args.fresh_days, max_calls=args.max_calls)
    sys.exit(0 if result.get("ok") else 1)
