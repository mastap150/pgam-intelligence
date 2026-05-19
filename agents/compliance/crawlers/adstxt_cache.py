"""
agents/compliance/crawlers/adstxt_cache.py

Load + store helpers for the ads.txt conditional-GET cache.

The crawler is the only consumer. Each call to fetch_adstxt() can pass
through this module to:
  1. load the last cached ETag / Last-Modified header
  2. build conditional GET headers
  3. on 200, store the new etag/last_modified + parsed lines
  4. on 304, increment hit_count and reuse cached parse

Keeping cache I/O isolated here means the parser+fetcher stays pure and
unit-testable without Neon.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from core.neon import connect


@dataclass(frozen=True)
class CacheEntry:
    publisher_key: str
    variant: str
    etag: str | None
    last_modified: str | None
    body_sha256: str | None
    parsed_lines: list[dict]
    parsed_variables: dict


_LOAD_SQL = """
SELECT etag, last_modified, body_sha256, parsed_lines, parsed_variables
FROM pgam_direct.compliance_adstxt_cache
WHERE publisher_key = %(publisher_key)s
  AND variant       = %(variant)s
"""

# 200 path — replace cache row with fresh content + reset hit_count.
_STORE_FRESH_SQL = """
INSERT INTO pgam_direct.compliance_adstxt_cache
    (publisher_key, variant, etag, last_modified, body_sha256,
     parsed_lines, parsed_variables, fetched_at, hit_count)
VALUES
    (%(publisher_key)s, %(variant)s, %(etag)s, %(last_modified)s,
     %(body_sha256)s, %(parsed_lines)s::jsonb, %(parsed_variables)s::jsonb,
     now(), 0)
ON CONFLICT (publisher_key, variant) DO UPDATE SET
    etag             = EXCLUDED.etag,
    last_modified    = EXCLUDED.last_modified,
    body_sha256      = EXCLUDED.body_sha256,
    parsed_lines     = EXCLUDED.parsed_lines,
    parsed_variables = EXCLUDED.parsed_variables,
    fetched_at       = now(),
    hit_count        = 0
"""

# 304 path — bump hit_count, touch fetched_at, leave content alone.
_BUMP_HIT_SQL = """
UPDATE pgam_direct.compliance_adstxt_cache
SET hit_count  = hit_count + 1,
    fetched_at = now()
WHERE publisher_key = %(publisher_key)s
  AND variant       = %(variant)s
"""


def load_cache_entry(publisher_key: str, variant: str) -> CacheEntry | None:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_LOAD_SQL, {"publisher_key": publisher_key, "variant": variant})
                row = cur.fetchone()
    except Exception as exc:
        print(f"[adstxt_cache] load failed for {publisher_key}/{variant}: {exc}")
        return None
    if row is None:
        return None
    etag, last_modified, body_sha256, parsed_lines, parsed_variables = row
    return CacheEntry(
        publisher_key=publisher_key, variant=variant,
        etag=etag, last_modified=last_modified, body_sha256=body_sha256,
        parsed_lines=parsed_lines or [],
        parsed_variables=parsed_variables or {},
    )


def store_fresh_entry(
    publisher_key: str, variant: str,
    *, etag: str | None, last_modified: str | None,
    body_sha256: str | None,
    parsed_lines: list[dict],
    parsed_variables: dict,
) -> None:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_STORE_FRESH_SQL, {
                    "publisher_key":    publisher_key,
                    "variant":          variant,
                    "etag":             etag,
                    "last_modified":    last_modified,
                    "body_sha256":      body_sha256,
                    "parsed_lines":     json.dumps(parsed_lines),
                    "parsed_variables": json.dumps(parsed_variables),
                })
            conn.commit()
    except Exception as exc:
        print(f"[adstxt_cache] store failed for {publisher_key}/{variant}: {exc}")


def bump_cache_hit(publisher_key: str, variant: str) -> None:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_BUMP_HIT_SQL, {
                    "publisher_key": publisher_key, "variant": variant,
                })
            conn.commit()
    except Exception as exc:
        print(f"[adstxt_cache] hit bump failed for {publisher_key}/{variant}: {exc}")


def conditional_headers(entry: CacheEntry | None) -> dict[str, str]:
    """Build If-None-Match / If-Modified-Since headers from a cache entry."""
    headers: dict[str, str] = {}
    if entry is None:
        return headers
    if entry.etag:
        headers["If-None-Match"] = entry.etag
    if entry.last_modified:
        headers["If-Modified-Since"] = entry.last_modified
    return headers
