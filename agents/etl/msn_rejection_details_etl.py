"""
agents/etl/msn_rejection_details_etl.py

Loads MSN Partner Hub "Content Rejection Report — Details" CSV variant.
This is the per-doc rejection export (one row per rejected article) —
strictly richer than the Overview variant (which just aggregates
reason → count).

Details CSV (real export shape from 2026-08-09):

    Line 1: "Data update time: 8/9/2026, 7:00:00 PM (UTC)"
    Line 2: Brand name,Feed name,Content type,Document ID,Source ID,
            Source type,Canonical URL,Content title,Appeal availability,
            Appeal status,Last updated time,Rejection details #1
    Line 3+: One row per rejected doc. `Rejection details #1` is a JSON
             blob like `{"reason": "...", "words": ["fuck"]}` or
             `{"reason": "...", "image urls": ["..."]}`.

Why this matters (vs the Overview loader):
  - Per-doc granularity → joinable to msn_article_snapshots.doc_id and
    boxingnews.articles via canonical_url
  - Actual flagged words → generators can auto-scrub them out
  - Actual flagged image URLs → thumbnail resolver / image picker can
    blacklist patterns
  - Appeal status / availability → tracks what MSN thinks we could
    appeal vs terminal rejections

Load path:
  1. Parse header line for data_update_time (window anchor)
  2. Parse each row → RejectedDoc record with:
     - partner_id (BoxingNews via PGAM = AA1lKiff)
     - doc_id
     - canonical_url
     - content_title
     - rejection_reason (text)
     - flagged_words (text[]) — pulled from JSON blob when present
     - flagged_image_urls (text[]) — pulled when present
     - failure_category (moderation vs ingestion — inferred from reason)
     - last_updated_at
     - appeal_availability, appeal_status
     - data_update_time (of this CSV)
     - source_filename
  3. UPSERT on (partner_id, doc_id, last_updated_at) — the same doc can
     appear in multiple exports as long as its rejection timestamp is
     the same

Idempotent: re-loading the same CSV is a no-op. Loading a later CSV
that includes the same doc keeps the earliest and any newer records
side by side.

Run from CLI:

    python3 -m agents.etl.msn_rejection_details_etl \\
        ~/Downloads/Content\\ Rejection\\ Report-...Details.csv
"""

from __future__ import annotations

import csv
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional

# Enable running as a script — repo root on sys.path.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.neon import connect  # noqa: E402

PARTNER_ID_DEFAULT = 'AA1lKiff'

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- Per-doc MSN rejection records. Populated from the Partner Hub
-- Details CSV export (agents/etl/msn_rejection_details_etl.py).
-- Every rejected article gets one row per rejection timestamp; the
-- same doc rejected twice at different times has two rows.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_rejection_docs (
    partner_id           TEXT        NOT NULL,
    doc_id               TEXT        NOT NULL,
    -- MSN's `Last updated time` — timestamp of the rejection decision.
    -- Part of the PK so we can capture re-rejection history if MSN
    -- rejects the same doc twice (e.g. after an appeal that failed).
    last_updated_at      TIMESTAMPTZ NOT NULL,

    brand_name           TEXT,
    feed_name            TEXT,
    content_type         TEXT,        -- "Article" | "Video" | "Image"
    canonical_url        TEXT,
    source_id            TEXT,
    source_type          TEXT,        -- typically "Feed"
    content_title        TEXT,

    rejection_reason     TEXT        NOT NULL,
    -- Free-form category inferred from rejection_reason. One of:
    -- 'profanity' | 'graphic-image' | 'thumbnail-size' | 'moderation-error'
    -- | 'ingestion-error' | 'other'
    failure_category     TEXT        NOT NULL,
    -- Flagged word list from the JSON blob (when profanity is the
    -- reason). May be empty even when category='profanity' — MSN
    -- doesn't always populate the words[] field.
    flagged_words        TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    -- Flagged image URLs (when reason is a graphic-image call). Kept
    -- separate from words to keep the array types clean.
    flagged_image_urls   TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],

    appeal_availability  TEXT,        -- "Appealable" | "Not Appealable"
    appeal_status        TEXT,        -- "N/A" | "Submitted" | "Denied" | ...

    -- Metadata from the CSV export header.
    data_update_time     TIMESTAMPTZ NOT NULL,
    source_filename      TEXT,
    imported_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (partner_id, doc_id, last_updated_at)
);

-- The join key we care about most in analytics — this article was
-- rejected recently, was it in the feed's realtime window?
CREATE INDEX IF NOT EXISTS idx_msn_rejection_docs_doc
    ON pgam_direct.msn_rejection_docs (partner_id, doc_id);

-- Time-range scans for "recent rejections" dashboards.
CREATE INDEX IF NOT EXISTS idx_msn_rejection_docs_time
    ON pgam_direct.msn_rejection_docs (partner_id, last_updated_at DESC);

-- Reason breakdown for the weekly-review agent.
CREATE INDEX IF NOT EXISTS idx_msn_rejection_docs_category
    ON pgam_direct.msn_rejection_docs (partner_id, failure_category, last_updated_at DESC);

COMMENT ON TABLE pgam_direct.msn_rejection_docs IS
'Per-doc rejection records from MSN Partner Hub Details CSV.
Populated by agents.etl.msn_rejection_details_etl. Joinable to
msn_article_snapshots via doc_id and to boxingnews.articles via
canonical_url (extract slug from the tail).';
"""


_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_rejection_docs (
    partner_id, doc_id, last_updated_at,
    brand_name, feed_name, content_type,
    canonical_url, source_id, source_type, content_title,
    rejection_reason, failure_category,
    flagged_words, flagged_image_urls,
    appeal_availability, appeal_status,
    data_update_time, source_filename
) VALUES (
    %(partner_id)s, %(doc_id)s, %(last_updated_at)s,
    %(brand_name)s, %(feed_name)s, %(content_type)s,
    %(canonical_url)s, %(source_id)s, %(source_type)s, %(content_title)s,
    %(rejection_reason)s, %(failure_category)s,
    %(flagged_words)s, %(flagged_image_urls)s,
    %(appeal_availability)s, %(appeal_status)s,
    %(data_update_time)s, %(source_filename)s
)
ON CONFLICT (partner_id, doc_id, last_updated_at) DO UPDATE SET
    brand_name          = EXCLUDED.brand_name,
    feed_name           = EXCLUDED.feed_name,
    content_type        = EXCLUDED.content_type,
    canonical_url       = EXCLUDED.canonical_url,
    source_id           = EXCLUDED.source_id,
    source_type         = EXCLUDED.source_type,
    content_title       = EXCLUDED.content_title,
    rejection_reason    = EXCLUDED.rejection_reason,
    failure_category    = EXCLUDED.failure_category,
    flagged_words       = EXCLUDED.flagged_words,
    flagged_image_urls  = EXCLUDED.flagged_image_urls,
    appeal_availability = EXCLUDED.appeal_availability,
    appeal_status       = EXCLUDED.appeal_status,
    data_update_time    = EXCLUDED.data_update_time,
    source_filename     = EXCLUDED.source_filename,
    imported_at         = now();
"""


@dataclass
class RejectedDoc:
    partner_id: str
    doc_id: str
    last_updated_at: datetime
    brand_name: Optional[str]
    feed_name: Optional[str]
    content_type: Optional[str]
    canonical_url: Optional[str]
    source_id: Optional[str]
    source_type: Optional[str]
    content_title: Optional[str]
    rejection_reason: str
    failure_category: str
    flagged_words: List[str] = field(default_factory=list)
    flagged_image_urls: List[str] = field(default_factory=list)
    appeal_availability: Optional[str] = None
    appeal_status: Optional[str] = None


@dataclass
class ParsedDetails:
    partner_id: str
    data_update_time: datetime
    source_filename: str
    docs: List[RejectedDoc]


# MSN's date format in the header (US style with AM/PM):
#   "Data update time: 8/9/2026, 7:00:00 PM (UTC)"
_HEADER_DATE_RX = re.compile(
    r'Data update time:\s*(\d{1,2})/(\d{1,2})/(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)\s*\(UTC\)',
    re.IGNORECASE,
)


def _parse_header_datetime(line: str) -> datetime:
    m = _HEADER_DATE_RX.search(line)
    if not m:
        raise ValueError(f'unrecognised header format: {line!r}')
    month, day, year, hour, minute, second, ampm = m.groups()
    h = int(hour) % 12
    if ampm.upper() == 'PM':
        h += 12
    return datetime(
        int(year), int(month), int(day), h, int(minute), int(second),
        tzinfo=timezone.utc,
    )


def _categorise(reason: str) -> str:
    """Bucket the free-form reason into a small analytics-friendly set."""
    r = reason.lower()
    if 'profanity' in r or 'vulgar' in r:
        return 'profanity'
    if 'graphic violence' in r or 'graphic image' in r or 'depicts graphic' in r:
        return 'graphic-image'
    if 'thumbnail' in r or '300 x 300' in r or '300x300' in r:
        return 'thumbnail-size'
    if 'moderation service' in r:
        return 'moderation-error'
    if 'ingestion' in r:
        return 'ingestion-error'
    return 'other'


def _parse_rejection_blob(raw: str) -> tuple[str, list[str], list[str]]:
    """The Rejection details #1 field is a JSON blob like:
        {"reason":"...","words":["fuck"]}
    or:
        {"reason":"...","image urls":["https://..."]}
    Some rows omit the arrays entirely.
    """
    if not raw:
        return ('', [], [])
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        # Some exports have trailing quote escapes that break strict
        # JSON — best-effort fallback: return the raw text as reason.
        return (raw[:500], [], [])
    reason = str(obj.get('reason', '')).strip()
    # MSN uses both "words" and "image urls" keys.
    words_raw = obj.get('words') or []
    imgs_raw = obj.get('image urls') or obj.get('imageUrls') or []
    words = [str(w) for w in words_raw if isinstance(w, str)]
    imgs = [str(u) for u in imgs_raw if isinstance(u, str)]
    return (reason, words, imgs)


def _parse_iso(ts: str) -> datetime:
    """MSN's per-row Last updated time is ISO 8601 with a `+00:00` tail."""
    # Python 3.11+ handles the tz offset natively via fromisoformat.
    return datetime.fromisoformat(ts.strip())


def parse_details_csv(path: Path, partner_id: str = PARTNER_ID_DEFAULT) -> ParsedDetails:
    """Parse the Details CSV into structured records."""
    with path.open('r', encoding='utf-8-sig') as fh:
        # First line: the update-time header.
        header_line = fh.readline().strip('"').strip()
        data_update_time = _parse_header_datetime(header_line)

        reader = csv.DictReader(fh)
        docs: list[RejectedDoc] = []
        for row in reader:
            # Skip blank lines / totals footers.
            doc_id = (row.get('Document ID') or '').strip()
            if not doc_id:
                continue
            reason, words, imgs = _parse_rejection_blob(row.get('Rejection details #1', ''))
            category = _categorise(reason)
            try:
                last_updated = _parse_iso(row.get('Last updated time', ''))
            except (ValueError, TypeError):
                # A row we can't timestamp is not usefully joinable —
                # skip it noisily. Never silently drop a row.
                print(f'[details-etl] skip {doc_id}: bad Last updated time', file=sys.stderr)
                continue
            docs.append(RejectedDoc(
                partner_id=partner_id,
                doc_id=doc_id,
                last_updated_at=last_updated,
                brand_name=(row.get('Brand name') or '').strip() or None,
                feed_name=(row.get('Feed name') or '').strip() or None,
                content_type=(row.get('Content type') or '').strip() or None,
                canonical_url=(row.get('Canonical URL') or '').strip() or None,
                source_id=(row.get('Source ID') or '').strip() or None,
                source_type=(row.get('Source type') or '').strip() or None,
                content_title=(row.get('Content title') or '').strip() or None,
                rejection_reason=reason,
                failure_category=category,
                flagged_words=words,
                flagged_image_urls=imgs,
                appeal_availability=(row.get('Appeal availability') or '').strip() or None,
                appeal_status=(row.get('Appeal status') or '').strip() or None,
            ))

    return ParsedDetails(
        partner_id=partner_id,
        data_update_time=data_update_time,
        source_filename=path.name,
        docs=docs,
    )


def ensure_schema() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()


def load(parsed: ParsedDetails) -> int:
    if not parsed.docs:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            for d in parsed.docs:
                cur.execute(_UPSERT_SQL, {
                    'partner_id':          d.partner_id,
                    'doc_id':              d.doc_id,
                    'last_updated_at':     d.last_updated_at,
                    'brand_name':          d.brand_name,
                    'feed_name':           d.feed_name,
                    'content_type':        d.content_type,
                    'canonical_url':       d.canonical_url,
                    'source_id':           d.source_id,
                    'source_type':         d.source_type,
                    'content_title':       d.content_title,
                    'rejection_reason':    d.rejection_reason,
                    'failure_category':    d.failure_category,
                    'flagged_words':       d.flagged_words,
                    'flagged_image_urls':  d.flagged_image_urls,
                    'appeal_availability': d.appeal_availability,
                    'appeal_status':       d.appeal_status,
                    'data_update_time':    parsed.data_update_time,
                    'source_filename':     parsed.source_filename,
                })
        conn.commit()
    return len(parsed.docs)


def _cli(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    path = Path(argv[1]).expanduser()
    if not path.exists():
        print(f'file not found: {path}', file=sys.stderr)
        return 1

    ensure_schema()
    parsed = parse_details_csv(path)
    n = load(parsed)

    # Compact summary — count by category.
    from collections import Counter
    cat_counts = Counter(d.failure_category for d in parsed.docs)
    all_words = [w for d in parsed.docs for w in d.flagged_words]
    word_counts = Counter(w.lower() for w in all_words)

    print(f'Loaded {n} rejection records from {path.name}')
    print(f'  data_update_time: {parsed.data_update_time.isoformat()}')
    print(f'  partner_id:       {parsed.partner_id}')
    print(f'  category breakdown:')
    for cat, count in cat_counts.most_common():
        print(f'    {cat:20s} {count}')
    if word_counts:
        print(f'  flagged words (top 10):')
        for word, count in word_counts.most_common(10):
            print(f'    {word!r:20s} {count}')
    return 0


if __name__ == '__main__':
    sys.exit(_cli(sys.argv))
