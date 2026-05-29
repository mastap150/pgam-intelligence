"""
agents/etl/msn_rejection_etl.py

Parses the "Content Rejection Report - Overview" CSV exported by hand
from MSN Partner Hub's home page → "Resolve content issues" → Download.

Schema (from a real export on 2026-05-29):

    Line 1: "Data update time: 5/29/2026, 4:15:00 AM (UTC)"
    Line 2: You've had N content items rejected in the last 7 days.,,
    Line 3: (blank)
    Line 4: Your content failure report is below. ... preamble ...
    Line 5: Rejection reason,Failure type,Total number of the failures
    Line 6+: "<reason>","<type>","<count>"

Failure types observed:
    - "Moderation failure"  : content policy (profanity, brand safety, etc.)
    - "Ingestion failure"   : feed/format problems (mRSS, image, schema)

Per-doc detail is NOT in this CSV — that requires the "Detail" export
variant from the same Download dropdown (TODO).

Why this matters: BoxingNews content generators currently have no
visibility into *why* MSN rejects things. Loading this report weekly
gives us:

  1. A rejection-rate KPI for admin.pgammedia.com/admin/msn-insights.
  2. Reason categorization the generators can react to (e.g., if
     profanity rejects rise, tighten the safe-content guard in the
     ingest-trending prompt).
  3. Moderation-vs-ingestion split — ingestion failures are *our*
     feed bugs, moderation failures are content choices.

Run from CLI:

    python3 -m agents.etl.msn_rejection_etl \
        ~/Downloads/Content\\ Rejection\\ Report-PGAM\\ Media\\ LLC-All\\ brands-*-Overview.csv

Idempotent: same window + same reason + same type → UPSERT, latest
count wins.
"""

from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from core.neon import connect

PARTNER_ID_DEFAULT = 'AA1lKiff'  # BoxingNews via PGAM Media LLC partner account

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- Aggregated rejection-report rows. One row per (partner, window,
-- reason, failure_type) tuple. The Overview CSV exports a single
-- rolling 7-day window per download; downstream queries can union
-- successive imports to build a time series.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_rejection_report (
    partner_id           TEXT        NOT NULL,
    window_start         TIMESTAMPTZ NOT NULL,
    window_end           TIMESTAMPTZ NOT NULL,
    rejection_reason     TEXT        NOT NULL,
    failure_type         TEXT        NOT NULL,
    failure_count        INTEGER     NOT NULL,
    data_update_time     TIMESTAMPTZ NOT NULL,
    source_filename      TEXT,
    imported_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, window_start, window_end, rejection_reason, failure_type)
);

CREATE INDEX IF NOT EXISTS idx_msn_rejection_window
    ON pgam_direct.msn_rejection_report (partner_id, window_end DESC);

COMMENT ON TABLE pgam_direct.msn_rejection_report IS
  'MSN Partner Hub "Content Rejection Report - Overview" CSV imports. '
  'Aggregate counts per rejection reason; per-doc data lives in the '
  'Detail variant (not yet ingested). Loaded by '
  'agents/etl/msn_rejection_etl.py from manual Downloads.';
"""

# Filename pattern e.g.
#   Content Rejection Report-PGAM Media LLC-All brands-202605221933UTC-202605291933UTC-Overview.csv
_FILENAME_WINDOW_RE = re.compile(
    r'-(\d{12})UTC-(\d{12})UTC-Overview\.csv$',
    re.IGNORECASE,
)
_DATA_UPDATE_RE = re.compile(
    r'Data update time:\s*([\d/]+,\s*[\d:]+\s*[AP]M)\s*\(UTC\)',
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RejectionRow:
    rejection_reason: str
    failure_type: str
    failure_count: int


@dataclass(frozen=True)
class RejectionReport:
    partner_id: str
    window_start: datetime
    window_end: datetime
    data_update_time: datetime
    source_filename: str
    rows: tuple[RejectionRow, ...]


def _parse_window_from_filename(name: str) -> tuple[datetime, datetime]:
    m = _FILENAME_WINDOW_RE.search(name)
    if not m:
        raise ValueError(
            f"Could not parse window from filename: {name!r}. "
            f"Expected ...-YYYYMMDDHHMMUTC-YYYYMMDDHHMMUTC-Overview.csv"
        )
    start_s, end_s = m.group(1), m.group(2)
    fmt = '%Y%m%d%H%M'
    start = datetime.strptime(start_s, fmt).replace(tzinfo=timezone.utc)
    end = datetime.strptime(end_s, fmt).replace(tzinfo=timezone.utc)
    return start, end


def _parse_data_update(line: str) -> datetime:
    m = _DATA_UPDATE_RE.search(line)
    if not m:
        # Fallback: use window_end if we can't parse. Caller substitutes.
        raise ValueError(f"No 'Data update time' header in line: {line!r}")
    # e.g. "5/29/2026, 4:15:00 AM"
    return datetime.strptime(m.group(1).strip(), '%m/%d/%Y, %I:%M:%S %p').replace(
        tzinfo=timezone.utc
    )


def parse_overview_csv(path: Path, *, partner_id: str = PARTNER_ID_DEFAULT) -> RejectionReport:
    """Parse the Overview CSV at `path`. Tolerant of the preamble lines
    that precede the header."""
    text = path.read_text(encoding='utf-8-sig')  # strip BOM if present
    lines = text.splitlines()

    # Top-line metadata
    data_update = None
    for line in lines[:5]:
        try:
            data_update = _parse_data_update(line)
            break
        except ValueError:
            continue

    window_start, window_end = _parse_window_from_filename(path.name)
    if data_update is None:
        # Use window_end as a best-effort
        data_update = window_end

    # Find the header row, then csv-parse from there.
    header_idx = None
    for i, line in enumerate(lines):
        if line.lower().startswith('rejection reason,'):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(
            f"No 'Rejection reason,Failure type,...' header found in {path.name}"
        )

    body = '\n'.join(lines[header_idx:])
    reader = csv.DictReader(body.splitlines())
    rows: list[RejectionRow] = []
    for raw in reader:
        reason = (raw.get('Rejection reason') or '').strip()
        ftype = (raw.get('Failure type') or '').strip()
        count_s = (raw.get('Total number of the failures') or '0').strip()
        if not reason and not ftype:
            continue
        try:
            count = int(count_s)
        except ValueError:
            continue
        rows.append(RejectionRow(reason, ftype, count))

    return RejectionReport(
        partner_id=partner_id,
        window_start=window_start,
        window_end=window_end,
        data_update_time=data_update,
        source_filename=path.name,
        rows=tuple(rows),
    )


_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_rejection_report
  (partner_id, window_start, window_end, rejection_reason, failure_type,
   failure_count, data_update_time, source_filename)
VALUES
  (%(partner_id)s, %(window_start)s, %(window_end)s,
   %(rejection_reason)s, %(failure_type)s, %(failure_count)s,
   %(data_update_time)s, %(source_filename)s)
ON CONFLICT (partner_id, window_start, window_end, rejection_reason, failure_type)
DO UPDATE SET
  failure_count    = EXCLUDED.failure_count,
  data_update_time = EXCLUDED.data_update_time,
  source_filename  = EXCLUDED.source_filename,
  imported_at      = now()
;
"""


def ensure_schema() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()


def load(report: RejectionReport) -> int:
    """Upsert the report rows. Returns row count written."""
    if not report.rows:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            for r in report.rows:
                cur.execute(_UPSERT_SQL, {
                    'partner_id':       report.partner_id,
                    'window_start':     report.window_start,
                    'window_end':       report.window_end,
                    'rejection_reason': r.rejection_reason,
                    'failure_type':     r.failure_type,
                    'failure_count':    r.failure_count,
                    'data_update_time': report.data_update_time,
                    'source_filename':  report.source_filename,
                })
        conn.commit()
    return len(report.rows)


def _cli(argv: list[str]) -> int:
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    path = Path(argv[1]).expanduser()
    if not path.exists():
        print(f'file not found: {path}', file=sys.stderr)
        return 1
    ensure_schema()
    report = parse_overview_csv(path)
    n = load(report)
    print(
        f'Loaded {n} rows from {path.name}\n'
        f'  partner_id:       {report.partner_id}\n'
        f'  window:           {report.window_start.isoformat()} → {report.window_end.isoformat()}\n'
        f'  data_update_time: {report.data_update_time.isoformat()}\n'
    )
    for r in report.rows:
        print(f'  {r.failure_type:20s}  {r.failure_count:4d}  {r.rejection_reason}')
    return 0


if __name__ == '__main__':
    sys.exit(_cli(sys.argv))
