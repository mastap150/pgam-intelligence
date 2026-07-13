"""
agents/etl/msn_partner_reports_etl.py

Pulls the three MSN Partner Hub "report" surfaces discovered via the
XHR sniffer on 2026-07-13 and lands them in Neon:

  1. Monthly earnings + interactions
     /msn/v0/pages/ugc/insights/earning/adsrev
     → pgam_direct.msn_earning_monthly

  2. Rolling 29-day publish-rate rollup
     /msn/v0/pages/ugc/contents/report/partnerdocstats
     → pgam_direct.msn_docstats_snapshot

  3. Automated rejection count (replaces manual CSV upload)
     /msn/v0/pages/ugc/contents/report/partnerrejecteddocstats
     → pgam_direct.msn_rejection_snapshot

Design choices
--------------
- Each pull is snapshotted with an `imported_at` / `snapshot_at`
  timestamp so we can build a time series from repeated pulls. MSN
  itself never gives us a per-day series for docstats/rejections;
  the series is *our* series, one row per successful ETL run.

- Idempotent: docstats and rejection snapshots use UPSERT keyed on
  (partner_id, snapshot_at) so a same-minute retry doesn't duplicate.

- Failure-isolated: any one endpoint failing doesn't block the others.
  A rejection endpoint hiccup shouldn't lose the month-boundary earnings
  snapshot we're actually here for.

Operating modes
---------------
- Default `run()`: pull all three and write.
- `run(dry_run=True)`: pull but skip writes; print summaries.
- `python -m agents.etl.msn_partner_reports_etl --once`
- `python -m agents.etl.msn_partner_reports_etl --dry-run`
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from core.msn_partner_hub import (
        DEFAULT_PARTNER_ID,
        PartnerHubClient,
        PartnerHubError,
    )
except ImportError as exc:  # pragma: no cover - import-time only
    PartnerHubClient = None  # type: ignore
    PartnerHubError = RuntimeError  # type: ignore
    _IMPORT_ERROR = exc
    DEFAULT_PARTNER_ID = "AA1lKiff"
else:
    _IMPORT_ERROR = None

from core.neon import connect


# ---------------------------------------------------------------------------
# Schema — embedded so first run on a fresh DB just works.
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS pgam_direct;

-- Monthly earnings + interactions. `report_month` is the first day
-- of the month the data pertains to. Rows are re-emitted every ETL
-- run for the last 12 months; we UPSERT on the same key so the
-- current-month row updates in place until MSN seals it.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_earning_monthly (
    partner_id       TEXT        NOT NULL,
    report_month     DATE        NOT NULL,
    interactions     BIGINT      NOT NULL,
    net_revenue_usd  NUMERIC(12, 4) NOT NULL,
    ads_amount_usd   NUMERIC(12, 4) NOT NULL,
    processed_date   DATE,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partner_id, report_month)
);

CREATE INDEX IF NOT EXISTS idx_msn_earning_monthly_partner_month
    ON pgam_direct.msn_earning_monthly (partner_id, report_month DESC);

-- One row per (partner_id, snapshot_at). docstats gives us a rolling
-- 29-day publish rate — we can't recover a daily series from the API,
-- but by writing our own snapshot each run we build one over time.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_docstats_snapshot (
    partner_id           TEXT        NOT NULL,
    snapshot_at          TIMESTAMPTZ NOT NULL,
    window_start         DATE        NOT NULL,
    window_end           DATE        NOT NULL,
    provider_id          TEXT        NOT NULL,
    publish_rate         NUMERIC(6, 2) NOT NULL,
    content_submitted    INTEGER     NOT NULL,
    content_published    INTEGER     NOT NULL,
    content_rejected     INTEGER     NOT NULL,
    PRIMARY KEY (partner_id, provider_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_msn_docstats_partner_time
    ON pgam_direct.msn_docstats_snapshot (partner_id, snapshot_at DESC);

-- Rejection count + raw failure payload (JSONB) so we can inspect
-- the failure list later even if MSN's shape changes. This REPLACES
-- the manual `msn_rejection_report` CSV upload workflow.
CREATE TABLE IF NOT EXISTS pgam_direct.msn_rejection_snapshot (
    partner_id     TEXT        NOT NULL,
    snapshot_at    TIMESTAMPTZ NOT NULL,
    log_end_time   TIMESTAMPTZ,
    doc_count      INTEGER     NOT NULL,
    failures       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (partner_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_msn_rejection_partner_time
    ON pgam_direct.msn_rejection_snapshot (partner_id, snapshot_at DESC);
"""


def _ensure_schema() -> None:
    """Idempotent — CREATE TABLE IF NOT EXISTS on every run is cheap."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA_SQL)
        conn.commit()


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

_EARNING_UPSERT_SQL = """
INSERT INTO pgam_direct.msn_earning_monthly
  (partner_id, report_month, interactions, net_revenue_usd,
   ads_amount_usd, processed_date, updated_at)
VALUES
  (%(partner_id)s, %(report_month)s, %(interactions)s, %(net_revenue_usd)s,
   %(ads_amount_usd)s, %(processed_date)s, now())
ON CONFLICT (partner_id, report_month) DO UPDATE SET
  interactions      = EXCLUDED.interactions,
  net_revenue_usd   = EXCLUDED.net_revenue_usd,
  ads_amount_usd    = EXCLUDED.ads_amount_usd,
  processed_date    = EXCLUDED.processed_date,
  updated_at        = now();
"""

_DOCSTATS_INSERT_SQL = """
INSERT INTO pgam_direct.msn_docstats_snapshot
  (partner_id, snapshot_at, window_start, window_end, provider_id,
   publish_rate, content_submitted, content_published, content_rejected)
VALUES
  (%(partner_id)s, %(snapshot_at)s, %(window_start)s, %(window_end)s,
   %(provider_id)s, %(publish_rate)s, %(content_submitted)s,
   %(content_published)s, %(content_rejected)s)
ON CONFLICT (partner_id, provider_id, snapshot_at) DO NOTHING;
"""

_REJECTION_INSERT_SQL = """
INSERT INTO pgam_direct.msn_rejection_snapshot
  (partner_id, snapshot_at, log_end_time, doc_count, failures)
VALUES
  (%(partner_id)s, %(snapshot_at)s, %(log_end_time)s, %(doc_count)s,
   %(failures)s::jsonb)
ON CONFLICT (partner_id, snapshot_at) DO NOTHING;
"""


def _parse_month(month_str: str) -> Optional[datetime]:
    """Parse MSN's 'YYYY-MM' to a UTC datetime pinned to the first of the month."""
    try:
        return datetime.strptime(month_str + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _write_earning(
    partner_id: str,
    records: list[dict[str, Any]],
) -> int:
    if not records:
        return 0
    rows: list[dict[str, Any]] = []
    for rec in records:
        month = _parse_month(rec.get("date", ""))
        if not month:
            continue
        rows.append({
            "partner_id":       partner_id,
            "report_month":     month.date(),
            "interactions":     int(rec.get("interaction") or 0),
            "net_revenue_usd":  float(rec.get("netRevenue") or 0),
            "ads_amount_usd":   float(rec.get("adsAmount") or 0),
            "processed_date":   (_parse_date(rec.get("processedDate")) or _parse_date(None)),
        })
        if rows[-1]["processed_date"] is not None:
            rows[-1]["processed_date"] = rows[-1]["processed_date"].date()
    if not rows:
        return 0
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_EARNING_UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def _write_docstats(
    partner_id: str,
    snapshot_at: datetime,
    window_start: datetime,
    window_end: datetime,
    records: list[dict[str, Any]],
) -> int:
    if not records:
        return 0
    rows: list[dict[str, Any]] = []
    for rec in records:
        rows.append({
            "partner_id":         partner_id,
            "snapshot_at":        snapshot_at,
            "window_start":       window_start.date(),
            "window_end":         window_end.date(),
            "provider_id":        rec.get("providerId") or "",
            "publish_rate":       float(rec.get("contentPublishRate") or 0),
            "content_submitted":  int(rec.get("contentSubmitted") or 0),
            "content_published":  int(rec.get("contentPublished") or 0),
            "content_rejected":   int(rec.get("contentRejected") or 0),
        })
    with connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(_DOCSTATS_INSERT_SQL, rows)
        conn.commit()
    return len(rows)


def _write_rejection(
    partner_id: str,
    snapshot_at: datetime,
    payload: dict[str, Any],
) -> int:
    log_end = payload.get("logEndTime")
    try:
        log_end_dt = datetime.fromisoformat(log_end.replace("Z", "+00:00")) if log_end else None
    except (AttributeError, ValueError):
        log_end_dt = None
    row = {
        "partner_id":     partner_id,
        "snapshot_at":    snapshot_at,
        "log_end_time":   log_end_dt,
        "doc_count":      int(payload.get("docCount") or 0),
        "failures":       json.dumps(payload.get("failures") or []),
    }
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_REJECTION_INSERT_SQL, row)
        conn.commit()
    return 1


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run(dry_run: bool = False, partner_id: str = DEFAULT_PARTNER_ID) -> dict[str, Any]:
    """Pull + write the three report endpoints. Returns a summary dict.

    Each endpoint is isolated: a failure in one doesn't block the others.
    """
    if PartnerHubClient is None:
        return {"ok": False, "error": f"playwright/client not available: {_IMPORT_ERROR}"}

    if not dry_run:
        _ensure_schema()

    summary: dict[str, Any] = {
        "partner_id":  partner_id,
        "started_at":  datetime.now(timezone.utc).isoformat(),
        "earning":     {},
        "docstats":    {},
        "rejection":   {},
    }

    with PartnerHubClient(partner_id=partner_id).session() as c:
        snapshot_at = datetime.now(timezone.utc)

        # 1. Monthly earnings
        try:
            earning = c.fetch_earning_adsrev(window_months=12)
            recs = earning.get("recordList", [])
            summary["earning"]["fetched"] = len(recs)
            if dry_run:
                summary["earning"]["dry_run"] = True
                for r in recs[:3]:
                    print(f"[earning] {r.get('date')}: int={r.get('interaction'):,.0f}, netRev=${r.get('netRevenue'):.2f}")
            else:
                summary["earning"]["written"] = _write_earning(partner_id, recs)
        except PartnerHubError as exc:
            summary["earning"]["error"] = str(exc)
            print(f"[earning] error: {exc}", file=sys.stderr)

        # 2. Docstats (29-day rolling)
        try:
            docstats = c.fetch_partner_docstats()
            recs = docstats.get("recordList", [])
            summary["docstats"]["fetched"] = len(recs)
            if recs:
                # window_end is yesterday-UTC per the endpoint contract;
                # window_start is 29 days before that.
                from datetime import timedelta
                window_end = snapshot_at - timedelta(days=1)
                window_start = window_end - timedelta(days=29)
                if dry_run:
                    summary["docstats"]["dry_run"] = True
                    for r in recs:
                        print(f"[docstats] {r.get('providerId')}: "
                              f"{r.get('contentPublishRate')}%, "
                              f"{r.get('contentPublished')}/{r.get('contentSubmitted')} "
                              f"({r.get('contentRejected')} rejected)")
                else:
                    summary["docstats"]["written"] = _write_docstats(
                        partner_id, snapshot_at, window_start, window_end, recs
                    )
        except PartnerHubError as exc:
            summary["docstats"]["error"] = str(exc)
            print(f"[docstats] error: {exc}", file=sys.stderr)

        # 3. Rejection snapshot
        try:
            rejection = c.fetch_partner_rejected_docstats()
            doc_count = int(rejection.get("docCount") or 0)
            failures = rejection.get("failures", []) or []
            summary["rejection"]["doc_count"] = doc_count
            summary["rejection"]["failures_returned"] = len(failures)
            if dry_run:
                summary["rejection"]["dry_run"] = True
                print(f"[rejection] docCount={doc_count} failures={len(failures)}")
            else:
                summary["rejection"]["written"] = _write_rejection(
                    partner_id, snapshot_at, rejection
                )
        except PartnerHubError as exc:
            summary["rejection"]["error"] = str(exc)
            print(f"[rejection] error: {exc}", file=sys.stderr)

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="MSN partner-reports ETL")
    parser.add_argument("--dry-run", action="store_true", help="Pull but do not write to Neon")
    parser.add_argument("--once", action="store_true", help="Single-shot invocation (alias for default)")
    parser.add_argument("--partner-id", default=DEFAULT_PARTNER_ID)
    args = parser.parse_args()
    result = run(dry_run=args.dry_run, partner_id=args.partner_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
