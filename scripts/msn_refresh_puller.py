#!/usr/bin/env python3
"""
scripts/msn_refresh_puller.py

HTTP-only MSN Partner Hub puller. Reads the OAuth refresh_token from
pgam_direct.msn_oauth_token, mints a fresh access_token via Microsoft's
OAuth endpoint, calls api.msn.com /realtime + buckets, writes results
to pgam_direct.msn_* tables, and persists the rotated refresh_token
back to Neon.

NO BROWSER. NO PLAYWRIGHT. Works on any Linux/macOS host.

Bootstrap: run scripts/msn_oauth_capture.py once locally (interactive
MFA needed). Then this script can run on GH Actions, Render, or
anywhere — every 15 minutes via cron.

Refresh-token chain semantics (Microsoft consumer accounts):
  - refresh_token has 24h lifetime
  - Using it returns a NEW refresh_token with a fresh 24h window
  - As long as we run at least once every 23h, the chain is indefinite

Failure modes:
  - "invalid_grant" from OAuth → chain broken (>24h since last refresh,
    or user re-authenticated elsewhere invalidating the chain). Need
    to re-run msn_oauth_capture.py.
  - 401 from api.msn.com → access_token didn't work even though we got
    one. Could be scope mismatch. Log + page.
  - Network error → transient. Retry on next cron tick.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlencode

try:
    from dotenv import load_dotenv
    from pathlib import Path
    here = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=str(here / '.env'), override=False)
except Exception:
    pass

import psycopg
import requests

# Make repo importable so we can reuse the existing snapshot writer
# from agents.etl.msn_insights_etl without copy-pasting it.
sys.path.insert(0, str(here))
from agents.etl.msn_insights_etl import (   # noqa: E402
    _ensure_schema,
    _write_snapshots,
    _write_traffic_buckets,
    _normalise_daily_row,
    _DAILY_UPSERT_SQL,
    _log_run,
)
from agents.etl.msn_rejection_etl import (  # noqa: E402
    ensure_schema as _ensure_rejection_schema,
    _UPSERT_SQL as _REJECTION_UPSERT_SQL,
)
from core.msn_partner_hub import (           # noqa: E402
    API_HOST, API_BASE_PATH, APIKEY, DEFAULT_PARTNER_ID, DEFAULT_PARTNER_TYPE,
    _iso_z, _now_utc,
)

# Report path is a sibling of the /realtime insights endpoint. Split
# from API_BASE_PATH so we don't accidentally couple with future
# insights-only path changes.
REPORT_BASE_PATH = "/msn/v0/pages/ugc/contents/report"
# Ingest at most once every 22h — the underlying data-update-time on
# the MSN side ticks daily, so more-frequent calls just re-upsert
# identical rows and burn Actions minutes.
REJECTION_MIN_INTERVAL_HOURS = 22

TOKEN_TABLE_ID = "msn-partner-hub-boxingnews-primary"
TOKEN_ENDPOINT_TMPL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
# When tenant is "consumers" or starts with M.C560, the consumer endpoint applies
LIVE_TOKEN_ENDPOINT = "https://login.live.com/oauth20_token.srf"


def _resolve_dsn() -> str:
    dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("PGAM_DIRECT_DATABASE_URL not set")
    return dsn.replace("-pooler.", ".")


def load_token_row() -> dict[str, Any]:
    dsn = _resolve_dsn()
    with psycopg.connect(dsn, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT client_id, tenant, scope, refresh_token, access_token,
                          access_expires_at, refresh_expires_at, redirect_uri,
                          refresh_count
                     FROM pgam_direct.msn_oauth_token
                    WHERE id = %s""",
                (TOKEN_TABLE_ID,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(
                    f"No row in pgam_direct.msn_oauth_token with id='{TOKEN_TABLE_ID}'. "
                    "Run scripts/msn_oauth_capture.py once to bootstrap."
                )
    return {
        'client_id': row[0],
        'tenant': row[1],
        'scope': row[2],
        'refresh_token': row[3],
        'access_token': row[4],
        'access_expires_at': row[5],
        'refresh_expires_at': row[6],
        'redirect_uri': row[7],
        'refresh_count': row[8],
    }


def save_rotated_token(client_id: str, tenant: str, scope: str,
                       refresh_token: str, access_token: str,
                       expires_in: int, refresh_token_expires_in: int,
                       redirect_uri: str) -> None:
    dsn = _resolve_dsn()
    now = datetime.now(tz=timezone.utc)
    access_expires_at = now + timedelta(seconds=expires_in)
    refresh_expires_at = now + timedelta(seconds=refresh_token_expires_in)
    with psycopg.connect(dsn, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE pgam_direct.msn_oauth_token SET
                     client_id = %s,
                     tenant = %s,
                     scope = %s,
                     refresh_token = %s,
                     access_token = %s,
                     access_expires_at = %s,
                     refresh_expires_at = %s,
                     redirect_uri = %s,
                     updated_at = NOW(),
                     updated_by = %s,
                     refresh_count = refresh_count + 1
                   WHERE id = %s""",
                (
                    client_id, tenant, scope, refresh_token, access_token,
                    access_expires_at, refresh_expires_at, redirect_uri,
                    f"refresh@{os.uname().nodename}",
                    TOKEN_TABLE_ID,
                ),
            )
        conn.commit()


def refresh_access_token(row: dict[str, Any]) -> dict[str, Any]:
    """Exchange the stored refresh_token for a new access_token (+
    rotated refresh_token). Returns the OAuth response dict."""
    # Pick the right endpoint based on tenant. Microsoft accounts
    # ("consumers" or the consumer tenant guid) use login.live.com's
    # oauth20_token.srf endpoint; work/school accounts use the
    # standard v2.0 endpoint.
    CONSUMER_TENANT = "9188040d-6c67-4c5b-b112-36a304b66dad"
    if row['tenant'] in ('consumers', CONSUMER_TENANT, 'common'):
        endpoint = LIVE_TOKEN_ENDPOINT
    else:
        endpoint = TOKEN_ENDPOINT_TMPL.format(tenant=row['tenant'])

    body = {
        'client_id': row['client_id'],
        'grant_type': 'refresh_token',
        'refresh_token': row['refresh_token'],
        'redirect_uri': row['redirect_uri'],
        'scope': row['scope'],
    }
    # Microsoft rejects SPA-issued refresh tokens without an Origin
    # header ("Tokens issued for the 'Single-Page Application'
    # client-type should only be redeemed via cross-origin requests").
    # Derive the origin from the stored redirect_uri, which points at
    # the SPA's auth bridge (e.g. https://www.msn.com/staticsb/…).
    from urllib.parse import urlparse
    parsed_redirect = urlparse(row.get('redirect_uri') or '')
    origin = (
        f"{parsed_redirect.scheme}://{parsed_redirect.netloc}"
        if parsed_redirect.scheme and parsed_redirect.netloc
        else 'https://www.msn.com'
    )
    r = requests.post(
        endpoint,
        data=body,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'Origin': origin,
        },
        timeout=20,
    )
    if r.status_code != 200:
        snippet = r.text[:500]
        raise RuntimeError(
            f"OAuth refresh failed: HTTP {r.status_code}. "
            f"Body: {snippet}"
        )
    data = r.json()
    if 'access_token' not in data or 'refresh_token' not in data:
        raise RuntimeError(
            f"OAuth response missing tokens: {json.dumps(data)[:300]}"
        )
    return data


def call_partner_hub_api(access_token: str, path: str, params: dict[str, str]) -> dict[str, Any]:
    url = f"{API_HOST}{path}?{urlencode(params, safe=',-')}"
    r = requests.get(
        url,
        headers={
            'Authorization': f'Bearer {access_token}',
            'Accept': '*/*',
        },
        timeout=30,
    )
    if r.status_code == 401:
        raise RuntimeError("Partner Hub API returned 401 — access_token didn't authenticate. "
                           "Scope or client_id may be wrong for this surface.")
    if r.status_code != 200:
        raise RuntimeError(f"Partner Hub API HTTP {r.status_code}: {r.text[:300]}")
    return r.json()


def build_common_params(start: datetime, end: datetime, partner_id: str) -> dict[str, str]:
    _ALL = "-2"
    _NO_TITLE = "-1"
    _UGC_FLIGHTS = (
        "prg-ugc-benchmark,prg-ugc-revagvnext,prg-ugc-timespent,"
        "prg-ugc-aiusage,prg-ugc-shortinsight,prg-ugc-pcm"
    )
    return {
        "apikey": APIKEY,
        "brandId": _ALL, "clickSource": _ALL, "contentType": _ALL,
        "date": _ALL, "device": _ALL,
        "endDate": _iso_z(end),
        "fdhead": _UGC_FLIGHTS,
        "lang": _ALL, "mkt": _ALL, "ocid": "msph",
        "partnerId": partner_id, "partnerType": DEFAULT_PARTNER_TYPE,
        "scn": "MSNRPSAuth", "skipaadal": "true",
        "startDate": _iso_z(start),
        "timeout": "30000", "title": _NO_TITLE,
        "ugc-flights": _UGC_FLIGHTS,
        "vertical": _ALL, "wrapodata": "false",
    }


def _rejection_recent_upsert_age_hours(partner_id: str) -> Optional[float]:
    """Return hours since the last row was upserted into msn_rejection_report
    for this partner. None if the table is empty."""
    dsn = _resolve_dsn()
    with psycopg.connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(imported_at) FROM pgam_direct.msn_rejection_report "
            "WHERE partner_id = %s",
            (partner_id,),
        )
        row = cur.fetchone()
    last = row[0] if row else None
    if last is None:
        return None
    return (datetime.now(tz=timezone.utc) - last).total_seconds() / 3600.0


def _parse_log_end_time(s: Any) -> Optional[datetime]:
    """logEndTime is emitted as e.g. '2026-05-29T04:15Z'. Tolerate both
    the trailing Z and full ISO forms; return None if unparseable."""
    if not s or not isinstance(s, str):
        return None
    txt = s.strip()
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _aggregate_failures(failures: list[dict[str, Any]]) -> list[tuple[str, str, int]]:
    """Reduce the API's `failures[]` list to (reason, type, count) tuples.

    Shape unknown at build time — the one prior capture returned an empty
    list, so we've inferred field names from the CSV variant (Overview
    columns: Rejection reason, Failure type, Total number of the failures;
    Detail columns per-doc: rejection details JSON with `reason`, plus
    the Overview grouping key). Handle both possibilities:
      (a) already-aggregated rows with a numeric `count`/`total`/`failureCount`
      (b) per-doc rows we roll up ourselves
    """
    tally: dict[tuple[str, str], int] = {}
    for f in failures or ():
        if not isinstance(f, dict):
            continue
        # Pull reason from any of the plausible field names.
        reason = (
            f.get("rejectionReason")
            or f.get("reason")
            or f.get("failureReason")
            or f.get("message")
            or "Unknown reason"
        )
        ftype = (
            f.get("failureType")
            or f.get("type")
            or f.get("category")
            or "Unknown"
        )
        # Aggregated row?
        n_raw = (
            f.get("count")
            if isinstance(f.get("count"), (int, float))
            else f.get("failureCount")
            if isinstance(f.get("failureCount"), (int, float))
            else f.get("total")
            if isinstance(f.get("total"), (int, float))
            else None
        )
        n = int(n_raw) if n_raw is not None else 1
        key = (str(reason).strip(), str(ftype).strip())
        tally[key] = tally.get(key, 0) + n
    return [(r, t, c) for (r, t), c in tally.items()]


def _pull_rejections(access_token: str, partner_id: str,
                     dry_run: bool = False) -> dict[str, Any]:
    """Call /partnerrejecteddocstats, aggregate the failures list, upsert
    into pgam_direct.msn_rejection_report.

    Skips the API call entirely if we upserted within the last
    REJECTION_MIN_INTERVAL_HOURS — the data-update-time ticks once/day
    on MSN's side so more-frequent pulls waste round-trips."""
    result: dict[str, Any] = {"called": False, "rows_written": 0,
                              "failures_seen": 0, "doc_count": 0}

    age = _rejection_recent_upsert_age_hours(partner_id)
    if age is not None and age < REJECTION_MIN_INTERVAL_HOURS:
        print(f"[refresh-puller] rejections: last upsert {age:.1f}h ago "
              f"(< {REJECTION_MIN_INTERVAL_HOURS}h); skipping")
        result["skipped_reason"] = f"last_upsert_{age:.1f}h_ago"
        return result

    # Match the params the Partner Hub SPA sent verbatim on the click.
    params = {
        "apikey": APIKEY,
        "fdhead": "prg-ugc-benchmark,prg-ugc-shortinsight,prg-ugc-pcm",
        "isExportingCsv": "false",
        "ocid": "msph",
        "partnerId": partner_id,
        "partnerType": DEFAULT_PARTNER_TYPE,
        "scn": "MSNRPSAuth",
        "skipaadal": "true",
        "timeout": "30000",
        "ugc-flights": "prg-ugc-benchmark,prg-ugc-shortinsight,prg-ugc-pcm",
        "wrapodata": "false",
    }
    payload = call_partner_hub_api(
        access_token,
        f"{REPORT_BASE_PATH}/partnerrejecteddocstats",
        params,
    )
    result["called"] = True

    failures = payload.get("failures") or []
    doc_count = int(payload.get("docCount") or 0)
    result["failures_seen"] = len(failures)
    result["doc_count"] = doc_count

    log_end = _parse_log_end_time(payload.get("logEndTime"))
    if log_end is None:
        log_end = datetime.now(tz=timezone.utc)
    # MSN's Overview CSV is a rolling 7-day window ending at logEndTime;
    # mirror that so downstream weekly-review queries line up.
    window_end = log_end
    window_start = log_end - timedelta(days=7)

    print(f"[refresh-puller] rejections: doc_count={doc_count}, "
          f"failures_len={len(failures)}, window={window_start.date()}→{window_end.date()}")

    if not failures:
        # Log a preview of the payload so the next iteration can see
        # exactly what MSN returned (helpful because prior captures
        # showed an empty list under one param combo).
        preview = json.dumps(payload)[:400]
        print(f"[refresh-puller] rejections: empty failures list; payload preview: {preview}")
        return result

    rows = _aggregate_failures(failures)
    if not rows:
        return result
    result["aggregated_rows"] = len(rows)

    if dry_run:
        for reason, ftype, n in rows:
            print(f"  [dry-run] {ftype:20s}  {n:4d}  {reason[:80]}")
        return result

    dsn = _resolve_dsn()
    with psycopg.connect(dsn, connect_timeout=30) as conn, conn.cursor() as cur:
        for reason, ftype, n in rows:
            cur.execute(_REJECTION_UPSERT_SQL, {
                "partner_id":       partner_id,
                "window_start":     window_start,
                "window_end":       window_end,
                "rejection_reason": reason,
                "failure_type":     ftype,
                "failure_count":    n,
                "data_update_time": log_end,
                "source_filename":  "api:partnerrejecteddocstats",
            })
        conn.commit()
    result["rows_written"] = len(rows)
    print(f"[refresh-puller] rejections: upserted {len(rows)} aggregate row(s)")
    return result


def run(partner_id: str = DEFAULT_PARTNER_ID, dry_run: bool = False) -> dict[str, Any]:
    started_at = datetime.now(tz=timezone.utc)
    t0 = time.perf_counter()
    err: Optional[str] = None
    realtime_rows = 0
    realtime_inserted = 0
    record_count = 0
    bucket_rows = 0
    pages_done = 0
    rejection_result: dict[str, Any] = {}

    try:
        if not dry_run:
            _ensure_schema()

        row = load_token_row()
        if datetime.now(tz=timezone.utc) >= row['refresh_expires_at']:
            raise RuntimeError(
                f"refresh_token expired (refresh_expires_at={row['refresh_expires_at']}). "
                "Re-run scripts/msn_oauth_capture.py to bootstrap a fresh chain."
            )
        print(f"[refresh-puller] using refresh_token (count={row['refresh_count']}, "
              f"refresh exp in {(row['refresh_expires_at'] - datetime.now(tz=timezone.utc)).total_seconds()/3600:.1f}h)")

        oauth_resp = refresh_access_token(row)
        access_token = oauth_resp['access_token']
        new_refresh_token = oauth_resp['refresh_token']
        if not dry_run:
            save_rotated_token(
                client_id=row['client_id'], tenant=row['tenant'],
                scope=oauth_resp.get('scope', row['scope']),
                refresh_token=new_refresh_token, access_token=access_token,
                expires_in=int(oauth_resp.get('expires_in', 3599)),
                refresh_token_expires_in=int(oauth_resp.get('refresh_token_expires_in', 86400)),
                redirect_uri=row['redirect_uri'],
            )
        print(f"[refresh-puller] refreshed access_token (expires in {oauth_resp.get('expires_in', 0)}s); "
              f"new refresh_token saved")

        # Paginate realtime
        records: list[dict[str, Any]] = []
        seen_doc_ids: set[str] = set()
        end_dt = _now_utc()
        start_dt = end_dt - timedelta(hours=24)
        for page in range(50):
            skip = page * 20
            params = build_common_params(start_dt, end_dt, partner_id)
            params.update({"$orderBy": "view", "$skip": str(skip), "$top": "20"})
            payload = call_partner_hub_api(access_token, f"{API_BASE_PATH}/realtime", params)
            chunk = payload.get("recordList") or []
            record_count = int(payload.get("recordCount") or 0)
            pages_done = page + 1
            if not chunk:
                break
            for rec in chunk:
                did = rec.get("docID")
                if not did or did in seen_doc_ids:
                    continue
                seen_doc_ids.add(did)
                records.append(rec)
            if skip + len(chunk) >= record_count:
                break
        realtime_rows = len(records)
        print(f"[refresh-puller] realtime: {realtime_rows} unique docs over {pages_done} page(s)")
        if records and not dry_run:
            realtime_inserted = _write_snapshots(
                partner_id=partner_id, records=records,
                record_count=record_count, snapshot_at=datetime.now(tz=timezone.utc),
            )
            print(f"[refresh-puller] {realtime_inserted} new snapshot rows persisted")

        # Time-bucket realtime (15-min slots, last 24h)
        params = build_common_params(start_dt, end_dt, partner_id)
        params.pop("title", None); params.pop("device", None)
        params.pop("clickSource", None); params.pop("vertical", None)
        params["date"] = "-1"; params["$skip"] = "0"; params["$top"] = "96"
        buckets = call_partner_hub_api(access_token, f"{API_BASE_PATH}/realtime", params)
        bucket_records = buckets.get("recordList") or []
        bucket_total = sum(int(b.get("readCount") or 0) for b in bucket_records)
        est_24h = round(bucket_total * 0.004, 2)
        print(f"[refresh-puller] buckets: {len(bucket_records)} slots, "
              f"sum readCount = {bucket_total} (est 24h revenue ${est_24h})")
        if bucket_records and not dry_run:
            bucket_rows = _write_traffic_buckets(partner_id=partner_id, records=bucket_records)

        # Rejection report — best-effort, isolated from the realtime path.
        # An API/schema change here must not fail the whole cron tick.
        try:
            if not dry_run:
                _ensure_rejection_schema()
            rejection_result = _pull_rejections(
                access_token=access_token,
                partner_id=partner_id,
                dry_run=dry_run,
            )
        except Exception as rex:
            rejection_result = {"error": f"{type(rex).__name__}: {rex}"}
            print(f"[refresh-puller] rejections: ✗ {rejection_result['error']}")

    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        print(f"[refresh-puller] ✗ {err}")
        traceback.print_exc()

    elapsed = time.perf_counter() - t0
    ok = err is None
    if not dry_run:
        _log_run({
            "started_at": started_at,
            "partner_id": partner_id,
            "realtime_rows_seen": realtime_rows,
            "realtime_pages": pages_done,
            "aggregate_rows_seen": 0,
            "ok": ok,
            "error_message": err,
        })

    result = {
        "ok": ok, "realtime_rows": realtime_rows, "realtime_inserted": realtime_inserted,
        "record_count": record_count, "bucket_rows": bucket_rows,
        "rejection": rejection_result,
        "elapsed_seconds": round(elapsed, 2), "error": err,
    }
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--partner-id", default=DEFAULT_PARTNER_ID)
    args = p.parse_args()
    result = run(partner_id=args.partner_id, dry_run=args.dry_run)
    print(f"[refresh-puller] result: {json.dumps(result, indent=2, default=str)}")
    sys.exit(0 if result.get("ok") else 1)
