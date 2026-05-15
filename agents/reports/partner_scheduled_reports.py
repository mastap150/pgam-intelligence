"""
agents/reports/partner_scheduled_reports.py

Runs every hour. Picks up rows from pgam_direct.scheduled_reports
that are due NOW, generates a partner-scoped ZIP report by calling
the dashboard's /api/portal/reports/download endpoint with a service
token, and emails the ZIP via SendGrid as a base64 attachment.

Due-now rules:
  - row.active = true
  - row.send_hour_utc == current UTC hour
  - frequency='daily'   → every day
  - frequency='weekly'  → only when current weekday == day_of_week
  - frequency='monthly' → only when current day-of-month == day_of_month
  - AND not yet sent today (last_sent_at < today UTC midnight)

The window is computed as [today - window_days, yesterday]. We always
exclude today because ETLs are still landing data for it.

Failures are recorded in scheduled_report_deliveries with status='failed'
+ error message. last_error / last_error_at on the schedule row also
get updated so the partner sees ⚠ in the portal UI.

PGAM_DASHBOARD_SERVICE_TOKEN env var is required — it lets this agent
call /api/portal/reports/download with internal-admin equivalent
authority. That endpoint internally re-scopes the data to the
partner_id the schedule belongs to, so there's no cross-partner risk.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

from core.neon import connect

DASHBOARD_BASE = os.environ.get("PGAM_DASHBOARD_BASE", "https://app.pgammedia.com")
DASHBOARD_SERVICE_TOKEN = os.environ.get("PGAM_DASHBOARD_SERVICE_TOKEN")
SENDGRID_KEY = os.environ.get("SENDGRID_KEY")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "reports@pgammedia.com")


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _utc_now_hour() -> int:
    return datetime.now(timezone.utc).hour


def _yesterday_utc() -> date:
    return _today_utc() - timedelta(days=1)


def _fetch_due_schedules(conn) -> list[dict]:
    """Find scheduled_reports rows due to fire this hour and not yet
    sent today. Caller computes the window + actually delivers."""
    today = _today_utc()
    hour = _utc_now_hour()
    weekday = (today.weekday() + 1) % 7   # python: Mon=0; our schema: Sun=0
    day_of_month = today.day

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.id, s.partner_id, p.display_name AS partner_name,
                   p.contact_email AS partner_contact,
                   s.recipient_email, s.report_type, s.frequency,
                   s.day_of_week, s.day_of_month, s.send_hour_utc,
                   s.window_days, s.last_sent_at
              FROM pgam_direct.scheduled_reports s
              JOIN pgam_direct.partners p ON p.id = s.partner_id
             WHERE s.active = true
               AND p.active = true
               AND s.send_hour_utc = %s
               AND (
                 s.frequency = 'daily'
                 OR (s.frequency = 'weekly'  AND s.day_of_week  = %s)
                 OR (s.frequency = 'monthly' AND s.day_of_month = %s)
               )
               AND (
                 s.last_sent_at IS NULL
                 OR s.last_sent_at < (CURRENT_DATE AT TIME ZONE 'UTC')
               )
            """,
            (hour, weekday, day_of_month),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _build_report_zip(partner_id: str, window_from: date, window_to: date) -> bytes:
    """Call the dashboard's /api/portal/reports/download endpoint
    *on behalf of* the partner. The dashboard accepts the service
    token and applies the partner_id we pass via query param.

    Note: the /api/portal/* endpoints currently scope by SESSION's
    partner_id, not a query param. So we need an internal helper
    that respects an X-Partner-ID override for service-token callers.
    See /api/portal/reports/admin-download which exposes that
    capability strictly for this agent."""
    if not DASHBOARD_SERVICE_TOKEN:
        raise RuntimeError("PGAM_DASHBOARD_SERVICE_TOKEN not set — partner reports can't be delivered")
    qs = urllib.parse.urlencode({
        "partner_id": partner_id,
        "from":       window_from.isoformat(),
        "to":         window_to.isoformat(),
    })
    url = f"{DASHBOARD_BASE}/api/portal/reports/admin-download?{qs}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {DASHBOARD_SERVICE_TOKEN}")
    req.add_header("User-Agent", "PGAM-Intelligence/1.0 partner_scheduled_reports")
    with urllib.request.urlopen(req, timeout=60) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Report build returned HTTP {resp.status}")
        return resp.read()


def _send_email_with_attachment(
    recipient: str,
    partner_name: str,
    window_from: date,
    window_to: date,
    zip_bytes: bytes,
    filename: str,
) -> None:
    if not SENDGRID_KEY:
        raise RuntimeError("SENDGRID_KEY not set — cannot deliver report")
    encoded = base64.b64encode(zip_bytes).decode("ascii")
    subject = f"{partner_name} reporting — {window_from} to {window_to}"
    html = (
        f"<p>Hello,</p>"
        f"<p>Attached is your {partner_name} performance report for "
        f"<b>{window_from} → {window_to}</b>.</p>"
        f"<p>The archive contains summary, daily, apps, domains, countries, and format-mix CSVs. "
        f"All revenue figures are net payout to {partner_name}.</p>"
        f"<p>To manage your scheduled reports, sign in at "
        f"<a href=\"{DASHBOARD_BASE}/portal/reports\">{DASHBOARD_BASE}/portal/reports</a>.</p>"
        f"<p style=\"color:#888;font-size:11px\">Sent by the {partner_name} partner portal · powered by PGAM Media</p>"
    )
    payload = {
        "personalizations": [{"to": [{"email": recipient}]}],
        "from": {"email": EMAIL_FROM, "name": f"{partner_name} reports"},
        "subject": subject,
        "content": [{"type": "text/html", "value": html}],
        "attachments": [{
            "content": encoded,
            "type": "application/zip",
            "filename": filename,
            "disposition": "attachment",
        }],
    }
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {SENDGRID_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (200, 202):
            raise RuntimeError(f"SendGrid returned HTTP {resp.status}: {resp.read().decode(errors='replace')[:200]}")


def _record_delivery(conn, schedule: dict, window_from: date, window_to: date,
                      status: str, error: str | None, bytes_sent: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pgam_direct.scheduled_report_deliveries
                (scheduled_report_id, partner_id, recipient_email,
                 window_from, window_to, status, error, file_size_bytes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (schedule["id"], schedule["partner_id"], schedule["recipient_email"],
             window_from, window_to, status, error, bytes_sent),
        )
        if status == "sent":
            cur.execute(
                """
                UPDATE pgam_direct.scheduled_reports
                   SET last_sent_at = now(),
                       last_sent_window_from = %s,
                       last_sent_window_to = %s,
                       last_error = NULL,
                       last_error_at = NULL,
                       updated_at = now()
                 WHERE id = %s
                """,
                (window_from, window_to, schedule["id"]),
            )
        else:
            cur.execute(
                """
                UPDATE pgam_direct.scheduled_reports
                   SET last_error = %s,
                       last_error_at = now(),
                       updated_at = now()
                 WHERE id = %s
                """,
                (error or "(unknown error)", schedule["id"]),
            )
    conn.commit()


def run() -> dict:
    started = time.time()
    if not DASHBOARD_SERVICE_TOKEN:
        print("[partner_scheduled_reports] PGAM_DASHBOARD_SERVICE_TOKEN missing — skipping run", flush=True)
        return {"ok": False, "skipped": "no_service_token"}
    if not SENDGRID_KEY:
        print("[partner_scheduled_reports] SENDGRID_KEY missing — skipping run", flush=True)
        return {"ok": False, "skipped": "no_sendgrid_key"}

    sent = 0
    failed = 0
    with connect() as conn:
        schedules = _fetch_due_schedules(conn)
        print(f"[partner_scheduled_reports] {len(schedules)} schedule(s) due this hour", flush=True)
        for s in schedules:
            window_days = s["window_days"]
            window_to   = _yesterday_utc()
            window_from = window_to - timedelta(days=window_days - 1)
            try:
                zip_bytes = _build_report_zip(s["partner_id"], window_from, window_to)
                filename = f"{s['partner_id']}-report-{window_from}-{window_to}.zip"
                _send_email_with_attachment(
                    recipient=s["recipient_email"],
                    partner_name=s["partner_name"],
                    window_from=window_from, window_to=window_to,
                    zip_bytes=zip_bytes, filename=filename,
                )
                _record_delivery(conn, s, window_from, window_to, "sent", None, len(zip_bytes))
                sent += 1
                print(f"[partner_scheduled_reports]   ✓ sent #{s['id']} to {s['recipient_email']} ({len(zip_bytes)} bytes)", flush=True)
            except Exception as exc:
                err = str(exc)[:500]
                _record_delivery(conn, s, window_from, window_to, "failed", err, 0)
                failed += 1
                print(f"[partner_scheduled_reports]   ✗ FAILED #{s['id']}: {err}", flush=True)

    elapsed = round(time.time() - started, 1)
    print(f"[partner_scheduled_reports] DONE — sent={sent} failed={failed} in {elapsed}s", flush=True)
    return {"ok": True, "sent": sent, "failed": failed, "elapsed_s": elapsed}


if __name__ == "__main__":
    res = run()
    sys.exit(0 if res.get("ok") else 1)
