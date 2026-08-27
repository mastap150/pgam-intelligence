"""
agents/alerts/ar_aging_sentry.py

Weekly accounts-receivable overdue monitor for PGAM Media LLC.

Pulls the A/R Aging Summary from QuickBooks Online, computes what is actually
overdue, diffs it against last week's snapshot, and emails a digest.

Why this exists
---------------
QBO's own "Total Overdue" figure is not usable for PGAM. Altura Advertising
carries a large unapplied credit (-$271,790.81 as of 2026-08-26) parked in the
91+ bucket, and QBO nets that credit against real debt owed by *other*
customers. On 2026-08-26 QBO reported $121,081.53 overdue when the true gross
figure was $392,872.34 — a 3.2x understatement. This agent always reports
GROSS overdue: negative (credit) balances are excluded from the bucket totals
and surfaced separately as an anomaly.

Bands reported
--------------
  Newly overdue  1-30 days   slipped this cycle
  Aging          31-90 days  chase now
  Stale          91+ days    likely bad debt

Never-paid flag
---------------
A customer 30 days late who has paid $477k historically is a process problem.
A customer 30 days late who has never sent a dollar is a credit problem. The
agent cross-checks every open balance against a cash-basis Sales by Customer
report covering all history; any customer holding A/R but absent from that
report has never paid and is flagged separately.

Credentials (Render env)
------------------------
  QBO_CLIENT_ID       Intuit app client id
  QBO_CLIENT_SECRET   Intuit app client secret
  QBO_REFRESH_TOKEN   seed refresh token (rotates; see note below)
  QBO_REALM_ID        company id — PGAM Media LLC is 193514590350384
  AR_ALERT_EMAIL      optional; comma-separated. Falls back to EMAIL_TO.

Intuit rotates the refresh token on every use and the previous one dies after
24h. Render's disk is ephemeral, so the rotated token is persisted to Neon
(table `qbo_oauth_token`) rather than to disk — otherwise the first redeploy
more than a day after the last run would leave the env seed stale and the job
would start failing auth. The env var is only ever a seed for the first run.

State
-----
  Neon table `qbo_oauth_token`     rotating refresh token, keyed by realm
  Neon table `ar_aging_snapshot`   last run's per-customer balances, for diffing
"""

from __future__ import annotations

import base64
import json
import urllib.parse
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytz

ET = pytz.timezone("US/Eastern")

QBO_API_BASE = "https://quickbooks.api.intuit.com"
QBO_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
MINOR_VERSION = "75"

# Cash-basis history start. Earlier than PGAM's first QBO invoice, so absence
# from this report genuinely means "never paid", not "paid before the window".
CASH_HISTORY_START = "2022-01-01"

# Balances at or below this are treated as noise, not debt worth alerting on.
MATERIALITY = Decimal("50.00")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _config() -> dict:
    import os
    from core.config import RECIPIENTS, SENDER_EMAIL, SENDGRID_KEY

    recipients = [
        a.strip() for a in os.environ.get("AR_ALERT_EMAIL", "").split(",") if a.strip()
    ] or RECIPIENTS

    return {
        "client_id":     os.environ.get("QBO_CLIENT_ID", ""),
        "client_secret": os.environ.get("QBO_CLIENT_SECRET", ""),
        "refresh_seed":  os.environ.get("QBO_REFRESH_TOKEN", ""),
        "realm_id":      os.environ.get("QBO_REALM_ID", ""),
        "sendgrid_key":  SENDGRID_KEY,
        "sender":        SENDER_EMAIL,
        "recipients":    recipients,
    }


# ---------------------------------------------------------------------------
# Token store — Neon, because Render's disk does not survive a redeploy
# ---------------------------------------------------------------------------

def _ensure_tables() -> None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS qbo_oauth_token (
                realm_id      TEXT PRIMARY KEY,
                refresh_token TEXT        NOT NULL,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ar_aging_snapshot (
                taken_on   DATE           NOT NULL,
                customer   TEXT           NOT NULL,
                bucket_1_30   NUMERIC(14,2) NOT NULL DEFAULT 0,
                bucket_31_90  NUMERIC(14,2) NOT NULL DEFAULT 0,
                bucket_91_plus NUMERIC(14,2) NOT NULL DEFAULT 0,
                total_overdue NUMERIC(14,2) NOT NULL DEFAULT 0,
                PRIMARY KEY (taken_on, customer)
            )
            """
        )
        conn.commit()


def _stored_refresh_token(realm_id: str) -> str | None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT refresh_token FROM qbo_oauth_token WHERE realm_id = %s",
            (realm_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _store_refresh_token(realm_id: str, token: str) -> None:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO qbo_oauth_token (realm_id, refresh_token, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (realm_id)
            DO UPDATE SET refresh_token = EXCLUDED.refresh_token,
                          updated_at    = now()
            """,
            (realm_id, token),
        )
        conn.commit()


def _access_token(cfg: dict) -> str:
    """Exchange the refresh token for an access token, persisting the rotation."""
    refresh = _stored_refresh_token(cfg["realm_id"]) or cfg["refresh_seed"]
    if not refresh:
        raise RuntimeError(
            "No QBO refresh token available — set QBO_REFRESH_TOKEN for the first run."
        )

    basic = base64.b64encode(
        f"{cfg['client_id']}:{cfg['client_secret']}".encode()
    ).decode()
    body = urllib.parse.urlencode(
        {"grant_type": "refresh_token", "refresh_token": refresh}
    ).encode()

    req = urllib.request.Request(
        QBO_TOKEN_URL,
        data=body,
        headers={
            "Authorization": f"Basic {basic}",
            "Content-Type":  "application/x-www-form-urlencoded",
            "Accept":        "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode())

    rotated = payload.get("refresh_token")
    if rotated and rotated != refresh:
        _store_refresh_token(cfg["realm_id"], rotated)
        print("[ar_aging_sentry] refresh token rotated and persisted")

    return payload["access_token"]


# ---------------------------------------------------------------------------
# QBO report fetch + parsing
# ---------------------------------------------------------------------------

def _report(cfg: dict, token: str, name: str, params: dict) -> dict:
    qs = urllib.parse.urlencode({**params, "minorversion": MINOR_VERSION})
    url = f"{QBO_API_BASE}/v3/company/{cfg['realm_id']}/reports/{name}?{qs}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def _walk_rows(node: Any) -> list[list[str]]:
    """Flatten a QBO report's nested Rows into a list of ColData value lists.

    QBO nests Row objects arbitrarily (sections, sub-sections, summaries), so
    recursion is the only reliable way to reach every data row.
    """
    out: list[list[str]] = []

    if isinstance(node, dict):
        if "ColData" in node:
            out.append([c.get("value", "") for c in node["ColData"]])
        for key in ("Rows", "Row", "Header", "Summary"):
            if key in node:
                out.extend(_walk_rows(node[key]))
    elif isinstance(node, list):
        for item in node:
            out.extend(_walk_rows(item))

    return out


def _money(raw: str) -> Decimal:
    raw = (raw or "").strip().replace(",", "").replace("$", "")
    if not raw:
        return Decimal("0")
    if raw.startswith("(") and raw.endswith(")"):
        raw = "-" + raw[1:-1]
    try:
        return Decimal(raw)
    except Exception:
        return Decimal("0")


def _aging(cfg: dict, token: str) -> dict[str, dict]:
    """Return {customer: {current, d1_30, d31_60, d61_90, d91, total}}."""
    report = _report(
        cfg, token, "AgedReceivables",
        {"report_date": date.today().isoformat(), "aging_method": "Report_Date"},
    )

    customers: dict[str, dict] = {}
    for cols in _walk_rows(report.get("Rows", {})):
        if len(cols) < 7:
            continue
        name = (cols[0] or "").strip()
        if not name or name.upper().startswith("TOTAL"):
            continue
        customers[name] = {
            "current": _money(cols[1]),
            "d1_30":   _money(cols[2]),
            "d31_60":  _money(cols[3]),
            "d61_90":  _money(cols[4]),
            "d91":     _money(cols[5]),
            "total":   _money(cols[6]),
        }
    return customers


def _ever_paid(cfg: dict, token: str) -> set[str]:
    """Customer names that have sent cash at any point, from a cash-basis report.

    Degrades to an empty set on failure — the never-paid section is then
    suppressed rather than the whole alert being lost.
    """
    try:
        report = _report(
            cfg, token, "CustomerSales",
            {
                "start_date":         CASH_HISTORY_START,
                "end_date":           date.today().isoformat(),
                "accounting_method":  "Cash",
                "summarize_column_by": "Total",
            },
        )
    except Exception as exc:
        print(f"[ar_aging_sentry] cash-basis lookup failed, skipping never-paid flag: {exc}")
        return set()

    paid: set[str] = set()
    for cols in _walk_rows(report.get("Rows", {})):
        if len(cols) < 2:
            continue
        name = (cols[0] or "").strip()
        if not name or name.upper().startswith("TOTAL"):
            continue
        if _money(cols[-1]) > 0:
            paid.add(name)
    return paid


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def _analyse(aging: dict[str, dict], paid: set[str]) -> dict:
    """Split A/R into bands, excluding credit balances from the overdue math."""
    rows, credits = [], []
    band_new = band_aging = band_stale = Decimal("0")

    for name, b in aging.items():
        # A negative total is an unapplied payment or credit, not debt. Netting
        # it against other customers' overdue is exactly the bug this guards.
        if b["total"] < 0:
            credits.append((name, b["total"]))
            continue

        new_ = max(b["d1_30"], Decimal("0"))
        aged = max(b["d31_60"], Decimal("0")) + max(b["d61_90"], Decimal("0"))
        stale = max(b["d91"], Decimal("0"))
        overdue = new_ + aged + stale
        if overdue <= 0:
            continue

        band_new += new_
        band_aging += aged
        band_stale += stale
        rows.append(
            {
                "customer":   name,
                "d1_30":      new_,
                "d31_90":     aged,
                "d91":        stale,
                "overdue":    overdue,
                "never_paid": name not in paid,
            }
        )

    rows.sort(key=lambda r: r["overdue"], reverse=True)
    return {
        "rows":        rows,
        "band_new":    band_new,
        "band_aging":  band_aging,
        "band_stale":  band_stale,
        "total":       band_new + band_aging + band_stale,
        "never_paid":  sum(r["overdue"] for r in rows if r["never_paid"]),
        "credits":     credits,
    }


def _previous_snapshot() -> dict[str, Decimal]:
    from core.neon import connect

    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT customer, total_overdue
            FROM   ar_aging_snapshot
            WHERE  taken_on = (SELECT MAX(taken_on) FROM ar_aging_snapshot)
            """
        )
        return {row[0]: Decimal(str(row[1])) for row in cur.fetchall()}


def _save_snapshot(rows: list[dict]) -> None:
    from core.neon import connect

    today = date.today()
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM ar_aging_snapshot WHERE taken_on = %s", (today,))
        for r in rows:
            cur.execute(
                """
                INSERT INTO ar_aging_snapshot
                    (taken_on, customer, bucket_1_30, bucket_31_90,
                     bucket_91_plus, total_overdue)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (today, r["customer"], r["d1_30"], r["d31_90"], r["d91"], r["overdue"]),
            )
        conn.commit()


def _diff(rows: list[dict], previous: dict[str, Decimal]) -> dict:
    """Who newly went overdue, who paid down, since the last snapshot."""
    current = {r["customer"]: r["overdue"] for r in rows}

    newly = [
        {"customer": c, "amount": amt}
        for c, amt in current.items()
        if c not in previous and amt >= MATERIALITY
    ]
    paid_down = [
        {"customer": c, "amount": prev - current.get(c, Decimal("0"))}
        for c, prev in previous.items()
        if prev - current.get(c, Decimal("0")) >= MATERIALITY
    ]
    newly.sort(key=lambda r: r["amount"], reverse=True)
    paid_down.sort(key=lambda r: r["amount"], reverse=True)
    return {"newly_overdue": newly, "paid_down": paid_down, "had_baseline": bool(previous)}


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _usd(amount: Decimal) -> str:
    return f"${amount:,.2f}"


def _html(analysis: dict, diff: dict, run_date: str) -> str:
    def band_row(label: str, note: str, amount: Decimal) -> str:
        return (
            f"<tr><td style='padding:6px 12px'><b>{label}</b>"
            f"<br><span style='color:#777;font-size:12px'>{note}</span></td>"
            f"<td style='padding:6px 12px;text-align:right'>{_usd(amount)}</td></tr>"
        )

    parts = [
        "<div style='font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;"
        "max-width:720px;color:#222'>",
        f"<h2 style='margin-bottom:4px'>A/R Overdue — {run_date}</h2>",
        f"<p style='font-size:26px;margin:0 0 4px'><b>{_usd(analysis['total'])}</b> overdue</p>",
    ]

    if analysis["never_paid"] > 0:
        pct = analysis["never_paid"] / analysis["total"] * 100 if analysis["total"] else 0
        parts.append(
            f"<p style='color:#b00;margin:0 0 16px'>{_usd(analysis['never_paid'])} "
            f"({pct:.0f}%) is owed by customers who have never paid.</p>"
        )

    parts.append("<table style='border-collapse:collapse;width:100%;margin:12px 0'>")
    parts.append(band_row("Newly overdue", "1–30 days — slipped this cycle", analysis["band_new"]))
    parts.append(band_row("Aging", "31–90 days — chase now", analysis["band_aging"]))
    parts.append(band_row("Stale", "91+ days — likely bad debt", analysis["band_stale"]))
    parts.append("</table>")

    if diff["newly_overdue"]:
        parts.append("<h3>Newly overdue since last check</h3><ul>")
        for r in diff["newly_overdue"]:
            parts.append(f"<li>{r['customer']} — {_usd(r['amount'])}</li>")
        parts.append("</ul>")

    if diff["paid_down"]:
        parts.append("<h3>Paid since last check</h3><ul>")
        for r in diff["paid_down"]:
            parts.append(f"<li>{r['customer']} — {_usd(r['amount'])}</li>")
        parts.append("</ul>")
    elif diff["had_baseline"]:
        parts.append("<p><b>No payments received since the last check.</b></p>")

    parts.append("<h3>Overdue by customer</h3>")
    parts.append(
        "<table style='border-collapse:collapse;width:100%;font-size:14px'>"
        "<tr style='background:#f4f4f4'>"
        "<th style='text-align:left;padding:6px'>Customer</th>"
        "<th style='text-align:right;padding:6px'>1–30</th>"
        "<th style='text-align:right;padding:6px'>31–90</th>"
        "<th style='text-align:right;padding:6px'>91+</th>"
        "<th style='text-align:right;padding:6px'>Total</th></tr>"
    )
    for r in analysis["rows"]:
        flag = " <span style='color:#b00'>&#9888; never paid</span>" if r["never_paid"] else ""
        parts.append(
            f"<tr><td style='padding:6px;border-top:1px solid #eee'>{r['customer']}{flag}</td>"
            f"<td style='padding:6px;text-align:right;border-top:1px solid #eee'>{_usd(r['d1_30'])}</td>"
            f"<td style='padding:6px;text-align:right;border-top:1px solid #eee'>{_usd(r['d31_90'])}</td>"
            f"<td style='padding:6px;text-align:right;border-top:1px solid #eee'>{_usd(r['d91'])}</td>"
            f"<td style='padding:6px;text-align:right;border-top:1px solid #eee'><b>{_usd(r['overdue'])}</b></td></tr>"
        )
    parts.append("</table>")

    if analysis["credits"]:
        parts.append(
            "<h3>Unapplied credits — excluded from the totals above</h3>"
            "<p style='color:#777;font-size:13px'>These are negative balances. "
            "QBO nets them against other customers' overdue, which understates "
            "the real figure.</p><ul>"
        )
        for name, amount in analysis["credits"]:
            parts.append(f"<li>{name} — {_usd(amount)}</li>")
        parts.append("</ul>")

    parts.append("</div>")
    return "".join(parts)


def _send_email(cfg: dict, html: str, run_date: str, total: Decimal) -> bool:
    payload = {
        "personalizations": [{"to": [{"email": r} for r in cfg["recipients"]]}],
        "from": {"email": cfg["sender"]},
        "subject": f"A/R overdue {_usd(total)} — {run_date}",
        "content": [{"type": "text/html", "value": html}],
    }
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {cfg['sendgrid_key']}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.getcode() in (200, 202):
                print(f"[ar_aging_sentry] emailed {len(cfg['recipients'])} recipient(s)")
                return True
            print(f"[ar_aging_sentry] unexpected SendGrid status {resp.getcode()}")
    except Exception as exc:
        print(f"[ar_aging_sentry] delivery failed: {exc}")
    return False


def _post_slack(analysis: dict, diff: dict) -> None:
    try:
        from core.slack import send_text
    except Exception:
        return

    lines = [
        f"*A/R overdue: {_usd(analysis['total'])}*",
        f"• Newly overdue (1–30): {_usd(analysis['band_new'])}",
        f"• Aging (31–90): {_usd(analysis['band_aging'])}",
        f"• Stale (91+): {_usd(analysis['band_stale'])}",
    ]
    if analysis["never_paid"] > 0:
        lines.append(f"⚠️ {_usd(analysis['never_paid'])} owed by customers who have never paid")
    if diff["newly_overdue"]:
        names = ", ".join(f"{r['customer']} {_usd(r['amount'])}" for r in diff["newly_overdue"][:5])
        lines.append(f"*Newly overdue:* {names}")
    if diff["paid_down"]:
        names = ", ".join(f"{r['customer']} {_usd(r['amount'])}" for r in diff["paid_down"][:5])
        lines.append(f"*Paid:* {names}")

    try:
        send_text("\n".join(lines))
    except Exception as exc:
        print(f"[ar_aging_sentry] slack post failed: {exc}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> None:
    now = datetime.now(ET)
    # Scheduler ticks daily; this is a Monday job. Guard inside the agent,
    # matching the pattern the other weekly agents use.
    if now.weekday() != 0:
        return

    cfg = _config()
    missing = [
        k for k in ("client_id", "client_secret", "realm_id")
        if not cfg[k]
    ]
    if missing:
        print(f"[ar_aging_sentry] skipping — missing QBO config: {', '.join(missing)}")
        return
    if not cfg["sendgrid_key"] or not cfg["recipients"]:
        print("[ar_aging_sentry] skipping — no SendGrid key or recipients configured")
        return

    _ensure_tables()

    token = _access_token(cfg)
    aging = _aging(cfg, token)
    if not aging:
        print("[ar_aging_sentry] aging report returned no customers — nothing to do")
        return

    paid = _ever_paid(cfg, token)
    analysis = _analyse(aging, paid)
    previous = _previous_snapshot()
    diff = _diff(analysis["rows"], previous)

    run_date = now.strftime("%Y-%m-%d")
    html = _html(analysis, diff, run_date)

    _send_email(cfg, html, run_date, analysis["total"])
    _post_slack(analysis, diff)
    _save_snapshot(analysis["rows"])

    print(
        f"[ar_aging_sentry] {_usd(analysis['total'])} overdue across "
        f"{len(analysis['rows'])} customers "
        f"({_usd(analysis['never_paid'])} from never-paid accounts)"
    )


if __name__ == "__main__":
    run()
