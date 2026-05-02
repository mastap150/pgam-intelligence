"""
agents/alerts/dashboard_alerts.py

Posts dashboard-derived alerts to Slack on a regular cadence:

  1. Anomalies — calls /api/reporting/partner-revenue/anomalies on
     app.pgammedia.com and surfaces critical/warning alerts.

  2. Reconciliation drift — queries finance.ssp_recon_daily directly
     (we already have a Neon helper) and surfaces partners with
     warning/critical SSP-vs-PGAM variance over the last 7 days.

  3. DSP health — surfaces demand brands whose win rate dropped
     >5pp WoW or whose gross revenue dropped >25% WoW.

Daily-deduped via core.slack.already_sent_today / mark_sent so the
hourly scheduler call doesn't spam.

Why hit the API rather than re-implement the queries here? The
anomalies + DSP-health logic lives in pgam-direct/web's TypeScript
server modules. Calling the API is the lowest-divergence path —
when we tune thresholds in the web code, the agent picks up the
new behaviour automatically.

Reconciliation queries Neon directly because (a) we already have
the connection set up via core.neon, and (b) it lets the agent
work even if the web app is down.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse

from core.neon import connect
from core.slack import send_blocks, already_sent_today, mark_sent

# We post the alerts to Slack on behalf of `dashboard_alerts`. The
# dashboard itself runs at app.pgammedia.com so that's where we hit
# the anomalies + dsp-health endpoints.
DASHBOARD_BASE = os.environ.get("PGAM_DASHBOARD_BASE", "https://app.pgammedia.com")

# A service token authenticates this agent to the dashboard's
# anomalies / dsp-health endpoints. Without it the agent skips the
# API-derived sections and only posts the recon section (which it
# can compute from Neon directly).
DASHBOARD_SERVICE_TOKEN = os.environ.get("PGAM_DASHBOARD_SERVICE_TOKEN")


def _api_get(path: str, timeout: int = 30) -> dict | None:
    if not DASHBOARD_SERVICE_TOKEN:
        return None
    url = f"{DASHBOARD_BASE}{path}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {DASHBOARD_SERVICE_TOKEN}")
    req.add_header("User-Agent", "PGAM-Intelligence/1.0 dashboard_alerts")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        print(f"[dashboard_alerts] {path} failed: {exc}", flush=True)
        return None


def _fetch_recon_drift_neon(window_days: int = 7) -> list[dict]:
    """Query the recon Neon DB directly. Same logic as the dashboard's
    /api/reporting/partner-revenue/reconciliation but inlined so the
    agent doesn't depend on the web app being up."""
    finance_url = os.environ.get("FINANCE_DATABASE_URL")
    if not finance_url:
        # Try DATABASE_URL fallback (some envs share)
        finance_url = os.environ.get("DATABASE_URL")
    if not finance_url:
        return []
    # Use the existing connect() with a short-lived swap.
    prev = os.environ.get("PGAM_DIRECT_DATABASE_URL")
    os.environ["PGAM_DIRECT_DATABASE_URL"] = finance_url
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT partner_key,
                           MAX(partner_sheet_name) AS partner_name,
                           SUM(ssp_dash_net)::float8  AS ssp,
                           SUM(pgam_ssp_dash)::float8 AS pgam,
                           SUM(difference)::float8    AS variance,
                           COUNT(*) FILTER (WHERE ABS(difference) > 0.005)::int AS days_drift,
                           COUNT(*)::int AS days_seen
                    FROM finance.ssp_recon_daily
                    WHERE target_date >= CURRENT_DATE - %s::int
                    GROUP BY partner_key
                    """,
                    (window_days,),
                )
                rows = cur.fetchall()
    finally:
        if prev is not None:
            os.environ["PGAM_DIRECT_DATABASE_URL"] = prev
        else:
            os.environ.pop("PGAM_DIRECT_DATABASE_URL", None)

    out: list[dict] = []
    for r in rows:
        partner_key, partner_name, ssp, pgam, variance, days_drift, days_seen = r
        ref = max(abs(ssp or 0), abs(pgam or 0))
        if ref < 50:
            continue  # noise floor
        pct = abs(variance / ref) if ref > 0 else 0
        # Mirror the dashboard's severity thresholds
        if pct <= 0.005:
            severity = "ok"
        elif pct <= 0.05:
            severity = "notice"
        elif pct <= 0.20:
            severity = "warning"
        else:
            severity = "critical"
        if severity in ("warning", "critical"):
            out.append({
                "partner_key": partner_key,
                "partner_name": partner_name or partner_key,
                "variance": float(variance or 0),
                "abs_variance": abs(float(variance or 0)),
                "ssp": float(ssp or 0),
                "pgam": float(pgam or 0),
                "variance_pct": pct * 100,
                "severity": severity,
                "days_drift": int(days_drift or 0),
                "days_seen": int(days_seen or 0),
            })
    out.sort(key=lambda x: x["abs_variance"], reverse=True)
    return out


def _fmt_usd(v: float) -> str:
    sign = "-" if v < 0 else ""
    a = abs(v)
    if a >= 1_000_000:
        return f"{sign}${a/1_000_000:.2f}M"
    if a >= 1_000:
        return f"{sign}${a/1_000:.2f}K"
    return f"{sign}${a:,.2f}"


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    return "—" if v is None else f"{v:.{digits}f}%"


def _build_blocks(
    anomalies: dict | None,
    recon_drift: list[dict],
    dsp_health: dict | None,
) -> list[dict] | None:
    """Build Slack Block Kit blocks. Returns None if there's nothing
    actionable to post (we don't spam an "all clear" message)."""
    sections: list[dict] = []

    # Anomalies
    if anomalies:
        crit = [a for a in anomalies.get("alerts", []) if a.get("severity") == "critical"]
        warn = [a for a in anomalies.get("alerts", []) if a.get("severity") == "warning"]
        if crit or warn:
            lines: list[str] = []
            for a in (crit + warn)[:8]:
                emoji = ":red_circle:" if a["severity"] == "critical" else ":large_yellow_circle:"
                lines.append(f"{emoji} *{a['brand']}* — {a['message']}")
            sections.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Anomaly alerts (last 7d vs prior 7d)*\n" + "\n".join(lines)},
            })

    # Reconciliation drift
    if recon_drift:
        lines = []
        for d in recon_drift[:6]:
            emoji = ":red_circle:" if d["severity"] == "critical" else ":large_yellow_circle:"
            sign = "+" if d["variance"] >= 0 else ""
            lines.append(
                f"{emoji} *{d['partner_name']}* — variance {sign}{_fmt_usd(d['variance'])} "
                f"({_fmt_pct(d['variance_pct'])}, {d['days_drift']}/{d['days_seen']} days)"
            )
        sections.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Reconciliation drift (SSP vs PGAM, last 7d)*\n" + "\n".join(lines)},
        })

    # DSP health
    if dsp_health:
        concerning = []
        for r in dsp_health.get("rows", []):
            wr = r.get("win_rate_delta_pct")
            rev = r.get("gross_revenue_pct_change")
            if (wr is not None and wr <= -5) or (rev is not None and rev <= -25):
                concerning.append(r)
        if concerning:
            concerning.sort(key=lambda r: r.get("gross_revenue", 0), reverse=True)
            lines = []
            for r in concerning[:6]:
                wr = r.get("win_rate_delta_pct")
                rev = r.get("gross_revenue_pct_change")
                bits = []
                if wr is not None and wr <= -5:
                    bits.append(f"win rate {wr:+.1f}pp")
                if rev is not None and rev <= -25:
                    bits.append(f"revenue {rev:+.1f}%")
                lines.append(f":small_red_triangle_down: *{r['demand_brand']}* — {' · '.join(bits)} ({_fmt_usd(r.get('gross_revenue', 0))})")
            sections.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*DSP health watch (WoW)*\n" + "\n".join(lines)},
            })

    if not sections:
        return None

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": ":mag:  Executive dashboard alerts", "emoji": True},
        },
    ]
    for i, s in enumerate(sections):
        if i > 0:
            blocks.append({"type": "divider"})
        blocks.append(s)
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": f"<{DASHBOARD_BASE}/admin/executive-dashboard|Open Executive Dashboard> · daily-deduped",
        }],
    })
    return blocks


def run() -> dict:
    """Daily-deduped (one post per day per signal mix) Slack push."""
    today = time.strftime("%Y-%m-%d")
    dedup_key = f"dashboard_alerts:{today}"
    if already_sent_today(dedup_key):
        print(f"[dashboard_alerts] already sent today ({dedup_key})", flush=True)
        return {"ok": True, "skipped": "deduped"}

    anomalies = _api_get("/api/reporting/partner-revenue/anomalies?window=7")
    dsp_health = _api_get("/api/reporting/partner-revenue/dsp-health")
    recon_drift = _fetch_recon_drift_neon(window_days=7)

    blocks = _build_blocks(anomalies, recon_drift, dsp_health)
    if not blocks:
        print("[dashboard_alerts] nothing to post — all clear.", flush=True)
        return {"ok": True, "skipped": "no_alerts"}

    fallback = "Executive dashboard alerts — open the dashboard for details."
    send_blocks(blocks=blocks, text=fallback)
    mark_sent(dedup_key)
    print(f"[dashboard_alerts] posted {len(blocks)} blocks", flush=True)
    return {"ok": True, "blocks": len(blocks)}


if __name__ == "__main__":
    res = run()
    sys.exit(0 if res.get("ok") else 1)
