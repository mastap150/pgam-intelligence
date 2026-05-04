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


def _fmt_age(min_value: float | None) -> str:
    if min_value is None:
        return "—"
    if min_value < 1:
        return "just now"
    if min_value < 60:
        return f"{round(min_value)} min ago"
    if min_value < 60 * 24:
        return f"{min_value / 60:.1f}h ago"
    return f"{int(min_value / 60 / 24)}d ago"


def _build_blocks(
    anomalies: dict | None,
    recon_drift: list[dict],
    dsp_health: dict | None,
    etl_health: dict | None,
) -> list[dict] | None:
    """Build Slack Block Kit blocks. Returns None if there's nothing
    actionable to post (we don't spam an "all clear" message)."""
    sections: list[dict] = []

    # ETL freshness — circuit-breaker section. Surfaces stale and
    # broken sources in the daily digest. Broken-tier sources also
    # fire a separate immediate alert via _post_broken_etl_alerts()
    # before the digest runs, so this section is the "remember these
    # are still down" reminder rather than the first warning.
    if etl_health:
        summary = etl_health.get("summary", {}) or {}
        sources = etl_health.get("sources", []) or []
        broken = [s for s in sources if s.get("status") == "broken"]
        stale  = [s for s in sources if s.get("status") == "stale"]
        unknown = [s for s in sources if s.get("status") == "unknown"]
        # Worth posting if anything's not fresh. Pure "all fresh" stays silent.
        if broken or stale or unknown:
            lines: list[str] = []
            for s in sorted(broken, key=lambda x: -(x.get("age_minutes") or 0))[:6]:
                lines.append(
                    f":red_circle: *{s.get('label')}* — last write "
                    f"{_fmt_age(s.get('age_minutes'))} (agent `{s.get('agent')}`)"
                )
            for s in sorted(stale, key=lambda x: -(x.get("age_minutes") or 0))[:4]:
                lines.append(
                    f":large_yellow_circle: *{s.get('label')}* — "
                    f"{_fmt_age(s.get('age_minutes'))}"
                )
            for s in unknown[:3]:
                lines.append(
                    f":white_circle: *{s.get('label')}* — empty/unknown "
                    f"(table `{s.get('table')}`)"
                )
            header = (
                f"*ETL freshness* — {summary.get('broken', 0)} broken, "
                f"{summary.get('stale', 0)} stale, "
                f"{summary.get('fresh', 0)} fresh"
            )
            sections.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": header + "\n" + "\n".join(lines)},
            })

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


def _post_broken_etl_alerts(etl_health: dict | None, today: str) -> int:
    """Circuit-breaker: any ETL source >4h stale (status='broken')
    fires immediately with per-source-per-day dedupe.

    The daily digest still echoes broken sources in its ETL section
    — but that runs once a day. If a backfill agent dies at 11am we
    want Slack to know by noon, not at 9am tomorrow. Per-source
    dedupe means we get one alert per breakage per day instead of
    one per scheduler tick.
    """
    if not etl_health:
        return 0
    broken = [s for s in (etl_health.get("sources") or []) if s.get("status") == "broken"]
    if not broken:
        return 0
    posted = 0
    for s in broken:
        key = f"dashboard_alerts:etl_broken:{s.get('key')}:{today}"
        if already_sent_today(key):
            continue
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": ":rotating_light: ETL pipeline broken", "emoji": True},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{s.get('label')}* hasn't written in {_fmt_age(s.get('age_minutes'))}.\n"
                        f"• Agent: `{s.get('agent')}`\n"
                        f"• Table: `{s.get('table')}`\n"
                        f"• Rows on file: {int(s.get('rows') or 0):,}\n"
                        f"\n_Dashboard numbers backed by this source are stale until the agent recovers._"
                    ),
                },
            },
            {
                "type": "context",
                "elements": [{
                    "type": "mrkdwn",
                    "text": f"<{DASHBOARD_BASE}/admin/executive-dashboard|Open dashboard> · "
                            f"check Render scheduler logs for `{s.get('agent')}`",
                }],
            },
        ]
        send_blocks(blocks=blocks, text=f"ETL broken: {s.get('label')} ({_fmt_age(s.get('age_minutes'))})")
        mark_sent(key)
        posted += 1
        print(f"[dashboard_alerts] posted ETL-broken alert for {s.get('key')}", flush=True)
    return posted


def run() -> dict:
    """Daily-deduped (one post per day per signal mix) Slack push,
    plus immediate per-source alerts when an ETL goes broken-tier.

    Order matters: broken-ETL alerts fire BEFORE the daily digest
    so an outage gets the loudest, fastest signal even on the same
    tick that produces the digest.
    """
    today = time.strftime("%Y-%m-%d")

    # Always probe ETL freshness — both for the immediate broken
    # alerts and for inclusion in the daily digest section.
    etl_health = _api_get("/api/reporting/partner-revenue/etl-health")

    # Fire immediate per-source alerts for any broken-tier source.
    # Independently deduped from the daily digest so a 9am digest
    # doesn't suppress an 11am breakage.
    broken_posted = _post_broken_etl_alerts(etl_health, today)

    dedup_key = f"dashboard_alerts:{today}"
    if already_sent_today(dedup_key):
        print(f"[dashboard_alerts] daily digest already sent ({dedup_key})", flush=True)
        return {"ok": True, "skipped": "deduped", "etl_broken_posted": broken_posted}

    anomalies = _api_get("/api/reporting/partner-revenue/anomalies?window=7")
    dsp_health = _api_get("/api/reporting/partner-revenue/dsp-health")
    recon_drift = _fetch_recon_drift_neon(window_days=7)

    blocks = _build_blocks(anomalies, recon_drift, dsp_health, etl_health)
    if not blocks:
        print("[dashboard_alerts] nothing to post — all clear.", flush=True)
        return {"ok": True, "skipped": "no_alerts", "etl_broken_posted": broken_posted}

    fallback = "Executive dashboard alerts — open the dashboard for details."
    send_blocks(blocks=blocks, text=fallback)
    mark_sent(dedup_key)
    print(f"[dashboard_alerts] posted {len(blocks)} blocks", flush=True)
    return {"ok": True, "blocks": len(blocks), "etl_broken_posted": broken_posted}


if __name__ == "__main__":
    res = run()
    sys.exit(0 if res.get("ok") else 1)
