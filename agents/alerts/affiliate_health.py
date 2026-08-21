"""
agents/alerts/affiliate_health.py
──────────────────────────────────────────────────────────────────────────────
Daily watchdog on boxingnews.com's affiliate attribution.

WHY THIS EXISTS
───────────────
`affiliate_clicks` was created with three stated jobs (see boxingnews
src/lib/affiliate/schema.ts). The third — "Alert if click volume drops to
zero (broken tracking link)" — was never built. Nothing in either repo
read the table except the Ticketmaster admin page, and pgam-intelligence
ran ~30 revenue alert agents with none on affiliate.

That gap has already cost us once. On 2026-08-06 boxingnews commit
fb59da1 tore out the DAZN Partnerize wrap on the stated premise that
"Partnerize dropped boxingnews.com's DAZN affiliate in 2026-08". No
termination notice from Partnerize or DAZN exists, and on 2026-08-21
Partnerize released funds against a 2026-06-28 conversion — so the
participation was live the whole time. The belief went unchallenged for
six weeks because no number was ever put in front of anyone.

This agent puts the numbers in front of someone. Three checks, each
independent so a missing credential degrades that check rather than the
run:

  A. ATTRIBUTION PROBE (needs only outbound HTTPS)
     Ask the live click bouncer where each operator resolves to. If the
     302 lands on the operator's own marketing domain, the affiliate URL
     is unset and those clicks earn nothing. This is the check that
     would have caught fb59da1 the next morning, and it is the one that
     will catch a sportsbook deal landing without its env var wired —
     the failure mode boxingnews's own cta-gate.ts is designed around
     but cannot report on.

  B. CLICK-VOLUME REGRESSION (needs BOXINGNEWS_DATABASE_URL)
     Per operator, compare the last 7 days of clicks against the prior
     28. An operator that used to get traffic and now gets none has a
     broken link, a removed CTA, or a gate that silently closed.

  C. CONVERSION RECONCILIATION (needs PARTNERIZE_APP_KEY/_API_KEY)
     Our ledger against Partnerize's. Clicks with no conversions over a
     long window is the signature of lost attribution rather than thin
     demand — the exact question the 2026-08 incident left open.

SAFETY POSTURE
──────────────
Read-only everywhere. The boxingnews DB connection is the read-only
helper in core/boxingnews_db.py; every Partnerize call is a GET; the
bouncer probe is a GET with redirects disabled, so the reader is never
actually sent to the operator.

One side effect is unavoidable and is handled explicitly: the bouncer
writes a row to `affiliate_clicks` for every hit, including ours. Probes
therefore carry `p=monitor`, and check B excludes that placement, so the
watchdog can't inflate the very metric it is watching.

SCHEDULING
──────────
Registered in scheduler.py for 08:45 ET, behind
PGAM_AFFILIATE_HEALTH_ENABLED (default on). 08:45 sits just after the
existing boxingnews ingest-health alert at 08:30 so the two boxingnews
watchdogs report together, and well after midnight UTC so "last 7 days"
covers whole days of traffic.
"""

from __future__ import annotations

import os
from typing import Any

import requests

from core.slack import send_text, already_sent_today, mark_sent

ALERT_KEY = "affiliate_health"

BASE_URL = os.environ.get("BOXINGNEWS_BASE_URL", "https://www.boxingnews.com").rstrip("/")

# Placement tag on our own probe hits. The bouncer logs every request, so
# this is what lets check B exclude the watchdog's own footprint.
PROBE_PLACEMENT = "monitor"

# Mirrors OPERATORS in boxingnews src/lib/affiliate/operators.ts: operator
# id -> the operator's own marketing domain, which is what
# `resolveOperatorUrl` falls back to when AFFILIATE_<OP>_URL is unset.
# A 302 landing on this domain therefore means "no affiliate attribution".
#
# Keep in sync when an operator is added there. An operator present in
# operators.ts but missing here is simply not probed — check A reports the
# ids it probed so a drift shows up as a short list rather than a silent
# pass. Ticketmaster is deliberately absent: it uses a different bouncer
# (/api/tickets/click) with different semantics.
OPERATOR_MARKETING_DOMAINS: dict[str, str] = {
    "dazn": "dazn.com",
    "fanduel": "fanduel.com",
    "draftkings": "draftkings.com",
    "bet365-uk": "bet365.com",
    "betmgm": "betmgm.com",
}

# Known affiliate-network redirect hosts. A 302 into one of these is
# positive evidence of attribution rather than merely "not the marketing
# domain", which keeps a future network swap from reading as a failure.
NETWORK_HOSTS = (
    "prf.hn",           # Partnerize
    "partnerize",
    "go.skimresources.com",
    "anrdoezrs.net",    # CJ
    "dpbolvw.net",      # CJ
    "kqzyfj.com",       # CJ
    "jdoqocy.com",      # CJ
    "impact.com",
    "sjv.io",           # Impact
    "awin1.com",
    "prf.io",
    "xtb.tf",
    "mediaservices",
)

# Check B thresholds. Deliberately conservative: an operator needs real
# prior traffic before a zero week counts as a regression, so a CTA that
# only ever saw a trickle doesn't page anyone.
LOOKBACK_RECENT_DAYS = 7
LOOKBACK_PRIOR_DAYS = 28
MIN_PRIOR_CLICKS = 4

HTTP_TIMEOUT = 20


# ─────────────────────────────────────────────────────────────────────────
# check A — attribution probe
# ─────────────────────────────────────────────────────────────────────────


def _classify_redirect(operator_id: str, location: str | None) -> tuple[str, str]:
    """Classify a bouncer 302 target as tracked / untracked / unknown.

    Returns (verdict, detail). Verdicts:
      tracked    — lands on a known affiliate-network host
      untracked  — lands on the operator's own marketing domain, i.e.
                   AFFILIATE_<OP>_URL is unset and the click earns nothing
      unknown    — somewhere else; reported but not alerted on, because a
                   new network we haven't listed shouldn't read as broken
    """
    if not location:
        return "unknown", "no Location header on the redirect"

    host = location.split("//", 1)[-1].split("/", 1)[0].split("?", 1)[0].lower()
    if any(n in host for n in NETWORK_HOSTS):
        return "tracked", host

    marketing = OPERATOR_MARKETING_DOMAINS.get(operator_id, "")
    if marketing and (host == marketing or host.endswith("." + marketing)):
        return "untracked", host

    return "unknown", host


def _probe_bouncer(operator_id: str) -> dict[str, Any]:
    """GET the bouncer for one operator without following the redirect."""
    url = f"{BASE_URL}/api/affiliate/click"
    try:
        resp = requests.get(
            url,
            params={"op": operator_id, "p": PROBE_PLACEMENT},
            allow_redirects=False,
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as exc:
        return {"operator": operator_id, "verdict": "error", "detail": str(exc)[:160]}

    if resp.status_code == 404:
        return {
            "operator": operator_id,
            "verdict": "error",
            "detail": "bouncer returned 404 — operator id not registered in operators.ts",
        }
    if resp.status_code not in (301, 302, 303, 307, 308):
        return {
            "operator": operator_id,
            "verdict": "error",
            "detail": f"expected a redirect, got HTTP {resp.status_code}",
        }

    verdict, detail = _classify_redirect(operator_id, resp.headers.get("Location"))
    return {"operator": operator_id, "verdict": verdict, "detail": detail}


def probe_attribution() -> list[dict[str, Any]]:
    """Check A across every operator we know how to classify."""
    return [_probe_bouncer(op) for op in sorted(OPERATOR_MARKETING_DOMAINS)]


# ─────────────────────────────────────────────────────────────────────────
# check B — click-volume regression
# ─────────────────────────────────────────────────────────────────────────


def click_volume() -> list[dict[str, Any]] | None:
    """Per-operator recent vs prior click counts from our own ledger.

    Returns None when BOXINGNEWS_DATABASE_URL isn't configured, so the
    caller can report the check as skipped rather than as passing.
    """
    if not os.environ.get("BOXINGNEWS_DATABASE_URL"):
        return None

    # Imported lazily: core.boxingnews_db raises at connect time when the
    # DSN is absent, and this agent must stay runnable on hosts that only
    # have HTTP (check A alone is still worth running).
    from core.boxingnews_db import connect as connect_boxingnews

    sql = f"""
        SELECT operator_id,
               COUNT(*) FILTER (
                   WHERE clicked_at >= NOW() - INTERVAL '{LOOKBACK_RECENT_DAYS} days'
               ) AS recent,
               COUNT(*) FILTER (
                   WHERE clicked_at <  NOW() - INTERVAL '{LOOKBACK_RECENT_DAYS} days'
               ) AS prior
          FROM affiliate_clicks
         WHERE clicked_at >= NOW() - INTERVAL '{LOOKBACK_RECENT_DAYS + LOOKBACK_PRIOR_DAYS} days'
           AND COALESCE(placement, '') <> %s
         GROUP BY operator_id
         ORDER BY operator_id
    """
    with connect_boxingnews() as conn, conn.cursor() as cur:
        cur.execute(sql, (PROBE_PLACEMENT,))
        return [
            {"operator": r[0], "recent": int(r[1] or 0), "prior": int(r[2] or 0)}
            for r in cur.fetchall()
        ]


# ─────────────────────────────────────────────────────────────────────────
# check C — conversion reconciliation (Partnerize)
# ─────────────────────────────────────────────────────────────────────────


def partnerize_conversions(days: int = 90) -> dict[str, Any] | None:
    """Partnerize conversion totals for the trailing window.

    Returns None when the Partnerize credentials or publisher id aren't
    configured. Any API failure also returns None with a logged reason —
    a watchdog must not take down its own run because one upstream is
    unhappy.
    """
    import base64
    from datetime import date, timedelta

    app_key = os.environ.get("PARTNERIZE_APP_KEY", "").strip()
    api_key = os.environ.get("PARTNERIZE_API_KEY", "").strip()
    publisher_id = os.environ.get("PARTNERIZE_PUBLISHER_ID", "").strip()
    if not (app_key and api_key and publisher_id):
        return None

    token = base64.b64encode(f"{app_key}:{api_key}".encode()).decode()
    end = date.today()
    start = end - timedelta(days=days)
    url = (
        f"https://api.partnerize.com/reporting/report_publisher"
        f"/publisher/{publisher_id}/conversion.json"
    )
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Basic {token}", "Accept": "application/json"},
            params={"start_date": start.isoformat(), "end_date": end.isoformat()},
            timeout=HTTP_TIMEOUT,
        )
        if resp.status_code >= 400:
            print(f"[affiliate_health] Partnerize HTTP {resp.status_code}, skipping check C")
            return None
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        print(f"[affiliate_health] Partnerize call failed, skipping check C: {exc}")
        return None

    return {
        "days": days,
        "conversions": data.get("total_conversion_count"),
        "commission": data.get("total_publisher_commission"),
    }


# ─────────────────────────────────────────────────────────────────────────
# report
# ─────────────────────────────────────────────────────────────────────────


def _conversion_total(count: Any) -> float:
    """Normalise Partnerize's conversion count to a single number.

    The reporting endpoints return currency-keyed maps for the totals —
    `{"EUR": 1}` in the API's own example — but a plain scalar in some
    responses (the console's CSV export emits a bare `1`). Sum the map so
    `{"GBP": 0, "USD": 0}` reads as zero rather than as truthy, which is
    what a naive falsiness check would get wrong.

    Anything unparseable returns 0.0, and check C only alerts when it
    ALSO sees real click volume — so a shape we don't understand can't
    manufacture an alert on its own.
    """
    if isinstance(count, dict):
        total = 0.0
        for value in count.values():
            try:
                total += float(value)
            except (TypeError, ValueError):
                continue
        return total
    try:
        return float(count)
    except (TypeError, ValueError):
        return 0.0


def build_findings(
    probes: list[dict[str, Any]],
    volume: list[dict[str, Any]] | None,
    conversions: dict[str, Any] | None,
) -> tuple[list[str], list[str]]:
    """Turn raw check output into (alerts, context) message lines.

    `alerts` are what make this worth pinging about. `context` is always
    attached so the reader can see what the checks actually observed —
    a watchdog that only ever says "problem" trains people to distrust
    its silence.
    """
    alerts: list[str] = []
    context: list[str] = []

    # --- check A -------------------------------------------------------
    untracked = [p for p in probes if p["verdict"] == "untracked"]
    errored = [p for p in probes if p["verdict"] == "error"]
    tracked = [p for p in probes if p["verdict"] == "tracked"]
    unknown = [p for p in probes if p["verdict"] == "unknown"]

    if untracked:
        names = ", ".join(f"`{p['operator']}`" for p in untracked)
        alerts.append(
            f"*Unattributed click-outs* — {names} redirect to the operator's own "
            "marketing site, so those clicks earn nothing. Set the matching "
            "`AFFILIATE_<OP>_URL` in the boxingnews Vercel project and redeploy "
            "(Vercel bakes env vars in at build time)."
        )
    if errored:
        detail = "; ".join(f"`{p['operator']}`: {p['detail']}" for p in errored)
        alerts.append(f"*Bouncer probe failed* — {detail}")

    context.append(
        f"Attribution probe: {len(tracked)} tracked, {len(untracked)} untracked, "
        f"{len(unknown)} unrecognised, {len(errored)} errored "
        f"(of {len(probes)} operators probed)."
    )
    if unknown:
        context.append(
            "  Unrecognised redirect targets (not alerted — could be a new network): "
            + ", ".join(f"`{p['operator']}` -> {p['detail']}" for p in unknown)
        )

    # --- check B -------------------------------------------------------
    if volume is None:
        context.append(
            "Click-volume check skipped: BOXINGNEWS_DATABASE_URL not set on this host."
        )
    else:
        regressed = [
            v for v in volume
            if v["recent"] == 0 and v["prior"] >= MIN_PRIOR_CLICKS
        ]
        if regressed:
            detail = "; ".join(
                f"`{v['operator']}` {v['prior']} clicks in the prior "
                f"{LOOKBACK_PRIOR_DAYS}d, 0 in the last {LOOKBACK_RECENT_DAYS}d"
                for v in regressed
            )
            alerts.append(
                f"*Click volume fell to zero* — {detail}. Likely a removed CTA, a "
                "gate that closed, or a broken link."
            )
        if volume:
            context.append(
                f"Click volume (last {LOOKBACK_RECENT_DAYS}d / prior {LOOKBACK_PRIOR_DAYS}d): "
                + ", ".join(f"{v['operator']} {v['recent']}/{v['prior']}" for v in volume)
            )
        else:
            context.append(
                f"Click volume: no rows in the last "
                f"{LOOKBACK_RECENT_DAYS + LOOKBACK_PRIOR_DAYS} days at all."
            )

    # --- check C -------------------------------------------------------
    if conversions is None:
        context.append(
            "Conversion reconciliation skipped: Partnerize credentials not set on this host."
        )
    else:
        context.append(
            f"Partnerize (trailing {conversions['days']}d): "
            f"{conversions['conversions']} conversions, "
            f"commission {conversions['commission']}."
        )
        # Clicks with no conversions over a long window is the signature of
        # lost attribution rather than thin demand. Only meaningful when we
        # can see both sides.
        dazn_clicks = None
        if volume:
            dazn_clicks = next(
                (v["recent"] + v["prior"] for v in volume if v["operator"] == "dazn"),
                None,
            )
        if dazn_clicks and dazn_clicks >= 50 and _conversion_total(conversions["conversions"]) == 0:
            alerts.append(
                f"*Clicks but no conversions* — we logged {dazn_clicks} DAZN clicks in "
                f"the last {LOOKBACK_RECENT_DAYS + LOOKBACK_PRIOR_DAYS}d and Partnerize "
                f"reports 0 conversions over {conversions['days']}d. That pattern is "
                "attribution loss, not thin demand — check the camref is still in the "
                "redirect chain."
            )

    return alerts, context


def run() -> None:
    """Daily check. Slacks only when something is actionable."""
    if already_sent_today(ALERT_KEY):
        print("[affiliate_health] already sent today, skipping")
        return

    probes = probe_attribution()
    volume = click_volume()
    conversions = partnerize_conversions()

    alerts, context = build_findings(probes, volume, conversions)

    for line in context:
        print(f"[affiliate_health] {line}")

    if not alerts:
        # Healthy — no Slack, and deliberately NOT deduped, so a break
        # tomorrow fires on tomorrow's run. Same posture as
        # boxingnews_ingest_health.
        print("[affiliate_health] healthy — no alert sent")
        return

    msg = (
        ":rotating_light: *BoxingNews affiliate health*\n\n"
        + "\n\n".join(alerts)
        + "\n\n_Observed:_\n"
        + "\n".join(f"• {line}" for line in context)
    )

    try:
        send_text(msg)
        mark_sent(ALERT_KEY)
        print(f"[affiliate_health] posted {len(alerts)} alert(s) to Slack")
    except Exception as exc:
        print(f"[affiliate_health] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
