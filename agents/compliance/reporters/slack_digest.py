"""
agents/compliance/reporters/slack_digest.py

Daily Slack digest of open compliance findings.

Reads pgam_direct.compliance_findings (status='open'), groups by severity,
and posts a single Block Kit message. Deduped per UTC date via slack helper
so an hourly runner doesn't re-spam — the digest is meant to be the
once-a-day "what's broken" board.

Routing
-------
If COMPLIANCE_SLACK_WEBHOOK is set in env, this digest posts there
(typically the dedicated #compliance channel) so daily-business alerts
don't drown out the operational #alerts feed. If not set, falls back to
the shared SLACK_WEBHOOK so the digest still gets delivered. Dedup keys
are channel-independent — flipping the webhook later won't cause a
double-post on the same day.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import date

import requests

from core import slack
from core.neon import connect


DEDUPE_KEY = "compliance_digest"
MAX_LINES_PER_SECTION = 10
COMPLIANCE_WEBHOOK_ENV = "COMPLIANCE_SLACK_WEBHOOK"


_QUERY = """
SELECT publisher_key, check_id, severity, detail,
       first_observed_at, last_observed_at
FROM pgam_direct.compliance_findings
WHERE status = 'open'
ORDER BY
    CASE severity
        WHEN 'critical' THEN 0
        WHEN 'high'     THEN 1
        WHEN 'medium'   THEN 2
        ELSE 3
    END,
    last_observed_at DESC;
"""

_SCORE_QUERY = """
SELECT publisher_key, compliance_score, open_critical, open_high
FROM pgam_direct.compliance_publisher_scores_daily
WHERE as_of = %(as_of)s
  AND compliance_score < 100
ORDER BY compliance_score ASC, open_critical DESC
LIMIT 5;
"""


def _load_open_findings() -> list[dict]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_QUERY)
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


def _load_lowest_scores(as_of: date) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCORE_QUERY, {"as_of": as_of})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] score query failed (non-fatal): {exc}")
        return []


def _format_pub_key(key: str) -> str:
    """Sentinel keys get readable labels; real publisher keys render as code."""
    if key.startswith("_ssp:"):
        return f"SSP `{key[5:]}`"
    if key.startswith("_ll_demand:"):
        return f"demand `{key[len('_ll_demand:'):]}`"
    if key.startswith("_ll_pub:"):
        return f"LL pub `{key[len('_ll_pub:'):]}`"
    return f"`{key}`"


def _format_line(f: dict) -> str:
    pub = _format_pub_key(f["publisher_key"])
    age_days = max((date.today() - f["first_observed_at"].date()).days, 0)
    age_tag = f"·{age_days}d" if age_days > 0 else "·new"
    return f"• {pub} {f['check_id']} {age_tag}"


def _format_score_line(row: dict) -> str:
    pub = _format_pub_key(row["publisher_key"])
    score = float(row["compliance_score"])
    sev = f"{int(row['open_critical'])}c/{int(row['open_high'])}h"
    return f"• {pub} *{score:.0f}* ({sev})"


def _build_blocks(findings: list[dict], summary: dict,
                  lowest_scores: list[dict]) -> list[dict]:
    by_sev: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        by_sev[f["severity"]].append(f)

    crit = by_sev.get("critical", [])
    high = by_sev.get("high", [])
    med = by_sev.get("medium", [])

    header = (
        f":shield: *Supply compliance — {date.today().isoformat()}*  "
        f"·  scanned {summary.get('publishers_scanned', 0)} publishers  "
        f"·  open: {len(crit)} crit / {len(high)} high / {len(med)} med"
    )
    context_bits = []
    if "ll_bridge_matched" in summary:
        context_bits.append(
            f"LL bridge {summary['ll_bridge_matched']}↔"
            f"{summary['ll_bridge_matched'] + summary.get('ll_bridge_unmatched', 0)}"
        )
    if "observed_ssp_rows" in summary:
        context_bits.append(f"{summary['observed_ssp_rows']} ssp×pub active")
    if "ssps_audited" in summary:
        context_bits.append(f"{summary['ssps_audited']} ssps audited")

    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
    ]
    if context_bits:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " · ".join(context_bits)}],
        })
    blocks.append({"type": "divider"})

    if crit:
        lines = [_format_line(f) for f in crit[:MAX_LINES_PER_SECTION]]
        if len(crit) > MAX_LINES_PER_SECTION:
            lines.append(f"…+{len(crit) - MAX_LINES_PER_SECTION} more critical")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":rotating_light: *Critical*\n" + "\n".join(lines)},
        })

    if high:
        lines = [_format_line(f) for f in high[:MAX_LINES_PER_SECTION]]
        if len(high) > MAX_LINES_PER_SECTION:
            lines.append(f"…+{len(high) - MAX_LINES_PER_SECTION} more high")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":warning: *High*\n" + "\n".join(lines)},
        })

    if med:
        lines = [_format_line(f) for f in med[: MAX_LINES_PER_SECTION // 2]]
        if len(med) > MAX_LINES_PER_SECTION // 2:
            lines.append(f"…+{len(med) - MAX_LINES_PER_SECTION // 2} more medium")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":small_orange_diamond: *Medium*\n" + "\n".join(lines)},
        })

    if lowest_scores:
        score_lines = [_format_score_line(r) for r in lowest_scores]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":chart_with_downwards_trend: *Lowest compliance scores*\n"
                             + "\n".join(score_lines)},
        })

    if not crit and not high and not med:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":white_check_mark: All publishers compliant."},
        })

    return blocks


def _post_to_compliance_webhook(webhook_url: str, blocks: list, text: str) -> bool:
    """POST blocks to the dedicated #compliance webhook."""
    resp = requests.post(
        webhook_url,
        json={"text": text, "blocks": blocks},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    return True


def post_digest(summary: dict, force: bool = False) -> bool:
    """Post (or skip-if-already-sent) the daily compliance digest."""
    if not force and slack.already_sent_today(DEDUPE_KEY):
        print("[compliance.slack_digest] already sent today — skipping")
        return False

    findings = _load_open_findings()
    lowest_scores = _load_lowest_scores(date.today())
    blocks = _build_blocks(findings, summary, lowest_scores)
    fallback = (
        f"Supply compliance: "
        f"{summary.get('findings_opened', 0)} opened, "
        f"{summary.get('findings_resolved', 0)} resolved this run."
    )

    webhook = os.environ.get(COMPLIANCE_WEBHOOK_ENV, "").strip()
    try:
        if webhook:
            _post_to_compliance_webhook(webhook, blocks, fallback)
            print(f"[compliance.slack_digest] posted to #compliance webhook")
        else:
            slack.send_blocks(blocks, text=fallback)
            print("[compliance.slack_digest] posted via default SLACK_WEBHOOK "
                  "(set COMPLIANCE_SLACK_WEBHOOK to route to #compliance)")
    except Exception as exc:
        print(f"[compliance.slack_digest] Slack post failed: {exc}")
        return False

    slack.mark_sent(DEDUPE_KEY)
    return True
