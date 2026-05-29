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
-- Lowest scores among publishers that ACTUALLY earned revenue in the
-- trailing window — without this gate the section is dominated by
-- stale sellers.json entries that are inactive but happen to have a
-- single info finding. Falls back to all-publishers if the activity
-- table hasn't been populated yet.
SELECT s.publisher_key, s.compliance_score, s.open_critical, s.open_high
FROM pgam_direct.compliance_publisher_scores_daily s
JOIN pgam_direct.compliance_publishers cp
  ON cp.publisher_key = s.publisher_key
WHERE s.as_of = %(as_of)s
  AND s.compliance_score < 100
  AND (cp.is_active_recent = TRUE OR cp.is_active_recent IS NULL)
ORDER BY s.compliance_score ASC, s.open_critical DESC
LIMIT 5;
"""

# Per-supply-partner rollup: count open findings against entities under
# each LL supply partner. Joins compliance_findings (which keys on
# entity_key for Phase 5) → compliance_supply_entities (which carries the
# ll_publisher_name + entity count per partner).
_PARTNER_ROLLUP_QUERY = """
SELECT
    e.ll_publisher_id,
    e.ll_publisher_name,
    COUNT(DISTINCT e.entity_key)                                       AS entities,
    COUNT(DISTINCT f.entity_key) FILTER (WHERE f.severity = 'critical') AS pubs_crit,
    COUNT(DISTINCT f.entity_key) FILTER (WHERE f.severity = 'high')     AS pubs_high,
    COUNT(f.finding_id) FILTER (WHERE f.severity = 'critical')          AS findings_crit,
    COUNT(f.finding_id) FILTER (WHERE f.severity = 'high')              AS findings_high
FROM pgam_direct.compliance_supply_entities e
LEFT JOIN (
    SELECT publisher_key AS entity_key, severity, finding_id
    FROM pgam_direct.compliance_findings
    WHERE status = 'open'
      AND (publisher_key LIKE 'dom:%%' OR publisher_key LIKE 'app:%%')
) f ON f.entity_key = e.entity_key
GROUP BY e.ll_publisher_id, e.ll_publisher_name
ORDER BY findings_crit DESC NULLS LAST,
         findings_high DESC NULLS LAST,
         entities DESC;
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


def _load_partner_rollup() -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_PARTNER_ROLLUP_QUERY)
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] partner rollup query failed (non-fatal): {exc}")
        return []


def _format_partner_row(r: dict) -> str:
    name = (r.get("ll_publisher_name") or "?")[:24]
    entities = int(r.get("entities") or 0)
    crit = int(r.get("findings_crit") or 0)
    high = int(r.get("findings_high") or 0)
    if crit == 0 and high == 0:
        verdict = ":white_check_mark:"
    elif crit > 0:
        verdict = ":rotating_light:"
    else:
        verdict = ":warning:"
    return (
        f"{verdict} *{name}*  ·  {entities} entities  ·  "
        f"{crit} critical / {high} high"
    )


def _format_pub_key(key: str) -> str:
    """Sentinel keys get readable labels; real publisher keys render as code."""
    if key.startswith("_ssp:"):
        return f"SSP `{key[5:]}`"
    if key.startswith("_ll_demand:"):
        return f"demand `{key[len('_ll_demand:'):]}`"
    if key.startswith("_ll_pub:"):
        return f"LL pub `{key[len('_ll_pub:'):]}`"
    if key.startswith("_dynamic_schain_pub:"):
        return f"emitted schain pub `{key[len('_dynamic_schain_pub:'):]}`"
    if key.startswith("_ll_publisher:"):
        return f"LL supply partner `{key[len('_ll_publisher:'):]}`"
    if key.startswith("_demand:"):
        return f"demand `{key[len('_demand:'):]}`"
    if key.startswith("_pub_config:"):
        return f"publisher_config `{key[len('_pub_config:'):]}`"
    if key.startswith("dom:"):
        return f"`{key[4:]}`"
    if key.startswith("app:"):
        return f"app `{key[4:]}`"
    return f"`{key}`"


def _detail_rev(f: dict) -> float:
    """Pull revenue_7d out of the finding detail jsonb; 0 if missing."""
    det = f.get("detail") or {}
    if isinstance(det, dict):
        try:
            return float(det.get("revenue_7d") or 0)
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def _format_line(f: dict) -> str:
    pub = _format_pub_key(f["publisher_key"])
    age_days = max((date.today() - f["first_observed_at"].date()).days, 0)
    age_tag = f"·{age_days}d" if age_days > 0 else "·new"
    rev = _detail_rev(f)
    rev_tag = f" ·${rev:,.0f}/7d" if rev >= 1.0 else ""
    # If the finding carries an SSP key (per-entity reseller checks
    # always do), surface it inline so 4 findings against the same
    # domain don't look like 4 duplicate lines.
    det = f.get("detail") or {}
    ssp = det.get("ssp") if isinstance(det, dict) else None
    check = f["check_id"]
    if ssp:
        # Compress the check_id since the SSP tells the whole story.
        short_check = check.replace("adstxt.reseller_", "reseller_")
        return f"• {pub} *{ssp}* `{short_check}` {age_tag}{rev_tag}"
    return f"• {pub} `{check}` {age_tag}{rev_tag}"


def _sort_by_revenue(findings: list[dict]) -> list[dict]:
    """Highest revenue first, then newest. Drives 'actionable first' ordering."""
    return sorted(
        findings,
        key=lambda f: (-_detail_rev(f), -f["last_observed_at"].timestamp()),
    )


def _format_score_line(row: dict) -> str:
    pub = _format_pub_key(row["publisher_key"])
    score = float(row["compliance_score"])
    sev = f"{int(row['open_critical'])}c/{int(row['open_high'])}h"
    return f"• {pub} *{score:.0f}* ({sev})"


def _humanize_check(check_id: str) -> str:
    """Map check_id → operator-facing label (no `code_format`, no namespaces)."""
    return {
        "adstxt.universal_direct_missing":      "PGAM DIRECT line missing",
        "adstxt.universal_direct_wrong_seller": "PGAM seller_id wrong",
        "adstxt.universal_direct_wrong_type":   "PGAM line marked RESELLER (should be DIRECT)",
        "adstxt.universal_direct_wrong_seat":   "Wrong PGAM seat",
        "adstxt.reseller_missing":              "RESELLER line missing",
        "adstxt.reseller_wrong_seller":         "RESELLER line wrong account",
        "adstxt.reseller_wrong_type":           "RESELLER line marked DIRECT",
        "adstxt.reseller_cert_mismatch":        "Cert authority mismatch",
        "adstxt.file_unreachable":              "ads.txt not reachable",
        "adstxt.file_empty":                    "ads.txt is empty",
    }.get(check_id, check_id)


def _trunc_list(items: list, cap: int = 4) -> str:
    """Render a list as `a, b, c, …+N more`. Keeps sections under
    Slack's 3000-char-per-section ceiling when a publisher's ads.txt
    has 100+ sub-publisher account_ids for the same SSP."""
    items = [str(x) for x in items if x not in (None, "")]
    if not items:
        return "(none)"
    if len(items) <= cap:
        return ", ".join(items)
    return ", ".join(items[:cap]) + f", …+{len(items) - cap} more"


def _fix_lines_for(f: dict) -> list[str]:
    """Render the literal copy-paste fix lines for one finding.

    The detail jsonb carries `expected_line` (what should be in ads.txt)
    and observed values where applicable. We surface both so the operator
    can grep-and-replace without round-tripping back to the validator.
    """
    det = f.get("detail") or {}
    if not isinstance(det, dict):
        return []
    check = f["check_id"]
    ssp = det.get("ssp")
    expected_line = det.get("expected_line")
    out: list[str] = []

    # Universal-DIRECT family
    if check == "adstxt.universal_direct_missing":
        if expected_line and "<unknown>" not in expected_line:
            out.append(f"  Add to ads.txt: `{expected_line}`")
        else:
            out.append("  Add to ads.txt: `pgamssp.com, <publisher's PGAM seller_id>, DIRECT`")
    elif check == "adstxt.universal_direct_wrong_seller":
        obs = det.get("observed_seller_ids") or []
        exp = det.get("expected_seller_id")
        out.append(f"  Found PGAM seat(s): `{_trunc_list(obs)}`  →  expect `{exp}`")
        out.append(f"  Replace with: `pgamssp.com, {exp}, DIRECT`")
    elif check == "adstxt.universal_direct_wrong_type":
        sid = det.get("seller_id")
        rels = det.get("observed_relationships") or []
        out.append(f"  PGAM line says {'/'.join(rels)} — must be DIRECT")
        out.append(f"  Replace with: `pgamssp.com, {sid}, DIRECT`")
    elif check == "adstxt.universal_direct_wrong_seat":
        seats = det.get("observed_seats") or []
        if seats:
            sample = ", ".join(
                f"{s.get('seller_id')} ({s.get('owner_name','?')})"
                for s in seats[:2] if isinstance(s, dict)
            )
            out.append(f"  PGAM seat in ads.txt: `{sample}`  →  not a PGAM-owned seat")
            out.append("  Replace with the publisher's PGAM-issued seller_id; "
                       "look it up in `https://www.pgamssp.com/sellers.json`")

    # RESELLER family
    elif check == "adstxt.reseller_missing":
        if expected_line:
            out.append(f"  Add to ads.txt: `{expected_line}`  ({ssp})")
    elif check == "adstxt.reseller_wrong_seller":
        obs = det.get("observed_account_ids") or []
        exp = det.get("expected_account_id")
        out.append(f"  Found in ads.txt: `{_trunc_list(obs)}`  →  expect `{exp}`")
        if expected_line:
            out.append(f"  Replace with: `{expected_line}`")
    elif check == "adstxt.reseller_wrong_type":
        rels = det.get("observed_relationships") or []
        out.append(f"  {ssp}: line marked {'/'.join(rels)} — must be RESELLER")
        if expected_line:
            out.append(f"  Replace with: `{expected_line}`")
    elif check == "adstxt.reseller_cert_mismatch":
        if expected_line:
            out.append(f"  {ssp}: cert authority on line doesn't match")
            out.append(f"  Canonical line: `{expected_line}`")
    elif check == "adstxt.file_unreachable":
        st = det.get("http_status")
        out.append(f"  ads.txt fetch returned {st} — verify the URL is public")
    elif check == "adstxt.file_empty":
        out.append("  ads.txt returned 200 but has no parsable lines")
    return out


def _action_card_blocks(findings: list[dict],
                        max_entities: int = 5) -> list[dict]:
    """Group findings by entity, render top-N as separate Slack section
    blocks (one per entity).

    Each card lists the entity, revenue, ads.txt URL, and the literal
    'Add this line / Replace this line' fixes — the section a human can
    act on without opening the validator code or rejoining ssp_registry.

    Returns a list of blocks (header + N cards + divider). Empty list
    if no real-entity findings to surface.
    """
    entity_findings: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        pk = f["publisher_key"]
        if pk.startswith(("dom:", "app:")) and f["severity"] in ("critical", "high"):
            entity_findings[pk].append(f)
    if not entity_findings:
        return []

    entities_ranked = sorted(
        entity_findings.items(),
        key=lambda kv: -max((_detail_rev(f) for f in kv[1]), default=0.0),
    )[:max_entities]
    total_at_risk = sum(
        max((_detail_rev(f) for f in fs), default=0.0)
        for _, fs in entities_ranked
    )

    out: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (":dart: *Action queue — fix these first*\n"
                          f"_${total_at_risk:,.0f}/7d across the top "
                          f"{len(entities_ranked)} entities. "
                          "Literal lines to add or replace below._")},
    }]

    sev_rank = {"critical": 0, "high": 1, "medium": 2, "info": 3}
    for i, (pk, fs) in enumerate(entities_ranked, start=1):
        pub = _format_pub_key(pk)
        entity_rev = max((_detail_rev(f) for f in fs), default=0.0)
        urls = sorted({(f.get("detail") or {}).get("url") for f in fs
                       if isinstance(f.get("detail"), dict)
                       and (f.get("detail") or {}).get("url")})
        ll_pub = next(
            ((f.get("detail") or {}).get("ll_publisher_name") for f in fs
             if isinstance(f.get("detail"), dict)
             and (f.get("detail") or {}).get("ll_publisher_name")),
            None,
        )
        ll_tag = f"  ·  via {ll_pub}" if ll_pub else ""
        lines = [
            f"*{i}. {pub}*  ·  ${entity_rev:,.0f}/7d  ·  "
            f"{len(fs)} issue{'s' if len(fs) > 1 else ''}{ll_tag}",
        ]
        for u in urls[:2]:
            lines.append(f"source: {u}")
        fs_sorted = sorted(fs, key=lambda f: (sev_rank.get(f["severity"], 9),
                                              f["check_id"]))
        for f in fs_sorted:
            label = _humanize_check(f["check_id"])
            ssp = (f.get("detail") or {}).get("ssp") if isinstance(f.get("detail"), dict) else None
            ssp_tag = f" — {ssp}" if ssp else ""
            lines.append(f"• *{label}*{ssp_tag}")
            for fix in _fix_lines_for(f):
                lines.append(fix)

        body = "\n".join(lines)
        # Slack section cap = 3000 chars; one card stays well under.
        if len(body) > 2900:
            body = body[:2880] + "\n_…see dashboard for full list._"
        out.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": body},
        })
    return out


_SSP_SCORECARD_QUERY = """
SELECT
    ssp_partner_name,
    SUM(revenue_7d)                                  AS revenue,
    COUNT(*)                                         AS entities,
    COUNT(*) FILTER (WHERE status = 'critical')      AS critical_n,
    COUNT(*) FILTER (WHERE status = 'warning')       AS warning_n,
    COUNT(*) FILTER (WHERE status = 'healthy')       AS healthy_n
FROM pgam_direct.compliance_entity_ssp_audit
WHERE as_of = %(as_of)s
GROUP BY ssp_partner_name
HAVING SUM(revenue_7d) > 0
ORDER BY revenue DESC
LIMIT 15;
"""


def _load_ssp_scorecard(as_of: date) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SSP_SCORECARD_QUERY, {"as_of": as_of})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] ssp scorecard query failed (non-fatal): {exc}")
        return []


def _ssp_scorecard_block(rows: list[dict]) -> dict | None:
    """Per-SSP compliance scorecard from the audit matrix.

    Tells the operator at a glance which SSP is the source of the most
    revenue-at-risk. Sorted by revenue desc — biggest dollar exposure
    first. Verdict emoji follows the per-partner rollup convention.
    """
    if not rows:
        return None
    lines: list[str] = []
    for r in rows:
        name = (r.get("ssp_partner_name") or "?")[:24]
        rev = float(r.get("revenue") or 0)
        ents = int(r.get("entities") or 0)
        c = int(r.get("critical_n") or 0)
        w = int(r.get("warning_n") or 0)
        h = int(r.get("healthy_n") or 0)
        if c > 0:
            verdict = ":rotating_light:"
        elif w > 0:
            verdict = ":warning:"
        else:
            verdict = ":white_check_mark:"
        compliant_pct = (100.0 * h / ents) if ents else 0.0
        lines.append(
            f"{verdict} *{name}*  ·  ${rev:,.0f}/7d  ·  "
            f"{ents} entities  ·  {compliant_pct:.0f}% healthy  ·  "
            f"{c}c / {w}w / {h}h"
        )
    return {
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": ":bar_chart: *Per-SSP scorecard*\n" + "\n".join(lines)},
    }


def _registry_gap_block(findings: list[dict], summary: dict) -> dict | None:
    """Dedicated section for ssp_registry hygiene — separates 'fix the
    config' work from 'fix a publisher' work. The two are routinely
    confused; surfacing them apart makes triage cleaner.
    """
    unmapped = [
        f for f in findings
        if f["check_id"] == "compliance.demand_unmapped_to_ssp"
    ]
    unmapped.sort(key=lambda f: -_detail_rev(f))
    new_demand = [
        f for f in findings
        if f["check_id"] == "compliance.new_demand_observed"
    ]
    if not unmapped and not new_demand:
        return None

    lines: list[str] = []
    if unmapped:
        lines.append(f"*Unmapped demand → SSP* ({len(unmapped)})  ·  add to "
                     "`ssp_registry.PHASE_2_SSP_EXPECTATIONS`:")
        for f in unmapped[:6]:
            det = f.get("detail") or {}
            name = (det.get("demand_name") or "?")[:32]
            rev = _detail_rev(f)
            lines.append(f"  • `{name}`  ${rev:,.0f}/7d")
        if len(unmapped) > 6:
            lines.append(f"  …+{len(unmapped) - 6} more")
    if new_demand:
        lines.append("")
        lines.append(f"*New demand observed* ({len(new_demand)})  ·  "
                     "verify expected:")
        for f in new_demand[:4]:
            det = f.get("detail") or {}
            name = (det.get("demand_name") or "?")[:32]
            rev = _detail_rev(f)
            ssp = det.get("classified_ssp") or "unmapped"
            lines.append(f"  • `{name}`  ${rev:,.0f}/7d  → {ssp}")
        if len(new_demand) > 4:
            lines.append(f"  …+{len(new_demand) - 4} more")

    return {
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": ":wrench: *Registry gaps*\n" + "\n".join(lines)},
    }


def _build_blocks(findings: list[dict], summary: dict,
                  lowest_scores: list[dict],
                  partner_rollup: list[dict] | None = None,
                  ssp_scorecard: list[dict] | None = None) -> list[dict]:
    by_sev: dict[str, list[dict]] = defaultdict(list)
    for f in findings:
        by_sev[f["severity"]].append(f)

    crit = _sort_by_revenue(by_sev.get("critical", []))
    high = _sort_by_revenue(by_sev.get("high", []))
    med  = _sort_by_revenue(by_sev.get("medium", []))

    # Matrix-driven KPIs — the operationally meaningful denominator
    # is "what % of revenue is flowing through a fully compliant path",
    # not "what's the average compliance score across 150 sellers.json
    # entries". Falls back to legacy active-publisher math if the
    # matrix hasn't run yet.
    pct = summary.get("audit_matrix_compliant_pct")
    audited = summary.get("audit_matrix_revenue_audited")
    at_risk = summary.get("audit_matrix_revenue_at_risk")
    crit_n = summary.get("audit_matrix_critical")
    warn_n = summary.get("audit_matrix_warning")
    healthy_n = summary.get("audit_matrix_healthy")
    ssps_n = summary.get("audit_matrix_ssps")
    if pct is not None and audited is not None:
        header = (
            f":shield: *Supply compliance — {date.today().isoformat()}*  ·  "
            f"*{pct:.1f}%* of *${audited:,.0f}/7d* compliant  ·  "
            f"*${at_risk or 0:,.0f}/7d at risk*"
        )
        context_bits = [
            f"{summary.get('audit_matrix_rows', 0)} (entity × SSP) audited "
            f"across {summary.get('phase5_domains', 0)} domains + "
            f"{summary.get('phase5_apps', 0)} apps × {ssps_n} SSPs",
            f":rotating_light: {crit_n} critical  ·  "
            f":warning: {warn_n} warning  ·  "
            f":white_check_mark: {healthy_n} healthy",
        ]
    else:
        # Legacy header (matrix not yet computed).
        active_n = summary.get("active_publishers") or 0
        scanned_n = summary.get("publishers_scanned", 0)
        header = (
            f":shield: *Supply compliance — {date.today().isoformat()}*  "
            f"·  {active_n}/{scanned_n} active  "
            f"·  open: {len(crit)} crit / {len(high)} high / {len(med)} med"
        )
        context_bits = []
        if "avg_score_active" in summary and active_n:
            context_bits.append(
                f"avg score (active) {summary['avg_score_active']:.0f}  ·  "
                f"{summary.get('publishers_below_75_active', 0)} below 75"
            )
        if "roundtrip_rev_at_risk_7d" in summary:
            rev_risk = float(summary.get("roundtrip_rev_at_risk_7d") or 0)
            if rev_risk > 0:
                context_bits.append(f"${rev_risk:,.0f}/7d at risk (undeclared)")
        if "ll_bridge_matched" in summary:
            context_bits.append(
                f"LL bridge {summary['ll_bridge_matched']}↔"
                f"{summary['ll_bridge_matched'] + summary.get('ll_bridge_unmatched', 0)}"
            )
        if "observed_ssp_rows" in summary:
            context_bits.append(f"{summary['observed_ssp_rows']} ssp×pub active")

    blocks: list[dict] = [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
    ]
    if context_bits:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " · ".join(context_bits)}],
        })
    blocks.append({"type": "divider"})

    # ── Action queue — what to fix today, grouped by entity ────────────
    # This is the section we read first thing in the morning. Each card
    # shows the literal "Add this line" / "Replace with this line"
    # guidance so it's actionable without opening the validator code or
    # rejoining ssp_registry by hand. One Slack section per card so
    # we stay under the 3000-char-per-section ceiling.
    action_cards = _action_card_blocks(findings, max_entities=5)
    if action_cards:
        blocks.extend(action_cards)
        blocks.append({"type": "divider"})

    # Sentinel findings (demand, supply partner, schain) live OUTSIDE
    # the action queue — they need separate workflows (registry edits,
    # bridge updates) so we surface them in a compact "Other critical /
    # high" tail rather than mixed with the entity action cards.
    def _is_sentinel(f: dict) -> bool:
        return f["publisher_key"].startswith("_")

    crit_sentinel = _sort_by_revenue([f for f in crit if _is_sentinel(f)])
    high_sentinel = _sort_by_revenue([f for f in high if _is_sentinel(f)])

    if crit_sentinel:
        lines = [_format_line(f) for f in crit_sentinel[:6]]
        if len(crit_sentinel) > 6:
            lines.append(f"…+{len(crit_sentinel) - 6} more")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":rotating_light: *Critical — config/registry*\n"
                             + "\n".join(lines)},
        })

    if high_sentinel:
        lines = [_format_line(f) for f in high_sentinel[:6]]
        if len(high_sentinel) > 6:
            lines.append(f"…+{len(high_sentinel) - 6} more")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":warning: *High — config/registry*\n"
                             + "\n".join(lines)},
        })

    # Tail summary for entity findings NOT in the action queue.
    queued_pubs = set()
    if action_cards:
        # Re-derive which entities the action card covered.
        ent_groups: dict[str, list[dict]] = defaultdict(list)
        for f in findings:
            if f["publisher_key"].startswith(("dom:", "app:")) and f["severity"] in ("critical", "high"):
                ent_groups[f["publisher_key"]].append(f)
        queued_pubs = set(sorted(
            ent_groups.keys(),
            key=lambda k: -max((_detail_rev(f) for f in ent_groups[k]), default=0.0),
        )[:5])

    tail_crit = [f for f in crit if not _is_sentinel(f) and f["publisher_key"] not in queued_pubs]
    tail_high = [f for f in high if not _is_sentinel(f) and f["publisher_key"] not in queued_pubs]
    if tail_crit or tail_high:
        # Group by entity, show one line per entity with issue count + revenue.
        entity_summary: dict[str, dict] = defaultdict(
            lambda: {"crit": 0, "high": 0, "rev": 0.0}
        )
        for f in tail_crit:
            es = entity_summary[f["publisher_key"]]
            es["crit"] += 1
            es["rev"] = max(es["rev"], _detail_rev(f))
        for f in tail_high:
            es = entity_summary[f["publisher_key"]]
            es["high"] += 1
            es["rev"] = max(es["rev"], _detail_rev(f))
        ordered = sorted(entity_summary.items(), key=lambda kv: -kv[1]["rev"])
        cap = 10
        lines = []
        for pk, s in ordered[:cap]:
            pub = _format_pub_key(pk)
            rev_tag = f" ·${s['rev']:,.0f}/7d" if s["rev"] >= 1 else ""
            tags = []
            if s["crit"]:
                tags.append(f"{s['crit']} crit")
            if s["high"]:
                tags.append(f"{s['high']} high")
            lines.append(f"• {pub}  {' · '.join(tags)}{rev_tag}")
        if len(ordered) > cap:
            lines.append(f"…+{len(ordered) - cap} more entities")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":clipboard: *Other affected entities* "
                             "(same fix pattern — see dashboard for line-by-line)\n"
                             + "\n".join(lines)},
        })

    med_cap = max(MAX_LINES_PER_SECTION // 3, 3)
    if med:
        lines = [_format_line(f) for f in med[:med_cap]]
        if len(med) > med_cap:
            lines.append(f"…+{len(med) - med_cap} more medium")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":small_orange_diamond: *Medium*\n" + "\n".join(lines)},
        })

    # Per-SSP scorecard sits between the action queue and the partner
    # rollup: action queue tells you what to fix first, scorecard tells
    # you which SSP is bleeding the most revenue across all entities.
    scorecard_block = _ssp_scorecard_block(ssp_scorecard or [])
    if scorecard_block is not None:
        blocks.append(scorecard_block)

    if partner_rollup:
        partner_lines = [_format_partner_row(r) for r in partner_rollup]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":busts_in_silhouette: *Per supply partner*\n"
                             + "\n".join(partner_lines)},
        })

    gap_block = _registry_gap_block(findings, summary)
    if gap_block is not None:
        blocks.append(gap_block)

    if lowest_scores:
        score_lines = [_format_score_line(r) for r in lowest_scores]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": ":chart_with_downwards_trend: "
                             "*Lowest compliance scores* (active publishers)\n"
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
    partner_rollup = _load_partner_rollup()
    ssp_scorecard = _load_ssp_scorecard(date.today())
    blocks = _build_blocks(findings, summary, lowest_scores,
                           partner_rollup, ssp_scorecard)
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
