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


def _render_table(rows: list[list], headers: list[str],
                  aligns: list[str] | None = None) -> str:
    """Render rows as a monospace code-block table for Slack.

    Slack renders triple-backtick blocks in a fixed-width font so the
    columns align consistently across web + mobile. We pad each cell
    to the column's max width (header or longest cell), with optional
    per-column alignment ('l'=left default, 'r'=right for numerics).

    Cell values are truncated to MAX_CELL_W (28 chars) so a long
    entity name doesn't blow out the whole table on mobile.
    """
    MAX_CELL_W = 28
    aligns = aligns or ["l"] * len(headers)

    norm: list[list[str]] = []
    for r in rows:
        norm.append([
            (str(c) if c is not None else "")[:MAX_CELL_W]
            for c in r
        ])

    widths = [len(h) for h in headers]
    for r in norm:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))

    def _fmt(row: list[str]) -> str:
        cells = []
        for i, c in enumerate(row):
            w = widths[i]
            if aligns[i] == "r":
                cells.append(c.rjust(w))
            else:
                cells.append(c.ljust(w))
        return "  ".join(cells).rstrip()

    sep = "  ".join("─" * w for w in widths)
    out = [_fmt(headers), sep]
    for r in norm:
        out.append(_fmt(r))
    return "```\n" + "\n".join(out) + "\n```"


def _partner_table_row(r: dict) -> list:
    """Render one LL supply partner as a table row (status, name, …)."""
    name = (r.get("ll_publisher_name") or "?")[:22]
    entities = int(r.get("entities") or 0)
    crit = int(r.get("findings_crit") or 0)
    high = int(r.get("findings_high") or 0)
    if crit == 0 and high == 0:
        status = "✅"
    elif crit > 0:
        status = "🚨"
    else:
        status = "⚠️"
    return [status, name, str(entities), str(crit), str(high)]


def _format_pub_key(key: str) -> str:
    """Sentinel keys get readable labels; real publisher keys render as code."""
    if key.startswith("_ssp:"):
        return f"SSP `{key[5:]}`"
    if key.startswith("_supply:"):
        return f"supply partner `{key[len('_supply:'):]}`"
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
        ssp_domain = (expected_line or "").split(",")[0].strip() if expected_line else ssp
        # Make it explicit: we searched all entries for this SSP and
        # our seat wasn't among them. Avoids the "well other IDs exist
        # so isn't it fine?" misread.
        out.append(
            f"  Checked {len(obs)} `{ssp_domain}` line"
            f"{'s' if len(obs) != 1 else ''} on this ads.txt — "
            f"our seat `{exp}` not among them."
        )
        if expected_line:
            out.append(f"  Add line: `{expected_line}`")
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

    # Header reveals the FULL volume so the operator knows what's NOT
    # in the action queue. The CSV is the complete record.
    total_entities_with_issues = len(entity_findings)
    out: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (
                     f":dart: *Action queue — top {len(entities_ranked)} of "
                     f"{total_entities_with_issues} entities with issues "
                     f"(${total_at_risk:,.0f}/7d shown)*\n"
                     "_Every line below is copy-paste ready. "
                     "Full list (all entities × SSP paths) in today's CSV: "
                     "`data/compliance_matrix_<today>.csv` on Render._")},
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


_NEW_DEMAND_QUERY = """
-- Demands first observed recently AND whose name is more specific than
-- the bare SSP — i.e. an actual variant (e.g. "TripleLift - Blitz") not
-- the canonical demand name (e.g. "TripleLift" itself). Variant names
-- typically contain a hyphen/colon/parenthesis tagging the campaign or
-- product line. This filter is what isolates "new variants we should
-- look at" from "the existing SSP's main demand".
--
-- Until the table accumulates ≥2 days of history, EVERY demand's
-- first_seen_at falls inside the 48h window (the seed run wrote them
-- all at once). The variant-name filter is what saves the signal in
-- that case.
SELECT
    demand_name,
    ssp_key,
    revenue_7d_latest,
    first_seen_at,
    seen_count
FROM pgam_direct.compliance_observed_demands
WHERE first_seen_at >= now() - interval '48 hours'
  AND revenue_7d_latest >= %(min_rev)s
  AND ssp_key IS NOT NULL
  AND (
      -- Hyphen-, colon-, parenthesis-, or slash-separated variant
      -- name. Catches "TripleLift - Blitz", "Sharethrough: CTV",
      -- "PubMatic (Test)", "Magnite/SpotX", etc.
      demand_name ~ '[ ]*[-:/(][ ]+'
      -- OR: never seen before this run AND not an exact-SSP-name match.
      OR seen_count <= 1
  )
ORDER BY revenue_7d_latest DESC
LIMIT 10;
"""

_DEMAND_ENTITY_SAMPLE_QUERY = """
-- Per-entity audit for one SSP — pulled when surfacing a new demand
-- variant so the digest shows WHERE the new revenue is landing and
-- whether those entities' ads.txt files are correctly authorized.
-- Skip numeric pseudo-entities (publisher_id leaked into entity_value)
-- and entities without a resolvable audit_host (can't link the file).
SELECT
    entity_value, kind, audit_host, revenue_7d,
    pgam_direct_present, ssp_line_present, sellers_json_match,
    status
FROM pgam_direct.compliance_entity_ssp_audit
WHERE as_of = %(as_of)s AND ssp_key = %(ssp_key)s
  AND audit_host IS NOT NULL
  AND entity_value !~ '^[0-9]+$'
ORDER BY revenue_7d DESC
LIMIT 6;
"""

NEW_DEMAND_MIN_REV_7D = 100.0


def _load_new_demands(min_rev: float = NEW_DEMAND_MIN_REV_7D) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_NEW_DEMAND_QUERY, {"min_rev": min_rev})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] new-demand query failed (non-fatal): {exc}")
        return []


def _load_demand_entity_sample(as_of: date, ssp_key: str) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_DEMAND_ENTITY_SAMPLE_QUERY,
                            {"as_of": as_of, "ssp_key": ssp_key})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return []


def _new_demand_variants_blocks(new_demands: list[dict],
                                as_of: date) -> list[dict]:
    """Flag demands first observed in last 48h with material revenue.

    Returns a LIST of blocks (header + N variant cards) so each card
    stays under Slack's 3000-char-per-section limit. With 6 entities
    per card × ~80 chars/line × extra labels, a single section can
    blow past 3000 chars when there are many active variants.
    """
    if not new_demands:
        return []

    out: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (f":new: *New demand variants "
                          f"({len(new_demands)}, last 48h)*\n"
                          "_Demands first observed recently with ≥ "
                          f"${NEW_DEMAND_MIN_REV_7D:.0f}/7d. They auto-fold "
                          "into their SSP's row, but the variant is new — "
                          "verify it's expected and that the entities below "
                          "have authorized ads.txt lines._")},
    }]
    for d in new_demands:
        name = d.get("demand_name") or "?"
        ssp = d.get("ssp_key") or "?"
        rev = float(d.get("revenue_7d_latest") or 0)
        first = d.get("first_seen_at")
        hours_ago = ""
        if first is not None:
            from datetime import datetime, timezone
            delta = datetime.now(timezone.utc) - first
            hrs = int(delta.total_seconds() / 3600)
            hours_ago = f" ·  first seen {hrs}h ago"

        header_line = (
            f"*`{name}`*  →  *{ssp}*  ·  ${rev:,.0f}/7d{hours_ago}"
        )
        sample = _load_demand_entity_sample(as_of, ssp)
        if not sample:
            body = (header_line +
                    "\n_(no per-entity audit available yet — check tomorrow)_")
        else:
            # Per-entity drill-down as a monospace table.
            # ✓/✗ glyphs align cleanly in Slack's fixed-width font.
            # ads.txt source links live below the table because hyperlinks
            # don't render inside code blocks.
            table_rows = []
            link_lines = []
            for s in sample:
                pgam = "✓" if s["pgam_direct_present"] else "✗"
                ssp_y = "✓" if s["ssp_line_present"] else "✗"
                json_y = "✓" if s["sellers_json_match"] else "✗"
                ent = (s.get("entity_value") or "")[:26]
                erev = float(s.get("revenue_7d") or 0)
                table_rows.append([ent, f"${erev:,.0f}", pgam, ssp_y, json_y])
                host = s.get("audit_host")
                variant = "app-ads.txt" if s.get("kind") == "app" else "ads.txt"
                if host:
                    link_lines.append(
                        f"• `{ent}` → <https://{host}/{variant}|{variant}>"
                    )
            table = _render_table(
                rows=table_rows,
                headers=["Entity", "Rev/7d", "PGAM", "SSP", "json"],
                aligns=["l", "r", "l", "l", "l"],
            )
            body = f"{header_line}\n{table}\n" + "\n".join(link_lines)
        if len(body) > 2900:
            body = body[:2880] + "\n_…see CSV for full list._"
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
    table_rows = []
    for r in rows:
        name = (r.get("ssp_partner_name") or "?")[:18]
        rev = float(r.get("revenue") or 0)
        ents = int(r.get("entities") or 0)
        c = int(r.get("critical_n") or 0)
        w = int(r.get("warning_n") or 0)
        h = int(r.get("healthy_n") or 0)
        status = "🚨" if c > 0 else ("⚠️" if w > 0 else "✅")
        pct = (100.0 * h / ents) if ents else 0.0
        table_rows.append([status, name, f"${rev:,.0f}",
                           str(ents), f"{pct:.0f}%", str(c), str(w), str(h)])
    table = _render_table(
        rows=table_rows,
        headers=["", "SSP", "Rev/7d", "Ents", "Heal%", "C", "W", "H"],
        aligns=["l", "l", "r", "r", "r", "r", "r", "r"],
    )
    return {
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (f":bar_chart: *Demand SSP audit "
                          f"({len(rows)} SSPs — who's buying + their authorization)*\n"
                          "_For each demand SSP, total $ they paid us and "
                          "the per-entity audit: is the SSP's reseller line "
                          "on the publisher's ads.txt, is PGAM declared as "
                          "DIRECT, is the seat in our sellers.json. Schain "
                          "compliance shown in the per-pair table below. "
                          "C/W/H = critical/warning/healthy paths._\n"
                          + table)},
    }


def _block_list_block(rows: list[dict], summary: dict) -> dict | None:
    """Daily 'paths queued for blocking' section.

    Surfaces compliance_path_block_list rows in 'pending_review' state.
    Each row is a (entity × supply_partner) path that the auditor
    flagged as non-compliant + material. No enforcement happens yet
    (Stage 1 of the block-list build); ops review + approve a row to
    flip it to 'active', at which point the PGAM bidder edge will
    actually block the path (Stage 3, in pgam-direct/web).
    """
    if not rows:
        return None
    table_rows = []
    total_at_risk = 0.0
    for r in rows:
        ent = (r.get("entity_value") or "")[:24]
        partner = (r.get("supply_partner_key") or "?")[:14]
        rev = float(r.get("revenue_7d") or 0)
        total_at_risk += rev
        flagged = int(r.get("flagged_count") or 1)
        reason_map = {
            "both_missing":         "no partner + no PGAM line",
            "partner_line_missing": "partner line absent",
            "pgam_line_missing":    "PGAM RESELLER line absent",
            "unknown_path":         "unknown",
        }
        reason = reason_map.get(r.get("reason") or "", r.get("reason") or "?")
        table_rows.append([
            ent, partner, f"${rev:,.0f}", reason[:24], f"{flagged}d",
        ])
    table = _render_table(
        rows=table_rows,
        headers=["Entity", "Via partner", "Rev/7d", "Why", "Flagged"],
        aligns=["l", "l", "r", "l", "r"],
    )
    pending = summary.get("block_list_pending") or len(rows)
    active = summary.get("block_list_active") or 0
    return {
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (
                     f":no_entry: *Paths queued for blocking "
                     f"({pending} pending · {active} active)*\n"
                     f"_Non-compliant (entity × supply partner) paths above $50/7d. "
                     f"${total_at_risk:,.0f}/7d in this queue. Stage 1 = surfaced "
                     f"for review; ops approves to flip to 'active'; Stage 3 "
                     f"(pgam-direct/web bidder edge) reads active rows and "
                     f"returns no-bid on matching requests. Auto-releases when "
                     f"the audit confirms the path is now healthy._\n" + table)},
    }


_REV_AT_RISK_BY_SSP_QUERY = """
-- Revenue-at-risk by demand SSP — for every SSP across all audited
-- entities, sum revenue attributed to compliant vs non-compliant
-- rows. Drives the 'where's the biggest dollar exposure' question.
SELECT
    ssp_partner_name,
    SUM(revenue_7d)                                     AS revenue,
    SUM(revenue_7d) FILTER (WHERE status = 'healthy')   AS compliant,
    SUM(revenue_7d) FILTER (WHERE status != 'healthy')  AS at_risk,
    COUNT(*) FILTER (WHERE status = 'healthy')          AS healthy_n,
    COUNT(*)                                             AS total_n
FROM pgam_direct.compliance_entity_ssp_audit
WHERE as_of = %(as_of)s
GROUP BY ssp_partner_name
HAVING SUM(revenue_7d) > 0
ORDER BY at_risk DESC NULLS LAST, revenue DESC
LIMIT 10;
"""

_REV_AT_RISK_BY_SUPPLY_PARTNER_QUERY = """
-- Revenue-at-risk by LL supply partner (the one bringing the
-- inventory). Mirror of the SSP view from the supply-side.
SELECT
    supply_partner_key,
    SUM(revenue_7d)                                     AS revenue,
    SUM(revenue_7d) FILTER (WHERE status = 'healthy')   AS compliant,
    SUM(revenue_7d) FILTER (WHERE status != 'healthy')  AS at_risk,
    COUNT(*) FILTER (WHERE status = 'healthy')          AS healthy_n,
    COUNT(*)                                             AS total_n
FROM pgam_direct.compliance_entity_supply_path_audit
WHERE as_of = %(as_of)s
  AND supply_partner_key IS NOT NULL
GROUP BY supply_partner_key
HAVING SUM(revenue_7d) > 0
ORDER BY at_risk DESC NULLS LAST, revenue DESC
LIMIT 10;
"""


def _load_rev_at_risk_by_ssp(as_of: date) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_REV_AT_RISK_BY_SSP_QUERY, {"as_of": as_of})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] rev-at-risk by SSP query failed: {exc}")
        return []


def _load_rev_at_risk_by_supply_partner(as_of: date) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_REV_AT_RISK_BY_SUPPLY_PARTNER_QUERY, {"as_of": as_of})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] rev-at-risk by partner query failed: {exc}")
        return []


def _rev_at_risk_blocks(rev_by_ssp: list[dict],
                       rev_by_partner: list[dict]) -> list[dict]:
    """Two summary tables answering 'where is the most $ exposed':
    once by demand SSP, once by LL supply partner. Both ordered by
    $at-risk so the biggest financial gap is on top.
    """
    blocks: list[dict] = []
    if rev_by_ssp:
        rows = []
        for r in rev_by_ssp:
            name = (r.get("ssp_partner_name") or "?")[:14]
            rev = float(r.get("revenue") or 0)
            risk = float(r.get("at_risk") or 0)
            pct_healthy = 100.0 * (float(r.get("compliant") or 0)) / rev if rev else 0
            rows.append([
                name, f"${rev:,.0f}", f"${risk:,.0f}", f"{pct_healthy:.0f}%"
            ])
        table = _render_table(
            rows=rows,
            headers=["Demand SSP", "Rev/7d", "$ at risk", "% healthy"],
            aligns=["l", "r", "r", "r"],
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (":moneybag: *Revenue at risk — by demand SSP*\n"
                              "_Where the biggest $ exposure sits, demand-side. "
                              "$ at risk = revenue on rows with status != healthy._\n"
                              + table)},
        })
    if rev_by_partner:
        rows = []
        for r in rev_by_partner:
            name = (r.get("supply_partner_key") or "?")[:18]
            rev = float(r.get("revenue") or 0)
            risk = float(r.get("at_risk") or 0)
            pct_healthy = 100.0 * (float(r.get("compliant") or 0)) / rev if rev else 0
            rows.append([
                name, f"${rev:,.0f}", f"${risk:,.0f}", f"{pct_healthy:.0f}%"
            ])
        table = _render_table(
            rows=rows,
            headers=["Supply Partner", "Rev/7d", "$ at risk", "% healthy"],
            aligns=["l", "r", "r", "r"],
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (":moneybag: *Revenue at risk — by LL supply partner*\n"
                              "_Same view, supply-side. Tells you which partner "
                              "relationship needs ads.txt + sellers.json attention._\n"
                              + table)},
        })
    return blocks


_DEMAND_PARTNER_TOP_QUERY = """
-- Top demand SSPs by trailing-7d revenue across all audited entities.
-- Anchors the per-demand-partner cards in the digest.
SELECT
    ssp_key,
    ssp_partner_name,
    SUM(revenue_7d)                                AS revenue,
    COUNT(*)                                       AS rows,
    COUNT(*) FILTER (WHERE status = 'critical')    AS critical_n,
    COUNT(*) FILTER (WHERE status = 'warning')     AS warning_n,
    COUNT(*) FILTER (WHERE status = 'healthy')     AS healthy_n
FROM pgam_direct.compliance_entity_ssp_audit
WHERE as_of = %(as_of)s
GROUP BY ssp_key, ssp_partner_name
HAVING SUM(revenue_7d) > 0
ORDER BY revenue DESC
LIMIT %(limit)s;
"""

_DEMAND_PARTNER_ENTITIES_QUERY = """
-- Entities buying through one demand SSP, joined to the supply-path
-- audit so we have the entity's LL supply partner alongside each row.
-- Ordered by revenue so the card surfaces the dollar-impactful gaps.
SELECT
    m.entity_value,
    m.kind,
    m.audit_host,
    m.revenue_7d,
    m.pgam_direct_present,
    m.ssp_line_present,
    m.sellers_json_match,
    m.status,
    sp.ll_publisher_id,
    sp.ll_publisher_name,
    sp.supply_partner_key,
    sp.supply_partner_pgam_seat
FROM pgam_direct.compliance_entity_ssp_audit m
LEFT JOIN pgam_direct.compliance_entity_supply_path_audit sp
       ON sp.entity_key = m.entity_key AND sp.as_of = m.as_of
WHERE m.as_of = %(as_of)s AND m.ssp_key = %(ssp_key)s
ORDER BY m.revenue_7d DESC
LIMIT %(limit)s;
"""

_SCHAIN_BAD_LL_PUBS_QUERY = """
-- LL supply partners with an open schain.* finding. Used to mark
-- schain ✗ per entity in the per-demand-partner cards.
SELECT publisher_key
FROM pgam_direct.compliance_findings
WHERE status = 'open' AND check_id LIKE 'schain.%%'
  AND publisher_key LIKE '_ll_pub:%%';
"""


def _load_demand_partner_top(as_of: date, limit: int = 6) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_DEMAND_PARTNER_TOP_QUERY,
                            {"as_of": as_of, "limit": limit})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] demand top query failed: {exc}")
        return []


def _load_demand_partner_entities(as_of: date, ssp_key: str,
                                  limit: int = 6) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_DEMAND_PARTNER_ENTITIES_QUERY,
                            {"as_of": as_of, "ssp_key": ssp_key, "limit": limit})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception:
        return []


def _load_schain_bad_ll_pubs() -> set[str]:
    """Return the set of `_ll_pub:<id>` sentinels with open schain
    findings. Used per-entity to render schain ✗ in the cards.
    """
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHAIN_BAD_LL_PUBS_QUERY)
                return {r[0] for r in cur.fetchall()}
    except Exception:
        return set()


def _demand_partner_card_blocks(as_of: date,
                                max_partners: int = 6,
                                max_entities_per_card: int = 6) -> list[dict]:
    """One Slack section per demand partner, showing entities + flags.

    Renders the user's requested view: a clear table for each demand
    partner showing where they're running and whether the supply
    chain is compliant on every leg (PGAM line / demand SSP line /
    sellers.json / schain).
    """
    tops = _load_demand_partner_top(as_of, max_partners)
    if not tops:
        return []

    schain_bad_ll_pubs = _load_schain_bad_ll_pubs()
    out: list[dict] = []

    # Header section.
    out.append({
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (f":receipt: *Demand partner audit — top "
                          f"{len(tops)} by revenue*\n"
                          "_For each demand partner: which entities they're "
                          "buying through us + the four compliance flags. "
                          "PGAM ✓ = publisher's ads.txt declares us; SSP ✓ = "
                          "demand partner's reseller line on publisher ads.txt; "
                          "json ✓ = demand partner declares our seat; "
                          "schain ✓ = no open schain.* finding for the "
                          "publisher's supply partner._")},
    })

    for d in tops:
        ssp_key = d.get("ssp_key") or ""
        name = d.get("ssp_partner_name") or ssp_key
        rev = float(d.get("revenue") or 0)
        c = int(d.get("critical_n") or 0)
        w = int(d.get("warning_n") or 0)
        h = int(d.get("healthy_n") or 0)
        total = c + w + h
        pct = (100.0 * h / total) if total else 0.0
        head_status = ":rotating_light:" if c else (":warning:" if w else ":white_check_mark:")
        header_line = (
            f"{head_status} *{name}*  ·  ${rev:,.0f}/7d  ·  "
            f"{total} entities  ·  {pct:.0f}% healthy "
            f"({c}c / {w}w / {h}h)"
        )

        entities = _load_demand_partner_entities(
            as_of, ssp_key, limit=max_entities_per_card,
        )
        if not entities:
            out.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": header_line},
            })
            continue

        # Build the per-entity table for this demand partner.
        table_rows = []
        for e in entities:
            ent = (e.get("entity_value") or "")[:24]
            erev = float(e.get("revenue_7d") or 0)
            pgam = "✓" if e.get("pgam_direct_present") else "✗"
            ssp_y = "✓" if e.get("ssp_line_present") else "✗"
            jsn = "✓" if e.get("sellers_json_match") else "✗"
            # Schain per entity — flag ✗ when the entity's LL supply
            # partner has any open `schain.*` finding (keyed
            # `_ll_pub:<id>`). On Render the Phase 4 schain audit
            # populates these; locally we usually get ✓ across the
            # board because the static audit needs the revenue
            # snapshot to fire.
            ll_pub_id = e.get("ll_publisher_id")
            schain_ok = True
            if ll_pub_id:
                schain_ok = f"_ll_pub:{ll_pub_id}" not in schain_bad_ll_pubs
            sch = "✓" if schain_ok else "✗"
            status = e.get("status") or "?"
            sym = {"critical": "🚨", "warning": "⚠️", "healthy": "✅"}.get(status, "·")
            partner = (e.get("supply_partner_key") or e.get("ll_publisher_name") or "?")[:14]
            table_rows.append([
                sym, ent, f"${erev:,.0f}", partner, pgam, ssp_y, jsn, sch,
            ])
        table = _render_table(
            rows=table_rows,
            headers=["", "Entity", "Rev/7d", "Via partner",
                     "PGAM", "SSP", "json", "schain"],
            aligns=["l", "l", "r", "l", "l", "l", "l", "l"],
        )
        more = ""
        if total > max_entities_per_card:
            more = f"\n_…+{total - max_entities_per_card} more entities (see CSV)_"

        body = f"{header_line}\n{table}{more}"
        if len(body) > 2900:
            body = body[:2880] + "\n_…see CSV for full list._"
        out.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": body},
        })
    return out


_SUPPLY_PATH_QUERY = """
-- Top supply-path audit rows for the digest. One row per entity (the
-- audit is per-entity, not per-SSP).
SELECT
    entity_value, kind, audit_host, revenue_7d,
    path_kind, ll_publisher_name,
    supply_partner_key, supply_partner_domain, supply_partner_pgam_seat,
    supply_partner_line_present, pgam_line_present_for_path,
    sellers_json_partner_declared, status, expected_pgam_line
FROM pgam_direct.compliance_entity_supply_path_audit
WHERE as_of = %(as_of)s
ORDER BY revenue_7d DESC
LIMIT %(limit)s;
"""


def _load_supply_path(as_of: date, limit: int = 12) -> list[dict]:
    try:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_SUPPLY_PATH_QUERY, {"as_of": as_of, "limit": limit})
                cols = [c.name for c in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[compliance.slack_digest] supply-path query failed (non-fatal): {exc}")
        return []


def _supply_path_block(rows: list[dict], summary: dict) -> dict | None:
    """Per-entity supply-path audit — the right compliance question.

    For each top-revenue entity: which LL supply partner brings it (or
    'PGAM direct'), is its domain on the entity ads.txt, is PGAM
    correctly declared (DIRECT for pgam-direct path, RESELLER with
    the partner's specific PGAM seat for via-partner path).
    """
    if not rows:
        return None
    table_rows = []
    for r in rows:
        ent = (r.get("entity_value") or "")[:24]
        rev = float(r.get("revenue_7d") or 0)
        path = r.get("path_kind") or "unknown"
        partner = (r.get("supply_partner_key") or "?")[:16]
        if path == "pgam_direct":
            partner = "PGAM direct"
        elif path == "unknown":
            partner = (r.get("ll_publisher_name") or "?")[:16] + " (unbridged)"
        sp_ok = "✓" if r.get("supply_partner_line_present") else (
            "—" if path == "pgam_direct" else "✗")
        pg_ok = "✓" if r.get("pgam_line_present_for_path") else "✗"
        sj_ok = "✓" if r.get("sellers_json_partner_declared") else "✗"
        status = r.get("status") or "?"
        sym = {"critical": "🚨", "warning": "⚠️", "healthy": "✅"}.get(status, "·")
        table_rows.append([
            sym, ent, f"${rev:,.0f}", partner, sp_ok, pg_ok, sj_ok,
        ])
    table = _render_table(
        rows=table_rows,
        headers=["", "Entity", "Rev/7d", "Via supply partner",
                 "Partner✓", "PGAM✓", "json✓"],
        aligns=["l", "l", "r", "l", "l", "l", "l"],
    )

    pct = summary.get("supply_path_compliance_pct")
    audited = summary.get("supply_path_revenue_audited") or 0
    at_risk = summary.get("supply_path_revenue_at_risk") or 0
    via_partner = summary.get("supply_path_via_partner") or 0
    pgam_direct = summary.get("supply_path_pgam_direct") or 0
    unknown = summary.get("supply_path_unknown") or 0

    summary_line = ""
    if pct is not None:
        summary_line = (
            f"_{pct:.0f}% of ${audited:,.0f}/7d compliant. "
            f"${at_risk:,.0f}/7d at risk. "
            f"{pgam_direct} pgam-direct · {via_partner} via supply partner · "
            f"{unknown} unbridged._\n"
        )

    body = (
        f":electric_plug: *Supply-path audit "
        f"(top {len(rows)} entities — the right compliance question)*\n"
        + summary_line +
        "_For each entity: partner = LL supply source bringing inventory; "
        "Partner✓ = partner's domain on publisher ads.txt; "
        "PGAM✓ = correct pgamssp.com line for the path (DIRECT if pgam-direct, "
        "RESELLER with partner's PGAM seat if via-partner)._\n"
        + table
    )
    return {"type": "section", "text": {"type": "mrkdwn", "text": body}}


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
                 "text": (":wrench: *Registry gaps*\n"
                          "_Demand partners earning revenue that aren't mapped "
                          "to any SSP in our `ssp_registry.py`. Each one "
                          "needs a name-pattern added so we can audit its "
                          "reseller line going forward._\n"
                          + "\n".join(lines))},
    }


def _build_blocks(findings: list[dict], summary: dict,
                  lowest_scores: list[dict],
                  partner_rollup: list[dict] | None = None,
                  ssp_scorecard: list[dict] | None = None,
                  new_demands: list[dict] | None = None,
                  supply_path_rows: list[dict] | None = None) -> list[dict]:
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
        ent_total = summary.get("audit_entities_total") or 0
        ent_clean = summary.get("audit_entities_fully_clean") or 0
        ent_issue = summary.get("audit_entities_with_issues") or 0
        header = (
            f":shield: *Supply compliance — {date.today().isoformat()}*  ·  "
            f"*{pct:.1f}%* of *${audited:,.0f}/7d* compliant  ·  "
            f"*${at_risk or 0:,.0f}/7d at risk*"
        )
        context_bits = [
            f"{ent_issue} of {ent_total} entities need attention  ·  "
            f"{ent_clean} fully clean",
            f"{summary.get('audit_matrix_rows', 0)} (entity × SSP) paths "
            f"audited across {summary.get('phase5_domains', 0)} domains + "
            f"{summary.get('phase5_apps', 0)} apps × {ssps_n} SSPs",
            f":rotating_light: {crit_n} critical paths  ·  "
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

    # Revenue-at-risk summary tables — answer "where is the biggest $
    # exposure, by demand SSP and by LL supply partner". Operator
    # reads this first to know which relationship to escalate.
    rev_at_risk_blocks = _rev_at_risk_blocks(
        _load_rev_at_risk_by_ssp(date.today()),
        _load_rev_at_risk_by_supply_partner(date.today()),
    )
    if rev_at_risk_blocks:
        blocks.extend(rev_at_risk_blocks)
        blocks.append({"type": "divider"})

    # Block-list queue — non-compliant paths queued for enforcement.
    # Imported lazily because the block_list module reads its own
    # rows from Neon at digest build time.
    try:
        from agents.compliance.block_list import load_pending_queue
        bl_rows = load_pending_queue(limit=15)
        bl_block = _block_list_block(bl_rows, summary)
        if bl_block is not None:
            blocks.append(bl_block)
            blocks.append({"type": "divider"})
    except Exception as _exc:
        print(f"[compliance.slack_digest] block_list section failed "
              f"(non-fatal): {_exc}")

    # Supply-path audit — the right compliance question — sits directly
    # after the action queue so it's visible above the demand-side
    # visibility sections below.
    sp_block = _supply_path_block(supply_path_rows or [], summary)
    if sp_block is not None:
        blocks.append(sp_block)
        blocks.append({"type": "divider"})

    # New demand variants — surfaces the case where a new SSP variant
    # (TripleLift - Blitz, Sharethrough - Blitz, etc.) spins up
    # overnight. Auto-folds into its SSP's row in the matrix but the
    # operational signal is "this is new and earning meaningful $".
    new_demand_blocks = _new_demand_variants_blocks(new_demands or [], date.today())
    if new_demand_blocks:
        blocks.extend(new_demand_blocks)
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
                     "text": (f":rotating_light: *Critical — config/registry "
                              f"({len(crit_sentinel)})*\n"
                              "_Problems in OUR configs or in OTHER SSPs' "
                              "sellers.json — not in publisher ads.txt files._\n"
                              + "\n".join(lines))},
        })

    if high_sentinel:
        lines = [_format_line(f) for f in high_sentinel[:6]]
        if len(high_sentinel) > 6:
            lines.append(f"…+{len(high_sentinel) - 6} more")
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f":warning: *High — config/registry "
                              f"({len(high_sentinel)})*\n"
                              "_Demands earning revenue but unmapped in our "
                              "`ssp_registry.py`, or LL supply partners not "
                              "yet bridged to a sellers.json entry._\n"
                              + "\n".join(lines))},
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
        table_rows = []
        for pk, s in ordered[:cap]:
            # Drop the dom:/app: prefix in the table — keeps the
            # entity column narrow on mobile.
            ent = pk.split(":", 1)[1] if pk.startswith(("dom:", "app:")) else pk
            table_rows.append([ent[:30], f"${s['rev']:,.0f}",
                               str(s["crit"]), str(s["high"])])
        more = (f"\n_…+{len(ordered) - cap} more entities (see CSV)_"
                if len(ordered) > cap else "")
        table = _render_table(
            rows=table_rows,
            headers=["Entity", "Rev/7d", "Crit", "High"],
            aligns=["l", "r", "r", "r"],
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f":clipboard: *Other affected entities "
                              f"({min(cap, len(ordered))} of {len(ordered)})*\n"
                              "_Same fix pattern as the action queue above. "
                              "Full per-SSP breakdown in today's CSV._\n"
                              + table + more)},
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
    # Per-demand-partner cards — the user's requested "everything by
    # demand partner" view. Sits between supply-path audit (per-entity)
    # and the demand SSP rollup scorecard.
    partner_card_blocks = _demand_partner_card_blocks(
        date.today(), max_partners=6, max_entities_per_card=6,
    )
    if partner_card_blocks:
        blocks.extend(partner_card_blocks)
        blocks.append({"type": "divider"})

    scorecard_block = _ssp_scorecard_block(ssp_scorecard or [])
    if scorecard_block is not None:
        blocks.append(scorecard_block)

    if partner_rollup:
        partner_table = _render_table(
            rows=[_partner_table_row(r) for r in partner_rollup],
            headers=["", "Supply Partner", "Ents", "Crit", "High"],
            aligns=["l", "l", "r", "r", "r"],
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f":busts_in_silhouette: *Per supply partner "
                              f"({len(partner_rollup)})*\n"
                              "_LL supply partners (Smaato, BidMachine, "
                              "Start.IO, …) and how many of their entities "
                              "have critical/high findings._\n"
                              + partner_table)},
        })

    gap_block = _registry_gap_block(findings, summary)
    if gap_block is not None:
        blocks.append(gap_block)

    if lowest_scores:
        score_lines = [_format_score_line(r) for r in lowest_scores]
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f":chart_with_downwards_trend: "
                              f"*Lowest compliance scores "
                              f"({len(lowest_scores)} of active publishers)*\n"
                              "_Score = 100 minus severity-weighted findings. "
                              "Filtered to publishers earning revenue./_\n"
                              + "\n".join(score_lines))},
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
    new_demands = _load_new_demands()
    supply_path_rows = _load_supply_path(date.today())
    blocks = _build_blocks(findings, summary, lowest_scores,
                           partner_rollup, ssp_scorecard, new_demands,
                           supply_path_rows)
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
