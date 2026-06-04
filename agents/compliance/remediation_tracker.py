"""
agents/compliance/remediation_tracker.py

"Did the partner actually fix it?" detector.

Reads the daily snapshots already written by the compliance runner
(compliance_entity_supply_path_audit + compliance_entity_ssp_audit are
keyed on (..., as_of)) and diffs today vs. a lookback date — by default
7 days back, falling back to the oldest snapshot we have if history is
shallow. Emits Slack blocks listing:

  • Confirmed fixes — a presence flag flipped FALSE → TRUE
      e.g. publisher finally added `pgamssp.com, <seat>, RESELLER` on
      Smaato to their ads.txt; an SSP added their canonical line; an
      upstream SSP's sellers.json finally declares PGAM.

  • Regressions — a flag flipped TRUE → FALSE (a previously-clean line
      disappeared). These are usually more important than fixes.

Flagging only. Never modifies anything.

Use:
    from agents.compliance.remediation_tracker import build_remediation_blocks
    blocks = build_remediation_blocks(today, lookback_days=7)
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from core.neon import connect


# Maximum rows surfaced per section before we truncate. Slack section
# blocks cap around ~3000 chars, so 12-ish lines per category keeps us
# safely under that even with the longest entity names.
MAX_ROWS_PER_SECTION = 12


_SUPPLY_PATH_DIFF_SQL = """
-- Three-point check: today (T), yesterday (Y), prior lookback (P).
-- We require BOTH Y and P to agree on the "before" value before
-- counting a flip — that filters out single-day crawler glitches
-- where the audit transiently scored a line as missing on one day
-- only (a real failure mode observed in early operation).
--
-- Also JOIN on audit_host across all three snapshots. If the crawler
-- learned a new audit_host for an entity between snapshots, the
-- presence values aren't directly comparable — "fixing" a line on a
-- different host isn't the same line. Requiring host equality keeps
-- every reported flip attributable to a real publisher action.
WITH today AS (
    SELECT entity_key, entity_value, audit_host, revenue_7d,
           supply_partner_key, supply_partner_pgam_seat,
           supply_partner_line_present  AS a_now,
           pgam_line_present_for_path   AS b_now,
           sellers_json_partner_declared AS d_now
    FROM pgam_direct.compliance_entity_supply_path_audit
    WHERE as_of = %(today)s
),
yday AS (
    SELECT entity_key, audit_host,
           supply_partner_key            AS partner,
           supply_partner_line_present   AS a_y,
           pgam_line_present_for_path    AS b_y,
           sellers_json_partner_declared AS d_y
    FROM pgam_direct.compliance_entity_supply_path_audit
    WHERE as_of = %(yday)s
),
prior AS (
    SELECT entity_key, audit_host,
           supply_partner_key            AS partner,
           supply_partner_line_present   AS a_p,
           pgam_line_present_for_path    AS b_p,
           sellers_json_partner_declared AS d_p
    FROM pgam_direct.compliance_entity_supply_path_audit
    WHERE as_of = %(prior)s
)
SELECT t.entity_key, t.entity_value, t.revenue_7d,
       t.supply_partner_key, t.supply_partner_pgam_seat,
       t.a_now, y.a_y, p.a_p,
       t.b_now, y.b_y, p.b_p,
       t.d_now, y.d_y, p.d_p
FROM today t
JOIN yday  y ON y.entity_key  = t.entity_key
             AND y.audit_host = t.audit_host
             AND y.partner    = t.supply_partner_key
JOIN prior p ON p.entity_key  = t.entity_key
             AND p.audit_host = t.audit_host
             AND p.partner    = t.supply_partner_key
WHERE
    -- Stable fix: was FALSE at prior, present at BOTH yesterday and
    -- today (the fix has held for at least 1 full day — filters a
    -- 1-day TRUE glitch where the crawler briefly saw the line).
       (t.a_now IS TRUE  AND y.a_y IS TRUE  AND p.a_p IS FALSE)
    OR (t.b_now IS TRUE  AND y.b_y IS TRUE  AND p.b_p IS FALSE)
    OR (t.d_now IS TRUE  AND y.d_y IS TRUE  AND p.d_p IS FALSE)
    -- Stable regression: was TRUE at prior, missing at BOTH yesterday
    -- and today (filters a 1-day FALSE glitch).
    OR (t.a_now IS FALSE AND y.a_y IS FALSE AND p.a_p IS TRUE)
    OR (t.b_now IS FALSE AND y.b_y IS FALSE AND p.b_p IS TRUE)
    OR (t.d_now IS FALSE AND y.d_y IS FALSE AND p.d_p IS TRUE)
ORDER BY t.revenue_7d DESC;
"""


_SSP_DIFF_SQL = """
-- Same 3-point (today / yesterday / prior) check as the supply-path
-- diff above — see that comment block for rationale.
WITH today AS (
    SELECT entity_key, entity_value, audit_host, revenue_7d,
           ssp_key, ssp_partner_name,
           ssp_line_present  AS line_now,
           pgam_direct_present AS pgam_now
    FROM pgam_direct.compliance_entity_ssp_audit
    WHERE as_of = %(today)s
),
yday AS (
    SELECT entity_key, audit_host, ssp_key,
           ssp_line_present  AS line_y,
           pgam_direct_present AS pgam_y
    FROM pgam_direct.compliance_entity_ssp_audit
    WHERE as_of = %(yday)s
),
prior AS (
    SELECT entity_key, audit_host, ssp_key,
           ssp_line_present  AS line_p,
           pgam_direct_present AS pgam_p
    FROM pgam_direct.compliance_entity_ssp_audit
    WHERE as_of = %(prior)s
)
SELECT t.entity_key, t.entity_value, t.revenue_7d,
       t.ssp_key, t.ssp_partner_name,
       t.line_now, y.line_y, p.line_p,
       t.pgam_now, y.pgam_y, p.pgam_p
FROM today t
JOIN yday  y ON y.entity_key  = t.entity_key
             AND y.audit_host = t.audit_host
             AND y.ssp_key    = t.ssp_key
JOIN prior p ON p.entity_key  = t.entity_key
             AND p.audit_host = t.audit_host
             AND p.ssp_key    = t.ssp_key
WHERE
    -- Stable fix: now+yesterday both TRUE, prior was FALSE
       (t.line_now IS TRUE  AND y.line_y IS TRUE  AND p.line_p IS FALSE)
    OR (t.pgam_now IS TRUE  AND y.pgam_y IS TRUE  AND p.pgam_p IS FALSE)
    -- Stable regression: now+yesterday both FALSE, prior was TRUE
    OR (t.line_now IS FALSE AND y.line_y IS FALSE AND p.line_p IS TRUE)
    OR (t.pgam_now IS FALSE AND y.pgam_y IS FALSE AND p.pgam_p IS TRUE)
ORDER BY t.revenue_7d DESC;
"""


def _resolve_prior(today: date, want_lookback: int) -> date | None:
    """Pick the closest snapshot date <= today - want_lookback days, but
    if we don't have that much history yet, fall back to the oldest day
    we DO have (so the tracker still works on day 6 of operation)."""
    target = today - timedelta(days=want_lookback)
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(as_of) FROM pgam_direct.compliance_entity_supply_path_audit
            WHERE as_of <= %s AND as_of < %s
        """, (target, today))
        hit = cur.fetchone()[0]
        if hit:
            return hit
        cur.execute("""
            SELECT MIN(as_of) FROM pgam_direct.compliance_entity_supply_path_audit
            WHERE as_of < %s
        """, (today,))
        return cur.fetchone()[0]


def _fmt_money(x) -> str:
    try:
        return f"${float(x):,.0f}"
    except Exception:
        return "$?"


def _classify(now, y, p) -> str | None:
    """Map a (today, yesterday, prior) tri-state to a flip category.

    Returns 'fix' if a previously-stable FALSE flipped TRUE today,
    'regression' if a previously-stable TRUE has been FALSE for ≥1 day,
    or None for unstable / unchanged / unknown."""
    # Match the same conditions as the SQL WHERE clauses — see
    # _SUPPLY_PATH_DIFF_SQL for rationale. now+yesterday must agree on
    # the new state, prior must disagree; the result holds for ≥1 day.
    if now is True and y is True and p is False:
        return "fix"
    if now is False and y is False and p is True:
        return "regression"
    return None


def _supply_path_lines(rows: list[tuple]) -> tuple[list[str], list[str]]:
    """Return (fixes, regressions) human lines for supply-path diffs.

    Row layout matches _SUPPLY_PATH_DIFF_SQL SELECT:
      0 entity_key, 1 entity_value, 2 revenue_7d,
      3 supply_partner_key, 4 supply_partner_pgam_seat,
      5 a_now, 6 a_y, 7 a_p,   (publisher ads.txt has partner's line)
      8 b_now, 9 b_y, 10 b_p,  (publisher ads.txt has our pgamssp.com line)
      11 d_now, 12 d_y, 13 d_p (partner's sellers.json declares PGAM)
    """
    fixes, regressions = [], []
    for r in rows:
        ev = r[1] or r[0]
        rev = _fmt_money(r[2])
        partner = r[3] or "<?>"
        seat = r[4] or "<seat?>"
        for now, y, p, label in (
            (r[5], r[6], r[7],   f"`{partner}` line on `{ev}` ads.txt"),
            (r[8], r[9], r[10],  f"`pgamssp.com, {seat}, RESELLER` on `{ev}` ads.txt"),
            (r[11], r[12], r[13], f"PGAM declared in `{partner}` sellers.json"),
        ):
            kind = _classify(now, y, p)
            if kind == "fix":
                fixes.append(f"• ✅ *{ev}* ({rev}/7d) — {label} *now present*")
            elif kind == "regression":
                regressions.append(f"• 🚨 *{ev}* ({rev}/7d) — {label} *disappeared*")
    return fixes, regressions


def _ssp_lines(rows: list[tuple]) -> tuple[list[str], list[str]]:
    """Return (fixes, regressions) human lines for demand-side diffs.

    Row layout from _SSP_DIFF_SQL:
      0 entity_key, 1 entity_value, 2 revenue_7d,
      3 ssp_key, 4 ssp_partner_name,
      5 line_now, 6 line_y, 7 line_p,   (SSP's canonical line)
      8 pgam_now, 9 pgam_y, 10 pgam_p   (publisher ads.txt has PGAM seller_id)
    """
    fixes, regressions = [], []
    for r in rows:
        ev = r[1] or r[0]
        rev = _fmt_money(r[2])
        ssp = r[4] or r[3]
        for now, y, p, label in (
            (r[5], r[6], r[7],   f"`{ssp}` line on `{ev}` ads.txt"),
            (r[8], r[9], r[10],  f"PGAM `pgamssp.com` line on `{ev}` ads.txt"),
        ):
            kind = _classify(now, y, p)
            if kind == "fix":
                fixes.append(f"• ✅ *{ev}* ({rev}/7d) — {label} *now present*")
            elif kind == "regression":
                regressions.append(f"• 🚨 *{ev}* ({rev}/7d) — {label} *disappeared*")
    return fixes, regressions


def _truncate(lines: list[str], cap: int = MAX_ROWS_PER_SECTION) -> list[str]:
    if len(lines) <= cap:
        return lines
    return lines[:cap] + [f"_…+{len(lines) - cap} more — see today's CSV_"]


def _resolve_yday(today: date) -> date | None:
    """The most recent snapshot strictly before today. Used as the
    middle anchor of the 3-point stability check — if it's missing
    (e.g. crawler skipped a day), we can't run the diff at all."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT MAX(as_of) FROM pgam_direct.compliance_entity_supply_path_audit
            WHERE as_of < %s
        """, (today,))
        return cur.fetchone()[0]


def build_remediation_blocks(today: date | None = None,
                              lookback_days: int = 7) -> list[dict]:
    """Slack blocks summarizing partner fixes & regressions since the
    chosen lookback day. Returns [] if no prior snapshot is available
    (e.g. first day of operation) or if nothing changed.

    Requires THREE snapshots: today, yesterday, and the lookback prior.
    A flip is only counted if it's stable across the (prior, yesterday)
    pair — see SQL comments for the rationale (filters one-day crawler
    glitches that would otherwise dominate the report)."""
    today = today or date.today()
    yday  = _resolve_yday(today)
    prior = _resolve_prior(today, lookback_days)
    # If yesterday and the lookback target collide (only 2 days of
    # history available), the 3-point check degenerates to a 2-point.
    # That's acceptable — both same-day NOT-glitch checks still pass —
    # but we still need to skip if either is missing or if prior is
    # not strictly earlier than yesterday.
    if not yday or not prior or prior > yday or yday >= today:
        return []

    with connect() as conn, conn.cursor() as cur:
        cur.execute(_SUPPLY_PATH_DIFF_SQL,
                    {"today": today, "yday": yday, "prior": prior})
        sp_rows = cur.fetchall()
        cur.execute(_SSP_DIFF_SQL,
                    {"today": today, "yday": yday, "prior": prior})
        ssp_rows = cur.fetchall()

    sp_fixes, sp_regs   = _supply_path_lines(sp_rows)
    ssp_fixes, ssp_regs = _ssp_lines(ssp_rows)

    fixes       = sp_fixes + ssp_fixes
    regressions = sp_regs  + ssp_regs

    if not fixes and not regressions:
        # Still surface a single quiet line so operators know the tracker
        # ran. Cheap, and avoids "did the cron skip?" doubt.
        return [{
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f":telescope: *Remediation watch* — no line "
                              f"changes since {prior.isoformat()} "
                              f"({(today - prior).days}d lookback).")},
        }]

    delta_days = (today - prior).days
    out: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (f":telescope: *Remediation watch* "
                          f"({delta_days}d lookback — since {prior.isoformat()})\n"
                          f"_{len(fixes)} fixes / {len(regressions)} regressions "
                          f"detected by re-crawling ads.txt + sellers.json._")},
    }]
    if fixes:
        out.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f"*Confirmed fixes ({len(fixes)})* — lines that "
                              "went from missing → present:\n"
                              + "\n".join(_truncate(fixes)))},
        })
    if regressions:
        out.append({
            "type": "section",
            "text": {"type": "mrkdwn",
                     "text": (f"*Regressions ({len(regressions)})* — lines "
                              "that went from present → missing:\n"
                              + "\n".join(_truncate(regressions)))},
        })
    return out
