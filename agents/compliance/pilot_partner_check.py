"""
agents/compliance/pilot_partner_check.py

Daily per-(pilot supply partner × top entities × test demand SSPs)
compliance report. Posts directly into the daily digest as a focused
"pilot mode" section.

Reads from the existing compliance_entity_supply_path_audit +
compliance_entity_ssp_audit tables (no extra crawls), so it adds
near-zero cost to the runner. Configurable via:

  PGAM_COMPLIANCE_PILOT_PARTNERS
    Comma-separated list of compliance_publishers.publisher_key values
    (e.g. 'algorix.co,smaato.com,zmaticoo.com'). Default = these three.

  PGAM_COMPLIANCE_PILOT_TEST_SSPS
    Comma-separated ssp_key list to verify per pilot partner
    (e.g. 'rubicon,unruly,sharethrough,appnexus'). Default = the four
    initially picked for the Algorix pilot.

Output is a list of Slack block dicts ready to extend into the digest.
Flagging only — never modifies anything.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import date

from core.neon import connect


DEFAULT_PILOT_PARTNERS = ("algorix.co", "smaato.com", "zmaticoo.com")
DEFAULT_TEST_SSPS = ("rubicon", "unruly", "sharethrough", "appnexus")

# Display names + canonical lines per test SSP. Sourced from
# ssp_registry.PHASE_2_SSP_EXPECTATIONS but pinned here so the report
# is self-contained and the labels match the ones operators read in
# digests every day. If the registry shifts, both move together.
TEST_SSP_META: dict[str, dict] = {
    "rubicon": {
        "name":     "Magnite/Rubicon",
        "line":     "rubiconproject.com, 24852, RESELLER, 0bfd66d529a55807",
        "audit_id": "Rubicon",
    },
    "unruly": {
        "name":     "Unruly",
        "line":     "video.unrulymedia.com, 5921144960123684292, RESELLER",
        "audit_id": "Unruly",
    },
    "sharethrough": {
        "name":     "Sharethrough (incl. Blitz)",
        "line":     "sharethrough.com, VQlYJeXR, RESELLER, d53b998a7bd4ecd2",
        "audit_id": "Sharethrough",
    },
    "appnexus": {
        "name":     "Xandr/AppNexus (incl. Blitz)",
        "line":     "appnexus.com, 8106, RESELLER",
        "audit_id": "Appnexus",
    },
}


def _get_pilot_partners() -> list[str]:
    env_val = (os.environ.get("PGAM_COMPLIANCE_PILOT_PARTNERS") or "").strip()
    if env_val:
        return [p.strip() for p in env_val.split(",") if p.strip()]
    return list(DEFAULT_PILOT_PARTNERS)


def _get_test_ssp_keys() -> list[str]:
    env_val = (os.environ.get("PGAM_COMPLIANCE_PILOT_TEST_SSPS") or "").strip()
    if env_val:
        return [s.strip() for s in env_val.split(",") if s.strip()]
    return list(DEFAULT_TEST_SSPS)


_PARTNER_AUDIT_SQL = """
SELECT entity_value, kind, audit_host, revenue_7d,
       ll_publisher_id, ll_publisher_name,
       supply_partner_pgam_seat,
       supply_partner_line_present,
       pgam_line_present_for_path,
       sellers_json_partner_declared,
       status
FROM pgam_direct.compliance_entity_supply_path_audit
WHERE as_of = %(as_of)s AND supply_partner_key = %(partner)s
ORDER BY revenue_7d DESC;
"""


_LAYER_D_SQL = """
SELECT COUNT(*) FROM pgam_direct.compliance_findings
WHERE status = 'open'
  AND publisher_key = %(sentinel)s
  AND check_id LIKE 'sellersjson.supply_partner_%%';
"""


_DEMAND_LINES_SQL = """
SELECT entity_value, ssp_partner_name, ssp_line_present
FROM pgam_direct.compliance_entity_ssp_audit
WHERE as_of = %(as_of)s
  AND entity_key = ANY(%(entity_keys)s)
  AND ssp_partner_name = ANY(%(ssp_names)s);
"""


def _render_table_local(rows: list[list], headers: list[str],
                        aligns: list[str] | None = None) -> str:
    """Local copy of slack_digest._render_table so this module is
    importable without circular dependencies."""
    MAX_CELL_W = 28
    aligns = aligns or ["l"] * len(headers)
    norm = [[(str(c) if c is not None else "")[:MAX_CELL_W] for c in r] for r in rows]
    widths = [len(h) for h in headers]
    for r in norm:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(c))
    def fmt(row):
        out = []
        for i, c in enumerate(row):
            out.append(c.rjust(widths[i]) if aligns[i] == "r" else c.ljust(widths[i]))
        return "  ".join(out).rstrip()
    sep = "  ".join("─" * w for w in widths)
    return "```\n" + "\n".join([fmt(headers), sep] + [fmt(r) for r in norm]) + "\n```"


def build_pilot_blocks(as_of: date | None = None) -> list[dict]:
    """Return Slack section blocks for the pilot-partner report.

    Used by reporters/slack_digest.py to insert into the daily message.
    Safe to call repeatedly; reads only.
    """
    as_of = as_of or date.today()
    partners = _get_pilot_partners()
    test_keys = _get_test_ssp_keys()
    test_meta = [TEST_SSP_META.get(k) for k in test_keys]
    test_meta = [m for m in test_meta if m]
    if not partners or not test_meta:
        return []

    test_audit_ids = [m["audit_id"] for m in test_meta]

    out: list[dict] = [{
        "type": "section",
        "text": {"type": "mrkdwn",
                 "text": (f":bookmark_tabs: *Pilot-partner compliance check*\n"
                          f"_Supply partners: {', '.join(partners)}_\n"
                          f"_Test demand SSPs: "
                          f"{', '.join(m['name'] for m in test_meta)}_\n"
                          "_Flagging only — no enforcement._")},
    }]

    with connect() as conn, conn.cursor() as cur:
        for partner_key in partners:
            cur.execute(_PARTNER_AUDIT_SQL,
                        {"as_of": as_of, "partner": partner_key})
            sp_rows = cur.fetchall()
            if not sp_rows:
                out.append({
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": f":grey_question: *{partner_key}* — no entities in today's audit universe."},
                })
                continue

            partner_total   = sum(float(r[3]) for r in sp_rows)
            partner_at_risk = sum(float(r[3]) for r in sp_rows if r[10] != "healthy")
            healthy_n       = sum(1 for r in sp_rows if r[10] == "healthy")
            seat            = sp_rows[0][6] or "<unknown>"

            cur.execute(_LAYER_D_SQL,
                        {"sentinel": f"_supply:{partner_key}"})
            layer_d_bad = (cur.fetchone()[0] or 0) > 0

            entity_keys = [
                ("dom:" + r[0]) if r[1] == "domain" else ("app:" + r[0])
                for r in sp_rows
            ]
            cur.execute(_DEMAND_LINES_SQL, {
                "as_of": as_of,
                "entity_keys": entity_keys,
                "ssp_names": test_audit_ids,
            })
            demand_by_entity: dict[str, dict[str, bool]] = defaultdict(dict)
            for ev, ssp, ok in cur.fetchall():
                demand_by_entity[ev][ssp] = bool(ok)

            head = "🚨" if (partner_at_risk > 0 or layer_d_bad) else "✅"
            layer_d_glyph = "✗" if layer_d_bad else "✓"
            header = (
                f"{head} *{partner_key}* · ${partner_total:,.0f}/7d total · "
                f"*${partner_at_risk:,.0f} at risk* · "
                f"{healthy_n}/{len(sp_rows)} healthy · "
                f"Layer D (their sellers.json): {layer_d_glyph}\n"
                f"_Expected on every publisher ads.txt routing via {partner_key}: "
                f"`pgamssp.com, {seat}, RESELLER`_"
            )

            # Per-entity row — supply-side 3 layers + 4 demand SSPs.
            table_rows = []
            for r in sp_rows:
                ev, kind, host, rev, _ll_id, _ll_name, _seat, A, B, C, st = r
                sym = {"critical": "🚨", "warning": "⚠️",
                       "healthy": "✅"}.get(st, "·")
                cells = [
                    sym, ev[:22], f"${float(rev):,.0f}",
                    "✓" if A else "✗",
                    "✓" if B else "✗",
                    "✓" if C else "✗",
                ]
                for m in test_meta:
                    ok = demand_by_entity.get(ev, {}).get(m["audit_id"])
                    cells.append("✓" if ok is True else "✗" if ok is False else "—")
                table_rows.append(cells)
            headers = ["", "Entity", "Rev/7d", "A) part", "B) PGAM", "C) ours"]
            headers.extend(m["name"].split("/")[0][:12] for m in test_meta)
            table = _render_table_local(
                table_rows, headers,
                aligns=["l"] * len(headers),
            )

            # Suggested-fix block (concrete copy-paste lines).
            fix_lines: list[str] = []
            for r in sp_rows:
                ev, kind, host, rev, _ll_id, _ll_name, _seat, A, B, C, st = r
                if st == "healthy":
                    continue
                needs = []
                if not A:
                    needs.append(f"add the `{partner_key}` supply line")
                if not B:
                    needs.append(f"add `pgamssp.com, {seat}, RESELLER`")
                for m in test_meta:
                    ok = demand_by_entity.get(ev, {}).get(m["audit_id"])
                    if ok is False:
                        needs.append(f"add `{m['line']}`")
                if needs:
                    fix_lines.append(
                        f"• *{ev}* needs:\n   " + "\n   ".join(needs)
                    )
            if layer_d_bad:
                fix_lines.append(
                    f"• *{partner_key}'s sellers.json* needs PGAM listed "
                    "(`name='PGAMmedia'`, `domain='pgamssp.com'`, "
                    "`seller_type='INTERMEDIARY'`)"
                )

            body = header + "\n\n" + table
            if fix_lines:
                body += "\n\n*Suggested fixes:*\n" + "\n\n".join(fix_lines)
            if len(body) > 2900:
                body = body[:2880] + "\n_…full list in today's CSV._"
            out.append({"type": "section",
                        "text": {"type": "mrkdwn", "text": body}})
    return out
