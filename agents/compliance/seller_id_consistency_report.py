"""
agents/compliance/seller_id_consistency_report.py

Seller-ID consistency cross-check (Task #57).

For every (publisher × supply_partner) pair in the latest
compliance_entity_supply_path_audit snapshot, compares:

  1. Partner-side consistency:
     • app-ads.txt observed seat for the partner domain
     • vs the partner's sellers.json declared seller_id for the publisher
     If both are present and they disagree → MISMATCH.

  2. PGAM-side consistency:
     • app-ads.txt observed seats for pgamssp.com
     • vs the expected pgam seat for this partner's path
       (compliance_ll_partner_bridge / supply_partner_pgam_seat)
     If observed_pgam_seats is non-empty but expected seat is not in it
     → MISMATCH (wrong seat declared).

  3. Seller type sanity:
     • partner_sellers_json_seller_type should be PUBLISHER or BOTH for
       audit_hosts that we treat as publishers.
     If declared as INTERMEDIARY → flag (the host is an intermediary,
     dev_domain resolution may be wrong, or the partner's classification
     differs from ours).

STRICT READ-ONLY:
  • Reads compliance_entity_supply_path_audit (latest as_of).
  • Writes ONLY one Slack message + one dedup row in
    compliance_alert_state.
  • Never writes to LL, never modifies any compliance_* table.

No schema migration — the raw data is already captured:
  • observed_partner_seats        — TEXT[] of seller_ids on app-ads.txt
  • partner_sellers_json_seller_id — partner's declared seat (already on table)
  • observed_pgam_seats           — TEXT[] of pgamssp seats on app-ads.txt
  • supply_partner_pgam_seat      — expected pgam seat (already on table)

Schedule: 08:15 ET daily — sits between pgam_sellers_validator (08:10)
and publisher_chain_audit (08:25). Morning #compliance chain:
    08:00  daily compliance digest
    08:05  live PubMatic morning report
    08:10  PGAM sellers.json findings        (Layer C direction-1)
    08:15  seller-ID consistency findings    (this — IDs match across layers?)
    08:25  partner sellers.json refresh      (silent)

Idempotent on re-fire via compliance_alert_state.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import requests

from core.neon import connect


ACTOR = "seller_id_consistency_report"
DEDUP_KEY_FMT = "seller_id_consistency_report:%Y-%m-%d"
SLACK_WEBHOOK = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()

# Top-N rows per failure category to show in the digest. Volume guard —
# the audit covers thousands of (host × partner) combos.
TOP_PER_CATEGORY = 15


def _post_slack(text: str) -> None:
    if not SLACK_WEBHOOK:
        print(f"[{ACTOR}] (no Slack webhook configured)")
        print(text)
        return
    try:
        r = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=15)
        print(f"[{ACTOR}] Slack post: {r.status_code}")
    except Exception as exc:
        print(f"[{ACTOR}] Slack post failed: {exc}")


def _already_sent_today(cur) -> bool:
    today_key = datetime.now(timezone.utc).strftime(DEDUP_KEY_FMT)
    cur.execute("""
        SELECT 1 FROM pgam_direct.compliance_alert_state
        WHERE dedup_key = %s LIMIT 1
    """, (today_key,))
    return cur.fetchone() is not None


def _mark_sent_today(cur) -> None:
    today_key = datetime.now(timezone.utc).strftime(DEDUP_KEY_FMT)
    cur.execute("""
        INSERT INTO pgam_direct.compliance_alert_state
            (dedup_key, as_of, marked_at)
        VALUES (%s, CURRENT_DATE, now())
        ON CONFLICT (dedup_key, as_of) DO NOTHING
    """, (today_key,))


_PULL_LATEST_AUDIT_SQL = """
SELECT entity_value, audit_host, supply_partner_key,
       partner_sellers_json_seller_id   AS sj_seat,
       partner_sellers_json_seller_type AS sj_type,
       observed_partner_seats,
       observed_pgam_seats,
       supply_partner_pgam_seat         AS expected_pgam_seat,
       supply_partner_line_present,
       pgam_line_present_for_path,
       revenue_7d
FROM pgam_direct.compliance_entity_supply_path_audit
WHERE as_of = (SELECT MAX(as_of) FROM pgam_direct.compliance_entity_supply_path_audit)
"""


def _classify(row: dict) -> tuple[str | None, dict]:
    """Return (issue_category, evidence_dict) or (None, evidence) if all OK.

    Categories:
      • 'partner_seat_mismatch'  — sj_seat ≠ any observed_partner_seats
      • 'pgam_seat_mismatch'     — expected_pgam_seat ∉ observed_pgam_seats
      • 'partner_type_warning'   — sj_type is INTERMEDIARY (not PUBLISHER/BOTH)

    Layer A/B "line missing" cases are NOT classified here — they're
    already surfaced by the existing daily compliance digest.
    """
    sj_seat = (row.get("sj_seat") or "").strip()
    sj_type = (row.get("sj_type") or "").upper()
    observed_partner = row.get("observed_partner_seats") or []
    observed_pgam    = row.get("observed_pgam_seats") or []
    expected_pgam    = (row.get("expected_pgam_seat") or "").strip()

    # Partner-side seat consistency
    partner_check = None
    if sj_seat and observed_partner:
        if sj_seat not in observed_partner:
            partner_check = "partner_seat_mismatch"

    # PGAM-side seat consistency
    pgam_check = None
    if expected_pgam and observed_pgam:
        if expected_pgam not in observed_pgam:
            pgam_check = "pgam_seat_mismatch"

    # Partner type sanity — only flag if the partner DOES declare them
    # but as INTERMEDIARY (mismatch of role — most app publishers should
    # be PUBLISHER or BOTH).
    type_check = None
    if sj_type == "INTERMEDIARY":
        type_check = "partner_type_warning"

    evidence = {
        "sj_seat": sj_seat or None,
        "sj_type": sj_type or None,
        "observed_partner_seats_sample": list(observed_partner)[:8],
        "observed_pgam_seats_sample":    list(observed_pgam)[:8],
        "expected_pgam_seat": expected_pgam or None,
    }
    # Prioritize: partner_seat_mismatch is most actionable, then pgam, then type
    return (partner_check or pgam_check or type_check), evidence


def _aggregate(rows: list[dict]) -> dict:
    """Group classified rows by issue category, dedup to (host × partner)
    so a single publisher with 100 bundles doesn't dominate."""
    findings_by_cat: dict[str, dict[tuple[str, str], dict]] = defaultdict(dict)
    total_checked = 0

    for row in rows:
        total_checked += 1
        category, evidence = _classify(row)
        if category is None:
            continue
        key = (row["audit_host"], row["supply_partner_key"])
        existing = findings_by_cat[category].get(key)
        rev_7d = float(row.get("revenue_7d") or 0)
        if existing is None or rev_7d > existing["max_rev_7d"]:
            findings_by_cat[category][key] = {
                "host":      row["audit_host"],
                "partner":   row["supply_partner_key"],
                "evidence":  evidence,
                "max_rev_7d": rev_7d,
            }
    return {"total_checked": total_checked,
            "findings_by_category": findings_by_cat}


def _build_slack_message(agg: dict) -> str:
    fbc: dict = agg["findings_by_category"]
    n_partner = len(fbc.get("partner_seat_mismatch", {}))
    n_pgam    = len(fbc.get("pgam_seat_mismatch",    {}))
    n_type    = len(fbc.get("partner_type_warning", {}))
    total_issues = n_partner + n_pgam + n_type

    lines = []
    lines.append(":id: *Seller-ID consistency cross-check*  "
                 "_(read-only — IDs across ads.txt / sellers.json / "
                 "PGAM bridge)_")
    lines.append("")
    lines.append(f"• Audit rows scanned: *{agg['total_checked']:,}*")
    lines.append(f"• Status: *{n_partner} partner seat mismatch*, "
                 f"*{n_pgam} PGAM seat mismatch*, "
                 f"*{n_type} type warnings*  "
                 f"({total_issues} total)")
    lines.append("")
    if total_issues == 0:
        lines.append(":white_check_mark: Every (host × partner) pair with "
                     "both an app-ads.txt line and a sellers.json declaration "
                     "has consistent seller IDs.")
        lines.append("")
        lines.append("_Layer A/B 'line missing' cases are surfaced by the daily "
                     "compliance digest; this report focuses on the layer above: "
                     "value consistency where lines exist._")
        return "\n".join(lines)

    if n_partner:
        lines.append(":rotating_light: *Partner seat mismatch — app-ads.txt declares one seat, "
                     "partner's sellers.json declares a different one*")
        rows = sorted(fbc["partner_seat_mismatch"].values(),
                      key=lambda f: -f["max_rev_7d"])[:TOP_PER_CATEGORY]
        lines.append("```")
        # Column widths: 28 / 14 / 16 / 30 / 10. The SJ-seat column is 16
        # wide (not 14) so the header "declared in SJ" (exactly 14 chars)
        # gets the trailing 2-char separator that prevents it bleeding
        # into the next column.
        lines.append(f"{'host':<28}{'partner':<14}{'declared in SJ':<16}{'on app-ads.txt':<30}{'rev_7d':>10}")
        for f in rows:
            ev = f["evidence"]
            atx_sample = ",".join(ev["observed_partner_seats_sample"][:3])
            lines.append(f"{f['host'][:26]:<28}"
                         f"{f['partner'][:12]:<14}"
                         f"{(ev['sj_seat'] or '-')[:14]:<16}"
                         f"{atx_sample[:28]:<30}"
                         f"${f['max_rev_7d']:>8,.0f}")
        lines.append("```")
        lines.append("")

    if n_pgam:
        lines.append(":warning: *PGAM seat mismatch — wrong pgamssp seat declared on app-ads.txt*")
        rows = sorted(fbc["pgam_seat_mismatch"].values(),
                      key=lambda f: -f["max_rev_7d"])[:TOP_PER_CATEGORY]
        lines.append("```")
        lines.append(f"{'host':<28}{'partner':<14}{'expected':<36}{'rev_7d':>10}")
        for f in rows:
            ev = f["evidence"]
            lines.append(f"{f['host'][:26]:<28}"
                         f"{f['partner'][:12]:<14}"
                         f"{(ev['expected_pgam_seat'] or '-')[:34]:<36}"
                         f"${f['max_rev_7d']:>8,.0f}")
        lines.append("```")
        lines.append("")

    if n_type:
        lines.append(":information_source: *Partner declares as INTERMEDIARY* — may indicate "
                     "dev_domain resolution issue or the partner classifying differently than expected")
        rows = sorted(fbc["partner_type_warning"].values(),
                      key=lambda f: -f["max_rev_7d"])[:TOP_PER_CATEGORY]
        lines.append("```")
        lines.append(f"{'host':<28}{'partner':<14}{'sj_type':<14}{'rev_7d':>10}")
        for f in rows:
            ev = f["evidence"]
            lines.append(f"{f['host'][:26]:<28}"
                         f"{f['partner'][:12]:<14}"
                         f"{(ev['sj_type'] or '-')[:12]:<14}"
                         f"${f['max_rev_7d']:>8,.0f}")
        lines.append("```")

    lines.append("")
    lines.append("_Read-only finding. Layer A/B 'line missing' failures are "
                 "in the daily compliance digest; this report focuses on the "
                 "layer above — VALUE consistency where lines exist._")
    return "\n".join(lines)


def _build_report(*, force: bool = False) -> dict:
    started = datetime.now(timezone.utc)

    with connect() as c, c.cursor() as cur:
        cur.execute(_PULL_LATEST_AUDIT_SQL)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        agg = _aggregate(rows)

        msg = _build_slack_message(agg)

        if not force and _already_sent_today(cur):
            print(f"[{ACTOR}] already sent today — skipping Slack")
            return {"ok": True, "skipped": "already_sent_today",
                    "checked": agg["total_checked"]}

        _post_slack(msg)
        _mark_sent_today(cur)
        c.commit()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    findings_count = {cat: len(d) for cat, d in agg["findings_by_category"].items()}
    return {
        "ok": True,
        "checked":            agg["total_checked"],
        "findings_by_category": findings_count,
        "elapsed_sec":        elapsed,
    }


def run() -> dict:
    """Cron entry point. Idempotent on re-fire same UTC day."""
    return _build_report(force=False)


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    print(json.dumps(_build_report(force=force), indent=2, default=str))
