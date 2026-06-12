"""
agents/compliance/pgam_sellers_validator.py

Layer C direction-1 validator: does PGAM's own sellers.json declare
every upstream supply partner as INTERMEDIARY (or BOTH) with the seat
ID that matches what we use on publisher app-ads.txt pgamssp.com lines?

Why this matters: PubMatic (and any rigorous buyer) verifies the full
chain by reading https://pgamssp.com/sellers.json. If our file doesn't
declare BidMachine as INTERMEDIARY with the right seat, every BidMachine
path fails their transparency check — regardless of how clean the
publisher-side ads.txt is. A miss here is multiplicative: one missing
partner declaration breaks every host routed through that partner.

STRICT READ-ONLY:
  • Reads PGAM's own public sellers.json (the same fetch any buyer makes)
  • Reads compliance_publishers, compliance_ll_partner_bridge
  • Writes ONLY:
      - one daily snapshot to compliance_pgam_sellers_index
      - one finding row per partner per day to compliance_pgam_sellers_findings
      - one Slack message in #compliance
  • Never modifies LL, never modifies sellers.json (ours or theirs),
    never mutates routing/wiring/demand config

Schedule: 08:10 ET daily — sits between live_pubmatic_report (08:05)
and publisher_chain_audit (08:25). The morning #compliance message
chain ends up as:
    08:00  daily compliance digest        (all partners, all dimensions)
    08:05  live PubMatic morning report   (focused on live PubMatic traffic)
    08:10  PGAM sellers.json findings     (this module)
    08:25  partner sellers.json refresh   (silent, no Slack)

Idempotent on re-fire via compliance_alert_state dedup.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import date, datetime, timezone

import requests

from core.neon import connect


ACTOR = "pgam_sellers_validator"
DEDUP_KEY_FMT = "pgam_sellers_validator:%Y-%m-%d"

# PGAM's sellers.json — the URL buyers actually read. Bare domain is the
# canonical location per IAB spec; the www variant is a fallback. The
# `sellers.` subdomain currently returns 403 and is NOT what buyers use.
PGAM_SELLERS_URL          = "https://pgamssp.com/sellers.json"
PGAM_SELLERS_URL_FALLBACK = "https://www.pgamssp.com/sellers.json"

SLACK_WEBHOOK = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()
TIMEOUT_SEC = 20
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"


def _normalize(d: str) -> str:
    d = (d or "").strip().lower()
    if d.startswith("www."): d = d[4:]
    return d


def _fetch_pgam_sellers_json() -> dict | None:
    for url in (PGAM_SELLERS_URL, PGAM_SELLERS_URL_FALLBACK):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
                if r.status != 200: continue
                data = json.loads(r.read().decode("utf-8", errors="ignore"))
                if isinstance(data, dict) and "sellers" in data:
                    print(f"[{ACTOR}] fetched {url}: {len(data['sellers'])} sellers")
                    return data
        except Exception as exc:
            print(f"[{ACTOR}] fetch failed for {url}: {exc}")
    return None


def _persist_snapshot(cur, sellers: list[dict]) -> int:
    """Replace today's snapshot in compliance_pgam_sellers_index."""
    today = date.today()
    cur.execute(
        "DELETE FROM pgam_direct.compliance_pgam_sellers_index "
        "WHERE snapshot_date = %s", (today,))
    rows = []
    seen = set()
    for s in sellers:
        sid = (s.get("seller_id") or "").strip()
        styp = (s.get("seller_type") or "").upper()
        dom = _normalize(s.get("domain", ""))
        if not sid or not styp: continue
        key = (today, sid, dom)
        if key in seen: continue
        seen.add(key)
        rows.append((today, sid, styp, s.get("name", ""), dom,
                     s.get("is_confidential", False)))
    if rows:
        cur.executemany(
            "INSERT INTO pgam_direct.compliance_pgam_sellers_index "
            "(snapshot_date, seller_id, seller_type, seller_name, domain, is_confidential) "
            "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", rows)
    return len(rows)


def _build_expected_partners(cur) -> list[dict]:
    """The set of upstream partners we expect to find declared in
    PGAM's sellers.json — pulled from compliance_publishers (filtered
    to active intermediaries that are linked to a live LL publisher)
    + their bridge-derived expected seat."""
    cur.execute("""
        SELECT cp.publisher_key, cp.domain,
               cp.seller_type, cp.ll_publisher_id, cp.ll_publisher_name,
               cp.revenue_recent_7d,
               -- expected seat: pull from compliance_ll_partner_bridge.
               -- The bridge may have multiple rows per partner (one per
               -- LL pub) — they all carry the same seller_id in practice,
               -- so pick the one for the linked ll_publisher_id when
               -- possible, fallback to any.
               (
                 SELECT seller_id FROM pgam_direct.compliance_ll_partner_bridge
                 WHERE publisher_key = cp.publisher_key
                 ORDER BY ll_publisher_id = cp.ll_publisher_id::text DESC
                 LIMIT 1
               ) AS expected_seat
        FROM pgam_direct.compliance_publishers cp
        WHERE cp.seller_type IN ('INTERMEDIARY','BOTH')
          AND cp.is_active = TRUE
          AND cp.ll_publisher_id IS NOT NULL
          AND cp.domain IS NOT NULL
        ORDER BY cp.publisher_key
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _evaluate(expected: list[dict], pgam_by_domain: dict[str, list[dict]]) -> list[dict]:
    """For each expected partner, compute a finding: ok / missing /
    wrong_type / wrong_seat."""
    findings = []
    for p in expected:
        dom = _normalize(p["domain"])
        candidates = pgam_by_domain.get(dom, [])
        # Prefer INTERMEDIARY/BOTH; if none, fall back to whatever is declared
        intermediary_rows = [c for c in candidates
                             if c.get("seller_type","").upper() in ("INTERMEDIARY","BOTH")]
        if intermediary_rows:
            row = intermediary_rows[0]
        elif candidates:
            row = candidates[0]
        else:
            row = None

        if row is None:
            findings.append({
                "partner_key":  p["publisher_key"],
                "domain":       dom,
                "expected_seat": p.get("expected_seat"),
                "declared_seat": None,
                "declared_type": None,
                "status":       "missing",
                "evidence":     {"reason": "no row in PGAM sellers.json with domain="
                                           + dom},
                "rev_7d":       float(p.get("revenue_recent_7d") or 0),
            })
            continue

        declared_type = (row.get("seller_type") or "").upper()
        declared_seat = row.get("seller_id")
        expected_seat = p.get("expected_seat")

        if declared_type not in ("INTERMEDIARY", "BOTH"):
            findings.append({
                "partner_key":  p["publisher_key"],
                "domain":       dom,
                "expected_seat": expected_seat,
                "declared_seat": declared_seat,
                "declared_type": declared_type,
                "status":       "wrong_type",
                "evidence":     {"reason":
                                 f"declared as {declared_type}, must be "
                                 "INTERMEDIARY or BOTH"},
                "rev_7d":       float(p.get("revenue_recent_7d") or 0),
            })
            continue

        if expected_seat and declared_seat and declared_seat != expected_seat:
            findings.append({
                "partner_key":  p["publisher_key"],
                "domain":       dom,
                "expected_seat": expected_seat,
                "declared_seat": declared_seat,
                "declared_type": declared_type,
                "status":       "wrong_seat",
                "evidence":     {"reason":
                                 f"PGAM declares seat {declared_seat} but "
                                 f"bridge expects {expected_seat} — buyers "
                                 "may not be able to match the publisher's "
                                 "pgamssp.com line to our declaration"},
                "rev_7d":       float(p.get("revenue_recent_7d") or 0),
            })
            continue

        findings.append({
            "partner_key":  p["publisher_key"],
            "domain":       dom,
            "expected_seat": expected_seat,
            "declared_seat": declared_seat,
            "declared_type": declared_type,
            "status":       "ok",
            "evidence":     {},
            "rev_7d":       float(p.get("revenue_recent_7d") or 0),
        })

    return findings


def _persist_findings(cur, findings: list[dict]) -> None:
    today = date.today()
    cur.execute(
        "DELETE FROM pgam_direct.compliance_pgam_sellers_findings "
        "WHERE snapshot_date = %s", (today,))
    rows = [(today, f["partner_key"], f["expected_seat"], f["declared_seat"],
             f["declared_type"], f["status"], json.dumps(f["evidence"]))
            for f in findings]
    if rows:
        cur.executemany(
            "INSERT INTO pgam_direct.compliance_pgam_sellers_findings "
            "(snapshot_date, partner_key, expected_seat, declared_seat, "
            " declared_type, status, evidence) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb) "
            "ON CONFLICT DO NOTHING", rows)


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


def _build_slack_message(findings: list[dict],
                         total_sellers_in_pgam_file: int) -> str:
    by_status = {"ok": 0, "missing": 0, "wrong_type": 0, "wrong_seat": 0}
    for f in findings: by_status[f["status"]] = by_status.get(f["status"], 0) + 1
    n_problems = by_status["missing"] + by_status["wrong_type"] + by_status["wrong_seat"]

    lines = []
    lines.append(":mag: *PGAM sellers.json — upstream partner declarations*  "
                 f"_(read-only)_")
    lines.append("")
    lines.append(f"• PGAM sellers.json: *{total_sellers_in_pgam_file}* total entries "
                 f"({PGAM_SELLERS_URL})")
    lines.append(f"• Active upstream intermediaries to validate: *{len(findings)}*")
    lines.append(f"• Status: *{by_status['ok']} OK*, "
                 f"*{by_status['missing']} missing*, "
                 f"*{by_status['wrong_type']} wrong type*, "
                 f"*{by_status['wrong_seat']} wrong seat*")
    lines.append("")

    if n_problems == 0:
        lines.append(":white_check_mark: All active intermediaries are correctly "
                     "declared in PGAM's sellers.json with the seat IDs that match "
                     "our LL bridge config.")
    else:
        # Group by status for visibility
        for status, label, emoji in (
            ("missing",    "Missing from PGAM sellers.json",  ":x:"),
            ("wrong_type", "Wrong seller_type",               ":warning:"),
            ("wrong_seat", "Seat ID mismatch",                ":warning:"),
        ):
            group = [f for f in findings if f["status"] == status]
            if not group: continue
            group.sort(key=lambda f: -f["rev_7d"])
            lines.append(f"{emoji} *{label}* ({len(group)}):")
            lines.append("```")
            lines.append(f"{'partner':<22}{'expected seat':<36}{'declared':<36}{'rev_7d':>10}")
            for f in group[:20]:
                exp = (f.get("expected_seat") or "—")[:34]
                dec = (f.get("declared_seat") or "—")[:34]
                if f["status"] == "wrong_type":
                    dec = f"{dec} ({f.get('declared_type','-')})"[:34]
                lines.append(f"{f['partner_key'][:20]:<22}"
                             f"{exp:<36}"
                             f"{dec:<36}"
                             f"${f['rev_7d']:>8,.0f}")
            lines.append("```")
            lines.append("")

    lines.append("_Each finding row also persisted to "
                 "`pgam_direct.compliance_pgam_sellers_findings` for query._")

    return "\n".join(lines)


def _build_report(*, force: bool = False) -> dict:
    started = datetime.now(timezone.utc)

    data = _fetch_pgam_sellers_json()
    if not data:
        msg = (":rotating_light: *PGAM sellers.json validator* — "
               "fetch FAILED. Buyers cannot verify our schain. "
               f"Tried {PGAM_SELLERS_URL} and {PGAM_SELLERS_URL_FALLBACK}.")
        _post_slack(msg)
        return {"ok": False, "error": "fetch_failed"}

    sellers = data.get("sellers", [])
    pgam_by_domain: dict[str, list[dict]] = {}
    for s in sellers:
        d = _normalize(s.get("domain", ""))
        pgam_by_domain.setdefault(d, []).append(s)

    with connect() as c, c.cursor() as cur:
        n_indexed = _persist_snapshot(cur, sellers)
        expected = _build_expected_partners(cur)
        findings = _evaluate(expected, pgam_by_domain)
        _persist_findings(cur, findings)

        msg = _build_slack_message(findings, len(sellers))

        if not force and _already_sent_today(cur):
            print(f"[{ACTOR}] already sent today — skipping Slack")
            c.commit()
            return {"ok": True, "skipped": "already_sent_today",
                    "indexed": n_indexed, "findings": len(findings)}

        _post_slack(msg)
        _mark_sent_today(cur)
        c.commit()

    by_status: dict[str, int] = {}
    for f in findings: by_status[f["status"]] = by_status.get(f["status"], 0) + 1
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return {
        "ok": True,
        "indexed_sellers": n_indexed,
        "expected_partners": len(expected),
        "findings_by_status": by_status,
        "elapsed_sec": elapsed,
    }


def run() -> dict:
    """Cron entry point. Idempotent on re-fire same UTC day."""
    return _build_report(force=False)


if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    print(json.dumps(_build_report(force=force), indent=2, default=str))
