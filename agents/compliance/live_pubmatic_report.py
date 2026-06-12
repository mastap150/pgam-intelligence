"""
agents/compliance/live_pubmatic_report.py

Daily morning report focused on LIVE PubMatic inventory.

Runs once at 08:05 ET — 5 min after the existing daily compliance digest
at 08:00 ET — so both messages land in #compliance together. The aim is
a focused "PubMatic is on, here's what's flowing right now, here's the
chain-of-custody status of every host" view, separate from the broader
daily compliance digest which spans every supply partner and demand.

STRICT READ-ONLY. NO ENFORCEMENT. NO STATE WRITES.
  • Does not call ll_mgmt.disable_publisher_demand
  • Does not mutate any compliance_* table
  • Does not touch FORCE_DRY_RUN guard on the drift-watch
  • Slack post is the only side-effect; that's the entire purpose

Data sources (all read):
  • LL /v1/demands              → 22 active PubMatic demands w/ QPS
  • LL /v1/demands/{id}/publishers → routing graph (the live primitive
                                     — publisher.biddingpreferences
                                     does NOT carry these wirings)
  • compliance_ll_partner_bridge   → publisher_id → supply_partner_key
  • compliance_entity_supply_path_audit (latest as_of) → per-(host ×
                                     supply) Layer A/B/C signals

Layer model in this report:
  • Layer D (schain config on the demand) — the PubMatic gate.
    supplyChainEnabled=True AND dontAddSupplyChainNode=False.
    Failure = "critical" (would fail PubMatic's enforcement at the gate).
  • Layer A — publisher's app-ads.txt has the supply partner's RESELLER line
  • Layer B — publisher's app-ads.txt has pgamssp.com with the partner-seat
  • Layer C — supply partner's sellers.json declares this publisher as
              PUBLISHER or BOTH
  Layers A/B/C = "transparency gaps". Reported but NOT framed as
  immediate enforcement targets — the calibration of "what counts as
  blockable" is the operator's call after a few days of observation.

Idempotent — uses compliance_alert_state to dedupe per day so a Render
restart can't double-post.
"""
from __future__ import annotations

import json
import os
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import requests

from core import ll_mgmt
from core.ll_mgmt import LL_MGMT_BASE, _headers, get_token
from core.neon import connect


ACTOR = "live_pubmatic_report"
DEDUP_KEY_FMT = "live_pubmatic_report:%Y-%m-%d"  # one post per ET-day

PUBMATIC_PARTNER_ID = 3  # LL's demandPartner id for PubMatic
SLACK_WEBHOOK = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()

# Top-N failures by 7d revenue to show in the digest. Bigger numbers add
# noise; smaller numbers may hide systemic issues. 10 is a usable default.
TOP_FAILURES_TO_SHOW = 10


def _post_slack(text: str) -> None:
    if not SLACK_WEBHOOK:
        print(f"[{ACTOR}] (no Slack webhook configured — would have posted)")
        print(text[:500] + ("…" if len(text) > 500 else ""))
        return
    try:
        r = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=15)
        print(f"[{ACTOR}] Slack post: {r.status_code}")
    except Exception as exc:
        print(f"[{ACTOR}] Slack post failed: {exc}")


def _already_sent_today(cur) -> bool:
    """ET-aware day-dedup. Returns True if we already posted today's report.

    08:05 ET = 12:05 UTC year-round (DST handling is in the scheduler, not
    here), so the UTC date matches the ET date at firing time.
    """
    today_key = datetime.now(timezone.utc).strftime(DEDUP_KEY_FMT)
    cur.execute("""
        SELECT 1 FROM pgam_direct.compliance_alert_state
        WHERE dedup_key = %s
        LIMIT 1
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


def _fetch_live_pubmatic_demands() -> list[dict]:
    """PubMatic demands that are status=1 AND have recent bid activity."""
    demands = ll_mgmt.get_demands(include_archived=False) or []
    pm = [d for d in demands
          if d.get("demandPartner") == PUBMATIC_PARTNER_ID
          and d.get("status") == 1]
    # Only those with QPS activity in the last hour or yesterday.
    return [d for d in pm
            if (d.get("qpsPreviousHour") or 0) > 0
            or (d.get("qpsYesterday")    or 0) > 0]


def _fetch_publishers_for_demand(demand_id: int, token: str) -> list[dict]:
    """The actual routing primitive — returns publishers wired to demand."""
    r = requests.get(f"{LL_MGMT_BASE}/v1/demands/{demand_id}/publishers",
                     headers=_headers(token), timeout=20)
    if not r.ok:
        return []
    body = r.json()
    data = body.get("body", body) if isinstance(body, dict) else body
    return data if isinstance(data, list) else []


def _bridge_map(cur) -> dict[str, dict]:
    """publisher_id → {key, seat, name}"""
    cur.execute("""SELECT ll_publisher_id, publisher_key, seller_id, ll_publisher_name
                   FROM pgam_direct.compliance_ll_partner_bridge""")
    return {r[0]: {"key": r[1], "seat": r[2], "name": r[3]}
            for r in cur.fetchall()}


def _audit_hosts_for_pub(cur, ll_pub_id: str, expected_partner: str) -> list[dict]:
    cur.execute("""
        SELECT entity_value, audit_host,
               supply_partner_line_present,
               pgam_line_present_for_path,
               publisher_declared_in_partner_sj,
               partner_sellers_json_seller_type,
               observed_pgam_seats,
               revenue_7d
        FROM pgam_direct.compliance_entity_supply_path_audit
        WHERE ll_publisher_id = %s
          AND supply_partner_key = %s
          AND as_of = (SELECT MAX(as_of) FROM pgam_direct.compliance_entity_supply_path_audit)
        ORDER BY revenue_7d DESC NULLS LAST
    """, (ll_pub_id, expected_partner))
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _infer_partner_from_name(name: str) -> str | None:
    n = (name or "").lower()
    if "bidmachine" in n or "blasto" in n: return "bidmachine.io"
    if "start.io" in n or "startio" in n or "start_io" in n: return "start.io"
    return None


def _build_report(*, force: bool = False) -> dict:
    started = datetime.now(timezone.utc)
    token = get_token()

    # 1. Live PubMatic demands
    live_demands = _fetch_live_pubmatic_demands()
    if not live_demands:
        msg = (
            f":zzz: *PubMatic — no live activity today*\n"
            f"No PubMatic demands have bid traffic right now (0 demands with QPS > 0). "
            f"Nothing live to audit. The chain-of-custody framework is monitoring; "
            f"a live report will follow as soon as traffic resumes."
        )
        _post_slack(msg)
        return {"ok": True, "live_demands": 0, "live_publishers": 0,
                "compliant_hosts": 0, "noncompliant_hosts": 0}

    # 2. For each live demand: which publishers serve its traffic?
    live_pubs_by_id: dict[int, dict] = {}  # ll_pub_id → publisher dict
    pub_to_demand_ids: dict[int, set]  = defaultdict(set)
    for d in live_demands:
        for p in _fetch_publishers_for_demand(d["id"], token):
            pid = p.get("id")
            if not pid: continue
            live_pubs_by_id.setdefault(pid, p)
            pub_to_demand_ids[pid].add(d["id"])

    # 3. Layer D check on every live demand
    n_d_pass = sum(1 for d in live_demands
                   if d.get("supplyChainEnabled", True)
                   and not d.get("dontAddSupplyChainNode"))
    n_d_fail = len(live_demands) - n_d_pass
    d_failures = [d for d in live_demands
                  if not (d.get("supplyChainEnabled", True)
                          and not d.get("dontAddSupplyChainNode"))]

    # 4. Per-publisher Layer A/B/C audit against the latest snapshot
    with connect() as c, c.cursor() as cur:
        bridge = _bridge_map(cur)

        per_partner_pass: dict[str, int] = defaultdict(int)
        per_partner_fail: dict[str, int] = defaultdict(int)
        unbridged_pubs: list[tuple[int, str]] = []
        all_failed_hosts: list[dict] = []  # for top-N table

        per_pub_summary: list[dict] = []
        for pid in sorted(live_pubs_by_id.keys()):
            p = live_pubs_by_id[pid]
            br = bridge.get(str(pid))
            if br:
                partner = br["key"]
            else:
                partner = _infer_partner_from_name(p.get("name", "")) or "?unbridged"
                unbridged_pubs.append((pid, p.get("name", "")))

            rows = _audit_hosts_for_pub(cur, str(pid),
                                        partner if partner != "?unbridged" else "")
            by_host = defaultdict(list)
            for r in rows: by_host[r["audit_host"]].append(r)
            n_pass = 0
            n_fail = 0
            failures_here = []
            for host, hr in by_host.items():
                h0 = hr[0]
                la = bool(h0["supply_partner_line_present"])
                lb = bool(h0["pgam_line_present_for_path"])
                lc = h0["publisher_declared_in_partner_sj"]
                ok = la and lb and (lc is not False)
                if ok: n_pass += 1
                else:
                    n_fail += 1
                    failures_here.append({
                        "ll_pub":   pid, "pub_name": p.get("name"),
                        "partner":  partner, "host": host,
                        "A": la, "B": lb, "C": lc,
                        "rev_7d": sum(float(x["revenue_7d"] or 0) for x in hr),
                    })
            per_partner_pass[partner] += n_pass
            per_partner_fail[partner] += n_fail
            all_failed_hosts.extend(failures_here)
            per_pub_summary.append({
                "ll_pub": pid, "pub_name": p.get("name"),
                "partner": partner, "demands_carried": len(pub_to_demand_ids[pid]),
                "hosts_audited": len(by_host), "pass": n_pass, "fail": n_fail,
            })

    # 5. Compose Slack message
    lines: list[str] = []
    lines.append(":eye: *PubMatic — live compliance audit*  "
                 f"_(read-only, no enforcement)_")
    lines.append("")
    lines.append(f"• Live demands: *{len(live_demands)}* with QPS > 0")
    lines.append(f"• Live publishers carrying traffic: *{len(live_pubs_by_id)}*"
                 f" (across {sum(len(s) for s in [unbridged_pubs])} unbridged)"
                 if unbridged_pubs else
                 f"• Live publishers carrying traffic: *{len(live_pubs_by_id)}*")
    total_pass = sum(per_partner_pass.values())
    total_fail = sum(per_partner_fail.values())
    lines.append(f"• Audit hosts: *{total_pass + total_fail}* "
                 f"(compliant: *{total_pass}*, with ≥1 gap: *{total_fail}*)")

    # Layer D (critical)
    if n_d_fail == 0:
        lines.append(f"• :white_check_mark: *Layer D (schain config)*: "
                     f"all {len(live_demands)} live demands pass")
    else:
        lines.append(f"• :rotating_light: *Layer D (schain config) — CRITICAL — "
                     f"{n_d_fail} of {len(live_demands)} demands FAIL*")
        for d in d_failures[:5]:
            lines.append(f"    • demand {d['id']} '{d['name']}': "
                         f"supplyChainEnabled={d.get('supplyChainEnabled')} "
                         f"dontAddSupplyChainNode={d.get('dontAddSupplyChainNode')}")

    # Per-partner roll-up (Layer A/B/C transparency)
    lines.append("")
    lines.append("*Transparency gaps (Layers A+B+C) by supply partner:*")
    lines.append("```")
    lines.append(f"{'partner':<18}{'compliant hosts':>18}{'gaps':>10}{'failure rate':>16}")
    for partner in sorted(per_partner_pass.keys() | per_partner_fail.keys()):
        p, f = per_partner_pass[partner], per_partner_fail[partner]
        rate = f"{(100.0 * f / (p + f)):>5.0f}%" if (p + f) else "    -"
        lines.append(f"{partner:<18}{p:>18}{f:>10}{rate:>16}")
    lines.append("```")

    # Top failures by 7d revenue
    if all_failed_hosts:
        all_failed_hosts.sort(key=lambda x: -x["rev_7d"])
        lines.append("")
        lines.append(f"*Top {min(len(all_failed_hosts), TOP_FAILURES_TO_SHOW)} "
                     f"transparency gaps by 7d revenue:*")
        lines.append("```")
        lines.append(f"{'host':<30}{'partner':<16}{'A':<3}{'B':<3}{'C':<3}{'rev_7d':>10}")
        for f in all_failed_hosts[:TOP_FAILURES_TO_SHOW]:
            lcd = "Y" if f["C"] is True else "N" if f["C"] is False else "-"
            lines.append(f"{f['host'][:28]:<30}{f['partner'][:14]:<16}"
                         f"{'Y' if f['A'] else 'N':<3}"
                         f"{'Y' if f['B'] else 'N':<3}"
                         f"{lcd:<3}"
                         f"${f['rev_7d']:>8,.0f}")
        lines.append("```")

    # Monitoring blind spots
    if unbridged_pubs:
        lines.append("")
        lines.append(":warning: *Monitoring blind spot — unbridged LL publishers on live PubMatic:*")
        for pid, pname in unbridged_pubs:
            inferred = _infer_partner_from_name(pname) or "(unknown)"
            lines.append(f"    • pub {pid} '{pname}'  — name suggests {inferred}, "
                         f"but no `compliance_ll_partner_bridge` row → chain audit can't evaluate")

    lines.append("")
    lines.append("_The full daily compliance digest (all supply partners + every demand) "
                 "was posted separately. This message focuses on PubMatic only._")

    msg = "\n".join(lines)

    # 6. Dedup + send
    with connect() as c, c.cursor() as cur:
        if not force and _already_sent_today(cur):
            print(f"[{ACTOR}] already sent today — skipping")
            return {"ok": True, "skipped": "already_sent_today"}
        _post_slack(msg)
        _mark_sent_today(cur)
        c.commit()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return {
        "ok": True,
        "live_demands":           len(live_demands),
        "live_publishers":        len(live_pubs_by_id),
        "compliant_hosts":        total_pass,
        "noncompliant_hosts":     total_fail,
        "layer_d_pass":           n_d_pass,
        "layer_d_fail":           n_d_fail,
        "unbridged_publishers":   len(unbridged_pubs),
        "elapsed_sec":            elapsed,
    }


def run() -> dict:
    """Cron entry point. Idempotent — safe to fire twice on the same day."""
    return _build_report(force=False)


if __name__ == "__main__":
    # CLI usage: --force ignores the day-dedup (useful for re-running today)
    import sys
    force = "--force" in sys.argv
    print(json.dumps(_build_report(force=force), indent=2, default=str))
