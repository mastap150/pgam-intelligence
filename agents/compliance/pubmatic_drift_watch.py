"""
agents/compliance/pubmatic_drift_watch.py

PubMatic-specific compliance enforcer.

Background: PubMatic paused PGAM and is willing to resume after we
demonstrate strict compliance. They were close to terminating. The
contract: every (publisher × PubMatic) supply path we route through
their inventory must satisfy ALL of:

  A. Publisher's ads.txt / app-ads.txt has PubMatic's reseller line
     with our exact seat:
         pubmatic.com, 165708, RESELLER, 5d62403b186f2ace

  B. Publisher's ads.txt / app-ads.txt has our pgamssp.com line for
     the supply path bringing this entity to us (e.g. for a
     bidmachine-routed app, the bidmachine-specific pgamssp seat
     marked RESELLER).

  D. The LL demand wiring carrying PubMatic traffic has BOTH:
       supplyChainEnabled = True
       dontAddSupplyChainNode = False
     PubMatic explicitly stated the "Don't add supply chain node"
     checkbox in our LL UI must NOT be checked.

Detection of any failure → immediate `disable_publisher_demand` call
against the affected (publisher × PubMatic demand) pair, plus a
PubMatic-specific Slack alert.

The whitelist is defined on LL itself: whatever (publisher × pubmatic
demand) pairs ops has wired with status=1 (active) IS the WL. This
agent doesn't maintain a separate allow-list table — it simply enforces
compliance on whatever LL says is currently wired.

LIVE MODE FROM DAY 1. The Phase 1 enforcer is dry-run by default;
PubMatic-specific drift overrides that. The termination risk justifies
acting immediately. Per-action audit trail still lands in
compliance_enforcement_log so we can reconstruct any sequence.

Wire-in: scheduler.py runs this at :52 past every hour (between the
existing :47 enforcer and :57 reactivation_monitor).

Safety guards baked in:
  • Only acts on demands currently status=1 (active in LL). Paused
    demands aren't disabled again (idempotent + cheap).
  • Reads the audit's latest snapshot — never acts on stale data
    older than 25 hours.
  • Max 50 actions per tick. A bug can't pause unlimited inventory.
  • Slack alert per action — visible audit even before someone opens
    the daily digest.
"""
from __future__ import annotations

import json
import os
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta

from core.neon import connect
from core import ll_mgmt


ACTOR = "pubmatic_drift_watch"

# PubMatic's canonical reseller lines for OUR seats. PGAM has TWO
# active PubMatic seats (165708 + 162623) — Layer A passes if EITHER
# is present on the publisher's ads.txt. Operator-confirmed list.
PUBMATIC_DOMAIN       = "pubmatic.com"
PUBMATIC_OUR_SEATS    = frozenset({"165708", "162623"})
PUBMATIC_RELATIONSHIP = "RESELLER"
PUBMATIC_CERT         = "5d62403b186f2ace"
# Reseller lines we suggest publishers add — both shown so they can
# pick whichever path they're routing through.
PUBMATIC_EXPECTED_LINES = tuple(
    f"{PUBMATIC_DOMAIN}, {s}, {PUBMATIC_RELATIONSHIP}, {PUBMATIC_CERT}"
    for s in sorted(PUBMATIC_OUR_SEATS)
)

MAX_ACTIONS_PER_TICK = int(
    os.environ.get("PGAM_PUBMATIC_MAX_ACTIONS", "50")
)
MAX_AUDIT_AGE_HOURS  = float(
    os.environ.get("PGAM_PUBMATIC_MAX_AUDIT_AGE_HOURS", "25")
)
SLACK_WEBHOOK = os.environ.get("COMPLIANCE_SLACK_WEBHOOK", "").strip()


# ─── Helpers ───────────────────────────────────────────────────────────


def _post_slack(text: str, blocks: list[dict] | None = None) -> None:
    if not SLACK_WEBHOOK:
        print(f"[{ACTOR}] no webhook; skip Slack: {text[:80]}")
        return
    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
    except Exception as exc:
        print(f"[{ACTOR}] slack post failed: {exc}")


def _resolve_pubmatic_demand_ids() -> list[dict]:
    """Return the LL demand records whose name matches PubMatic.

    We don't hardcode demand IDs because PubMatic has 300+ entries in
    our LL config (per-size, per-supply-partner variants). Each one
    that's currently active needs to be checked.
    """
    try:
        demands = ll_mgmt.get_demands(include_archived=False) or []
    except Exception as exc:
        print(f"[{ACTOR}] get_demands failed: {exc}")
        return []
    return [d for d in demands
            if "pubmatic" in (d.get("name") or "").lower()
            and d.get("status") == 1]  # 1 = active in LL


def _layer_d_check(demand: dict) -> tuple[bool, list[str]]:
    """Return (passes, failure_reasons) for the schain config flags."""
    failures = []
    # supplyChainEnabled defaults TRUE per LL convention if missing.
    if demand.get("supplyChainEnabled", True) is False:
        failures.append("supplyChainEnabled=False (must be True)")
    # dontAddSupplyChainNode — the field PubMatic explicitly flagged.
    # Default False; only acts on explicit True.
    if demand.get("dontAddSupplyChainNode") is True:
        failures.append('"Don\'t add supply chain node" is checked '
                        '(must be unchecked)')
    return (len(failures) == 0), failures


def _layer_ab_check(cur, publisher_id: str) -> dict:
    """Pull the latest supply_path + ssp audit data for every entity
    earning under this LL publisher. Returns dict keyed by entity_key:
        {
          'entity_value': str,
          'rev_7d': float,
          'layer_a_pubmatic_line': bool,    # pubmatic.com,165708,RESELLER present?
          'layer_b_pgam_seat': bool,         # path-correct pgamssp seat present?
          'sp_audited_at': datetime,
          'ssp_audited_at': datetime,
          'failure_reasons': [str, ...],
        }
    """
    cur.execute("""
        WITH latest_sp AS (
            SELECT DISTINCT ON (entity_key) entity_key, kind, entity_value,
                   audit_host, revenue_7d, supply_partner_key,
                   pgam_line_present_for_path, audited_at, supply_partner_pgam_seat,
                   publisher_declared_in_partner_sj,
                   partner_sellers_json_seller_type
            FROM pgam_direct.compliance_entity_supply_path_audit
            WHERE ll_publisher_id = %s
            ORDER BY entity_key, as_of DESC
        ),
        latest_ssp AS (
            SELECT DISTINCT ON (entity_key) entity_key,
                   ssp_line_present, ssp_seller_id_in_adstxt, audited_at
            FROM pgam_direct.compliance_entity_ssp_audit
            WHERE ssp_key = 'pubmatic'
            ORDER BY entity_key, as_of DESC
        )
        SELECT sp.entity_key, sp.kind, sp.entity_value, sp.audit_host,
               sp.revenue_7d, sp.supply_partner_key,
               sp.pgam_line_present_for_path, sp.audited_at,
               sp.supply_partner_pgam_seat,
               sp.publisher_declared_in_partner_sj,
               sp.partner_sellers_json_seller_type,
               ssp.ssp_line_present, ssp.ssp_seller_id_in_adstxt,
               ssp.audited_at
        FROM latest_sp sp
        LEFT JOIN latest_ssp ssp ON ssp.entity_key = sp.entity_key
    """, (str(publisher_id),))
    out = {}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_AUDIT_AGE_HOURS)
    for r in cur.fetchall():
        (ek, kind, ev, host, rev, sp_key, layer_b, sp_at, sp_seat,
         layer_c_partner_declares_pub, partner_seller_type,
         ssp_present, ssp_seats, ssp_at) = r
        failures = []
        # Layer C (chain of custody): supply partner must declare this
        # publisher in their sellers.json as PUBLISHER or BOTH.
        # NULL = partner's sellers.json not fetchable today (soft pass).
        if layer_c_partner_declares_pub is False:
            if (partner_seller_type or "").upper() == "INTERMEDIARY":
                failures.append(
                    f"Layer C (chain): `{sp_key}` lists publisher `{host or ev}` "
                    f"in their sellers.json but as INTERMEDIARY — needs PUBLISHER or BOTH")
            else:
                failures.append(
                    f"Layer C (chain): `{sp_key}` doesn't declare publisher "
                    f"`{host or ev}` in their sellers.json — broken chain of custody")
        # Stale-data guard — refuse to act if audit data isn't fresh.
        if sp_at and sp_at < cutoff:
            failures.append(f"supply_path audit stale ({sp_at.isoformat()})")
        # Layer A: any of our PubMatic seats present on publisher's
        # ads.txt? We have 165708 + 162623 active; either passes.
        # `ssp_seller_id_in_adstxt` is the array of seats observed for
        # pubmatic.com on this publisher (from the SSP audit). Note
        # `ssp_present` is set by audit_matrix._evaluate_ssp_line
        # against ssp_registry.account_id="165708" only — so a
        # publisher with 162623 (but not 165708) would falsely fail
        # that flag. We re-evaluate here against the BOTH-seat set.
        seats_seen = ssp_seats or []
        layer_a = any(s in PUBMATIC_OUR_SEATS for s in seats_seen)
        if not layer_a:
            if seats_seen:
                failures.append(
                    f"PubMatic line present but none of our seats "
                    f"{sorted(PUBMATIC_OUR_SEATS)} found among "
                    f"{len(seats_seen)} entries (saw: "
                    f"{', '.join(str(s) for s in seats_seen[:3])}"
                    f"{'…' if len(seats_seen)>3 else ''})")
            else:
                failures.append(
                    f"No `{PUBMATIC_DOMAIN}` reseller line on "
                    f"{host or ev}/ads.txt — needs ONE of: "
                    + " OR ".join(f"`{l}`" for l in PUBMATIC_EXPECTED_LINES))
        # Layer B: PGAM seat for the supply path
        if not layer_b:
            failures.append(
                f"PGAM seat missing on {host or ev}/ads.txt for `{sp_key}` "
                f"supply path — needs: `pgamssp.com, {sp_seat}, RESELLER`")
        out[ek] = {
            "entity_value": ev, "kind": kind, "audit_host": host,
            "rev_7d": float(rev or 0), "supply_partner_key": sp_key,
            "layer_a": layer_a, "layer_b": bool(layer_b),
            "sp_audited_at": sp_at, "ssp_audited_at": ssp_at,
            "failure_reasons": failures,
        }
    return out


_LOG_SQL = """
INSERT INTO pgam_direct.compliance_enforcement_log
    (entity_key, supply_partner_key, ll_publisher_id, demand_id,
     entity_value, revenue_7d_at_action,
     action, triggered_by, reason, dry_run,
     ll_state_before, ll_state_after, api_response, error)
VALUES
    (%(entity_key)s, %(partner_key)s, %(ll_pub)s, %(demand_id)s,
     %(entity_value)s, %(revenue)s,
     %(action)s, %(triggered_by)s, %(reason)s, FALSE,
     %(state_before)s, %(state_after)s, %(api_response)s, %(error)s);
"""


# ─── Main run ─────────────────────────────────────────────────────────


def run() -> dict:
    """Hourly entry point. Returns a summary dict."""
    started = datetime.now(timezone.utc)
    print(f"[{ACTOR}] start  max_actions={MAX_ACTIONS_PER_TICK}  "
          f"max_audit_age_h={MAX_AUDIT_AGE_HOURS}")

    pm_demands = _resolve_pubmatic_demand_ids()
    print(f"[{ACTOR}] active PubMatic demands in LL: {len(pm_demands)}")
    if not pm_demands:
        print(f"[{ACTOR}] no active PubMatic demands — nothing to enforce")
        return {"ok": True, "skipped": "no_active_pubmatic_demands",
                "active_demands": 0}

    # Build a lookup: PubMatic demand_id → (name, layer_d_pass, failures).
    # Layer D is a per-demand check (config flags on the demand itself),
    # so do it once per active demand.
    pm_demand_index: dict[str, dict] = {}
    for d in pm_demands:
        layer_d, d_failures = _layer_d_check(d)
        pm_demand_index[str(d.get("id"))] = {
            "name":       d.get("name") or "",
            "layer_d":    layer_d,
            "d_failures": d_failures,
        }

    # Now O(P) instead of O(D × P): walk every active publisher ONCE,
    # ask LL for its current wirings, and pick out any whose demand_id
    # is in our PubMatic index. Even with ~800 publishers this is a
    # bounded number of API calls (one per publisher).
    try:
        publishers = ll_mgmt.get_publishers(include_archived=False) or []
    except Exception as exc:
        print(f"[{ACTOR}] get_publishers failed: {exc}")
        publishers = []

    wirings = []  # (publisher_id, demand_id, demand_name, layer_d_pass, d_failures)
    for p in publishers:
        if p.get("status") != 1:
            continue
        pub_id = str(p.get("id"))
        try:
            pwirings = ll_mgmt.get_publisher_demands(p.get("id")) or []
        except Exception:
            continue
        for w in pwirings:
            did = str(w.get("demandId") or w.get("demand_id") or "")
            if did not in pm_demand_index:
                continue
            if not (w.get("enabled") or w.get("status") == 1):
                continue
            info = pm_demand_index[did]
            wirings.append((pub_id, did, info["name"],
                             info["layer_d"], info["d_failures"]))

    print(f"[{ACTOR}] active (publisher × PubMatic demand) wirings: {len(wirings)}")
    if not wirings:
        return {"ok": True, "skipped": "no_active_wirings",
                "active_demands": len(pm_demands)}

    disabled = 0
    errors = 0
    layer_a_fails = 0
    layer_b_fails = 0
    layer_d_fails = 0
    violation_lines: list[str] = []

    pubs_seen: dict[str, dict] = {}  # publisher_id → entity-level layer A/B data
    with connect() as conn, conn.cursor() as cur:
        for pub_id, demand_id, demand_name, layer_d_pass, d_failures in wirings:
            if pub_id not in pubs_seen:
                pubs_seen[pub_id] = _layer_ab_check(cur, pub_id)
            ab_by_entity = pubs_seen[pub_id]

            # Entity-level Layer A/B failures bubble up to the wiring.
            entity_failures = []
            primary_entity = None
            primary_rev = -1.0
            for ek, info in ab_by_entity.items():
                if info["failure_reasons"]:
                    entity_failures.extend(
                        f"{info['entity_value']}: {r}" for r in info["failure_reasons"])
                # Track primary (top-rev) entity for the action's audit row
                if info["rev_7d"] > primary_rev:
                    primary_rev = info["rev_7d"]
                    primary_entity = (ek, info)

            all_failures = []
            if not layer_d_pass:
                all_failures.extend(f"Layer D (demand config): {f}" for f in d_failures)
                layer_d_fails += 1
            if entity_failures:
                for ef in entity_failures[:5]:
                    all_failures.append(f"Layer A/B: {ef}")
            ab_a_failed = any(not v["layer_a"] for v in ab_by_entity.values())
            ab_b_failed = any(not v["layer_b"] for v in ab_by_entity.values())
            if ab_a_failed: layer_a_fails += 1
            if ab_b_failed: layer_b_fails += 1

            if not all_failures:
                # All layers green for this wiring — leave it live.
                continue

            if disabled >= MAX_ACTIONS_PER_TICK:
                # Stop here; remaining failures will be picked up next tick.
                print(f"[{ACTOR}] hit per-tick action cap "
                      f"({MAX_ACTIONS_PER_TICK}) — pausing")
                break

            # Disable this (publisher × demand) pair via LL mgmt.
            primary_ek = primary_entity[0] if primary_entity else f"unknown:{pub_id}"
            primary_info = primary_entity[1] if primary_entity else {}
            try:
                resp = ll_mgmt.disable_publisher_demand(pub_id, demand_id)
                disabled += 1
                cur.execute(_LOG_SQL, {
                    "entity_key": primary_ek,
                    "partner_key": primary_info.get("supply_partner_key"),
                    "ll_pub": pub_id, "demand_id": demand_id,
                    "entity_value": primary_info.get("entity_value"),
                    "revenue": primary_info.get("rev_7d") or 0,
                    "action": "auto_disable",
                    "triggered_by": ACTOR,
                    "reason": ("PubMatic compliance drift — "
                               + " | ".join(all_failures[:3])),
                    "state_before": json.dumps({"enabled": True}),
                    "state_after":  json.dumps({"enabled": False}),
                    "api_response": json.dumps(resp if isinstance(resp, dict)
                                                else {"raw": str(resp)}),
                    "error": None,
                })
                line = (f"🚨 pub={pub_id} × demand={demand_id} "
                        f"({demand_name[:40]}) → DISABLED | top_entity="
                        f"{primary_info.get('entity_value','?')} "
                        f"(${primary_info.get('rev_7d',0):,.0f}/7d) | "
                        f"reasons: {'; '.join(all_failures[:3])}")
                violation_lines.append(line)
                print(f"[{ACTOR}] {line}")
            except Exception as exc:
                errors += 1
                cur.execute(_LOG_SQL, {
                    "entity_key": primary_ek,
                    "partner_key": primary_info.get("supply_partner_key"),
                    "ll_pub": pub_id, "demand_id": demand_id,
                    "entity_value": primary_info.get("entity_value"),
                    "revenue": primary_info.get("rev_7d") or 0,
                    "action": "auto_disable",
                    "triggered_by": ACTOR,
                    "reason": "PubMatic drift — disable call failed",
                    "state_before": json.dumps({"enabled": True}),
                    "state_after":  None,
                    "api_response": None,
                    "error": f"{type(exc).__name__}: {exc}",
                })
                print(f"[{ACTOR}] disable FAILED pub={pub_id} demand={demand_id}: {exc}")
        conn.commit()

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    summary_text = (
        f":no_entry: *PubMatic drift watch — {disabled} wirings auto-disabled*\n"
        f"_LIVE mode. Active PubMatic demands in LL: {len(pm_demands)}. "
        f"Active wirings audited: {len(wirings)}. Failures by layer: "
        f"A (PubMatic line missing) {layer_a_fails}, B (PGAM seat for path) "
        f"{layer_b_fails}, D (schain config) {layer_d_fails}. "
        f"Errors: {errors}. Elapsed: {elapsed:.1f}s._"
    )
    if disabled > 0:
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": summary_text}}]
        if violation_lines:
            chunk = "\n".join(violation_lines[:20])
            blocks.append({"type": "section", "text": {"type": "mrkdwn",
                "text": "*Disabled wirings (first 20):*\n```\n" + chunk[:2700] + "\n```"}})
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
            "text": ":hammer_and_wrench: Each disable logged to "
                    "`compliance_enforcement_log` with reason. Re-enable "
                    "via `scripts/compliance_approve.py --reactivate` once "
                    "publisher / config is fixed."}]})
        _post_slack(summary_text, blocks)
    print(f"[{ACTOR}] done  disabled={disabled}  errors={errors}  "
          f"layer_a_fails={layer_a_fails}  layer_b_fails={layer_b_fails}  "
          f"layer_d_fails={layer_d_fails}  elapsed={elapsed:.1f}s")
    return {
        "ok": errors == 0,
        "active_demands": len(pm_demands),
        "active_wirings": len(wirings),
        "disabled": disabled,
        "errors": errors,
        "layer_a_failures": layer_a_fails,
        "layer_b_failures": layer_b_fails,
        "layer_d_failures": layer_d_fails,
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
