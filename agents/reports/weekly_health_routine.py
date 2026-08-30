"""
agents/reports/weekly_health_routine.py

Weekly health digest — Sunday 06:00 ET, posted to #pubops Slack.

Purpose
-------
A single scannable digest that ties together the optimization + compliance
signals scattered across daily agents into one weekly view aimed at the
publisher-operations team (Sagar + others). Read-only by design:
recommends, never executes. Actions are for humans to review + apply in
LL UI on Monday morning.

Sections
--------
OPTIMIZATION (revenue-side)
  1a. Week-over-week partner scorecard (top gainers + decliners)
  1b. At-cap demands with opportunity cost
  1c. Wiring gap opportunities (top 10 from demand_gap.json)
  1d. New-partner activation health (watchlist)

COMPLIANCE (risk-side)
  2a. HUMAN Mediaguard/Post-Bid consumption vs 150B / 90M monthly budget
  2b. Frozen/blocked partner status (Unruly freeze, QPS blocklist, Pubmatic)
  2c. LL UI drift check (config changes without ledger entries this week)

Safety
------
- Zero writes. Every "recommendation" is a Slack line, not an API call.
- Idempotent — running it twice on the same day just re-posts the same digest.
- Delivery gated by SUNDAY_ET check; direct-invocation for testing bypasses.

Config
------
- PUBOPS_SLACK_WEBHOOK env var (falls back to SLACK_WEBHOOK if unset).
- HUMAN_MEDIAGUARD_BUDGET (default 150_000_000_000)
- HUMAN_POSTBID_BUDGET (default 90_000_000)
"""
from __future__ import annotations

import json
import os
import requests
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core import ll_report, ll_mgmt, floor_ledger, slack, partner_freeze
from agents.optimization.config_health_scanner import (
    QPS_DEMAND_NAME_BLOCKLIST,
    QPS_PARTNER_ID_BLOCKLIST,
    _qps_demand_blocked,
)

DATA_DIR = Path(__file__).parent.parent.parent / "data"
GAPS_PATH = DATA_DIR / "demand_gaps.json"

HUMAN_MEDIAGUARD_BUDGET = int(os.environ.get("HUMAN_MEDIAGUARD_BUDGET", 150_000_000_000))
HUMAN_POSTBID_BUDGET = int(os.environ.get("HUMAN_POSTBID_BUDGET", 90_000_000))

# Partners we treat as "new / watchlist" for activation health
NEW_PARTNER_WATCHLIST = {
    8:  "33Across",
    36: "Zmaticoo-CTV",
    46: "Robust Apps",
    47: "Criteo",
    48: "PubFusion",
}

# Canonical partner-id → name (best-effort; fills in unknown as "dp=N")
PARTNER_NAMES = {
    3: "Pubmatic", 4: "Magnite", 5: "Unruly", 7: "Sovrn", 8: "33Across",
    10: "OneTag", 13: "Xandr", 17: "Illumin", 18: "Verve", 22: "LoopMe",
    25: "Sharethrough-Blitz", 26: "Synatix", 27: "Cas.ai", 31: "Zeta",
    32: "Basis", 33: "TruBid", 36: "Zmaticoo-CTV", 38: "Adnimation",
    39: "Blasto", 40: "BidMachine", 41: "Xandr-Blitz",
    42: "TripleLift-Blitz", 43: "AdElement", 44: "Undertone", 45: "Nexxen",
    46: "Robust Apps", 47: "Criteo", 48: "PubFusion",
}


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------

def _pubops_webhook() -> str | None:
    """Return the #pubops webhook if set, else fall back to SLACK_WEBHOOK."""
    return os.environ.get("PUBOPS_SLACK_WEBHOOK") or os.environ.get("SLACK_WEBHOOK")


def _post_to_pubops(text: str) -> bool:
    """POST plain-text or markdown to the #pubops channel. Returns True on success."""
    hook = _pubops_webhook()
    if not hook:
        print("[weekly_health_routine] No webhook configured — skipping post.")
        return False
    try:
        resp = requests.post(
            hook, json={"text": text},
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[weekly_health_routine] Slack post failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _pull_daily_by_partner(start: date, end: date) -> tuple[dict, dict, dict]:
    """Return (daily_by_partner, daily_by_did, did_to_dp).

    daily_by_partner: {dp_id: {date: revenue}}
    daily_by_did:     {did: {date: revenue}}
    did_to_dp:        {demand_id: partner_id}
    """
    rows = ll_report.report(
        dimensions=["DATE", "DEMAND_ID"],
        metrics=["GROSS_REVENUE", "BID_REQUESTS", "BIDS", "IMPRESSIONS"],
        start_date=start.isoformat(), end_date=end.isoformat(),
    )
    start_iso, end_iso = start.isoformat(), end.isoformat()

    demands_resp = ll_mgmt._get("/v1/demands")
    items = demands_resp.get("items", demands_resp) if isinstance(demands_resp, dict) else demands_resp
    did_to_dp = {d["id"]: d.get("demandPartner") for d in items}

    daily_partner = defaultdict(lambda: defaultdict(float))
    daily_did = defaultdict(lambda: defaultdict(float))
    for r in rows:
        d = str(r.get("DATE", ""))
        if not (start_iso <= d <= end_iso):
            continue
        did = int(r.get("DEMAND_ID", 0) or 0)
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        dp = did_to_dp.get(did)
        if dp is not None:
            daily_partner[dp][d] += rev
        daily_did[did][d] += rev
    return dict(daily_partner), dict(daily_did), did_to_dp


def _sum(series: dict, days: list[str]) -> float:
    return sum(series.get(d, 0) for d in days)


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _section_1a_partner_scorecard(daily_partner: dict, this_week: list[str],
                                   prior_week: list[str]) -> str:
    """Top gainers + decliners week-over-week."""
    movers = []
    for dp, series in daily_partner.items():
        this_w = _sum(series, this_week)
        prior_w = _sum(series, prior_week)
        if this_w < 100 and prior_w < 100:
            continue
        delta = this_w - prior_w
        pct = (delta / prior_w * 100) if prior_w > 0 else (999 if this_w > 0 else 0)
        movers.append((PARTNER_NAMES.get(dp, f"dp={dp}"), dp, prior_w, this_w, delta, pct))

    gainers = sorted(movers, key=lambda x: -x[4])[:3]
    decliners = sorted(movers, key=lambda x: x[4])[:3]

    lines = ["*1a. Week-over-week partner movers*"]
    total_this = sum(m[3] for m in movers)
    total_prior = sum(m[2] for m in movers)
    net = total_this - total_prior
    lines.append(f"   Network total: ${total_prior:,.0f} → ${total_this:,.0f}  "
                 f"({'📈' if net >= 0 else '📉'} ${net:+,.0f}, "
                 f"{(net/total_prior*100 if total_prior else 0):+.0f}%)")
    lines.append("   ")
    lines.append("   🟢 Gainers:")
    for name, dp, p, l, d, pct in gainers:
        lines.append(f"      • {name}: ${p:,.0f} → ${l:,.0f} (+${d:,.0f} / {pct:+.0f}%)")
    lines.append("   🔴 Decliners:")
    for name, dp, p, l, d, pct in decliners:
        lines.append(f"      • {name}: ${p:,.0f} → ${l:,.0f} (${d:+,.0f} / {pct:+.0f}%)")
    return "\n".join(lines)


def _section_1b_at_cap(items: list, daily_did: dict, this_week: list[str]) -> str:
    """Demands at ≥90% QPS util AND earning ≥$50 last 7d.
    Split into 'safe to auto-raise' vs 'blocked / opportunity cost'.
    """
    safe = []
    blocked = []
    for d in items:
        if d.get("status") != 1:
            continue
        cap = d.get("qpsLimit") or 0
        qy = d.get("qpsYesterday") or 0
        rev = _sum(daily_did.get(d["id"], {}), this_week)
        if cap <= 0 or qy < 0.9 * cap or rev < 50:
            continue
        util = qy / cap * 100
        row = (d["id"], d.get("name", "")[:40], cap, qy, util, rev)
        if partner_freeze.is_frozen(demand_id=d["id"]) or \
           _qps_demand_blocked(d.get("name", ""), d.get("demandPartner")):
            blocked.append(row)
        else:
            safe.append(row)

    lines = ["*1b. Demands at cap*"]
    lines.append(f"   Auto-raise candidates for Monday scanner: {len(safe)}")
    for did, name, cap, qy, util, rev in sorted(safe, key=lambda x: -x[5])[:5]:
        lines.append(f"      • d={did}: cap={cap:,} @ {util:.0f}%, ${rev:,.0f}/wk — will auto-2x → {cap*2:,}")
    if not safe:
        lines.append("      (none — either already sized or nothing at cap)")

    blocked_total_rev = sum(r[5] for r in blocked)
    lines.append(f"   Blocked (protected/frozen) at cap — opportunity cost ${blocked_total_rev:,.0f}/wk:")
    for did, name, cap, qy, util, rev in sorted(blocked, key=lambda x: -x[5])[:5]:
        lines.append(f"      • d={did} {name}: {util:.0f}% util, ${rev:,.0f}/wk  🔒")
    if not blocked:
        lines.append("      (none)")
    return "\n".join(lines)


def _section_1c_wiring_gaps() -> str:
    """Top wiring opportunities from data/demand_gaps.json."""
    lines = ["*1c. Wiring-gap opportunities for Sagar to add in LL UI*"]
    if not GAPS_PATH.exists():
        lines.append("   ⚠ demand_gaps.json missing — demand_gap agent hasn't run this week.")
        return "\n".join(lines)
    try:
        data = json.loads(GAPS_PATH.read_text())
    except Exception:
        lines.append("   ⚠ demand_gaps.json unreadable.")
        return "\n".join(lines)
    gaps = data.get("gaps", []) or []
    if not gaps:
        lines.append("   (no gaps identified)")
        return "\n".join(lines)
    top = sorted(gaps, key=lambda g: -float(g.get("est_lift_30d", 0) or 0))[:10]
    lines.append(f"   Top 10 by est. 30-day lift:")
    for g in top:
        pname = str(g.get("publisher_name", "?"))[:32]
        dname = str(g.get("demand_name", "?"))[:35]
        lift = float(g.get("est_lift_30d", 0) or 0)
        wr = float(g.get("peer_median_win_rate", 0) or 0) * 100
        lines.append(f"      • {pname} × {dname}: ~${lift:,.0f}/30d (peer WR {wr:.1f}%)")
    return "\n".join(lines)


def _section_1d_new_partners(items: list, daily_partner: dict,
                              this_week: list[str], prior_week: list[str]) -> str:
    """Watchlist for newly-activated partners."""
    lines = ["*1d. New-partner activation health*"]
    for dp, name in NEW_PARTNER_WATCHLIST.items():
        p_rev = _sum(daily_partner.get(dp, {}), prior_week)
        t_rev = _sum(daily_partner.get(dp, {}), this_week)
        delta = t_rev - p_rev
        p_dems = [d for d in items if d.get("demandPartner") == dp]
        active = sum(1 for d in p_dems if d.get("status") == 1)
        total_qps_y = sum(int(d.get("qpsYesterday") or 0) for d in p_dems)
        status = "🟢" if delta >= 0 or t_rev >= 500 else "🟡" if t_rev >= 100 else "🔴"
        lines.append(f"   {status} {name}: prev wk ${p_rev:,.0f} → this wk ${t_rev:,.0f} "
                     f"({delta:+,.0f}), {active} active demands, {total_qps_y:,} QPS/y")
    return "\n".join(lines)


def _section_2a_human_budget(this_week: list[str]) -> str:
    """Approximate Mediaguard + Post-Bid consumption vs budget.
    Read-only — no per-pub sample data available, so this is portfolio-level."""
    end = date.today()
    mtd_start = date(end.year, end.month, 1)
    days_elapsed = (end - mtd_start).days + 1
    days_in_month = 30
    rows = ll_report.report(
        dimensions=["DATE"],
        metrics=["BID_REQUESTS", "IMPRESSIONS"],
        start_date=mtd_start.isoformat(), end_date=end.isoformat(),
    )
    reqs = sum(int(r.get("BID_REQUESTS", 0) or 0)
               for r in rows
               if mtd_start.isoformat() <= str(r.get("DATE", "")) <= end.isoformat())
    imps = sum(int(r.get("IMPRESSIONS", 0) or 0)
               for r in rows
               if mtd_start.isoformat() <= str(r.get("DATE", "")) <= end.isoformat())
    proj_reqs = reqs * days_in_month / max(1, days_elapsed)
    proj_imps = imps * days_in_month / max(1, days_elapsed)

    lines = ["*2a. HUMAN budget tracking (LL side only — combine with TB manually)*"]
    lines.append(f"   Mediaguard budget: {HUMAN_MEDIAGUARD_BUDGET:,} requests/mo")
    lines.append(f"     LL MTD reqs:    {reqs:,}")
    lines.append(f"     LL projected:   {proj_reqs:,.0f}  "
                 f"({proj_reqs/HUMAN_MEDIAGUARD_BUDGET*100:.0f}% of budget @ 100% sample)")
    lines.append(f"   Post-Bid budget:   {HUMAN_POSTBID_BUDGET:,} impressions/mo")
    lines.append(f"     LL MTD imps:    {imps:,}")
    lines.append(f"     LL projected:   {proj_imps:,.0f}  "
                 f"({proj_imps/HUMAN_POSTBID_BUDGET*100:.0f}% of budget @ 100% sample)")
    lines.append(f"   ⚠ TB volume not aggregated — combine manually for full picture")
    return "\n".join(lines)


def _section_2b_freeze_status() -> str:
    """Restate current freeze / blocklist state so nothing gets forgotten."""
    lines = ["*2b. Freeze / blocklist status*"]
    lines.append(f"   Fully frozen partners (no writes allowed): "
                 f"{sorted(partner_freeze.FROZEN_PARTNERS)}")
    freeze_names = [PARTNER_NAMES.get(p, f"dp={p}") for p in sorted(partner_freeze.FROZEN_PARTNERS)]
    lines.append(f"     → {', '.join(freeze_names) if freeze_names else 'none'}")
    lines.append(f"   QPS auto-raise blocklist ({len(QPS_DEMAND_NAME_BLOCKLIST)} name tokens + "
                 f"{len(QPS_PARTNER_ID_BLOCKLIST)} partner IDs):")
    lines.append(f"     Name tokens: {', '.join(QPS_DEMAND_NAME_BLOCKLIST)}")
    pid_names = [PARTNER_NAMES.get(p, f"dp={p}") for p in sorted(QPS_PARTNER_ID_BLOCKLIST)]
    lines.append(f"     Partner IDs: {', '.join(pid_names)}")
    return "\n".join(lines)


def _section_2c_ui_drift(this_week: list[str]) -> str:
    """Automation writes in the last 7 days — includes 'nothing changed' as a signal."""
    lines = ["*2c. Automation writes this week (from floor_ledger)*"]
    cutoff = this_week[0]
    recent = [r for r in floor_ledger.read_all() if r.get("ts_utc", "")[:10] >= cutoff]
    if not recent:
        lines.append("   (no automation writes recorded this week)")
        return "\n".join(lines)
    # Group by actor
    by_actor = defaultdict(int)
    for r in recent:
        actor = (r.get("actor", "") or "unknown").split("_2026")[0]  # trim date suffix
        by_actor[actor] += 1
    lines.append(f"   Total ledger entries: {len(recent)}")
    for actor, cnt in sorted(by_actor.items(), key=lambda x: -x[1])[:8]:
        lines.append(f"      • {actor}: {cnt}")
    lines.append(f"   💡 LL UI changes made without ledger entries will not appear here. "
                 f"Ask Sagar / team what they touched manually.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------

def build_digest() -> str:
    """Assemble the full weekly digest as a single markdown blob for Slack."""
    end = date.today()
    this_week = [(end - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    prior_week = [(end - timedelta(days=i)).isoformat() for i in range(13, 6, -1)]

    daily_partner, daily_did, did_to_dp = _pull_daily_by_partner(
        date.fromisoformat(prior_week[0]), end,
    )

    demands_resp = ll_mgmt._get("/v1/demands")
    items = demands_resp.get("items", demands_resp) if isinstance(demands_resp, dict) else demands_resp

    header = (
        f"📊 *PGAM Weekly Health Digest — {end.isoformat()}*\n"
        f"   This week: {this_week[0]} → {this_week[-1]}\n"
        f"   Prior wk:  {prior_week[0]} → {prior_week[-1]}\n"
        f"\n"
        f"_Read-only. Recommendations for Monday-morning review — no changes will be applied automatically._\n"
        f"\n"
        f"━━━ OPTIMIZATION ━━━"
    )
    optimization = "\n\n".join([
        _section_1a_partner_scorecard(daily_partner, this_week, prior_week),
        _section_1b_at_cap(items, daily_did, this_week),
        _section_1c_wiring_gaps(),
        _section_1d_new_partners(items, daily_partner, this_week, prior_week),
    ])

    compliance_header = "\n\n━━━ COMPLIANCE ━━━"
    compliance = "\n\n".join([
        _section_2a_human_budget(this_week),
        _section_2b_freeze_status(),
        _section_2c_ui_drift(this_week),
    ])

    footer = (
        "\n\n━━━━━━━━━━━━━━━━━\n"
        "Questions or want a deeper dive on any item?  Reply in-thread and ping Priyesh."
    )
    return header + "\n\n" + optimization + compliance_header + "\n\n" + compliance + footer


def _is_sunday_6am_et() -> bool:
    """Gate: only fire on Sunday 06:xx ET."""
    try:
        import pytz
        et = datetime.now(pytz.timezone("US/Eastern"))
        return et.weekday() == 6 and et.hour == 6
    except Exception:
        return False


def run(force: bool = False) -> dict:
    """Scheduler entry. Only fires Sundays 06:xx ET unless force=True."""
    if not force and not _is_sunday_6am_et():
        return {"skipped": True, "reason": "not Sunday 06:xx ET"}

    digest = build_digest()
    posted = _post_to_pubops(digest)

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "posted": posted,
        "digest_len_chars": len(digest),
        "target_webhook": "PUBOPS_SLACK_WEBHOOK" if os.environ.get("PUBOPS_SLACK_WEBHOOK")
                          else "SLACK_WEBHOOK (fallback)",
    }


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv or "--print" in sys.argv:
        print(build_digest())
    else:
        print(json.dumps(run(force=True), indent=2, default=str))
