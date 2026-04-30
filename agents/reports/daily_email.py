"""
agents/reports/daily_email.py
──────────────────────────────────────────────────────────────────────────────
Daily HTML email report for PGAM Intelligence.

Sends once per day at ~7 AM ET via SendGrid.  Aggregates data from:
  • core API (revenue pacing, floor gaps, opp/fill)
  • agents/reports/floor_elasticity  → get_optimization_data()
  • agents/alerts/ctv_optimizer      → export_ctv_section()
  • intelligence/claude_analyst      → synthesize_daily_brief()

Deduped via /tmp/pgam_email_state.json (date-keyed, same pattern as slack.py).
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Lazy imports – keep startup fast and failures isolated
# ---------------------------------------------------------------------------

def _core():
    from core.api import fetch, yesterday, today, n_days_ago, sf, pct, fmt_usd, fmt_n
    from core.config import (
        SENDGRID_KEY, SENDER_EMAIL, RECIPIENTS,
        THRESHOLDS,
    )
    return fetch, yesterday, today, n_days_ago, sf, pct, fmt_usd, fmt_n, \
           SENDGRID_KEY, SENDER_EMAIL, RECIPIENTS, THRESHOLDS


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_FILE   = Path("/tmp/pgam_email_state.json")
ET           = pytz.timezone("America/New_York")
SEND_HOUR_ET = 7          # Send at or after 7 AM ET

# API breakdown / metric strings
BD_PUBLISHER   = "PUBLISHER"
BD_DATE        = "DATE"
BD_DATE_PUB    = "DATE,PUBLISHER"
BD_BUNDLE      = "BUNDLE"
METRICS_REV    = "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,OPPORTUNITIES"
METRICS_FLOOR  = "GROSS_REVENUE,BIDS,WINS,OPPORTUNITIES,AVG_FLOOR_PRICE,AVG_BID_PRICE"


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _already_sent(date_str: str) -> bool:
    if not STATE_FILE.exists():
        return False
    try:
        data = json.loads(STATE_FILE.read_text())
        return data.get("sent_date") == date_str
    except Exception:
        return False


def _mark_sent(date_str: str) -> None:
    try:
        STATE_FILE.write_text(json.dumps({"sent_date": date_str}))
    except Exception as exc:
        print(f"[daily_email] State write failed: {exc}")


# ---------------------------------------------------------------------------
# Data collection helpers
# ---------------------------------------------------------------------------

def _collect_topline(yesterday_fn, n_days_ago_fn) -> dict:
    """
    Cross-platform supply rollup for YESTERDAY (full day) — LL + TB + Combined.

    Compares yesterday vs:
      - Day-before-yesterday  (DoD)
      - 7-day daily average ending day-before-yesterday (clean WoW baseline)

    Also returns top movers (per-publisher revenue delta vs the 7d-avg
    baseline) tagged with their platform.
    """
    from core import ll_data, tb_data

    yest    = yesterday_fn()
    d2_ago  = n_days_ago_fn(2)
    d8_ago  = n_days_ago_fn(8)   # baseline range start (8 days ago through 2 days ago = 7 days)

    # ── LL ────────────────────────────────────────────────────────────────────
    ll_yest    = ll_data.fetch_summary(yest, yest)
    ll_d2      = ll_data.fetch_summary(d2_ago, d2_ago)
    ll_7d_sum  = ll_data.fetch_summary(d8_ago, d2_ago)
    ll_7d_avg  = ll_data.avg_per_day(ll_7d_sum, 7)

    ll_pubs_yest = ll_data.fetch_top_publishers(yest, yest, n=30)
    ll_pubs_7d   = ll_data.fetch_top_publishers(d8_ago, d2_ago, n=80)

    # ── TB ────────────────────────────────────────────────────────────────────
    # TB API times out on long ranges + has shorter history than LL, so we use
    # per-day summaries for the 7d window and skip the 7d publisher breakdown.
    # Movers fall back to DoD baseline (day-before-yesterday) when 7d is empty.
    tb_yest = tb_data.fetch_summary(yest, yest)
    tb_data.sleep_between()
    tb_d2   = tb_data.fetch_summary(d2_ago, d2_ago)
    tb_data.sleep_between()
    tb_7d_sum = tb_data.fetch_summary_by_day(d8_ago, d2_ago)
    tb_data.sleep_between()
    tb_7d_avg = tb_data.avg_per_day(tb_7d_sum, 7)

    tb_pubs_yest = tb_data.fetch_top_publishers(yest, yest, n=30)
    tb_data.sleep_between()
    tb_pubs_d2   = tb_data.fetch_top_publishers(d2_ago, d2_ago, n=80)

    # ── Combined totals ───────────────────────────────────────────────────────
    combined_yest = _combine(ll_yest, tb_yest)
    combined_d2   = _combine(ll_d2,   tb_d2)
    combined_7d   = _combine(ll_7d_avg, tb_7d_avg)

    # ── Movers ────────────────────────────────────────────────────────────────
    # LL uses 7d-avg baseline; TB uses DoD (day-before) baseline since 7d
    # publisher fetches time out and TB has limited history. If the TB baseline
    # fetch comes back empty we skip TB movers entirely rather than fabricate
    # a "NEW" tag for every TB publisher.
    ll_movers = _compute_movers(ll_pubs_yest, ll_pubs_7d, "LL",
                                baseline_label="7d avg", baseline_divisor=7)
    tb_movers = (_compute_movers(tb_pubs_yest, tb_pubs_d2, "TB",
                                  baseline_label="day-before", baseline_divisor=1)
                 if tb_pubs_d2 else [])
    movers = ll_movers + tb_movers
    movers.sort(key=lambda m: abs(m["delta"]), reverse=True)

    return {
        "date":     yest,
        "ll":       {"yest": ll_yest, "d2": ll_d2, "d7avg": ll_7d_avg},
        "tb":       {"yest": tb_yest, "d2": tb_d2, "d7avg": tb_7d_avg},
        "combined": {"yest": combined_yest, "d2": combined_d2, "d7avg": combined_7d},
        "movers":   movers[:6],
        # Raw publisher rankings — exposed so health checks (concentration,
        # dead inventory) can reuse them without re-fetching.
        "_pubs": {
            "ll_yest": ll_pubs_yest, "ll_7d":   ll_pubs_7d,
            "tb_yest": tb_pubs_yest, "tb_d2":   tb_pubs_d2,
        },
    }


def _combine(a: dict, b: dict) -> dict:
    """Sum two summary dicts and recompute derived ratios."""
    if not a and not b:
        return {}
    a = a or {}
    b = b or {}
    rev  = a.get("revenue", 0)     + b.get("revenue", 0)
    pay  = a.get("payout", 0)      + b.get("payout", 0)
    imp  = a.get("impressions", 0) + b.get("impressions", 0)
    wins = a.get("wins", 0)        + b.get("wins", 0)
    bids = a.get("bids", 0)        + b.get("bids", 0)
    return {
        "revenue": rev, "payout": pay, "impressions": imp, "wins": wins, "bids": bids,
        "margin":   ((rev - pay) / rev * 100) if rev > 0 else 0.0,
        "ecpm":     (rev / imp * 1000) if imp > 0 else 0.0,
        "win_rate": (wins / bids * 100) if bids > 0 else 0.0,
    }


def _compute_movers(yest_pubs: list, baseline_pubs: list, platform: str,
                    baseline_label: str = "7d avg",
                    baseline_divisor: int = 7,
                    min_abs_delta: float = 50.0) -> list[dict]:
    """
    For each pub in yest, compute revenue delta vs a baseline.

    baseline_divisor: how many days the baseline_pubs revenue sums cover
                      (7 for a 7-day range, 1 for a single-day DoD baseline).
    Pubs with no baseline history are returned with delta_pct=None and tagged
    as "new" rather than fabricating a meaningless +0% / +inf%.
    """
    base_by_name = {p["name"]: p["revenue"] / baseline_divisor for p in baseline_pubs}
    movers = []
    for p in yest_pubs:
        baseline = base_by_name.get(p["name"], 0.0)
        delta    = p["revenue"] - baseline
        if abs(delta) < min_abs_delta:
            continue
        is_new   = baseline <= 0
        delta_pct = None if is_new else ((p["revenue"] - baseline) / baseline * 100)
        movers.append({
            "platform":       platform,
            "publisher":      p["name"],
            "yest_rev":       p["revenue"],
            "baseline":       baseline,
            "baseline_label": baseline_label,
            "delta":          delta,
            "delta_pct":      delta_pct,
            "is_new":         is_new,
        })
    return movers


def _delta_pct(now: float, base: float) -> float | None:
    if base is None or base == 0:
        return None
    return (now - base) / base * 100.0


def _collect_today_actions(dead_ll: list, losers: list,
                            max_actions: int = 7) -> list[dict]:
    """
    Build prioritized action list by reading the recommendation snapshots
    written by the optimizer agents, plus live signals from the topline
    health computation.

    Output shape (per action):
        {severity: HIGH|MED|LOW, category: str, title: str, context: str,
         source: str}
    """
    from pathlib import Path

    actions: list[dict] = []

    def _load(path: str) -> dict | list | None:
        try:
            with open(path) as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None

    # 1. Partner churn radar — the highest-priority signal: pubs collapsing
    churn = _load("logs/partner_churn_recs.json") or {}
    for alert in (churn.get("alerts") or [])[:5]:
        delta = alert.get("rev_delta_pct", 0)
        if delta > -30:
            continue
        sev  = "HIGH" if delta <= -50 else "MED"
        actions.append({
            "severity": sev,
            "category": "INVESTIGATE",
            "title":    f"Investigate {alert.get('publisher','?')} (rev {delta:+.0f}%)",
            "context":  f"${alert.get('prior_rev',0):,.0f}/d → ${alert.get('cur_rev',0):,.0f}/d · imps {alert.get('imp_delta_pct',0):+.0f}%",
            "source":   "partner_churn",
            "_sort_key": abs(delta),
        })

    # 2. Dead / silent inventory (already computed for the health card —
    #    surface as actions because each one is a real ops item)
    for d in dead_ll[:3]:
        actions.append({
            "severity": "HIGH",
            "category": "PAUSE/CHECK",
            "title":    f"Check {d['name']} — {d['drop_pct']:.0f}% drop",
            "context":  f"7d baseline ${d['baseline']:,.0f}/d · yest ${d['yest_rev']:,.0f} (likely outage or wiring break)",
            "source":   "dead_inventory",
            "_sort_key": abs(d['drop_pct']),
        })

    # 3. Demand concentration risk — pubs over-dependent on single DSP
    conc = _load("logs/demand_concentration_recs.json") or {}
    for alert in (conc.get("alerts") or [])[:4]:
        top1_pct = alert.get("top1_pct", 0)
        if top1_pct < 70:
            continue
        sev = "HIGH" if top1_pct >= 80 else "MED"
        actions.append({
            "severity": sev,
            "category": "DIVERSIFY",
            "title":    f"Diversify {alert.get('title','?')} demand",
            "context":  f"{alert.get('top1_dsp','?')} = {top1_pct:.0f}% of ${alert.get('total_revenue',0):,.0f}/d · {alert.get('dsp_count',0)} DSPs wired",
            "source":   "demand_concentration",
            "_sort_key": top1_pct,
        })

    # 4. Yield compression — placements where eCPM dropped meaningfully
    yc = _load("logs/yield_compression_recs.json") or {}
    for alert in (yc.get("alerts") or [])[:3]:
        ecpm_d = alert.get("ecpm_delta_pct", 0)
        if ecpm_d > -15:
            continue
        actions.append({
            "severity": "MED",
            "category": "FLOOR",
            "title":    f"Review floor on placement {alert.get('placement_id','?')}",
            "context":  f"eCPM ${alert.get('prior_ecpm',0):.2f} → ${alert.get('cur_ecpm',0):.2f} ({ecpm_d:+.0f}%) · rev {alert.get('revenue_delta_pct',0):+.0f}%",
            "source":   "yield_compression",
            "_sort_key": abs(ecpm_d),
        })

    # 5. Top losers (cross-platform mover analysis) — annotate as MED actions
    for m in losers[:2]:
        actions.append({
            "severity": "MED",
            "category": "INVESTIGATE",
            "title":    f"Investigate {m['publisher']} drop",
            "context":  f"yest ${m['yest_rev']:,.0f} vs baseline ${m['baseline']:,.0f} (Δ ${m['delta']:+,.0f})",
            "source":   "movers",
            "_sort_key": abs(m['delta']) / 100,  # smaller scale — won't crowd out churn
        })

    # 6. SSP Company optimizer prune candidates — LOW priority cleanup
    ssp_opt = _load("logs/ssp_company_optimizer_recs.json") or {}
    by_class = ssp_opt.get("by_class") or {}
    prune = (by_class.get("PRUNE") or [])[:3]
    for p in prune:
        actions.append({
            "severity": "LOW",
            "category": "PRUNE",
            "title":    f"Prune {p.get('company','?')} (zero/low yield)",
            "context":  f"{p.get('endpoint_count',0)} endpoints · ${p.get('revenue',0):,.0f} over window",
            "source":   "ssp_company_optimizer",
            "_sort_key": 0,
        })

    # Sort: severity rank, then magnitude
    sev_rank = {"HIGH": 0, "MED": 1, "LOW": 2}
    actions.sort(key=lambda a: (sev_rank.get(a["severity"], 9), -a["_sort_key"]))

    # Strip private sort key
    for a in actions:
        a.pop("_sort_key", None)
    return actions[:max_actions]


def _collect_yesterday_outcomes(yesterday_fn, n_days_ago_fn) -> dict:
    """
    Summarize what auto-agents actually did in the last 24h by reading the
    action ledger files. Pairs with Today's Actions to close the loop.
    """
    from datetime import datetime, timezone
    from pathlib import Path

    cutoff = datetime.now(timezone.utc).timestamp() - 86400  # last 24h

    def _ts(rec: dict) -> float:
        ts = rec.get("timestamp") or rec.get("ts") or rec.get("applied_at")
        if not ts:
            return 0.0
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    def _load(path: str) -> list:
        try:
            with open(path) as f:
                d = json.load(f)
            return d if isinstance(d, list) else (d.get("entries") or d.get("actions") or [])
        except (OSError, json.JSONDecodeError):
            return []

    # ── TB floor nudges ──────────────────────────────────────────────────────
    nudges = _load("logs/tb_floor_nudge_actions.json")
    recent_nudges = [n for n in nudges if _ts(n) >= cutoff]
    nudge_applied = sum(1 for n in recent_nudges
                         if not n.get("dry_run") and n.get("action") not in (None, "skip"))
    nudge_proposed = sum(1 for n in recent_nudges if n.get("action") not in (None, "skip"))
    nudge_skipped = sum(1 for n in recent_nudges if n.get("action") == "skip")

    # ── Geo floor updates ────────────────────────────────────────────────────
    geo_actions = _load("logs/geo_floor_actions.json")
    recent_geo = [a for a in geo_actions if _ts(a) >= cutoff]
    geo_applied = sum(1 for a in recent_geo if a.get("applied"))
    geo_proposed = len(recent_geo)

    # ── Domain blocks ────────────────────────────────────────────────────────
    blocks = _load("logs/blocked_domains_actions.json")
    recent_blocks = [b for b in blocks if _ts(b) >= cutoff]
    blocks_applied = sum(1 for b in recent_blocks if b.get("applied"))
    blocks_proposed = len(recent_blocks)

    # ── Placement pauses/enables ─────────────────────────────────────────────
    placement = _load("logs/placement_status_actions.json")
    recent_placement = [p for p in placement if _ts(p) >= cutoff]
    placement_applied = sum(1 for p in recent_placement if p.get("applied"))

    # ── Revenue Guardian floor restores ──────────────────────────────────────
    guardian = _load("logs/guardian_ledger.json")
    recent_guardian = [g for g in guardian if _ts(g) >= cutoff]
    guardian_count = len(recent_guardian)

    return {
        "nudges": {
            "applied":  nudge_applied,
            "proposed": nudge_proposed,
            "skipped":  nudge_skipped,
            "total":    len(recent_nudges),
        },
        "geo_floors":   {"applied": geo_applied,    "proposed": geo_proposed},
        "domain_blocks":{"applied": blocks_applied, "proposed": blocks_proposed},
        "placement":    {"applied": placement_applied, "total": len(recent_placement)},
        "guardian":     {"count": guardian_count},
    }


def _collect_geo(yesterday_fn, n_days_ago_fn, top_n: int = 5) -> dict:
    """
    Top countries by revenue for LL (TB doesn't track country reliably).
    Yesterday + 7d-avg baseline for WoW comparison.
    """
    from core import ll_data, tb_data

    yest   = yesterday_fn()
    d2_ago = n_days_ago_fn(2)
    d8_ago = n_days_ago_fn(8)

    ll_yest = ll_data.fetch_by_country(yest, yest, n=20)
    ll_7d   = ll_data.fetch_by_country(d8_ago, d2_ago, n=20)

    base_by_country = {c["country"]: c["revenue"] / 7.0 for c in ll_7d}
    out_ll = []
    for c in ll_yest[:top_n]:
        baseline = base_by_country.get(c["country"], 0.0)
        delta_pct = ((c["revenue"] - baseline) / baseline * 100) if baseline > 0 else None
        out_ll.append({**c, "baseline": baseline, "delta_pct": delta_pct})

    # TB: try country, but fall back gracefully if it's all "Unknown"
    tb_yest = tb_data.fetch_by_country(yest, yest, n=20)
    tb_data.sleep_between()
    tb_meaningful = [c for c in tb_yest if c["country"] not in ("Unknown", "", "ZZ")]
    out_tb = tb_meaningful[:top_n] if tb_meaningful else []

    return {"date": yest, "ll": out_ll, "tb": out_tb}


def _collect_demand_margin(yesterday_fn, n_days_ago_fn, top_n: int = 8) -> dict:
    """Demand-partner profitability ranking, LL + TB. Sort by revenue, surface margin."""
    from core import ll_data, tb_data

    yest   = yesterday_fn()
    d8_ago = n_days_ago_fn(8)
    d2_ago = n_days_ago_fn(2)

    ll_yest = ll_data.fetch_by_demand_partner(yest, yest, n=30)
    ll_7d   = ll_data.fetch_by_demand_partner(d8_ago, d2_ago, n=30)
    base_ll = {d["demand"]: d["margin"] for d in ll_7d}
    for d in ll_yest:
        d["margin_7d"] = base_ll.get(d["demand"])
        d["margin_delta_pp"] = ((d["margin"] - d["margin_7d"])
                                 if d["margin_7d"] is not None else None)

    tb_data.sleep_between()
    tb_yest = tb_data.fetch_by_demand_partner(yest, yest, n=30)
    tb_data.sleep_between()
    tb_7d   = tb_data.fetch_by_demand_partner(d8_ago, d2_ago, n=30)
    base_tb = {d["demand"]: d["margin"] for d in tb_7d}
    for d in tb_yest:
        d["margin_7d"] = base_tb.get(d["demand"])
        d["margin_delta_pp"] = ((d["margin"] - d["margin_7d"])
                                 if d["margin_7d"] is not None else None)

    return {
        "date": yest,
        "ll":   ll_yest[:top_n],
        "tb":   tb_yest[:top_n],
    }


def _compute_concentration(topline: dict) -> dict:
    """Top-3 publisher revenue share, with a flag at >= 70%."""
    if not topline:
        return {}
    return {
        "ll": _conc_for_platform(topline.get("ll", {})),
        "tb": _conc_for_platform(topline.get("tb", {})),
    }


def _conc_for_platform(plat: dict) -> dict:
    """
    Compute concentration from the platform's yest summary alone — actual
    publisher list comes from movers + we re-fetch via existing top-pubs path
    embedded in topline. Returns approximation when only summary is present.
    """
    yest = plat.get("yest") or {}
    return {
        "total_revenue": yest.get("revenue", 0.0),
        "platform_margin": yest.get("margin", 0.0),
    }


def _compute_dead_inventory(yest_pubs: list, baseline_pubs: list,
                             min_baseline: float = 100.0,
                             max_yest_ratio: float = 0.10) -> list[dict]:
    """
    Pubs that delivered >= min_baseline daily-avg over the last 7d but <= 10%
    of that yesterday — likely outage, partner removal, or wiring break.
    """
    yest_by_name = {p["name"]: p["revenue"] for p in yest_pubs}
    dead = []
    for p in baseline_pubs:
        baseline = p["revenue"] / 7.0
        if baseline < min_baseline:
            continue
        yest_rev = yest_by_name.get(p["name"], 0.0)
        if yest_rev <= baseline * max_yest_ratio:
            dead.append({
                "name":       p["name"],
                "baseline":   baseline,
                "yest_rev":   yest_rev,
                "drop_pct":   ((yest_rev - baseline) / baseline * 100) if baseline > 0 else 0,
            })
    dead.sort(key=lambda x: x["baseline"], reverse=True)
    return dead


def _split_movers(movers: list, n_each: int = 3) -> tuple[list, list]:
    """Split mixed mover list into top gainers and worst drops."""
    if not movers:
        return [], []
    gainers = sorted([m for m in movers if m["delta"] > 0],
                     key=lambda m: -m["delta"])[:n_each]
    losers  = sorted([m for m in movers if m["delta"] < 0],
                     key=lambda m: m["delta"])[:n_each]
    return gainers, losers


def _compute_pub_concentration(yest_pubs: list, top_n: int = 3) -> dict:
    """Top-N publisher share of total platform revenue."""
    if not yest_pubs:
        return {"top_n": top_n, "share_pct": 0.0, "total": 0.0,
                "top_pubs": [], "flag": False}
    total = sum(p["revenue"] for p in yest_pubs)
    top   = sorted(yest_pubs, key=lambda p: -p["revenue"])[:top_n]
    top_rev = sum(p["revenue"] for p in top)
    share = (top_rev / total * 100) if total > 0 else 0.0
    return {
        "top_n":     top_n,
        "share_pct": share,
        "total":     total,
        "top_pubs":  [{"name": p["name"], "revenue": p["revenue"],
                        "share_pct": (p["revenue"]/total*100) if total > 0 else 0}
                       for p in top],
        "flag":      share >= 70.0,
    }


def _collect_top_combos(yesterday_fn, top_pubs_ll: list[str],
                        n_pubs: int = 5, n_combos_per_pub: int = 3) -> dict:
    """
    Per-publisher top (bundle × demand) combos for LL, plus a flat
    (publisher × demand) leaderboard for TB.

    "Working well" = combo eCPM > publisher's average eCPM, ranked by revenue.
    Falls back to top-by-revenue (no quality filter) if no combos pass for
    a given pub, so the publisher row is never empty.
    """
    from core import ll_data, tb_data

    yest = yesterday_fn()
    out = {"date": yest, "ll": [], "tb": []}

    # ── LL: BUNDLE × PUBLISHER × DEMAND_PARTNER ───────────────────────────────
    combos = ll_data.fetch_bundle_pub_demand(yest, yest)
    if combos:
        # Group by publisher
        by_pub: dict[str, list[dict]] = {}
        for c in combos:
            by_pub.setdefault(c["publisher"], []).append(c)

        # Restrict to top N pubs by total revenue (use the topline-derived
        # ranking if provided, otherwise compute from these combos)
        if top_pubs_ll:
            pubs_ordered = [p for p in top_pubs_ll if p in by_pub][:n_pubs]
            # Fill from combo-derived ranking if topline list was short
            if len(pubs_ordered) < n_pubs:
                seen = set(pubs_ordered)
                extra = sorted(by_pub.keys(),
                               key=lambda p: -sum(c["revenue"] for c in by_pub[p]))
                for p in extra:
                    if p not in seen:
                        pubs_ordered.append(p)
                        if len(pubs_ordered) >= n_pubs:
                            break
        else:
            pubs_ordered = sorted(by_pub.keys(),
                                  key=lambda p: -sum(c["revenue"] for c in by_pub[p]))[:n_pubs]

        for pub in pubs_ordered:
            pub_combos = by_pub[pub]
            total_rev  = sum(c["revenue"] for c in pub_combos)
            total_imp  = sum(c["impressions"] for c in pub_combos)
            pub_avg_ecpm = (total_rev / total_imp * 1000) if total_imp > 0 else 0.0

            quality = [c for c in pub_combos if c["ecpm"] > pub_avg_ecpm]
            ranked  = sorted(quality or pub_combos, key=lambda c: -c["revenue"])[:n_combos_per_pub]

            out["ll"].append({
                "publisher":     pub,
                "total_revenue": total_rev,
                "avg_ecpm":      pub_avg_ecpm,
                "filtered":      bool(quality),  # False => fell back to raw top-revenue
                "combos":        ranked,
            })

    # ── TB: PUBLISHER × DEMAND_PARTNER (no bundle granularity available) ─────
    tb_combos = tb_data.fetch_pub_demand_combos(yest, yest)
    if tb_combos:
        # Compute platform-wide avg eCPM as quality threshold
        total_rev = sum(c["revenue"] for c in tb_combos)
        total_imp = sum(c["impressions"] for c in tb_combos)
        platform_avg_ecpm = (total_rev / total_imp * 1000) if total_imp > 0 else 0.0
        quality = [c for c in tb_combos if c["ecpm"] > platform_avg_ecpm]
        ranked  = sorted(quality or tb_combos, key=lambda c: -c["revenue"])[:n_pubs * n_combos_per_pub]
        out["tb"] = {
            "platform_avg_ecpm": platform_avg_ecpm,
            "filtered":          bool(quality),
            "combos":            ranked,
        }

    return out


def _collect_revenue_summary(fetch, yesterday_fn, today_fn, n_days_ago_fn,
                              sf, pct, fmt_usd, fmt_n) -> dict:
    """Fetch today + yesterday publisher-level data and build a summary dict."""
    yest  = yesterday_fn()
    tod   = today_fn()
    w7ago = n_days_ago_fn(7)

    try:
        rows_today  = fetch(BD_PUBLISHER, METRICS_REV, tod,  tod)
        rows_yest   = fetch(BD_PUBLISHER, METRICS_REV, yest, yest)
        rows_7d     = fetch(BD_PUBLISHER, METRICS_REV, w7ago, yest)
    except Exception as exc:
        print(f"[daily_email] Revenue fetch failed: {exc}")
        return {}

    def _sum(rows: list, field: str) -> float:
        return sum(sf(r.get(field, 0)) for r in rows)

    rev_today  = _sum(rows_today, "GROSS_REVENUE")
    rev_yest   = _sum(rows_yest,  "GROSS_REVENUE")
    imps_today = _sum(rows_today, "IMPRESSIONS")
    imps_yest  = _sum(rows_yest,  "IMPRESSIONS")
    bids_today = _sum(rows_today, "BIDS")
    wins_today = _sum(rows_today, "WINS")
    rev_7d     = _sum(rows_7d,    "GROSS_REVENUE")

    now_et  = datetime.now(ET)
    hour_et = now_et.hour + now_et.minute / 60.0
    exp_rev = rev_yest * (max(hour_et, 1) / 24.0) if rev_yest > 0 else 0.0
    pacing  = (rev_today / exp_rev * 100.0) if exp_rev > 0 else None

    return {
        "date":           tod,
        "revenue_today":  round(rev_today, 2),
        "revenue_yest":   round(rev_yest, 2),
        "expected_rev":   round(exp_rev, 2),
        "pacing_pct":     round(pacing, 1) if pacing is not None else None,
        "impressions_today": int(imps_today),
        "impressions_yest":  int(imps_yest),
        "win_rate_pct":   round(pct(wins_today, bids_today), 1),
        "revenue_7d_avg": round(rev_7d / 7.0, 2) if rev_7d else 0.0,
        "publisher_count": len({r.get("PUBLISHER_NAME", r.get("publisher","")) for r in rows_today if r.get("PUBLISHER_NAME") or r.get("publisher")}),
    }


def _collect_floor_gaps(fetch, yesterday_fn, sf) -> dict:
    """Collect top raise / lower floor gap candidates for the report."""
    yest = yesterday_fn()
    try:
        rows = fetch("PUBLISHER", METRICS_FLOOR, yest, yest)
    except Exception as exc:
        print(f"[daily_email] Floor gap fetch failed: {exc}")
        return {"raise": [], "lower": []}

    raise_cands = []
    lower_cands = []
    for r in rows:
        bids      = sf(r.get("BIDS",            0))
        wins      = sf(r.get("WINS",            0))
        revenue   = sf(r.get("GROSS_REVENUE",   0))
        avg_floor = sf(r.get("AVG_FLOOR_PRICE", 0))
        avg_bid   = sf(r.get("AVG_BID_PRICE",   0))
        pub       = r.get("PUBLISHER_NAME") or r.get("publisher", "Unknown")

        if bids < 5_000 or avg_floor <= 0 or avg_bid <= 0:
            continue

        ratio = avg_bid / avg_floor
        if ratio >= 2.0:
            raise_cands.append({
                "publisher":    pub,
                "avg_floor":    round(avg_floor, 3),
                "avg_bid":      round(avg_bid, 3),
                "recommended":  round(avg_bid, 3),
                "revenue":      round(revenue, 2),
                "ratio":        round(ratio, 2),
            })
        elif ratio <= 0.5:
            lower_cands.append({
                "publisher":    pub,
                "avg_floor":    round(avg_floor, 3),
                "avg_bid":      round(avg_bid, 3),
                "recommended":  round(avg_bid * 1.1, 3),
                "revenue":      round(revenue, 2),
                "ratio":        round(ratio, 2),
            })

    raise_cands.sort(key=lambda x: x["revenue"], reverse=True)
    lower_cands.sort(key=lambda x: x["revenue"], reverse=True)
    return {"raise": raise_cands[:5], "lower": lower_cands[:5]}


def _collect_opp_fill(fetch, today_fn, n_days_ago_fn, sf, pct) -> dict:
    """Fetch MTD opportunity / fill rate metrics."""
    tod        = today_fn()
    month_start = tod[:8] + "01"

    try:
        rows = fetch(BD_DATE, METRICS_REV, month_start, tod)
    except Exception as exc:
        print(f"[daily_email] Opp/fill fetch failed: {exc}")
        return {}

    def _sum(field: str) -> float:
        return sum(sf(r.get(field, 0)) for r in rows)

    opps = _sum("OPPORTUNITIES")
    imps = _sum("IMPRESSIONS")
    rev  = _sum("GROSS_REVENUE")

    fill_rate = imps / opps if opps > 0 else 0.0
    threshold = 0.0005

    return {
        "mtd_opportunities":  int(opps),
        "mtd_impressions":    int(imps),
        "mtd_revenue":        round(rev, 2),
        "fill_rate":          round(fill_rate, 6),
        "fill_rate_pct":      round(fill_rate * 100, 4),
        "threshold_pct":      threshold * 100,
        "above_threshold":    fill_rate >= threshold,
        "imps_needed":        max(0, int(opps * threshold - imps)),
    }


# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------

# Colour palette — light theme.
# Email clients (especially Gmail in dark mode) aggressively override or
# invert dark themes, leaving body text invisible. A light theme renders
# consistently across Gmail/Outlook/Apple Mail/mobile.
_BG      = "#f3f4f6"   # page background (light grey)
_CARD    = "#ffffff"   # card background (white)
_BORDER  = "#e5e7eb"   # card border
_TEXT    = "#111827"   # primary text (near-black)
_MUTED   = "#6b7280"   # secondary text (grey)
_GREEN   = "#16a34a"   # success / gainer
_RED     = "#dc2626"   # warning / loser
_YELLOW  = "#d97706"   # caution
_BLUE    = "#2563eb"   # info / LL platform
_PURPLE  = "#7c3aed"   # exec brief accent
_HEADER_BG_FROM = "#1e293b"  # dark header gradient (still dark — lots of contrast w/ white below)
_HEADER_BG_TO   = "#0f172a"
_TABLE_DIVIDER  = "#f1f5f9"


def _css() -> str:
    # Note: every cell-level element (td, strong, .metric .value) gets an
    # explicit color so Gmail dark-mode overrides can't strip the inherited
    # color: from body and leave text invisible on white.
    return f"""
    body {{
        margin: 0; padding: 0; background: {_BG};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
        color: {_TEXT}; font-size: 14px; line-height: 1.6;
    }}
    .wrapper {{ max-width: 700px; margin: 0 auto; padding: 24px 16px; }}
    .header {{
        background: linear-gradient(135deg, {_HEADER_BG_FROM} 0%, {_HEADER_BG_TO} 100%);
        border-radius: 12px;
        padding: 28px 32px; margin-bottom: 20px;
    }}
    .header h1 {{ margin: 0 0 4px; font-size: 22px; font-weight: 700; color: #ffffff; }}
    .header .sub {{ color: #cbd5e1; font-size: 13px; margin: 0; }}
    .card {{
        background: {_CARD}; border: 1px solid {_BORDER};
        border-radius: 10px; padding: 20px 24px; margin-bottom: 16px;
        color: {_TEXT};
    }}
    .card h2 {{
        margin: 0 0 16px; font-size: 13px; font-weight: 700;
        color: {_MUTED}; text-transform: uppercase; letter-spacing: 0.06em;
    }}
    .metric-grid {{
        display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;
        margin-bottom: 12px;
    }}
    .metric {{ background: {_BG}; border-radius: 8px; padding: 14px 16px; }}
    .metric .label {{ font-size: 11px; color: {_MUTED}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }}
    .metric .value {{ font-size: 20px; font-weight: 700; color: {_TEXT}; }}
    .metric .change {{ font-size: 12px; margin-top: 2px; color: {_MUTED}; }}
    .green {{ color: {_GREEN}; }}
    .red   {{ color: {_RED}; }}
    .yellow {{ color: {_YELLOW}; }}
    .blue  {{ color: {_BLUE}; }}
    .purple {{ color: {_PURPLE}; }}
    .muted {{ color: {_MUTED}; }}
    strong {{ color: {_TEXT}; }}
    .progress-bar-bg {{
        background: {_BG}; border-radius: 4px; height: 8px;
        overflow: hidden; margin: 8px 0 4px;
    }}
    .progress-bar-fill {{ height: 100%; border-radius: 4px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; color: {_TEXT}; }}
    th {{
        text-align: left; color: {_MUTED}; font-weight: 600;
        font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em;
        padding: 0 0 8px; border-bottom: 1px solid {_BORDER};
    }}
    td {{ padding: 8px 0; border-bottom: 1px solid {_TABLE_DIVIDER}; color: {_TEXT}; }}
    tr:last-child td {{ border-bottom: none; }}
    .badge {{
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 600; text-transform: uppercase;
    }}
    .badge-green  {{ background: #dcfce7; color: #15803d; }}
    .badge-red    {{ background: #fee2e2; color: #b91c1c; }}
    .badge-yellow {{ background: #fef3c7; color: #b45309; }}
    .badge-blue   {{ background: #dbeafe; color: #1d4ed8; }}
    .brief-para {{ color: {_TEXT}; margin: 0 0 14px; line-height: 1.7; }}
    .brief-para:last-child {{ margin: 0; }}
    .footer {{ color: {_MUTED}; font-size: 11px; text-align: center; padding-top: 12px; }}
    """


def _pacing_color(pct: float | None) -> str:
    if pct is None:
        return _MUTED
    if pct >= 90:
        return _GREEN
    if pct >= 70:
        return _YELLOW
    return _RED


def _pacing_badge(pct: float | None) -> str:
    if pct is None:
        return '<span class="badge badge-yellow">N/A</span>'
    if pct >= 90:
        return '<span class="badge badge-green">On Track</span>'
    if pct >= 70:
        return '<span class="badge badge-yellow">Caution</span>'
    return '<span class="badge badge-red">Behind</span>'


def _html_header(date_str: str, now_et: datetime) -> str:
    ts = now_et.strftime("%I:%M %p ET")
    return f"""
    <div class="header">
      <h1>PGAM Intelligence — Daily Report</h1>
      <p class="sub">{date_str} &nbsp;·&nbsp; Generated {ts}</p>
    </div>
    """


def _html_today_actions_section(actions: list[dict]) -> str:
    """Top-of-email action list — what to do today, prioritized by severity."""
    if not actions:
        return f"""
        <div class="card" style="border-color:{_GREEN};border-width:1px 1px 1px 4px;">
          <h2 style="color:{_GREEN};">Today's Actions</h2>
          <p class="muted" style="margin:0;">All clear — no high-severity items detected from yesterday's data.</p>
        </div>
        """

    sev_styles = {
        "HIGH": ("badge-red",    "🔴"),
        "MED":  ("badge-yellow", "🟡"),
        "LOW":  ("badge-blue",   "🔵"),
    }

    rows = ""
    for a in actions:
        badge_cls, icon = sev_styles.get(a["severity"], ("badge-blue", "•"))
        rows += f"""
        <tr>
          <td style="width:80px;vertical-align:top;padding-right:12px;">
            <span class="badge {badge_cls}">{a['severity']}</span>
          </td>
          <td style="vertical-align:top;">
            <div><strong>{a['title']}</strong></div>
            <div class="muted" style="font-size:12px;margin-top:2px;">{a['context']}</div>
          </td>
          <td class="muted" style="vertical-align:top;font-size:11px;text-align:right;white-space:nowrap;">
            {a['category']}
          </td>
        </tr>"""

    n_high = sum(1 for a in actions if a["severity"] == "HIGH")
    n_med  = sum(1 for a in actions if a["severity"] == "MED")
    summary_line = f"{n_high} high · {n_med} medium · {len(actions)} total" if n_high or n_med else f"{len(actions)} items"

    return f"""
    <div class="card" style="border-color:{_RED if n_high else _YELLOW};border-width:1px 1px 1px 4px;">
      <h2 style="color:{_TEXT};">Today's Actions <span class="muted" style="text-transform:none;letter-spacing:normal;font-weight:400;font-size:12px;">· {summary_line}</span></h2>
      <table>
        <tbody>{rows}</tbody>
      </table>
    </div>
    """


def _html_outcomes_section(outcomes: dict) -> str:
    """Yesterday's auto-agent activity — closes the loop on automation."""
    if not outcomes:
        return ""

    n   = outcomes.get("nudges", {})
    g   = outcomes.get("geo_floors", {})
    b   = outcomes.get("domain_blocks", {})
    p   = outcomes.get("placement", {})
    gu  = outcomes.get("guardian", {})

    total_activity = (n.get("total", 0) + g.get("proposed", 0) + b.get("proposed", 0)
                       + p.get("total", 0) + gu.get("count", 0))
    if total_activity == 0:
        return f"""
        <div class="card">
          <h2>Yesterday's Outcomes (last 24h)</h2>
          <p class="muted" style="margin:0;">No auto-agent activity recorded.</p>
        </div>
        """

    def _stat(label: str, applied: int, proposed: int = None, color: str = None) -> str:
        color = color or _TEXT
        if proposed is not None and proposed != applied:
            value_html = (f'<span style="color:{color};">{applied}</span>'
                          f'<span class="muted" style="font-size:13px;"> / {proposed}</span>')
            sub = "applied / proposed"
        else:
            value_html = f'<span style="color:{color};">{applied}</span>'
            sub = "applied"
        return f"""
        <div class="metric">
          <div class="label">{label}</div>
          <div class="value">{value_html}</div>
          <div class="change">{sub}</div>
        </div>"""

    grid = (
        _stat("Floor Nudges (TB)", n.get("applied", 0),  n.get("proposed", 0), _BLUE) +
        _stat("Geo Floors",        g.get("applied", 0),  g.get("proposed", 0), _BLUE) +
        _stat("Domain Blocks",     b.get("applied", 0),  b.get("proposed", 0), _BLUE) +
        _stat("Placement Actions", p.get("applied", 0),  p.get("total", 0),    _BLUE) +
        _stat("Guardian Restores", gu.get("count", 0),   None,                  _GREEN)
    )

    nudge_skipped_note = ""
    if n.get("skipped", 0) > 0:
        nudge_skipped_note = (f'<div class="muted" style="font-size:11px;margin-top:8px;">'
                               f'{n["skipped"]} floor nudges skipped (insufficient volume / outside elasticity band)</div>')

    return f"""
    <div class="card">
      <h2>Yesterday's Outcomes (last 24h)</h2>
      <div class="metric-grid" style="grid-template-columns:repeat(5, 1fr);">
        {grid}
      </div>
      {nudge_skipped_note}
    </div>
    """


def _html_topline_section(top: dict, fmt_usd, fmt_n) -> str:
    """Cross-platform LL+TB+Combined supply rollup for yesterday."""
    if not top:
        return ""

    yest_date = top.get("date", "")
    ll  = top.get("ll", {})
    tb  = top.get("tb", {})
    cb  = top.get("combined", {})
    movers = top.get("movers", [])

    def _delta_html(now: float, base: float, kind: str = "pct") -> str:
        d = _delta_pct(now, base)
        if d is None:
            return f'<span class="muted">—</span>'
        cls  = "green" if d >= 0 else "red"
        arr  = "▲" if d >= 0 else "▼"
        sign = "+" if d >= 0 else ""
        return f'<span class="{cls}">{arr} {sign}{d:.1f}%</span>'

    def _row(label: str, plat: dict) -> str:
        y    = plat.get("yest", {}) or {}
        d2   = plat.get("d2", {})   or {}
        d7   = plat.get("d7avg", {}) or {}
        rev  = y.get("revenue", 0)
        imp  = y.get("impressions", 0)
        ecpm = y.get("ecpm", 0)
        marg = y.get("margin", 0)
        return f"""
        <tr>
          <td><strong>{label}</strong></td>
          <td>{fmt_usd(rev)}<div style="font-size:11px;">{_delta_html(rev, d2.get('revenue'))} <span class="muted">DoD</span> · {_delta_html(rev, d7.get('revenue'))} <span class="muted">vs 7d</span></div></td>
          <td>{fmt_n(imp)}<div style="font-size:11px;">{_delta_html(imp, d7.get('impressions'))} <span class="muted">vs 7d</span></div></td>
          <td>{fmt_usd(ecpm)}<div style="font-size:11px;">{_delta_html(ecpm, d7.get('ecpm'))} <span class="muted">vs 7d</span></div></td>
          <td>{marg:.1f}%<div style="font-size:11px;" class="muted">7d: {d7.get('margin', 0):.1f}%</div></td>
        </tr>"""

    has_ll = bool(ll.get("yest"))
    has_tb = bool(tb.get("yest"))
    rows_html = ""
    if has_ll:
        rows_html += _row("LL (Limelight)", ll)
    if has_tb:
        rows_html += _row("TB (Teqblaze)", tb)
    if has_ll and has_tb:
        rows_html += _row("Combined", cb)

    if not rows_html:
        return '<div class="card"><h2>Yesterday — Supply Rollup</h2><p class="muted">No platform data available.</p></div>'

    # Movers block
    movers_html = ""
    if movers:
        m_rows = ""
        for m in movers:
            plat_badge = ('<span class="badge badge-blue">LL</span>'
                          if m["platform"] == "LL" else
                          '<span class="badge badge-yellow">TB</span>')
            delta = m["delta"]
            cls   = "green" if delta >= 0 else "red"
            arr   = "▲" if delta >= 0 else "▼"
            sign  = "+" if delta >= 0 else ""
            if m.get("is_new"):
                baseline_cell = '<span class="badge badge-green">new</span>'
                delta_cell    = f'<span class="{cls}">{arr} {sign}{fmt_usd(delta)}</span>'
            else:
                baseline_cell = f'<span class="muted">{fmt_usd(m["baseline"])}<br><span style="font-size:11px;">{m["baseline_label"]}</span></span>'
                pct_str       = f'({sign}{m["delta_pct"]:.0f}%)' if m["delta_pct"] is not None else ''
                delta_cell    = f'<span class="{cls}">{arr} {sign}{fmt_usd(delta)} <span class="muted" style="font-size:11px;">{pct_str}</span></span>'
            m_rows += f"""
            <tr>
              <td>{plat_badge}</td>
              <td>{m['publisher']}</td>
              <td>{fmt_usd(m['yest_rev'])}</td>
              <td>{baseline_cell}</td>
              <td>{delta_cell}</td>
            </tr>"""
        movers_html = f"""
        <div style="margin-top:18px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;">
            Top Movers vs Baseline
          </div>
          <table>
            <thead><tr>
              <th></th><th>Publisher</th><th>Yesterday</th><th>Baseline</th><th>Delta</th>
            </tr></thead>
            <tbody>{m_rows}</tbody>
          </table>
        </div>"""

    # Movers are now rendered in the dedicated _html_health_section.
    return f"""
    <div class="card" style="border-color:{_BLUE};border-width:1px 1px 1px 4px;">
      <h2>Yesterday — Supply Rollup ({yest_date})</h2>
      <table>
        <thead><tr>
          <th>Platform</th><th>Revenue</th><th>Impressions</th><th>eCPM</th><th>Margin</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def _html_health_section(gainers: list, losers: list, dead: dict,
                          concentration: dict, fmt_usd) -> str:
    """
    Combined health card: top gainers, worst drops, dead inventory, and
    revenue concentration. One scannable card replaces the old movers
    block + adds the operational alerts.
    """
    if not gainers and not losers and not dead.get("ll") and not dead.get("tb") \
       and not concentration.get("ll", {}).get("top_pubs"):
        return ""

    def _mover_row(m: dict) -> str:
        plat_badge = ('<span class="badge badge-blue">LL</span>'
                      if m["platform"] == "LL" else
                      '<span class="badge badge-yellow">TB</span>')
        delta = m["delta"]
        cls   = "green" if delta >= 0 else "red"
        arr   = "▲" if delta >= 0 else "▼"
        sign  = "+" if delta >= 0 else ""
        if m.get("is_new"):
            base_cell = '<span class="badge badge-green">new</span>'
        else:
            base_cell = f'<span class="muted">{fmt_usd(m["baseline"])}</span>'
        pct_str = f' ({sign}{m["delta_pct"]:.0f}%)' if m.get("delta_pct") is not None else ''
        return f"""
        <tr>
          <td>{plat_badge}</td>
          <td>{m['publisher']}</td>
          <td>{fmt_usd(m['yest_rev'])}</td>
          <td>{base_cell}</td>
          <td class="{cls}">{arr} {sign}{fmt_usd(delta)}<span class="muted" style="font-size:11px;">{pct_str}</span></td>
        </tr>"""

    # Gainers + losers tables (side-by-side header)
    movers_html = ""
    if gainers or losers:
        gain_html = ""
        if gainers:
            gain_rows = "".join(_mover_row(m) for m in gainers)
            gain_html = f"""
            <div style="margin-bottom:14px;">
              <div style="font-size:12px;color:{_GREEN};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">
                ▲ Top Gainers
              </div>
              <table>
                <thead><tr><th></th><th>Publisher</th><th>Yest</th><th>Baseline</th><th>Delta</th></tr></thead>
                <tbody>{gain_rows}</tbody>
              </table>
            </div>"""
        loss_html = ""
        if losers:
            loss_rows = "".join(_mover_row(m) for m in losers)
            loss_html = f"""
            <div style="margin-bottom:14px;">
              <div style="font-size:12px;color:{_RED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">
                ▼ Worst Drops
              </div>
              <table>
                <thead><tr><th></th><th>Publisher</th><th>Yest</th><th>Baseline</th><th>Delta</th></tr></thead>
                <tbody>{loss_rows}</tbody>
              </table>
            </div>"""
        movers_html = gain_html + loss_html

    # Dead inventory: pubs with ≥$100/day baseline that delivered ≤10% yesterday
    dead_html = ""
    dead_all = []
    for plat, items in (("LL", dead.get("ll", [])), ("TB", dead.get("tb", []))):
        for d in items:
            dead_all.append({**d, "platform": plat})
    if dead_all:
        dead_rows = ""
        for d in dead_all[:5]:
            badge = ('<span class="badge badge-blue">LL</span>'
                     if d["platform"] == "LL" else
                     '<span class="badge badge-yellow">TB</span>')
            dead_rows += f"""
            <tr>
              <td>{badge}</td>
              <td>{d['name']}</td>
              <td class="muted">{fmt_usd(d['baseline'])}/day</td>
              <td class="red">{fmt_usd(d['yest_rev'])}</td>
              <td class="red">{d['drop_pct']:.0f}%</td>
            </tr>"""
        dead_html = f"""
        <div style="margin-bottom:14px;">
          <div style="font-size:12px;color:{_RED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">
            ⚠ Dead / Silent Inventory  <span class="muted" style="font-size:11px;text-transform:none;letter-spacing:normal;">(≥$100/d baseline, ≤10% yest)</span>
          </div>
          <table>
            <thead><tr><th></th><th>Publisher</th><th>7d Avg</th><th>Yest</th><th>Drop</th></tr></thead>
            <tbody>{dead_rows}</tbody>
          </table>
        </div>"""

    # Concentration risk: top 3 share per platform
    conc_html = ""
    conc_blocks = []
    for plat, plat_label, conc in (("LL", "Limelight", concentration.get("ll", {})),
                                    ("TB", "Teqblaze", concentration.get("tb", {}))):
        if not conc or not conc.get("top_pubs"):
            continue
        share   = conc["share_pct"]
        flag    = conc["flag"]
        flag_badge = '<span class="badge badge-red">HIGH</span>' if flag else \
                     ('<span class="badge badge-yellow">elevated</span>' if share >= 50 else
                      '<span class="badge badge-green">healthy</span>')
        names = " · ".join(f"{p['name']} ({p['share_pct']:.0f}%)" for p in conc["top_pubs"])
        conc_blocks.append(f"""
        <tr>
          <td><strong>{plat_label}</strong></td>
          <td>{share:.1f}%</td>
          <td>{flag_badge}</td>
          <td class="muted" style="font-size:12px;">{names}</td>
        </tr>""")
    if conc_blocks:
        conc_html = f"""
        <div style="margin-bottom:6px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">
            Revenue Concentration  <span style="text-transform:none;letter-spacing:normal;">(top 3 publishers)</span>
          </div>
          <table>
            <thead><tr><th>Platform</th><th>Top-3 Share</th><th>Status</th><th>Top Publishers</th></tr></thead>
            <tbody>{''.join(conc_blocks)}</tbody>
          </table>
        </div>"""

    return f"""
    <div class="card">
      <h2>Movers, Drops & Health</h2>
      {movers_html}
      {dead_html}
      {conc_html}
    </div>
    """


def _html_geo_section(geo: dict, fmt_usd, fmt_n) -> str:
    """Top countries by revenue for LL (TB skipped — no reliable country data)."""
    if not geo or not geo.get("ll"):
        return ""

    def _delta_html(d):
        if d is None:
            return '<span class="muted">—</span>'
        cls  = "green" if d >= 0 else "red"
        arr  = "▲" if d >= 0 else "▼"
        sign = "+" if d >= 0 else ""
        return f'<span class="{cls}">{arr} {sign}{d:.0f}%</span>'

    rows = ""
    for c in geo["ll"]:
        rows += f"""
        <tr>
          <td><strong>{c['country']}</strong></td>
          <td>{fmt_usd(c['revenue'])}</td>
          <td>{fmt_n(c['impressions'])}</td>
          <td>{fmt_usd(c['ecpm'])}</td>
          <td class="muted">{c['margin']:.1f}%</td>
          <td>{_delta_html(c.get('delta_pct'))} <span class="muted" style="font-size:11px;">vs 7d</span></td>
        </tr>"""

    tb_note = ""
    if not geo.get("tb"):
        tb_note = ('<div class="muted" style="font-size:11px;margin-top:8px;">'
                   'TB country breakdown not yet available in the AdX API — '
                   'TB engineering scoping the build.</div>')

    return f"""
    <div class="card">
      <h2>Geographic Breakdown — LL ({geo.get('date','')})</h2>
      <table>
        <thead><tr>
          <th>Country</th><th>Revenue</th><th>Imps</th><th>eCPM</th><th>Margin</th><th>vs 7d Avg</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>
      {tb_note}
    </div>
    """


def _html_demand_margin_section(dm: dict, fmt_usd) -> str:
    """Margin by demand partner — LL + TB. Surfaces partner profitability."""
    if not dm or (not dm.get("ll") and not dm.get("tb")):
        return ""

    def _margin_delta_html(d):
        if d is None:
            return '<span class="muted">—</span>'
        cls  = "green" if d >= 0 else "red"
        sign = "+" if d >= 0 else ""
        return f'<span class="{cls}">{sign}{d:.1f} pp</span>'

    def _table(items: list, plat_label: str, badge_class: str) -> str:
        if not items:
            return ""
        rows = ""
        for d in items:
            margin_color = _GREEN if d["margin"] >= 30 else (_YELLOW if d["margin"] >= 20 else _RED)
            rows += f"""
            <tr>
              <td>{d['demand']}</td>
              <td>{fmt_usd(d['revenue'])}</td>
              <td style="color:{margin_color};">{d['margin']:.1f}%</td>
              <td>{_margin_delta_html(d.get('margin_delta_pp'))}</td>
              <td class="muted">{fmt_usd(d['ecpm'])}</td>
              <td class="muted">{d['win_rate']:.1f}%</td>
            </tr>"""
        return f"""
        <div style="margin-bottom:14px;">
          <div style="margin-bottom:6px;">
            <span class="badge {badge_class}">{plat_label}</span>
          </div>
          <table>
            <thead><tr>
              <th>Demand Partner</th><th>Revenue</th><th>Margin</th><th>vs 7d</th><th>eCPM</th><th>WR</th>
            </tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </div>"""

    body = _table(dm.get("ll", []), "LL", "badge-blue") + \
           _table(dm.get("tb", []), "TB", "badge-yellow")

    tb_note = ""
    if not dm.get("tb"):
        tb_note = ('<div class="muted" style="font-size:11px;margin-top:8px;">'
                   'TB demand-partner breakdown not yet available in the AdX API — '
                   'TB engineering scoping the build.</div>')

    return f"""
    <div class="card">
      <h2>Margin by Demand Partner ({dm.get('date','')})</h2>
      {body}
      {tb_note}
    </div>
    """


def _html_top_combos_section(combos: dict, fmt_usd, fmt_n) -> str:
    """
    Per-publisher top (bundle × demand) combos for LL, plus flat
    (publisher × demand) leaderboard for TB. "Working well" filter:
    eCPM above publisher (or platform) average, ranked by revenue.
    """
    if not combos or (not combos.get("ll") and not combos.get("tb")):
        return ""

    ll_pubs = combos.get("ll", []) or []
    tb_data = combos.get("tb") or {}
    yest_date = combos.get("date", "")

    # ── LL block: one mini-table per publisher ───────────────────────────────
    ll_html = ""
    if ll_pubs:
        pub_blocks = []
        for pub in ll_pubs:
            pub_name   = pub["publisher"]
            avg_ecpm   = pub["avg_ecpm"]
            filtered   = pub["filtered"]
            label_note = '' if filtered else f' <span class="muted" style="font-size:11px;">(top by revenue — none beat avg eCPM)</span>'

            rows_html = ""
            for c in pub["combos"]:
                ecpm_color = _GREEN if c["ecpm"] > avg_ecpm else _MUTED
                rows_html += f"""
                <tr>
                  <td style="font-family:ui-monospace,monospace;font-size:12px;">{c['bundle'][:36]}</td>
                  <td>{c['demand']}</td>
                  <td>{fmt_usd(c['revenue'])}</td>
                  <td style="color:{ecpm_color};">{fmt_usd(c['ecpm'])}</td>
                  <td class="muted">{c['win_rate']:.1f}%</td>
                </tr>"""

            pub_blocks.append(f"""
            <div style="margin-bottom:18px;">
              <div style="margin-bottom:6px;">
                <strong style="color:{_TEXT};">{pub_name}</strong>
                <span class="muted" style="font-size:11px;margin-left:8px;">
                  pub avg eCPM {fmt_usd(avg_ecpm)} · total {fmt_usd(pub['total_revenue'])}
                </span>{label_note}
              </div>
              <table>
                <thead><tr>
                  <th>Bundle / App ID</th><th>Demand</th><th>Revenue</th><th>eCPM</th><th>WR</th>
                </tr></thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>""")

        ll_html = f"""
        <div style="margin-bottom:6px;">
          <span class="badge badge-blue">LL</span>
          <span style="font-size:12px;color:{_MUTED};margin-left:6px;">
            Top {len(ll_pubs)} publishers · top combos by bundle × demand (eCPM &gt; pub avg)
          </span>
        </div>
        {''.join(pub_blocks)}"""

    # ── TB block: flat leaderboard ────────────────────────────────────────────
    tb_html = ""
    if not tb_data or not tb_data.get("combos"):
        # Show a pending note so recipients know TB is intentionally absent here
        tb_html = ('<div style="margin-top:18px;" class="muted" style="font-size:11px;">'
                   '<span class="badge badge-yellow">TB</span> '
                   '&nbsp;Publisher × demand breakdown not yet available in the AdX API — '
                   'TB engineering scoping the build.</div>')
    if tb_data and tb_data.get("combos"):
        plat_avg = tb_data["platform_avg_ecpm"]
        filtered = tb_data["filtered"]
        rows_html = ""
        for c in tb_data["combos"]:
            ecpm_color = _GREEN if c["ecpm"] > plat_avg else _MUTED
            rows_html += f"""
            <tr>
              <td>{c['publisher']}</td>
              <td>{c['demand']}</td>
              <td>{fmt_usd(c['revenue'])}</td>
              <td style="color:{ecpm_color};">{fmt_usd(c['ecpm'])}</td>
              <td class="muted">{c['win_rate']:.1f}%</td>
            </tr>"""
        label_note = '' if filtered else f' <span class="muted" style="font-size:11px;">(top by revenue — none beat platform avg)</span>'
        tb_html = f"""
        <div style="margin-top:18px;">
          <div style="margin-bottom:6px;">
            <span class="badge badge-yellow">TB</span>
            <span style="font-size:12px;color:{_MUTED};margin-left:6px;">
              Top publisher × demand combos · platform avg eCPM {fmt_usd(plat_avg)}
            </span>{label_note}
          </div>
          <table>
            <thead><tr>
              <th>Publisher</th><th>Demand</th><th>Revenue</th><th>eCPM</th><th>WR</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

    if not ll_html and not tb_html:
        return ""

    return f"""
    <div class="card">
      <h2>Top Combos by Publisher ({yest_date})</h2>
      {ll_html}
      {tb_html}
    </div>
    """


def _html_revenue_section(rev: dict, fmt_usd, fmt_n) -> str:
    if not rev:
        return '<div class="card"><h2>Revenue Overview</h2><p class="muted">Data unavailable</p></div>'

    pacing        = rev.get("pacing_pct")
    rev_today     = rev.get("revenue_today", 0)
    rev_yest      = rev.get("revenue_yest", 0)
    exp_rev       = rev.get("expected_rev", 0)
    imps          = rev.get("impressions_today", 0)
    win_rate      = rev.get("win_rate_pct", 0)
    avg_7d        = rev.get("revenue_7d_avg", 0)
    pub_count     = rev.get("publisher_count", 0)

    dod_pct = ((rev_today - rev_yest) / rev_yest * 100) if rev_yest > 0 else None
    bar_pct = min(pacing or 0, 100)
    bar_color = _pacing_color(pacing)

    dod_html = ""
    if dod_pct is not None:
        cls  = "green" if dod_pct >= 0 else "red"
        sign = "+" if dod_pct >= 0 else ""
        dod_html = f'<span class="{cls}">{sign}{dod_pct:.1f}% DoD</span>'

    return f"""
    <div class="card">
      <h2>Revenue Overview</h2>
      <div class="metric-grid">
        <div class="metric">
          <div class="label">Today (so far)</div>
          <div class="value">{fmt_usd(rev_today)}</div>
          <div class="change">{dod_html}</div>
        </div>
        <div class="metric">
          <div class="label">Expected by now</div>
          <div class="value">{fmt_usd(exp_rev)}</div>
          <div class="change muted">Based on yesterday</div>
        </div>
        <div class="metric">
          <div class="label">7-Day Avg</div>
          <div class="value">{fmt_usd(avg_7d)}</div>
          <div class="change muted">Daily average</div>
        </div>
        <div class="metric">
          <div class="label">Impressions</div>
          <div class="value">{fmt_n(imps)}</div>
          <div class="change muted">Today</div>
        </div>
        <div class="metric">
          <div class="label">Win Rate</div>
          <div class="value">{win_rate:.1f}%</div>
          <div class="change muted">Bids → wins</div>
        </div>
        <div class="metric">
          <div class="label">Publishers</div>
          <div class="value">{pub_count}</div>
          <div class="change muted">Active today</div>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;margin-top:4px;">
        <div style="flex:1;">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{bar_pct:.1f}%;background:{bar_color};"></div>
          </div>
          <div style="font-size:12px;color:{_MUTED};">Pacing: {f"{pacing:.1f}" if pacing is not None else "N/A"}% of expected</div>
        </div>
        <div>{_pacing_badge(pacing)}</div>
      </div>
    </div>
    """


def _html_floor_section(floors: dict, fmt_usd) -> str:
    raise_list = floors.get("raise", [])
    lower_list = floors.get("lower", [])

    if not raise_list and not lower_list:
        return '<div class="card"><h2>Floor Price Actions</h2><p class="muted">No floor gap actions needed today.</p></div>'

    def _table(items: list, action: str, color: str) -> str:
        if not items:
            return ""
        action_badge = f'<span class="badge badge-{color}">{action}</span>'
        rows_html = ""
        for r in items:
            rows_html += f"""
            <tr>
              <td>{r['publisher']}</td>
              <td class="muted">{fmt_usd(r['avg_floor'])}</td>
              <td style="color:{_BLUE};">{fmt_usd(r['avg_bid'])}</td>
              <td style="color:{_GREEN if action == 'Raise' else _YELLOW};">{fmt_usd(r['recommended'])}</td>
              <td style="color:{_MUTED};">${r['revenue']:,.2f}</td>
            </tr>"""
        return f"""
        <div style="margin-bottom:16px;">
          <div style="margin-bottom:8px;">{action_badge}</div>
          <table>
            <thead><tr>
              <th>Publisher</th><th>Current Floor</th><th>Avg Bid</th>
              <th>Recommended</th><th>Revenue</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

    body = _table(raise_list, "Raise", "green") + _table(lower_list, "Lower", "yellow")
    return f'<div class="card"><h2>Floor Price Actions</h2>{body}</div>'


def _html_opp_fill_section(opp: dict, fmt_n) -> str:
    if not opp:
        return '<div class="card"><h2>MTD Opportunity Fill Rate</h2><p class="muted">Data unavailable</p></div>'

    fill_pct   = opp.get("fill_rate_pct", 0)
    threshold  = opp.get("threshold_pct", 0.05)
    above      = opp.get("above_threshold", False)
    imps_needed = opp.get("imps_needed", 0)
    mtd_rev    = opp.get("mtd_revenue", 0)
    mtd_opps   = opp.get("mtd_opportunities", 0)
    mtd_imps   = opp.get("mtd_impressions", 0)

    status_badge = ('<span class="badge badge-green">Above Threshold</span>'
                    if above else
                    '<span class="badge badge-red">Below Threshold</span>')
    bar_pct   = min(fill_pct / threshold * 100, 100) if threshold > 0 else 0
    bar_color = _GREEN if above else _RED

    imps_row = ""
    if not above and imps_needed > 0:
        imps_row = f'<p style="font-size:13px;color:{_YELLOW};margin:8px 0 0;">Need {fmt_n(imps_needed)} more impressions to reach threshold.</p>'

    return f"""
    <div class="card">
      <h2>MTD Opportunity Fill Rate</h2>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
        <div style="flex:1;">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{bar_pct:.1f}%;background:{bar_color};"></div>
          </div>
          <div style="font-size:12px;color:{_MUTED};">Fill Rate: {fill_pct:.4f}% (threshold: {threshold:.2f}%)</div>
        </div>
        <div>{status_badge}</div>
      </div>
      <div class="metric-grid">
        <div class="metric"><div class="label">MTD Revenue</div><div class="value">${mtd_rev:,.0f}</div></div>
        <div class="metric"><div class="label">Opportunities</div><div class="value">{fmt_n(mtd_opps)}</div></div>
        <div class="metric"><div class="label">Impressions</div><div class="value">{fmt_n(mtd_imps)}</div></div>
      </div>
      {imps_row}
    </div>
    """


def _html_floor_elasticity_section(opps: list, fmt_usd) -> str:
    if not opps:
        return ""

    rows_html = ""
    for o in opps[:8]:
        pub      = o.get("publisher", "")
        direction = o.get("direction", "")
        cur_floor = o.get("current_floor", 0)
        opt_floor = o.get("optimal_floor", 0)
        uplift    = o.get("daily_rev_uplift", 0)
        conf      = o.get("confidence", 0)
        priority  = o.get("priority", "medium")

        badge_cls = {"high": "badge-red", "medium": "badge-yellow", "low": "badge-blue"}.get(priority, "badge-blue")
        dir_arrow = "↑" if direction == "raise" else "↓"
        uplift_color = _GREEN if uplift >= 0 else _RED

        rows_html += f"""
        <tr>
          <td>{pub}</td>
          <td><span class="badge {badge_cls}">{priority}</span></td>
          <td class="muted">{dir_arrow} {fmt_usd(cur_floor)} → {fmt_usd(opt_floor)}</td>
          <td style="color:{uplift_color};">${abs(uplift):,.2f}/day</td>
          <td class="muted">{conf:.0%}</td>
        </tr>"""

    return f"""
    <div class="card">
      <h2>Floor Elasticity Opportunities</h2>
      <table>
        <thead><tr>
          <th>Publisher</th><th>Priority</th><th>Floor Change</th>
          <th>Est. Daily Uplift</th><th>Confidence</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def _html_ctv_section(ctv: dict, fmt_usd, fmt_n) -> str:
    if not ctv:
        return ""

    summary  = ctv.get("summary", {})
    pubs     = ctv.get("top_publishers", [])
    proj     = ctv.get("projections", {})
    n_pubs   = ctv.get("n_publishers", 0)

    avg_ecpm      = summary.get("avg_ecpm", 0)
    fill_rate     = summary.get("fill_rate", 0)
    avg_daily_rev = summary.get("avg_daily_revenue", 0)
    total_rev     = summary.get("total_revenue", 0)

    pub_rows = ""
    for p in pubs[:5]:
        name       = p.get("publisher", "")
        ecpm       = p.get("ecpm", 0)
        fill       = p.get("fill_rate", 0)
        opp_score  = p.get("opportunity_score", 0)
        pub_rows += f"""
        <tr>
          <td>{name}</td>
          <td style="color:{_PURPLE};">{fmt_usd(ecpm)}</td>
          <td class="muted">{fill:.2%}</td>
          <td style="color:{_GREEN};">{fmt_usd(opp_score)}/day</td>
        </tr>"""

    proj_html = ""
    for tier in ("10pct", "25pct", "50pct"):
        p = proj.get(tier, {})
        if p:
            label     = tier.replace("pct", "%")
            daily     = p.get("daily_revenue", 0)
            annual    = p.get("annual_revenue", 0)
            proj_html += f"""
            <tr>
              <td>+{label} volume</td>
              <td style="color:{_BLUE};">{fmt_usd(daily)}/day</td>
              <td style="color:{_BLUE};">{fmt_usd(annual)}/yr</td>
            </tr>"""

    proj_section = ""
    if proj_html:
        proj_section = f"""
        <div style="margin-top:16px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;">
            Revenue Projections
          </div>
          <table>
            <thead><tr><th>Scenario</th><th>Daily</th><th>Annual</th></tr></thead>
            <tbody>{proj_html}</tbody>
          </table>
        </div>"""

    return f"""
    <div class="card">
      <h2>CTV / OTT Opportunities</h2>
      <div class="metric-grid" style="margin-bottom:14px;">
        <div class="metric">
          <div class="label">Avg eCPM</div>
          <div class="value purple">{fmt_usd(avg_ecpm)}</div>
        </div>
        <div class="metric">
          <div class="label">Fill Rate</div>
          <div class="value">{fill_rate:.2%}</div>
        </div>
        <div class="metric">
          <div class="label">Avg Daily Rev</div>
          <div class="value">{fmt_usd(avg_daily_rev)}</div>
        </div>
      </div>
      {'<table><thead><tr><th>Publisher</th><th>eCPM</th><th>Fill Rate</th><th>Scale Opp</th></tr></thead><tbody>' + pub_rows + '</tbody></table>' if pub_rows else ''}
      {proj_section}
    </div>
    """


def _html_brief_section(brief: str) -> str:
    if not brief:
        return ""

    paragraphs = [p.strip() for p in brief.strip().split("\n\n") if p.strip()]
    paras_html = "".join(f'<p class="brief-para">{p}</p>' for p in paragraphs)

    return f"""
    <div class="card" style="border-color:{_PURPLE};border-width:1px 1px 1px 4px;background:#faf5ff;">
      <h2 style="color:{_PURPLE};">Executive Intelligence Brief</h2>
      {paras_html}
    </div>
    """


def _html_win_rate_section(wr: dict, fmt_usd) -> str:
    if not wr or not wr.get("top_combinations"):
        return ""

    combos       = wr["top_combinations"]
    total_daily  = wr.get("total_daily_recovery", 0)
    total_weekly = wr.get("total_weekly_recovery", 0)
    n_found      = wr.get("total_combos_found", 0)

    rows_html = ""
    for c in combos[:8]:
        wr_pct     = c.get("win_rate_pct", 0)
        cur_floor  = c.get("current_floor", 0)
        new_floor  = c.get("new_floor", 0)
        add_rev    = c.get("add_rev_per_day", 0)
        adj_pct    = c.get("floor_adj_pct", 0)
        rows_html += f"""
        <tr>
          <td>{c['publisher']}</td>
          <td style="color:{_MUTED};">{c['demand_partner']}</td>
          <td style="color:{_RED};">{wr_pct:.2f}%</td>
          <td class="muted">{fmt_usd(cur_floor)} → <span style="color:{_GREEN};">{fmt_usd(new_floor)}</span> <span style="color:{_MUTED};font-size:11px;">({adj_pct:+.1f}%)</span></td>
          <td style="color:{_GREEN};">+{fmt_usd(add_rev)}/day</td>
        </tr>"""

    return f"""
    <div class="card">
      <h2>Win Rate Opportunities</h2>
      <div style="display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap;">
        <div class="metric" style="min-width:160px;">
          <div class="label">Daily Recovery</div>
          <div class="value green">+{fmt_usd(total_daily)}</div>
          <div class="change muted">{n_found} combinations</div>
        </div>
        <div class="metric" style="min-width:160px;">
          <div class="label">Weekly Recovery</div>
          <div class="value green">+{fmt_usd(total_weekly)}</div>
          <div class="change muted">Win rate target 10%</div>
        </div>
      </div>
      <table>
        <thead><tr>
          <th>Publisher</th><th>Demand Partner</th><th>Win Rate</th>
          <th>Floor Adjustment</th><th>Est. Recovery</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def _html_footer(date_str: str) -> str:
    return f"""
    <div class="footer">
      PGAM Intelligence &nbsp;·&nbsp; {date_str} &nbsp;·&nbsp;
      Automated daily report &nbsp;·&nbsp; Unsubscribe not applicable (internal ops)
    </div>
    """


def _build_html(
    date_str:       str,
    now_et:         datetime,
    today_actions:  list,
    outcomes:       dict,
    topline:        dict,
    health:         dict,
    geo:            dict,
    demand_margin:  dict,
    top_combos:     dict,
    rev_summary:    dict,
    floors:         dict,
    opp_fill:       dict,
    floor_opps:     list,
    ctv:            dict,
    win_rate:       dict,
    brief:          str,
    fmt_usd,
    fmt_n,
) -> str:
    body_parts = [
        _html_header(date_str, now_et),
        # Operations cards at the top: what to do, what we did.
        _html_today_actions_section(today_actions),
        _html_outcomes_section(outcomes),
        # Executive Brief removed 2026-04-30 — recipients said the prose summary
        # was redundant with the structured data below. Brief generation is still
        # wired (Claude analyst still runs) but no longer rendered. Restore by
        # adding _html_brief_section(brief) back if needed.
        _html_topline_section(topline, fmt_usd, fmt_n),
        _html_health_section(
            gainers       = health.get("gainers", []),
            losers        = health.get("losers", []),
            dead          = health.get("dead", {}),
            concentration = health.get("concentration", {}),
            fmt_usd       = fmt_usd,
        ),
        _html_geo_section(geo, fmt_usd, fmt_n),
        _html_demand_margin_section(demand_margin, fmt_usd),
        _html_top_combos_section(top_combos, fmt_usd, fmt_n),
        _html_revenue_section(rev_summary, fmt_usd, fmt_n),
        _html_opp_fill_section(opp_fill, fmt_n),
        _html_floor_section(floors, fmt_usd),
        _html_floor_elasticity_section(floor_opps, fmt_usd),
        _html_win_rate_section(win_rate, fmt_usd),
        _html_ctv_section(ctv, fmt_usd, fmt_n),
        _html_footer(date_str),
    ]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>PGAM Intelligence — {date_str}</title>
  <style>{_css()}</style>
</head>
<body>
  <div class="wrapper">
    {''.join(body_parts)}
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# SendGrid delivery
# ---------------------------------------------------------------------------

def _send_email(
    html_body: str,
    date_str:  str,
    sendgrid_key: str,
    sender:    str,
    recipients: list[str],
    subject_prefix: str = "",
) -> bool:
    """Send HTML email via SendGrid REST API. Returns True on success."""
    try:
        import urllib.request
    except ImportError:
        print("[daily_email] urllib not available")
        return False

    subject = f"{subject_prefix}PGAM Intelligence — Daily Report {date_str}"
    payload = {
        "personalizations": [{"to": [{"email": r} for r in recipients]}],
        "from": {"email": sender},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }

    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {sendgrid_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.getcode()
            if status in (200, 202):
                print(f"[daily_email] Email delivered to {len(recipients)} recipient(s). Status {status}.")
                return True
            print(f"[daily_email] Unexpected status {status}")
            return False
    except Exception as exc:
        print(f"[daily_email] Delivery failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(force_test: bool = False, test_recipients: list[str] | None = None):
    """
    Build and send the daily report.

    Args:
        force_test:      If True, bypass the time-of-day gate and the once-per-day
                         dedup, prefix the subject with "[TEST] ", and skip the
                         dedup state write so production sends still go out later.
        test_recipients: Override the configured recipient list (test mode only).
                         Useful for sending the test to just one inbox.
    """
    now_et    = datetime.now(ET)
    hour_et   = now_et.hour
    date_str  = now_et.strftime("%Y-%m-%d")

    if not force_test:
        # Hour gate — only send at or after SEND_HOUR_ET
        if hour_et < SEND_HOUR_ET:
            print(f"[daily_email] Too early ({hour_et:02d}:xx ET). Will send at {SEND_HOUR_ET:02d}:00 ET.")
            return

        # Deduplication — once per day
        if _already_sent(date_str):
            print(f"[daily_email] Already sent for {date_str}. Exiting.")
            return
    else:
        print(f"[daily_email] TEST MODE — bypassing time gate and dedup")

    # ------------------------------------------------------------------
    # Load core dependencies
    # ------------------------------------------------------------------
    try:
        (fetch, yesterday_fn, today_fn, n_days_ago_fn,
         sf, pct, fmt_usd, fmt_n,
         sendgrid_key, sender, recipients, thresholds) = _core()
    except Exception as exc:
        print(f"[daily_email] Core import failed: {exc}")
        traceback.print_exc()
        return

    if not sendgrid_key:
        print("[daily_email] SENDGRID_KEY not set. Exiting.")
        return
    if not recipients:
        print("[daily_email] No EMAIL_TO recipients configured. Exiting.")
        return

    print(f"[daily_email] Building report for {date_str}…")

    # ------------------------------------------------------------------
    # Collect data from all sources (failures are non-fatal)
    # ------------------------------------------------------------------
    topline: dict = {}
    try:
        topline = _collect_topline(yesterday_fn, n_days_ago_fn)
        ll_rev = topline.get("ll", {}).get("yest", {}).get("revenue", 0)
        tb_rev = topline.get("tb", {}).get("yest", {}).get("revenue", 0)
        print(f"[daily_email] Topline: LL ${ll_rev:,.0f}  |  TB ${tb_rev:,.0f}  |  "
              f"{len(topline.get('movers', []))} movers")
    except Exception as exc:
        print(f"[daily_email] Topline collection failed: {exc}")
        traceback.print_exc()

    # Top combos: per-pub bundle × demand (LL) + pub × demand (TB)
    top_combos: dict = {}
    try:
        # Use the LL movers' publisher list to anchor the top-pubs ranking,
        # falling back to combo-derived ranking if movers list is empty.
        ll_top_pubs = [m["publisher"] for m in topline.get("movers", [])
                       if m.get("platform") == "LL"]
        top_combos = _collect_top_combos(yesterday_fn, ll_top_pubs)
        n_ll = len(top_combos.get("ll", []))
        n_tb = len((top_combos.get("tb") or {}).get("combos", []))
        print(f"[daily_email] Top combos: {n_ll} LL pubs · {n_tb} TB combos")
    except Exception as exc:
        print(f"[daily_email] Top combos collection failed: {exc}")
        traceback.print_exc()

    # Health: gainers/losers split + dead inventory + concentration risk.
    # All derived from data already fetched by _collect_topline.
    health: dict = {}
    try:
        gainers, losers = _split_movers(topline.get("movers", []), n_each=3)
        pubs_data = topline.get("_pubs", {})
        dead_ll = _compute_dead_inventory(
            pubs_data.get("ll_yest", []), pubs_data.get("ll_7d", []))
        # TB doesn't have a 7d publisher list (uses d2), so skip TB dead-check
        conc_ll = _compute_pub_concentration(pubs_data.get("ll_yest", []))
        conc_tb = _compute_pub_concentration(pubs_data.get("tb_yest", []))
        health = {
            "gainers":       gainers,
            "losers":        losers,
            "dead":          {"ll": dead_ll, "tb": []},
            "concentration": {"ll": conc_ll, "tb": conc_tb},
        }
        print(f"[daily_email] Health: {len(gainers)} gainers · {len(losers)} drops · "
              f"{len(dead_ll)} dead pubs · LL top-3 share {conc_ll['share_pct']:.1f}%")
    except Exception as exc:
        print(f"[daily_email] Health computation failed: {exc}")
        traceback.print_exc()

    # Geo breakdown (LL only — TB doesn't track country reliably)
    geo: dict = {}
    try:
        geo = _collect_geo(yesterday_fn, n_days_ago_fn, top_n=5)
        print(f"[daily_email] Geo: {len(geo.get('ll', []))} LL countries · "
              f"{len(geo.get('tb', []))} TB countries")
    except Exception as exc:
        print(f"[daily_email] Geo collection failed: {exc}")
        traceback.print_exc()

    # Margin by demand partner
    demand_margin: dict = {}
    try:
        demand_margin = _collect_demand_margin(yesterday_fn, n_days_ago_fn, top_n=8)
        print(f"[daily_email] Demand margin: {len(demand_margin.get('ll', []))} LL · "
              f"{len(demand_margin.get('tb', []))} TB partners")
    except Exception as exc:
        print(f"[daily_email] Demand margin collection failed: {exc}")
        traceback.print_exc()

    # Today's Actions: prioritized op items pulled from rec snapshots + live signals
    today_actions: list = []
    try:
        today_actions = _collect_today_actions(
            dead_ll = health.get("dead", {}).get("ll", []),
            losers  = health.get("losers", []),
        )
        n_high = sum(1 for a in today_actions if a["severity"] == "HIGH")
        print(f"[daily_email] Today's actions: {len(today_actions)} total · {n_high} HIGH")
    except Exception as exc:
        print(f"[daily_email] Today's actions collection failed: {exc}")
        traceback.print_exc()

    # Yesterday's Outcomes: auto-agent ledger summary (last 24h)
    outcomes: dict = {}
    try:
        outcomes = _collect_yesterday_outcomes(yesterday_fn, n_days_ago_fn)
        n = outcomes.get("nudges", {})
        print(f"[daily_email] Outcomes: {n.get('applied',0)} nudges applied / "
              f"{n.get('proposed',0)} proposed · {n.get('skipped',0)} skipped")
    except Exception as exc:
        print(f"[daily_email] Outcomes collection failed: {exc}")
        traceback.print_exc()

    rev_summary = _collect_revenue_summary(
        fetch, yesterday_fn, today_fn, n_days_ago_fn, sf, pct, fmt_usd, fmt_n
    )

    floors = _collect_floor_gaps(fetch, yesterday_fn, sf)

    opp_fill = _collect_opp_fill(fetch, today_fn, n_days_ago_fn, sf, pct)

    # Floor elasticity (weekly report module)
    floor_opps: list = []
    try:
        from agents.reports.floor_elasticity import get_optimization_data
        floor_opps = get_optimization_data(top_n=8)
        print(f"[daily_email] Floor elasticity: {len(floor_opps)} opportunities")
    except Exception as exc:
        print(f"[daily_email] Floor elasticity import failed: {exc}")

    # CTV section
    ctv: dict = {}
    try:
        from agents.alerts.ctv_optimizer import export_ctv_section
        ctv = export_ctv_section(top_n=5)
        print(f"[daily_email] CTV section: {'ok' if ctv else 'empty'}")
    except Exception as exc:
        print(f"[daily_email] CTV import failed: {exc}")

    # Win rate maximizer section
    win_rate: dict = {}
    try:
        from agents.reports.win_rate_maximizer import export_win_rate_section
        win_rate = export_win_rate_section(top_n=8)
        print(f"[daily_email] Win rate: {win_rate.get('total_combos_found', 0)} combos, "
              f"${win_rate.get('total_daily_recovery', 0):,.0f}/day recoverable")
    except Exception as exc:
        print(f"[daily_email] Win rate import failed: {exc}")

    # Claude executive brief
    brief = ""
    try:
        from intelligence.claude_analyst import synthesize_daily_brief

        anomalies: list = []
        # Populate anomalies from floor gaps if any meaningful gaps exist
        if floors.get("raise"):
            anomalies.append({
                "type": "floor_underpriced",
                "count": len(floors["raise"]),
                "top_publisher": floors["raise"][0]["publisher"] if floors["raise"] else None,
            })
        if floors.get("lower"):
            anomalies.append({
                "type": "floor_overpriced",
                "count": len(floors["lower"]),
                "top_publisher": floors["lower"][0]["publisher"] if floors["lower"] else None,
            })
        if opp_fill and not opp_fill.get("above_threshold", True):
            anomalies.append({
                "type": "fill_rate_below_threshold",
                "fill_rate_pct": opp_fill.get("fill_rate_pct"),
                "imps_needed": opp_fill.get("imps_needed"),
            })

        fix_summary = {
            "raise_count":  len(floors.get("raise", [])),
            "lower_count":  len(floors.get("lower", [])),
            "top_raise":    floors.get("raise", [{}])[0] if floors.get("raise") else {},
            "top_elasticity_opps": floor_opps[:3] if floor_opps else [],
        }

        brief = synthesize_daily_brief(
            summary   = rev_summary,
            fix       = fix_summary,
            anomalies = anomalies,
            opp_fill  = opp_fill,
            date_str  = date_str,
        )
        print("[daily_email] Claude brief: generated")
    except Exception as exc:
        print(f"[daily_email] Claude brief failed: {exc}")

    # ------------------------------------------------------------------
    # Build and send HTML
    # ------------------------------------------------------------------
    html = _build_html(
        date_str      = date_str,
        now_et        = now_et,
        today_actions = today_actions,
        outcomes      = outcomes,
        topline       = topline,
        health        = health,
        geo           = geo,
        demand_margin = demand_margin,
        top_combos    = top_combos,
        rev_summary   = rev_summary,
        floors      = floors,
        opp_fill    = opp_fill,
        floor_opps  = floor_opps,
        ctv         = ctv,
        win_rate    = win_rate,
        brief       = brief,
        fmt_usd     = fmt_usd,
        fmt_n       = fmt_n,
    )

    # Recipient + subject overrides for test mode
    if force_test:
        active_recipients = test_recipients or recipients[:1]  # default: first recipient only
        subject_prefix    = "[TEST] "
        print(f"[daily_email] Test send → {active_recipients}")
    else:
        active_recipients = recipients
        subject_prefix    = ""

    success = _send_email(html, date_str, sendgrid_key, sender, active_recipients,
                          subject_prefix=subject_prefix)

    if success and not force_test:
        _mark_sent(date_str)
    elif force_test:
        print("[daily_email] Test send complete — production dedup state untouched.")
    else:
        print("[daily_email] Email not delivered — state NOT marked as sent.")


if __name__ == "__main__":
    import sys
    force_test = "--test" in sys.argv
    # Optional: --to=email@example.com to override recipient
    test_to = None
    for arg in sys.argv[1:]:
        if arg.startswith("--to="):
            test_to = [arg.split("=", 1)[1]]
    run(force_test=force_test, test_recipients=test_to)
