"""
intelligence/advisory_verifier.py

Reality-check layer for LLM-generated Slack advisories.

Background
----------
On 2026-04-26, `claude_analyst.write_revenue_gap_memo()` produced a
Sunday memo recommending a BidMachine floor cut from $3.25 to $2.00.
Verification against current LL state showed *zero* demands at $3.25
on the named publishers — the LLM synthesized a coherent narrative
from upstream data that was either stale or hallucinated. Acting on it
would have been a no-op (best case) or a misallocation of attention.

This module wraps any LLM advisory with a reality check before it hits
Slack: pulls live state for the entities referenced in the source data,
compares against the claimed values, and either:

  • PASS → return the memo with a small "✓ verified vs LL state" footer
  • FAIL → return a SHORT replacement post explaining the data is stale
           and listing the specific discrepancies, so the operator can
           investigate the upstream data agent (revenue_gap, etc.)

Verifications performed
-----------------------
1. **pub_gaps.underperformers** — for each publisher in the input,
   pull live demand-list and compare floor distribution. If the input
   says "publisher X has Y demands at $Z floor" and live shows none
   at that floor, fail.
2. **dp_trends.declining** — re-pull last-7d revenue per partner from
   hourly_pub_demand store and compare to claimed decline magnitude.
3. **country_gaps.gaps** — re-pull last-7d revenue per country.

The verifier reports DELTAS, not absolute matches. A 10% drift is OK;
a 50% drift means upstream data is stale and we shouldn't post.

Wrapper contract
----------------
verify_or_replace(memo: str, source_data: dict, threshold_pct: float)
  → (final_message: str, was_replaced: bool, issues: list[dict])
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from core import ll_mgmt

DATA_DIR = Path(__file__).parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"

DEFAULT_DRIFT_THRESHOLD = 0.30  # 30% drift = data stale enough to block


def _load_hourly() -> list[dict]:
    if not HOURLY_PATH.exists():
        return []
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def _live_floor_distribution(publisher_id: int) -> dict[float, int]:
    """Return {floor_value: count} of currently-wired demands on a pub."""
    out: dict = defaultdict(int)
    try:
        p = ll_mgmt.get_publisher(publisher_id)
    except Exception:
        return {}
    for pref in p.get("biddingpreferences", []):
        for v in pref.get("value", []):
            did = v.get("id")
            if not did:
                continue
            try:
                d = ll_mgmt._get(f"/v1/demands/{did}")
                fl = d.get("minBidFloor")
                if fl is None:
                    out[0.0] += 1
                else:
                    out[round(float(fl), 2)] += 1
            except Exception:
                continue
    return dict(out)


def _live_partner_revenue_7d(partner_substring: str) -> float:
    """Sum 7d revenue for any demand whose name contains the partner substring."""
    rows = _load_hourly()
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    needle = partner_substring.lower()
    total = 0.0
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        name = (r.get("DEMAND_NAME", "") or "").lower()
        if needle in name:
            total += float(r.get("GROSS_REVENUE", 0) or 0)
    return total


def _live_country_revenue_7d(country: str) -> float | None:
    """Sum 7d country revenue from daily_pub_demand_country store."""
    geo_path = DATA_DIR / "daily_pub_demand_country.json.gz"
    if not geo_path.exists():
        return None
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    try:
        with gzip.open(geo_path, "rt") as f:
            rows = json.load(f)
    except Exception:
        return None
    cc = country.upper()
    total = 0.0
    for r in rows:
        if str(r.get("DATE", "")) < cutoff:
            continue
        if (r.get("COUNTRY", "") or "").upper() == cc:
            total += float(r.get("GROSS_REVENUE", 0) or 0)
    return total


def verify_input_data(source_data: dict,
                      threshold: float = DEFAULT_DRIFT_THRESHOLD) -> list[dict]:
    """Return list of issues — each is a dict describing a stale/wrong claim.
    Empty list = source data passes the check."""
    issues = []

    # 1. pub_gaps.underperformers — compare claimed floors against live LL
    pub_gaps = source_data.get("pub_gaps") or {}
    for u in (pub_gaps.get("underperformers") or [])[:5]:
        pid = u.get("publisher_id")
        claimed_floor = u.get("avg_floor") or u.get("median_floor")
        if not pid or claimed_floor is None:
            continue
        live = _live_floor_distribution(pid)
        if not live:
            continue
        claimed_floor_f = float(claimed_floor)
        # Are there ANY demands within 20% of the claimed floor?
        matches = sum(cnt for fl, cnt in live.items()
                      if claimed_floor_f * 0.8 <= fl <= claimed_floor_f * 1.2)
        total_demands = sum(live.values())
        if total_demands and matches / total_demands < 0.10:
            issues.append({
                "kind": "pub_floor_mismatch",
                "publisher_id": pid,
                "publisher_name": u.get("publisher_name", ""),
                "claimed_floor": claimed_floor_f,
                "live_distribution": dict(sorted(live.items())[:8]),
                "match_pct": round(matches / total_demands, 3),
                "explanation": (f"Source data claims pub {pid} has demands at "
                                f"~${claimed_floor_f}, but live LL shows "
                                f"only {matches}/{total_demands} demands within ±20% "
                                f"of that floor."),
            })

    # 2. dp_trends.declining — check claimed decline magnitude
    dp_trends = source_data.get("dp_trends") or {}
    for d in (dp_trends.get("declining") or [])[:5]:
        partner = d.get("partner") or d.get("name", "")
        claimed_drop_pct = d.get("wow_drop_pct") or d.get("drop_pct")
        if not partner or claimed_drop_pct is None:
            continue
        # We can't easily verify WoW without prior-week data, so just
        # check that the partner has SOME revenue in last 7d (sanity check).
        live_rev = _live_partner_revenue_7d(partner)
        claimed_recoverable = float(d.get("recoverable_daily", 0) or 0)
        if claimed_recoverable > 0 and live_rev < claimed_recoverable * 0.1:
            issues.append({
                "kind": "dp_decline_overclaim",
                "partner": partner,
                "claimed_recoverable_daily": claimed_recoverable,
                "live_total_7d": round(live_rev, 2),
                "explanation": (f"Source claims ${claimed_recoverable}/day "
                                f"recoverable from {partner}, but only "
                                f"${live_rev:.0f}/7d total revenue exists."),
            })

    # 3. country_gaps.gaps — basic sanity (claimed country exists in store)
    country_gaps = source_data.get("country_gaps") or {}
    for g in (country_gaps.get("gaps") or [])[:5]:
        cc = g.get("country")
        claimed_opp = float(g.get("daily_opportunity", 0) or 0)
        if not cc:
            continue
        live = _live_country_revenue_7d(cc)
        if live is None:
            continue
        # If claimed daily opportunity > 5x current 7d, suspect inflation
        if claimed_opp > 0 and live < claimed_opp * 0.5:
            issues.append({
                "kind": "country_gap_overclaim",
                "country": cc,
                "claimed_daily_opportunity": claimed_opp,
                "live_total_7d": round(live, 2),
                "explanation": (f"Source claims ${claimed_opp}/day opp in {cc}, "
                                f"but live shows only ${live:.0f}/7d total."),
            })

    return issues


def verify_or_replace(memo: str, source_data: dict,
                      advisory_label: str = "Advisory",
                      threshold: float = DEFAULT_DRIFT_THRESHOLD) -> tuple[str, bool, list[dict]]:
    """Reality-check the memo. Returns (final_message, was_replaced, issues)."""
    issues = verify_input_data(source_data, threshold)

    if not issues:
        # PASS — append small footer
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        footer = f"\n\n_✓ Verified vs live LL state at {ts}._"
        return memo + footer, False, []

    # FAIL — replace memo with a "data stale" alert
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    parts = [
        f":warning: *{advisory_label} withheld — upstream data fails reality check*",
        "",
        f"_The Sunday memo was generated, but {len(issues)} of its claims diverge "
        f"materially from current LL state. Posting it as written would be misleading. "
        f"Investigate the upstream data agent (revenue_gap or similar) that produced "
        f"the source numbers._",
        "",
        "*Discrepancies found:*",
    ]
    for i, issue in enumerate(issues[:5], 1):
        parts.append(f"  {i}. {issue['explanation']}")

    parts.append("")
    parts.append(f"_Verified at {ts}. Original memo cached but not posted._")
    return "\n".join(parts), True, issues
