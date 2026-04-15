"""
agents/alerts/geo_leak.py
──────────────────────────────────────────────────────────────────────────────
Weekly geo revenue leak agent (Thursdays).

Finds publisher × country combinations where traffic volume exists but NO
demand partner covers that geography — pure revenue left on the table.

Two finding categories
─────────────────────
  UNCOVERED GEO   — opportunities > 200k, zero revenue, but same publisher
                    earns in at least one other country (demand IS connected,
                    just not geo-targeted here).

  PARTIAL COVERAGE — opportunities > 200k, revenue > 0 but only 1 active
                     demand partner AND fill rate < 2% (single point of
                     failure + almost no monetisation).

Algorithm
─────────
1. Fetch last 7 days via report_pub_demand_country() (publisher × demand × country).
2. Aggregate per publisher × country: total opps, revenue, active demand count,
   bidding-but-blocked count, untargeted count.
3. Determine which publishers have at least one earning country (to confirm
   demand is live).
4. Flag uncovered + partial-coverage geos against thresholds.
5. For uncovered geos, identify which demand partners ARE active on other geos
   for that publisher (= expansion candidates).
6. Estimate weekly upside using the publisher's avg eCPM in covered geos.
7. Post Block Kit Slack message; weekly dedup via STATE_FILE.

State file: /tmp/pgam_geo_leak_state.json
Runs:       Thursdays only (weekday == 3)
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, date, timedelta

import pytz

from core.api import yesterday, n_days_ago, sf, fmt_usd, fmt_n
from core.ll_report import report_pub_demand_country
from core.slack import send_blocks
from core.ui_nav import geo_target_add, demand_seat_add

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ET            = pytz.timezone("America/New_York")
ALERT_KEY     = "geo_leak_weekly"
STATE_FILE    = "/tmp/pgam_geo_leak_state.json"

MIN_OPPS          = 200_000   # minimum weekly opportunities to consider
PARTIAL_MAX_FILL  = 0.02      # < 2 % fill rate = partial-coverage flag
MAX_PUBLISHERS    = 8         # cap displayed publishers in Slack


# ---------------------------------------------------------------------------
# State / dedup helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def _already_sent_this_week() -> bool:
    """
    Return True if the alert was already sent during the current ISO week.
    Key: "YYYY-WXX" (ISO year + week number).
    """
    state   = _load_state()
    week_key = date.today().strftime("%G-W%V")
    return state.get(ALERT_KEY) == week_key


def _mark_sent() -> None:
    state   = _load_state()
    week_key = date.today().strftime("%G-W%V")
    state[ALERT_KEY] = week_key
    _save_state(state)


# ---------------------------------------------------------------------------
# Data aggregation
# ---------------------------------------------------------------------------

def _aggregate(rows: list[dict]) -> tuple[
    dict[tuple[str, str], dict],   # pub_country_stats
    dict[str, dict[str, dict]],    # pub_demand_country  [pub][demand][country]
]:
    """
    Aggregate raw publisher × demand × country rows into two lookups.

    pub_country_stats key: (publisher_name, country)
    Value:
        opportunities, gross_revenue, active_demand_count,
        bidding_blocked_count, untargeted_count,
        opportunity_fill_rate (avg)

    pub_demand_country: nested dict for checking which demands are active
    where.
    """
    # Accumulator: (pub, country) → per-row metrics aggregated
    pcs: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "opportunities": 0.0,
        "gross_revenue": 0.0,
        "bids":          0.0,
        "wins":          0.0,
        "impressions":   0.0,
        "bid_requests":  0.0,
        # demand-level counters
        "_demand_active":  set(),   # demands with impressions > 0
        "_demand_bidding": set(),   # demands with bids > 0 but wins == 0
        "_demand_silent":  set(),   # demands with bid_requests == 0
    })

    # [pub][demand] → set of countries where that demand has impressions > 0
    pdc: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))

    for row in rows:
        pub     = str(row.get("PUBLISHER_NAME") or "Unknown").strip()
        country = str(row.get("COUNTRY") or "Unknown").strip()
        demand  = str(row.get("DEMAND_NAME") or "Unknown").strip()

        if pub == "Unknown" or country == "Unknown":
            continue

        opps   = sf(row.get("OPPORTUNITIES", 0))
        rev    = sf(row.get("GROSS_REVENUE",  0))
        bids   = sf(row.get("BIDS",           0))
        wins   = sf(row.get("WINS",           0))
        imps   = sf(row.get("IMPRESSIONS",    0))
        breqs  = sf(row.get("BID_REQUESTS",   0))

        key = (pub, country)
        agg = pcs[key]
        agg["opportunities"] += opps
        agg["gross_revenue"] += rev
        agg["bids"]          += bids
        agg["wins"]          += wins
        agg["impressions"]   += imps
        agg["bid_requests"]  += breqs

        if imps > 0:
            agg["_demand_active"].add(demand)
        if bids > 0 and wins == 0:
            agg["_demand_bidding"].add(demand)
        if breqs == 0:
            agg["_demand_silent"].add(demand)

        if imps > 0:
            pdc[pub][demand].add(country)

    # Flatten set counters to ints
    for agg in pcs.values():
        agg["active_demand_count"]   = len(agg.pop("_demand_active"))
        agg["bidding_blocked_count"] = len(agg.pop("_demand_bidding"))
        agg["untargeted_count"]      = len(agg.pop("_demand_silent"))

        total_opps = agg["opportunities"]
        agg["opportunity_fill_rate"] = (
            agg["impressions"] / total_opps if total_opps > 0 else 0.0
        )

    return dict(pcs), dict(pdc)


# ---------------------------------------------------------------------------
# Leak detection
# ---------------------------------------------------------------------------

def _detect_leaks(
    pub_country_stats: dict[tuple[str, str], dict],
    pub_demand_country: dict[str, dict[str, set]],
) -> tuple[
    dict[str, list[dict]],   # uncovered: pub → list of geo findings
    dict[str, list[dict]],   # partial:   pub → list of geo findings
    dict[str, float],        # pub_avg_ecpm: publisher → avg eCPM in covered geos
]:
    # ── Which publishers have at least one earning country? ─────────────────
    pub_earning_countries: dict[str, set[str]] = defaultdict(set)
    pub_covered_revenue:   dict[str, float]    = defaultdict(float)
    pub_covered_imps:      dict[str, float]    = defaultdict(float)

    for (pub, country), stats in pub_country_stats.items():
        if stats["gross_revenue"] > 0:
            pub_earning_countries[pub].add(country)
            pub_covered_revenue[pub]   += stats["gross_revenue"]
            pub_covered_imps[pub]      += stats["impressions"]

    # Publisher avg eCPM in covered (earning) geos
    pub_avg_ecpm: dict[str, float] = {}
    for pub in pub_earning_countries:
        imps = pub_covered_imps[pub]
        rev  = pub_covered_revenue[pub]
        pub_avg_ecpm[pub] = (rev / imps * 1000) if imps > 0 else 0.0

    uncovered: dict[str, list[dict]] = defaultdict(list)
    partial:   dict[str, list[dict]] = defaultdict(list)

    for (pub, country), stats in pub_country_stats.items():
        opps   = stats["opportunities"]
        rev    = stats["gross_revenue"]
        fill   = stats["opportunity_fill_rate"]
        active = stats["active_demand_count"]

        if opps < MIN_OPPS:
            continue

        # Identify which demand partners are active on other countries for
        # this publisher (= candidates to expand into this uncovered geo)
        pdc_pub = pub_demand_country.get(pub, {})
        expansion_candidates = [
            demand for demand, active_countries in pdc_pub.items()
            if country not in active_countries and len(active_countries) > 0
        ]

        # ── Uncovered geo ────────────────────────────────────────────────
        if (
            rev == 0
            and pub in pub_earning_countries      # publisher IS earning elsewhere
            and country not in pub_earning_countries[pub]
        ):
            # Estimate weekly upside: opps × avg_ecpm / 1000 × typical fill
            # Use a conservative 1 % assumed fill for completely dark geos
            ecpm   = pub_avg_ecpm.get(pub, 0.0)
            upside = opps * 0.01 * ecpm / 1000

            # Collect which active demands are in other geos for this pub
            candidate_details = []
            for demand, active_countries in pdc_pub.items():
                if country not in active_countries and len(active_countries) > 0:
                    sorted_countries = sorted(active_countries)[:3]
                    candidate_details.append({
                        "demand":           demand,
                        "active_geos":      sorted_countries,
                        "active_geo_count": len(active_countries),
                    })
            # Sort by geo coverage (most active demand first)
            candidate_details.sort(key=lambda x: -x["active_geo_count"])

            uncovered[pub].append({
                "country":              country,
                "opportunities":        opps,
                "gross_revenue":        rev,
                "active_demand_count":  active,
                "bidding_blocked":      stats["bidding_blocked_count"],
                "untargeted":           stats["untargeted_count"],
                "fill_rate":            fill,
                "est_weekly_upside":    round(upside, 2),
                "expansion_candidates": candidate_details[:5],
            })

        # ── Partial coverage geo ─────────────────────────────────────────
        elif (
            rev > 0
            and active == 1
            and fill < PARTIAL_MAX_FILL
        ):
            partial[pub].append({
                "country":             country,
                "opportunities":       opps,
                "gross_revenue":       rev,
                "active_demand_count": active,
                "fill_rate":           fill,
                "expansion_candidates": [
                    c["demand"] for c in [
                        {"demand": d, "geos": len(geos)}
                        for d, geos in pdc_pub.items()
                        if country not in geos
                    ]
                ][:3],
            })

    # Sort each publisher's findings by opportunities descending
    for pub in uncovered:
        uncovered[pub].sort(key=lambda x: -x["opportunities"])
    for pub in partial:
        partial[pub].sort(key=lambda x: -x["opportunities"])

    return dict(uncovered), dict(partial), pub_avg_ecpm


# ---------------------------------------------------------------------------
# Estimation helpers
# ---------------------------------------------------------------------------

def _total_uncovered_opps(uncovered: dict[str, list[dict]]) -> int:
    return int(sum(g["opportunities"] for geos in uncovered.values() for g in geos))


def _total_est_upside(uncovered: dict[str, list[dict]]) -> float:
    return sum(g["est_weekly_upside"] for geos in uncovered.values() for g in geos)


def _count_pubs(uncovered: dict, partial: dict) -> int:
    return len(set(list(uncovered.keys()) + list(partial.keys())))


# ---------------------------------------------------------------------------
# Slack Block Kit builder
# ---------------------------------------------------------------------------

def _geo_flag(country: str) -> str:
    FLAGS = {
        "US": "🇺🇸", "GB": "🇬🇧", "CA": "🇨🇦", "AU": "🇦🇺",
        "DE": "🇩🇪", "FR": "🇫🇷", "JP": "🇯🇵", "IN": "🇮🇳",
        "BR": "🇧🇷", "MX": "🇲🇽", "ID": "🇮🇩", "NG": "🇳🇬",
        "ZA": "🇿🇦", "NL": "🇳🇱", "ES": "🇪🇸", "IT": "🇮🇹",
        "SG": "🇸🇬", "TH": "🇹🇭", "PH": "🇵🇭", "MY": "🇲🇾",
        "VN": "🇻🇳", "PK": "🇵🇰", "AR": "🇦🇷", "CO": "🇨🇴",
        "EG": "🇪🇬", "TR": "🇹🇷", "SA": "🇸🇦", "AE": "🇦🇪",
        "KE": "🇰🇪", "SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰",
        "FI": "🇫🇮", "PL": "🇵🇱", "RU": "🇷🇺", "UA": "🇺🇦",
        "KR": "🇰🇷", "HK": "🇭🇰", "TW": "🇹🇼", "NZ": "🇳🇿",
    }
    return FLAGS.get(country.upper(), ":earth_americas:")


def _fmt_opps(n: float) -> str:
    """Format opportunity count as e.g. '4.2M' or '850K'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return str(int(n))


def _build_blocks(
    uncovered:    dict[str, list[dict]],
    partial:      dict[str, list[dict]],
    pub_avg_ecpm: dict[str, float],
    date_range:   str,
    total_upside: float,
) -> list:
    n_uncov_geos = sum(len(v) for v in uncovered.values())
    n_pub        = _count_pubs(uncovered, partial)

    blocks: list = [
        # ── Header ────────────────────────────────────────────────────────
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":earth_americas:  Geo Revenue Leak Report",
                "emoji": True,
            },
        },
        # ── Summary ───────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{date_range}*\n"
                    f"*{n_uncov_geos}* uncovered geo{'s' if n_uncov_geos != 1 else ''} "
                    f"across *{n_pub}* publisher{'s' if n_pub != 1 else ''} — "
                    f"est. *{fmt_usd(total_upside)}/week* upside "
                    f"(based on publisher avg eCPM in covered geos)"
                ),
            },
        },
        {"type": "divider"},
    ]

    # ── Per-publisher findings ─────────────────────────────────────────────
    all_pubs = sorted(
        set(list(uncovered.keys()) + list(partial.keys())),
        key=lambda p: sum(g["opportunities"] for g in uncovered.get(p, []))
                    + sum(g["opportunities"] for g in partial.get(p, [])),
        reverse=True,
    )

    for pub in all_pubs[:MAX_PUBLISHERS]:
        pub_uncov    = uncovered.get(pub, [])
        pub_partial  = partial.get(pub, [])
        ecpm         = pub_avg_ecpm.get(pub, 0.0)
        pub_upside   = sum(g["est_weekly_upside"] for g in pub_uncov)

        header_line  = (
            f":office: *{pub}*"
            + (f"  ·  avg eCPM in covered geos: `{fmt_usd(ecpm)}`" if ecpm > 0 else "")
            + (f"  ·  est. weekly upside: *{fmt_usd(pub_upside)}*" if pub_upside > 0 else "")
        )

        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_line},
        })

        # Uncovered geos
        if pub_uncov:
            lines = [":no_entry_sign: *Uncovered geos (zero demand coverage):*"]
            for geo in pub_uncov[:5]:
                flag     = _geo_flag(geo["country"])
                opps_fmt = _fmt_opps(geo["opportunities"])
                upside   = geo["est_weekly_upside"]

                cand_strs = []
                for c in geo["expansion_candidates"][:3]:
                    geo_list = "/".join(c["active_geos"])
                    cand_strs.append(f"{c['demand']} (active in {geo_list})")

                cand_line = ""
                if cand_strs:
                    nav_steps = [
                        geo_target_add(c["demand"], geo["country"])
                        for c in geo["expansion_candidates"][:2]
                    ]
                    cand_line = (
                        f"\n    Candidates: {', '.join(cand_strs)}"
                        + "".join(f"\n    {s}" for s in nav_steps)
                    )
                else:
                    cand_line = (
                        f"\n    No demand partner currently targets this publisher; "
                        f"connect a new demand seat first.\n    "
                        + demand_seat_add(pub, "<demand partner>")
                    )

                lines.append(
                    f"• {flag} *{geo['country']}* — "
                    f"{opps_fmt} opps, $0 revenue"
                    + (f", est. *{fmt_usd(upside)}/wk*" if upside > 0 else "")
                    + cand_line
                )

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        # Partial coverage geos
        if pub_partial:
            lines = [":warning: *Partial coverage geos (single demand, low fill):*"]
            for geo in pub_partial[:3]:
                flag     = _geo_flag(geo["country"])
                opps_fmt = _fmt_opps(geo["opportunities"])
                fill_pct = geo["fill_rate"] * 100
                cands    = geo.get("expansion_candidates", [])

                cand_line = ""
                if cands:
                    nav_steps = [geo_target_add(c, geo["country"]) for c in cands[:2]]
                    cand_line = (
                        f"\n    Add: {', '.join(cands[:2])}"
                        + "".join(f"\n    {s}" for s in nav_steps)
                    )

                lines.append(
                    f"• {flag} *{geo['country']}* — "
                    f"{opps_fmt} opps, {fmt_usd(geo['gross_revenue'])} rev, "
                    f"only 1 demand partner active, fill {fill_pct:.1f}%"
                    + cand_line
                )

            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            })

        blocks.append({"type": "divider"})

    # ── Footer ────────────────────────────────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"PGAM Intelligence · Geo Leak Agent · "
                f"Threshold: >{fmt_n(MIN_OPPS)} opps · "
                f"Partial fill threshold: <{PARTIAL_MAX_FILL * 100:.0f}% · "
                f"Weekly · Thursday · "
                + datetime.now(ET).strftime("%H:%M ET")
            ),
        }],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the geo leak analysis.
    Called by the scheduler or directly: `python -m agents.alerts.geo_leak`
    """
    now_et  = datetime.now(ET)
    weekday = now_et.weekday()   # 0=Mon … 6=Sun

    # ── Thursday gate ────────────────────────────────────────────────────────
    if weekday != 3:
        print(f"[geo_leak] Not Thursday (weekday={weekday}). Skipping.")
        return

    # ── Weekly dedup ─────────────────────────────────────────────────────────
    if _already_sent_this_week():
        print("[geo_leak] Already sent this week — skipping.")
        return

    start_date = n_days_ago(7)
    end_date   = yesterday()
    date_range = f"{start_date} → {end_date}"

    print(f"[geo_leak] Fetching publisher × demand × country data {date_range}…")

    # ── 1. Fetch data ────────────────────────────────────────────────────────
    try:
        rows = report_pub_demand_country(start_date, end_date)
    except Exception as exc:
        print(f"[geo_leak] API fetch failed: {exc}")
        return

    if not rows:
        print("[geo_leak] No data returned from API — aborting.")
        return

    print(f"[geo_leak] {len(rows)} rows received.")

    # ── 2. Aggregate ─────────────────────────────────────────────────────────
    pub_country_stats, pub_demand_country = _aggregate(rows)
    print(f"[geo_leak] {len(pub_country_stats)} pub×country combinations aggregated.")

    # ── 3. Detect leaks ───────────────────────────────────────────────────────
    uncovered, partial, pub_avg_ecpm = _detect_leaks(pub_country_stats, pub_demand_country)

    n_uncov  = sum(len(v) for v in uncovered.values())
    n_part   = sum(len(v) for v in partial.values())
    print(f"[geo_leak] Uncovered geos: {n_uncov} | Partial coverage: {n_part}")

    if not uncovered and not partial:
        print("[geo_leak] No geo leaks found above threshold — nothing to post.")
        _mark_sent()
        return

    # ── 4. Build and send Slack message ──────────────────────────────────────
    total_upside = _total_est_upside(uncovered)
    blocks       = _build_blocks(
        uncovered, partial, pub_avg_ecpm, date_range, total_upside,
    )

    fallback = (
        f":earth_americas: Geo Leak Report: {n_uncov} uncovered geos across "
        f"{_count_pubs(uncovered, partial)} publishers — "
        f"est. {fmt_usd(total_upside)}/week upside"
    )

    try:
        send_blocks(blocks, text=fallback)
        _mark_sent()
        print(
            f"[geo_leak] Report sent — {n_uncov} uncovered, {n_part} partial."
        )
    except Exception as exc:
        print(f"[geo_leak] Slack post failed: {exc}")


if __name__ == "__main__":
    run()
