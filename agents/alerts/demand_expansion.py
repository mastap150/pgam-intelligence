"""
agents/alerts/demand_expansion.py

Identifies the highest-probability new demand connections by finding demand
partners that are already buying similar inventory elsewhere in the network
but are not yet connected to specific publishers.

Algorithm
---------
1.  Fetch PUBLISHER × DEMAND_PARTNER_NAME data for the last 7 days.

2.  Build an activity matrix:
      activity[publisher][demand_partner] = {revenue, wins, bids, ecpm}

3.  Compute each publisher's eCPM profile:
      pub_ecpm[P] = wins-weighted mean of GROSS_ECPM across all demand partners on P

4.  For every (publisher P, demand partner D) pair where D is NOT on P:
      a.  Find publishers where D IS active.
      b.  Filter to "similar" publishers: eCPM within ECPM_SIMILARITY_THRESHOLD of P.
      c.  Require ≥ MIN_ACTIVE_ON_SIMILAR (3) such similar active publishers.
      d.  Compute:
            similarity_coeff[p] = 1 - |ecpm_P - ecpm_p| / max(ecpm_P, ecpm_p)
            opportunity_score   = Σ (activity[p][D].revenue × similarity_coeff[p])

5.  Rank all missing connections by opportunity_score (highest first), take top 10.

6.  Ask Claude to write one specific action sentence per connection.

7.  Post to Slack every Wednesday (once per day, deduped).

Similarity thresholds
---------------------
  ECPM_SIMILARITY_THRESHOLD  0.70   demand partner must achieve ≥ 70% eCPM match
  MIN_ACTIVE_ON_SIMILAR      3      must be active on at least 3 similar publishers
  MIN_DP_REVENUE             5.0    demand partner must have ≥ $5 revenue on a similar pub
                                    (avoids amplifying test traffic noise)
"""

from collections import defaultdict
from datetime import datetime

import pytz

from core.api import fetch, n_days_ago, today, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from core.ui_nav import publisher_demand_connect
from intelligence.claude_analyst import analyze_demand_expansion

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BREAKDOWN               = "PUBLISHER,DEMAND_PARTNER"
METRICS                 = ["GROSS_REVENUE", "WINS", "BIDS", "GROSS_ECPM"]
LOOKBACK_DAYS           = 7
ALERT_KEY               = "demand_expansion_weekly"
TOP_OPPORTUNITIES       = 10
ECPM_SIMILARITY_THRESHOLD = 0.70   # similarity coefficient floor
MIN_ACTIVE_ON_SIMILAR   = 3        # min similar-publisher connections the DP must have
MIN_DP_REVENUE          = 5.0      # min 7d revenue on a similar pub (noise filter)
ET                      = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# Row parsing
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _pub_name(row: dict) -> str:
    return str(
        row.get("PUBLISHER_NAME") or row.get("PUBLISHER") or row.get("publisher")
        or row.get("pubName") or row.get("pub_name") or "Unknown"
    )


def _dp_name(row: dict) -> str:
    return str(
        row.get("DEMAND_PARTNER_NAME") or row.get("demand_partner_name")
        or row.get("demandPartnerName") or row.get("demandPartner")
        or row.get("dsp") or "Unknown"
    )


def _parse_activity(rows: list) -> dict[str, dict[str, dict]]:
    """
    Returns activity[publisher][demand_partner] = {revenue, wins, bids, ecpm}.
    For duplicate (pub, dp) pairs (shouldn't happen, but API may return multiple
    rows for the same pair) values are accumulated.
    """
    activity: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "revenue": 0.0, "wins": 0.0, "bids": 0.0, "ecpm_sum": 0.0, "ecpm_n": 0,
    }))

    for row in rows:
        pub = _pub_name(row)
        dp  = _dp_name(row)
        rec = activity[pub][dp]

        rec["revenue"]  += _extract(row, "GROSS_REVENUE",  "gross_revenue")
        rec["wins"]     += _extract(row, "WINS",           "wins")
        rec["bids"]     += _extract(row, "BIDS",           "bids")

        ecpm = _extract(row, "GROSS_ECPM", "gross_ecpm", "ecpm")
        if ecpm > 0:
            rec["ecpm_sum"] += ecpm
            rec["ecpm_n"]   += 1

    # Finalise eCPM as average of observed values
    for pub in activity:
        for dp in activity[pub]:
            rec  = activity[pub][dp]
            wins = rec["wins"]
            if rec["ecpm_n"] > 0:
                rec["ecpm"] = rec["ecpm_sum"] / rec["ecpm_n"]
            elif wins > 0:
                rec["ecpm"] = rec["revenue"] / wins * 1_000
            else:
                rec["ecpm"] = 0.0

    return {p: dict(dps) for p, dps in activity.items()}


# ---------------------------------------------------------------------------
# Publisher eCPM profiles
# ---------------------------------------------------------------------------

def _publisher_ecpm(activity: dict[str, dict[str, dict]]) -> dict[str, float]:
    """
    Compute a wins-weighted mean eCPM for each publisher across all its
    demand partners.
    """
    pub_ecpm: dict[str, float] = {}
    for pub, dps in activity.items():
        total_wins    = sum(d["wins"]    for d in dps.values())
        total_revenue = sum(d["revenue"] for d in dps.values())
        if total_wins > 0:
            pub_ecpm[pub] = total_revenue / total_wins * 1_000
        else:
            # No wins recorded — use unweighted mean of eCPM values
            ecpms = [d["ecpm"] for d in dps.values() if d["ecpm"] > 0]
            pub_ecpm[pub] = sum(ecpms) / len(ecpms) if ecpms else 0.0
    return pub_ecpm


# ---------------------------------------------------------------------------
# Similarity coefficient
# ---------------------------------------------------------------------------

def _similarity(ecpm_a: float, ecpm_b: float) -> float:
    """
    Normalised eCPM similarity between two publishers.
    Returns a value in [0, 1]; 1.0 = identical eCPMs, 0.0 = completely dissimilar.

    Uses the relative difference capped at 1:
        sim = 1 - |a - b| / max(a, b)
    """
    if ecpm_a <= 0 or ecpm_b <= 0:
        return 0.0
    return max(0.0, 1.0 - abs(ecpm_a - ecpm_b) / max(ecpm_a, ecpm_b))


# ---------------------------------------------------------------------------
# Missing connection scoring
# ---------------------------------------------------------------------------

def _find_missing_connections(
    activity: dict[str, dict[str, dict]],
    pub_ecpm: dict[str, float],
) -> list[dict]:
    """
    Score every (publisher, demand_partner) pair that is currently missing
    and has evidence of opportunity from similar publishers.

    Returns a list of opportunity dicts sorted by score descending.
    """
    # Pre-index: for each demand partner, which publishers are they active on?
    dp_to_pubs: dict[str, list[str]] = defaultdict(list)
    for pub, dps in activity.items():
        for dp in dps:
            dp_to_pubs[dp].append(pub)

    all_publishers = set(activity.keys())
    opportunities  = []

    for target_pub in all_publishers:
        target_ecpm   = pub_ecpm.get(target_pub, 0.0)
        if target_ecpm <= 0:
            continue

        active_dps_on_target = set(activity[target_pub].keys())

        # All demand partners active anywhere on the network
        all_dps = set(dp_to_pubs.keys())
        missing_dps = all_dps - active_dps_on_target

        for dp in missing_dps:
            active_pubs_for_dp = dp_to_pubs[dp]

            # Compute similarity for each publisher the DP is active on
            similar_pubs = []
            for other_pub in active_pubs_for_dp:
                if other_pub == target_pub:
                    continue
                other_ecpm = pub_ecpm.get(other_pub, 0.0)
                sim = _similarity(target_ecpm, other_ecpm)
                if sim >= ECPM_SIMILARITY_THRESHOLD:
                    dp_data = activity[other_pub].get(dp, {})
                    if dp_data.get("revenue", 0.0) >= MIN_DP_REVENUE:
                        similar_pubs.append({
                            "publisher":  other_pub,
                            "revenue":    dp_data["revenue"],
                            "ecpm":       dp_data["ecpm"],
                            "wins":       dp_data["wins"],
                            "similarity": sim,
                        })

            if len(similar_pubs) < MIN_ACTIVE_ON_SIMILAR:
                continue

            # Opportunity score = Σ (revenue_on_similar_pub × similarity_coefficient)
            score = sum(p["revenue"] * p["similarity"] for p in similar_pubs)

            # Summary stats
            dp_revenue_similar = sum(p["revenue"] for p in similar_pubs)
            avg_ecpm_similar   = (
                sum(p["ecpm"] * p["revenue"] for p in similar_pubs) / dp_revenue_similar
                if dp_revenue_similar > 0 else 0.0
            )
            avg_similarity     = sum(p["similarity"] for p in similar_pubs) / len(similar_pubs)

            similar_pubs.sort(key=lambda p: p["revenue"], reverse=True)

            opportunities.append({
                "publisher":           target_pub,
                "demand_partner":      dp,
                "opportunity_score":   round(score, 2),
                "dp_revenue_similar":  round(dp_revenue_similar, 2),
                "n_similar_pubs":      len(similar_pubs),
                "similar_pub_names":   [p["publisher"] for p in similar_pubs[:5]],
                "avg_ecpm_similar":    round(avg_ecpm_similar, 4),
                "publisher_ecpm":      round(target_ecpm, 4),
                "avg_similarity":      round(avg_similarity, 4),
            })

    opportunities.sort(key=lambda o: o["opportunity_score"], reverse=True)
    return opportunities


# ---------------------------------------------------------------------------
# Slack Block Kit builders
# ---------------------------------------------------------------------------

def _similarity_bar(sim: float, width: int = 8) -> str:
    filled = max(0, min(int(sim * width), width))
    return "█" * filled + "░" * (width - filled)


def _build_blocks(
    opportunities: list[dict],
    actions: list[dict],          # Claude's action sentences, same order as opportunities
    lookback_days: int,
    date_label: str,
    now_label: str,
    total_publishers: int,
    total_demand_partners: int,
    total_connections: int,
    total_missing_scored: int,
) -> list:
    # Map (publisher, dp) → action sentence from Claude
    action_map: dict[tuple, str] = {
        (a["publisher"], a["demand_partner"]): a.get("action", "")
        for a in actions
    }

    total_opp = sum(o["opportunity_score"] for o in opportunities)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":electric_plug:  Demand Expansion — {date_label}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Publishers analysed:*\n{total_publishers}"},
                {"type": "mrkdwn", "text": f"*Demand partners:*\n{total_demand_partners}"},
                {"type": "mrkdwn", "text": f"*Active connections:*\n{fmt_n(total_connections)}"},
                {"type": "mrkdwn", "text": f"*Scored missing connections:*\n{fmt_n(total_missing_scored)}"},
                {"type": "mrkdwn", "text": f"*Lookback:*\n{lookback_days} days"},
                {"type": "mrkdwn", "text": f"*Total weekly opportunity:*\n{fmt_usd(total_opp)}"},
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    ":robot_face: *Top 10 Missing Demand Connections*\n"
                    f"Ranked by revenue opportunity · similarity threshold {ECPM_SIMILARITY_THRESHOLD:.0%} · "
                    f"min {MIN_ACTIVE_ON_SIMILAR} active similar publishers"
                ),
            },
        },
    ]

    rank_emojis = [":one:", ":two:", ":three:", ":four:", ":five:",
                   ":six:", ":seven:", ":eight:", ":nine:", ":keycap_ten:"]

    for i, opp in enumerate(opportunities):
        rank_e  = rank_emojis[i] if i < len(rank_emojis) else f"*{i+1}.*"
        sim_bar = _similarity_bar(opp["avg_similarity"])
        action  = action_map.get((opp["publisher"], opp["demand_partner"]), "")
        similar_str = ", ".join(opp["similar_pub_names"][:3])
        if opp["n_similar_pubs"] > 3:
            similar_str += f" +{opp['n_similar_pubs'] - 3} more"

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{rank_e}  *{opp['demand_partner']}*  →  *{opp['publisher']}*\n"
                    f"  :moneybag: opportunity: *{fmt_usd(opp['opportunity_score'])}/week*  |  "
                    f"7d rev on similar: {fmt_usd(opp['dp_revenue_similar'])}\n"
                    f"  :bar_chart: eCPM — target pub: `{fmt_usd(opp['publisher_ecpm'])}`  |  "
                    f"DP avg on peers: `{fmt_usd(opp['avg_ecpm_similar'])}`\n"
                    f"  :link: similarity: `{sim_bar}` {opp['avg_similarity']:.2f}  |  "
                    f"active on: {opp['n_similar_pubs']} similar pubs\n"
                    f"  :mag: similar publishers: _{similar_str}_"
                ),
            },
        })

        if action:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"  :white_check_mark: *Action:* {action}",
                },
            })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": (
                    f"Score = Σ(DP revenue on similar pub × similarity coefficient)  |  "
                    f"Similarity = 1 − |eCPM_A − eCPM_B| / max(eCPM_A, eCPM_B)  |  "
                    f"PGAM Intelligence · Demand Expansion · {now_label}"
                ),
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the demand expansion analysis. Designed to be scheduled weekly
    on Wednesdays, or run directly: `python -m agents.alerts.demand_expansion`.
    """
    now_et     = datetime.now(ET)
    date_label = now_et.strftime("%A, %B %-d")
    now_label  = now_et.strftime("%H:%M ET")

    # ── 1. Wednesday gate ────────────────────────────────────────────────────
    if now_et.weekday() != 2:   # 0=Mon … 2=Wed … 6=Sun
        print(
            f"[demand_expansion] Skipping — today is {now_et.strftime('%A')}, "
            f"this report runs on Wednesdays."
        )
        return

    # ── 2. Dedup ─────────────────────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[demand_expansion] Weekly report already sent today — skipping.")
        return

    # ── 3. Fetch data ─────────────────────────────────────────────────────────
    start_date = n_days_ago(LOOKBACK_DAYS)
    end_date   = today()
    print(f"[demand_expansion] Fetching {BREAKDOWN} data {start_date} → {end_date}…")
    try:
        rows = fetch(BREAKDOWN, METRICS, start_date, end_date)
    except Exception as exc:
        print(f"[demand_expansion] Fetch failed: {exc}")
        return

    if not rows:
        print("[demand_expansion] No data returned — aborting.")
        return

    print(f"[demand_expansion] {len(rows)} rows received.")

    # ── 4. Build activity matrix ──────────────────────────────────────────────
    activity = _parse_activity(rows)
    pub_ecpm = _publisher_ecpm(activity)

    total_publishers     = len(activity)
    total_demand_partners = len({dp for dps in activity.values() for dp in dps})
    total_connections    = sum(len(dps) for dps in activity.values())

    print(
        f"[demand_expansion] Matrix: {total_publishers} publishers × "
        f"{total_demand_partners} demand partners = "
        f"{total_connections} active connections."
    )

    # ── 5. Score missing connections ──────────────────────────────────────────
    all_opportunities = _find_missing_connections(activity, pub_ecpm)
    total_missing_scored = len(all_opportunities)
    top_opps = all_opportunities[:TOP_OPPORTUNITIES]

    print(
        f"[demand_expansion] {total_missing_scored} eligible missing connections. "
        f"Top opportunity: "
        f"{top_opps[0]['demand_partner']} → {top_opps[0]['publisher']} "
        f"({fmt_usd(top_opps[0]['opportunity_score'])}/wk)"
        if top_opps else "[demand_expansion] No opportunities found."
    )

    if not top_opps:
        print("[demand_expansion] No scoreable missing connections — skipping post.")
        mark_sent(ALERT_KEY)
        return

    # ── 6. Get Claude's action sentences ─────────────────────────────────────
    print(f"[demand_expansion] Sending {len(top_opps)} opportunities to Claude…")
    try:
        actions = analyze_demand_expansion(top_opps)
    except Exception as exc:
        print(f"[demand_expansion] Claude analysis failed: {exc}")
        # Fallback: templated sentences with execute steps
        actions = [
            {
                "publisher":      o["publisher"],
                "demand_partner": o["demand_partner"],
                "action": (
                    f"Add {o['demand_partner']} to {o['publisher']} — "
                    f"they are spending {fmt_usd(o['dp_revenue_similar'])}/week across "
                    f"{o['n_similar_pubs']} similar publishers at "
                    f"~{fmt_usd(o['avg_ecpm_similar'])} eCPM.\n"
                    + publisher_demand_connect(o["publisher"], o["demand_partner"])
                ),
            }
            for o in top_opps
        ]

    # ── 7. Build and post Slack message ───────────────────────────────────────
    blocks = _build_blocks(
        opportunities=top_opps,
        actions=actions,
        lookback_days=LOOKBACK_DAYS,
        date_label=date_label,
        now_label=now_label,
        total_publishers=total_publishers,
        total_demand_partners=total_demand_partners,
        total_connections=total_connections,
        total_missing_scored=total_missing_scored,
    )

    total_opp = sum(o["opportunity_score"] for o in top_opps)
    fallback  = (
        f"Demand Expansion ({date_label}): {len(top_opps)} missing connections identified. "
        f"Total weekly opportunity: {fmt_usd(total_opp)}. "
        f"Top: {top_opps[0]['demand_partner']} → {top_opps[0]['publisher']} "
        f"({fmt_usd(top_opps[0]['opportunity_score'])}/wk)."
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(
        f"[demand_expansion] Report sent — {len(top_opps)} opportunities, "
        f"{fmt_usd(total_opp)} total weekly opportunity."
    )


if __name__ == "__main__":
    run()
