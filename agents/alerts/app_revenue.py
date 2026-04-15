"""
agents/alerts/app_revenue.py

Daily (6 AM ET) app/bundle revenue mover report.

Compares yesterday's completed revenue against the day-before-yesterday to
surface bundles with significant day-over-day swings. Since this agent runs
at 6 AM when today's data is only a few hours old, it always compares two
fully-completed days:

  primary_date  = yesterday       (the day that just finished)
  compare_date  = 2 days ago      (the baseline)

Pipeline
--------
  1. Fetch BUNDLE metrics for both dates.
  2. Filter to bundles with >= MIN_BUNDLE_REVENUE on the primary date
     or the compare date (catches bundles that went to zero).
  3. Calculate revenue % change; keep movers >= MOVER_THRESHOLD (30%).
  4. Classify each mover as supply-side, demand-side, or mixed using
     eCPM shift vs impression volume shift heuristics.
  5. For movers, fetch BUNDLE+PUBLISHER (supply) and
     BUNDLE+DEMAND_PARTNER_NAME (demand) context for both dates.
  6. Build rising / falling sections and post to Slack.

Classification heuristic
------------------------
  Let  Δimps  = abs impression % change
       ΔeCPM  = abs eCPM % change

  If both Δimps > CLASSIFY_THRESHOLD and ΔeCPM > CLASSIFY_THRESHOLD → MIXED
  Elif Δimps >= ΔeCPM  → SUPPLY  (volume moved, price held)
  Else                  → DEMAND  (price moved, volume held)

Deduplication
-------------
  Alert key "app_revenue_movers" fires once per day via core/slack.py.
"""

from collections import defaultdict
from datetime import datetime

import pytz

from core.api import fetch, n_days_ago, sf, fmt_usd, fmt_n, pct
from core.config import THRESHOLDS
from core.slack import already_sent_today, mark_sent, send_blocks
from intelligence.claude_analyst import analyze_app_revenue_movers

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BUNDLE_BREAKDOWN  = "BUNDLE"
SUPPLY_BREAKDOWN  = "BUNDLE,PUBLISHER"
DEMAND_BREAKDOWN  = "BUNDLE,DEMAND_PARTNER"
BUNDLE_METRICS    = ["GROSS_REVENUE", "IMPRESSIONS", "WINS", "BIDS"]
CONTEXT_METRICS   = ["GROSS_REVENUE", "IMPRESSIONS"]

ALERT_KEY         = "app_revenue_movers"
MIN_BUNDLE_REVENUE = 50.0    # minimum $ on either day to be considered
MOVER_THRESHOLD   = 30.0     # minimum abs % revenue change to surface
CLASSIFY_THRESHOLD = 15.0    # both eCPM and impressions must shift this much to be MIXED
MAX_MOVERS_SHOWN  = 8        # cap each section to keep Slack message readable
TOP_CONTEXT_ROWS  = 3        # top N publishers / demand partners to show per bundle

ET = pytz.timezone("US/Eastern")


# ---------------------------------------------------------------------------
# Row parsing helpers
# ---------------------------------------------------------------------------

def _extract(row: dict, *keys) -> float:
    for k in keys:
        if k in row:
            return sf(row[k])
    return 0.0


def _bundle_name(row: dict) -> str:
    return str(
        row.get("BUNDLE") or row.get("bundle")
        or row.get("appBundle") or row.get("app_bundle") or "unknown"
    )


def _parse_bundle_rows(rows: list) -> dict[str, dict]:
    """
    Collapse BUNDLE breakdown rows into a dict keyed by bundle ID.
    Returns derived eCPM and win_rate alongside raw totals.
    """
    by_bundle: dict[str, dict] = {}
    for row in rows:
        bname = _bundle_name(row)
        revenue     = _extract(row, "GROSS_REVENUE", "gross_revenue")
        impressions = _extract(row, "IMPRESSIONS",   "impressions")
        wins        = _extract(row, "WINS",           "wins")
        bids        = _extract(row, "BIDS",           "bids")
        ecpm        = (revenue / impressions * 1000) if impressions > 0 else 0.0
        win_rate    = pct(wins, bids)
        by_bundle[bname] = {
            "revenue":     revenue,
            "impressions": impressions,
            "wins":        wins,
            "bids":        bids,
            "ecpm":        ecpm,
            "win_rate":    win_rate,
        }
    return by_bundle


def _parse_context_rows(rows: list, partner_key: str) -> dict[str, list[dict]]:
    """
    Parse multi-dimensional (BUNDLE + partner) breakdown rows.

    Returns dict keyed by bundle, each value is a list of
    {name, revenue, impressions} dicts sorted by revenue desc.
    """
    by_bundle: dict[str, list] = defaultdict(list)
    for row in rows:
        bname   = _bundle_name(row)
        partner = str(
            row.get(partner_key) or row.get(partner_key.lower())
            or row.get("partnerName") or row.get("partner_name") or "Unknown"
        )
        revenue     = _extract(row, "GROSS_REVENUE", "gross_revenue")
        impressions = _extract(row, "IMPRESSIONS",   "impressions")
        by_bundle[bname].append({
            "name":        partner,
            "revenue":     revenue,
            "impressions": impressions,
        })
    # Sort each bundle's partners by revenue desc
    for bname in by_bundle:
        by_bundle[bname].sort(key=lambda x: x["revenue"], reverse=True)
    return dict(by_bundle)


# ---------------------------------------------------------------------------
# Mover classification
# ---------------------------------------------------------------------------

def _pct_change(new: float, old: float) -> float:
    """Signed % change from old to new. Returns 0 if old is zero."""
    if old == 0:
        return 100.0 if new > 0 else 0.0
    return (new - old) / old * 100.0


def _classify(primary: dict, compare: dict) -> str:
    """
    Return 'SUPPLY', 'DEMAND', or 'MIXED' based on whether the revenue swing
    was driven by impression volume or eCPM shift.
    """
    delta_imps = abs(_pct_change(primary["impressions"], compare["impressions"]))
    delta_ecpm = abs(_pct_change(primary["ecpm"],        compare["ecpm"]))

    if delta_imps >= CLASSIFY_THRESHOLD and delta_ecpm >= CLASSIFY_THRESHOLD:
        return "MIXED"
    return "SUPPLY" if delta_imps >= delta_ecpm else "DEMAND"


# ---------------------------------------------------------------------------
# Context diff helpers
# ---------------------------------------------------------------------------

def _top_context_diff(
    primary_ctx: list[dict],
    compare_ctx: list[dict],
    top_n: int = TOP_CONTEXT_ROWS,
) -> list[dict]:
    """
    Return the top_n partners by primary revenue, annotated with DoD change.
    """
    compare_by_name = {r["name"]: r for r in compare_ctx}
    results = []
    for row in primary_ctx[:top_n]:
        prev = compare_by_name.get(row["name"], {"revenue": 0.0, "impressions": 0.0})
        results.append({
            "name":       row["name"],
            "revenue":    row["revenue"],
            "rev_change": _pct_change(row["revenue"], prev["revenue"]),
            "is_new":     prev["revenue"] == 0.0,
        })
    return results


def _disappeared_partners(
    primary_ctx: list[dict],
    compare_ctx: list[dict],
    top_n: int = TOP_CONTEXT_ROWS,
) -> list[dict]:
    """Return top_n partners that were present yesterday but are gone today."""
    primary_names = {r["name"] for r in primary_ctx}
    gone = [r for r in compare_ctx if r["name"] not in primary_names]
    gone.sort(key=lambda x: x["revenue"], reverse=True)
    return gone[:top_n]


# ---------------------------------------------------------------------------
# Slack formatting helpers
# ---------------------------------------------------------------------------

def _change_badge(pct_chg: float) -> str:
    if pct_chg >= 0:
        return f":small_green_square: +{pct_chg:.1f}%"
    return f":small_red_triangle_down: {pct_chg:.1f}%"


def _classify_badge(classification: str) -> str:
    return {
        "SUPPLY": ":truck: Supply",
        "DEMAND": ":briefcase: Demand",
        "MIXED":  ":twisted_rightwards_arrows: Mixed",
    }.get(classification, classification)


def _context_lines(
    supply_diff: list[dict],
    demand_diff: list[dict],
    supply_gone: list[dict],
    demand_gone: list[dict],
    direction: str,   # "up" or "down"
) -> str:
    """Build a compact context block for one bundle."""
    lines = []

    if supply_diff:
        pub_parts = []
        for p in supply_diff:
            tag = " _(new)_" if p["is_new"] else f" ({_change_badge(p['rev_change'])})"
            pub_parts.append(f"{p['name']}{tag}")
        lines.append(f"  :busts_in_silhouette: *Supply:* {' · '.join(pub_parts)}")

    if supply_gone and direction == "down":
        gone_names = ", ".join(p["name"] for p in supply_gone)
        lines.append(f"  :x: *Lost publishers:* {gone_names}")

    if demand_diff:
        dp_parts = []
        for p in demand_diff:
            tag = " _(new)_" if p["is_new"] else f" ({_change_badge(p['rev_change'])})"
            dp_parts.append(f"{p['name']}{tag}")
        lines.append(f"  :money_with_wings: *Demand:* {' · '.join(dp_parts)}")

    if demand_gone and direction == "down":
        gone_names = ", ".join(p["name"] for p in demand_gone)
        lines.append(f"  :x: *Lost demand:* {gone_names}")

    return "\n".join(lines) if lines else "  _No context available._"


def _mover_block(
    bundle: str,
    primary: dict,
    compare: dict,
    rev_change: float,
    classification: str,
    supply_diff: list[dict],
    demand_diff: list[dict],
    supply_gone: list[dict],
    demand_gone: list[dict],
    direction: str,
) -> dict:
    """Build a single Slack section block for one mover."""
    delta_imps = _pct_change(primary["impressions"], compare["impressions"])
    delta_ecpm = _pct_change(primary["ecpm"],        compare["ecpm"])

    header = (
        f"*{bundle}*   "
        f"{_change_badge(rev_change)}   "
        f"{_classify_badge(classification)}\n"
        f"  rev: {fmt_usd(compare['revenue'])} → *{fmt_usd(primary['revenue'])}*   "
        f"imps: {_change_badge(delta_imps)}   "
        f"eCPM: {_change_badge(delta_ecpm)}\n"
    )
    context = _context_lines(supply_diff, demand_diff, supply_gone, demand_gone, direction)

    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": header + context},
    }


def _build_slack_blocks(
    rising: list[dict],
    falling: list[dict],
    primary_label: str,
    compare_label: str,
    now_label: str,
    total_checked: int,
    claude_analysis: str = "",
) -> list:
    n_rising  = len(rising)
    n_falling = len(falling)
    top_mover = (rising[0] if rising else falling[0]) if (rising or falling) else None
    status_line = (
        f":iphone: *App Revenue Movers — {primary_label} vs {compare_label}:* "
        f"{n_rising} rising, {n_falling} falling above {MOVER_THRESHOLD:.0f}% threshold."
        + (
            f"  Biggest move: *{top_mover['bundle']}* "
            f"({top_mover['rev_change']:+.0f}%, "
            f"${top_mover['compare']['revenue']:,.0f} → "
            f"${top_mover['primary']['revenue']:,.0f}, "
            f"{_classify_badge(top_mover['classification'])})."
            if top_mover else "  No significant movers today."
        )
    )

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":iphone:  App Revenue Movers — {primary_label}",
                "emoji": True,
            },
        },
        # ── Status line ──────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": status_line},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Comparing *{primary_label}* vs *{compare_label}*  |  "
                        f"{total_checked} bundles scanned  |  "
                        f"threshold: >{MOVER_THRESHOLD:.0f}% change  |  "
                        f"min revenue: {fmt_usd(MIN_BUNDLE_REVENUE)}"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]

    # ── Claude's analysis is the centerpiece — before the data lists ─────────
    if claude_analysis:
        blocks += [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":robot_face: *Claude's Analysis*\n{claude_analysis}",
                },
            },
            {"type": "divider"},
        ]

    # ── Rising apps ──────────────────────────────────────────────────────────
    if rising:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":chart_with_upwards_trend: *Rising Apps ({len(rising)} movers)*",
            },
        })
        for m in rising[:MAX_MOVERS_SHOWN]:
            blocks.append(m["block"])
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":chart_with_upwards_trend: *Rising Apps* — none above threshold."},
        })

    blocks.append({"type": "divider"})

    # ── Falling apps ─────────────────────────────────────────────────────────
    if falling:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f":chart_with_downwards_trend: *Falling Apps ({len(falling)} movers)*",
            },
        })
        for m in falling[:MAX_MOVERS_SHOWN]:
            blocks.append(m["block"])
    else:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": ":chart_with_downwards_trend: *Falling Apps* — none above threshold."},
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"PGAM Intelligence · App Revenue Agent · {now_label}",
            }
        ],
    })

    return blocks


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    """
    Execute the app revenue mover report. Designed to be called by a scheduler
    at ~6 AM ET or run directly: `python -m agents.alerts.app_revenue`.
    """
    now_et = datetime.now(ET)
    now_label     = now_et.strftime("%H:%M ET")
    primary_date  = n_days_ago(1)   # yesterday — fully completed
    compare_date  = n_days_ago(2)   # day before — the baseline
    primary_label = datetime.strptime(primary_date, "%Y-%m-%d").strftime("%b %-d")
    compare_label = datetime.strptime(compare_date, "%Y-%m-%d").strftime("%b %-d")

    # ── 1. Dedup ─────────────────────────────────────────────────────────────
    if already_sent_today(ALERT_KEY):
        print("[app_revenue] Report already sent today — skipping.")
        return

    # ── 2. Fetch bundle metrics for both days ─────────────────────────────────
    print(f"[app_revenue] Fetching {BUNDLE_BREAKDOWN} metrics: {compare_date} and {primary_date}…")
    try:
        primary_rows = fetch(BUNDLE_BREAKDOWN, BUNDLE_METRICS, primary_date,  primary_date)
        compare_rows = fetch(BUNDLE_BREAKDOWN, BUNDLE_METRICS, compare_date,  compare_date)
    except Exception as exc:
        print(f"[app_revenue] Bundle fetch failed: {exc}")
        return

    primary_bundles = _parse_bundle_rows(primary_rows)
    compare_bundles = _parse_bundle_rows(compare_rows)

    print(
        f"[app_revenue] {len(primary_bundles)} bundles on {primary_label}, "
        f"{len(compare_bundles)} bundles on {compare_label}."
    )

    # ── 3. Identify significant movers ───────────────────────────────────────
    all_bundles = set(primary_bundles) | set(compare_bundles)
    candidates  = []

    for bname in all_bundles:
        pri  = primary_bundles.get(bname, {"revenue": 0, "impressions": 0, "wins": 0, "bids": 0, "ecpm": 0, "win_rate": 0})
        comp = compare_bundles.get(bname, {"revenue": 0, "impressions": 0, "wins": 0, "bids": 0, "ecpm": 0, "win_rate": 0})

        # Must meet minimum revenue threshold on at least one day
        if max(pri["revenue"], comp["revenue"]) < MIN_BUNDLE_REVENUE:
            continue

        rev_change = _pct_change(pri["revenue"], comp["revenue"])

        if abs(rev_change) < MOVER_THRESHOLD:
            continue

        classification = _classify(pri, comp)
        candidates.append({
            "bundle":         bname,
            "primary":        pri,
            "compare":        comp,
            "rev_change":     rev_change,
            "classification": classification,
        })

    print(f"[app_revenue] {len(candidates)} significant movers found (>{MOVER_THRESHOLD:.0f}% change).")

    if not candidates:
        print("[app_revenue] No significant movers — skipping alert.")
        # Still mark as sent so we don't keep rechecking this morning
        mark_sent(ALERT_KEY)
        return

    # Sort movers into rising / falling by abs revenue change
    rising_candidates  = sorted(
        [c for c in candidates if c["rev_change"] >  0],
        key=lambda x: x["rev_change"], reverse=True,
    )
    falling_candidates = sorted(
        [c for c in candidates if c["rev_change"] <= 0],
        key=lambda x: x["rev_change"],
    )

    # ── 4. Fetch supply and demand context (one call per breakdown per day) ───
    mover_bundles = {c["bundle"] for c in candidates}

    def _safe_fetch_context(breakdown, date_str, label):
        try:
            rows = fetch(breakdown, CONTEXT_METRICS, date_str, date_str)
            return rows
        except Exception as exc:
            print(f"[app_revenue] Context fetch {breakdown}/{label} failed (non-fatal): {exc}")
            return []

    supply_primary_rows  = _safe_fetch_context(SUPPLY_BREAKDOWN,  primary_date,  "supply/primary")
    supply_compare_rows  = _safe_fetch_context(SUPPLY_BREAKDOWN,  compare_date,  "supply/compare")
    demand_primary_rows  = _safe_fetch_context(DEMAND_BREAKDOWN,  primary_date,  "demand/primary")
    demand_compare_rows  = _safe_fetch_context(DEMAND_BREAKDOWN,  compare_date,  "demand/compare")

    supply_primary  = _parse_context_rows(supply_primary_rows,  "PUBLISHER_NAME")
    supply_compare  = _parse_context_rows(supply_compare_rows,  "PUBLISHER_NAME")
    demand_primary  = _parse_context_rows(demand_primary_rows,  "DEMAND_PARTNER_NAME")
    demand_compare  = _parse_context_rows(demand_compare_rows,  "DEMAND_PARTNER_NAME")

    # ── 5. Build per-mover Slack blocks ──────────────────────────────────────
    def _enrich(candidates_list: list, direction: str) -> list:
        enriched = []
        for c in candidates_list:
            b = c["bundle"]

            sp_pri  = supply_primary.get(b, [])
            sp_comp = supply_compare.get(b, [])
            dm_pri  = demand_primary.get(b, [])
            dm_comp = demand_compare.get(b, [])

            s_diff  = _top_context_diff(sp_pri, sp_comp)
            d_diff  = _top_context_diff(dm_pri, dm_comp)
            s_gone  = _disappeared_partners(sp_pri, sp_comp)
            d_gone  = _disappeared_partners(dm_pri, dm_comp)

            block = _mover_block(
                bundle=b,
                primary=c["primary"],
                compare=c["compare"],
                rev_change=c["rev_change"],
                classification=c["classification"],
                supply_diff=s_diff,
                demand_diff=d_diff,
                supply_gone=s_gone,
                demand_gone=d_gone,
                direction=direction,
            )
            enriched.append({**c, "block": block})
        return enriched

    rising  = _enrich(rising_candidates,  "up")
    falling = _enrich(falling_candidates, "down")

    # ── 6. Call Claude with the movers for analysis ──────────────────────────
    claude_analysis = ""
    try:
        movers_for_claude = [
            {
                "bundle":         m["bundle"],
                "rev_change_pct": round(m["rev_change"], 1),
                "revenue_today":  round(m["primary"]["revenue"], 2),
                "revenue_prior":  round(m["compare"]["revenue"], 2),
                "classification": m["classification"],
                "ecpm_change_pct": round(_pct_change(m["primary"]["ecpm"], m["compare"]["ecpm"]), 1),
                "imps_change_pct": round(_pct_change(m["primary"]["impressions"], m["compare"]["impressions"]), 1),
            }
            for m in (rising + falling)[:10]
        ]
        claude_analysis = analyze_app_revenue_movers(
            movers=movers_for_claude,
            primary_label=primary_label,
            compare_label=compare_label,
        )
    except Exception as exc:
        print(f"[app_revenue] Claude analysis failed (non-fatal): {exc}")
        # Specific fallback with real numbers
        parts = []
        for m in (rising[:2] + falling[:2]):
            direction = "up" if m["rev_change"] > 0 else "down"
            cls = m["classification"]
            parts.append(
                f"• *{m['bundle']}* {m['rev_change']:+.0f}% ({cls.lower()}-side): "
                f"${m['compare']['revenue']:,.0f} → ${m['primary']['revenue']:,.0f}. "
                f"{'Investigate supply gain — add demand partners to capture upside.' if direction == 'up' else 'Check for publisher outage or demand partner drop — restore lost connections.'}"
            )
        claude_analysis = "\n".join(parts)

    # ── 7. Build and post Slack message ──────────────────────────────────────
    blocks = _build_slack_blocks(
        rising=rising,
        falling=falling,
        primary_label=primary_label,
        compare_label=compare_label,
        now_label=now_label,
        total_checked=len(all_bundles),
        claude_analysis=claude_analysis,
    )

    n_rising  = len(rising)
    n_falling = len(falling)
    fallback  = (
        f"App Revenue Movers ({primary_label} vs {compare_label}): "
        f"{n_rising} rising, {n_falling} falling bundles above {MOVER_THRESHOLD:.0f}% threshold."
    )

    send_blocks(blocks=blocks, text=fallback)
    mark_sent(ALERT_KEY)
    print(f"[app_revenue] Report sent — {n_rising} rising, {n_falling} falling.")


if __name__ == "__main__":
    run()
