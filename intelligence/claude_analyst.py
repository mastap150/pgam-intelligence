import time
import hashlib
import json
import anthropic
from dotenv import load_dotenv

load_dotenv(override=True)

from core.ui_nav import NAV_INSTRUCTIONS

MODEL = "claude-sonnet-4-20250514"
CACHE_TTL = 300  # 5 minutes in seconds

_client = None
_cache: dict[str, tuple[str, float]] = {}  # key -> (response_text, timestamp)


def _get_client():
    global _client
    if _client is None:
        _client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env
    return _client


def _cache_key(*args) -> str:
    """Build a deterministic cache key from the function arguments."""
    payload = json.dumps(args, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _get_cached(key: str):
    """Return cached value if it exists and has not expired, else None."""
    if key in _cache:
        text, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return text
        del _cache[key]
    return None


def _set_cached(key: str, text: str):
    _cache[key] = (text, time.time())


def _ask(system: str, user: str) -> str:
    """Send a single-turn message to Claude and return the response text."""
    response = _get_client().messages.create(
        model=MODEL,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return next(
        (block.text for block in response.content if block.type == "text"), ""
    )


# ---------------------------------------------------------------------------
# Public analyst functions
# ---------------------------------------------------------------------------

def analyze_revenue_pacing(
    today_spend: float,
    expected_spend: float,
    yesterday_spend: float,
    hour_et: int,
    traffic_mix: dict,
) -> str:
    """
    Ask Claude why revenue is behind pace and what actions to take.

    Args:
        today_spend    (float): Revenue accumulated so far today ($).
        expected_spend (float): Expected revenue at this hour of day ($).
        yesterday_spend(float): Full-day revenue yesterday ($).
        hour_et        (int):   Current hour in US/Eastern (0-23).
        traffic_mix    (dict):  Breakdown of spend by channel/publisher/etc.

    Returns:
        str: Claude's diagnostic and recommended actions.
    """
    key = _cache_key("revenue_pacing", today_spend, expected_spend,
                     yesterday_spend, hour_et, traffic_mix)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are an expert programmatic advertising revenue analyst. "
        "You specialize in diagnosing revenue pacing issues and providing "
        "actionable, specific recommendations for ad ops teams. "
        "Be concise and direct. Focus on root causes and concrete next steps."
    )

    pct_of_expected = (
        (today_spend / expected_spend * 100) if expected_spend else 0.0
    )
    behind_by = expected_spend - today_spend

    user = f"""Analyze the following revenue pacing situation and explain why revenue may be behind and what the team should do immediately.

Current hour (ET): {hour_et}:00
Today's revenue so far: ${today_spend:,.2f}
Expected revenue at this hour: ${expected_spend:,.2f}
Pacing at: {pct_of_expected:.1f}% of expected
Behind by: ${behind_by:,.2f}
Yesterday's full-day revenue: ${yesterday_spend:,.2f}

Traffic mix breakdown:
{json.dumps(traffic_mix, indent=2)}

Provide:
1. The most likely root causes (2-3 bullet points)
2. Immediate actions the team should take (2-3 bullet points)
3. A one-sentence risk summary if no action is taken"""

    result = _ask(system, user)
    _set_cached(key, result)
    return result


def analyze_floor_gaps(raise_list: list, lower_list: list) -> str:
    """
    Ask Claude to prioritize CPM floor price actions.

    Args:
        raise_list (list[dict]): Publishers/placements where floor should be raised,
                                 each with keys like name, current_floor, suggested_floor, revenue.
        lower_list (list[dict]): Publishers/placements where floor should be lowered,
                                 each with keys like name, current_floor, suggested_floor, fill_rate.

    Returns:
        str: Claude's prioritized action plan for floor adjustments.
    """
    key = _cache_key("floor_gaps", raise_list, lower_list)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are a programmatic advertising yield optimization specialist. "
        "You analyze CPM floor price data and prioritize actions to maximize "
        "revenue while maintaining healthy fill rates. Be specific and actionable. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    user = f"""Review the following CPM floor price opportunities and provide a prioritized action plan.

FLOOR RAISES (high CPM, underpriced inventory):
{json.dumps(raise_list, indent=2) if raise_list else "None identified."}

FLOOR REDUCTIONS (low fill rate, floors may be too aggressive):
{json.dumps(lower_list, indent=2) if lower_list else "None identified."}

Provide:
1. Top 3 highest-impact floor raises to action first, with brief reasoning and → *Execute:* navigation step
2. Top 3 most urgent floor reductions to action first, with brief reasoning and → *Execute:* navigation step
3. Any patterns or systemic issues worth flagging to the broader team"""

    result = _ask(system, user)
    _set_cached(key, result)
    return result


def synthesize_daily_brief(
    summary: dict,
    fix: dict,
    anomalies: list,
    opp_fill: dict,
    date_str: str,
) -> str:
    """
    Produce a 3-paragraph executive summary connecting signals across all agents.

    Args:
        summary   (dict): High-level revenue / performance metrics for the day.
        fix       (dict): Floor fix recommendations from the floor agent.
        anomalies (list): Anomalies detected (publisher drops, spend spikes, etc.).
        opp_fill  (dict): Opportunity and fill rate metrics.
        date_str  (str):  Report date as "YYYY-MM-DD".

    Returns:
        str: A 3-paragraph executive brief suitable for leadership.
    """
    key = _cache_key("daily_brief", summary, fix, anomalies, opp_fill, date_str)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are a senior programmatic advertising analyst writing a concise "
        "daily intelligence brief for company leadership. "
        "Connect signals across revenue pacing, floor pricing, anomalies, and fill rates "
        "into a coherent narrative. Write exactly 3 paragraphs. "
        "Paragraph 1: overall business performance. "
        "Paragraph 2: key risks or anomalies requiring attention. "
        "Paragraph 3: recommended priorities for the team today. "
        "Use plain business language — no bullet points, no headers."
    )

    user = f"""Write a 3-paragraph executive daily brief for {date_str} using the following intelligence signals.

REVENUE & PERFORMANCE SUMMARY:
{json.dumps(summary, indent=2)}

FLOOR PRICE RECOMMENDATIONS:
{json.dumps(fix, indent=2)}

ANOMALIES DETECTED:
{json.dumps(anomalies, indent=2) if anomalies else "No significant anomalies detected."}

OPPORTUNITY & FILL RATE METRICS:
{json.dumps(opp_fill, indent=2)}

Write exactly 3 paragraphs as instructed."""

    result = _ask(system, user)
    _set_cached(key, result)
    return result


def analyze_demand_saturation(publishers: list) -> list[dict]:
    """
    Given up to 10 underserved publishers (low bid density), ask Claude to
    select the 3 highest-priority opportunities and return one specific action
    for each.

    Args:
        publishers (list[dict]): Each dict must contain:
            name              (str)
            bid_density       (float)  current bids-per-opportunity
            opportunities_7d  (int)
            revenue_7d        (float)
            ecpm              (float)
            win_rate_pct      (float)
            revenue_opp       (float)  estimated $ uplift if density → 5

    Returns:
        list[dict]: Exactly 3 items, each:
            {
              "publisher":  str,
              "action":     str,   # one specific, concrete recommendation
              "reasoning":  str,   # brief (1-2 sentence) justification
            }
        Falls back to the top 3 by revenue_opp if Claude's response cannot
        be parsed.
    """
    key = _cache_key("demand_saturation", publishers)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a programmatic advertising supply optimization expert. "
        "You specialise in identifying publishers where DSP bid density is "
        "too low and recommending specific, technical actions to increase "
        "auction participation. Your recommendations must be concrete — "
        "name the exact lever (e.g. 'add 3 DSP seat IDs', 'lower floor to $0.40', "
        "'expand geo targeting to LATAM', 'enable header bidding adapter X'). "
        "Respond ONLY with a valid JSON array — no prose, no markdown fences. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    user = f"""Below are up to 10 publishers where bid density (bids per opportunity) is below 3.0,
meaning DSPs want to buy but are not getting enough chances to bid.

Select the 3 publishers that represent the highest-priority opportunities this week.
For each, provide one specific, actionable recommendation.

Publishers:
{json.dumps(publishers, indent=2)}

Return ONLY a JSON array with exactly 3 objects in this format:
[
  {{
    "publisher": "<exact publisher name from the list>",
    "action": "<one specific action>\\n→ *Execute:* <SSP navigation steps using the patterns above>",
    "reasoning": "<1-2 sentence justification based on the data>"
  }},
  ...
]"""

    raw = _ask(system, user)

    # Parse Claude's JSON response; fall back gracefully
    try:
        clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, list) and len(parsed) >= 1:
            result = parsed[:3]
            _set_cached(key, json.dumps(result))
            return result
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # Fallback: top 3 by revenue_opp with a generic action
    fallback = sorted(publishers, key=lambda p: p.get("revenue_opp", 0), reverse=True)[:3]
    result = [
        {
            "publisher": p["name"],
            "action":    "Review DSP seat configuration and floor price to increase bid participation.",
            "reasoning": (
                f"Bid density {p['bid_density']:.2f} with {p['opportunities_7d']:,} opportunities "
                f"represents significant untapped revenue."
            ),
        }
        for p in fallback
    ]
    _set_cached(key, json.dumps(result))
    return result


def analyze_floor_elasticity(opportunities: list) -> list[dict]:
    """
    Given up to 10 floor price optimization opportunities, ask Claude to rank
    them by confidence × impact and return a prioritised list with context.

    Args:
        opportunities (list[dict]): Each dict contains:
            publisher         (str)
            current_floor     (float)
            optimal_floor     (float)
            current_daily_rev (float)
            projected_daily_rev (float)
            daily_rev_uplift  (float)   projected - current
            confidence        (float)   0.0 – 1.0
            r_squared         (float)   model fit quality
            days_of_data      (int)
            avg_daily_bids    (float)
            avg_bid_price     (float)
            direction         (str)     "raise" | "lower"

    Returns:
        list[dict]: Up to 10 items, sorted by priority, each:
            {
              "publisher":    str,
              "rank":         int,
              "priority":     "high" | "medium" | "low",
              "rationale":    str,   # 1-2 sentence explanation
              "caution":      str,   # risk or caveat to flag (empty string if none)
            }
        Falls back to original order if parsing fails.
    """
    key = _cache_key("floor_elasticity", opportunities)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a programmatic advertising yield optimization expert with deep "
        "knowledge of floor price mechanics. You evaluate mathematical floor price "
        "optimization models and rank opportunities by their real-world actionability, "
        "accounting for model confidence, revenue impact, direction of change, and "
        "practical risks (e.g. raising a floor for a publisher with volatile traffic "
        "is riskier than for a stable one). "
        "Respond ONLY with a valid JSON array — no prose, no markdown fences. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    user = f"""Below are floor price optimization opportunities derived from 30-day elasticity models.
Each has a confidence score (0-1) based on data consistency and model fit (R²).

Rank these opportunities from highest to lowest priority for the ad ops team to act on this week.
Consider: revenue impact, confidence, direction (raising floor is riskier than lowering), and days of data.

Opportunities:
{json.dumps(opportunities, indent=2)}

Return ONLY a JSON array with one object per opportunity in this format:
[
  {{
    "publisher":  "<exact name>",
    "rank":       <1-based integer>,
    "priority":   "<high|medium|low>",
    "rationale":  "<1-2 sentence explanation>\\n→ *Execute:* <SSP floor price navigation steps>",
    "caution":    "<risk or caveat, or empty string if none>"
  }},
  ...
]"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, list) and len(parsed) >= 1:
            result = parsed
            _set_cached(key, json.dumps(result))
            return result
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # Fallback: original order, mark all as medium priority
    result = [
        {
            "publisher": o["publisher"],
            "rank":      i + 1,
            "priority":  "medium",
            "rationale": (
                f"Projected uplift of {o['direction']} floor from "
                f"${o['current_floor']:.4f} to ${o['optimal_floor']:.4f} "
                f"(+${o['daily_rev_uplift']:.2f}/day, confidence {o['confidence']:.2f})."
            ),
            "caution": "",
        }
        for i, o in enumerate(opportunities)
    ]
    _set_cached(key, json.dumps(result))
    return result


def analyze_publisher_monetization(publishers: list, benchmarks: dict) -> dict:
    """
    Classify new publishers into those needing demand partner intervention vs
    natural self-serve performers, based on their revenue ramp trajectories.

    Args:
        publishers (list[dict]): Each dict contains:
            name                (str)
            days_since_start    (int)
            status              (str)   "Outperforming"|"On Track"|"At Risk"
            cumulative_rev      (float)
            current_daily_rev   (float)
            vs_avg_pct          (float) % above(+) or below(-) avg curve
            revenue_at_risk     (float) cumulative gap vs avg (At Risk only)
            trajectory_7d       (list[float]) last 7 days of revenue

        benchmarks (dict): Benchmark reference values at key milestones.

    Returns:
        dict: {
            "demand_attention": [
                {"publisher": str, "reason": str, "urgency": "immediate"|"this_week"}
            ],
            "self_serve": [
                {"publisher": str, "reason": str}
            ],
            "summary": str
        }
    """
    key = _cache_key("pub_monetization", publishers, benchmarks)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a publisher development expert at a programmatic advertising company. "
        "You analyse new publisher revenue ramp data to determine which publishers need "
        "hands-on demand partner configuration (connecting more DSPs, adjusting floors, "
        "fixing ad unit setup) and which are ramping naturally through self-serve. "
        "Be specific about WHY a publisher needs intervention — reference their trajectory data. "
        "Respond ONLY with valid JSON — no prose, no markdown fences."
    )

    user = f"""Analyse the following new publisher monetization trajectories against the benchmark curves.

BENCHMARK REFERENCE (average new publisher):
{json.dumps(benchmarks, indent=2)}

NEW PUBLISHERS:
{json.dumps(publishers, indent=2)}

Classify each publisher into:
  demand_attention  — needs ad ops / demand partner intervention to unlock revenue
  self_serve        — ramping naturally; monitor but no immediate action needed

For demand_attention publishers, set urgency:
  "immediate"   — At Risk, significant revenue being lost now
  "this_week"   — On Track or Outperforming but trajectory suggests future risk

Return ONLY a JSON object in this exact format:
{{
  "demand_attention": [
    {{"publisher": "<name>", "reason": "<specific reason based on data>", "urgency": "<immediate|this_week>"}}
  ],
  "self_serve": [
    {{"publisher": "<name>", "reason": "<why they don't need intervention>"}}
  ],
  "summary": "<one paragraph connecting the patterns across all new publishers>"
}}"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, dict) and "demand_attention" in parsed:
            _set_cached(key, json.dumps(parsed))
            return parsed
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    fallback = {
        "demand_attention": [
            {
                "publisher": p["name"],
                "reason":    f"Revenue {p['vs_avg_pct']:.1f}% below average curve after {p['days_since_start']} days.",
                "urgency":   "immediate",
            }
            for p in publishers if p.get("status") == "At Risk"
        ],
        "self_serve": [
            {"publisher": p["name"], "reason": "Trajectory meets or exceeds average benchmark."}
            for p in publishers if p.get("status") != "At Risk"
        ],
        "summary": (
            f"{len(publishers)} new publishers analysed. "
            f"{sum(1 for p in publishers if p['status'] == 'At Risk')} flagged At Risk."
        ),
    }
    _set_cached(key, json.dumps(fallback))
    return fallback


def analyze_demand_expansion(opportunities: list) -> list[dict]:
    """
    For each missing demand-partner ↔ publisher connection, write one crisp,
    specific action sentence that an ad ops person can act on immediately.

    Args:
        opportunities (list[dict]): Each dict contains:
            publisher              (str)
            demand_partner         (str)
            opportunity_score      (float)  estimated weekly revenue ($)
            dp_revenue_similar     (float)  demand partner's 7d revenue on similar pubs
            n_similar_pubs         (int)    how many similar pubs the DP is already on
            similar_pub_names      (list[str])
            avg_ecpm_similar       (float)  DP's avg eCPM on similar pubs
            publisher_ecpm         (float)  target publisher's overall eCPM
            avg_similarity         (float)  0-1 similarity coefficient

    Returns:
        list[dict]: Same length as input, each:
            {
              "publisher":     str,
              "demand_partner": str,
              "action":        str,   # one-sentence, specific action
            }
        Falls back to templated sentences on parse failure.
    """
    key = _cache_key("demand_expansion", opportunities)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a programmatic advertising partnerships manager. "
        "You write clear, specific action items for ad ops teams "
        "to connect demand partners to new publisher inventory. "
        "Each action must name the demand partner, the publisher, the estimated "
        "revenue opportunity, the number of similar publishers already connected, "
        "and the eCPM context. Use active voice. Be concise. "
        "Respond ONLY with a valid JSON array — no prose, no markdown. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    user = f"""Write one specific action for each of the following missing demand-partner connections.

Action format:
"Add [Demand Partner] to [Publisher] — they are spending $X/week across [N] similar publishers at ~$Y eCPM."
Then on a new line, append the → *Execute:* navigation step.

Connections:
{json.dumps(opportunities, indent=2)}

Return ONLY a JSON array with one object per connection:
[
  {{
    "publisher":      "<exact publisher name>",
    "demand_partner": "<exact demand partner name>",
    "action":         "<one-sentence action>\\n→ *Execute:* <SSP navigation steps>"
  }},
  ...
]"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, list) and len(parsed) >= 1:
            result = [r for r in parsed if "action" in r]
            if result:
                _set_cached(key, json.dumps(result))
                return result
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # Fallback: templated action sentences
    result = [
        {
            "publisher":      o["publisher"],
            "demand_partner": o["demand_partner"],
            "action": (
                f"Add {o['demand_partner']} to {o['publisher']} — "
                f"they are spending {_fmt_usd_simple(o['dp_revenue_similar'])}/week across "
                f"{o['n_similar_pubs']} similar publishers at "
                f"~{_fmt_usd_simple(o['avg_ecpm_similar'])} eCPM."
            ),
        }
        for o in opportunities
    ]
    _set_cached(key, json.dumps(result))
    return result


def analyze_geo_expansion(
    gaps:           list[dict],
    supply_summary: dict,
    dp_summary:     list[dict],
) -> list[dict]:
    """
    Identify the top 3 geographic expansion opportunities from supply/demand gap data.

    Args:
        gaps           (list): Scored gaps sorted by 7-day revenue opportunity.
                               Each dict: country, demand_partner, supply_impressions_7d,
                               supply_ecpm, dp_avg_ecpm, dp_win_rate, opportunity_7d, etc.
        supply_summary (dict): Network supply overview (top countries, total revenue).
        dp_summary     (list): Top demand partners with total revenue and active country count.

    Returns:
        list[dict]:  Up to 3 dicts, each with keys:
                       country, demand_partner, action, rationale, opportunity_7d
    """
    key    = _cache_key("geo_expansion", gaps, supply_summary, dp_summary)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a senior programmatic advertising partnerships analyst. "
        "Your job is to identify the highest-value geographic expansion opportunities "
        "for a supply-side platform — specifically, where top demand partners are "
        "already buying at premium eCPMs in similar markets but are absent from "
        "supply markets where the inventory already exists. "
        "Your recommendations are specific and actionable: name the supply partner "
        "to approach, the country to target, the demand partner to activate, and "
        "the concrete step (e.g. 'add to PMP deal', 'lower floor to $X', "
        "'create geo-targeted deal ID'). "
        "Respond ONLY with valid JSON — no prose, no markdown fences. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    # Build a compact representation of the top gaps for the prompt
    top_gaps_compact = [
        {
            "country":              g["country"],
            "demand_partner":       g["demand_partner"],
            "supply_imps_7d":       g["supply_impressions_7d"],
            "supply_ecpm":          g["supply_ecpm"],
            "dp_avg_ecpm":          g["dp_avg_ecpm"],
            "dp_win_rate_pct":      g["dp_win_rate"],
            "dp_active_countries":  g["dp_active_countries"],
            "dp_total_rev_7d":      g["dp_total_revenue_7d"],
            "opportunity_7d_usd":   g["opportunity_7d"],
            "opportunity_annual":   g["opportunity_annual"],
        }
        for g in gaps[:15]
    ]

    user = f"""Analyse the following geographic demand-supply gaps and identify the
top 3 highest-priority expansion opportunities.

NETWORK SUPPLY OVERVIEW (7-day):
{json.dumps(supply_summary, indent=2)}

TOP DEMAND PARTNERS (7-day revenue rank):
{json.dumps(dp_summary, indent=2)}

REVENUE GAPS — supply exists but demand partner absent (sorted by opportunity, $ / week):
{json.dumps(top_gaps_compact, indent=2)}

For each of your top 3 picks, provide:
- The specific country to expand into
- The specific demand partner to activate there
- A concrete action the partnerships team should take this week, followed by a → *Execute:* navigation step
- A one-sentence rationale explaining why this gap is the priority

Return ONLY a JSON array of 3 objects:
[
  {{
    "country":          "<country name>",
    "demand_partner":   "<demand partner name>",
    "action":           "<specific one-sentence action>\\n→ *Execute:* <SSP geo targeting navigation steps>",
    "rationale":        "<one-sentence why>",
    "opportunity_7d":   <estimated 7-day revenue as a number>
  }}
]"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, list) and parsed and "country" in parsed[0]:
            _set_cached(key, json.dumps(parsed))
            return parsed
    except (json.JSONDecodeError, KeyError, TypeError, IndexError):
        pass

    # Fallback: return top 3 gaps as plain recommendations
    fallback = [
        {
            "country":        g["country"],
            "demand_partner": g["demand_partner"],
            "action": (
                f"Activate {g['demand_partner']} in {g['country']} — "
                f"supply exists ({g['supply_impressions_7d']:,} imps/7d) "
                f"at ${g['supply_ecpm']:.2f} eCPM vs DP's "
                f"${g['dp_avg_ecpm']:.2f} network eCPM."
            ),
            "rationale": (
                f"{g['demand_partner']} is active in {g['dp_active_countries']} "
                f"countries generating ${g['dp_total_revenue_7d']:,.0f}/wk total "
                f"but has not bid in {g['country']}."
            ),
            "opportunity_7d": g["opportunity_7d"],
        }
        for g in gaps[:3]
    ]
    _set_cached(key, json.dumps(fallback))
    return fallback


def analyze_win_rate_maximizer(combinations: list[dict]) -> list[dict]:
    """
    Review publisher × demand-partner combinations with high bid volume and low win rate.
    For each, confirm or refine the recommended floor and estimate weekly revenue recovery.

    Args:
        combinations (list): Top qualifying combos, each with:
            publisher, demand_partner, bids_7d, win_rate_pct,
            current_floor, new_floor, floor_adj_pct, avg_bid,
            ecpm_current, add_rev_per_day, add_rev_per_week.

    Returns:
        list[dict]: One dict per combination, keys:
            publisher, demand_partner, recommended_floor, weekly_recovery, rationale
    """
    key    = _cache_key("win_rate_maximizer", combinations)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a programmatic advertising yield optimisation expert. "
        "You are reviewing publisher × demand-partner combinations where the "
        "win rate is below 8% despite strong bid volume, indicating the floor "
        "price is slightly too high. "
        "For each combination, you assess the floor reduction needed, the "
        "expected revenue recovery, and any cautions (e.g. margin squeeze, "
        "thin demand, seasonality). "
        "Your output is precise and actionable. "
        "Respond ONLY with valid JSON — no prose, no markdown fences. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    compact = [
        {
            "publisher":       c["publisher"],
            "demand_partner":  c["demand_partner"],
            "bids_7d":         c["bids_7d"],
            "win_rate_pct":    c["win_rate_pct"],
            "current_floor":   c["current_floor"],
            "suggested_floor": c["new_floor"],
            "floor_adj_pct":   c["floor_adj_pct"],
            "avg_bid":         c["avg_bid"],
            "ecpm_current":    c["ecpm_current"],
            "add_rev_per_day": c["add_rev_per_day"],
            "add_rev_per_week": c["add_rev_per_week"],
        }
        for c in combinations
    ]

    user = f"""Review these {len(compact)} publisher × demand-partner combinations.
Each has strong bid volume (>2,000/7d) but a win rate below 8%, suggesting the
floor price is suppressing wins.

COMBINATIONS (sorted by estimated daily revenue recovery):
{json.dumps(compact, indent=2)}

For each combination:
1. Confirm or adjust the suggested floor (use your judgement on the bid/floor ratio).
2. Estimate the realistic weekly revenue recovery.
3. Provide a one-sentence rationale or caution.

Return ONLY a JSON array — one object per combination, in the same order:
[
  {{
    "publisher":          "<name>",
    "demand_partner":     "<name>",
    "recommended_floor":  <number>,
    "weekly_recovery":    <number>,
    "rationale":          "<one sentence>\\n→ *Execute:* <SSP DP-level floor navigation steps>"
  }}
]"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, list) and parsed and "publisher" in parsed[0]:
            _set_cached(key, json.dumps(parsed))
            return parsed
    except (json.JSONDecodeError, KeyError, TypeError, IndexError):
        pass

    # Fallback: return combinations as-is with minimal annotation
    fallback = [
        {
            "publisher":         c["publisher"],
            "demand_partner":    c["demand_partner"],
            "recommended_floor": c["new_floor"],
            "weekly_recovery":   c["add_rev_per_week"],
            "rationale": (
                f"Win rate {c['win_rate_pct']:.2f}% on {c['bids_7d']:,} bids — "
                f"reducing floor from ${c['current_floor']:.3f} to ${c['new_floor']:.3f} "
                f"targets {10}% win rate."
            ),
        }
        for c in combinations
    ]
    _set_cached(key, json.dumps(fallback))
    return fallback


def write_weekly_briefing(
    this_week:  dict,
    prior_week: dict,
    top_pubs:   list[dict],
    top_dps:    list[dict],
    mtd:        dict,
    week_label: str,
) -> str:
    """
    Write the complete Monday morning executive Slack briefing.

    Args:
        this_week  (dict): Aggregated stats for the past 7 days.
        prior_week (dict): Aggregated stats for the 7 days before that.
        top_pubs   (list): Top 5 publishers by revenue.
        top_dps    (list): Top 5 demand partners by revenue.
        mtd        (dict): Month-to-date pacing vs the $1 M monthly target.
        week_label (str):  Human-readable date range, e.g. "2026-04-06 – 2026-04-12".

    Returns:
        str: Complete Slack message ready to post.  Plain text with Slack
             markdown (*bold*, _italic_, bullet points).  No Block Kit JSON.
    """
    key    = _cache_key("weekly_briefing", this_week, prior_week, top_pubs, top_dps, mtd, week_label)
    cached = _get_cached(key)
    if cached:
        return cached

    monthly_target = mtd.get("monthly_target", 1_000_000)

    system = (
        "You are the head of revenue operations at a programmatic advertising "
        "company writing a Monday morning briefing for the leadership team. "
        "Your tone is direct, data-driven, and decisive — like a sharp COO, not "
        "a consultant. You use specific numbers. You do not hedge. You do not "
        "waste words. You use Slack markdown: *bold* for emphasis, _italic_ for "
        "labels, bullet points with •. "
        "Write the complete Slack message — nothing before it, nothing after it. "
        "Start directly with the opening line. No subject line, no preamble."
    )

    wow_pct  = this_week.get("wow_pct_change")
    wow_sign = f"{wow_pct:+.1f}%" if wow_pct is not None else "N/A"
    wow_rev  = this_week.get("wow_revenue_change", 0)
    wow_rev_s = f"${abs(wow_rev):,.0f} {'more' if wow_rev >= 0 else 'less'} than prior week"

    user = f"""Write a Monday morning executive briefing for the PGAM programmatic advertising team.

WEEK: {week_label}

THIS WEEK PERFORMANCE:
• Total revenue: ${this_week.get('total_revenue', 0):,.2f}
• Avg daily revenue: ${this_week.get('avg_daily_revenue', 0):,.2f}
• Week-on-week change: {wow_sign} ({wow_rev_s})
• Best day: {this_week.get('best_day', {}).get('date', '?')} at ${this_week.get('best_day', {}).get('revenue', 0):,.2f}
• Worst day: {this_week.get('worst_day', {}).get('date', '?')} at ${this_week.get('worst_day', {}).get('revenue', 0):,.2f}
• Overall margin: {this_week.get('avg_margin_pct', 0):.1f}% ({this_week.get('margin_trend', 'stable')})
• Win rate: {this_week.get('win_rate_pct', 0):.2f}%
• Total impressions: {this_week.get('total_impressions', 0):,}

PRIOR WEEK (for context):
• Total revenue: ${prior_week.get('total_revenue', 0):,.2f}
• Avg daily: ${prior_week.get('avg_daily_revenue', 0):,.2f}
• Margin: {prior_week.get('avg_margin_pct', 0):.1f}%

TOP 5 PUBLISHERS THIS WEEK:
{json.dumps(top_pubs, indent=2)}

TOP 5 DEMAND PARTNERS THIS WEEK:
{json.dumps(top_dps, indent=2)}

MONTHLY TARGET PACING (${monthly_target:,.0f} target):
• MTD revenue: ${mtd.get('mtd_revenue', 0):,.2f}
• Days elapsed: {mtd.get('days_elapsed', 0)} of {mtd.get('days_in_month', 30)}
• Daily run rate: ${mtd.get('daily_run_rate', 0):,.2f}
• Projected monthly: ${mtd.get('projected_monthly', 0):,.2f} ({mtd.get('pct_of_target', 0):.0f}% of target)
• Revenue still needed: ${mtd.get('revenue_needed_rest', 0):,.2f} over {mtd.get('days_remaining', 0)} days = ${mtd.get('needed_per_day_rest', 0):,.2f}/day required

Write a Slack message that answers these four questions in order:
1. *Are we on track for the ${monthly_target:,.0f} monthly target?*  Be direct — yes, no, or at risk — and state exactly what the numbers show.
2. *What was the single biggest win last week?*  Pick one specific thing from the data.
3. *What was the single biggest missed opportunity last week?*  Pick one specific thing — a day that underperformed, a partner that slipped, a gap.
4. *What are the three most important actions for this week?*  Numbered, specific, and directly tied to the data above.

Format guidelines:
• Open with the week dates and a one-line verdict on the week (good/bad/mixed and why)
• Use *bold* for section headers
• Keep the whole message under 400 words
• End with a one-line motivating close that references the monthly target number"""

    response = _ask(system, user)
    _set_cached(key, response)
    return response


def analyze_monthly_forecast(projections: dict, day_of_month: int) -> dict:
    """
    Assess month-end revenue projections and provide a confidence level,
    biggest risk, and required daily run rate commentary.

    Args:
        projections   (dict): Output of _compute_projections() — includes
                              mtd_revenue, daily rates, three proj_* dicts,
                              needed_per_day, days_remaining, etc.
        day_of_month  (int):  1, 10, or 20 (checkpoint day).

    Returns:
        dict: {
            "confidence":              "high" | "medium" | "low",
            "confidence_pct":          int,      # 0-100
            "biggest_risk":            str,
            "needed_daily_commentary": str,
            "actions":                 list[str],
            "summary":                 str,
        }
    """
    key    = _cache_key("monthly_forecast", projections, day_of_month)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    target      = projections.get("monthly_target", 1_000_000)
    days_rem    = projections.get("days_remaining", 0)
    days_el     = projections.get("days_elapsed", 0)
    needed_pd   = projections.get("needed_per_day", 0)
    mtd_rev     = projections.get("mtd_revenue", 0)
    simple_dr   = projections.get("simple_daily_rate", 0)
    w7_dr       = projections.get("weighted_daily_rate", 0)

    checkpoint = {1: "Month Start (1st)", 10: "10-Day Check", 20: "20-Day Alert"}.get(day_of_month, "Mid-Month")

    system = (
        "You are a revenue forecasting analyst at a programmatic advertising company. "
        "You evaluate month-end revenue projections and give a precise, data-driven "
        "confidence assessment. You identify the single biggest risk to the forecast "
        "and state concrete actions. "
        "Respond ONLY with valid JSON — no prose, no markdown fences."
    )

    user = f"""Evaluate the following month-end revenue forecast and provide your assessment.

CHECKPOINT: {checkpoint} (day {day_of_month} of {projections.get('days_in_month', 30)})
MONTHLY TARGET: ${target:,.0f}

MTD PERFORMANCE:
• MTD revenue: ${mtd_rev:,.2f}
• MTD margin: {projections.get('mtd_margin_pct', 0):.1f}%
• Days elapsed: {days_el}
• Days remaining: {days_rem}
• Simple daily run rate (MTD avg): ${simple_dr:,.2f}/day
• Weighted daily rate (last-7d avg): ${w7_dr:,.2f}/day

THREE PROJECTIONS:
• Simple run rate:        ${projections['proj_simple']['projection']:,.0f}  ({projections['proj_simple']['gap_pct']:+.1f}% vs target)
• Weighted (last-7d):     ${projections['proj_weighted']['projection']:,.0f}  ({projections['proj_weighted']['gap_pct']:+.1f}% vs target)
• Seasonally adjusted:    ${projections['proj_adjusted']['projection']:,.0f}  ({projections['proj_adjusted']['gap_pct']:+.1f}% vs target)

REQUIRED TO HIT TARGET:
• Revenue still needed: ${projections.get('revenue_needed_rest', 0):,.0f}
• Required daily rate for remainder: ${needed_pd:,.0f}/day

Provide:
1. A confidence level ("high", "medium", or "low") on hitting the ${target:,.0f} target, with a % probability (0-100).
2. The single biggest risk to the forecast (one sentence).
3. A one-sentence commentary on what ${needed_pd:,.0f}/day means relative to current performance.
4. Two or three specific, actionable recommendations (not generic) tied to this data.
5. A one-paragraph executive summary (2-3 sentences) appropriate for a {checkpoint} update.

Return ONLY a JSON object:
{{
  "confidence":              "<high|medium|low>",
  "confidence_pct":          <integer 0-100>,
  "biggest_risk":            "<one sentence>",
  "needed_daily_commentary": "<one sentence on the required daily rate>",
  "actions":                 ["<action 1>", "<action 2>", "<action 3 optional>"],
  "summary":                 "<2-3 sentence executive summary>"
}}"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, dict) and "confidence" in parsed:
            _set_cached(key, json.dumps(parsed))
            return parsed
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # Fallback
    best_proj = max(
        projections["proj_simple"]["projection"],
        projections["proj_weighted"]["projection"],
        projections["proj_adjusted"]["projection"],
    )
    gap_pct = (best_proj - target) / target * 100
    on_track = best_proj >= target * 0.95

    conf = "high" if gap_pct >= 5 else "medium" if gap_pct >= -5 else "low"
    conf_pct = max(10, min(95, int(50 + gap_pct * 3)))

    fallback = {
        "confidence":     conf,
        "confidence_pct": conf_pct,
        "biggest_risk":   (
            "Run rate deceleration in the remaining days could push the forecast below target."
            if on_track else
            f"Current trajectory falls short of the ${target:,.0f} target — "
            f"${needed_pd:,.0f}/day is required but the last-7d rate is only ${w7_dr:,.0f}/day."
        ),
        "needed_daily_commentary": (
            f"${needed_pd:,.0f}/day is required for the remaining {days_rem} days; "
            f"the last-7d rate of ${w7_dr:,.0f}/day is "
            f"{'sufficient' if w7_dr >= needed_pd else 'insufficient by $' + f'{needed_pd - w7_dr:,.0f}/day'}."
        ),
        "actions": [
            "Review top publisher floor prices for near-term revenue uplift.",
            "Check demand partner bid density on highest-volume publishers.",
            "Confirm no active integrations are reporting reduced bid volume.",
        ],
        "summary": (
            f"At day {days_el} of the month, MTD revenue is ${mtd_rev:,.0f} "
            f"({mtd_rev / target * 100:.0f}% of the ${target:,.0f} target). "
            f"The seasonally adjusted projection of ${projections['proj_adjusted']['projection']:,.0f} "
            f"is {'on track' if on_track else 'below target'}."
        ),
    }
    _set_cached(key, json.dumps(fallback))
    return fallback


def write_revenue_gap_memo(
    run_rate:       dict,
    pub_gaps:       dict,
    dp_trends:      dict,
    country_gaps:   dict,
    daily_target:   float,
    monthly_target: float,
    week_label:     str,
) -> str:
    """
    Write a Sunday strategic revenue memo — not a data dump.

    The memo answers three questions in narrative form:
      1. Where exactly is the daily gap coming from?
      2. What three specific actions close it fastest?
      3. What does the revenue trajectory look like — act vs don't act?

    Args:
        run_rate       (dict): Current 7-day daily rate, monthly rate, daily gap,
                               pct_of_target, daily_series.
        pub_gaps       (dict): Publisher underperformance analysis —
                               underperformers list, total_daily_gap, n_flagged.
        dp_trends      (dict): Demand partner WoW trends —
                               declining list, total_recoverable_daily, n_declining.
        country_gaps   (dict): Country supply gap analysis —
                               gaps list, total_daily_opportunity, network_avg_ecpm.
        daily_target   (float): Target daily revenue (MONTHLY_TARGET / 30).
        monthly_target (float): Monthly target (e.g. 1_000_000).
        week_label     (str):  Human-readable date range.

    Returns:
        str: Complete Slack message, plain text with Slack markdown (*bold*,
             _italic_, bullet •).  Reads like a memo from a senior analyst.
             Approximately 350–500 words.
    """
    key    = _cache_key("revenue_gap_memo", run_rate, pub_gaps, dp_trends, country_gaps, week_label)
    cached = _get_cached(key)
    if cached:
        return cached

    daily_rate    = run_rate.get("daily_rate", 0)
    daily_gap     = run_rate.get("daily_gap", 0)
    monthly_rate  = run_rate.get("monthly_rate", 0)
    monthly_gap   = run_rate.get("monthly_gap", 0)
    pct_of_target = run_rate.get("pct_of_target", 0)

    pub_total_gap = pub_gaps.get("total_daily_gap", 0)
    dp_total_rec  = dp_trends.get("total_recoverable_daily", 0)
    geo_total_opp = country_gaps.get("total_daily_opportunity", 0)
    total_addressable = pub_total_gap + dp_total_rec + geo_total_opp

    # Top underperforming publisher
    top_pub = (pub_gaps.get("underperformers") or [{}])[0]
    # Top declining DP
    top_dp  = (dp_trends.get("declining") or [{}])[0]
    # Top country gap
    top_geo = (country_gaps.get("gaps") or [{}])[0]

    system = (
        "You are a senior revenue analyst at a programmatic advertising company. "
        "Every Sunday you write a strategy memo for the CEO and head of revenue ops. "
        "Your writing is direct, specific, and narrative — you weave data into prose "
        "rather than listing it. You think in terms of levers and consequences, not "
        "observations. You write like a thoughtful operator who has seen these patterns "
        "before and knows what moves the needle. "
        "Use Slack markdown: *bold* for section headers, _italic_ for labels, "
        "bullet points with •. No data tables. No preamble. Start with the first word "
        "of your analysis."
    )

    user = f"""Write a Sunday revenue strategy memo for the PGAM programmatic advertising team.

WEEK: {week_label}
MONTHLY TARGET: ${monthly_target:,.0f}  (${daily_target:,.0f}/day required)

CURRENT PERFORMANCE:
• 7-day average daily revenue: ${daily_rate:,.2f}/day
• Monthly run rate projection: ${monthly_rate:,.0f}
• Gap to $1M/month target: ${daily_gap:,.0f}/day  (${monthly_gap:,.0f}/month)
• Pacing at {pct_of_target:.1f}% of required daily rate

──────────────────────────────────────────────
GAP ANALYSIS — WHERE THE ${daily_gap:,.0f}/DAY IS COMING FROM
──────────────────────────────────────────────

PUBLISHER FLOOR PRESSURE (${pub_total_gap:,.0f}/day addressable):
{json.dumps(pub_gaps.get("underperformers", [])[:5], indent=2)}
{pub_gaps.get("n_flagged", 0)} publishers earning below 50% of bid-density potential.

DEMAND PARTNER SPEND DECLINE (${dp_total_rec:,.0f}/day recoverable):
{json.dumps(dp_trends.get("declining", [])[:5], indent=2)}
{dp_trends.get("n_declining", 0)} demand partners showing >15% week-on-week revenue drop.

GEOGRAPHIC SUPPLY GAPS (${geo_total_opp:,.0f}/day opportunity):
{json.dumps(country_gaps.get("gaps", [])[:5], indent=2)}
High-eCPM countries with impressions well below the network median.

TOTAL ADDRESSABLE GAP: ${total_addressable:,.0f}/day
──────────────────────────────────────────────

Write a strategic Sunday memo — 350 to 500 words — that:

1. *Opens* with a one-paragraph verdict on where the business is right now versus the $1M target. Be direct: is this a floor problem, a demand problem, a supply problem, or all three? Use the data to make the case.

2. *Section: "Where the gap lives"* — In 2-3 short paragraphs, explain the three gap sources narratively. Don't list the data — interpret it. Which source is the primary driver? Why is the floor pressure happening? What does the DP decline tell us about demand health? What does the geo gap tell us about where we are under-indexed?

3. *Section: "Three actions, ranked by speed of impact"* — Write three specific, named actions (not generic recommendations). Each action should name the specific publisher, demand partner, or country from the data above, state the specific change to make, and give the expected daily revenue impact in dollars.

4. *Section: "Trajectory"* — Two scenarios in one short paragraph each:
   — If we action all three: what does the next 30 days look like? Name a specific projected monthly revenue and what that means for the $1M milestone.
   — If we action none: what does the trend line look like at current pace? Be blunt.

End with one sentence. Make it land."""

    result = _ask(system, user)
    _set_cached(key, result)
    return result


def analyze_action_patterns(
    agent_hit_rates:  dict,
    metric_hit_rates: dict,
    recent_log:       list[dict],
    stats:            dict,
) -> str:
    """
    Identify which recommendation types have the highest hit rate and what
    PGAM should do differently based on past recommendation outcomes.

    Args:
        agent_hit_rates  (dict): {agent_name: {"total": int, "successful": int, "hit_rate": float}}
        metric_hit_rates (dict): {metric_affected: {"total": int, "successful": int, "hit_rate": float}}
        recent_log       (list): Last 30 completed recommendation entries (status != "pending").
        stats            (dict): Overall stats — total_fired, pending, successful, ineffective,
                                 actioned_pct, avg_impact_successful, avg_impact_all.

    Returns:
        str: Claude's pattern analysis and suggestions (plain Slack markdown, 150–250 words).
    """
    key    = _cache_key("action_patterns", agent_hit_rates, metric_hit_rates, stats)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are a revenue operations analyst reviewing the historical performance "
        "of automated recommendations made by a programmatic advertising intelligence "
        "system. Your job is to identify patterns in which recommendation types work "
        "and which don't, and suggest how to improve the system's hit rate. "
        "Be specific and data-driven. Use Slack markdown (*bold*, bullet points •). "
        "Write 150–250 words."
    )

    user = f"""Review the following recommendation outcome data and identify patterns.

OVERALL STATS (last 7 days):
{json.dumps(stats, indent=2)}

HIT RATE BY AGENT:
{json.dumps(agent_hit_rates, indent=2)}

HIT RATE BY METRIC AFFECTED:
{json.dumps(metric_hit_rates, indent=2)}

RECENT COMPLETED RECOMMENDATIONS (sample):
{json.dumps(recent_log[:20], indent=2)}

Answer these questions:
1. Which agent or recommendation type has the highest hit rate, and why might that be?
2. Which recommendation type is underperforming, and what is the likely reason?
3. One specific change to the recommendation logic that would improve outcomes.

Format as a short Slack message — concise, direct, with *bold* headers and bullet points.
Do not exceed 250 words."""

    result = _ask(system, user)
    _set_cached(key, result)
    return result


def _fmt_usd_simple(v) -> str:
    """Minimal USD formatter used inside claude_analyst fallbacks."""
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def analyze_ctv_opportunity(
    ctv_summary: dict,
    scale_publishers: list,
    projections: dict,
) -> dict:
    """
    Build a CTV supply expansion business case with specific publisher
    recommendations and a single top action for the week.

    Args:
        ctv_summary (dict):
            total_revenue_14d     (float)
            avg_daily_revenue     (float)
            avg_ecpm              (float)
            fill_rate             (float)  0-1
            win_rate              (float)  0-1
            pct_of_network        (float)  CTV share of total revenue (%)
            total_publishers      (int)

        scale_publishers (list[dict]): Top scale opportunity publishers, each:
            publisher             (str)
            ecpm                  (float)
            fill_rate             (float)
            win_rate              (float)
            revenue_14d           (float)
            avg_daily_revenue     (float)
            fill_gap              (float)  distance below network avg fill
            opportunity_score     (float)

        projections (dict):
            current_daily         (float)
            tier_10_daily         (float)
            tier_25_daily         (float)
            tier_50_daily         (float)
            tier_10_annual        (float)
            tier_25_annual        (float)
            tier_50_annual        (float)

    Returns:
        dict: {
            "business_case":          str,   # 2-3 sentence executive narrative
            "priority_publishers":    list[{publisher, rationale, approach}],
            "top_action":             str,   # single most important action this week
            "projections_narrative":  str,   # one sentence on growth tiers
        }
    """
    key = _cache_key("ctv_opportunity", ctv_summary, scale_publishers, projections)
    cached = _get_cached(key)
    if cached:
        try:
            return json.loads(cached)
        except json.JSONDecodeError:
            pass

    system = (
        "You are a CTV (Connected TV) monetization expert at a programmatic "
        "advertising company. You write clear, executive-ready business cases "
        "for growing CTV supply. Your recommendations are specific, quantified, "
        "and prioritised. You understand that CTV has premium eCPMs and that "
        "the key levers are: adding publisher integrations, optimising floor prices, "
        "and improving bid density. "
        "Respond ONLY with valid JSON — no prose, no markdown fences."
    )

    user = f"""Build a CTV supply expansion business case from the following data.

CTV NETWORK SUMMARY (last 14 days):
{json.dumps(ctv_summary, indent=2)}

TOP SCALE OPPORTUNITY PUBLISHERS (high eCPM, low fill — sorted by opportunity score):
{json.dumps(scale_publishers, indent=2)}

REVENUE PROJECTIONS (daily and annual):
{json.dumps(projections, indent=2)}

Provide:
1. A 2-3 sentence executive business case connecting CTV's premium eCPM position
   with the revenue opportunity from closing the fill gap.
2. Top 3 specific publishers to approach this week, with a concrete outreach approach
   (e.g. "Lower floor from $2.10 to $1.40 to unlock LoopMe and Magnite demand").
3. The single most important action the team should take this week.
4. One sentence summarising the 10/25/50% growth projections in dollar terms.

Return ONLY a JSON object:
{{
  "business_case":         "<2-3 sentence executive narrative>",
  "priority_publishers":   [
    {{"publisher": "<name>", "rationale": "<why this one>", "approach": "<specific action>"}}
  ],
  "top_action":            "<single most important action this week>",
  "projections_narrative": "<one sentence on the three growth tiers>"
}}"""

    raw = _ask(system, user)

    try:
        clean  = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        parsed = json.loads(clean)
        if isinstance(parsed, dict) and "business_case" in parsed:
            _set_cached(key, json.dumps(parsed))
            return parsed
    except (json.JSONDecodeError, KeyError, TypeError):
        pass

    # Fallback
    top_pub = scale_publishers[0]["publisher"] if scale_publishers else "top publisher"
    fallback = {
        "business_case": (
            f"CTV inventory generates {_fmt_usd_simple(ctv_summary.get('avg_ecpm', 0))} eCPM "
            f"but represents only {ctv_summary.get('pct_of_network', 0):.1f}% of network revenue. "
            f"Closing the fill gap across {ctv_summary.get('total_publishers', 0)} CTV publishers "
            f"represents significant incremental revenue at premium CPMs."
        ),
        "priority_publishers": [
            {
                "publisher": p["publisher"],
                "rationale": f"eCPM {_fmt_usd_simple(p['ecpm'])}, fill rate {p['fill_rate']*100:.1f}%",
                "approach":  "Review floor price and demand partner configuration.",
            }
            for p in scale_publishers[:3]
        ],
        "top_action": f"Prioritise floor price optimisation on {top_pub} to increase CTV fill rate.",
        "projections_narrative": (
            f"A 10% volume increase adds {_fmt_usd_simple(projections.get('tier_10_daily', 0))}/day; "
            f"50% adds {_fmt_usd_simple(projections.get('tier_50_daily', 0))}/day."
        ),
    }
    _set_cached(key, json.dumps(fallback))
    return fallback


def analyze_weekend_floors(candidates: list[dict]) -> str:
    """
    Prioritise weekend floor reduction candidates and explain which to action first.

    Args:
        candidates (list[dict]): Each dict contains:
            publisher            (str)
            weekday_floor        (float)   — current floor price
            recommended_floor    (float)   — suggested weekend floor
            gap_pct              (float)   — % floor reduction
            win_rate_current_pct (float)
            win_rate_optimal_pct (float)
            delta_win_rate_pp    (float)   — win rate improvement in percentage points
            daily_recovery       (float)   — estimated $/day revenue recovery
            avg_rev_per_day      (float)   — baseline daily revenue
            r2                   (float)   — regression fit quality
            avg_ecpm_weekend     (float)

    Returns:
        str: Slack markdown (120–200 words). Names specific publishers, states exact
             floors and dollar impacts. Prioritises by expected recovery × confidence (R²).
    """
    if not candidates:
        return "No weekend floor optimisation candidates identified this cycle."

    key = _cache_key("weekend_floors", candidates)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are a programmatic ad ops specialist. "
        "You review weekend floor optimisation candidates and write a concise Slack briefing. "
        "Your output is Slack markdown (bold with *asterisks*, bullets with •). "
        "Lead with the total estimated weekend revenue recovery, then list the top 3 publishers "
        "to action in order of priority. For each, state the exact floor change "
        "(e.g. $0.450 → $0.310) and the expected daily revenue impact. "
        "After each publisher's action, include a → *Execute:* navigation line. "
        "Close with one sentence on why the bottom candidates are lower priority. "
        "Be specific — never say 'consider adjusting'; say exactly what to change and why. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    total_recovery = sum(c.get("daily_recovery", 0) * 2 for c in candidates)  # Sat+Sun
    user = (
        f"Weekend floor optimisation candidates ({len(candidates)} publishers, "
        f"${total_recovery:,.0f} est. total weekend recovery):\n\n"
        + json.dumps(candidates, indent=2)
        + "\n\nWrite the Slack briefing now."
    )

    text = _ask(system, user)

    if text and len(text) > 50:
        _set_cached(key, text)
        return text

    # Specific fallback using real data
    top = sorted(candidates, key=lambda c: c.get("daily_recovery", 0), reverse=True)[:3]
    lines = [
        f"*Weekend Floor Optimisation — {len(candidates)} publishers, "
        f"~${total_recovery:,.0f} est. recovery Sat+Sun.*\n"
    ]
    for c in top:
        lines.append(
            f"• *{c['publisher']}:* lower floor "
            f"${c['weekday_floor']:.3f} → ${c['recommended_floor']:.3f} "
            f"({c['gap_pct']:.0f}% reduction). "
            f"Win rate {c['win_rate_current_pct']:.1f}% → {c['win_rate_optimal_pct']:.1f}% "
            f"(+{c['delta_win_rate_pp']:.1f}pp). "
            f"Est. +${c['daily_recovery']:.0f}/day "
            f"on ${c['avg_rev_per_day']:.0f} baseline (R²={c['r2']:.2f})."
        )
    result = "\n".join(lines)
    _set_cached(key, result)
    return result


def analyze_app_revenue_movers(
    movers: list[dict],
    primary_label: str,
    compare_label: str,
) -> str:
    """
    Identify root causes for the biggest app/bundle revenue moves and recommend actions.

    Args:
        movers (list[dict]): Top significant movers (up to 10), each containing:
            bundle           (str)
            rev_change_pct   (float)   — % change vs prior period
            revenue_today    (float)   — revenue in primary window ($)
            revenue_prior    (float)   — revenue in comparison window ($)
            classification   (str)     — "rising" or "falling"
            ecpm_change_pct  (float)   — eCPM % change
            imps_change_pct  (float)   — impressions % change
        primary_label  (str): e.g. "Today"
        compare_label  (str): e.g. "Yesterday"

    Returns:
        str: Slack markdown (120–200 words). Separates root causes (eCPM-driven vs
             volume-driven vs both), names the biggest risers and fallers with exact
             dollar/% figures, and gives one specific action per falling bundle.
    """
    if not movers:
        return "No significant app revenue movers identified this cycle."

    key = _cache_key("app_revenue_movers", movers, primary_label, compare_label)
    cached = _get_cached(key)
    if cached:
        return cached

    system = (
        "You are a mobile programmatic revenue analyst. "
        "You review daily app/bundle revenue movers and write a concise Slack briefing. "
        "Your output is Slack markdown (bold with *asterisks*, bullets with •). "
        "Separate analysis into: (1) top risers — explain if gain is eCPM-driven, volume-driven, "
        "or both; (2) top fallers — diagnose root cause and give one specific action to recover. "
        "Use exact dollar and percentage figures. Never say 'investigate' — name a specific action "
        "(e.g. 'Lower floor from $1.20 to $0.85 to recover LoopMe demand' or "
        "'Contact publisher to confirm ad tag is firing correctly'). "
        "For any floor change action, append a → *Execute:* navigation line on a new line. "
        "Keep it under 220 words. "
        "\n\n" + NAV_INSTRUCTIONS
    )

    rising  = [m for m in movers if m.get("classification") == "rising"]
    falling = [m for m in movers if m.get("classification") == "falling"]
    user = (
        f"App revenue movers: {primary_label} vs {compare_label}\n\n"
        f"Rising ({len(rising)}): {json.dumps(rising, indent=2)}\n\n"
        f"Falling ({len(falling)}): {json.dumps(falling, indent=2)}\n\n"
        "Write the Slack briefing now."
    )

    text = _ask(system, user)

    if text and len(text) > 50:
        _set_cached(key, text)
        return text

    # Specific fallback using real data
    top_rising  = sorted(rising,  key=lambda m: m.get("revenue_today", 0), reverse=True)[:3]
    top_falling = sorted(falling, key=lambda m: abs(m.get("rev_change_pct", 0)), reverse=True)[:3]

    lines = [f"*App Revenue Movers — {primary_label} vs {compare_label}*\n"]

    if top_rising:
        lines.append("*:arrow_up: Top risers:*")
        for m in top_rising:
            driver = (
                "eCPM-driven" if abs(m.get("ecpm_change_pct", 0)) > abs(m.get("imps_change_pct", 0))
                else "volume-driven"
            )
            lines.append(
                f"• *{m['bundle']}:* +{m['rev_change_pct']:.1f}% "
                f"(${m['revenue_prior']:,.0f} → ${m['revenue_today']:,.0f}) — {driver}."
            )

    if top_falling:
        lines.append("\n*:arrow_down: Top fallers:*")
        for m in top_falling:
            if m.get("ecpm_change_pct", 0) < -10:
                action = f"Review floor price — eCPM dropped {abs(m['ecpm_change_pct']):.1f}%."
            elif m.get("imps_change_pct", 0) < -20:
                action = f"Check ad tag firing — impressions down {abs(m['imps_change_pct']):.1f}%."
            else:
                action = "Audit DSP seat config for demand partner eligibility."
            lines.append(
                f"• *{m['bundle']}:* {m['rev_change_pct']:.1f}% "
                f"(${m['revenue_prior']:,.0f} → ${m['revenue_today']:,.0f}). "
                f"{action}"
            )

    result = "\n".join(lines)
    _set_cached(key, result)
    return result
