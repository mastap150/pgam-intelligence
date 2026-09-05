"""
video/opportunity.py — Content Opportunity Engine (spec §4).

Every active content source gets a Content Opportunity Score from configurable
weights (settings()["opportunity_weights"]). Each component score carries a
reason string so editors can see *why* something ranked. Editors can boost or
bury via set_manual_boost(); the boost is additive and audited on the record.

Signals used per component (0-100 each):
  audience_interest       — comment-demand topics + newsletter/theme priors
  youtube_appeal          — franchise fit heuristics (lists, warnings, fares, news)
  historical_performance  — mean video_score of published videos for the same
                            destination (channel average when none exists yet)
  editorial_relevance     — source quality × first-party bonus
  search_demand           — keyword presence (real search volume plugs in later)
  commercial_potential    — flight/affiliate relevance from ingestion
  freshness               — decay score from ingestion
"""

from video import settings
from video.models import Opportunity, new_id
from video.store import store

APPEAL_PATTERNS = [
    (("mistake", "avoid", "before you", "know before"), 85, "warning/utility angle"),
    (("cheap", "fare", "deal", "$", "budget"), 82, "price-led angle"),
    (("best time", "when to", "itinerary", "guide"), 75, "planning utility"),
    ((" vs ", "instead", "overlooked", "nobody"), 78, "comparison/contrarian angle"),
    (("points", "miles", "business class"), 72, "points & premium travel"),
]


def _audience_interest(src: dict, demand_topics: dict[str, float]) -> tuple[float, str]:
    dest = (src.get("destination") or "").lower()
    country = (src.get("country") or "").lower()
    boost = max(demand_topics.get(dest, 0.0), demand_topics.get(country, 0.0))
    base = 50.0
    if boost:
        return min(100.0, base + boost), f"viewer demand signal +{boost:.0f} for {dest or country}"
    return base, "no direct viewer demand signal yet"


def _youtube_appeal(src: dict) -> tuple[float, str]:
    text = f"{src.get('headline', '')} {src.get('summary', '')}".lower()
    best, why = 55.0, "generic editorial topic"
    for words, score, reason in APPEAL_PATTERNS:
        if any(w in text for w in words) and score > best:
            best, why = float(score), reason
    return best, why


def _historical_performance(src: dict) -> tuple[float, str]:
    s = store()
    scored = [v for v in s.find("videos", {"status": "published"})
              if v.get("video_score")]
    if not scored:
        return 50.0, "no published history yet (channel prior)"
    channel_avg = sum(v["video_score"] for v in scored) / len(scored)
    same_dest = [v for v in scored
                 if v.get("destination") and v["destination"] == src.get("destination")]
    if same_dest:
        dest_avg = sum(v["video_score"] for v in same_dest) / len(same_dest)
        rel = 50.0 + (dest_avg - channel_avg)
        return max(0.0, min(100.0, rel)), (
            f"{src.get('destination')} avg score {dest_avg:.0f} vs channel {channel_avg:.0f} "
            f"({len(same_dest)} videos)")
    return 50.0, f"no {src.get('destination') or 'destination'} history (channel prior)"


def _search_demand(src: dict) -> tuple[float, str]:
    kws = src.get("keywords") or []
    if kws:
        return min(100.0, 40.0 + 12.0 * len(kws)), f"{len(kws)} target keywords attached"
    return 35.0, "no keyword targeting on source"


def score_source(src: dict, demand_topics: dict[str, float] | None = None) -> Opportunity:
    demand_topics = demand_topics or _demand_topics()
    weights = settings.settings()["opportunity_weights"]

    components: dict[str, float] = {}
    reasons: list[str] = []
    for name, (val, why) in {
        "audience_interest": _audience_interest(src, demand_topics),
        "youtube_appeal": _youtube_appeal(src),
        "historical_performance": _historical_performance(src),
        "editorial_relevance": (src.get("source_quality_score", 50.0),
                                "first-party editorial" if src.get("rights_status") == "owned"
                                else "external source"),
        "search_demand": _search_demand(src),
        "commercial_potential": (src.get("commercial_relevance", 0.0),
                                 "flight/affiliate relevance from source text"),
        "freshness": (src.get("freshness_score", 30.0), "publish-date decay"),
    }.items():
        components[name] = round(val, 1)
        reasons.append(f"{name}={val:.0f} ({why}, weight {weights[name]:.0%})")

    total = sum(components[k] * weights[k] for k in weights)
    return Opportunity(
        id=new_id("opp"),
        source_id=src["id"],
        title=src.get("headline", ""),
        destination=src.get("destination", ""),
        score=round(total, 1),
        component_scores=components,
        reasons=reasons,
        status="open",
    )


def _demand_topics() -> dict[str, float]:
    """Viewer-demand boosts from classified comments (§21 feeds §4)."""
    s = store()
    topics: dict[str, float] = {}
    for c in s.find("comments", {"status": "classified"}):
        for t in c.get("topics") or []:
            topics[t.lower()] = min(40.0, topics.get(t.lower(), 0.0) + 8.0)
    return topics


def set_manual_boost(opportunity_id: str, boost: float, actor: str) -> dict:
    """Editor priority adjustment (§4). Additive, recorded on the record."""
    s = store()
    opp = s.get("opportunities", opportunity_id)
    if not opp:
        raise KeyError(opportunity_id)
    opp["manual_boost"] = boost
    opp["score"] = round(sum(
        opp["component_scores"][k] * settings.settings()["opportunity_weights"][k]
        for k in opp["component_scores"]) + boost, 1)
    opp["reasons"] = [r for r in opp["reasons"] if not r.startswith("manual_boost")]
    opp["reasons"].append(f"manual_boost={boost:+.0f} by {actor}")
    return s.put("opportunities", opp)


def run(limit: int | None = None) -> list[dict]:
    """Scheduler entry: score all active sources, upsert one open opportunity
    per source, return the ranked list."""
    s = store()
    demand = _demand_topics()
    ranked = []
    sources = s.find("content_sources", {"status": "active"})
    for src in sources[:limit] if limit else sources:
        existing = s.find("opportunities", {"source_id": src["id"]})
        opp = score_source(src, demand)
        keep = next((e for e in existing if e["status"] == "open"), None)
        if keep:  # preserve id + manual boost across rescores
            opp.id = keep["id"]
            opp.manual_boost = keep.get("manual_boost", 0.0)
            opp.score = round(opp.score + opp.manual_boost, 1)
        rec = s.put("opportunities", opp.to_record())
        ranked.append(rec)
    ranked.sort(key=lambda o: o["score"], reverse=True)
    top = ", ".join(f"{o['score']:.0f} {o['title'][:40]}" for o in ranked[:5])
    settings.log("opportunity", f"scored {len(ranked)} sources; top: {top}")
    return ranked
