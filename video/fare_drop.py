"""
video/fare_drop.py — Fare Drop Engine (spec §14).

Fares arrive from the Destination.com fare system (API push / CSV / manual
entry — no first-party feed is exposed to this repo yet, so ingest_deal() is
the interface it will call). Each deal is scored for interest; deals clearing
the configured gates become candidate Fare Drop videos with the compliance
copy baked in: a fare is never represented as guaranteed.
"""

from datetime import datetime, timezone

from video import settings
from video.models import Concept, ContentSource, FareDeal, new_id
from video.store import store

# Origin markets and destinations with strong audience demand (§14).
POPULAR_ORIGINS = {"MIA": 15, "JFK": 15, "NYC": 15, "LAX": 12, "ORD": 8, "DFW": 6, "SFO": 8}
POPULAR_DESTINATIONS = {
    "tokyo": 20, "japan": 20, "paris": 18, "london": 16, "rome": 16, "italy": 16,
    "madrid": 12, "barcelona": 14, "lisbon": 12, "athens": 12, "greece": 14,
    "caribbean": 12, "cancun": 10, "reykjavik": 10, "bali": 14,
}

DISCLAIMER = "Fare found at time of publishing. Prices and availability can change."


def score_deal(deal: FareDeal) -> FareDeal:
    """Interest score 0-100 with reasons (§14 criteria)."""
    reasons = []
    score = 0.0

    if deal.normal_fare_estimate and deal.fare:
        disc = 100.0 * (1 - deal.fare / deal.normal_fare_estimate)
        deal.discount_percentage = round(disc, 1)
        pts = min(45.0, disc * 1.1)
        score += max(0.0, pts)
        reasons.append(f"{disc:.0f}% below normal ${deal.normal_fare_estimate:.0f} (+{max(0,pts):.0f})")
    else:
        reasons.append("no normal-fare baseline — cannot verify discount")

    dest_pts = POPULAR_DESTINATIONS.get(deal.destination.lower(), 4)
    score += dest_pts
    reasons.append(f"destination demand {deal.destination} (+{dest_pts})")

    origin_pts = POPULAR_ORIGINS.get(deal.origin.upper(), 3)
    score += origin_pts
    reasons.append(f"origin market {deal.origin} (+{origin_pts})")

    if deal.cabin.lower() in ("business", "first"):
        score += 12
        reasons.append("premium cabin (+12)")
    if deal.points_price:
        score += 6
        reasons.append("points redemption available (+6)")
    if deal.fare and deal.fare < 500 and deal.cabin.lower() == "economy":
        score += 8
        reasons.append("under-$500 headline fare (+8)")

    deal.interest_score = round(min(100.0, score), 1)
    deal.interest_reasons = reasons
    return deal


def ingest_deal(payload: dict) -> dict:
    """Entry point for the fare feed. Scores, stores, and — when the deal
    clears the gates — creates the fare content source + candidate concept."""
    s = store()
    deal = FareDeal(id=new_id("fare"), **{
        k: v for k, v in payload.items() if k in FareDeal.__dataclass_fields__ and k != "id"
    })
    deal = score_deal(deal)
    deal.status = "scored"
    rec = s.put("fare_deals", deal.to_record())

    cfg = settings.settings()
    if (deal.interest_score >= cfg["fare_min_interest_score"]
            and (deal.discount_percentage or 0) >= cfg["fare_min_discount_pct"]):
        _create_candidate(deal)
        rec = s.put("fare_deals", {**rec, "status": "produced"})
    else:
        settings.log("fare_drop",
                     f"{deal.origin}→{deal.destination} ${deal.fare:.0f} below gates "
                     f"(score {deal.interest_score}, disc {deal.discount_percentage or 0:.0f}%)")
    return rec


def _travel_window(deal: FareDeal) -> str:
    if deal.departure_date and deal.return_date:
        return f"Travel {deal.departure_date} – {deal.return_date}"
    return ""


def _create_candidate(deal: FareDeal) -> dict:
    """A Fare Drop video is templated, not free-generated: the numbers on
    screen come from the deal record, never from an LLM (§30)."""
    s = store()
    headline = f"FARE DROP: {deal.origin} → {deal.destination} ${deal.fare:.0f} {deal.cabin}"
    src = ContentSource(
        id=new_id("src"), source_type="fare_alert",
        source_url=deal.deep_link or settings.SITE_BASE_URL + "/flights",
        headline=headline,
        summary=f"{deal.origin} to {deal.destination} for ${deal.fare:.0f} "
                f"{deal.currency} round trip ({deal.cabin}). "
                f"Normally ${deal.normal_fare_estimate:.0f}+. {_travel_window(deal)}. {DISCLAIMER}",
        destination=deal.destination, source_quality_score=90.0,
        commercial_relevance=100.0, flight_relevance=100.0,
        publish_date=datetime.now(timezone.utc).date().isoformat(),
    )
    s.put("content_sources", src.to_record())

    concept = Concept(
        id=new_id("con"), source_id=src.id, franchise="fare_drop",
        working_title=f"Fare Drop: {deal.origin} → {deal.destination} ${deal.fare:.0f}",
        hook=f"${deal.fare:.0f} round trip to {deal.destination}.",
        angle=(f"Price-first fare card. Route {deal.origin} → {deal.destination}, "
               f"${deal.fare:.0f} vs normally ${deal.normal_fare_estimate:.0f}+. "
               f"{_travel_window(deal)}. Close on the search CTA. {DISCLAIMER}"),
        destination=deal.destination, target_length=20,
        visual_style="fare_drop_card + destination b-roll",
        commercial_goal="flight_search",
        cta="Search the fare on Destination.com",
        confidence_score=95.0, editorial_score=70.0, originality_score=60.0,
        predicted_performance_score=80.0, generator="template",
    )
    rec = s.put("concepts", concept.to_record())
    settings.log("fare_drop", f"candidate created: {concept.working_title} "
                              f"(score {deal.interest_score})")
    return rec
