"""
agents/outbound/icp.py
──────────────────────
Apollo `mixed_people/search` filters for PGAM's two outbound segments.

The two segments mirror the DSP rate-card SKUs:

    BRAND_AWARENESS — brand and growth marketers at mid-market consumer
                      and B2B companies who already invest in CTV / video.
                      Pitch: attention scoring + brand-lift attribution.

    PERFORMANCE     — heads of acquisition / performance marketing at
                      call-driven verticals (Medicare, ACA, insurance,
                      legal, home services, solar, mortgage).
                      Pitch: per-call attribution + CPA-call commercial.

Both segments are deliberately narrow — outbound conversion rate scales
inversely with list size, and reply quality scales inversely with reply
volume. We would rather send 50/day to a tight list than 500/day to a
loose one.

ICP NOTES
─────────
Apollo filter syntax (relevant fields):

    person_titles                  — list[str], OR within the field
    person_seniorities             — list[str] from Apollo's seniority taxonomy
    organization_locations         — list[str] (city, state, country)
    organization_num_employees_ranges — list[str] like "51,200", "201,500"
    organization_industry_tag_ids  — list[str] (Apollo industry IDs)
    q_organization_keyword_tags    — list[str] (free-text industry keywords)
    contact_email_status           — ["verified"] for highest deliverability
    page                           — int, 1-based
    per_page                       — int, max 100

The tag IDs below are common Apollo industry ID stubs — verify against
your Apollo account before going live; IDs can drift. Switch to
q_organization_keyword_tags if you want to skip the ID lookup.

TUNING
──────
After the first 200 sends per segment, look at:
    - bounce rate by ICP → tighten contact_email_status / company size
    - reply rate by title → cut titles producing zero positive replies
    - "positive" reply rate by industry → double down on industries that
      reply, drop the others
"""

from __future__ import annotations

from typing import Any


# ─────────────────────────────────────────────────────────────────────
# SKU 1 — Brand Awareness (CTV / OLV)
# ─────────────────────────────────────────────────────────────────────
BRAND_AWARENESS: dict[str, Any] = {
    "label": "brand_awareness",
    "sku": "Brand Awareness — CTV / OLV",
    "apollo_filter": {
        "person_titles": [
            "CMO",
            "Chief Marketing Officer",
            "VP Marketing",
            "VP Brand",
            "VP Growth",
            "VP Performance Marketing",
            "Head of Brand",
            "Head of Marketing",
            "Head of Media",
            "Head of Paid Media",
            "Director Marketing",
            "Director Brand Marketing",
            "Director Performance Marketing",
            "Director Media",
            "Director Paid Media",
            "Director Video",
        ],
        "person_seniorities": ["director", "vp", "head", "c_suite"],
        "organization_locations": ["United States"],
        "organization_num_employees_ranges": [
            "51,200",
            "201,500",
            "501,1000",
            "1001,5000",
        ],
        # Free-text industry keywords — easier to maintain than tag IDs.
        "q_organization_keyword_tags": [
            "B2B SaaS",
            "Retail",
            "Direct to Consumer",
            "Consumer Packaged Goods",
            "Financial Services",
            "Travel",
            "Automotive",
        ],
        "contact_email_status": ["verified"],
        "per_page": 50,
    },
    # Which Instantly campaign to add new leads to (set in env).
    "instantly_campaign_env": "INSTANTLY_CAMPAIGN_BRAND_AWARENESS_ID",
    # HubSpot deal label prefix (helps Priyesh see the segment at a glance).
    "deal_label_prefix": "[Brand Awareness]",
}


# ─────────────────────────────────────────────────────────────────────
# SKU 2 — Performance / CPA-Call
# ─────────────────────────────────────────────────────────────────────
PERFORMANCE: dict[str, Any] = {
    "label": "performance",
    "sku": "Performance — CPA-Call",
    "apollo_filter": {
        "person_titles": [
            "VP Acquisition",
            "VP Growth",
            "VP Performance Marketing",
            "Head of Acquisition",
            "Head of Growth",
            "Head of Performance",
            "Head of Paid Media",
            "Director Acquisition",
            "Director Growth",
            "Director Performance Marketing",
            "Director Paid Media",
            "Director Media Buying",
            "Senior Media Buyer",
            "Media Buyer",
        ],
        "person_seniorities": ["director", "vp", "head", "manager"],
        "organization_locations": ["United States"],
        "organization_num_employees_ranges": [
            "11,50",
            "51,200",
            "201,500",
            "501,1000",
        ],
        # Call-driven verticals. Add/remove as we learn what replies.
        "q_organization_keyword_tags": [
            "Medicare",
            "Health Insurance",
            "Insurance",
            "Legal Services",
            "Personal Injury Law",
            "Home Services",
            "Solar",
            "Mortgage",
            "Debt Relief",
            "Senior Living",
        ],
        "contact_email_status": ["verified"],
        "per_page": 50,
    },
    "instantly_campaign_env": "INSTANTLY_CAMPAIGN_PERFORMANCE_ID",
    "deal_label_prefix": "[Performance / Call]",
}


ICP_SEGMENTS: list[dict[str, Any]] = [BRAND_AWARENESS, PERFORMANCE]
