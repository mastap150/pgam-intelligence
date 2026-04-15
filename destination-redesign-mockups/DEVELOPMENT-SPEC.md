# Destination.com — AI Trip Planner + Expedia Integration
## Development Specification & Enhancement Plan

**Date:** April 14, 2026
**Version:** 1.0
**Mockup files:** `homepage-variations.html`, `article-mockup.html`, `destination-page-mockup.html`

---

## 1. Scope of Work

### 1A. Homepage — Section Replacement (Minimal Change)
Replace the existing 4-category image card section (Featured Destinations, Unique Experiences, Art & Culture, Nature & Expeditions) with an AI Trip Planner entry point + Expedia integration strip.

**Three variations designed (see `homepage-variations.html`):**

| Variation | Description | Effort | Recommendation |
|-----------|-------------|--------|----------------|
| **A: Smart Search** | Airbnb-style search bar with trip type pills + Expedia footer. Minimal disruption. | Low | Good for Phase 1 launch |
| **B: Split Planner + Book** | Two-column: AI planner form on left, Expedia quick-book on right. | Medium | **Recommended** — best balance of conversion + UX |
| **C: Conversational AI** | Chat-style interface for natural language trip planning. | High | Best for Phase 2 after AI engine is proven |

### 1B. Article/Guide Pages — Sidebar Integration (Primary Focus)
Add two new sticky sidebar widgets to all article pages (replacing/augmenting current ad slots):

1. **AI Trip Planner Widget** — Context-aware (pre-fills destination from article)
2. **Expedia Booking Widget** — Hotels, flights, packages, activities for that destination

Plus inline CTAs within article body content.

### 1C. Destination Guide Pages — Full Integration
Full trip planning + booking experience on destination overview pages:
- Floating planner bar below hero
- Inline hotel recommendation carousels (Expedia affiliate)
- Sample itinerary cards
- Inline CTA banners between content sections
- Sidebar: Trip planner + Expedia booking + weather widget

---

## 2. Component Inventory

### New Components to Build

| Component | Used On | Priority |
|-----------|---------|----------|
| `TripPlannerWidget` (sidebar, compact) | Articles, Destination pages | P0 |
| `ExpediaBookingWidget` (sidebar) | Articles, Destination pages | P0 |
| `TripPlannerBar` (floating, inline) | Destination pages | P0 |
| `InlineHotelCarousel` | Destination pages, select articles | P1 |
| `InlineCTABanner` | Articles, Destination pages | P1 |
| `TripPlannerSearch` (homepage, full-width) | Homepage | P0 |
| `TripTypeTiles` (reusable) | Homepage, Sidebar, Planner | P0 |
| `HotelCard` / `MiniHotelCard` | Multiple pages | P0 |
| `ExpediaQuickLinks` | Sidebar, inline | P1 |
| `WeatherWidget` | Destination pages | P2 |
| `ConversationalPlanner` (chat UI) | Homepage (Phase 2) | P2 |

### Existing Components to Modify

| Component | Change |
|-----------|--------|
| Homepage hero section | Update CTA button to "Plan My Trip" linking to planner |
| Article page layout | Add sidebar widget slots, reduce ad density |
| Destination page template | Complete restructure with planner bar + sidebar |
| Site navigation | Add "Plan a Trip" nav item |
| Footer | Add affiliate disclosure |

---

## 3. Technical Architecture

### 3.1 AI Trip Planner Engine

**Backend endpoint:** `POST /api/plan`

```
Request:
{
  destination: string | "surprise_me",
  dates: { start: string, end: string, flexible: boolean },
  duration: number,  // nights
  travellers: { adults: number, children: number[], type: string },
  budget: { amount: number, currency: string, per: "person" | "total" },
  tripTypes: string[],
  accommodation: string,
  flightPreference: string
}

Response (streamed via SSE):
{
  destinations: [{ name, country, matchScore, reason }],
  itinerary: [{ day, title, morning, afternoon, evening }],
  costBreakdown: { flights, accommodation, food, activities, transport, total },
  hotelRecommendations: [{ name, expediaUrl, rating, pricePerNight, image }],
  flightOptions: [{ airline, price, duration, expediaUrl }],
  activities: [{ name, price, viatorUrl }]
}
```

**AI Stack:**
- OpenAI GPT-4o for itinerary generation (structured JSON output)
- Custom destination database for scoring and context enrichment
- Expedia EAN API for live pricing injection into results
- Server-Sent Events for streaming the plan to the UI

### 3.2 Expedia Affiliate Integration

**Phase 1 — EAN Deep Links (Day 1 launch):**
- Utility function: `buildExpediaUrl(type, params, affiliateId)`
- Covers: hotel search, flight search, packages, activities
- Tracking: `affcid` parameter on all outbound links
- Click tracking: GA4 custom events on all Expedia CTAs

**Deep link patterns:**
```
Hotels:  /Hotels-in-{city}.d{locationId}.Travel-Guide-Hotels?affcid={id}&chkin={in}&chkout={out}&adults={n}
Flights: /Flights-Search?trip=oneway&leg1=from:{orig},to:{dest},departure:{date}&affcid={id}
Packages: /Packages?affcid={id}&packageType=FH&...
```

**Phase 2 — EPS Rapid API (Month 6+):**
- In-site hotel search, availability, pricing
- Room-level data, cancellation policies
- On-platform booking flow
- Higher commission rates (25-40% uplift over EAN)

### 3.3 Context-Aware Widget System

The sidebar widgets should be context-aware:
- **On article pages:** Auto-detect destination from article metadata/tags, pre-fill widget
- **On destination pages:** Pre-fill with page destination, show relevant hotels
- **On homepage:** Generic, show trending destinations
- **Widget state persistence:** Save user inputs across pages via localStorage

---

## 4. Homepage Optimization Suggestions

Beyond the section replacement, here are enhancements to the existing homepage:

### Quick Wins
1. **Replace "Start Exploring" CTA** with "Plan My Trip" — higher intent, links to planner
2. **Add Expedia deal ticker** below hero — scrolling strip of live deals
3. **Add social proof** — "50,000+ trips planned" counter near planner
4. **Lazy-load below-fold images** — current hero slider images are large

### Medium Effort
5. **Add trending destinations carousel** — data-driven from search volume / bookings
6. **Newsletter popup** — exit-intent with "Get personalized deal alerts" framing
7. **Search bar enhancement** — current search bar ("Where do you want to go?") should become the AI planner entry point rather than a passive search

### Site-Wide
8. **Sticky bottom bar on mobile** — "Plan My Trip" CTA that's always visible
9. **Breadcrumb navigation** — currently missing, important for SEO
10. **Schema markup** — add TouristDestination, FAQPage, HowTo schemas
11. **Core Web Vitals** — audit LCP/CLS/INP; current hero slider may hurt LCP

---

## 5. Page-by-Page Integration Points

### Homepage
| Location | Integration | Type |
|----------|-------------|------|
| Replace 4 image cards | AI Trip Planner section (Variation B recommended) | Primary |
| Below hero | Expedia deal ticker strip | Secondary |
| Between guide articles | "Plan your next adventure" CTA banner | Secondary |

### Article Pages (e.g., "10 Bangkok Street Foods")
| Location | Integration | Type |
|----------|-------------|------|
| Sidebar (top) | AI Trip Planner widget (context-aware) | Primary |
| Sidebar (below planner) | Expedia booking widget with hotels + quick links | Primary |
| After 2nd section in article | Inline CTA banner ("Planning a trip to X?") | Secondary |
| Between content sections | Inline hotel recommendations (horizontal scroll) | Secondary |

### Destination Guide Pages (e.g., "Bangkok")
| Location | Integration | Type |
|----------|-------------|------|
| Below hero | Floating planner bar (dates, travellers, budget, CTA) | Primary |
| Sidebar (sticky) | AI Trip Planner widget + Expedia widget + weather | Primary |
| After overview section | Inline hotel carousel (Expedia) | Primary |
| Between itinerary cards | Inline CTA banner | Secondary |
| Tab navigation | "Where to Stay" tab links to Expedia results | Secondary |

---

## 6. Data Requirements

### Destination Database (PostgreSQL)
```sql
destinations (id, name, slug, country, region, lat, lng, description,
  best_months, visa_info, currency, avg_hotel_price, avg_daily_cost,
  tags[], hero_image, created_at, updated_at)

expedia_mappings (id, destination_id, expedia_location_id,
  hotel_search_url, flight_search_url)
```

### Content Metadata
Each article/guide needs:
- `destination_id` — links to destination database
- `destination_name` — for widget pre-fill
- `related_destinations[]` — for cross-selling

---

## 7. Analytics & Tracking

### Custom GA4 Events
| Event | Trigger | Parameters |
|-------|---------|------------|
| `planner_start` | User opens trip planner | `source` (homepage/sidebar/inline) |
| `planner_step` | Each form step completed | `step_number`, `step_name` |
| `plan_generated` | AI plan successfully displayed | `destination`, `budget`, `duration` |
| `expedia_click` | Any Expedia affiliate link click | `type` (hotel/flight/package), `destination`, `source` |
| `hotel_impression` | Hotel card shown to user | `hotel_name`, `price`, `source` |
| `widget_interaction` | Sidebar widget field changed | `widget_type`, `field_name` |
| `inline_cta_click` | Inline CTA clicked | `cta_type`, `page_type` |

### Revenue Attribution
- Track `affcid` + `source_page` on all outbound Expedia clicks
- EAN postback for confirmed bookings
- Dashboard: revenue by page type, widget type, destination

---

## 8. Development Phases

### Phase 1 — MVP (Weeks 1-4)
- [ ] Set up Expedia EAN affiliate account
- [ ] Build `TripPlannerWidget` sidebar component
- [ ] Build `ExpediaBookingWidget` sidebar component
- [ ] Integrate widgets into article page template
- [ ] Build `TripPlannerSearch` for homepage (Variation A or B)
- [ ] Replace 4 image cards on homepage
- [ ] EAN deep-link utility function
- [ ] GA4 event tracking setup
- [ ] Affiliate disclosure on all pages

### Phase 2 — Destination Pages (Weeks 5-8)
- [ ] Destination page template redesign
- [ ] Floating planner bar component
- [ ] Inline hotel carousel component
- [ ] Inline CTA banner component
- [ ] Context-aware widget system (auto-detect destination)
- [ ] Destination database (top 100 destinations)
- [ ] Expedia location ID mapping

### Phase 3 — AI Engine (Weeks 9-12)
- [ ] `/api/plan` endpoint with OpenAI GPT-4o
- [ ] Prompt engineering for structured itinerary output
- [ ] SSE streaming for real-time plan generation
- [ ] Trip results page
- [ ] Save trip functionality
- [ ] Budget calculator integration

### Phase 4 — Scale & Optimize (Weeks 13-16)
- [ ] EPS Rapid API integration (in-site booking)
- [ ] Conversational planner (Variation C)
- [ ] A/B testing framework for widget placements
- [ ] Programmatic destination page generation (5,000+ pages)
- [ ] Deal alerts email system
- [ ] Mobile-optimized sticky CTA bar

---

## 9. Affiliate Compliance

Required on every page with Expedia links:
> "destination.com earns a commission when you book through our links. This does not affect the price you pay."

- Disclosure must appear **before** first Expedia link on page
- Footer disclosure on all pages
- No bidding on Expedia branded keywords in paid search
- All prices marked "from" where not guaranteed
- AI-generated itineraries carry disclaimer about verifying information

---

## 10. Files Delivered

| File | Description |
|------|-------------|
| `homepage-variations.html` | 3 variations for the homepage section replacement |
| `homepage-mockup.html` | Full homepage mockup (original full redesign concept) |
| `article-mockup.html` | Article page with sidebar trip planner + Expedia widgets |
| `destination-page-mockup.html` | Destination guide page with full integration |
| `DEVELOPMENT-SPEC.md` | This document |

**To view mockups:** Open any HTML file directly in a browser, or run:
```bash
cd destination-redesign-mockups
python3 -m http.server 8767
# Then visit http://localhost:8767/
```
