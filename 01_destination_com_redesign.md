# DESTINATION.COM — REDESIGN PACKAGE
## Homepage Wireframe, Navigation, Section Copy, UX Improvements

---

## CURRENT SITE AUDIT — PROBLEMS TO FIX

| Area | Current Issue | Fix |
|------|--------------|-----|
| Hero | Static headline, weak CTA hierarchy | Add search bar + destination mood filter |
| Navigation | Flat 5-item nav with no mega-menu | Add mega-menu with region + experience dropdowns |
| Content cards | 4 cards with no taxonomy filtering | Add filterable grid (by region, type, budget) |
| Points & Miles | Buried below fold | Promote to nav + dedicated landing strip |
| SEO | Thin H1, no schema markup signals | Rewrite H1, add breadcrumbs, structured data |
| Newsletter | Generic CTA copy | Personalize with value proposition |
| Mobile | No thumb-zone nav | Sticky bottom nav bar on mobile |

---

## NAVIGATION STRUCTURE (REVISED)

### Primary Navigation Bar
```
[DESTINATION.COM LOGO]  |  Destinations ▾  |  Experiences ▾  |  Travel Guides ▾  |  Points & Miles ▾  |  [Newsletter CTA]  |  [Search 🔍]
```

### Mega-Menu: Destinations
```
BY REGION                    BY TYPE                      TRENDING NOW
─────────────────────────    ─────────────────────────    ──────────────────────
Europe                       Beach Getaways               Japan in Cherry Blossom
  → France, Italy, Greece    Adventure & Trekking         Morocco Desert Tours
  → Spain, Portugal, Croatia Luxury Resorts               Iceland Northern Lights
Asia & Southeast Asia        Budget Backpacking           Patagonia Hiking
  → Japan, Thailand, Bali    City Breaks                  Amalfi Coast Drive
  → Vietnam, India, Sri Lanka Cultural Immersion          Tanzania Safari
Africa                       Food & Wine Tours            [View All Destinations →]
  → Tanzania, Morocco, Kenya Family Travel
Americas                     Romantic Escapes
  → USA, Mexico, Colombia    Solo Travel
Middle East & Pacific
```

### Mega-Menu: Experiences
```
ADVENTURE          FOOD & DRINK        CULTURE & ART       WELLNESS
──────────────     ──────────────      ──────────────       ──────────
Safari Tours       Street Food Trails  UNESCO Sites         Yoga Retreats
Trekking & Hikes   Wine Regions        Local Festivals      Spa Destinations
Diving & Snorkel   Cooking Classes     Museum Cities        Digital Detox
Skiing & Snow      Farm-to-Table       Architecture Tours   Hot Springs
Surfing            Night Markets       Street Art           Silent Retreats
```

### Mega-Menu: Travel Guides
```
PLANNING                LOGISTICS              MONEY
──────────────────      ──────────────────     ──────────────────
Visa Requirements       Packing Lists          Points & Miles 101
Best Times to Visit     Travel Insurance       Best Travel Cards
Itinerary Builder       Airport Guides         Budget Breakdowns
Safety by Country       Phrase Books           Currency Tips
Booking Windows         Health Requirements    Cheap Flights Guide
```

---

## HOMEPAGE WIREFRAME (TEXT-BASED)

```
╔══════════════════════════════════════════════════════════════════════════╗
║  STICKY HEADER                                                           ║
║  Logo | Destinations | Experiences | Guides | Points & Miles | [Search]  ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  HERO SECTION (full-width, 80vh)                                         ║
║  ┌─────────────────────────────────────────────────────────────────────┐ ║
║  │  [BACKGROUND: Cinematic rotating destination photography]           │ ║
║  │                                                                     │ ║
║  │  OVERLINE: Guides written by people who've actually been there      │ ║
║  │                                                                     │ ║
║  │  H1: Find Your Next Journey                                         │ ║
║  │      Worth Every Mile                                               │ ║
║  │                                                                     │ ║
║  │  ┌────────────────────────────────────────────────────────────┐    │ ║
║  │  │  🔍 Search destinations, experiences, or guides...    [Go] │    │ ║
║  │  └────────────────────────────────────────────────────────────┘    │ ║
║  │                                                                     │ ║
║  │  MOOD FILTERS:  [Beach]  [Adventure]  [Culture]  [Food]  [Budget]  │ ║
║  │                                                                     │ ║
║  │  ↓  Scroll to explore                                              │ ║
║  └─────────────────────────────────────────────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  TRUST BAR (thin strip, dark bg)                                         ║
║  ✓ Written by on-the-ground travelers  ✓ No sponsored rankings           ║
║  ✓ Updated monthly  ✓ 2.1M monthly readers                              ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  TRENDING NOW (horizontal scroll, 5 cards)                               ║
║  H2: "Where Travelers Are Headed Right Now"                              ║
║  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐                     ║
║  │ IMG  │  │ IMG  │  │ IMG  │  │ IMG  │  │ IMG  │                     ║
║  │Japan │  │Moroc │  │Colom │  │Bali  │  │ Peru │                     ║
║  │Guide │  │-co   │  │-bia  │  │Guide │  │Guide │                     ║
║  └──────┘  └──────┘  └──────┘  └──────┘  └──────┘                     ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  FEATURED EDITORIAL (2-column: 1 large + 2 stacked small)                ║
║  H2: "Guides Worth Saving"                                               ║
║                                                                          ║
║  ┌───────────────────────────┐  ┌───────────────────────────────────┐   ║
║  │                           │  │ [IMG] Tanzania Safari Guide        │   ║
║  │  [LARGE HERO IMAGE]       │  │ Best Parks, Seasons & Cost        │   ║
║  │                           │  ├───────────────────────────────────┤   ║
║  │  CATEGORY: Southeast Asia │  │ [IMG] Barcelona Hidden Beaches     │   ║
║  │  H3: The Complete Bali    │  │ 12 Beaches the Tourists Miss      │   ║
║  │  Travel Guide for 2025    │  └───────────────────────────────────┘   ║
║  │  [Read the Guide →]       │                                          ║
║  └───────────────────────────┘                                          ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  EXPLORE BY REGION (6-grid icon blocks)                                  ║
║  H2: "Explore by Region"                                                 ║
║                                                                          ║
║  [🌍 Europe]  [🌏 Asia]  [🌎 Americas]  [🌍 Africa]  [🏝 Pacific]  [🕌 Middle East] ║
║  142 guides    89 guides   76 guides      45 guides    32 guides    28 guides ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  EXPERIENCE TYPES (horizontal filter + card grid)                        ║
║  H2: "What Kind of Traveler Are You?"                                    ║
║  [All] [Adventure] [Beach] [Food] [Culture] [Budget] [Luxury] [Solo]    ║
║                                                                          ║
║  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐               ║
║  │  [IMG]   │  │  [IMG]   │  │  [IMG]   │  │  [IMG]   │               ║
║  │Adventure │  │  Beach   │  │   Food   │  │ Culture  │               ║
║  │  Guide   │  │  Guide   │  │  Guide   │  │  Guide   │               ║
║  │ 12 reads │  │ 24 reads │  │ 18 reads │  │  9 reads │               ║
║  └──────────┘  └──────────┘  └──────────┘  └──────────┘               ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  POINTS & MILES SECTION (dark/contrast bg)                               ║
║  OVERLINE: Free travel starts here                                       ║
║  H2: "Fly Free. We'll Show You How."                                     ║
║  COPY: The best credit card bonuses, transfer partners, and sweet spots  ║
║         updated every week so you never miss a deal.                     ║
║                                                                          ║
║  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         ║
║  │ Best Sign-Up    │  │ Transfer Partner │  │ Hotel Points    │         ║
║  │ Bonuses 2025    │  │ Sweet Spots      │  │ Sweet Spots     │         ║
║  └─────────────────┘  └─────────────────┘  └─────────────────┘         ║
║                                                                          ║
║                    [Explore Points & Miles →]                            ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  HIDDEN GEMS (editorial, off-white bg)                                   ║
║  OVERLINE: Off the beaten path                                           ║
║  H2: "Places Most Travelers Never Find"                                  ║
║  COPY: Our writers spent months in the field to surface these.           ║
║                                                                          ║
║  [Card] [Card] [Card]    [See All Hidden Gems →]                        ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  NEWSLETTER (full-width, warm earth tone)                                ║
║  H2: "The World, In Your Inbox Every Tuesday"                            ║
║  COPY: 2.1M travelers get our weekly guide — new destinations, flight    ║
║        deals, visa changes, and honest reviews. Free, always.            ║
║                                                                          ║
║  [Email address...........................] [Get the Newsletter →]       ║
║  "No spam. No sponsored content. Unsubscribe any time."                  ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  FOOTER                                                                  ║
║  Logo + tagline  |  Regions  |  Experiences  |  Resources  |  Company   ║
║  Social icons  |  © 2025 Destination.com  |  Privacy  |  Editorial Policy║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## SECTION COPY — PRODUCTION READY

### Hero Section
**Overline:** Guides written by travelers who've actually been there
**H1:** Find Your Next Journey Worth Every Mile
**Subheadline:** Real guides from people on the ground — not recycled listicles, not sponsored posts. Every recommendation earned.
**Search placeholder:** Search destinations, experiences, or guides...
**Mood filter labels:** Beach | Adventure | Culture & Art | Food & Drink | Budget | Luxury

---

### Trust Bar
Written by on-the-ground travelers &nbsp;·&nbsp; No paid rankings &nbsp;·&nbsp; Updated monthly &nbsp;·&nbsp; 2.1M monthly readers

---

### Trending Now Section
**H2:** Where Travelers Are Headed Right Now
**Subtext:** Updated weekly based on reader searches and writer dispatches.

---

### Featured Editorial Section
**H2:** Guides Worth Saving
**Subtext:** Long-form, deeply researched — the guides you'll return to when you actually book.

---

### Explore By Region
**H2:** Explore by Region
**Subtext:** Every continent, hundreds of destinations, one honest voice.

Region labels + guide counts:
- Europe → 142 guides
- Asia → 89 guides
- Americas → 76 guides
- Africa → 45 guides
- Pacific → 32 guides
- Middle East → 28 guides

---

### Experience Filter Section
**H2:** What Kind of Traveler Are You?
**Subtext:** Filter by the way you travel, not just where you go.

---

### Points & Miles Section
**Overline:** Free travel starts here
**H2:** Fly Free. We'll Show You How.
**Body:** The points and miles landscape changes every week — new transfer bonuses, devalued programs, and card offers that disappear overnight. Our team tracks it all so you can focus on booking the trip.
**CTA:** Explore Points & Miles →

**3 feature cards:**
1. **Best Sign-Up Bonuses Right Now** — The highest-value offers this month, ranked by cents-per-point
2. **Transfer Partner Sweet Spots** — Hidden value routes most travelers miss
3. **Hotel Points: Where to Redeem** — Which programs give the most for free nights

---

### Hidden Gems Section
**Overline:** Off the beaten path
**H2:** Places Most Travelers Never Find
**Body:** These destinations made the cut because a writer spent real time there — not a press trip, not a weekend. These are the places that stay with you.
**CTA:** See All Hidden Gems →

---

### Newsletter Section
**H2:** The World, In Your Inbox Every Tuesday
**Body:** Join 2.1 million travelers who get our weekly guide — new destination deep-dives, visa changes, flight deal alerts, and credit card bonuses. Free, always.
**CTA button:** Get the Newsletter →
**Disclaimer:** No spam. No sponsored content. Unsubscribe any time.

---

## MOBILE UX IMPROVEMENTS

### Sticky Bottom Navigation Bar (Mobile Only)
```
[🏠 Home]  [🗺 Explore]  [✈️ Guides]  [💳 Miles]  [🔍 Search]
```

### Additional Mobile Fixes
- Hamburger menu replaced with bottom sheet drawer
- Horizontal card scroll with peek (shows next card edge)
- Search bar promoted above the fold on mobile
- Hero image optimized for portrait aspect ratio
- Tap targets minimum 48px height

---

## INTERNAL LINKING STRATEGY — DESTINATION.COM

### Hub & Spoke Model
Each **region page** (hub) links to:
- Country guides (spoke level 1)
- City/destination guides (spoke level 2)
- Experience guides within that region (spoke level 3)
- Related planning resources (visa, budget, best time)

### Cross-Link Rules
1. Every article links to its parent category page
2. Every article links to 3–5 related articles (contextual inline links)
3. Every article links to relevant Points & Miles content where applicable
4. All planning resources link back to relevant destination content
5. Use exact-match anchor text for primary keyword, partial-match for secondary

### Breadcrumb Structure
```
Home > Region > Country > City/Guide Title
Home > Experiences > [Type] > [Article]
Home > Points & Miles > [Topic]
```
