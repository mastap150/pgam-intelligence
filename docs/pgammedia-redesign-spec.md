# PGAM Media — Full Site Audit & Redesign Specification

**Prepared:** 2026-04-16
**Scope:** pgammedia.com — design, content, new sections, implementation roadmap

---

## DELIVERABLE 1: AUDIT REPORT — Current State Critique

### Site structure discovered

| Page | URL path | Status |
|---|---|---|
| Homepage | `/` | Live — JS SPA |
| About | `/about/` | Live |
| The Platform | `/the-platform/` | Live |
| Advertisers | `/advertisers/` | Live |
| Publishers | `/publishers/` | Live |
| Blog | `/blog/` | Live (multiple posts) |
| Contact | `/contact/` | Live |
| Reports portal | `reports.pgammedia.com` | Separate subdomain |

### Design & visual audit

**Typography & hierarchy**
- The site is a fully JS-rendered SPA, which means search engines and link-preview crawlers receive only a bare `<title>` tag — the single biggest technical liability for a company selling media performance
- Hero headline "Performance Media Optimized by Attention" is generic and passive; "optimized by" is weak language that buries the value prop
- Body copy across pages leans on industry jargon without concrete differentiation — it reads like every other programmatic company
- Headings lack a clear weight hierarchy; no visible system of H1 → H2 → H3 progression that guides the reader through a narrative

**Color & brand feel**
- Current palette is dark-themed (blacks/dark grays) — appropriate for ad tech but needs sharpening
- No strong accent color system to create visual energy or direct the eye to CTAs
- Lacks the premium polish of competitors like The Trade Desk, Peer39, or start.io — which pair dark backgrounds with vibrant, controlled accent systems

**Layout & whitespace**
- Section rhythm is inconsistent — some pages feel dense, others feel empty
- Grid system appears inconsistent across pages
- Publisher and Advertiser pages mirror each other structurally but don't differentiate enough visually to signal distinct value propositions
- Blog is functional but not a thought-leadership showcase — posts don't carry enough visual weight or internal linking

**Hero section**
- Does not communicate value in under 5 seconds
- No quantified proof (no stats in the hero)
- No visual supporting element (data visualization, platform screenshot, animation)
- CTA is weak or absent — no clear "what to do next"

**CTAs**
- Inconsistent CTA language across pages
- No urgency or specificity ("Contact Us" is the lowest-performing CTA pattern in B2B)
- Missing secondary CTAs (e.g. "See the platform", "Read the case study")

**Mobile responsiveness**
- SPA architecture means core content depends entirely on JS execution — slow on mobile, invisible to crawlers
- Navigation structure unclear on smaller viewports

**Navigation**
- Current nav: Home, About, The Platform, Advertisers, Publishers, Blog, Contact
- "The Platform" is vague — what platform? This label does zero selling
- No dropdown or mega-menu structure to expose depth
- Missing: any reference to DSP capabilities, audience data, attribution, or self-serve — the exact things that differentiate PGAM from a generic ad network

### SEO & technical issues
- Entire site is client-side rendered — Google can index JS but it's slower and less reliable; critical for a company that publishes blog content for organic traffic
- No visible structured data (JSON-LD) for organization, articles, or FAQ
- Subpages all return the same `<title>` tag to crawlers — catastrophic for SEO
- No `sitemap.xml` detected
- Blog posts have decent topic authority (CTV, political advertising, attention metrics) but are undermined by the rendering architecture

---

## DELIVERABLE 2: DESIGN SPECIFICATION

### Refined color palette

| Role | Hex | Usage |
|---|---|---|
| Primary dark | `#0A0E17` | Backgrounds, hero sections |
| Surface dark | `#111827` | Card backgrounds, secondary surfaces |
| Surface mid | `#1E293B` | Elevated cards, hover states |
| Accent primary | `#3B82F6` | Primary CTAs, interactive elements, data highlights |
| Accent secondary | `#06B6D4` | Charts, secondary highlights, gradients paired with primary |
| Accent warm | `#F59E0B` | Attention/alert callouts, stat highlights, badges |
| Success | `#10B981` | Performance indicators, upward metrics |
| Text primary | `#F8FAFC` | Headings on dark backgrounds |
| Text secondary | `#94A3B8` | Body copy, descriptions |
| Text muted | `#64748B` | Captions, labels |
| Border subtle | `#1E293B` | Card borders, dividers |

**Gradient system:**
- Hero gradient: `linear-gradient(135deg, #3B82F6 0%, #06B6D4 100%)` — used for accent bars, hero overlays, and primary CTA hovers
- Dark gradient: `linear-gradient(180deg, #0A0E17 0%, #111827 100%)` — section transitions

**Rationale:** This palette moves from "dark website" to "premium data platform." The blue-cyan anchor signals trust and technology. The amber accent creates visual tension for performance metrics and attention callouts — directly reinforcing the brand's core concept.

### Typography pairing

| Role | Font | Weight | Size range |
|---|---|---|---|
| Display / H1 | **Inter** or **Satoshi** | 700–800 | 48–72px |
| Section headings / H2 | Inter or Satoshi | 600–700 | 32–40px |
| Subsection / H3 | Inter or Satoshi | 600 | 24–28px |
| Body | **Inter** | 400 | 16–18px, 1.6–1.7 line height |
| UI / Labels | Inter | 500 | 13–14px, all-caps with 0.05em tracking for labels |
| Code / Data | **JetBrains Mono** | 400 | 14px — for any technical specs or data displays |

**Why Inter/Satoshi:** Clean, geometric sans-serifs that read as modern and technical without being cold. Excellent at small sizes for data-dense sections. Satoshi has slightly more character for display use if you want to differentiate from the Inter-everywhere baseline.

### Layout principles

1. **Max content width:** 1280px, centered with `auto` margins
2. **Section vertical padding:** 96–120px top/bottom — generous breathing room between sections
3. **Grid:** 12-column with 24px gutters; content blocks on 4-col, 6-col, or full-width
4. **Card system:** Rounded corners (12–16px), subtle `border: 1px solid #1E293B`, background `#111827` — no heavy drop shadows; elevation comes from border and background contrast
5. **Section rhythm pattern:** Alternate between full-width dark sections and slightly lighter card-grid sections to create visual cadence
6. **Stat blocks:** Large numeric displays (48–64px, weight 700, accent color) with small descriptive labels beneath — these are visual anchors
7. **Image/graphic treatment:** Abstract data visualizations, particle networks, or gradient meshes — no stock photography of "people in meetings"

### Section-by-section redesign notes

**Navigation (global)**
- Sticky header, `#0A0E17` with slight blur backdrop
- Logo left, primary nav center, CTA button right ("Get Started" or "Book a Demo")
- Dropdown mega-menu for "Platform" revealing: DSP, Attention Intelligence, Attribution, Self-Serve
- "Solutions" dropdown: Advertisers, Publishers, Agencies
- Blog and About as secondary items; Contact absorbed into the CTA button

**Hero section**
- Full-viewport height, dark gradient background
- Left-aligned headline + subheadline + CTA stack
- Right side: abstract visualization (attention signal graphic, or animated data particle field)
- Three stat blocks inline below the CTA: "50+ attention signals" / "18% lower CPA" / "32% higher recall"
- Trust strip below hero: partner/integration logos (Innovid, Kochava, Cedara, Prebid)

**Platform section**
- Three-column card layout: DSP | Attention Intelligence | Attribution
- Each card: icon, heading, 2-line description, "Learn more →" link
- Below: full-width explanatory section with left text / right graphic pattern

**Advertisers section**
- Problem → solution narrative flow
- Left column: "The problem" (fragmented buying, wasted spend, opaque measurement)
- Right column: "PGAM's answer" (unified platform, attention-optimized inventory, transparent reporting)
- Below: horizontal flow of 3–4 steps showing "How it works"

**Publishers section**
- Revenue-focused messaging
- Stat blocks: fill rate, yield improvement, transparency metrics
- Card grid of capabilities: AI traffic shaping, margin control, first-party data activation

**Blog**
- Magazine-style grid: featured post (large), 3 recent posts (cards), category filter bar
- Each card: post image, category tag, title, excerpt (2 lines), read time
- Internal linking CTAs within posts pointing to platform/product pages

**Contact**
- Split layout: left side with direct contact options (email, phone, Calendly embed), right side with a short form
- Below: office location, trust badges, partner logos

---

## DELIVERABLE 3: REWRITTEN COPY — Existing Sections

### Homepage hero — REWRITE

**Current:** "Performance Media Optimized by Attention"

**New headline:**
> Every satisfying ad impression starts with attention. We measure it. You profit from it.

**New subheadline:**
> PGAM Media's proprietary Attention Layering Platform scores 50+ engagement signals in real time — isolating the impressions that actually drive outcomes across CTV, display, video, and mobile.

**Primary CTA:** `See the Platform`
**Secondary CTA:** `Book a Demo`

---

### About page — REWRITE

**Current positioning (from search index):** "About PGAM Media | Performance Marketing Experts" — generic, could be any agency.

**New headline:**
> Built for the buyers who read the log-level data

**New body:**
> PGAM Media is not an agency. We are an infrastructure company.
>
> We built a unified SSP/DSP platform from the ground up because we got tired of watching performance budgets evaporate inside opaque auction chains. Our Attention Layering technology evaluates 50+ engagement signals — time-in-view, completion rate, interaction depth, device context — to isolate the inventory that holds attention and converts.
>
> We work with agencies, advertisers, and publishers who demand transparency, performance accountability, and access to premium supply without the markup.
>
> Headquartered in Coral Gables, Florida. Integrated with Prebid, Innovid, Kochava, and Cedara. Serving buyers across CTV, mobile, display, and video.

**Stats row:**
- 50+ attention signals scored per impression
- 18% lower CPA vs. CTR-optimized campaigns
- 32% higher brand recall in high-attention segments
- Direct publisher paths — no hidden resellers

---

### Platform page — REWRITE

**Current title:** "The Platform" — completely generic.

**New headline:**
> One platform. Every screen. Attention-verified.

**New body:**
> The PGAM platform connects supply and demand through a single transparent layer — powered by real-time attention scoring, AI bid optimization, and direct publisher relationships.
>
> **For advertisers:** Execute cross-channel campaigns across CTV, OTT, display, video, and mobile web. Access curated PMPs, activate attention-scored audiences via Deal IDs, and measure outcomes through a unified attribution layer.
>
> **For publishers:** Maximize yield with AI-powered traffic shaping, adaptive margin controls, and first-party data integration. Our SSP evaluates demand quality to protect your inventory and grow your revenue.
>
> **For agencies:** Replace the black-box stack with full log-level transparency. Allocate budgets toward inventory proven to hold attention — not just inventory that clears the auction.

---

### Advertisers page — REWRITE

**New headline:**
> Stop buying impressions. Start buying attention.

**New body:**
> Most programmatic platforms optimize for delivery — did the ad load? PGAM optimizes for engagement — did anyone actually watch?
>
> Our Attention Layering Platform scores every impression against 50+ signals before your bid fires. The result: your budget flows only toward inventory that holds attention, drives completion, and converts.

**Capabilities grid:**

| Capability | Description |
|---|---|
| Cross-channel execution | CTV, OTT, display, video, mobile web — unified campaign management |
| Attention-scored inventory | Every impression evaluated on engagement quality, not just viewability |
| Curated PMPs | Direct publisher relationships with transparent pricing and brand safety |
| Real-time optimization | AI-driven bid adjustments based on live attention signals |
| Unified attribution | Cross-device, view-through, and outcome-based measurement in one dashboard |

**CTA:** `Request a media plan` / `Book a demo`

---

### Publishers page — REWRITE

**New headline:**
> Your inventory is worth more than the open auction says.

**New body:**
> PGAM's SSP uses AI-powered traffic shaping and attention scoring to match your best inventory with the highest-intent demand. No more racing to the bottom on CPMs.
>
> We evaluate demand quality before it reaches your ad server — blocking low-value buyers, optimizing floor prices in real time, and surfacing your premium placements to the buyers willing to pay for verified attention.

**Key stats to display:**
- Higher effective CPMs through attention-premium pricing
- AI-driven traffic shaping reducing wasted impressions
- First-party data activation without third-party dependency
- Full transparency — log-level reporting on every transaction

**CTA:** `Become a supply partner`

---

### Contact page — REWRITE

**Current title:** "Contact PGAM Media | Let's Work Together" — acceptable but bland.

**New headline:**
> Let's talk numbers.

**New subheadline:**
> Whether you're an advertiser looking to lower CPA, a publisher ready to grow yield, or an agency that needs transparent programmatic infrastructure — we should talk.

**CTA options:**
- `Book a 15-min intro call` (Calendly embed)
- `Email us directly` → hello@pgammedia.com
- `Request a media plan` (form)

---

## DELIVERABLE 4: NEW SECTION COPY

### A. Programmatic DSP Platform

**Section heading:**
> The PGAM DSP

**Subheading:**
> Cross-channel programmatic execution, built on attention — not assumptions.

**Body copy:**
> The PGAM DSP is a demand-side platform purpose-built for performance buyers who need more than reach. We connect advertisers to premium inventory across CTV, OTT, display, video, and mobile web — with every impression scored for attention quality before your bid is placed.
>
> Unlike platforms that optimize toward delivery metrics, PGAM's bidding infrastructure evaluates engagement signals in real time: time-in-view, completion rate, scroll depth, device context, and interaction patterns. The result is a buying environment where spend flows toward inventory that holds attention and drives measurable outcomes.

**Capability blocks (card layout):**

**Cross-channel campaign execution**
Launch and manage campaigns across CTV, OTT, display, video, and mobile web from a single interface. Unified frequency management, creative rotation, and pacing across every screen.

**Premium supply access**
Direct publisher relationships and curated private marketplaces — not remnant inventory aggregated through six resellers. You know where your ads run.

**Transparent buying**
Full log-level data on every impression. See the publisher, the placement, the attention score, and the price. No hidden fees. No opaque auction dynamics.

**Real-time bidding infrastructure**
Sub-50ms bid response times. AI-optimized bid shading. Predictive attention scoring applied before the bid — not after the report.

**Brand safety controls**
Pre-bid filtering across content categories, domain blocklists, and contextual signals. Integrated with IAS and DoubleVerify for third-party verification.

**Primary CTA:** `Request DSP access`
**Secondary CTA:** `See a live demo`

---

### B. Self-Serve DSP Access

**Section heading:**
> Self-serve programmatic. No agency required.

**Subheading:**
> Launch CTV and digital campaigns in minutes — with the same targeting precision and attention-scored inventory that powers our managed service.

**Body copy:**
> Programmatic advertising has been locked behind agency retainers and managed service minimums for too long. PGAM's self-serve DSP gives SMB and mid-market advertisers direct access to premium CTV, display, and video inventory — with built-in attention scoring, real-time optimization, and transparent pricing.
>
> No six-figure minimum. No black-box reporting. Just a clean interface, powerful targeting, and inventory that actually performs.

**"How it works" flow (4 steps):**

**1. Set up your campaign**
Define your objective, budget, and schedule. Choose your channels: CTV, display, video, or run across all three.

**2. Build your audience**
Target by geography, device type, content affinity, household composition, or activate PGAM's proprietary attention segments via Deal IDs. Layer in first-party data or use our pre-built audience packs.

**3. Launch**
Go live in minutes. Our platform handles bid optimization, frequency capping, and brand safety automatically. Every impression is attention-scored before your budget is spent.

**4. Optimize and measure**
Real-time dashboards show spend, delivery, attention scores, and outcomes. Adjust targeting, creative, or budgets on the fly. Attribution built in — not bolted on.

**Comparison block:**

| Feature | PGAM self-serve | Typical managed service |
|---|---|---|
| Time to launch | Minutes | Days to weeks |
| Minimum spend | Low, flexible | $25K–$100K+ |
| Reporting | Real-time, self-service | Weekly PDF from your rep |
| Attention scoring | Built in | Not available |
| Markup | Transparent platform fee | Hidden margin + agency fee |

**Primary CTA:** `Request early access`
**Secondary CTA:** `Book a demo`

---

### C. Attention Intelligence Platform

**Section heading:**
> Attention Intelligence

**Subheading:**
> Not all impressions are created equal. We score them so you only buy the ones that matter.

**Body copy:**
> The PGAM Attention Intelligence Platform is a proprietary scoring system that evaluates every available impression against 50+ engagement signals — before your bid is placed. The output is a set of audience segments defined not by who the user is, but by how they engage.
>
> This is a fundamental shift in targeting. Demographic data tells you someone is 35 and lives in Miami. Attention data tells you they watch CTV ads to completion, engage with mid-roll placements, and respond to direct-response creative. One of these drives performance. The other drives waste.

**The eight attention segments:**

Each segment should be displayed as a card with an icon, name, and one-line description:

| Segment | Description |
|---|---|
| **High Attention** | Users consistently scoring in the top decile for time-in-view and interaction depth across all formats |
| **CTV Premium** | Viewers completing 95%+ of CTV ad units with measurable post-exposure engagement signals |
| **Active Engagers** | Users who interact — click, scroll, hover, expand — at 3x the baseline rate |
| **Lean-Forward Mobile** | Mobile users exhibiting high scroll depth, session duration, and return frequency |
| **Completion Champions** | Video viewers reaching 100% completion across pre-roll, mid-roll, and outstream |
| **Cross-Screen Converters** | Users whose attention pattern spans CTV, mobile, and desktop within attribution windows |
| **High-Intent Browsers** | Users whose browsing behavior, dwell time, and content consumption signal purchase consideration |
| **Primetime Streamers** | CTV viewers engaging during peak evening hours with premium long-form content |

**How buyers activate these segments:**
> Every attention segment is packaged as a Deal ID. Buyers activate them through the PGAM DSP or through any third-party DSP that supports PMP deals. No custom integration. No data transfer. Just a Deal ID that maps directly to attention-verified inventory.
>
> This is PGAM's data moat. While other platforms sell commoditized inventory segmented by age and gender, PGAM segments by the only metric that predicts outcomes: whether someone is actually paying attention.

**Primary CTA:** `Explore attention segments`
**Secondary CTA:** `Request a Deal ID`

---

### D. Attribution & Measurement

**Section heading:**
> Unified attribution. One source of truth.

**Subheading:**
> Stop reconciling five dashboards. PGAM consolidates cross-channel measurement into a single reporting layer.

**Body copy — The problem:**
> Attribution in programmatic advertising is fragmented by design. Your CTV campaign reports through one platform. Display through another. Mobile through a third. Each uses different methodologies, different lookback windows, and different definitions of "conversion." The result: conflicting data, double-counted conversions, and no clear picture of what actually worked.

**Body copy — PGAM's solution:**
> PGAM's attribution layer sits across all supply sources and stitches together a unified view of campaign performance — from first impression to final conversion. We integrate directly with Innovid for CTV delivery verification and Kochava for cross-device attribution, giving buyers a consolidated reporting environment they can actually trust.

**Measurement capabilities (icon + label + description):**

**View-through attribution**
Track conversions that occur after ad exposure — even without a click. Critical for CTV, where click-based measurement is meaningless.

**Pixel-based conversion tracking**
Place PGAM pixels on landing pages, confirmation pages, and key funnel steps. Real-time conversion data feeds back into bid optimization.

**Household graph matching**
Map CTV impressions to household-level devices — connecting a living room ad view to a mobile conversion on the same Wi-Fi network.

**Cross-device measurement**
Follow the user journey from CTV exposure to desktop research to mobile purchase. Deterministic and probabilistic matching across device types.

**Outcome-based reporting**
Report on what matters: CPA, ROAS, cost-per-completed-view, attention-adjusted CPM. Not vanity metrics.

**Incrementality testing**
Run hold-out groups and exposed/unexposed comparisons to measure true lift — not just last-touch attribution credit.

**Integration callout strip:**
> Integrated with: **Innovid** · **Kochava** · **Cedara** · **Prebid** · **IAS** · **DoubleVerify**

**Primary CTA:** `See a reporting demo`
**Secondary CTA:** `Talk to our measurement team`

---

## DELIVERABLE 5: DEVICE-BASED AUDIENCE TARGETING — New Section

*Modeled on the structure and depth of start.io/device-based-audience/*

**Section heading:**
> Device-based audience targeting

**Subheading:**
> Reach real households. On real devices. With signals that don't depend on cookies.

**Hero body copy:**
> In a post-cookie world, device-level signals are the most durable and accurate foundation for audience targeting — especially across CTV, where cookies never existed. PGAM's device-based targeting uses IP intelligence, ACR data, device graphs, and contextual signals to build audience segments tied to real devices in real households.
>
> No probabilistic guesswork. No third-party cookie dependency. Just deterministic signals mapped to the screens where your ads will run.

**Section: Data signals we use**

| Signal | Description |
|---|---|
| **IP-based intelligence** | Household-level geolocation, ISP identification, and network context — mapping ad exposure to physical locations |
| **ACR (Automatic Content Recognition)** | Real-time viewing data from smart TVs identifying what content is being watched, enabling content-affinity targeting |
| **Device graph** | Deterministic and probabilistic linkage across household devices — CTV, mobile, tablet, desktop — for cross-screen activation |
| **Contextual signals** | Content category, genre, daypart, and programming context used to target moments of engagement, not just users |
| **First-party publisher data** | Registration data, subscription tiers, and viewing history from direct publisher relationships |

**Section: Audience segment categories**

**By device type**
CTV-only households, mobile-primary users, multi-screen households, smart TV owners by manufacturer — target the device environment that matches your creative format.

**By content affinity**
Sports viewers, news consumers, entertainment streamers, reality TV audiences, kids' content households — built from ACR data and publisher content taxonomies.

**By household composition**
Household size, estimated income bracket, presence of children, urban/suburban/rural classification — derived from IP intelligence and device density signals.

**By viewing behavior**
Binge viewers, primetime-only watchers, cord-cutters, ad-supported tier subscribers, live sports viewers — behavioral segments built from actual viewing patterns.

**By engagement quality**
High-attention households (PGAM's proprietary layer), high completion rate devices, high interaction-rate environments — the only segments that predict performance outcomes.

**Section: How advertisers activate**

**Via Deal ID**
Every device-based audience segment is packaged as a Deal ID accessible through the PGAM DSP or any third-party DSP supporting PMP deals. No SDK. No pixel. No data transfer.

**Via self-serve DSP**
Build custom audiences using PGAM's self-serve interface. Combine device signals with attention scoring, geography, and daypart targeting. Launch in minutes.

**Via managed service**
Work with PGAM's campaign team to build bespoke audience strategies combining device-level data with first-party advertiser data and attention intelligence.

**Section: Why device-level targeting outperforms cookies for CTV**

> Cookies were built for desktop web browsers. They have never worked on CTV. They are increasingly blocked on mobile. And they tell you nothing about whether someone is in the room when the ad plays.
>
> Device-level signals solve these problems:
>
> - **Persistence:** Device IDs and IP mappings are durable across sessions and don't reset when a user clears their browser
> - **Household context:** CTV is a shared screen. Device-level data maps to the household — not an individual browser session
> - **Cross-screen linkage:** A device graph connects the CTV in the living room to the mobile in the viewer's hand — enabling sequential messaging and cross-device attribution
> - **Privacy compliance:** Device-based targeting operates within consent frameworks (TCF, USP/CCPA, GPP) without relying on third-party tracking scripts
>
> Cookies measure a browser. Devices measure a household. For CTV, there is no comparison.

**CTA:** `Explore device-based audiences` / `Request a segment list`

---

## DELIVERABLE 6: NAVIGATION RECOMMENDATION

### Proposed primary navigation

```
Logo                                                    [Book a Demo]

Platform ▾         Solutions ▾        Resources ▾       About    Contact
```

**Platform dropdown (mega-menu):**
| Column 1 | Column 2 |
|---|---|
| **DSP** | **Attention Intelligence** |
| Cross-channel programmatic buying | Proprietary audience attention scoring |
| → Learn more | → Learn more |
| | |
| **Self-Serve Access** | **Attribution & Measurement** |
| Launch campaigns in minutes | Unified cross-channel reporting |
| → Request early access | → Learn more |

**Solutions dropdown:**
| Column 1 | Column 2 |
|---|---|
| **For Advertisers** | **For Publishers** |
| Lower CPA through attention | Grow yield with AI optimization |
| → Learn more | → Learn more |
| | |
| **For Agencies** | **Device-Based Audiences** |
| Transparent programmatic infra | Household-level targeting at scale |
| → Learn more | → Learn more |

**Resources dropdown:**
- Blog
- Case studies (new — to be built)
- Political advertising insights
- Documentation / Integration guides

### Proposed footer structure

```
Platform              Solutions           Resources         Company
─────────             ─────────           ─────────         ─────────
DSP                   Advertisers         Blog              About
Self-Serve            Publishers          Case Studies      Contact
Attention Intelligence Agencies           Docs              Careers
Attribution           Device Audiences    Newsletter        Press

[Partner logos: Innovid · Kochava · Cedara · Prebid · IAS · DoubleVerify]

© 2026 PGAM Media · Privacy Policy · Terms of Service
Coral Gables, FL
```

---

## DELIVERABLE 7: IMPLEMENTATION PRIORITY ORDER

### Phase 1 — Critical (Week 1–2)

**1. Fix the rendering architecture**
The single most damaging issue. Move to Next.js with SSR/SSG or implement prerendering. Until this is fixed, search engines and every tool that previews links (Slack, LinkedIn, email) see only the `<title>` tag. No amount of content work matters if it's invisible.

**2. Rewrite the homepage hero**
First impression, highest-traffic page. Implement the new headline, subheadline, stat blocks, and dual CTA. Add the trust strip with integration partner logos.

**3. Restructure the navigation**
Implement the mega-menu with Platform and Solutions dropdowns. This immediately communicates the depth of PGAM's offering to every visitor.

### Phase 2 — High impact (Week 2–4)

**4. Build the DSP platform page**
This is the single most important new section. It positions PGAM as a technology company, not an ad network. Use the copy from Section A above.

**5. Build the Attention Intelligence page**
The eight audience segments + Deal ID activation model is PGAM's core differentiator. This page should be the thing every sales deck links to.

**6. Rewrite Advertisers and Publishers pages**
Replace the current generic copy with the rewritten versions above.

### Phase 3 — Differentiation (Week 4–6)

**7. Build the Attribution & Measurement page**
This closes the loop: targeting → delivery → measurement. Buyers need to see this to trust the full-funnel story.

**8. Build the Self-Serve DSP page**
This signals growth ambition and opens a new buyer segment. Include the "Request early access" CTA to build a waitlist.

**9. Build the Device-Based Audiences page**
This is the content-marketing play — a searchable, linkable page that competes for organic traffic against start.io, The Trade Desk, and Peer39.

### Phase 4 — Polish & growth (Week 6–8)

**10. Redesign the blog**
Magazine-style layout, category filtering, internal linking strategy. Each post should drive traffic toward product pages.

**11. Add case studies section**
Even one or two anonymized performance case studies (with real metrics) will outperform ten blog posts for conversion.

**12. Implement the full design system**
Apply the color palette, typography, spacing, and card system globally. Ensure consistency across all new and existing pages.

**13. SEO technical foundations**
- Server-side rendering (from Phase 1)
- Unique `<title>` and `<meta description>` per page
- `sitemap.xml` generation
- JSON-LD structured data for Organization, Article, FAQ
- Open Graph and Twitter Card meta tags for link previews

---

## BRAND TONE-OF-VOICE DIRECTION

**Voice:** Authoritative. Precise. Performance-led.

**Principles:**
1. **Lead with the metric, not the adjective.** "18% lower CPA" beats "dramatically improved performance." Numbers are the language of media buyers.
2. **Name the problem before the solution.** Buyers don't trust companies that only talk about how great they are. Acknowledge the industry's dysfunction — then show how PGAM fixes it.
3. **Be specific or be silent.** "Advanced targeting capabilities" says nothing. "50+ attention signals scored per impression before your bid fires" says everything.
4. **No startup enthusiasm.** PGAM is infrastructure. The tone is confident, technical, and direct — like a platform that doesn't need to sell you because the data already did.
5. **Avoid:** "Cutting-edge," "next-generation," "innovative," "leverage," "synergy," "holistic," "end-to-end" (unless paired with specifics), "seamless," "unlock."
6. **Use:** Concrete verbs. Short sentences. Specific numbers. Named integrations. Direct comparisons.

**Reference voice:** Think The Trade Desk's investor communications crossed with Cloudflare's product pages. Technical credibility meets clear business value.

---

*End of specification.*
