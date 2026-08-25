# Claude design prompt — Destination.com media kit

Paste the block below into a fresh Claude session (Claude.ai, Claude Code, or
Claude Design) to regenerate or restyle the advertiser deck. It carries the
brand system, the data, and the constraints, so the session does not need this
repo to produce the deck.

Two things to change before you send it:

1. **The numbers.** Newsletter figures and every segment split are internal
   projections, not measured. Swap in real values once the ESP can report them.
2. **The rate card.** Indicative pricing. Confirm against what has actually
   been sold before it reaches an advertiser.

Companion file: `destination-com-media-kit.html` (the built deck).

---

## The prompt

```
Design an advertiser media kit for Destination.com — a travel publisher owned
and operated by PGAM Media. The audience is media buyers, brand marketers, and
affiliate/commerce partners. The deck's single job is to get a buyer to ask for
a plan against a specific audience segment.

Build it as a single self-contained HTML page: no external scripts or
stylesheets except Google Fonts, all CSS inline in one <style> block. Structure
it as a sequence of full-viewport "sheets" that scroll (not a click-through
slideshow) so it reads on a phone, with scroll-snap on desktop only, keyboard
arrow navigation, and a print stylesheet so it exports cleanly to PDF.

BRAND SYSTEM — use these exact tokens, they are the live destination.com
design system, do not substitute your own palette:

  --ink        #1a1714    near-black, primary text
  --ink-mid    #3d3530    secondary text
  --ink-soft   #6b5f58    warm neutral (biased toward the accent, not grey)
  --ink-faint  #a09484    captions, meta
  --paper      #faf7f2    primary ground
  --sand       #f5f0e8    alternate ground
  --sand-dark  #ede5d6    fills, tracks
  --terra      #c4622d    the single accent — spend it on numbers and rules
  --gold       #a8802a    secondary data emphasis only
  --deep       #1a2634    atlas navy, for the data sheets

  Display face: Playfair Display (500/700) — slide titles and figures only
  Body face:    Inter (400/500/600)
  Data face:    IBM Plex Mono — eyebrows, labels, rates, all tabular figures

Type rules: running text near 65 characters wide, text-wrap: balance on
headings, a touch of letter-spacing on uppercase mono labels,
font-variant-numeric: tabular-nums anywhere digits stack in a column.

LAYOUT CONCEPT: a numbered sheet sequence with a thin fixed left "compass
rail" carrying section ticks and a vertical label. Alternate warm sand sheets
for narrative against deep-navy sheets for the data moments (newsletter,
segmentation, close) so the numbers land. Flat panels and hairline terracotta
rules — no rounded cards with accent bars, no gradient hero, no emoji section
markers, nothing centered by default. The numbering is honest: a deck is a
real sequence, so number the sheets.

THEMING: the page must render correctly in light, dark, and unstamped-system
states. Define the complete light palette on bare :root; redefine only tokens
under @media (prefers-color-scheme: dark) guarded as
:root:not([data-theme="light"]); redefine them again under
:root[data-theme="dark"]. Every component color comes from a token — never a
literal, and never a color whose only definition sits inside a media or
[data-theme] block. Set an explicit background on body. Keep the deep-navy
sheets committed to dark in both themes.

SHEETS AND CONTENT:

0 · COVER — wordmark "Destination.com" with the period in terracotta, kicker
"Advertising & Partnerships · 2026", and the claim: "Guides written by
travelers who have actually been there — and an audience that books what we
recommend." A mono fact list alongside it: category (travel, points & miles),
owner (PGAM Media, O&O), 60,000 newsletter subscribers, 50%+ open rate, 10%
click rate, surfaces (web, email, iOS & Android).

1 · THE PROPERTY — a travel publisher that owns its own demand stack: PGAM
Media runs its own SSP, so there is no reseller between the buyer and the
inventory. Cover the editorial promise (guides written from the ground, no paid
rankings ever — sponsorship buys placement and attention, never a ranking
position), the publishing cadence (new guides weekly, existing guides revisited
on a rolling schedule so the catalogue stays current on visas, prices, and
seasonality), and why it is built for intent (the reader arrives mid-decision).
Then three reasons the audience is worth more than its size suggests: a
high-ticket category where one decision is worth thousands in flights, hotels,
tours, insurance and cards; a points & miles pillar that is a first-class
section rather than a footnote; and a closed loop where site, newsletter, and
app share one identity layer.

2 · THE WEBSITE — DO NOT STATE ANY TRAFFIC NUMBERS. Describe the structure
instead: a hub-and-spoke library where region hubs feed country guides, country
guides feed city and experience guides, and every guide routes back to the
planning and points content that converts. Four pillars as a flush 4-up grid:
Destinations (six regions — Europe, Asia, Americas, Africa, Pacific, Middle
East — down to city level); Experiences (adventure, food & drink, culture &
art, wellness — readers filter by how they travel, not only where);
Travel Guides (visas, best-time-to-visit, insurance, packing, safety, airports,
booking windows — highest intent, last thing read before booking); Points &
Miles (card bonuses, transfer sweet spots, hotel redemptions, tracked weekly).
Then four supporting notes: the AI Trip Planner (dates, budget, party, trip
type in; a day-by-day costed itinerary with live hotel and flight options out —
every plan is a declared intent and sponsorable as such); the native iOS and
Android app (guides, saved trips, signed-in identity, a push channel); live
commerce rails (hotels, rentals, packages, cars, tours and activities routed
through live Expedia Group, Viator, and GetYourGuide integrations); and a line
that full traffic, geography, and viewability reporting is shared on request
under NDA.

3 · THE NEWSLETTER — deep navy. Headline the three figures as large serif
numerals: 60,000 subscribers, 50%+ open rate (~30,000 opens per issue, against
a 28–38% typical range for travel lists at this size), 10% click rate (~6,000
clicks per issue, a click-to-open near 20%). Note the list is grown from
editorial, not bought or co-registered. Under the figures put a mono strip
deriving what a $2,500 primary sponsorship works out to: $42 CPM delivered,
$83 CPM on opens, $0.42 per click. Then the send structure: a Tuesday flagship
(destination deep-dive, visa and entry changes, flight deals, one honest
review; one primary sponsor and one native dispatch per issue, never more) and
a Friday award-travel drop to the points & miles segment only. Close with
reader-value-first sponsorship framing and 72-hour per-placement reporting.

4 · SEGMENTATION — the argument is "you are not buying 60,000 people, you are
buying the right 9,000." Every subscriber declares interests and regions at
signup and every click refines them; segments are addressable individually,
combinable, and available for standalone sends from 5,000 names up. Render each
cut as horizontal proportional bands (not pie or donut charts), showing percent
and absolute count:

  By interest — points & miles 22% (13,200), adventure & outdoors 18% (10,800),
  beach & islands 16% (9,600), food & wine 13% (7,800), culture & city breaks
  12% (7,200), luxury & slow travel 10% (6,000), budget & backpacking 9%
  (5,400)

  By trip stage — dreaming, no dates yet 45%; planning, booking in 30–90 days
  32%; booking, in-market now 15%; just back, reviewing and resharing 8%

  By region intent — Europe 34%, Asia 22%, Americas 19%, Africa 11%, Middle
  East & Pacific 8%, open to anywhere 6%

  By engagement — opens every issue 28%, most issues 34%, occasional 26%,
  re-engagement 12%. Note that a frequency-capped campaign can buy the top two
  tiers only.

  By geography — US 58%, UK 12%, EU 10%, Canada 8%, Australia & New Zealand
  7%, rest of world 5%. About two thirds of opens are on mobile.

5 · INVENTORY & RATES — one scannable table, grouped, with right-aligned mono
rates. Note that pricing is rate card before volume and annual terms, that
everything is à la carte or packaged, and that programmatic buyers can
transact the display inventory directly through PGAM's own seats as a PMP deal.

  Newsletter — primary sponsorship (top-of-issue billboard, logo lockup, 40
  words, one link, one per issue) $2,500/issue; native dispatch (in-body
  sponsored block, image, 80–100 words in house voice, labelled, one per issue)
  $1,500/issue; text link block (three-line classified in the resources footer)
  $600/issue; segment-targeted send (5,000-name minimum) $900 per 10k; Points &
  Miles solo send (13,200 names, card and financial services) $2,000; dedicated
  solo send (full list, single advertiser) $4,500; presenting sponsor (primary
  slot in all 13 issues of a quarter plus one solo send) $26,000/quarter.

  Site display — run of site (responsive leaderboard and in-content 300×250,
  max three in-content units per article) $8–12 CPM; high impact (sticky
  300×600 half-page, desktop sidebar, highest viewability on the property)
  $15–22 CPM; mobile anchor (320×50 sticky, five-second delay) $10–14 CPM;
  Trip Planner results (sponsored slot inside a generated itinerary, with
  destination, dates and budget declared — the highest intent inventory on the
  property) $18–25 CPM.

  Site content & ownership — sponsored guide (1,500+ words to house editorial
  standard, labelled, evergreen, internally linked) $1,500–3,500; region hub
  sponsorship (presenting brand on a hub and its guides, 30 days)
  $3,000/month; homepage takeover (hero surround and trust bar, 24 hours, one
  advertiser) $2,500/day.

  Commerce — commerce partnership (preferred placement in booking CTAs,
  comparison tables, and planner output; revenue share or hybrid) custom;
  launch package (four newsletter primaries, one region hub month, one
  sponsored guide — $16,500 rate card value) $14,000.

6 · STANDARDS — open with the observation that most of what depresses
performance in this category is self-inflicted: too many units, unverified
supply, disclosure as an afterthought. Then six cards: verified direct supply
(authorised seats declared DIRECT in ads.txt and machine-checked daily, a
missing or downgraded line pages an engineer the same morning); owned and
operated (one publisher, one domain, no arbitraged or resold traffic in the
path); a hard three-unit cap (no more than three in-content ads per article,
none in the first 200 words, never two stacked); no dark patterns (no pop-ups,
pop-unders, interstitials, or auto-playing video — Core Web Vitals are a
revenue input, not a compliance chore); labelled always (sponsored labels above
the fold, paid links marked, affiliate disclosure to FTC standard); editorial
independence (sponsorship never buys a ranking, a rewrite, or the removal of a
recommendation — that is why a click here is worth buying).

7 · CLOSE — deep navy. "Let's build the plan against a segment, not a guess."
Three numbered steps: tell us the audience you actually want (region, interest,
trip stage, or a combination) and we size it in a day; we come back with a
costed plan, availability, and traffic and viewability detail under NDA; test
on a single issue or a two-week flight with full reporting within 72 hours of
the last send. Contact block in mono: Destination.com — Advertising &
Partnerships, PGAM Media, advertise@destination.com, pgammedia.com.

Finish with a small mono footnote, in the deck itself: newsletter subscriber
counts, open and click rates, and all segment splits are internal projections
for planning purposes and are not independently audited; site traffic,
viewability, and geography detail are shared on request under NDA; rate card
pricing is indicative and excludes agency commission, volume, and annual
commitment terms.

TITLE the page "Destination.com Media Kit".
```

---

## Variants worth asking for

Follow-ups that work well from the same session:

- **"Now give me a one-page version"** — a single-sheet leave-behind: cover
  facts, the three newsletter figures, the interest segments, and a five-line
  rate summary.
- **"Rebuild sheets 3 and 4 for a card issuer"** — leads on the points & miles
  segment and puts the $2,000 solo send at the top of the rate card.
- **"Make a tourism-board version"** — leads on region intent and the region
  hub sponsorship, drops the card and financial-services framing.
- **"Turn this into a .pptx"** — same content, same tokens, as slides for
  sending as an attachment rather than a link.
