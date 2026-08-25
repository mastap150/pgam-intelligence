# EF Adventures — CJ Link Pack Review

**Received:** Aug 2026, 57-link CJ export (`links_5.csv`) forwarded for review
**Property in scope:** destination.com
**Verdict:** **Run it.** $250 flat CPA is a strong rate — 5× what a hotel booking returns —
and at ~$4/article the payback threshold is one booking per ~60 articles. Deploy deep text
links across the Europe/solo cluster and commission the Tour de France piece. Two things
still gate it: the **cookie window** and whether the CPA fires on **deposit or final
payment**. Do not deploy the four 2025 banners.

**Revised 2026-08-25** after Priyesh confirmed the rate ($250 CPA via CJ). The original
verdict — "narrow test, don't commission content" — was written without it and is
superseded below.

---

## What this file actually is

Not a curated shortlist. It is the entire creative feed for **one advertiser**:

| Field | Value |
|---|---|
| Advertiser | EF Adventures (CID `7556501`) — all 57 rows |
| Category | Vacation — all 57 rows |
| Relationship status | Active — all 57 rows |
| Publisher website id | `101849129` — all 57 rows |
| Link types | 38 Text Link, 18 Banner, 1 Evergreen Link |
| Targeted countries | blank on all 57 (no geo restriction declared) |
| Coupon codes | none — every offer is a landing page, not a code |

The PID `101849129` is **our destination.com CJ website id** (confirmed in
`docs/expedia-affiliate-decision.md` and `docs/affiliate/club-del-sole-evaluation.md`).
So these are correctly built for the right property — this is not a generic pack someone
pasted from a different account.

---

## Are they "tested"?

**Two different questions, two different answers.**

### Tracking integrity: yes, verified

Everything checkable in the file is internally consistent:

- All 57 `LINK ID`s match the id embedded in their own `CLICK URL`. No copy-paste
  mismatches between the tracking id and the creative.
- All 57 carry PID `101849129`. None are missing our publisher id, which is the classic
  way a pasted link earns nothing.
- All 57 are `Active` — no expired relationships.
- The five hostnames (`kqzyfj.com`, `anrdoezrs.net`, `dpbolvw.net`, `tkqlhce.com`,
  `jdoqocy.com`) are CJ's normal rotating redirect pool. Nothing unusual.

### Live redirects: not verified, and not verifiable from here

Cloud sessions run behind an egress proxy that **blocks all five CJ redirect domains**
(403 on CONNECT, policy denial). So no link in this pack has been click-tested end to end
in this review. Somebody needs to click a sample manually before launch — specifically the
deep links, to confirm each lands on the tour page its name claims. See the copy defects
below for why that is not a formality.

### Earnings history: thin, but now interpretable

CJ reports EPC per **hundred** clicks. 54 of 57 links report `N/A`, which means the network
has insufficient activity to compute a figure at all.

| Link | 3-month EPC | Per click |
|---|---|---|
| Evergreen Link (`17141071`) | $8.12 / 100 | $0.081 |
| Home page text link (`17133003`) | $0.00 | $0.00 |
| Logo 125x125 banner (`17129942`) | $0.00 | $0.00 |

**With the $250 CPA known, that EPC becomes a conversion-rate reading.** EPC is the rate
times the conversion rate, so:

    $0.0812 / click ÷ $250 CPA = 0.0325% → one booking per ~3,080 clicks

That is the network-wide click-to-booking rate across all CJ publishers carrying this
program. It is low, but entirely normal for a $3,000–$6,000 considered purchase with a
multi-month decision cycle.

The important consequence: **a high CPA does not raise the EPC — the EPC already contains
it.** Eight cents a click is the bottom line, and knowing the rate is $250 doesn't change
that number, it just explains it. What a good placement changes is the conversion rate.

| Scenario | Conv. rate | Clicks per booking | EPC/click | Sessions per booking @ 3% CTR |
|---|---|---|---|---|
| Network average | 0.032% | 3,080 | $0.081 | ~103,000 |
| 3× — well-matched article | 0.097% | 1,030 | $0.244 | ~34,000 |
| 10× — high-intent article | 0.325% | 308 | $0.812 | ~10,000 |

destination.com should beat the network average, because the network average includes
coupon sites and generic travel aggregators sending untargeted traffic. A Camino de
Santiago guide sending readers to EF's Camino tour is about as matched as affiliate traffic
gets. But the honest expectation at current scale is **low hundreds of dollars a year**,
arriving as one or two lumpy $250 hits — not a steady trickle.

### Why it is still worth doing

Two reasons the modest EPC doesn't sink it:

**1. Content is nearly free.** `06_automation_workflow.md` puts cost per article at
**$3.50–$4.50**, or roughly $12–18 with the freelance editor pass at 30 min/article. One
$250 booking pays for **56–71 raw articles, or 14–21 edited ones.** Against a 4% hotel
commission you need six bookings to clear the same bar. Here you need one.

**2. It is incremental to display, not instead of it.** The same 34,000 sessions earn
$510–$1,360 in display at the $15–40 travel RPM in `08_monetization_strategy.md`. Display
still out-earns this affiliate by 2–5× on identical traffic — but the affiliate link rides
on top of traffic already being monetized. The marginal cost of adding a contextual CTA to
an article that exists is zero.

---

## Defects found — fix or avoid before launch

### 1. Four 2025 banners have broken render dimensions

The `width`/`height` in the `<img>` tag contradicts the creative's own name:

| Link ID | Name says | Tag renders at | Mobile optimized |
|---|---|---|---|
| `17129936` | ADV_160x600 | **56 × 210** | No |
| `17129937` | ADV_300x250 | **120 × 100** | Yes |
| `17129940` | ADV_728x90 | **342 × 42** | No |
| `17129939` | ADV_468x60 | **351 × 45** | No |

Dropped in as-is, these render at roughly a third of their intended size — unreadable, and
in the 728x90 slot, visibly broken. Either the tag attributes are wrong or the assets are.
Strip the `width`/`height` attributes and size in CSS, or don't use these four. Given that
banners convert poorly on a considered purchase anyway, simply skipping them costs little.

The 14 banners from the Apr-2026 refresh (`17278845`–`17278855`) are clean: 300×250,
320×50, 600×500, 1280×330, 1280×164, 320×1200 all match their creatives.

### 2. Banner inventory doesn't cover our declared ad slots

`08_monetization_strategy.md` specifies 728×90 leaderboard, 300×250 in-content, and
300×600 half-page for destination.com. Against that:

- **300×250** — covered by `17278845`. Fine.
- **728×90** — only exists in the broken 2025 set. Nothing usable.
- **300×600 half-page** — does not exist in this pack at all. Closest is 320×1200, which
  is a different aspect ratio and will not drop into that slot.

The 1280×164 and 1280×330 units are full-width hero strips, not IAB slots. They'd need a
custom placement.

### 3. Two links share identical anchor copy but claim different destinations

| Link ID | Name | Anchor text |
|---|---|---|
| `17308031` | Spain Biking for Solo Travelers: **The Island of Mallorca** | "…Spain Biking for Solo Travelers: The Island of Mallorca" |
| `17308033` | **Portugal** Multi-Adventure for Solo Travelers: Algarve & Alentejo | "…Spain Biking for Solo Travelers: **The Island of Mallorca**" |

`17308033` carries Mallorca copy under a Portugal name. One of the two fields is wrong and
the export can't tell us which. Do not use `17308033` until someone clicks it — at $250 a
booking, routing Portugal-intent readers to a Spanish cycling tour is a real loss, not a nit.

### 4. One link's name and its own offer disagree

`17277142` is named "…save up to **$200**" while its anchor text reads "save up to
**$500**". Publishing either number without checking is a compliance risk on a discount
claim.

### 5. Two links are exact duplicates

`17133009` and `17166879` share an identical name *and* identical anchor text ("Find Your
Tour; We have the perfect trip for you"). Using both splits attribution across two ids for
no benefit. Pick one.

### 6. One offer expires in six days

`17315877` — "$500 off featured departures, **through August 31**" (promo window ends
7-Sep-2026). Today is 25 Aug. Fine for a newsletter or social post this week; do not hard-code
it into an evergreen article.

`17315882` (Tour de France 2027) also ends 7-Sep-2026, despite selling a 2027 departure.
Worth asking CJ whether that end date is a mistake.

The remaining promo windows are long — mostly running to 2027 or 2028 — so evergreen
placement is safe for those.

---

## Content fit: the real constraint

EF's inventory is **entirely European**, plus one Canadian Rockies tour. Geography counts
across all 57 link names and descriptions:

| Region | Presence |
|---|---|
| Alps / Switzerland / France / Italy / Dolomites | heavy (19 Alps mentions) |
| Spain & Portugal (incl. Camino, Mallorca, Algarve) | strong |
| Greece, Croatia/Slovenia, Ireland | one tour each |
| Canadian Rockies (Banff) | one tour |
| **Asia, Africa, South America, USA** | **zero** |

Set against the 50 planned articles in `04_article_ideas_destination.md`:

**Genuine deep-link matches (~8 articles):**

| Article | EF link | Strength |
|---|---|---|
| #12 Camino de Santiago guide | `17167876`, `17308028` | Strong — two dedicated Camino tours |
| #38 Solo female travel safety | the whole solo cluster (5 links) | Strong — EF's "solo, never alone" line is built for this |
| #4 Greece islands comparison | `17167878` Santorini & Crete | Good |
| #2 / #5 Portugal articles | `17167879` Algarve & Alentejo | Good |
| #11 Tuscany road trip | `17167881` Cinque Terre & Tuscany | Framing mismatch — road trip vs guided hike |
| #8 Croatia island hopping | `17167880` Istria & Julian Alps | Adjacent — inland, not island hopping |
| #1 Best time to visit Italy | Dolomites / Alps links | Adjacent |
| #39 Europe packing list | generic Europe links | Weak but placeable |

**Zero inventory (30 articles):** all 10 Asia, all 6 Africa, 7 of 8 Americas, and all 6
Points & Miles pieces. The $250 CPA does not change this — a good rate on inventory that
doesn't exist is worth nothing. The Europe cluster is the whole opportunity.

**Active intent conflict.** Several planned headlines are explicitly independent-travel or
budget-framed, which is the opposite of a $150-deposit premium guided group tour:

- #32 "Machu Picchu: Getting There **Without a Tour Group**"
- #17 "The Maldives **on a Budget**"
- #6 "How Much Does It Cost to Travel Spain? A **Realistic Budget** Breakdown"
- #43 "How to Avoid Tourist Traps"

destination.com's editorial voice is the independent traveller. EF sells the guided group
tour. On the solo-travel cluster those two genuinely converge — "adventure solo, never
alone" answers a real objection our #38 article raises. Everywhere else they pull apart.

There are also two Tour de France links (`17133008`, `17315882`) with a strong,
high-intent hook and **no article to host them**. With the rate now known, this is the one
content gap worth closing: high purchase intent, low SEO competition, genuine
exclusive-access inventory, and a $4 article against a $250 payout.

---

## What the rate answers, and what it doesn't

**Answered:** $250 flat CPA via CJ. On a $3,000–$6,000 tour that is an effective **4–8%**,
at the top of the travel band, and flat means a cheaper tour pays exactly the same. For
comparison, the Expedia/Partnerize hotel line destination.com already runs pays 4% — about
$48 on a $1,200 booking. This is **5× that per conversion.**

**Still open, and these matter more than the rate did:**

1. **Cookie window.** The single biggest remaining variable. Adventure tours carry a
   two-to-six-month consideration cycle. At 7 days (Expedia's window) almost every booking
   is lost to a later touchpoint; at 30–45 days a meaningful share survives. This could
   swing captured revenue 3–5×.

2. **Deposit or final payment?** EF's entire pitch is "$150 down." If the CPA fires on the
   deposit, conversion is fast and attribution holds. If it fires on final payment — often
   60–90 days before a departure that may be a year out — most of it dies in the cookie
   window regardless of length.

3. **Per booking or per traveler?** Group tours commonly book 2–4 people on one
   transaction. Per-traveler would double or quadruple effective value; per-booking is
   what's assumed above.

4. **Return/cancellation clawback.** Tour cancellations are common and often generous.
   Ask what reverses a paid CPA and over what period.

---

## Recommendation

1. **Ask CJ the four questions above** — cookie window, deposit vs final payment, per
   booking vs per traveler, clawback terms. The rate is good enough that these now decide
   how hard to lean in, not whether to run at all.
2. **Manually click-test the deep links.** This session could not (see below), and
   `17308033` is known-suspect. At $250 a booking, a link that lands on the wrong tour is
   an expensive defect, not a cosmetic one.
3. **Deploy deep text links first, banners second.** A $3,000+ considered purchase does not
   convert off a display banner. The 38 text links are where a $250 CPA actually gets
   earned; use the Apr-2026 banner set only as supporting furniture, and skip the four
   broken 2025 units entirely.
4. **Commission the Tour de France article.** This reverses the original recommendation.
   Two EF links (`17133008`, `17315882`) sell 2027 Tour de France access with no article to
   host them — high intent, low competition, and EF has genuine exclusive-access inventory.
   At $4 an article against $250 a booking, this is worth writing on spec.
5. **Weight the solo-travel cluster.** Article #38 (solo female travel safety) against EF's
   "adventure solo, never alone" line is the strongest intent match in the pack — EF's
   product directly answers the objection the article raises.
6. **Route through `/api/go/cj/{advertiser}`** like Hotels.com and Vrbo, so clicks land in
   `affiliate_clicks`. With one booking per ~1,000–3,000 clicks, network EPC will tell you
   nothing useful about our own placements for a long time. Our own click data will.

---

## Verification notes

- Structural checks (id/PID consistency, dimensions, duplicates, promo windows, geography)
  were run directly against the CSV and are reproducible from it.
- Live redirect resolution was **not** performed — egress proxy denies all five CJ
  redirect hostnames from a cloud session.
- EPC-per-hundred-clicks convention confirmed against `docs/affiliate/club-del-sole-evaluation.md`.
- destination.com CJ website id `101849129` confirmed against `docs/expedia-affiliate-decision.md`.
