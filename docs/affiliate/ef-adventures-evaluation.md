# EF Adventures — CJ Link Pack Review

**Received:** Aug 2026, 57-link CJ export (`links_5.csv`) forwarded for review
**Property in scope:** destination.com
**Verdict:** **Links are technically sound but commercially unproven, and the pack only
covers ~8 of the 50 planned articles.** Run it as a narrow Europe/solo-travel test on
existing content. Do not commission content for it, and do not deploy the four 2025
banners as shipped. **Get the commission rate before anything goes live — it is not in
this export.**

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

### Earnings history: effectively none

CJ reports EPC per **hundred** clicks. 54 of 57 links report `N/A`, which means the
network has insufficient activity to compute a figure at all.

| Link | 3-month EPC | Per click |
|---|---|---|
| Evergreen Link (`17141071`) | $8.12 / 100 | $0.081 |
| Home page text link (`17133003`) | $0.00 | $0.00 |
| Logo 125x125 banner (`17129942`) | $0.00 | $0.00 |
| The other 54 | N/A | no data |

The one real number is **3.2× Club del Sole's $2.50/100**, which is the last CJ program we
looked at and rejected. That is a meaningful improvement. But it is a single data point on
the *Evergreen Link* — CJ's auto-rotating unit, whose EPC reflects network-wide
performance across all publishers, not ours and not any specific creative. At $0.081/click
you need ~12,400 clicks to make $1,000.

**Nothing in this pack has a proven per-creative conversion record.**

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
Strip the `width`/`height` attributes and size in CSS, or don't use these four.

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
the export can't tell us which. Do not use `17308033` until someone clicks it.

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
Points & Miles pieces.

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
high-intent hook and **no article to host them**. That is the one content gap here worth
considering — but only after the rate is known.

---

## The missing number

**This export contains no commission rate and no cookie window.** Neither field exists in
a CJ link export. Without them the pack cannot be valued:

- Adventure tours run $3,000–$6,000. At 4% that is $120–$240 per booking — genuinely
  strong, several times a hotel commission.
- At 1–2%, against a months-long consideration cycle and a 7–30 day cookie, it is not
  worth the placement.

Ask CJ for the rate card, the cookie window, and whether the program pays on deposit or on
final payment. Tour operators frequently pay only on the balance, months after the click,
which interacts badly with a short cookie.

---

## Recommendation

1. **Get the rate card, cookie window, and payout trigger** from CJ before any deployment.
2. **Manually click-test** the deep links — this session could not, and `17308033` is
   known-suspect.
3. **Then run a bounded test** on the ~8 articles above, weighted to the Camino and
   solo-travel pieces, using the Apr-2026 banner set only.
4. **Do not commission content** for this program. The Tour de France angle is the only
   piece worth writing for, and only if the rate justifies it.
5. **Route through `/api/go/cj/{advertiser}`** like Hotels.com and Vrbo, so clicks land in
   `affiliate_clicks` and this can be judged on our own numbers rather than CJ's network EPC.

Better than the last CJ program we turned down, and correctly built for our account. But
"active and well-formed" is not "tested", and eight articles is a narrow base.

---

## Verification notes

- Structural checks (id/PID consistency, dimensions, duplicates, promo windows, geography)
  were run directly against the CSV and are reproducible from it.
- Live redirect resolution was **not** performed — egress proxy denies all five CJ
  redirect hostnames from a cloud session.
- EPC-per-hundred-clicks convention confirmed against `docs/affiliate/club-del-sole-evaluation.md`.
- destination.com CJ website id `101849129` confirmed against `docs/expedia-affiliate-decision.md`.
