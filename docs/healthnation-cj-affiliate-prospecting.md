# HealthNation × CJ — which advertisers fit, and what has to happen first

**Date:** 2026-08-30
**Question:** what's on the CJ account that would align with healthnation.com?
**Verified against:** `mastap150/destination-com` @ `main` (CJ wiring),
`mastap150/healthnation-web` @ `main` (bouncer + schema),
`docs/healthnation-amazon-affiliate-review-2026-08-30.md` (current state).

---

## 1. TL;DR

**Nothing on the CJ account aligns with HealthNation today, because the account
is a travel account.** Every joined advertiser is lodging or tours. There is no
health advertiser on it to wire up.

That is a smaller problem than it sounds, because the account is a *container*,
not a roster — CJ's catalog is browseable without joining anything, so the
health bench is one API call away. Three things are worth knowing before you
spend time in the members portal:

1. **healthnation.com is almost certainly not a website on the CID yet.** CJ
   approves a *property*, not a company. destination.com is website
   `101849129`; a health site needs its own id, and advertisers judge the
   application on that site's content. This is the gate in front of everything
   else.
2. **Wiring costs zero code.** healthnation-web's `/go/[network]/[slug]`
   bouncer already handles CJ correctly by accident — see §4. This is not a
   build, it's a data change.
3. **CJ is the wrong network for the supplement catalog you already have,**
   and the right one for the content you haven't monetised. See §5. The 12
   products in `products-seed.json` are premium DTC brands that mostly run
   Impact/ShareASale/direct. CJ's health strength is retailers, gear, and
   gated services.

`scripts/cj_healthnation_prospect.py` (added alongside this doc) turns §5 from
a list of guesses into a data pull. Run it with the PAT and it reports the real
advertiser roster per content lane.

---

## 2. What the CJ account actually is

From `destination-com/docs/CJ_AFFILIATES.md` and `src/data/cj-advertisers.ts`:

| | |
|---|---|
| Publisher CID | `7112482`, live 2026-08-02 |
| Website id (PID) | `101849129` — **destination.com** |
| Auth | `CJ_PAT` bearer token, `developers.cj.com` → Authentication |
| Catalog endpoint | `ads.api.cj.com/query` (GraphQL) |
| Ledger endpoint | `commissions.api.cj.com/query` (31-day window cap) |
| Puller | `destination-com/scripts/cj-pull.mjs` |

Joined advertisers, all travel:

| Advertiser | Status | Rate |
|---|---|---|
| Hotels.com (`1702763`) | approved 2026-08-14, linkId `13344203` | ~4% |
| Vrbo | approved 2026-08-14, linkId `10697641` | 2% + $20/listing |
| Casa Andina | accepted 2026-08-17 via AffiliRed | 4% / 30d |
| EF Adventures (`7556501`) | accepted Aug 2026, linkId `17133009` | **$250 flat CPA** |
| Club del Sole (`7748599`) | joined, parked | 4% / 30d |
| Marriott / Hilton / IHG / Hyatt | applied, `linkId: null` | 4–6% |
| Expedia US (`1874913`) + 15 POS | approved, **deliberately not activated** | duplicate of Partnerize |

Credit cards are approved-but-empty: the finance catalog returns zero until
individual issuers approve the property (TILA/Reg Z). That is the same gating
model health services will apply.

---

## 3. The gate: property, not company

`cj-advertisers.ts` records this the hard way — *"Passing the wrong one errors
with 'cannot access requested publisherid' — burned an hour of my life on
2026-08-14."* The click URL segment is the **website id**, not the CID.

Consequences for HealthNation:

- **Add healthnation.com as a second website under CID 7112482** (members.cj.com
  → Account → Websites). Until it exists there is no PID to mint links against
  and no property for an advertiser to approve.
- **Travel approvals do not carry over.** Hotels.com approving destination.com
  says nothing about a health site. Every health advertiser is a fresh
  application, judged on healthnation.com's published content.
- **Applications will be read against the live site.** The audit found articles
  carrying Amazon links with no disclosure block (`/[hub]/[slug]` renders none).
  A reviewer landing on an undisclosed affiliate page is a rejection risk on a
  health property specifically, where networks are strictest. **Fix the
  disclosure gap before applying, not after** — that's P0 item 2 in the audit
  and it just became load-bearing for something else.

---

## 4. Wiring costs nothing — the bouncer already does CJ

`healthnation-web/src/app/go/[network]/[slug]/route.ts` looks up
`affiliate_url`, logs the click, then:

```ts
if (amazonTag && (network === 'amazon' || isAmazonUrl(final))) { … }
else if (dest && network === 'skimlinks') { … }
return NextResponse.redirect(final, { status: 302 });
```

Neither branch fires for `network === 'cj'`, so `final` stays as the stored
`affiliate_url` and the route 302s to it verbatim — which is exactly right,
because a CJ tracking URL *is* the affiliate URL. CJ pre-signs it; nothing
needs wrapping at redirect time the way Skimlinks does.

And `migrations/0003-products-and-reviews.sql` already declares the column as
free-text with CJ named in the comment:

```sql
affiliate_network  TEXT,   -- 'impact' | 'shareasale' | 'cj' | 'amazon' | 'skimlinks' | 'direct'
```

**So: no migration, no route change, no new library.** Wiring a CJ advertiser
is an UPDATE on `healthnation.products` — set `affiliate_url` to the CJ click
URL and `affiliate_network` to `'cj'`. Clicks then log with
`affiliate_network='cj'` and Q7 of the affiliate audit splits revenue by
network for free.

This compares well against the alternative the audit recommended (install
Skimlinks): CJ is an account that already exists, pays direct merchant rates
rather than a share of them, and needs less code than the network currently
wired to zero revenue.

The one thing worth adding later is a `cj` branch that appends a per-placement
`sid` parameter, so `pos=top_pick` vs `comparison_table` is visible in CJ's own
reporting and not just ours. Not required to earn.

---

## 5. Where CJ's health bench actually is

**This section is a hypothesis, not an observation.** I could not query the
account — this was a cloud session and `CJ_PAT` lives in destination-com's
local `.env.local` (per CLAUDE.md, cloud sessions start with zero project
credentials). Network membership also churns; brands move between CJ, Impact
and ShareASale. Treat the named brands as search terms for the members portal,
and let the script settle it.

The structural claim I'm confident in is the shape, not the names:

> CJ's health vertical is **retailers, gear, and gated services**. The premium
> DTC supplement brands are mostly elsewhere.

That matters because it inverts the obvious plan. The 12 products already in
`products-seed.json` — Thorne, Seed, AG1, Ritual, Momentous, Pure
Encapsulations, Nordic Naturals, NOW, Optimum Nutrition, Klean Athlete — are
largely Impact / ShareASale / direct-program brands. **Applying to CJ hoping to
fix those 12 rows is likely to fail on most of them.** CJ earns its place on a
different set of pages.

### 5a. Supplement retailers — the catalog fallback lane

Worth searching for: **iHerb, Vitacost, Puritan's Pride, Life Extension,
Swanson, Vitamin Shoppe, GNC**.

Rate is lower than a direct brand deal (retailer margins), but the coverage is
the point: a retailer feed carries thousands of SKUs including many of the 12
brands. That makes it the **fallback lane** — the answer to "we mention a
supplement we have no direct deal with", which is precisely the job Skimlinks
was supposed to do and never did. One approval covers the long tail.

This is the lane that most directly replaces the dead Skimlinks rows.

### 5b. Fitness equipment and wearables — the highest-fit lane

Worth searching for: **Bowflex/Nautilus, NordicTrack/iFIT, Under Armour,
Reebok, Fitbit**, plus recovery gear.

The audit's own P3 conclusion applies verbatim here: *"Where Amazon earns its
keep on a health site is equipment and wearables, not supplements."* Same is
true of CJ, and better — gear carries high AOV, real feeds, and rates above
Amazon's health-category floor. Maps onto Fitness, Recovery and Biomarkers in
the nav (`02_healthnation_com_structure.md`): HRV & Wearables, Foam Rolling &
Tools, VO2 Max Training, Cold & Heat Therapy.

**This is the lane I'd chase first.** It has no incumbent — there is no gear
content monetised on the site at all today — so it adds revenue rather than
re-plumbing revenue that already exists.

### 5c. Diet, meal delivery and programs

Worth searching for: **Nutrisystem, WeightWatchers, BistroMD, Diet-to-Go**, and
the meal-kit brands.

Flat CPA rather than percentage, often $20–$100/signup — the EF Adventures
shape, where one conversion beats dozens of 4% bookings. Maps onto Nutrition →
Diets & Plans (Mediterranean, intermittent fasting, keto, caloric deficit),
which is real published surface.

**Editorial caution:** HealthNation's stated positioning is *"Evidence-first.
No wellness fluff or detox teas"* with a trust bar promising **"No affiliate
bias."** Commercial diet programs sit closer to that line than gear does. Worth
a deliberate yes/no rather than joining because the CPA is good.

### 5d. Lab testing and telehealth — invisible to the API

**Function Health, InsideTracker, LetsGetChecked, Everlywell** are named in
`08_monetization_strategy.md` at $30–$60/signup, and they're the best fit on
the site — the Longevity → Biomarkers menu (Blood Panels to Track, ApoB, CGM,
Inflammation Markers) is built for them.

They publish **no product feed**, so the prospecting script cannot see them, in
exactly the way the credit-card catalog reads empty. These have to be found by
hand in members.cj.com → Advertisers → Health & Wellness, and several run on
Impact rather than CJ. Check both.

### 5e. What to skip

- **Nootropics, testosterone boosters, "detox", weight-loss supplements.** High
  CPA, and a direct contradiction of the site's own article standards. The
  audit's framing applies: affiliating a product your own articles contradict
  destroys E-E-A-T. The monetization doc already sets this rule — *"Only
  affiliate supplements with legitimate evidence."*
- **Health insurance.** Highest payouts in the vertical, but brand-risk next to
  medical content and heavy compliance. Not a first move.

---

## 6. Order of operations

**P0 — before touching CJ**
1. Add the affiliate disclosure block to `/[hub]/[slug]`. Compliance today,
   and an approval blocker for every application in §5.
2. Add healthnation.com as a website under CID 7112482. Note its PID.

**P1 — find out what's actually there**
3. Run the prospector from a machine with the PAT:
   ```bash
   export CJ_PAT=...            # same token as destination-com/.env.local
   export CJ_CID=7112482
   python3 scripts/cj_healthnation_prospect.py
   ```
   Replaces §5a–5b with observed advertiser names, ranked by how many
   HealthNation content lanes each one stocks.
4. Browse members.cj.com → Advertisers → Health & Wellness by 7-day EPC for the
   feed-less half (§5d).

**P2 — apply, narrowly**
5. Apply to one retailer (§5a) and two or three gear advertisers (§5b). Not
   twenty — each application is judged, and a thin approval history on a new
   property is better spent on programs you'll actually place.

**P3 — wire**
6. On approval, set `CJ_HEALTHNATION_WEBSITE_ID` and re-run the script to
   confirm link minting against the new PID.
7. `UPDATE healthnation.products SET affiliate_url = '<cj click url>',
   affiliate_network = 'cj'` for the covered SKUs. No deploy needed.
8. Read Q7 of `scripts/healthnation_affiliate_audit.py` after a few weeks —
   clicks by network is now a live comparison of CJ vs Amazon on the same
   pages.

---

## 7. Open items

- **Whether healthnation.com is already a website on the CID.** Inferred to be
  absent from `cj-advertisers.ts` documenting only `101849129`; not observed.
  One look at members.cj.com → Account → Websites settles it.
- **Which of §5's brands are on CJ vs Impact vs ShareASale.** Memory, not data.
  The script answers the feed half; the portal answers the rest.
- **Whether CJ passes a placement parameter we can read back.** The `sid`
  approach in §4 is standard CJ but unverified against this account.
- **Rates.** None of the percentages in §5 are quoted from CJ — advertiser
  rate cards are only visible post-approval, which is also when they can
  change.
