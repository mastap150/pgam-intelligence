# HealthNation × Amazon Associates — what's actually wired, what earned, what to fix

**Date:** 2026-08-30
**Trigger:** Amazon Associates payments notice — Store ID `healthnation2-20`,
$0.55 commission income for 06/2026, unpaid, with payment method and tax
interview both flagged incomplete.
**Scope:** `mastap150/healthnation-web` (the Next.js site). Nothing
Amazon-related lives in `pgam-intelligence`; this doc is the record.

---

## 1. TL;DR

Amazon was never set up as a channel. It was set up as a **safety net** — a
server-side auto-tagger that stamps our Associate tag onto any `amazon.com`
link that happens to appear in AI-generated article or buyer-guide HTML.

That safety net is the **only affiliate mechanism on the site currently
capable of earning money.** The deliberate one — a 12-product catalog routed
through a click-tracking bouncer — is wired to Skimlinks, and **Skimlinks was
never installed.** Every one of those 12 "Check current price" buttons
currently redirects to an untagged brand homepage and earns exactly $0.

So the $0.55 is real signal: the incidental path converted, the intentional
path didn't. That inversion is the whole finding.

---

## 2. What is actually built

### 2.1 The Amazon auto-tagger — `src/lib/amazon-tag.ts`

Server-side string rewrite over content HTML. For every `<a href>` pointing at
an Amazon domain it:

- sets `tag=$AMAZON_PARTNER_TAG` (replacing any existing tag — no co-tagging),
- strips any existing `rel` and re-adds `rel="sponsored noopener"`,
- adds `target="_blank"`.

No-ops entirely when `AMAZON_PARTNER_TAG` is unset. Called from exactly two
render paths:

| Path | File | Line |
|---|---|---|
| Articles | `src/app/[hub]/[slug]/page.tsx` | 71 |
| Buyer guides | `src/app/best/[slug]/page.tsx` | 142 |

**This is where the $0.55 came from.** Nothing else on the site can produce an
Amazon-attributed click.

### 2.2 The click bouncer — `src/app/go/[network]/[slug]/route.ts`

Genuinely good infrastructure. Every outbound product click hits
`/go/{network}/{slug}`, gets logged to `healthnation.affiliate_clicks` with
source page, on-page position (`?pos=top_pick|inline|comparison_table|…`),
UTMs, session cookie and user agent, then 302s to the product's
`affiliate_url`. It also stamps the Amazon tag at redirect time if the
destination is an Amazon URL (route.ts:64).

Used by `/reviews/[slug]` (page.tsx:126) and the stack builder.

### 2.3 The product catalog — `scripts/products-seed.json`

12 supplements (Thorne, NOW, Nordic Naturals, Pure Encapsulations, Optimum
Nutrition, Seed, AG1, Momentous, Ritual, Klean Athlete). All 12 rows carry
`affiliate_network: "skimlinks"` and a bare brand product URL.

The seed script's own header comment states the plan: *"Skimlinks wraps these
on outbound click; when Impact direct deals come online we update the row to
the Impact tracking URL."*

### 2.4 Disclosure

`/affiliate-disclosure` carries the required Amazon language verbatim: *"As an
Amazon Associate, HealthNation earns from qualifying purchases."* Buyer guides
link to it (`best/[slug]/page.tsx:162`).

---

## 3. What worked

1. **The tag propagates end to end.** Env var → server render → live HTML →
   Amazon attribution → recorded commission. Every hop in that chain is
   verified by the fact that money arrived. That is not nothing.
2. **The safety-net design was the right instinct.** Content is AI-generated
   and unpredictable; catching Amazon links at render rather than trusting the
   generator to write tagged URLs is the correct architecture. It earned
   without anyone maintaining it.
3. **`rel="sponsored"` is applied automatically** — Amazon Operating Agreement
   and FTC-aligned by construction, not by editorial discipline.
4. **The bouncer's attribution model is better than most affiliate sites
   ship.** Position-level granularity (`pos=top_pick` vs `inline` vs
   `comparison_table`) is exactly the dimension you need to optimize placement.
   It's just pointed at a network that isn't live.

---

## 4. What didn't

### 4.1 The entire product catalog earns nothing — **highest impact**

Skimlinks is referenced in `.env.local.example` (`SKIMLINKS_PUBLISHER_ID=`),
in the migration's network enum, and in the seed script's comment. It appears
**nowhere in `src/`** — no script tag in `layout.tsx`, no wrapper, no API call.

Consequence: `/go/skimlinks/thorne-magnesium-bisglycinate` logs the click, then
302s to `https://www.thorne.com/products/dp/magnesium-bisglycinate` with no
tracking parameter of any kind. The click is recorded; the revenue is not.
Twelve products × every "Check current price" button on every review page.

### 4.2 Zero Amazon products in the catalog

`affiliate_network` supports `'amazon'` and the bouncer has a dedicated Amazon
branch — but no row uses it. The one network that is demonstrably earning has
no intentional placement anywhere on the site. All Amazon revenue is
accidental: whatever links Claude happened to write into body copy.

### 4.3 Amazon clicks bypass the bouncer entirely

Auto-tagged inline links go straight to `amazon.com`. They are never logged to
`affiliate_clicks`. We have no idea which article, which position, or which
product produced the $0.55 — only that something did. The one channel with
proven revenue is the one channel with zero first-party data.

### 4.4 International Amazon domains are mis-tagged

`AMAZON_HOST` matches 18 Amazon TLDs (`.co.uk`, `.ca`, `.de`, `.com.au`, …) and
`setAmazonTag` stamps the **US** tag on all of them. A US Associate tag on
`amazon.co.uk` earns nothing — those are dead clicks. Health content pulls
meaningful non-US organic traffic.

### 4.5 Article pages carry no disclosure

`/best/[slug]` links to `/affiliate-disclosure`. `/[hub]/[slug]` does **not** —
yet it runs the same auto-tagger and is the higher-traffic surface. Amazon's
Operating Agreement requires the disclosure on pages containing Associate
links. This is the one item on the list that is a compliance exposure rather
than a revenue leak.

### 4.6 `AMAZON_PARTNER_TAG` is undocumented

The code reads it; `.env.local.example` never mentions it. It exists only in
Vercel's dashboard. Any fresh clone or new environment silently loses all
Amazon revenue with no error — `tagAmazonLinks` no-ops by design.

### 4.7 Account housekeeping (from the notice itself)

- **Tax interview and payment method are incomplete.** Amazon withholds
  payment until both are done.
- **$0.55 is below the US payout threshold** ($10 direct deposit / $100
  check). It will sit indefinitely regardless.
- **Inactivity risk.** Amazon closes Associate accounts that go long stretches
  without a qualifying sale. One $0.55 order in June is thin cover.

---

## 5. Optimization — in priority order

### P0 — Stop the bleeding and de-risk the account

1. **Complete the tax interview and add a payment method.** Ten minutes, and
   nothing else on this list pays out until it's done.
2. **Add the disclosure block to `/[hub]/[slug]`.** Reuse the markup from
   `best/[slug]/page.tsx:162`. Compliance, not revenue.
3. **Add `AMAZON_PARTNER_TAG=` to `.env.local.example`** with a comment
   pointing at Vercel. Prevents a silent zero-revenue regression.

### P1 — Turn on the catalog

4. **Pick a network and actually install it.** Two viable paths:
   - *Skimlinks* — install the script, keep the 12 bare URLs as-is, and it
     wraps everything site-wide including brands we have no direct deal with.
     Fastest to revenue; lowest rate; takes a cut.
   - *Direct programs* — `08_monetization_strategy.md` already quotes Thorne
     10–15%, Pure Encapsulations 10%, Momentous 10–15%. That's roughly 3–10×
     Amazon's health-category rate. Slower (per-brand applications) but this is
     where the actual money is for a supplement site.

   **Recommendation: do both, in that order.** Skimlinks as the floor for
   long-tail brands, direct Impact/ShareASale deals for the top 5 brands by
   click volume — and the bouncer's click log tells you which 5 those are the
   moment it has traffic.

5. **Add Amazon as a deliberate second option per product.** For each of the
   12 products, add an Amazon `affiliate_url` alongside the direct one and
   surface a secondary "Also on Amazon" CTA routed through
   `/go/amazon/{slug}`. Rationale: Amazon's rate is poor but its *conversion
   rate* is far better than a cold brand checkout — logged-in, saved card,
   Prime shipping. On a low-margin/high-conversion vs high-margin/low-conversion
   split, you want both offered and the click log to settle the argument
   empirically. This also fixes 4.2 and 4.3 at once, since these clicks go
   through the bouncer.

### P2 — Fix the tagger

6. **Route auto-tagged inline links through the bouncer too.** Rewrite
   `tagAmazonLinks` to emit `/go/amazon/inline?u={encoded}&src={slug}` rather
   than a direct Amazon URL, and have the route stamp the tag and log with
   `pos=inline`. Turns the one earning channel from a black box into a measured
   one.
7. **Handle non-US Amazon domains.** Either enroll in **Amazon OneLink** (it
   handles geo-redirect and attribution automatically) or restrict
   `AMAZON_HOST` to `amazon.com` so we stop generating dead international
   clicks. OneLink is the better answer if non-US traffic is material.
8. **Don't add `target="_blank"` unconditionally** — if the source anchor
   already has one, the output carries a duplicate attribute. Cosmetic, but
   it's a two-line fix while the file is open.

### P3 — Play to Amazon's actual strength

9. **Amazon's cookie is 24 hours.** It converts in-session or not at all. That
   argues for high-purchase-intent placement — comparison tables, "Check
   price" buttons, buyer-guide top picks — and against incidental inline
   mentions in explainer articles, which is exactly what we have today.
10. **Amazon's health/supplement category is a low-rate category** (the repo's
    own monetization doc pegs it at 3–5%; verify against the current rate card
    in the Associates dashboard). Where Amazon earns its keep on a health site
    is **equipment and wearables**, not supplements — the same doc lists home
    gym gear under Amazon and Oura/Garmin/WHOOP as direct programs. If we want
    Amazon to be more than rounding error, the content that should carry it is
    gear roundups, not magnesium explainers.

---

## 6. Rough sizing

$0.55 at a low-single-digit health category rate implies somewhere in the
region of $15–50 of qualifying order value in June — one or two small orders.
That is a proof of wiring, not a revenue stream.

The number worth acting on is the counterfactual: **12 products × every review
page × every buyer guide, currently converting at $0** because the network
behind them was never installed. Fixing §5.4 is the difference between an
affiliate system that logs clicks and one that banks them.

---

## 7. Open items needing data I couldn't reach

This was a cloud session — no `DATABASE_URL`, and `healthnation.com` is
blocked by the egress proxy. Unverified from here:

- How many articles and buyer guides are actually published.
- How many contain `amazon.com` links at all (i.e. how wide the auto-tagger's
  surface really is).
- Whether `AMAZON_PARTNER_TAG` is set in Vercel prod — inferred from the fact
  that money arrived, not observed.
- Actual `affiliate_clicks` volume, and the position breakdown.

A local session with `DATABASE_URL` can settle all four in about four queries.
