# Conocer Digital Holdings — Strategic Fit Assessment for PGAM Media

**Prepared:** 2026-08-18
**Source material:** *Conocer Digital — Buyer Audience & Media Overview* (19 slides, data as of Jul 21–22, 2026) and *Conocer Digital — DD Workbook* (9 tabs, pulls Jul 20–23, 2026). Both files were supplied twice; the duplicates are byte-identical.
**PGAM baseline used for comparison:** internal `pgam-intelligence` repo — LL revenue scaling plan (Apr 17, 2026), DSP managed-service rate card v1.0, MSN syndication stack, margin model (`core/margin.py`), partner scorecard (May 4, 2026).

---

## 1. Bottom line

**Not a fit as an acquisition on current disclosure. Strong fit as a commercial partnership.**

The two largest sources of value in this deal are *PGAM capabilities applied to Conocer's assets* — not Conocer capabilities PGAM would be buying:

1. PGAM's DSP managed service (CTV/OLV + Attention + Invoca call attribution) sold into Spanish-language Medicare / ACA / legal / home-services demand, using Conocer's creative studio and Latino supply pool.
2. PGAM's MSN / SmartNews / NewsBreak PV-growth playbook applied to a Spanish-language feed.

Both can be captured through a supply-and-co-sell agreement at zero capital risk. Neither requires owning the equity.

Meanwhile the things you *would* be buying — the audience, the Meta page monetization, the podcast network, the direct-sales function — are small, declining, platform-dependent, and presented with reconciliation problems serious enough that the headline revenue figure cannot be taken at face value.

**Recommendation:** commercial partnership now; revisit acquisition in two quarters priced against proven partnership performance, and only after the Google AdX / MCM question below is answered in writing.

---

## 2. What Conocer actually is

A Hispanic-audience digital media network built around one flagship brand (MundoNOW), plus adjacent properties:

| Asset | Scale | Notes |
|---|---|---|
| Facebook — 9 pages | 8.75M followers, 261.5M views (mixed 30/90d) | 5.9M followers sit on the flagship page alone |
| Instagram — 5 accounts | 391K followers, 38.2M views (30d) | 380K of 391K on one account |
| YouTube — MundoNow | 691K subs, 237.5K views (90d) | 60% of watch time on TV; World Cup-driven |
| TikTok — MundoNow | 90.5K followers, 237K views (28d) | Views down 15.2% MoM |
| X / Twitter | 8.7K followers | Dormant |
| mundonow.com | 1.49M active users, 3.79M PV / 90d | ~1.26M PV/mo |
| Óyenos podcast | 22 titles, 708.5K downloads YTD, 309K listeners | 22% ad fill rate |
| HispanIQ | Latino CTV/video curation marketplace | Magnite Curated Seats + Index Marketplace + Cadent |
| Direct ad sales | $35.9K contracted lifetime on the tracker | One named sales owner |
| Editorial tech | Astro headless CMS, in-house editorial dashboard, AI translation | Genuinely built, not licensed |
| Creator roster | 10 freelancers, 3 Emmy winners | Freelance, not employed |
| Mundo Media Press | 1/3 minority equity | Non-controlling, print + mundohispanico.com, Atlanta |

The "11.7M reach" headline is a **sum of follower counts across platforms with acknowledged overlap**, not a deduplicated audience. 75% of it is Facebook followers, which are not sellable ad inventory — you monetize them through Meta's programs or through branded-content posts, not through a supply path PGAM controls.

---

## 3. The financials

### 3.1 Three different revenue totals in one package

| Figure | Where it appears |
|---|---|
| **$26,232 / mo** | Deck slide 2 ("Executive Summary") and slide 15 chart |
| **$23,300.66 / mo** | DD Workbook, Revenue Summary tab (the underlying detail) |
| **$17.4K** | Deck slide 19 ("confirmed revenue", closing slide) |

The workbook is the auditable number. The deck's slide-15 bars ($8,999 / $8,300 / $5,500 / $1,200 / $1,100 / $566 / $345 / $231) do not map to the workbook's line items — there is no $8,300 or $5,500 revenue line anywhere in the workbook.

### 3.2 The workbook's own detail (June 2026 basis)

| Source | Monthly | Basis |
|---|---:|---|
| Meta (Facebook page monetization) | $8,999 | QuickBooks June actual |
| Direct Sold | $4,900 | QuickBooks June actual |
| **MCM — terminated June 30, 2026** | **$3,908** | *ends* |
| Connatix / MediaTradecraft | $2,814 | Apr–Jun avg |
| Podcast (Óyenos) | $1,120 | QuickBooks June actual |
| Third-party syndication (Microsoft et al.) | $900 | Partial — "missing data on others" |
| LinkBy (affiliate) | $429 | 90-day avg |
| Taboola | $231 | 90-day avg |
| HispanIQ | $0 | Apr–Jun avg; July forecast also $0 |
| **Total as presented** | **$23,301** | |
| **Recurring baseline ex-MCM** | **$19,393** | *the honest run-rate* |

**The MCM line is dead revenue.** It is labelled "Terminated June 30, 2026" in the workbook and is still inside the $23,301 total. Strip it and the real forward run-rate is **~$19.4K/mo — about 26% below the $26.2K the deck leads with.**

### 3.3 The July "estimate" is not a run-rate

The workbook forecasts $33,639 for July. That is driven by a $15,000 Direct Sold line — the entire PDLM Law Firm campaign, a **two-month** flight (Jul 16 – Sep 15) booked into a single month. On a monthly basis it is ~$7.5K, and it is a one-off. Meta's $10,500 is "trending MTD." Neither belongs in a normalized run-rate.

### 3.4 The single biggest unanswered question: Google AdX

Buried in a footnote on the Revenue Summary tab:

> *"a 'Google Adx' revenue line ($67,662 Apr, $16,475 May, $0 Jun) far larger than any source above… intentionally excluded here pending explanation of the June drop to zero."*

Sitting next to it: **"MCM — Terminated June 30, 2026."** MCM is Google's Multiple Customer Management programme. The most likely reading is that Conocer's Google demand access ran through an MCM parent that was terminated, or that Google took a policy/invalid-traffic action.

This matters more than anything else in the package:

- In April, this one line was **~3× the entire rest of the business combined**. If it is recoverable, Conocer is a materially larger company than it is presenting. If it was terminated for cause, it is a materially riskier one.
- **For PGAM specifically, a Google policy termination is close to disqualifying.** PGAM's whole compliance stack (`agents/compliance/` — schain audit, sellers.json validator, ads.txt monitor, brand-safety sweep, block lists) exists to keep contaminated supply out of the chain. PGAM's Pubmatic, Magnite, Verve and Xandr relationships are worth substantially more than Conocer is. You do not want a supply asset with a Google enforcement history attached to your seat.

**Nothing should progress until the termination notice and stated reason are produced in writing.**

### 3.5 No cost side at all

The package contains **no P&L, no headcount cost, no creator/contributor costs, no hosting or tech spend, no EBITDA**. A business carrying 10 creators, an editorial team, a sales function and two in-house tech products on ~$19–23K/mo of revenue is, on any reasonable assumption, **loss-making**. This is an asset purchase, not a cash-flow acquisition, and it should be priced and structured as one.

### 3.6 Other reconciliation failures

| Item | Value A | Value B | Gap |
|---|---|---|---|
| Podcast revenue, June 2026 | $1,120 (workbook) | $69.71 (deck slide 10) | **16×** |
| Podcast downloads, 2026 YTD | 708,513 (overview) | 896,966 (per-title total) | 27% |
| Mundo Narco downloads | 362,679 (workbook) | 290,536 (deck) | 25% |
| Direct-sold customers | 3 in QuickBooks | Only 1 of 3 maps to the tracker | — |

The workbook is admirably honest about most of these — it flags them itself. But taken together they mean **no number in this package can be relied on without independent verification**, and diligence cost will be higher than the deal size justifies.

---

## 4. Audience and traffic quality — the part that should worry an SSP operator

**Traffic composition (mundonow.com, GA4, 90 days):**

| Channel | Share of sessions |
|---|---:|
| Organic Social | 41.4% |
| Referral (SmartNews / NewsBreak / MSN) | 35.5% |
| Direct | 9.9% |
| Unassigned | 9.7% |
| **Organic Search** | **3.6%** |

**77% of sessions are platform-sourced.** Only 3.6% organic search and 9.9% direct. That is the classic profile of a low-intent, arbitrage-adjacent audience, and the monetization confirms it: **$1.49 RPM** on GA4-tracked inventory, **$3.10 RPM** through Connatix/MTC. For comparison, PGAM's own O&O monetization plan targets $15–40 RPM on destination.com and $20–50 on healthnation.com.

**Everything measurable is declining:**

- Podcast download run-rate on the three flagship shows: **−29% to −45% YoY**
- TikTok video views: **−15.2% MoM** (−42,500)
- MiMundo Sabor: 161,557 followers → **2,830 views in 30 days**
- MiMundo Latina: 408,972 followers → **72,723 views in 30 days**
- Connatix RPM: $4.23 (Apr) → $2.43 (May) → $2.92 (Jun)

Several of the nine Facebook pages are effectively zombie audiences inflating the reach headline.

**Geography dilutes the sellable audience further:** the podcast is Mexico-dominant across nearly every title, and Instagram is 37% US / 12% Venezuela / 8.5% Mexico. Mexican and LatAm impressions clear at a fraction of US CPMs. The US-sellable audience is materially smaller than 11.7M implies.

**Meta concentration:** Facebook page monetization is 39–48% of revenue and is entirely at Meta's discretion. Meta has cut creator monetization programmes repeatedly and without notice. This is the single largest revenue line and PGAM would have zero control over it.

---

## 5. HispanIQ — the one genuinely interesting asset, and its problem

HispanIQ is a Latino-audience CTV/video curation marketplace: Magnite Curated Seats + Index Exchange Marketplace + Cadent audience extension, with supply from McClatchy, Paramount, Spanglish and MundoNOW, selling into agency PMPs (Horizon Media, IPG/Kinesso, Publicis, WPP pending).

**Conceptually this is the closest thing in the package to PGAM's business.** PGAM's stated structural problem in its own scaling plan is demand concentration — *78% of BidMachine-InApp and 99% of Algorix revenue flows through Pubmatic; one policy change = one-day catastrophe* — and open-market eCPMs of $0.59–$1.89. Agency-direct curated PMP demand is exactly the diversification PGAM needs.

**But the performance is close to zero, and the failure mode is diagnostic:**

| Deal | Bid requests | Won impressions | Revenue (3mo) |
|---|---:|---:|---:|
| Horizon × Primo Brands (Magnite) | — | 582,361 | $1,504.65 |
| Horizon × Primo Brands (Index) | — | 66,209 | $193.01 |
| **Horizon × CommonSpirit Health** | **8.4B** | **0** | **$0** |
| **Kinesso (IPG) private curation** | **3.1B** | **0** | **$0** |

Total: **$566/mo** of curator revenue, from a single advertiser relationship. Two agency deals with **11.5 billion combined bid requests and literally zero won impressions**. That is either deal mis-configuration, a floor/format mismatch, or — the worrying reading — a curated segment that agencies onboarded and then found no reason to buy.

**Three further cautions:**

1. **The segments don't match PGAM's supply.** PGAM's inventory is gaming/esports and in-app D&V (Modrinth, OP.GG, BidMachine, Algorix, Smaato, Illumin). You cannot fill a Latino-audience PMP with Modrinth traffic. The "plug PGAM's supply into HispanIQ's deals" synergy does not exist as stated.
2. **PGAM already has Magnite.** It can open its own curated seat. The incremental asset is the *agency contacts* and the *Latino supply pool*, not the seat.
3. **Assignability and key-person risk.** Magnite Curated Seat and Index Marketplace agreements are typically not transferable without consent, and Horizon/IPG relationships live with individuals, not entities. Confirm both before assigning any value.

---

## 6. Synergy map — scored

| # | Synergy | Mechanism | Est. annual value to PGAM | Verdict |
|---|---|---|---:|---|
| **A** | **Spanish-language performance CTV/OLV SKU** | PGAM DSP managed service (Attention + Invoca TFN attribution) + Conocer creative studio + Latino supply, sold into Medicare / ACA / insurance / legal / home services | **$100K–$600K gross profit** | **Strongest. Doesn't require ownership.** |
| **B** | **MSN / SmartNews / NewsBreak PV growth** | PGAM's existing MSN Partner Hub telemetry + headline A/B stack applied to a Spanish feed; Conocer already gets 35.5% of sessions from these sources | **$50K–$240K/yr media** at $4 CPM if PVs reach 3–5M/mo | **Strong, uniquely PGAM, unproven** |
| **C** | HispanIQ curation seats + agency PMPs | Demand diversification away from Pubmatic concentration | Currently $6.8K/yr; option value only | **Interesting, unproven, may not transfer** |
| **D** | Editorial tech reuse | Astro CMS + editorial dashboard + AI translation applied to destination.com / healthnation.com / boxingnews.com; Spanish editions at near-zero marginal cost | ~$50K–$150K of avoided build cost | **Real but modest** |
| **E** | mundonow.com supply onto PGAM's SSP | 2.67M impressions/mo moved off Connatix; PGAM captures the margin | ~$15K–$25K/yr net | **Immaterial** |
| **F** | Podcast fill-rate lift (22% → 45%) | ~117K impressions/mo today, +122K achievable at $6.53 eCPM | ~$10K/yr gross | **Immaterial; PGAM has no audio demand stack** |
| **G** | Direct-sales cross-sell | PDLM Law Firm is exactly PGAM SKU 2 (call-driven legal) | Small but immediate | **Real, small, proves the model** |
| **H** | Mundo Media Press 1/3 stake | Non-controlling minority in a print/local publisher | **Assume $0** | **Value at zero** |

**The pattern is the conclusion.** Synergies A, B and D — everything above $50K — are PGAM doing something to Conocer's assets. Synergies C, E, F and H — the things you'd actually be purchasing — total well under $50K/yr of realizable value.

### Scale context

PGAM's LL platform was running **$6.2–6.8K/day gross** as of the April scaling plan, plus ~$1K/day on TB, against a 30% minimum-margin policy. Conocer's entire business, at its honest ~$19.4K/mo recurring baseline, is roughly **10% of PGAM's gross media revenue** — and it is declining, while PGAM's plan targets $10–15K/day. On a net-revenue-retained basis the gap narrows (Conocer's revenue is largely net, PGAM's is gross), but Conocer is still the smaller, lower-quality, negative-trend asset.

---

## 7. Diligence questions that must be answered before any offer

**Blocking — do not proceed without these:**

1. The **Google MCM termination notice**, in writing, with the stated reason. Was this a commercial wind-down or an enforcement action?
2. Does **mundonow.com retain direct Google AdX / GAM access** today, in its own name? What is the current monthly AdX revenue?
3. Full explanation of the **AdX line's $67,662 → $16,475 → $0** trajectory, tied to Google's own reporting.
4. **A P&L.** Twenty-four months, by month. Headcount, creator costs, tech spend, hosting. What is EBITDA?

**Material:**

5. Reconcile the **three revenue totals** ($26.2K / $23.3K / $17.4K) and the deck's slide-15 bars to the workbook line items.
6. Reconcile podcast June revenue: **$1,120 vs $69.71**.
7. Reconcile podcast downloads: **708,513 vs 896,966**.
8. **Withheld syndication revenue** — SmartNews, NewsBreak, MS Publisher Hub are explicitly excluded from the package as "pending." Produce them. This is the segment PGAM is best equipped to grow, so its current size determines the whole Synergy B case.
9. **HispanIQ contracts**: Magnite Curated Seat agreement, Index Marketplace agreement, Cadent agreement, and the McClatchy / Paramount / Spanglish supply contracts. Are they assignable on change of control?
10. Why do **CommonSpirit Health (8.4B requests) and Kinesso (3.1B requests) have zero won impressions**? Deal config, floors, format mismatch, or no buyer intent?
11. **Meta monetization eligibility** — programme terms, any prior strikes, page-level monetization status for all nine pages.
12. **Invalid-traffic posture**: any IVT/MFA flags from Google, Meta, Taboola, Connatix, or the exchanges. Sourced-traffic spend, if any, by month. Given 77% platform-sourced sessions, this needs a direct answer.
13. **Creator contracts** — are the 10 creators (including the three Emmy winners) under contract, exclusive, and assignable? Who owns the IP in the back catalogue?
14. **Mundo Media Press** — cap table position, transfer restrictions, distributions history.

---

## 8. Recommended path

### Phase 1 — Commercial partnership (now, 90 days, no capital)

1. **Supply agreement.** mundonow.com display + video inventory and Óyenos audio inventory routed through PGAM's stack on a revenue share. Low value on its own; high value as an operating-data source you cannot get from a data room.
2. **MSN / SmartNews PV-growth engagement.** PGAM runs its existing telemetry and headline A/B playbook on the Spanish-language feed, paid on a share of incremental syndication revenue. This is the fastest way to test Synergy B for real.
3. **Co-sell a Spanish-language performance CTV/OLV SKU.** PGAM's DSP managed service, Conocer's creative studio and Latino supply. Start with the verticals already in PGAM's rate card — Medicare, ACA, legal — and use the PDLM legal campaign as the pilot, since PGAM's TFN call attribution is exactly what that advertiser should be buying.
4. **Curation pilot.** Fix or rebuild the two zero-fill agency deals inside HispanIQ using PGAM's floor and demand-tuning expertise. If PGAM can make CommonSpirit and Kinesso monetize where Conocer could not, that is both a revenue result and the clearest possible proof that PGAM adds something Conocer cannot do alone.

This captures the great majority of the identified value, generates the operating data required to underwrite a purchase, and risks nothing but time.

### Phase 2 — Revisit acquisition (Q1 2027, conditional)

Only if: the Google/MCM question resolves cleanly, a real P&L appears, and the partnership demonstrates traction.

**Structure:** asset purchase, not equity — you do not want Conocer's platform-enforcement history, unmatched QuickBooks entries or minority stakes. Carve out the Mundo Media Press interest entirely.

**Price anchor:** for a declining, platform-dependent business at a ~$19.4K/mo recurring baseline (~$233K/yr) with unknown-to-negative EBITDA, **0.5–1.0× recurring revenue on the media assets, or roughly $115K–$235K**, plus a separate earnout tied to actual HispanIQ curation revenue and to the Spanish-language DSP SKU's realized gross profit. The deck's $26.2K headline would imply a materially higher number and should not be the basis of negotiation.

Anything above ~$500K requires the AdX line to be explained, reinstated, and verified.

---

## 9. Walk-away triggers

Any one of these should end the process:

- The Google/MCM termination was an **enforcement action for invalid traffic or policy violation**. PGAM's exchange relationships are worth far more than this asset.
- **Meta monetization** carries prior strikes or is at risk — that is ~40% of revenue with no PGAM control.
- The **HispanIQ agreements are not assignable**, or the agency relationships are personal to individuals who are not staying.
- **No P&L is produced**, or the produced P&L shows a cash burn that the purchase price would have to fund.
- The **withheld syndication numbers** turn out to be zero *and* the seller continues to present a $26.2K headline that its own workbook contradicts.

---

## 10. One-line answer

**Conocer is a real Hispanic media asset with genuine strategic adjacency to PGAM's curation and CTV/OLV business — wrapped around a small, declining, platform-dependent P&L, a headline revenue number its own workbook does not support, and an unexplained Google revenue line that vanished six weeks before the data room was assembled. Partner with them; don't buy them yet.**
