# Expedia via CJ — run it or not?

**Date:** 2026-08-19
**Trigger:** CJ acceptance email for *Expedia United States* (CID 1874913), plus 15 other
Expedia points of sale.
**Verified against:** `mastap150/destination-com` @ `main`, 2026-08-19.

## Decision

**No. Do not deploy CJ Expedia US links. The rate card is identical to what is already live.**

This is now a settled comparison rather than a judgement call. `destination-com`
`docs/AFFILIATE_COMMISSION_MODEL.md` carries the Expedia Travel Creator Program rate card,
verified 2026-08-05 by Priyesh directly from the Partnerize Merchant Portal. Set against the
CJ acceptance email:

| Trip element | Partnerize (live, verified 2026-08-05) | CJ Expedia US (this email) |
|---|---|---|
| Hotel | 4% | 4% |
| Activities | 4% | 4% |
| Vacation rental | 2% | 2% |
| Package | 2% | 2% |
| Car | 1.5% | 1.5% |
| Flight / air | not supported | $0 |
| Cruise | not supported | 0% |
| Booking window | 7 days | 7 days |

Line for line the same. There is no rate arbitrage, no cookie-window advantage, and no
inventory CJ reaches that Partnerize does not. What CJ would add is a second tracker
competing for last click on the same points of sale, splitting one revenue stream across two
dashboards.

Note the CJ email contradicts itself on packages — the rate line says 2%, the T&C section says
Expedia pays no commission on packages or cruises at all. Partnerize pays 2%, which is another
reason to keep package CTAs where they are.

## What is actually live (checked, not assumed)

- **Expedia → Partnerize.** camref `1101l5I7Wc`, built in `src/lib/expedia.ts` via
  `buildExpediaUrl()` / `buildRawExpediaUrl()`, wrapped in the `/api/go/expedia` click bouncer
  that writes an `affiliate_clicks` row before the 302. `pubref` segments attribution per
  placement.
- **destination.com is already a CJ publisher.** CID `7112482` (live 2026-08-02), website id
  `101849129`. Documented in `docs/CJ_AFFILIATES.md`.
- **Two Expedia Group brands are already running through CJ, deliberately.** Hotels.com
  (advertiserId 1702763, ad id 13344203) and Vrbo (ad id 10697641), both approved 2026-08-14,
  registered in `src/data/cj-advertisers.ts` and routed via `/api/go/cj/{advertiser}`.
- **`HOTEL_CTA_PARTNER`** switches hotel CTAs between Partnerize and Hotels.com CJ; default is
  `partnerize`. `ACTIVITIES_CTA_PARTNER` does the same for activities, moving them to
  Viator/GetYourGuide at 8%+.

So the house already runs Expedia Group inventory across both networks on purpose. The CJ
Expedia US approval is not a new capability — it is a duplicate of the lane that already pays
the same rate.

## Correction to the first read of this

An earlier draft of this document assumed the existing integration was Expedia's EAN/EPS
partner path and that CJ's terms would therefore be the weaker, retail-tier offer. Both halves
were wrong. The live integration is the **Partnerize Travel Creator Program**, not EAN/EPS —
the "EAN deep links" language in `destination-redesign-mockups/DEVELOPMENT-SPEC.md` §3.2 was
planning language that did not survive into the shipped code. And the CJ rates are not worse
than Partnerize; they are identical. The conclusion is unchanged, but it now rests on a
verified rate-card comparison instead of an inference.

## The international argument does not hold either

The remaining case for CJ was non-US points of sale. It does not survive contact with the code:
Partnerize's Travel Creator Program is already global — its rate card footnote carves out car
commission for Hong Kong SAR, Singapore and South Korea, and `src/lib/expedia.ts` maps site
locales to Expedia checkout locales (`es_ES`, `fr_FR`, `ja_JP`) through `withExpediaLocale()`.
Non-US traffic is already monetised on the existing camref. The 15 other CIDs would duplicate
that too.

## Discrepancy worth reconciling (unrelated to this decision)

Two places state Expedia hotel commission as **~2.5%**, and use that figure to justify routing
decisions:

- `src/data/cj-advertisers.ts` header — "Each hotel brand pays 4-6% commission vs Expedia's
  2.5% — same booking flow for the user, 2× the payout for us."
- `docs/EXPEDIA_HOTEL_ID_REGISTRY.md` — chain traffic "worth 4-6% commission via
  `/api/go/cj/{advertiser}` … vs Expedia's ~2.5%".
- `src/lib/expedia.ts` also cites "~2.5% commission" for Expedia's things-to-do endpoint.

The verified rate card says hotel **4%** and activities **4%**. If 4% is correct, the chain-
routing rationale is "4-6% vs 4%", a far thinner margin than the stated 2×, and the copy in
those two files is overstating the case for CJ chain brands. The Viator/GetYourGuide activity
routing still wins on its own numbers (8%+ vs 4%), so only the stated figure is wrong there,
not the decision. `AFFILIATE_COMMISSION_MODEL.md` is declared source of truth and says to
update it first, then the code — worth settling which number is right before the next routing
change leans on 2.5%.

## Cruise remains an open gap

Both networks pay zero on cruise. `AFFILIATE_COMMISSION_MODEL.md` already notes a future cruise
vertical needs its own deal (CJ, Impact, or direct with Norwegian/Royal/Carnival). CJ Expedia US
does not help here — confirmed dead end, not an untested option.

## Compliance terms that bind us regardless

Worth reading once even though we are not activating the program, because PGAM buys paid media
and these are stricter than typical affiliate boilerplate:

- No bidding on Expedia trademarks or misspellings without written authorisation.
- "Expedia" may not appear in ad copy without prior approval.
- `www.expedia.com` may not be used as a display URL; `ourdomain.com/expedia` is permitted.
- **No direct linking to Expedia US from any paid or sponsored listing** — paid traffic must
  land on our own page first, then click through via the affiliate link.
- **No promotion via Twitter, Facebook, or Facebook advertising without written approval.**
- No toolbars, browser applications, or extensions without written approval.

Any paid-social push to destination.com pages carrying Expedia links needs a written waiver
first.

## What to do with the CJ account

Nothing. Leave the Expedia programs unactivated. The CJ account itself stays valuable and in
active use for the lanes where CJ genuinely pays more than Expedia Group does — the chain hotel
brands in `cj-advertisers.ts` still awaiting approval (Marriott, Hilton, IHG, Hyatt, all
`linkId: null`), and the gated credit-card catalogue, which is the highest-value affiliate
category on the site. Chasing those approvals is worth more than anything the Expedia CIDs
offer.
