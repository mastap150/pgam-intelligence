# Expedia via CJ — run it or not?

**Date:** 2026-08-19
**Trigger:** CJ acceptance email for *Expedia United States* (CID 1874913), plus 15 other
Expedia POS programs offered in the same mail.
**Existing relationship:** destination.com already monetises Expedia — recorded in
`training/00-company.md` as "Expedia Partnerize affiliate", and
`destination-redesign-mockups/DEVELOPMENT-SPEC.md` §3.2 builds on EAN deep links
(`buildExpediaUrl(type, params, affiliateId)` + `affcid` tracking).

## Decision

**Do not deploy CJ Expedia US links on destination.com. Keep the CJ account open and dormant.**

Rationale: destination.com already has one Expedia tracker wired into the page templates.
Adding a second tracker for the same point of sale buys no incremental commission — the same
bookings just land in whichever dashboard fired the last click — while splitting volume across
two reporting surfaces and weakening our position at rate-review time.

The CJ US terms are a default retail tier, not an improvement on a direct/API relationship:

| Term | CJ Expedia US (per acceptance email) |
|------|--------------------------------------|
| Cookie window | 7 days |
| Payment trigger | travel *consumed*, 48h after checkout date |
| Hotel | 4% |
| Activities | 4% |
| Vacation rental | 2% |
| Package | 2% — **but** the T&C section says Expedia pays *no* commission on packages or cruises |
| Car | 1.5% |
| Air | $0 |
| Cruise | 0% |

The package line contradicts itself inside the same email; treat package/cruise as zero until
an account manager confirms otherwise in writing.

## Where CJ is still worth having

**International points of sale.** The mail lists 16 separate CIDs (UK, DE/AT/CH, FR, IT, ES,
NL/BE, SE, NO, DK, FI, IE, CA, MX, BR, AR). Expedia's own guidance is that approval on one
program auto-approves the others, so the marginal cost of holding them is zero. If our
Partnerize/EAN integration is US-only and non-US traffic on destination.com is material, CJ is
the fastest way to monetise that traffic — and geo-routing means **no attribution collision**,
because the tracker is chosen by the reader's POS rather than by click order.

Also useful without placing a single link: the CJ Hotel Product Catalog and creative library.

## Open items before this is final

1. **Confirm which Expedia program destination.com is actually on.** Grep the live repo for the
   outbound link format — `prf.hn` means Partnerize, `anrdoezrs.net`/`dpbolvw.net`/`kqzyfj.com`/
   `jdoqocy.com` means CJ, a bare `expedia.com/...?affcid=` means a direct/EAN deep link:
   `rg -n 'prf\.hn|anrdoezrs|dpbolvw|kqzyfj|jdoqocy|affcid' ~/Desktop/destination-com`
2. **Get dual enrollment ruled on in writing** — email `expediagroup@cj.com` and the
   Partnerize/EPS account manager: is enrollment in the same POS through two channels permitted
   on one domain, and which POS does each channel cover? This is the deciding factor and must
   not be assumed.
3. **Pull the actual Partnerize/EAN rate card and cookie window** and compare line-by-line
   against the table above. Only reverse this decision if CJ wins on the numbers for a POS the
   direct deal does not cover.
4. **Measure non-US share of destination.com sessions.** That number alone decides whether to
   activate the 15 non-US CIDs.

## Compliance flags relevant to PGAM specifically

We are an ad company that buys paid media, so these CJ terms are not boilerplate for us:

- No bidding on Expedia trademarks or misspellings without written authorisation.
- "Expedia" may not appear in ad copy without prior approval.
- `www.expedia.com` may not be used as a display URL; `ourdomain.com/expedia` is allowed.
- **No direct linking to Expedia US from any paid or sponsored listing** — paid traffic must
  land on our own page first, then click through via the affiliate link.
- **No promotion via Twitter, Facebook, or Facebook advertising without written approval.**
- No toolbars, browser applications, or extensions without written approval.

Any paid-social push to destination.com content carrying Expedia links needs a written waiver
first. Affiliate disclosure is already mandated site-wide by DEVELOPMENT-SPEC §9.

## If we do activate the non-US CIDs

Route by POS, never run two trackers on the same page: US → existing Partnerize/EAN
`buildExpediaUrl()` path; non-US → CJ link for the matching CID. Air stays UX-only regardless —
$0 commission on CJ matches the existing guidance in `08_monetization_strategy.md` that flight
affiliate revenue is not worth chasing.
