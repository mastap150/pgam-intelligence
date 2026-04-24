# Publisher size-gap outreach — copy-paste ready

Estimates use account-wide observed RPM applied to that publisher's
request volume. Real realization typically lands at 25–40% of the
estimate (demand for the size may be saturated elsewhere). Shown here
as the estimate × 0.30 "realistic" figure for conversations.

---

## Modrinth (inventory_id 544, ~3.4B requests / 14d)

**Estimated realistic uplift: $30K–$45K / month** if top 5 slots added.

**Subject:** Missing ad sizes on Modrinth — $30-45K/mo in untapped demand

Hey [AM at Modrinth] — quick data pull from our backend. Modrinth
currently serves ads on our SSP via a subset of the IAB standard
sizes, but there's substantial unfilled demand for several sizes you
don't expose yet. Combined, the top 5 missing slots represent an
estimated **$30K–$45K/month** in recoverable revenue at current eCPM
rates:

| Missing size | Format | Account-wide RPM | Est. monthly uplift (realistic 30%) |
|---|---|---|---|
| 300x600 | Half-page | $5.49 | $12,000 |
| 320x480 | Mobile interstitial | $2.78 | $6,000 |
| 728x90 | Leaderboard | $1.96 | $4,300 |
| 300x50 | Mobile small | $1.91 | $4,200 |
| 970x90 | Super leaderboard | $1.79 | $3,900 |

This is incremental to existing inventory — these sizes don't
cannibalize current fill, they open net-new inventory. The win on our
side: we create the placements once your dev team adds the tags. We
handle demand-partner rollout.

**Next step:** would your team be open to a 15-min call to walk
through which placements fit your templates? Happy to send over
exact tag snippets when you're ready to deploy.

---

## OP.GG (inventory_id 64, ~3.4B requests / 14d)

**Estimated realistic uplift: $30K–$45K / month**

Nearly identical profile to Modrinth. Same pitch, same sizes. One
difference: OP.GG is more gaming/esports traffic, so CTV/video
expansion could also be worth a conversation (video RPM typically
3-5× display).

**Subject:** Expanding OP.GG inventory — $30-45K/mo size-gap opportunity

Hey [AM at OP.GG] — running yield analysis across our SSP and OP.GG
stands out as having several high-demand sizes we're not exposing.
Concrete numbers:

| Missing size | Format | Account-wide RPM | Est. monthly uplift (realistic 30%) |
|---|---|---|---|
| 300x600 | Half-page | $5.49 | $12,000 |
| 320x480 | Mobile interstitial | $2.78 | $6,000 |
| 970x90 | Super leaderboard | $1.79 | $3,900 |
| 320x100 | Mobile banner | $1.61 | $3,500 |
| 160x600 | Wide skyscraper | $1.25 | $2,700 |

All incremental. Would love to schedule 15 min to align on which of
these fit your templates.

---

## VerticalScope (inventory_id 1354, ~402M requests / 14d)

**Estimated realistic uplift: $1.5K–$2K / month** on 300x600 alone.

Smaller volume but clean single-size fix. Shorter pitch:

**Subject:** One missing size = $1.5K+/mo on VerticalScope

Hey — noticed VerticalScope is missing 300x600 in its active TB
placements. At current account RPM ($5.49) it looks like $1.5K–$2K/mo
untapped on that one slot. If your team can add the slot I'll wire
the demand side in a day.

---

## Pre-call checklist (internal)

Before each call, double-check the publisher isn't intentionally
excluding a size (e.g. brand guidelines, mobile-only strategy).
Pull the last 30 days' request breakdown for them via
`scripts/fast_partner_analysis.py` keyed to their publisher name.
