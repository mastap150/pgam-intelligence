# SMB deals — margin floors and sales commission

Companion to `docs/dsp/PGAM_DSP_RATE_CARD.md`. Covers the $3–5K buy band, which
the rate card does not price, and which runs on **Vibe** — not the
SpringServe/ClearLine stack the rate card assumes.

Not yet approved — Priyesh is sole approver on P&L-affecting terms
(`training/00-company.md`).

## 1. Vibe changes the economics, in our favour

Two facts, both confirmed against the platform rather than assumed.

**There is no Vibe fee to model.** Vibe's billing rules: *"No platform,
subscription, seat, or joining fees. Budget set = amount charged."* Whatever
Vibe's own take is, it is invisible to us and inside the CPM we clear. So our
media cost is exactly the strategy budget we set, and:

```
Vibe strategy budget = advertiser gross × (1 − margin)
```

**Our achieved CPM is $12.50.** Hatford Funding, Jun 1 – Aug 31 2026:
$4,257.48 spend, 340,703 impressions, **$12.50 CPM**, 98.1% completion rate,
$0.0625 cost per household. That is on automatic bidding.

> Caveat: n=1. One advertiser, one quarter. Treat $12.50 as indicative and
> re-check as the book grows — every margin number below moves with it.

A $12.50 base CPM is the whole story. Managed CTV sells at $25–45. We can quote
a market-competitive ~$20 CPM and still hold 40% margin.

**And the servicing load collapses.** The rate card's 18.5h estimate was built
on SpringServe: demand-tag wiring, the Wizard→SS field-mapping drops that force
manual QA (`02-dsp-playbook.md`), ClearLine setup. None of that exists on Vibe.
Creative approval is an automatic ACR scan, **~30 minutes, not days**. Campaign
build and reporting are both reachable over the Vibe API.

| Task | Hours |
|---|---|
| Asset intake (form, not a kickoff call) | 0.5 |
| Campaign + strategy build in Vibe | 1.0 |
| Creative upload + ACR approval | 0.5 |
| Launch + pacing check | 0.25 |
| Optimisation touches (0.5h/wk) | 2.0 |
| Automated monthly report + review, invoicing | 0.75 |
| **Total** | **~5.0 → ~$270 at $60/h** |

Down from ~$1,100. **This is what makes SMB viable at all** — the earlier case
for a high margin was cost recovery against a load that Vibe removes.

## 2. Margin: 40% target, 33% floor

At a $12.50 clearing CPM, a $5,000 gross buy:

| Margin | Vibe budget | Impressions | Advertiser's effective CPM | Our net | Contribution* |
|---|---|---|---|---|---|
| 30% | $3,500 | 280,000 | $17.85 | $1,500 | $980 (19.6%) |
| **40%** | **$3,000** | **240,000** | **$20.83** | **$2,000** | **$1,430 (28.6%)** |
| 45% | $2,750 | 220,000 | $22.72 | $2,250 | $1,655 (33.1%) |

<sub>*after ~$270 servicing, 10% commission on net, and ~$100 amortised Meta CAC.</sub>

**Target 40%.** Stretch to 45% where the advertiser is CPM-insensitive — most
SMB buyers think in monthly budget, not CPM, and have no CTV reference point.
**Floor 33%**, below which Priyesh approves.

| Advertiser spend / month | Target | Ryan can close at |
|---|---|---|
| $2K – $5K | 40% (stretch 45%) | 33% |
| $5K – $10K | 35% | 30% |
| $10K – $25K | 28% | 22% |
| $25K+ / agency | 15–20% | 12% |

**Minimum drops to $2,000/month.** At $2,000 gross and 40%, net is $800 against
~$270 servicing — thin but real. Below $2,000 the calendar cost wins; take it
only self-serve with no managed touch.

**Say the honest part out loud:** at 40% the advertiser pays $20.83 against the
$12.50 they would pay signing up to Vibe themselves. Vibe *is* public
self-serve, so the markup is discoverable. Our defence is the managed service,
attention scoring and attribution — not opacity. Existing practice already
applies (`00-company.md`: gross CPM never surfaces to the buying platform).
The moment an advertiser asks which platform we buy on, or wants a media-plan
breakdown, they have graduated to the $10K+ tier — switch that account to a
transparent *media at cost + management fee* model rather than defending a
blended CPM.

**CPM only.** Do not sell CPA-call below ~$10K/month: at a $100 CPA a $5K
budget is 50 calls, we bear the media, and variance at that volume can flip a
priced-for-profit flight into a loss.

## 3. Budgeting a $5,000 spend — the mechanics

1. **Vibe strategy budget is $3,000, never $5,000.** The one failure mode that
   destroys the deal is loading the advertiser's stated budget into Vibe. $5,000
   is our top line; media sits below it.
2. **Hold a pacing buffer.** Set the strategy at ~95% of allocated media
   ($2,850) and keep $150 in reserve — it absorbs overdelivery and funds a
   top-up if pacing lags late in the flight.
3. **Collect from the advertiser upfront, card on file, before launch.** Vibe
   invoices us *continuously* — every $500 of balance or every 30 days,
   whichever comes first. Billing in arrears means we float the media on every
   account at once.
4. **Pre-fund the Vibe balance by ACH top-up; never run the book off card
   charges.** Vibe supports **one primary and one backup card per account, shared
   across all advertisers**, and a failed payment **pauses every active
   campaign**. One card problem takes down the whole SMB book simultaneously.
   Hold roughly a month of aggregate book media as balance, with a floor alert.
5. **Automatic bidding.** Manual bidding needs ≥$18 CPM to deliver at all, which
   would eat the margin outright; the $12.50 was achieved on automatic.
6. **Reconcile on Vibe's reported spend, not the budget set.** Margin = gross
   collected − Vibe actual spend. If Vibe underdelivers, extend the flight —
   the rate card settles make-goods in impressions, so don't pocket it.
7. **Onboarding compresses to 2–3 days**, not the rate card's 8, because ACR
   approval is ~30 minutes. Sell that.
8. **Harvest the referral credit** — $500 for a referred advertiser's first $500
   of spend, which is pure margin. Ask every account that renews.

## 4. Ryan's commission

### Define "net" first

Ryan's agency deal is "15% of net". Two readings, one solvent:

- **Net = gross billings − media cost** (our margin dollars). $50K agency flight
  at 10% margin → net $5,000, Ryan gets $750.
- **Net = billings less the agency's 15%** (traditional media usage) → net
  $42,500, Ryan gets $6,375 against our entire $5,000 margin.

The second is underwater on day one, so the first is what's meant. Put it in his
agreement explicitly:

> **Net** = gross billings collected − media cost (the Vibe strategy spend) −
> third-party data fees. Not gross billings.

Paying on net is the discipline: every dollar he discounts comes out of his own
cheque, so he cannot buy a close with our margin.

### 7% on Meta leads? No — keep 10%

The Meta CAC is a real cost and a fair thing to want covered. But the arithmetic
does not support a rate cut:

| | 10% | 7% |
|---|---|---|
| Per $5,000 deal (net $2,000) | $200 | $140 |
| **Difference** | | **$60/deal** |
| Over a 6-month account life | $1,200 | $840 |

Against an account returning ~$12,000 net over six months, of which Meta CAC
(~$600) and servicing (~$1,620) already take $2,220: cutting to 7% improves
contribution by **$360 across the account's entire life** — roughly 3% of net on
an account that returns over 70%. Immaterial to us.

What it costs is not immaterial. At 7% against 15% on agency, SMB pays less than
half per dollar, and he will quietly deprioritise the exact book he was brought
in to build — a book that today is one advertiser. He is also already exposed to
our pricing: paid on net, his cheque falls with every margin decision we make.

**The 60-day churn clawback is the right tool for Meta-lead risk**, not a rate
cut. It stops him burning paid leads on bad-fit advertisers to bank a
commission. A blanket cut punishes good closes and bad closes identically.

If you want more protection than the clawback gives, make the rate conditional
rather than lower: **a close-rate gate** — if his close rate on PGAM-supplied
Meta leads falls below ~10% over a quarter, the SMB rate steps to 7% for the
following quarter. Lead waste becomes his problem; performance isn't punished.

### Recommended rates

| Deal type | Rate | Basis |
|---|---|---|
| Agency (unchanged) | 15% of net | as today |
| **SMB, PGAM-sourced Meta lead** | **10% of net** | recurring, while the account spends |
| **SMB, Ryan-sourced lead** | **15% of net** | recurring, while the account spends |
| SMB closed below the tier floor | 7% of net | that deal only |
| Converted to unmanaged self-serve | 5% of net | residual |

Terms: paid monthly **on collected cash**; recurring while the account spends;
**60-day churn clawback**; no commission on make-goods; pricing authority to the
floor column; **13% accelerator** in any month the SMB book clears $25K net; no
cap. Plus the close-rate gate above if you want it.

### Does it pay him enough

At $5K accounts and 40% margin (net $2,000/account/month):

| Active accounts | Gross/mo | Net/mo | Ryan @ 10% | Annualised |
|---|---|---|---|---|
| 10 | $50K | $20K | $2,000/mo | $24K |
| 15 | $75K | $30K | $3,000/mo | $36K |
| 30 | $150K | $60K | $6,000/mo (13% accel: $7,800) | $72K–$93.6K |

Show him the per-hour: $200 for a two-call close beats $750 for an agency deal
that takes a quarter.

## 5. Open questions for Priyesh

1. **Does Ryan run the Vibe campaigns himself?** If so the ~$270 servicing folds
   into his comp and 12–13% is the fair rate. If AdOps runs them, 10%.
2. **How much Vibe balance do we pre-fund, and who watches the floor?** A failed
   payment pauses every client's campaign at once. This needs an owner and an
   alert, not a card.
3. **Is the Meta funnel producing?** 10% assumes leads arrive. If Ryan ends up
   prospecting them, it is 15% work.
4. **What is our actual Meta CAC per closed account?** The $600 above is an
   assumption. It is the one input that would change the commission answer.
5. **Rate card v1.1** should add the Vibe SMB tier: $2,000 minimum, CPM-only,
   2–3 day onboarding, and SKU 1's `<TBD min spend>` / `<TBD weeks>` filled.
