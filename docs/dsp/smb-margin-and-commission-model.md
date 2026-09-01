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

> **Caveat, and it matters more than it looks: n=1, and it is an outlier.**
> One advertiser, one quarter. Vibe's own reference bounds call an Awareness
> CPM target **below $15 "probably too low"** (too high above $40) — so $12.50
> sits *beneath* the floor of what the platform considers a realistic ask. It
> may reflect Hatford's vertical, geo, season, and no Live Sports rather than
> what we will clear generally. **Every margin number below moves with this
> figure**, so §2's rule is written to price off the clearing CPM rather than
> off a fixed percentage.

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

## 2. Margin: price to an effective CPM, don't fix a percentage

At a $12.50 clearing CPM, a $5,000 gross buy:

| Margin | Vibe budget | Impressions | Advertiser's effective CPM | Our net | Contribution* |
|---|---|---|---|---|---|
| 30% | $3,500 | 280,000 | $17.85 | $1,500 | $980 (19.6%) |
| **40%** | **$3,000** | **240,000** | **$20.83** | **$2,000** | **$1,430 (28.6%)** |
| 45% | $2,750 | 220,000 | $22.72 | $2,250 | $1,655 (33.1%) |

<sub>*after ~$270 servicing, 10% commission on net, and ~$100 amortised Meta CAC.</sub>

**A fixed 40% is the wrong instrument.** It only works while we clear $12.50,
and that figure is one advertiser sitting below Vibe's own realistic floor. If
typical clearing turns out to be $18, a 40% margin puts the advertiser at a
$33 effective CPM — close to Vibe's "too high" bound of $40, a price that is
easy to beat and hard to defend.

**The rule instead: price to a $22–25 effective CPM to the advertiser, cap
margin at 45%, floor at 30%.** Margin then falls out of what we actually
clear, and self-corrects if CPM rises:

| We clear | Margin for a $25 effective CPM | Vibe budget on $5K | Impressions |
|---|---|---|---|
| $12.50 | 50% → **capped at 45%** | $2,750 | 220,000 |
| $15.00 | 40% | $3,000 | 200,000 |
| $18.00 | 28% | $3,600 | 200,000 |
| $20.00 | 20% | $4,000 | 200,000 |
| $22.00 | 12% → below floor, decline or reprice | $4,400 | 200,000 |

At today's $12.50 that lands us at the 45% cap — so **40–45% is right now**,
which is where the first draft landed, but for a reason that survives the CPM
moving. $22–25 is comfortably inside Vibe's own $15–40 Awareness band and
normal for managed CTV, so it is a price we can defend out loud.

| Advertiser spend / month | Target | Ryan can close at |
|---|---|---|
| $2K – $5K | 40% (stretch 45%) | 33% |
| $5K – $10K | 35% | 30% |
| $10K – $25K | 28% | 22% |
| $25K+ / agency | 15–20% | 12% |

**Minimum $2,000/month, and a 3-month minimum term** (see §3 — a one-month
flight is half learning phase). At $2,000 gross and 40%, net is $800 against
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

## 3. Does the margin hurt performance?

Mostly no — but the reasons are specific, and the real threat to an SMB
campaign is not the margin at all.

**Efficiency: barely affected.** The optimisation playbook's own logic is
diminishing returns — "the first dollar is most efficient, each additional
dollar slightly less so." The dollars margin removes are therefore the *least*
efficient ones. The advertiser's cost per outcome stays roughly intact; they get
**fewer outcomes, not worse ones**. At $12.50 clearing, a $5K buy delivers
~48,000 households at 40% margin against ~60,000 at 25% — a scale difference of
about a fifth, not a quality difference.

**Optimiser starvation: real, and it depends on the campaign goal.** The
playbook is explicit that "the bidding algorithm requires data volume to learn,"
and its sample-size guide says an observation needs ~100 conversions before it is
even actionable. That splits SMB in two:

- **Awareness / ABM** — the signal is impressions and households, which are
  plentiful at a $2,750–3,000 media budget. Hold the full margin here.
- **Leads / Sales / Traffic** — the signal is conversions. At $3,000 media and a
  $50 cost per lead that is ~60 leads a month: below where the bidder can
  optimise and below where our own reporting can separate signal from noise.
  **Take less margin on performance goals, or require a larger budget** — this
  is the one case where 40% genuinely buys a worse campaign.

**The 14-day learning phase is the actual problem, and margin has nothing to do
with it.** New campaigns carry a **14-day learning phase** during which
performance is expected to fluctuate; pausing/resuming or duplicating with
changes resets a further **5-day** phase. A one-month $5,000 flight spends
roughly half its life learning, then gets judged on the result.

Two consequences, both worth more than ten points of margin:

1. **Sell a 3-month minimum term, not one-month flights.** Otherwise every SMB
   advertiser churns on a false read of a campaign that never left its learning
   phase — and we lose an LTV worth many times the margin difference.
2. **Never pause/resume to manage pacing.** It resets a 5-day learning phase.
   Adjust budget instead.

**Also independent of margin:** with the 30-day default attribution window, the
playbook says prioritise reach over frequency — a newly reached household opens a
fresh window, while re-exposing one already in an open window adds almost
nothing. Hatford ran ~5 exposures per household across the quarter, which is
reasonable spacing, but the Insights tab's *Reached vs. Recalled* chart is the
thing to watch: the closer Recalled tracks Reached, the more efficient the
frequency strategy. There is performance available there without giving up a
point of margin.

## 4. Budgeting a $5,000 spend — the mechanics

1. **Vibe strategy budget is $3,250, never $5,000.** The one failure mode that
   destroys the deal is loading the advertiser's stated budget into Vibe. $5,000
   is our top line; media sits below it at 65% (§2).
2. **The margin is the make-good reserve — no separate pacing buffer.** Vibe
   budget is a cap, not a target, so it cannot overspend and holding a slice
   back buys nothing. What can happen is a higher-than-expected clearing CPM
   leaving us short of the impressions on the IO. At $3,250 media against the
   30% floor there is **$250 of top-up headroom** before the floor breaks.
   Needing more than that repeatedly is a pricing problem, not a topping-up
   problem — reprice at renewal.
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

### Three numbers, three different places — do not mix them

The commonest way to get this wrong is to try to express margin as a CPM inside
Vibe. Margin is a **budget** decision, not a bid decision.

| Number | Where it lives | Who sets it |
|---|---|---|
| **Price** — the $22–25 effective CPM | our IO / invoice to the advertiser | us, per §2 |
| **Margin** — the gross-to-media split | the Vibe strategy **budget** | us, per §2 |
| **Performance target** — cost per household, CPM, CPL | the Vibe **optimisation goal** | **the advertiser** |

That third row is not optional. The optimisation playbook is explicit that its
reference bounds are *"reference only. Never quote these numbers to the
advertiser, and never use them to propose or pre-fill a value… The value always
comes from the advertiser."* Our margin has no business in the optimisation goal.

### Use automatic bidding — it is worth 22 points of margin

Manual bidding needs a **≥$18 CPM bid to deliver at all**. Automatic bidding is
what produced the $12.50. On the same $25 sell price:

| Bidding mode | Clearing CPM | Media for 200,000 imps | Our margin |
|---|---|---|---|
| **Automatic** (achieved) | $12.50 | $2,500 | **50%** |
| Manual (at the $18 floor) | $18.00 | $3,600 | 28% |

Setting a manual CPM to "control" margin destroys it. Set the budget, pick the
advertiser's optimisation goal, leave bidding automatic.

### The build sequence for a $5,000/month buy

1. Advertiser commits $5,000/month, **3-month minimum term** (§3).
2. Choose the sell price: $22–25 effective CPM → we owe 200,000–227,000
   impressions a month.
3. Media budget = those impressions × our actual clearing CPM. Open at **65% of
   gross ($3,250)** — see below — not at the 55% today's CPM would allow.
4. Set the Vibe **strategy budget** to the full $3,250 — **per month**, not per
   term. Budget is a cap, so there is nothing to hold back.
5. Set the **optimisation goal** from the advertiser's own number. Automatic
   bidding.
6. Reconcile on Vibe's reported spend at month end; move the split on renewal,
   not mid-flight.

**Open at 65% media / 35% margin, not 55/45.** Today's clearing would permit 45%,
but that rests on one advertiser below Vibe's own realistic floor. 65% survives
clearing at $15–18 without repricing, and it is far better to overdeliver in
month 1 — which sits inside the 14-day learning phase and is where churn
happens — than to underdeliver and argue about it. If month 1 clears near
$12.50 you will have overdelivered against the IO: that is the renewal
conversation, and the renewal is where you move to 55/45.

**If the $5,000 is the whole contract rather than monthly**, do not stretch it
across three months — $1,000/month of media is ~$33/day and too thin to deliver
or optimise. Run it as a **6-week burst** instead (the awareness playbook's Burst
profile), and set expectations as a burst, not an always-on campaign.

**The one that will actually bite: the budget is monthly.** A 3-month term at
$5,000/month is $15,000 gross and **$9,750** of media. Setting $3,250 once for
the whole term funds a third of what was sold. Either set a monthly-recurring
strategy budget or re-set it every month, and put it on the calendar.

**What to watch, and when.** The number that says whether it is working is
delivered impressions against what the IO promised (200,000–227,000/month at a
$22–25 sell price). Check it at the **three-week mark**, not weekly — inside the
14-day learning phase and with attribution lag, a weekly read is noise (§3).

## 5. Ryan's commission

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

## 6. Open questions for Priyesh

1. **Does Ryan run the Vibe campaigns himself?** If so the ~$270 servicing folds
   into his comp and 12–13% is the fair rate. If AdOps runs them, 10%.
2. **How much Vibe balance do we pre-fund, and who watches the floor?** A failed
   payment pauses every client's campaign at once. This needs an owner and an
   alert, not a card.
3. **Is the Meta funnel producing?** 10% assumes leads arrive. If Ryan ends up
   prospecting them, it is 15% work.
4. **What is our actual Meta CAC per closed account?** The $600 above is an
   assumption. It is the one input that would change the commission answer.
5. **Will we hold a 3-month minimum term?** §3 says a one-month flight is half
   learning phase. This is the single biggest lever on SMB retention, and it is
   a sales-posture decision, not a pricing one.
6. **Rate card v1.1** should add the Vibe SMB tier: $2,000 minimum, 3-month
   term, CPM-only, 2–3 day onboarding, margin priced to a $22–25 effective CPM,
   and SKU 1's `<TBD min spend>` / `<TBD weeks>` filled.
