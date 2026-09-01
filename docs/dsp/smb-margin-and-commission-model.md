# SMB deals — margin floors and sales commission

Companion to `docs/dsp/PGAM_DSP_RATE_CARD.md`. Covers the $3–5K buy band, which
the rate card does not price. Two decisions live here: what margin an SMB deal
has to carry, and what Ryan gets paid to close and hold the book.

Not yet approved — Priyesh is sole approver on P&L-affecting terms
(`training/00-company.md`).

## 1. Why 10% is the wrong number here

The DSP takes a **10% platform fee** on direct/agency campaigns. That number
was set against Amazon-Business-sized flights, where 10% of gross is a real
sum and servicing is amortised over a big buy.

Servicing cost barely moves with deal size. The rate card promises, on every
campaign: creative QA and trafficking, soft-launch validation, pacing touches,
a **weekly written recap**, monthly reporting, and a QBR. Delivered as written
on a one-month flight that is roughly:

| Task | Hours |
|---|---|
| Kickoff + asset/brand-safety collection | 2.0 |
| Wizard build + manual SS QA (known field-mapping drops, `02-dsp-playbook.md`) | 3.0 |
| Soft launch delivery/attribution validation | 1.0 |
| Pacing + optimisation touches (2h/wk) | 8.0 |
| Weekly written recaps (0.75h × 4) | 3.0 |
| Monthly report + invoicing/admin | 1.5 |
| **Total** | **~18.5** |

At a fully-loaded ops rate of ~$60/h that is **~$1,100 — 28% of a $4,000
buy, before a cent of media margin.** A 10% fee on that deal is $400. The
deal loses money.

**So the fix is two-part, and the margin half is the smaller half:** raise the
margin *and* strip the product so the cost base actually falls. A templated
SMB flight — no kickoff call, no QBR, dashboard in place of a written recap,
automated monthly summary — lands nearer 8 hours (~$480). Every number below
assumes the stripped SKU. Sold as the full managed service, no margin fixes it.

## 2. Margin floors

Target **40% net margin** in the $3–5K band, gliding down to the existing 10%
platform fee at enterprise size.

| Advertiser spend / month | Target margin | Ryan can close at | Below floor |
|---|---|---|---|
| $3K – $5K | 40% | 33% | Priyesh approval |
| $5K – $10K | 35% | 30% | Priyesh approval |
| $10K – $25K | 28% | 22% | Priyesh approval |
| $25K+ / agency | 15–20% | 12% | existing 10% platform fee |

The glide path is the argument to make to an advertiser who benchmarks us: the
premium is cost recovery on a small buy, and it falls as they grow.

What a $4,000 buy actually returns:

| | at 40% | at 25% | at 15% |
|---|---|---|---|
| Advertiser pays (gross) | $4,000 | $4,000 | $4,000 |
| Media cost | $2,400 | $3,000 | $3,400 |
| **Net (gross profit)** | **$1,600** | **$1,000** | **$600** |
| Templated servicing (~8h @ $60) | ($480) | ($480) | ($480) |
| Sales commission (10% of net) | ($160) | ($100) | ($60) |
| **Contribution** | **$960** | **$420** | **$60** |
| Contribution as % of gross | 24% | 11% | 1.5% |

At 15% the deal is free work. That is the whole case for 40%.

**Minimum deal size: $3,000 per flight.** At $2,000 gross, 40% margin is $800
net against ~$480 servicing — $320 contribution, not worth the calendar. Take
anything below $3K only with a **one-time $500 onboarding fee**, or as pure
self-serve with no managed touch at all.

**SMB is CPM only.** Do not sell SKU 2 (CPA-call) below ~$10K/month. At a $100
CPA a $4K budget is 30–50 calls; PGAM bears the media cost, and call-quality
variance across 30 calls is wide enough to turn a priced-for-profit flight into
a loss. Keep CPA for advertisers big enough for the law of large numbers.

Existing guardrails already work in our favour and should be pointed at this
band: the buyer agent's one-way CPM ratchet means setup CPM is a ceiling and
descending bids capture margin against real delivery, and `margin pause` halts
a campaign that drops below threshold. Set that threshold to the floor column
above, per tier.

## 3. Ryan's commission

### Define "net" before anything else

Ryan's agency deal is "15% of net". Two readings, and only one is solvent:

- **Net = gross billings − media cost** (i.e. our margin dollars). On a $50K
  agency flight at 10% margin: net $5,000, Ryan gets $750.
- **Net = billings less the agency's 15%** (the traditional media meaning). On
  the same flight: net $42,500, Ryan gets $6,375 — more than PGAM's entire
  $5,000 margin.

The second reading is underwater on day one, so the first is what's meant.
Write it into his agreement explicitly:

> **Net** = gross billings collected − media cost − third-party ad-serving and
> data fees. Not gross billings.

This matters more than the percentage. It is also the discipline: paying on net
means every dollar he discounts comes out of his own cheque.

### Recommended rates

| Deal type | Rate | Basis |
|---|---|---|
| Agency (unchanged) | 15% of net | as today |
| **SMB, PGAM-sourced lead** | **10% of net** | recurring, while the account spends |
| **SMB, Ryan-sourced lead** | **15% of net** | recurring, while the account spends |
| SMB closed below the tier floor | 7% of net | that deal only |
| Account converted to unmanaged self-serve | 5% of net | residual |

10% rather than 15% on inbound SMB because we supply the lead — the marketing
cost is already borne, the cycle is two calls not two quarters, and the deal is
templated with no RFP. The 15% agency rate pays for hunting, long cycles and
relationship risk. Where Ryan does his own hunting, he gets the hunting rate.

Not lower than 10%, because he also has to *hold* the book. Paid too little on
a $4K account, he ignores thirty of them to chase one whale.

### Terms

- **Paid monthly on collected cash**, not on booking. No collection, no
  commission.
- **Recurring while active.** He keeps earning as long as the account spends —
  that is what makes retention his problem too.
- **Clawback** on any account that churns inside 60 days of launch. Stops
  bad-fit closes.
- **No commission on make-goods.** The rate card settles make-goods in
  additional impressions, no cash — so they cut margin, and correctly cut his
  net with it.
- **Pricing authority** to the "can close at" column above. Below it, Priyesh
  approves and that deal pays 7%.
- **Accelerator:** in any calendar month where the SMB book clears $25K net,
  all SMB net that month pays **13%**.
- No cap.

### Does it pay him enough

| Active SMB accounts | Gross/mo @ $4K | Net @ 40% | Ryan @ 10% | Annualised |
|---|---|---|---|---|
| 10 | $40K | $16K | $1,600/mo | $19.2K |
| 15 | $60K | $24K | $2,400/mo | $28.8K |
| 30 | $120K | $48K | $4,800/mo | $57.6K |
| 40 | $160K | $64K | $6,400/mo (13% accel: $8,320) | $76.8K–$99.8K |

Per-hour it beats agency work at the bottom of the funnel: $160 for a deal that
closes in two calls, against $750 for one that takes a quarter. Worth showing
him that arithmetic — otherwise 15% vs 10% reads as a demotion.

## 4. Open questions for Priyesh

1. **Does Ryan do the AdOps, or does AdOps?** If Ryan runs his own campaigns
   the ~$480 servicing cost largely disappears into his comp, and 12–13% is the
   fair rate. If AdOps still services, 10%. This changes the number.
2. **Is the stripped SMB SKU approved?** The margin table is void if these
   deals are sold with weekly written recaps and QBRs attached.
3. **Volume floor on the inbound funnel.** 10% only holds if the leads are
   genuinely arriving; if Ryan ends up prospecting them, it is 15% work.
4. **Rate card v1.1** should add the SMB tier with the $3K minimum and the
   CPM-only constraint, filling the `<TBD min spend>` placeholders in SKU 1.
