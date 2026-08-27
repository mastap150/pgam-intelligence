# HomeBuyerForCash — campaign wiring plan

**Status: nothing live, nothing sent.** The existing draft campaign is
unchanged and unpublished. This is the spec for what to build once the blockers
below clear.

Date: 2026-08-26 · Market: OKC metro · Objective: calls + website visits

---

## 1. "Calls and website visits" is two campaigns, not one

There is no cost-per-call optimisation goal. `COST_PER_CALL` exists as a
*reporting* metric, but no campaign goal can optimise toward it. A call has to
be recorded as a **Lead** event and bought through a Leads campaign.

That leaves two objectives that cannot share one campaign:

| Client wants | Goal | Optimises | Can launch day one? |
|---|---|---|---|
| Website visits | Traffic | cost per session | **Yes** — needs Page View in last 12h |
| Calls | Leads | cost per lead | **No** — needs Lead events for 7 days + ≥0.1% conversion rate |

**A Leads campaign is not launchable at launch.** The 0.1% floor is
`leads ÷ page views`, and on day one there are no leads because the tag has
never run. Anyone promising a lead-optimised CTV campaign from a standing start
is going to miss the date.

### Sequence

1. **Now** — tag live on the site, page views flowing.
2. **+12h** — publish the **Traffic** campaign, optimising cost per session.
   Website visits are the stated objective; this delivers them immediately.
3. **Throughout** — Lead events accumulate from `tel:` clicks and the enquiry
   confirmation page. No campaign change needed; the tag does this from day one.
4. **+7 days, once ≥0.1% CVR** — publish a second **Leads** campaign against
   the same geo, optimising cost per lead. Split budget with Traffic.
5. **Day 14+** — first real read. Not before; see §5.

Goal is immutable after publish, so this has to be two campaigns from the
outset. Do not plan to "switch" the Traffic campaign to Leads later.

---

## 2. The current draft has to be rebuilt

Draft `4ef7a674-1e41-480c-838f-dcd0b802f035` is **Awareness / cost per unique
household**. Changing the goal on a draft deletes every strategy on it, so this
is a rebuild, not an edit. One tool call once the inputs are in.

Two consequences of moving Awareness → Traffic, both worth knowing:

- **Frequency capping disappears.** It exists only on Awareness, ABM and App
  Promotion. The 3/day cap in the draft cannot carry over; Traffic paces
  algorithmically.
- **Attribution window becomes configurable** (it isn't on Awareness). Default
  30 days, which is also the longest. Keep it — see §5.

### Correction to the A/B structure I built

The draft has two strategies — a hard homeowner filter, and a soft
distress-signal read — over the same four counties. **That is a structural
defect and should not ship.** The two audiences overlap heavily: a homeowner
interested in financial services sits in both. Overlapping strategies bid
against each other and inflate our own CPMs, split the budget so neither gets
the volume the bidder needs to learn, and make attribution between them
unreadable.

**Launch with one strategy.** Same geo, homeowner as the spine, distress
signals as suggestions, one budget, all the learning signal in one place. Split
into a genuine test later, with mutually exclusive audiences and equal budgets,
once there is enough conversion volume for a comparison to mean anything.

### Target spec

| Field | Value |
|---|---|
| Goal | Traffic |
| Optimisation | Cost per session — **value must come from the client** (§4) |
| Attribution window | 30 days |
| Geo | Oklahoma `40109`, Cleveland `40027`, Canadian `40017`, Pottawatomie `40125` |
| Strategy count | **1** |
| Budget | Daily. $100/day ≈ $3,000/mo matches the proposal |
| Bidding | Automatic (the only option on Traffic) |
| Interests | Homeowner + financial services, real estate, estate planning, family law, home & property insurance, roofing — as **suggestions** |
| Age | 45–54, 55–64, 65+ as suggestions |
| Demographics | Income and net-worth lower bands — **max 2 groups**, so income + net worth, no language |
| Delivery | Any time any day |
| Creative | None in the account yet — blocker |

Tulsa (`40143`) is in the current draft and is **not** in their stated
footprint. Drop it unless they confirm otherwise.

---

## 3. Measurement wiring

Full detail in [`integrations/attune-tag/`](../integrations/attune-tag/). The
short version:

- The client installs one PGAM-hosted file, `tag.pgammedia.com/attune.js`, and
  calls `attune('event','lead')`. The vendor pixel is wrapped underneath and
  never appears in their codebase or install docs.
- **Never proxy the beacon server-side.** Attribution is IP-based and matches
  the household. Relaying through a PGAM origin makes the tracker see our
  server IP and every conversion goes unattributed. Client-side wrapping is
  safe; server-side proxying destroys the product.
- The wrapper is not a cloak — the vendor host is visible in DevTools. It
  removes the vendor from the install, not from inspection.

**Calls** are specified separately in
[`homebuyerforcash-call-tracking.md`](./homebuyerforcash-call-tracking.md).
Short version: the provider is **not yet chosen** — waiting on the client.
CallRail is the recommended fallback because it is the only call-tracking
platform with a native integration; any other provider means building the
postback ourselves and re-opening the question of whether it exposes the web
visitor's IP. Either way, attribution needs dynamic number insertion — a static
TV number carries no IP and is measured in the provider's reporting only. We buy
our own numbers that forward to the call centre rather than using theirs. The
open item is whether attributed calls count toward the Leads 0.1% conversion
floor, which could move the Leads launch date.

---

## 4. Blockers — none of these are ours to decide

1. **A HomeBuyerForCash advertiser must be created in the dashboard.** There is
   no create-advertiser API. This is now hard-blocking, not cosmetic: the pixel
   belongs to the advertiser, so without their own advertiser their conversions
   land on PGAM SS's pixel (`DgRNCK`) and mix with our own traffic.
2. **`tag.pgammedia.com` is not deployed.** The file exists; the origin does
   not. See the hosting notes in the integration README.
3. **Tag installed and 12h of live traffic** before Traffic can publish.
4. **A creative.** No video exists on the account for this advertiser. 15s or
   30s, 16:9, 1080p, MP4.
5. **The cost-per-session target.** Platform rules are explicit that this comes
   from the advertiser and must not be pre-filled by us — not from a default,
   an industry average, or another client's campaign. Ask what a website
   enquiry is worth to them, or what they pay per visit elsewhere, and derive
   it from their answer.
6. **Payment method** on the account.

---

## 5. Optimisation, once live

**Do not touch it for 14 days.** New campaigns carry a 14-day learning phase,
and pausing or duplicating resets a further 5. Layered on top, a 30-day
attribution window means today's conversions reflect the last month of
impressions — cut the budget and performance looks unchanged for days, raise it
and short-term cost per session looks worse. Both are false signals. Wait at
least half the attribution window before drawing any conclusion.

Run two windows in parallel and never confuse them:

- **Operational (1 day)** — tactical reads. A channel with zero sessions on a
  1-day window is a real signal.
- **Reporting (30 days)** — what we show the client, and what the campaign is
  actually judged on.

Three traps specific to this account:

- **Sample size beats gap size.** A channel at 8% conversion on 15 conversions
  against a 4% baseline is noise. Roughly: 20 conversions need ~2.5× baseline
  to mean anything, 100 need ~1.75×, 500 need ~1.4×.
- **Never cut a channel on last-touch cost alone.** Broad-reach channels open
  the path and get no credit for it; cutting them can collapse the performance
  of whatever was closing.
- **Budget goes where the *next* conversion is cheapest**, not where the
  average is best. Equal marginal cost across strategies is the target, which
  only becomes a live question once there are two campaigns running.

Expect our reported numbers to sit below the client's Google Analytics. GA uses
last-click; this is IP-based view-through. It is a modelling difference, and
worth saying out loud to the client before they notice it themselves.
