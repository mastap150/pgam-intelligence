# destination.com newsletter sponsorship outreach — review

**Reviewed:** 2026-08-25
**Source:** `Campaign1_Booking_Hospitality`, `Campaign2_Transport_Experiences`,
`Campaign3_Gear_Fintech_DMO` — 5-email Instantly sequences (15 emails total)
**Reviewer note:** review + full rewrite. The originals are competent and safe;
they are also interchangeable and unfalsifiable, which is the problem.

---

## Verdict

The mechanics are right — spacing, stop-on-reply, plain text, breakup email,
referral ask in email 3. The *content* has one flaw that dominates everything
else: **across five emails and roughly 900 words, there is not one number.**
No list size, no open rate, no price, no geography, no named past sponsor. A
media buyer's first two questions are "how big is it?" and "what does it cost?"
and the sequence answers neither — it asks for a call to find out what could be
said in two lines.

Compare the house style in `docs/outreach/radial_entertainment_demand_partner_email.md`:
specific vendors, specific verticals, specific timelines. These three sequences
are the opposite of that.

Expect ~1–2% reply on these as written. The fixes below are worth more than
another round of wordsmithing.

---

## Priority fixes

### 1. Put real numbers in, or don't send yet

Every claim in the current copy is an adjective: "engaged", "higher-intent",
"strong fit", "stronger engagement rates". Adjectives are free, so buyers
discount them to zero.

One honest number beats vague scale, even if the number is small — it
self-qualifies the prospect and it's checkable:

> 6,400 subscribers, 43% open rate, 78% US, ~$450 per issue.

A brand that finds 6,400 too small self-selects out in email 1 instead of
costing you four more touches. A brand that likes the intent profile replies
*because* the number was stated plainly.

**Blocker:** `08_monetization_strategy.md` treats newsletter sponsorship as a
planned line with pricing that starts at 10,000 subscribers, and there is no
newsletter platform, list size, or open-rate data anywhere in this repo. Get
the actual figures before this campaign sends. If the list is genuinely
pre-10k, sell it as founding-sponsor inventory at a founding-sponsor price
(§6) rather than implying scale you don't have.

### 2. Cut the claims you can't back

Three specific liabilities:

| Line | Problem |
|---|---|
| "past sponsor results" (C1 email 2) | If there are no past sponsors, this is a promise that breaks the moment someone asks for the media kit. If there are, name them — that's the strongest sentence available and it's being withheld. |
| "a lot of brands in your space are shifting some of their paid media budget toward newsletter placements" (C1 email 3) | Unattributed industry hearsay. Either cite something real or replace with a question. |
| "Q4/Q1 sponsorship slots… before spots fill up" (C1 email 4), "before slots fill up" (C2/C3 email 4) | Manufactured scarcity on a calendar that is, per the repo, not yet booked. Media buyers pattern-match this instantly and it costs more credibility than the urgency buys. |

Replace the scarcity with a real constraint: one sponsor per issue, or a
fixed number of Q4 issues, or the rate going up at a stated list size. Those
are true and they create the same pressure.

### 3. Add a CAN-SPAM opt-out and postal address to all 15 emails

None of the fifteen have either. Cold commercial email to 2,500 contacts needs
a functioning opt-out mechanism and a valid physical postal address in *every*
message — "stop-on-reply: ON" is a sequencing rule, not an unsubscribe. Also
likely EU/UK contacts in the airline, tour-operator and DMO lists, where
GDPR/PECR want a documented legitimate-interest basis and a real opt-out.

One-line footer, plain text, doesn't hurt reply rate:

```
Don't want to hear from me? Reply "stop" and I'll take you off the list.
PGAM Media, [street address], Coral Gables, FL [zip]
```

### 4. The three campaigns are the same campaign

Diff campaigns 2 and 3: they're the same email with the noun swapped. The
segmentation exists in the filenames and not in the copy — which forfeits the
entire point of splitting a 2,500-contact list. What each segment actually
cares about:

- **Booking / accommodation / hospitality** — heads in beds, tracked bookings,
  attributable revenue per issue. They already buy metasearch and want to know
  how this compares.
- **Transport & experiences** — filling specific dated departures and
  low-season capacity. Timing and seasonality is the whole pitch, not intent.
- **Gear / retail** — product placement, review coverage, and affiliate
  economics. They think in ROAS and will happily do a hybrid flat-fee + CPA.
- **Fintech / insurance** — CPA and compliance. Nobody is buying a flat-fee
  brand placement here; they want a conversion path and pre-approved claim
  language.
- **DMOs** — annual budgets, RFP cycles, agency of record, co-op programs.
  **Most DMOs literally cannot buy off a cold email** — it goes to procurement.
  Grouping them with luggage brands in one sequence is the biggest segmentation
  error in the set.

Recommendation: split campaign 3 into three (gear, fintech/insurance, DMO), and
give DMOs a different motion entirely — a co-op media proposal to the agency
of record, timed to their fiscal planning cycle, not a 5-touch cold sequence.

### 5. De-escalate the ask across the sequence

All five emails ask for the same 15-minute call. Email 2 offers a media kit but
gates it behind "just let me know", and email 5 gates it again behind "reply
anytime and I'll get it right over" — which is asking for a reply in order to
give them a reason to reply. Link the media kit. It costs nothing and it's the
only asset in the sequence a prospect actually wants.

Better ladder: call → linked media kit + price → one answerable question →
yes/no with a booking link → close with the asset attached.

### 6. Nothing here reverses the risk

Nobody buys a first placement on an unproven newsletter at rate card. Pick one
and put it in email 4:

- **Founding sponsor rate** — 40–50% off, locked for two issues, honest about
  why (early list).
- **Performance floor** — guaranteed opens or clicks, next issue free if missed.
- **Hybrid flat + CPA** — credible for gear, fintech, insurance, and cheap to
  operate: destination.com already runs Expedia Partnerize affiliate plumbing,
  so tracked-conversion deals are a short walk, not a build.

This is the single highest-leverage addition to the sequence.

### 7. Line-level notes

- **Email 1 subjects** ("newsletter sponsorship for {{companyName}}?",
  "sponsorship idea for…", "partnership idea for…") state the *sender's* goal
  and read as templated. Lead with their world instead: "6,400 trip-planners a
  week — worth a test for {{companyName}}?"
- **"(not just browsing)" / "not just dreaming"** — defensive; arguing against
  an objection the reader hasn't raised. Cut both.
- **"I think there could be a strong fit"** appears in all three email 1s, and
  the suggested `{{personalization}}` example in the setup notes ends with "I
  think there's a strong fit here" — so the personalization line and the body
  say the same hedged thing back to back. Fix in all three docs.
- **"re:" subjects** (email 2, all three) on a thread the recipient never
  replied to. Mild deception pattern, weighted by some filters, and noticed by
  the exact buyers you want. Let the follow-up thread properly instead.
- **"Wishing you a great rest of the year"** (C1 email 5) and hard-coded
  "Q4/Q1" (C1 email 4) break if the sequence re-runs in Q1–Q3. C2/C3 say
  "upcoming" — make it consistent and season-agnostic.
- **Dead merge fields.** The setup notes list `{{lastName}}`, `{{phone}}` and
  `{{website}}` as "merge fields used"; none appear in any of the fifteen
  emails. Trim the list.
- **"Media kit" is named 5 times across the set and linked zero times.** No
  calendar link either. Add both.
- **Newsletter names are inconsistent across the docs** — C1 cites *The Weekly
  Deal Drop* + *The Stay Edit*, C2 cites *Deal Drop* + *The City Edit*, C3
  cites "*The Stay Edit*, and others". Verify all three exist, are publicly
  findable under those names, and are cited consistently. A prospect who
  googles a newsletter and finds nothing is gone.
- **No reply-bait variant.** A question-only email with no pitch
  ("Are you buying any newsletter placements in 2026, or is it all paid social
  right now?") routinely out-replies the pitch and belongs in the sequence.

### 8. Deliverability, given 2,500+ contacts

- Send from a **secondary sending domain**, not root `destination.com` — a cold
  campaign at this size will take reputation damage and the root domain is
  needed for the newsletter itself.
- SPF, DKIM and DMARC on the sending domain before the first send.
- 2–4 warmed mailboxes, **20–30 sends/mailbox/day**, ramped over two weeks.
- Plain text, and **no link in email 1** — links in a first cold touch are the
  main spam signal here. Media-kit link goes in email 2 onward.
- Verify the list (bounce rate above ~3% starts hurting) and suppress role
  addresses (`info@`, `marketing@`) into a separate low-priority sequence.

---

## Pre-flight checklist

- [ ] Actual subscriber count, open rate, click rate, geo split
- [ ] Rate card, even provisional, that can go in writing in email 4
- [ ] Media kit built and hosted at a stable link
- [ ] Newsletter names verified and publicly findable
- [ ] "Past sponsor results" either substantiated with names or cut
- [ ] Opt-out line + postal address in all templates
- [ ] Sending domain, SPF/DKIM/DMARC, mailbox warmup
- [ ] DMO contacts pulled out of campaign 3 into their own motion
- [ ] Risk-reversal offer chosen (founding rate / performance floor / hybrid CPA)

---

# Rewritten sequences

Bracketed `[ALL-CAPS]` values are facts to fill before sending. Every email
ends with the opt-out footer; it's shown once below rather than repeated.

```
—
Don't want to hear from me? Reply "stop" and I'll take you off the list.
PGAM Media, [STREET ADDRESS], Coral Gables, FL [ZIP]
```

---

## Campaign 1 — Booking, accommodation & hospitality

### Email 1 — Day 1 (no links)

**Subject:** [SUBS] trip-planners a week — worth a test for {{companyName}}?

> Hi {{firstName}},
>
> I run sponsorships for Destination.com's travel newsletters — *The Weekly
> Deal Drop* and *The Stay Edit*, [SUBS] subscribers, [OPEN]% open rate,
> [GEO]% US.
>
> {{personalization}}
>
> Our readers open these while they're picking where to stay, which is a
> narrower window than most travel media sells. One sponsor per issue, native
> placement written by our editors.
>
> Want the numbers and pricing? Reply "send" and I'll get them over — no call
> needed.
>
> Best, {{accountSignature}}

*Why:* number in the first line, one-word ask, no link, no call demanded.

### Email 2 — Day 4

**Subject:** what a Stay Edit placement looks like

> Hi {{firstName}},
>
> Following up with the specifics rather than making you ask.
>
> Media kit: [MEDIA KIT LINK]
>
> Short version — [SUBS] subscribers, [OPEN]% opens, [CTR]% click rate,
> [GEO]% US, one sponsor per issue at [PRICE]/issue. Placement is a written
> editorial feature, not a banner, and we can point it at a specific property,
> region or rate.
>
> If bookings are what you're measuring, we can pass a tracked link and report
> clicks and conversions per issue.
>
> Worth a look for {{companyName}}?
>
> Best, {{accountSignature}}

### Email 3 — Day 8 (reply bait — no pitch)

**Subject:** quick question

> Hi {{firstName}},
>
> Genuinely curious, and it saves us both time: is {{companyName}} buying any
> newsletter or editorial placements this year, or is the budget going to
> metasearch and paid social?
>
> If it's the latter, I'll stop — no pitch. If newsletter is on the table at
> all, I'll show you what a single test issue would look like.
>
> And if this sits with someone else on your team, a name is all I need.
>
> Best, {{accountSignature}}

### Email 4 — Day 13 (the offer)

**Subject:** founding sponsor rate for {{companyName}}

> Hi {{firstName}},
>
> Concrete offer, since a first placement on a newsletter you don't know is a
> hard sell:
>
> [PRICE] for one issue instead of [RATE CARD], as a founding sponsor. If it
> doesn't clear [X] opens, the next issue is on us.
>
> Two [MONTH] issues left uncommitted; one sponsor each. Book directly here if
> it's easier than a call: [BOOKING LINK]
>
> Best, {{accountSignature}}

### Email 5 — Day 19 (breakup)

**Subject:** closing this out

> Hi {{firstName}},
>
> Last note from me. Media kit's here if it's useful later:
> [MEDIA KIT LINK]
>
> If newsletter sponsorship comes up for {{companyName}} down the line, reply
> to this and I'll pick it back up. Otherwise I'll leave you be.
>
> Best, {{accountSignature}}

---

## Campaign 2 — Transport & experiences

Angle: **capacity and dated departures**, not intent. These buyers have
perishable inventory.

### Email 1 — Day 1 (no links)

**Subject:** filling [SEASON] departures for {{companyName}}?

> Hi {{firstName}},
>
> I run sponsorships for Destination.com's travel newsletters — [SUBS]
> subscribers, [OPEN]% open rate, and readers open them while they're locking
> in tours, transport and activities for a trip they've already booked.
>
> {{personalization}}
>
> That timing is the reason I'm writing: if {{companyName}} has departures or
> routes that need filling in [SEASON], a single issue can put a specific date
> and a specific offer in front of [SUBS] people who are actively building an
> itinerary.
>
> Want the numbers and pricing? Reply "send" — no call needed.
>
> Best, {{accountSignature}}

### Email 2 — Day 4

**Subject:** pricing and reach for a single-issue test

> Hi {{firstName}},
>
> The specifics, so you don't have to ask:
>
> Media kit: [MEDIA KIT LINK]
>
> [SUBS] subscribers, [OPEN]% opens, [CTR]% click rate, [GEO]% US, one sponsor
> per issue at [PRICE]. We write the placement editorially and can build it
> around one departure, route or destination rather than the brand generally.
>
> Tracked links included, so you'd see clicks and bookings attributable to the
> issue.
>
> Best, {{accountSignature}}

### Email 3 — Day 8 (reply bait)

**Subject:** quick question

> Hi {{firstName}},
>
> Straight question: how is {{companyName}} filling soft departures right now —
> OTA promos, paid social, email?
>
> Asking because newsletter placement competes with exactly that budget, and if
> it's not a channel you'd consider I'd rather stop here than send another
> three emails.
>
> If someone else owns this, just point me at them.
>
> Best, {{accountSignature}}

### Email 4 — Day 13 (the offer)

**Subject:** one test issue, [PRICE]

> Hi {{firstName}},
>
> Making it easy to say yes: [PRICE] for one issue as a founding sponsor
> ([RATE CARD] after), built around whichever departure or route you most need
> to move. If it doesn't clear [X] clicks, the next one's free.
>
> [MONTH] has [N] open issues, one sponsor each: [BOOKING LINK]
>
> Best, {{accountSignature}}

### Email 5 — Day 19 (breakup)

**Subject:** closing this out

> Hi {{firstName}},
>
> Last note. Kit's here whenever it's useful: [MEDIA KIT LINK]
>
> If a season comes up where {{companyName}} needs demand fast, reply to this
> and we'll put something together.
>
> Best, {{accountSignature}}

---

## Campaign 3a — Travel gear & retail

Angle: **ROAS and product coverage.** Offer a hybrid so the flat fee isn't the
whole risk.

### Email 1 — Day 1 (no links)

**Subject:** gear coverage in front of [SUBS] trip-planners

> Hi {{firstName}},
>
> I run sponsorships for Destination.com's travel newsletters — [SUBS]
> subscribers, [OPEN]% open rate, readers actively packing for trips they've
> booked.
>
> {{personalization}}
>
> Two ways brands like {{companyName}} use it: a sponsored placement in an
> issue, or product coverage in a gear roundup with tracked links. Happy to
> quote either, or a hybrid of a small flat fee plus affiliate.
>
> Want reach and pricing? Reply "send".
>
> Best, {{accountSignature}}

### Email 2 — Day 4

**Subject:** flat, affiliate, or both

> Hi {{firstName}},
>
> Options, with numbers:
>
> Media kit: [MEDIA KIT LINK]
>
> - **Sponsored placement:** [PRICE]/issue, one sponsor, editorial copy.
> - **Gear roundup inclusion:** [PRICE], evergreen on-site plus one newsletter
>   send.
> - **Hybrid:** [LOW PRICE] flat plus [X]% affiliate — you carry less of the
>   risk, we're paid on performance.
>
> [SUBS] subscribers, [OPEN]% opens, [CTR]% clicks, [GEO]% US.
>
> Best, {{accountSignature}}

### Email 3 — Day 8 (reply bait)

**Subject:** quick question

> Hi {{firstName}},
>
> What ROAS does a placement need to hit for {{companyName}} to keep buying it?
>
> Asking because it tells me whether to quote you a flat rate or a hybrid — and
> if the number is high, I'd rather say so than sell you a brand placement that
> won't clear it.
>
> Best, {{accountSignature}}

### Email 4 — Day 13 (the offer)

**Subject:** hybrid test — [LOW PRICE] plus affiliate

> Hi {{firstName}},
>
> Lowest-risk version I can offer: [LOW PRICE] flat plus [X]% on tracked
> sales, one issue. If it doesn't perform you're out [LOW PRICE]; if it does,
> we both want the second one.
>
> Send product or a link and we can be live in the [DATE] issue:
> [BOOKING LINK]
>
> Best, {{accountSignature}}

### Email 5 — Day 19 (breakup)

**Subject:** closing this out

> Hi {{firstName}},
>
> Last note — kit and rates here if it's useful later: [MEDIA KIT LINK]
>
> If {{companyName}} runs a launch or a seasonal push where editorial coverage
> would help, reply anytime.
>
> Best, {{accountSignature}}

---

## Campaign 3b — Fintech & insurance

Angle: **CPA and compliance.** These buyers do not buy flat-fee brand
placements from unproven publishers, and their legal team owns the copy.

### Email 1 — Day 1 (no links)

**Subject:** CPA placement in front of [SUBS] trip-planners

> Hi {{firstName}},
>
> I run sponsorships for Destination.com's travel newsletters — [SUBS]
> subscribers, [OPEN]% open rate, readers reading us while they're booking and
> prepping trips.
>
> {{personalization}}
>
> I'd guess a flat-fee brand placement isn't how {{companyName}} buys, so:
> we'll run it on a CPA or hybrid basis, with tracked links, and we'll take
> your approved copy and disclosures as-is rather than writing claims for you.
>
> Want reach numbers and a proposed CPA? Reply "send".
>
> Best, {{accountSignature}}

### Email 2 — Day 4

**Subject:** proposed terms and reach

> Hi {{firstName}},
>
> Specifics:
>
> Media kit: [MEDIA KIT LINK]
>
> [SUBS] subscribers, [OPEN]% opens, [CTR]% clicks, [GEO]% US, [X]% booking a
> trip in the next 90 days. Proposed: [CPA] per [QUALIFIED ACTION], your
> creative and disclosure language, our tracking or yours.
>
> We can run this through an existing affiliate network if that's simpler for
> your compliance review.
>
> Best, {{accountSignature}}

### Email 3 — Day 8 (reply bait)

**Subject:** quick question

> Hi {{firstName}},
>
> Does {{companyName}} work with publishers directly, or does everything go
> through a network?
>
> Tells me who to talk to and saves you a procurement conversation you didn't
> ask for.
>
> Best, {{accountSignature}}

### Email 4 — Day 13 (the offer)

**Subject:** performance-only test, no fee

> Hi {{firstName}},
>
> Simplest version: no placement fee, [CPA] on [QUALIFIED ACTION], one issue as
> a test. You risk nothing but the review cycle.
>
> Send an existing offer and creative and we can slot it into the [DATE] issue:
> [BOOKING LINK]
>
> Best, {{accountSignature}}

### Email 5 — Day 19 (breakup)

**Subject:** closing this out

> Hi {{firstName}},
>
> Last note. Kit and audience detail here: [MEDIA KIT LINK]
>
> If {{companyName}} adds publisher partnerships to the mix later, reply and
> I'll pick this up.
>
> Best, {{accountSignature}}

---

## Campaign 3c — DMOs (different motion — do not cold-sequence)

Most DMOs cannot buy from a cold email: budgets are annual, spend goes through
an agency of record, and anything above a low threshold hits procurement. A
5-touch sequence pushing a 15-minute call mostly generates silence.

**What to do instead:**

1. **Identify the agency of record** before contacting the DMO. Pitch the
   agency's media planner, not the tourism board's marketing coordinator.
2. **Time it to the fiscal cycle** — get on the plan while next year's budget
   is being built, not when you happen to have an open issue.
3. **Lead with a co-op**, not a solo placement: a destination feature plus
   newsletter send plus [X] partner listings, so hotels and operators in the
   region share the cost. Co-op is a format DMOs already have a line item for.
4. **Two touches, not five** — a proposal and one follow-up. If it's a fit,
   they'll route you internally; if not, more emails don't change the budget
   cycle.

### Email 1 — the proposal

**Subject:** co-op destination feature — [DESTINATION] in [SEASON]

> Hi {{firstName}},
>
> Destination.com reaches [SUBS] newsletter subscribers and [SESSIONS] monthly
> readers, [GEO]% US, planning trips in the next 90 days.
>
> {{personalization}}
>
> I'd like to put a [DESTINATION] co-op package in front of whoever handles
> your media plan: an editorial destination feature, a dedicated newsletter
> send, and [X] partner slots your local hotels and operators can buy into —
> so the board covers a fraction of the total.
>
> One-pager with reach, deliverables and cost: [PROPOSAL LINK]
>
> If this sits with your agency, I'm glad to take it to them directly — just
> point me at the right name.
>
> Best, {{accountSignature}}

### Email 2 — one follow-up, ~10 days later

**Subject:** re: [DESTINATION] co-op — timing question

> Hi {{firstName}},
>
> One question rather than another pitch: when does [DMO] build its media plan
> for [NEXT PERIOD]?
>
> If it's already set, I'd rather come back at the right point in the cycle
> than push something that has nowhere to go. And if partner co-op sits with
> your agency, a name is all I need.
>
> Proposal is here in the meantime: [PROPOSAL LINK]
>
> Best, {{accountSignature}}

---

## Instantly setup

Unchanged from the originals except where noted.

- **Spacing:** Day 1 → 4 → 8 → 13 → 19. Fine as is.
- **Stop on reply:** ON.
- **Plain text**, no HTML. **No link in email 1** for any sequence.
- **Merge fields actually used:** `{{firstName}}`, `{{companyName}}`,
  `{{personalization}}`, `{{accountSignature}}`. Drop `{{lastName}}`,
  `{{phone}}` and `{{website}}` from the spec — they appear in no template.
- **`{{personalization}}`** — one sentence, specific enough that it could only
  have been written to this company. Not "given {{companyName}}'s focus on
  boutique hotel experiences" (that's a category, and it collides with the
  hedge in the body). Something closer to: "Saw you opened the [PROPERTY] in
  [CITY] this spring — that's exactly the kind of stay our readers ask about."
- **Sending:** secondary domain, SPF/DKIM/DMARC, 2–4 warmed mailboxes,
  20–30/mailbox/day, ramped over two weeks. Suppress role addresses into a
  lower-priority list.
- **Subject-line A/B for email 1:** test the number-led subject against a
  question-led one. That's the highest-variance element in the sequence.
- **Segments:** five, not three — booking/hospitality, transport/experiences,
  gear, fintech/insurance, DMO (separate motion).
