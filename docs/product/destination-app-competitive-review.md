# Destination app — competitive review and product decisions

_2026-08-25. Sources: the Wanderlust products (both of them), our own
destination.com codebase, and the 25 Aug QA pass. Implementation landed
in `mastap150/destination-app` on `claude/app-product-polish-2873hq`;
the finding-by-finding QA trace lives in that repo at
`docs/qa-response-2026-08-25.md`._

---

## 1. Who we were actually looking at

"Wanderlust" is two different products, and they're worth separating
because they pull in opposite directions.

**Wanderlust (wanderlustapp.io)** — "Trip Planner You Live & Share". A
planner grounded in city, cost and safety data. You give it a sentence
("10 days in Japan, mostly food, one hike, under $3k") and it drafts an
itinerary with travel times, the reservations you should make now, and
mornings deliberately left unplanned. The itinerary pins to one map;
**Optimize Day** reroutes a day's stops for least transit, anchors the
hotel, flight and timed reservations in place, and **previews how many
minutes you'd save before you commit**. It then runs the trip live and
offline.

**Wanderlust: Travel Inspiration (App Store)** — a much smaller idea,
executed cleanly. A personality quiz with no typing, suggestions
grouped by category, heart-to-save into lists, a Saved Trips section.
"Like travelling with a close friend who knows you well — and also
happens to be a local."

Ours sits between them: the second app's onboarding, the first app's
ambitions, plus an editorial moat neither has.

Sources: [wanderlustapp.io](https://www.wanderlustapp.io/) ·
[itinerary features](https://www.wanderlustapp.io/itinerary-features) ·
[App Store listing](https://apps.apple.com/us/app/wanderlust-travel-inspiration/id6746957492)

---

## 2. The uncomfortable comparison

| | Wanderlust | destination (before) | destination (now) |
|---|---|---|---|
| **Discovery** | Personality quiz → categorised suggestions | Quiz existed; drove one eyebrow line | Quiz drives mood, ranking, planner defaults, itinerary pacing |
| **Search** | Sentence → structured brief | Sentence → scattered fields; a fake search bar that discarded typing | Sentence → visible, auditable brief; a real search that carries the query |
| **Curation** | Cost + safety data | Genuine editorial moat, mostly linked off-app | Same moat, with real empty/error states so it stops looking broken |
| **Itinerary** | Map-pinned, Optimize Day with a previewed gain | Day-by-day, map, swap-a-slot, reroll | Unchanged — see §4 |
| **Flights** | Anchored in the plan | A "best months" card needing an optional field, no search | Real search on planner, deals, and saved trip |
| **Monetization** | Subscription | Affiliate, four inconsistent treatments, one leg unmonetized | One primitive, category-correct routing, flights monetized |
| **Reliability** | — | Could not complete its core job | See the QA response |

The row that matters is the last one. We were not losing on features.

---

## 3. What we took, and why

Not features — the reasoning behind them.

### 3.1 A brief you can audit (from: the sentence → itinerary flow)

Wanderlust's strongest move is that a one-line brief becomes a
**visible, structured plan-of-a-plan** before anything is generated.
Our chat bar already parsed a sentence — and then scattered the result
into form controls with a single confirmation line, so the user never
saw the whole brief, and never saw which parts came from *them*.

Our version adds something theirs doesn't: **provenance per field**.
Each chip says whether it came from your sentence, your taste quiz, or
our default, and tapping one jumps to the control that changes it.

That addition is a direct consequence of the QA pass. The report noted
that onboarding answers never reached the planner and called it out as
*"the entire reason for asking the questions"*. A silent pre-fill would
have technically closed that finding while leaving the real problem
intact: **a default the user can't see is indistinguishable from a
default that ignored them.** Copying the feature would have fixed the
mechanism and missed the point.

### 3.2 "What to book now" (from: reservations you should make now)

A finished itinerary is not a finished task. The gap between "I have a
plan" and "I have bookings" is where planning tools lose people, and
Wanderlust closes it by naming what's worth reserving now.

We adapted rather than copied, because we can't back the same claims.
Wanderlust leans on live inventory; we have a plan, a lead time and
route seasonality. So `BookNowBlock` orders flights → stay → timed
tickets and **explains the ordering from what we actually know** —
"you're 20 days out, fares this close to departure are usually at their
highest" — with no countdowns, no scarcity badges, no invented urgency.

The editorial side of this brand spent years earning the right to be
believed about travel. A fake urgency badge spends that faster than it
earns.

It is also, not coincidentally, the honest place for booking links: the
one moment where a booking CTA is a service rather than an
interruption.

### 3.3 Honest price context (from: grounded in real data)

Wanderlust's positioning is that its plans come from real city, cost
and safety data. We have a stronger version of that asset — the indices
and the 101-route dataset — and were barely using it in the app.

The flight card now states seasonality and lead time as **general
patterns, explicitly not predictions** about the user's fare. It is
less impressive than a live price and it is defensible, which given
where our brand equity comes from is the correct trade.

---

## 4. What we deliberately did not take

**Optimize Day.** The best single feature in Wanderlust's planner: it
reroutes a day for least transit, keeps anchored items fixed, and
previews the minutes saved before committing. The preview is the clever
part — it makes an opaque algorithm auditable, the same instinct as our
brief.

We have the data for it (`itineraryV2` carries geocoded coordinates)
and did not build it in this round. Two reasons. It is a substantial
feature that deserves its own design pass rather than being appended to
a QA-response change. And more importantly, the app's day-level
itinerary had to become **reliably reachable** before optimising it was
worth anything — a route optimiser behind a planner that returns
"Failed to fetch" is a rounding error.

Recommended as the next round's headline feature. Ordering: nearest-
neighbour over each day's coordinates with hotel/flight/timed items
pinned, presented as "saves ~40 min" with an explicit Apply.

**Their subscription model.** We're affiliate-funded, which is a real
strategic difference, not an oversight. It's also why removing the
unconfirmed Pro pricing from the UI cost us nothing.

**Their social/sharing loop.** We already have public chat archives and
share links. Nothing in their execution beat ours.

---

## 5. What our own website was doing better than our app

Reviewing destination.com alongside the competitors was the more
productive half of this exercise. The web team had already solved
problems the app was still failing at.

**Flights.** The web has a full flight product — Travelpayouts cache,
a Duffel supply lane, fare-brand and baggage disclosure, a flexible-
dates calendar, price alerts. The app had a "best months to book" card
that only appeared once the user filled an **optional** field, and
offered no search. So the majority of users never saw a flight surface
on the screen where they were planning a flight.

**The `/api/go/flights` bouncer** is the piece most worth having taken.
Book clicks route through it so the server can pick the partner A/B arm
and, on the Duffel arm, mint a Links session — which can only be
created server-side and expires in 24h. Building the partner URL
client-side would have silently opted the app out of the experiment the
web is running *and* out of the booking lane that captures our own
margin instead of 1.6%×70% of someone else's. The app now inherits both
for free.

**The commission model.** `docs/AFFILIATE_COMMISSION_MODEL.md` records
that Expedia's Travel Creator programme pays **0% on standalone
flights** (verified 2026-08-05), which is why flights route to
Aviasales while hotels (4%) and activities (4%) stay on Expedia. The
app had no equivalent rule and four different visual treatments for
"this link earns us money". One `AffiliateCta` primitive now carries
the routing, the disclosure, and per-surface attribution — so revenue
can be split by placement, and a click we can't attribute to a surface
is a click we can't learn from.

---

## 6. Follow-ups

**1. Confirm the deployed API base URL.** The QA report's §3 could not
be closed from the code, and still can't. The app now reports its own
API origin and reachability under Account → Diagnostics, and the deploy
script refuses to ship a non-production base and smoke-tests it first.
Somebody still has to look.

**2. CORS on `/api/flight-calendar`** in `destination-com`. It returns
cheapest-price-per-day for a month and would make the flight card's
date fields genuinely smart, but it doesn't send CORS headers, so the
app's web build can't call it. Four lines; deliberately not made in
this change to keep it to one repository.

**3. Optimize Day**, per §4.

**4. Watch the affiliate split by placement.** Every CTA now carries a
per-surface `sub_id`. First read worth taking at the next audit: which
surface converts — the planner's booking block, the deals tab, or the
saved trip. That number decides where the next round of work goes, and
until now we had no way to ask.
