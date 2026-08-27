# Call tracking — HomeBuyerForCash

Decisions and open questions for measuring phone enquiries alongside web
enquiries. Companion to [`homebuyerforcash-wiring-plan.md`](./homebuyerforcash-wiring-plan.md).

Status: **specified, nothing provisioned.** The client's phone service is
**smrtPhone.io**. That does not have to change — the tracking layer sits in
front of it (§3). Whether smrtPhone can also *be* the tracking layer depends on
two unverified facts; CallRail in front of it is the fallback that works
regardless.

> **Revised, 2026-08-27.** An earlier version specified building the
> call→conversion postback by hand and listed "does the provider's webhook
> expose the visitor IP?" as the gating unknown. That is answered **for CallRail
> only**, which has a native integration. For any other provider the manual
> route and the open IP question both still apply — see §3.

---

## 1. Calls are measurable — but not optimisable

Once a call postback is working, the dashboard carries two metrics: **Calls**
(attributed call events) and **Cost per Call**. On the CallRail path these come
free with the integration; on any other path they only appear if the hand-built
postback in §3 works.

Either way this retires the workaround proposed earlier — no tagging
`{source:'call'}` and reconciling against the provider's reporting by hand, and
no need to ask about custom event slots.

What has *not* changed: **cost per call is a reporting metric, not an
optimisation goal.** The goal/metric matrix offers no campaign goal that
optimises toward it, so the campaign still optimises cost per lead or cost per
session. (`COST_PER_CALL` does appear in the API's optimisation-goal enum while
being absent from the documented matrix — worth one question to the account
manager, but plan on it being unavailable.)

**Open question that matters.** Do integration-sourced calls also register as
`lead` events, or only as the separate Calls metric? This decides whether calls
count toward the **0.1% conversion floor** that gates the Leads campaign from
publishing at all. If they do, calls accelerate the launch of the Leads
campaign; if they don't, the floor has to be cleared on form fills alone. Ask
before promising a Leads launch date.

---

## 2. Who owns the number matters

The client owns their smrtPhone account, so numbers issued there are already
theirs — that is fine, and better than a number a call centre hands them.

The principle still holds if a call centre is ever inserted: never advertise a
number the call centre owns. Whoever owns the number owns the call log, and a
number baked into a TV commercial is expensive to change. Numbers should belong
to the client (or to us on their behalf), and forward to whoever answers.

---

## 3. Working with smrtPhone

The client uses [smrtPhone.io](https://www.smrtphone.io/) — a phone system and
power dialer built for real-estate investors, with Podio/CRM sync, call flows
and a mobile app.

**The key point: their phone service and the tracking layer are different
things, and tracking sits in front.** A tracking number receives the call, then
forwards it to their smrtPhone number. Their dialer, CRM sync, recordings, call
flows and team workflow all continue unchanged. Nothing about how they answer
the phone has to move.

So the question is not "can we replace smrtPhone" — we should not — but "who
provides the number that rings first".

smrtPhone does advertise **Dynamic Number Insertion** and **webhooks**, so it
may be able to do both jobs itself. Two facts decide it, and **neither could be
verified** — their documentation hosts are unreachable from our environment, so
this must be asked, not assumed.

### The question that decides it

DNI comes in two kinds, and both get marketed as "call tracking":

| | What it does | Gives us a visitor IP? |
|---|---|---|
| **Source-level** | one fixed number per campaign or source | **No** |
| **Visitor-level** | a pool, one number per visitor session | **Yes** |

Only visitor-level identifies an individual visitor, and only an individual
visitor has an IP to match a household against. smrtPhone's public description
("a campaign-specific unique number based on the lead source") reads as
source-level, but that is a marketing page, not a spec — do not conclude from it.

### Four questions for smrtPhone

1. Is your DNI **visitor-level — a number pool assigning a distinct number per
   website visitor session** — or source-level, one fixed number per campaign?
2. Does the tracking script record the **website visitor's IP address** against
   that session?
3. Can a webhook fire on call completion carrying **that visitor IP**? Not the
   caller's carrier IP, not your server's.
4. Does the webhook include a **stable unique call ID** for deduplication?

Question 1 is the one that decides everything and the one most likely to be
answered loosely. A "yes, we do DNI" is not an answer to it.

### Two tiers of outcome

**Tier 1 — works today, needs nothing new.** Give the CTV campaign its own
dedicated number in smrtPhone, used nowhere else. Every call to it is provably
TV-driven and shows in their existing reporting. No IP, no integration, no new
vendor, no cost. This answers "is the TV advertising making my phone ring",
which is the client's actual question.

**Tier 2 — attributed calls in our dashboard.** Requires visitor-level DNI plus
the visitor IP in a webhook. Then calls post back and populate Calls / Cost per
Call. Two routes:

- **smrtPhone answers yes to all four** → we build the postback against the S2S
  endpoint below. One system, no extra vendor.
- **smrtPhone answers no to any** → **CallRail Website Pool numbers on the
  website, forwarding to their smrtPhone number.** Native integration, IP
  confirmed, zero build on our side, ~$50/mo. They keep smrtPhone for everything
  they do today; CallRail only owns the number that rings first.

```
GET https://t.vibe.co/s2s-conversion/events
  ?aid=PIXEL_ID &a=lead &eid=UNIQUE_CALL_ID &ip=VISITOR_IP &ts=UNIX_MS
```

The IP must be the end user's, not a server's. A provider that relays through
its own backend without forwarding the original visitor IP fails (2) and (3)
even though it technically has webhooks.

S2S events do **not** populate retargeting audiences, on any route.

### If we forward

Check how the forwarded call presents caller ID to their team — some setups show
the tracking number rather than the original caller, which breaks callbacks and
CRM matching. Configure it to pass the original caller through, and verify on a
live test call before the client's team relies on it.

---

## 4. Two numbers, because TV and web are different problems

| | Number type | Attributed? | Where it shows |
|---|---|---|---|
| Website | DNI pool (CallRail Website Pool, or equivalent) | **Yes** — carries visitor IP | Calls / Cost per Call in the dashboard, and the provider |
| TV creative | one dedicated static number | **No** — no IP exists | the provider's reporting only |

You cannot do dynamic number insertion on a television screen, so the TV number
is necessarily static and necessarily unattributed. That is a property of the
medium, not a misconfiguration.

Use a TV number that appears **nowhere else** — not the website, not Google
Business, not a van livery. Then every call to it is provably TV-driven, and
the provider's own reporting gives a clean count even though the platform dashboard
will never show it. For a client whose main question is "is the TV advertising
making my phone ring", that count is the answer, and it does not depend on IP
matching at all.

---

## 5. Cost

Tier 1 costs nothing new — a number they already pay for.

For Tier 2 via CallRail, entry is roughly $50/month plus per-number and
per-minute usage, with a free trial. DNI pools consume several numbers at once
(that is how the swapping works), so check how pool size affects the bill before
sizing it — true of any DNI provider, smrtPhone included.

---

## 6. Before switching recording on

Call-recording consent rules vary by state, and one-party vs two-party changes
what has to be announced at the start of a call. Confirm Oklahoma's position —
and any state the client expands into — rather than inheriting a default from
another market. This is a legal check; get it confirmed rather than inferred.

---

## 7. What is not done

- **The four smrtPhone questions in §3 are unanswered** — their doc hosts are
  unreachable from here, so this needs a human to ask. Question 1
  (visitor-level vs source-level DNI) decides whether Tier 2 is possible at all
- No numbers dedicated, no DNI pool, no integration started (also blocked on
  the advertiser record)
- Caller-ID passthrough on forwarded calls untested
- **Whether integration-sourced calls count toward the Leads 0.1% floor** (§1)
  — the one open item that could move a launch date
- Recording consent position unconfirmed (§6)
