# Call tracking — HomeBuyerForCash

Decisions and open questions for measuring phone enquiries alongside web
enquiries. Companion to [`homebuyerforcash-wiring-plan.md`](./homebuyerforcash-wiring-plan.md).

Status: **specified, nothing provisioned.** No numbers bought, no provider
account opened.

---

## 1. A call and a form fill are the same event

The pixel has three event types — page view, lead, purchase. There is no call
event, and no cost-per-call optimisation goal (`COST_PER_CALL` exists as a
*reporting* metric only, and no campaign goal can optimise toward it).

So a phone enquiry and a form enquiry both fire `lead`:

```js
attune('event', 'lead', { source: 'form' });   // enquiry confirmation page
attune('event', 'lead', { source: 'call' });   // tel: click — already automatic
```

Two consequences, both fine:

- The campaign optimises **cost per lead across both combined**. For a cash
  buyer that is defensible — a call and a form fill are worth about the same.
- **Counting calls as leads reaches the 0.1% conversion floor faster**, which is
  what gates the Leads campaign from being publishable at all. This helps.

**Open question — reporting split.** The reporting dimensions do not include a
`source` field, so `{source:'call'}` is not queryable there. Custom event slots
do exist in the metrics list (`number_of_custom_1/2/3`, `cost_per_custom_1/2/3`)
but the pixel documentation describes only the three types and never says how to
fire a custom event. **Ask the account manager whether custom events 1–3 can be
wired up.** Until then the call/form split comes from the call-tracking
platform's own reporting, matched by date. Do not promise the client a clean
in-dashboard split before this is answered.

---

## 2. Use our own numbers, not the call centre's

If the call centre supplies a number and we put it on the ads, they own the
data and we are reading their homework. Buy tracking numbers that **forward** to
the call centre instead. That gives us:

- the call log, timestamps, durations, caller ID and recordings, first-hand
- independent measurement rather than a supplied report
- the ability to change call centre without changing a number that is baked
  into a TV commercial

This is a strategic point, not a technical one, and it is worth holding even if
the call centre pushes back.

---

## 3. Two numbers, because TV and web are different problems

### TV creative → one dedicated static number

You cannot do dynamic number insertion on a television screen. Use a single
tracking number that appears **nowhere else** — not the website, not Google
Business, not a van livery. Then every call to it is provably TV-driven.

- No visitor IP, so these calls **cannot** be fed back as attributed
  conversions. That is a real limitation, not a configuration mistake.
- What it does give is a clean, honest count of calls the TV campaign caused,
  which is the number the client will actually care about.

### Website → dynamic number insertion

DNI swaps the displayed number per visitor and ties the call back to that web
session. The session is where the visitor's IP comes from, and the IP is the
only thing that lets a call be posted back as an attributed conversion.

Server-side event:

```
GET https://t.vibe.co/s2s-conversion/events
  ?aid=PIXEL_ID
  &a=lead
  &eid=UNIQUE_CALL_ID          # deduplication
  &ip=VISITOR_IP               # NOT our server's IP
  &ts=UNIX_MS
```

**The blocker, still unresolved.** S2S requires the *visitor's* IP; a call
webhook hands you a phone number. This only works if the DNI provider exposes
the originating web session's IP in its webhook payload. **Verify against the
provider's real payload before promising the client Vibe-attributed call
tracking** — do not assume it is in there.

Also worth knowing: S2S events appear in reports but do **not** populate
retargeting audiences.

---

## 4. Providers

| | DNI quality | Entry cost | Notes |
|---|---|---|---|
| CallRail | strongest | ~$50/mo | best-documented webhooks; check the IP field first |
| CallTrackingMetrics | comparable | similar | worth quoting against CallRail |
| Twilio | build it yourself | cheapest per number | you own the DNI layer, and its bugs |

---

## 5. Before switching recording on

Call-recording consent rules vary by state, and one-party vs two-party changes
what has to be announced at the start of a call. Confirm Oklahoma's position —
and any state the client expands into — rather than taking a default from
another market. This is a legal check, not a technical one; get it confirmed
rather than inferred.

---

## 6. What is not done

- No numbers purchased, no provider account opened
- DNI webhook IP availability unverified (§3) — the gating unknown
- Custom-event reporting split unanswered (§1)
- Recording consent position unconfirmed (§5)
