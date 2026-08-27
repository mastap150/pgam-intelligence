# Call tracking — HomeBuyerForCash

Decisions and open questions for measuring phone enquiries alongside web
enquiries. Companion to [`homebuyerforcash-wiring-plan.md`](./homebuyerforcash-wiring-plan.md).

Status: **specified, nothing provisioned, provider not chosen.** CallRail is
the recommended default and the fallback if the client has nothing of their own,
but the decision is open pending what they come back with. §3 covers both paths.

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

## 2. Use our own numbers, not the call centre's

If the call centre supplies a number and we put it on the ads, they own the data
and we are reading their homework. Buy tracking numbers that **forward** to the
call centre instead:

- the call log, timestamps, durations, caller ID and recordings, first-hand
- independent measurement rather than a supplied report
- freedom to change call centre without changing a number baked into a TV
  commercial

Strategic, not technical. Worth holding even if the call centre pushes back.

---

## 3. Choosing a provider

**The integration surface is not symmetric, and this should inform the choice.**
CallRail is the *only* call-tracking platform with a native integration — the
full integration catalogue covers CRM/audience sync, app-attribution partners
(AppsFlyer, Adjust, Singular, Airbridge, Kochava, Gamesight) and analytics
(GA4, Northbeam, Triple Whale), and CallRail is the sole call-tracking entry.

So the two paths cost very different amounts:

| | CallRail | Any other provider |
|---|---|---|
| Integration | native, 3 config steps | hand-built S2S postback |
| Visitor IP | **confirmed available** | **unverified — the open risk** |
| Calls / Cost per Call metrics | yes | only if the postback works |
| Our build effort | none | webhook receiver + mapping + testing |

If the client has no existing provider, CallRail is the obvious pick: cheap,
fast, and the only one where the attribution question is already settled.

### Path A — CallRail (Beta)

Attributes calls to ad impressions **using the caller's IP address**.

**Hard requirement: Website Pool numbers.**

> Must use a CallRail **Website Pool** (not static numbers) — only Website Pool
> calls include the IP address.

A Website Pool is CallRail's dynamic number insertion: it swaps the displayed
number per visitor, which is what ties a call back to a web session and its IP.

**Setup**

1. **CallRail** — Numbers → Create Number → Online → My Website → create a
   Website Pool.
2. **Vibe** — Marketplace → CallRail → Start Integration → choose the advertiser
   → copy the S2S Conversion link.
3. **CallRail** — Settings → Integrations → Webhooks → **Call Routing Complete**
   → Add URL → paste the S2S link → Advanced Settings → tick **Include IP
   Address** → Update.

It is the *Call Routing Complete* webhook, not post-call. The "Include IP
Address" checkbox is the whole ballgame — without it the integration silently
has nothing to match on.

Per-advertiser, so blocked behind the same missing advertiser record as
everything else. Also **Beta** — verify calls actually land in the Calls metric
during the trial rather than assuming.

### Path B — the client brings their own provider

Then we build the postback ourselves against:

```
GET https://t.vibe.co/s2s-conversion/events
  ?aid=PIXEL_ID &a=lead &eid=UNIQUE_CALL_ID &ip=VISITOR_IP &ts=UNIX_MS
```

**Ask their provider these four questions before agreeing to anything.** Any
"no" means calls cannot be attributed on that platform, whatever else it does:

1. Does it do **dynamic number insertion** — a different number per web visitor?
   Static numbers are unattributable by construction.
2. Does it capture and **retain the web visitor's IP** for the session that
   produced the call?
3. Can it fire a **webhook on call completion that includes that IP**? Not the
   caller's carrier IP, not our server's — the visitor's.
4. Does the webhook carry a **stable unique call ID** for deduplication?

Note the IP must be the end user's, not a server's. A provider that relays
through its own backend without forwarding the original visitor IP fails (2)
and (3) even if it has a webhook.

S2S events also do **not** populate retargeting audiences, on either path.

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

## 5. Cost (CallRail path)

Entry tier is roughly $50/month plus per-number and per-minute usage, with a
free trial — the right vehicle for the initial test.

DNI pools consume several numbers at once (that is how the swapping works), so
check how pool size affects the bill before sizing it. This applies to any DNI
provider, not just CallRail.

---

## 6. Before switching recording on

Call-recording consent rules vary by state, and one-party vs two-party changes
what has to be announced at the start of a call. Confirm Oklahoma's position —
and any state the client expands into — rather than inheriting a default from
another market. This is a legal check; get it confirmed rather than inferred.

---

## 7. What is not done

- **Provider not chosen** — waiting on the client. CallRail is the fallback
- No account, no DNI pool, no numbers purchased; integration not started
  (also blocked on the advertiser record)
- If they bring their own provider: the four questions in §3 Path B are
  unanswered, and (2) and (3) are where these usually fail
- **Whether integration-sourced calls count toward the Leads 0.1% floor** (§1)
  — the one open item that could move a launch date
- Recording consent position unconfirmed (§6)
