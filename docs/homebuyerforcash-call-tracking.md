# Call tracking — HomeBuyerForCash

Decisions and open questions for measuring phone enquiries alongside web
enquiries. Companion to [`homebuyerforcash-wiring-plan.md`](./homebuyerforcash-wiring-plan.md).

Status: **specified, nothing provisioned.** No numbers bought, no CallRail
account opened, integration not started.

> **Superseded, 2026-08-27.** An earlier version of this document specified
> building the call→conversion postback by hand against the S2S endpoint, and
> listed "does the provider's webhook expose the visitor IP?" as the gating
> unknown. Both are obsolete: there is a **native CallRail integration** and the
> IP question is answered — see §3. The manual S2S route is no longer the plan.

---

## 1. Calls are measurable natively — but not optimisable

The native integration adds two metrics to the dashboard: **Calls** (attributed
call events) and **Cost per Call**. That is a real improvement on what I
described before, and it retires the workaround I had proposed: there is no need
to tag `{source:'call'}` and reconcile against CallRail's own reporting by hand,
and no need to ask about custom event slots.

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

## 3. The native CallRail integration (Beta)

Attributes phone calls to ad impressions **using the caller's IP address**.

### Hard requirement: Website Pool numbers

> Must use a CallRail **Website Pool** (not static numbers) — only Website Pool
> calls include the IP address.

A Website Pool is CallRail's dynamic number insertion: it swaps the displayed
number per visitor, which is what ties a call back to a web session and its IP.
Static numbers carry no IP and therefore cannot be attributed. This confirms the
two-number architecture in §4 — and it is the reason a TV-only number can never
appear in the Calls metric.

### Setup

1. **CallRail** — Numbers → Create Number → Online → My Website → create a
   Website Pool.
2. **Vibe** — Marketplace → CallRail → Start Integration → choose the advertiser
   → copy the S2S Conversion link.
3. **CallRail** — Settings → Integrations → Webhooks → **Call Routing Complete**
   → Add URL → paste the S2S link → Advanced Settings → tick **Include IP
   Address** → Update.

Note it is the *Call Routing Complete* webhook, not post-call. And the
"Include IP Address" checkbox is the whole ballgame — without it the integration
silently has nothing to match on.

The integration is per-advertiser, so it is blocked behind the same missing
advertiser record as everything else.

It is also **Beta**. Budget some contingency and verify calls actually appear in
the Calls metric during the trial rather than assuming.

---

## 4. Two numbers, because TV and web are different problems

| | Number type | Attributed? | Where it shows |
|---|---|---|---|
| Website | CallRail Website Pool (DNI) | **Yes** — carries visitor IP | Calls / Cost per Call in the dashboard, and CallRail |
| TV creative | one dedicated static number | **No** — no IP exists | CallRail only |

You cannot do dynamic number insertion on a television screen, so the TV number
is necessarily static and necessarily unattributed. That is a property of the
medium, not a misconfiguration.

Use a TV number that appears **nowhere else** — not the website, not Google
Business, not a van livery. Then every call to it is provably TV-driven, and
CallRail's own reporting gives a clean count even though the platform dashboard
will never show it. For a client whose main question is "is the TV advertising
making my phone ring", that count is the answer, and it does not depend on IP
matching at all.

---

## 5. Cost

CallRail's entry tier is roughly $50/month plus per-number and per-minute usage.
There is a free trial, which is the right vehicle for the initial test.

Website Pools consume several numbers at once (that is how the swapping works),
so check how the pool size affects the bill before sizing it.

---

## 6. Before switching recording on

Call-recording consent rules vary by state, and one-party vs two-party changes
what has to be announced at the start of a call. Confirm Oklahoma's position —
and any state the client expands into — rather than inheriting a default from
another market. This is a legal check; get it confirmed rather than inferred.

---

## 7. What is not done

- No CallRail account, no Website Pool, no numbers purchased
- Integration not started (blocked on the advertiser record)
- **Whether integration-sourced calls count toward the Leads 0.1% floor** (§1)
  — the one open item that could move a launch date
- Recording consent position unconfirmed (§6)
