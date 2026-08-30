# Launch runbook — CTV client onboarding

From signed client to live campaign. Written for HomeBuyerForCash but the shape
is reusable: one config file per client, and every gate is a command that either
passes or tells you what is missing.

```
clients/<client>.json          every launch parameter, one file
scripts/preflight.py           can we launch? what is blocking, and whose job is it?
scripts/build_campaign_payload.py   config -> validated campaign payload
integrations/attune-tag/       the measurement layer (tag, bridge, probes)
```

The rule throughout: **a gate is a command, not a memory.** If a step can be
checked, it is checked.

---

## Stage 1 — open the account

| | Action | Gate |
|---|---|---|
| 1.1 | Create the advertiser in the dashboard. **No API for this.** | `preflight.py` stops reporting it |
| 1.2 | Copy the pixel ID into `clients/<client>.json` → `vibe.pixel_id` | as above |
| 1.3 | Payment method on the account | dashboard |

Until 1.1 exists, nothing downstream can be tested. The pixel belongs to the
advertiser, so a shared advertiser means mixed conversion data.

## Stage 2 — deploy measurement

| | Action | Gate |
|---|---|---|
| 2.1 | Serve `attune-tag.js` from `tag.pgammedia.com`, TLS, `Cache-Control` ≤ 1h | `preflight.py` shows `tag hosted ok` |
| 2.2 | Client installs the snippet site-wide + `attune('event','lead')` on the enquiry confirmation page | — |
| 2.3 | Open `integrations/attune-tag/test/live-check.html?id=<pixel>` | all rows green |
| 2.4 | Confirm page views on the Web Pixel page | events visible |
| 2.5 | **Wait 12 hours of real traffic** | Traffic campaigns will not publish without it |

2.3 is the first time the tag runs against the real tracker. Everything before
that was tested against a stub.

## Stage 3 — call tracking (parallel with 1 and 2)

| | Action | Gate |
|---|---|---|
| 3.1 | `test/dni-probe.py https://<client-site>` and again with `--plain` | exit `0` visitor-level, `1` static |
| 3.2 | webhook.site + a real call to one of their numbers | read the actual payload |
| 3.3 | Pick the config from those two results | see below |
| 3.4 | Provision the TV number — **before the video edit is locked** | `preflight.py` stops warning |

**Choosing from 3.1 / 3.2**

| Result | Config | Work |
|---|---|---|
| Visitor-level **and** visitor IP in the payload | vendor's own DNI | point their webhook at `/v1/call`; it uses the payload IP |
| Either fails, client accepts a second vendor | CallRail | Website Pool forwarding to their phone system, native integration |
| Either fails, no second vendor | our bridge | deploy `integrations/attune-tag/bridge/` |

`/v1/call` handles the first and third identically — payload IP if usable,
otherwise lease lookup — so the endpoint can be wired before 3.1 and 3.2 return.

**If deploying the bridge**, in this order:

1. Buy the pool numbers, all forwarding to the client's main line
2. Set `ATTUNE_CALL_TOKEN` — the endpoint is open without it
3. Set `ATTUNE_TRUSTED_PROXY_HOPS` and **verify from a known external IP** that
   the stored lease carries that address, not a private one
4. Run with `ATTUNE_DRY_RUN=1`, make a real call, check the logged IP
5. Turn dry-run off

Step 3 is the one that fails silently. Everything else fails loudly.

## Stage 4 — build and publish

```sh
scripts/preflight.py clients/<client>.json          # must exit 0
scripts/build_campaign_payload.py clients/<client>.json > /tmp/payload.json
```

The builder refuses to emit while anything is unset or contradicts a platform
rule — including `optimization_goal_value`, which must come from the advertiser
and never from a default, an industry average, or another client's campaign.

Then fire `create_or_update_campaign` with that payload and publish.

**Prove the pipe before you publish.** From a normal connection: check your
public IP, open the site, note the number shown, call it, confirm an event
arrives carrying *your* IP.

## Stage 5 — after launch

**Leave it alone for 14 days.** New campaigns carry a 14-day learning phase, and
pausing or duplicating resets a further 5. With a 30-day attribution window,
today's conversions reflect the last month of impressions — so cutting budget
looks like nothing changed, and raising it looks like performance got worse.
Both are false signals.

Run two windows and never confuse them: **1 day** for tactical reads, **30 days**
for what the client is shown and judged on.

Once lead events have run 7 days and the conversion rate clears 0.1%, publish a
**second** campaign on the Leads goal. Goal is immutable after publish, so this
is never a switch.

---

## The thing that will look like a bug

Pre-launch, a test conversion registers but shows as **unattributed**. That is
correct.

- **Web Pixel page** — every event from every source. Your test appears here.
  This proves the plumbing.
- **Reports page** — only events matched to a household that saw an ad. Your
  test does *not* appear, because no ads have run.

Prove the pipe before launch; confirm the match at day 2–3. Reporting updates
hourly and can be partial for 6–7 hours after an impression.

## Standing constraints

- **Never proxy the pixel beacon server-side.** The tracker infers the household
  IP from the request source; relay it and every conversion is attributed to our
  server. The S2S conversion endpoint is different — it takes `ip` as a
  parameter and is built to be called server-to-server.
- **Never invent an IP.** A call with no matching lease is dropped. A wrong
  attribution is worse than a missing one.
- **No ethnicity targeting** on housing-adjacent advertising. The builder
  rejects it.
- **Expect our numbers below the client's GA.** Different models, not a bug —
  say so before they notice.

## Tests

```sh
scripts/test_build_campaign_payload.py          # 29 — config validation
integrations/attune-tag/test/run.sh             # 22 — browser, tag + number swap
integrations/attune-tag/bridge/test_bridge.py   # 29 — bridge, real socket
```

80 checks. None of them touch a live vendor: the tag suite stubs the tracker and
the bridge suite captures conversions instead of sending them. That proves our
side of every contract and none of theirs — which is what `live-check.html` and
Stage 3 are for.
