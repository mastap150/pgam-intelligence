# Partnerize / DAZN affiliate — boxingnews.com

Written 2026-08-21 after a Partnerize payout alert prompted the question
"I didn't realise we had this wired in". Short answer: it has been wired in
since June 2024, and it was switched off on 2026-08-06 on a premise that
does not hold up.

## What exists

boxingnews.com is a partner on **DAZN Global Partners** via Partnerize.

| | |
|---|---|
| Network | Partnerize (Performance Horizon Group) |
| Advertiser | DAZN |
| Campaign | DAZN Global Partners |
| Partner account | `ppatel@pgammedia.com` (console username `priyeshp`) |
| Camref | `1101l3MQmm` |
| Console | https://console.partnerize.com |

Provenance, from the `Affiliate Setup` mail thread:

- **2024-06-04** — Priyesh approaches Nick Zod (Head of Affiliates, DAZN).
- **2024-06-05** — Zod invites us to apply; Will Harbord-Hamond invites the
  account to the Global Partners campaign and issues the tracking link
  `https://prf.hn/click/camref:1101l3MQmm`. Partnerize confirms by email:
  *"Your request to join DAZN GLOBAL Partners has been accepted."*
- **2024-06-24** — Harbord-Hamond explains adref segmentation:
  *"If you want to differentiate between your schedule link and editorial,
  you can add an adref to the end of your link, which you can filter by in
  your reporting. E.g. `https://prf.hn/click/camref:1101l3MQmm/ar:Schedule`"*

That adref guidance is why the code used `ar:` rather than `pubref:`.

## Where it lives in code (`mastap150/boxingnews`)

| File | Role |
|---|---|
| `src/lib/affiliate/operators.ts` | Operator registry; DAZN entry + `active` flag |
| `src/lib/affiliate/broadcaster-link.ts` | Maps `events.broadcaster` → click-out URL |
| `src/app/api/affiliate/click/route.ts` | Click bouncer: logs to `affiliate_clicks`, then 302s |
| `src/lib/article-body-affiliate-injector.ts` | Injects in-body CTAs (gated on `active`) |
| `src/lib/affiliate/dazn.ts` | **Deleted 2026-08-06** — built the `prf.hn` URL |

Surfaces that link DAZN: `/schedule`, `/schedule/today`,
`/schedule/this-weekend`, `/event/[slug]`, `/how-to-watch/[slug]`, plus
article-body injections. All route through the bouncer, so
`affiliate_clicks` holds click volume independent of Partnerize reporting.

## What changed on 2026-08-06

Commit `fb59da1` — *"chore(affiliate): retire Partnerize DAZN wrap; add
operator active flag"*:

- deleted `src/lib/affiliate/dazn.ts` (the `prf.hn` builder),
- dropped the Partnerize special-case from the click bouncer, so DAZN now
  resolves through the generic `resolveOperatorUrl()` — the
  `AFFILIATE_DAZN_URL` env var if set, otherwise plain `https://www.dazn.com/`,
- set `active: false` on the DAZN operator, which hides it from the
  affiliate strip (`select.ts`) and stops article-body injection.

Its stated premise:

> Partnerize dropped boxingnews.com's DAZN affiliate in 2026-08 — every
> click that landed in the prf.hn tunnel from then on was unattributed.

## Why that premise looks wrong

1. **No termination notice exists.** A search of `ppatel@pgammedia.com`
   across all mail (including trash/spam) for `partnerize`, `prf.hn`,
   `camref` and `affiliate program` returns no message from Partnerize or
   DAZN ending, suspending, or repricing the participation. The only
   Partnerize mail in the last 180 days is the payout alert below.
2. **The account is still being paid.** On **2026-08-21 08:00 UTC**
   Partnerize sent *"Funds are available to withdraw"* against a DAZN
   subscription conversion attributed to boxingnews. A terminated
   participation does not release funds. Note the conversion itself is dated
   **2026-06-28**, not August — see the aggregate-report section below, which
   is the authoritative read on volume and timing.
3. **The likely source of the confusion is a 403, not a termination.** The
   deleted `dazn.ts` documented one already:

   > The publisher account at console.partnerize.com lacks 'update'
   > permission on its own publisher entity, so
   > `POST /v2/publishers/.../links` returns 403.

   A 403 on a *link-management* endpoint says nothing about whether the
   participation is active. `scripts/partnerize_audit.py` deliberately
   distinguishes the two cases.

## Consequence

Since 2026-08-06, unless `AFFILIATE_DAZN_URL` is still set to a `prf.hn`
value in the boxingnews Vercel project, every DAZN click has been 302-ing
to bare `dazn.com` — traffic delivered, no attribution, no commission.
In-body DAZN CTAs stopped rendering entirely.

`AFFILIATE_DAZN_URL` **could not be verified from a cloud session**:
`boxingnews.com` is blocked by the environment's egress proxy (so the live
redirect can't be probed), and the Vercel MCP connector is read-only with
no env-var read tool. Check it in the Vercel dashboard, or probe the live
bouncer from an unrestricted machine:

```bash
curl -sI "https://www.boxingnews.com/api/affiliate/click?op=dazn" | grep -i location
# Location: https://prf.hn/...   -> still attributed; only the CTAs were lost
# Location: https://www.dazn.com/ -> unattributed since 2026-08-06
```

## Verifying against the API

`scripts/partnerize_audit.py` answers each question from the Partners API
rather than from a code comment. Read-only; needs the two console keys:

```bash
export PARTNERIZE_APP_KEY=...      # console -> Settings -> Account settings
export PARTNERIZE_API_KEY=...
export PARTNERIZE_PUBLISHER_ID=... # or --whoami to look it up

python3 scripts/partnerize_audit.py --all
```

| Question | Flag | Endpoint |
|---|---|---|
| Is DAZN still approved? | `--participations` | `/user/publisher/{id}/campaign/{a,p,r}`, `/v3/partner/{id}/participations` |
| Do we still hold the camref? | `--camrefs` | `/reference/publisher/camref/{id}` |
| What converted, and when was the click? | `--conversions` | `/reporting/report_publisher/publisher/{id}/conversion.json` |
| What is unwithdrawn? | `--balance` | `/user/publisher/{id}/available_commission` |

The conversions report is the one that settles it: each row carries both
`conversion_time` and the originating `click.set_time`. The script labels
every conversion by whether its **click** predates 2026-08-06:

- click **before** 2026-08-06 → pre-teardown pipeline converting on DAZN's
  own validation lag; consistent with the wrap being genuinely dead now.
- click **on/after** 2026-08-06 → traffic is still reaching the `prf.hn`
  tunnel, which is only possible via a tracked `AFFILIATE_DAZN_URL` left
  set in Vercel. Attribution never actually stopped; the teardown only
  cost us the in-body CTAs.

Note that `ar:` adrefs surface as `advertiser_reference` on the conversion
record, and the report's `multipivot` filter supports only `campaign`,
`product` and `publisher_reference` — so filter adrefs client-side (the
script prints the field) rather than server-side.

## What the aggregate report actually shows (2026-08-21)

Daily aggregate export from the console, 2026-01-01 -> 2026-08-21 (233 days):

| | |
|---|---|
| Total conversions | **1** |
| Total partner commission | **14.65946** |
| Total order value | 0 |
| Date of the only conversion | **2026-06-28** |
| Conversions on/after the 2026-08-06 teardown | **0** |

This materially changes the reading:

- **The conversion predates the teardown by 39 days.** Today's "Funds are
  available to withdraw" alert is that single June commission clearing DAZN's
  validation window (~7.5 weeks) and becoming payable. It is *not* evidence of
  current attribution.
- **It still refutes "terminated".** Partnerize does not release funds on a
  dead participation, and no termination notice exists. But it is weaker
  evidence than a fresh conversion would have been.
- **Zero conversions since 2026-08-06 is not diagnostic.** At a base rate of
  1 conversion per 233 days, the expected count over the 15 days since the
  teardown is ~0.06. Observing 0 tells us nothing either way.
- **The teardown cost almost no realized revenue.** The baseline was ~1
  conversion per 8 months. This is not an emergency; treat restoring it as
  buying back optionality, not recovering revenue.
- `total_order_value` is 0 while commission is non-zero, and
  `percentage_average_partner_commission` is `Infinity` — i.e. DAZN pays a
  **flat CPA bounty per subscription**, not a revenue share. The export
  carries no currency column; confirm GBP vs USD in the console (the 5-decimal
  value suggests an FX conversion into the reporting currency).

### The real question is clicks, not conversions

One conversion in eight months has two very different explanations:

- **(A) Placement/demand** — we barely send DAZN clicks, or we send clicks
  from readers who will not subscribe.
- **(B) Attribution loss** — we send plenty of clicks and they are not landing
  on camref `1101l3MQmm`.

Distinguish them by comparing two numbers over **2026-01-01 to 2026-08-05**
(scope it pre-teardown; after that we deliberately stopped sending `prf.hn`
traffic, so a post-Aug-6 gap is expected, not a bug):

1. **Partnerize's count** — the clicks report, same export screen as the
   aggregate: `/reporting/report_publisher/publisher/{id}/click.json`.
2. **Our count** — the boxingnews click ledger, which the bouncer writes
   independently of Partnerize:

   ```sql
   SELECT date_trunc('month', clicked_at) AS month, count(*)
     FROM affiliate_clicks
    WHERE operator_id = 'dazn'
      AND clicked_at >= '2026-01-01' AND clicked_at < '2026-08-06'
    GROUP BY 1 ORDER BY 1;
   ```

A large gap points at (B) and the wrap is worth fixing properly. Both numbers
small points at (A), and the fix is editorial placement, not code.

The June 28 conversion's `advertiser_reference` (the `ar:` adref) identifies
which surface converted — schedule, event page, how-to-watch, or article body.
The aggregate export does not carry it; `partnerize_audit.py --conversions`
prints it.

## Two gating bugs found while tracing this

Both matter more than DAZN does, because sportsbook CPA is $50-200/signup
against DAZN's ~15/subscription, and Priyesh had DraftKings / BetMGM /
BetRivers applications in flight as of 2026-08-19.

`operators.ts` documents `active` as "defaults to true when omitted", and the
sportsbook entries (fanduel, draftkings, bet365-uk, betmgm) all omit it. Two
consumers then read that `undefined` in **opposite** directions:

1. **`src/lib/affiliate/select.ts`** (affiliate strip on preview/recap
   articles) filters `active !== false`, so `undefined` counts as **active** —
   and it applies **no `isOperatorTracked` check**. With the
   `AFFILIATE_*_URL` vars unset, `resolveOperatorUrl` returns the operator's
   marketing homepage, so the strip pushes readers to **unattributed**
   sportsbook homepages. That is precisely what `cta-gate.ts` was written to
   prevent, and the strip bypasses it.

2. **`src/lib/betting/cta-gate.ts`** (betting hub: `/betting`,
   `/betting/preview/*`, `/betting/props/*`) requires `active === true`
   strictly, so `undefined` counts as **inactive**. Deliberate fail-closed,
   and defensible — but it means that when a sportsbook deal lands and the
   env var is set, those CTAs stay dark until someone also adds
   `active: true`. A landmine timed to the exact moment the deals land.

Pick one default and make both consumers agree. Fail-closed (`active === true`
plus a tracked-URL check) is the safer choice, applied to the strip as well —
but then every operator needs an explicit `active: true`, and that should be
asserted in a test rather than left to memory.

## Monitoring (added 2026-08-21)

The root cause of this whole episode is that **affiliate attribution had no
monitoring at all**. `affiliate_clicks` was created with three stated jobs
(boxingnews `src/lib/affiliate/schema.ts`); the third —

> Alert if click volume drops to zero (broken tracking link)

— was never built. Nothing read the table except the Ticketmaster admin page,
and pgam-intelligence ran ~30 revenue alert agents with none on affiliate. So
a live program could be believed dead for six weeks with nothing to contradict
it.

`agents/alerts/affiliate_health.py` closes that. Daily 08:45 ET, behind
`PGAM_AFFILIATE_HEALTH_ENABLED`, registered just after the existing
08:30 ingest-health alert so both boxingnews watchdogs report together.
Three independent checks, each degrading to a logged skip rather than
failing the run:

| Check | Needs | Catches |
|---|---|---|
| **A. Attribution probe** | outbound HTTPS only | An operator whose `AFFILIATE_<OP>_URL` is unset, so its clicks 302 to the operator's own marketing site and earn nothing |
| **B. Click-volume regression** | `BOXINGNEWS_DATABASE_URL` | An operator that used to get clicks and now gets none — removed CTA, closed gate, broken link |
| **C. Conversion reconciliation** | `PARTNERIZE_*` keys | Real click volume against zero conversions — the signature of lost attribution rather than thin demand |

Check A is the important one. It needs no credentials, it would have caught
`fb59da1` the next morning, and it is the only thing that will catch a
sportsbook deal going live without its env var wired — the failure that
`cta-gate.ts` is designed around but cannot report on.

Two implementation details worth knowing:

- **The probe pollutes the table it watches.** The bouncer logs every hit,
  including the watchdog's. Probes therefore carry `p=monitor`, and check B
  excludes that placement — otherwise the watchdog would manufacture the
  click volume it is supposed to be measuring.
- **`OPERATOR_MARKETING_DOMAINS` mirrors `operators.ts` by hand.** An operator
  added there but not here is simply not probed, so check A reports how many
  operators it probed — drift shows up as a short list rather than a silent
  pass. Ticketmaster is deliberately excluded (different bouncer, different
  semantics).

Check A classifies each 302 as **tracked** (lands on a known affiliate-network
host), **untracked** (lands on the operator's own marketing domain), or
**unknown** (anything else — reported but never alerted on, so a network we
haven't listed reads as unrecognised rather than broken).

## Finding new programs

`partnerize_audit.py --discover` lists every brand and campaign this partner
can join, from `GET /v2/publishers/{id}/discovery/advertisers`. Each campaign
carries a per-partner status — `AVAILABLE`, `REQUESTED`, `INVITED`,
`REJECTED` — and the command marks the actionable ones:

```bash
python3 scripts/partnerize_audit.py --discover
python3 scripts/partnerize_audit.py --discover --discover-keyword bet
```

This is how to answer "is there a sportsbook on Partnerize we could apply
to?" without clicking through the console.

Joining is a **write** (`POST /v2/publishers/{id}/campaign-requests`) and the
script deliberately does not do it — applying to a program is a commercial
decision, not a side effect of an audit.

## Restoring it (if the checks confirm the program is live)

In `mastap150/boxingnews`, essentially a revert of `fb59da1`:

1. Restore `src/lib/affiliate/dazn.ts` (`git show fb59da1^:src/lib/affiliate/dazn.ts`).
2. Re-add the `op=dazn` branch in `src/app/api/affiliate/click/route.ts` so
   it composes `buildDaznUrl({ adref: '{slug}-{placement}' })` — the
   per-click adref is what makes each CTA filterable in DAZN's reporting.
3. Drop `active: false` from the DAZN operator in `operators.ts`. The
   affiliate strip and the article-body injector both pick DAZN back up on
   the next render with no further change.
4. Confirm `DAZN_CAMREF=1101l3MQmm` is set in the boxingnews Vercel project
   (the builder falls back to the same value, so this is belt-and-braces).
5. Redeploy — Vercel bakes env vars in at build time.

Keep the bouncer in front of it either way: `affiliate_clicks` is the only
click record we control, and it is what makes DAZN's reporting auditable.

## Related

- `destination-com` PR #388 — *"partnerize: wire the real auth shape + fail
  loudly on missing entitlement"* (2026-08-16). Separate Partnerize
  integration for Expedia on destination.com, but it hit the same auth
  surface; worth reading before writing new Partnerize client code.
