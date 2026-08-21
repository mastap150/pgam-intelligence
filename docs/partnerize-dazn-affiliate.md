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
   Partnerize sent *"Funds are available to withdraw"*, and the console
   shows a DAZN subscription conversion attributed to boxingnews. A
   terminated participation does not accrue withdrawable commission.
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
