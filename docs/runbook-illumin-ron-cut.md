# Runbook — pause the five zero-yield Illumin RON demand endpoints

> **PARKED, 2026-08-20.** Not urgent and nobody should be hand-executing it.
> The cut frees QPS capacity and **$0 of revenue** — five endpoints earning
> nothing will still be earning nothing next month. It was written up because it
> was the first clean output of the new cut rule, which is a poor reason to put
> it at the front of a queue.
>
> Do it when either is true: someone in ad ops has spare time, or the
> `TBX_EMAIL` / `TBX_PASSWORD` secrets exist and it can run through
> `set_demand_source_status()` with dry-run, read-after-write verification and a
> ledger entry — which is a better way to make a live change than a person
> typing into an API console.
>
> Ahead of it, by value: the Advetisi question (~$62k/month of profit,
> unexplained) and the Verve render loss (~$34k/30d upper bound).

Authorised by Priyesh, 2026-08-19; deprioritised 2026-08-20. **This is a manual
action.** Nothing in this repo can execute it; see "Why this isn't automated"
below.

## What and why

Five demand endpoints on the legacy platform (`ssp.pgammedia.com`) have taken
**11.61 billion bid requests over 14 days and returned $0.00** — a GPM of
$0.0000/M against a marketplace baseline of $0.8225/M.

| Endpoint | Requests (30d where known) | Wins | Gross | Evidence |
|---|---|---|---|---|
| `Illumin - RON copy1 #2179` | 6.40B | 0 | $0.00 | 30d **and** 14d |
| `Illumin Endpoint3 - RON #1549` | 6.26B | 0 | $0.00 | 30d **and** 14d |
| `Illumin - RON #1553` | 4.67B | 0 | $0.00 | 30d **and** 14d |
| `Illumin Endpoint3 - RON copy1 #2178` | — | 0 | $0.00 | 14d only |
| `Illumin - RON copy2 #2311` | — | 0 | $0.00 | 14d only |

The zero is a real measurement, not a filtered row: `agents/etl/tb_revenue_etl.py`,
which writes `tb_daily_demand_revenue`, applies no `gross <= 0` filter — unlike
`tb_segments_etl.py` and nine other ETLs that do.

**What this buys: capacity, not revenue.** Teqblaze does not shape traffic, so
those 11.61B requests simply stop being sent. Nothing is redistributed and no
revenue appears elsewhere. The gain is 6.6% of QPS headroom plus whatever infra
cost tracks request volume.

## Order of operations

**1. Ask Illumin first.** They are PGAM's second-largest relationship —
~$73,280 of the 30-day $357,521 (≈20% of gross) across six *supply* setups.
We have been firing 17B+ requests at these endpoints, so we believed they were
live. One question:

> These five RON endpoints have taken ~17 billion bid requests over the last 30
> days and returned zero wins. Are they meant to be live? If so we have a broken
> integration; if not we will disable them.

If the answer is "they should be earning", that is worth more than the QPS.

**2. Wait 48 hours, then pause regardless.** The endpoints earn nothing while
we wait.

**3. Pause the three with 30-day evidence first** — `#2179`, `#1549`, `#1553`.
Leave `#2178` and `#2311` for one more weekly sweep, so every cut has two
independent observations behind it. That is the rule's own standard
(`docs/optimization-cadence.md` §3) and there is no reason to make an exception
on the first application of it.

**4. Pause, never delete.** Status off is reversible and keeps the config and
the ID. Deleting loses both, and loses the ability to re-test.

## After the cut — verify, don't assume

A cut you don't measure is a cut you can't learn from.

1. **Record it.** Append a ledger entry for each endpoint actually paused, with
   `actor` set to whoever did it. The sentry already records the *proposals*
   with `applied=False`; the manual action needs its own entry with
   `applied=True`, otherwise the ledger shows a recommendation and no outcome.
2. **At +48h**, re-run the Marketplace Headroom workflow. Confirm total bid
   requests fell by roughly 11.6B over the comparable window and that no
   *other* source's volume moved to compensate.
3. **At +7d**, run `scripts/tb_whatchanged.py --pivot <cut-date> --days 7`.
   The expected result is that these five do not appear in the decliners,
   because they contributed $0 to begin with. **If revenue moves at all, the
   cut is not the cause** — that would mean something else changed, and the
   attribution will say what.
4. **Diary the quarterly re-test** (`docs/optimization-cadence.md` §3, quarterly
   row) so a seasonal partner is not permanently removed by one fortnight.

## Why this isn't automated

Three independent blockers, all real:

1. **No credential.** There is no `TB_EMAIL` / `TB_PASSWORD` repo secret — only
   `TBX_*`, which are for the new platform and are not set either. Nothing in
   CI can authenticate against `ssp.pgammedia.com`.
2. **No code path.** `core/tb_mgmt.py` covers inventory and placements — the
   supply side. It has no demand-source or DSP-endpoint functions at all, so
   even with a credential there is nothing to call.
3. **No network.** Claude Code sessions are blocked from every PGAM host by the
   environment's egress policy (403 CONNECT, verified 2026-08-19).

And one deliberate reason on top of those: the posture agreed for this work is
propose-only, with the promotion gate in `docs/optimization-cadence.md` §3.5.
The first application of a new cut rule is the worst possible moment to also
debut an automated writer.

### A credential would not fix blocker 2

Worth being explicit, because it is the counter-intuitive one. Only **six**
legacy endpoints are known to this repo, all reverse-engineered by probing:
`list_placement`, `edit_inventory`, `edit_placement_banner`,
`edit_placement_video`, `edit_placement_native`, `set_floor`. Every one is
supply-side. **No demand-side mutation on `ssp.pgammedia.com` has ever been
found**, which is why `core/tb_mgmt.py` has no function for it.

So handing a session a legacy password would not enable this cut — it would
enable *guessing* an endpoint name and POSTing a mutation at the platform that
currently carries PGAM's live floor decisions. That is the April failure mode
with extra steps.

There is precedent for the alternative: `docs/vadym_ssp_company_endpoint_request.md`
is a previous request to Teqblaze for API surface that could not be found.

### The durable fix is the new platform, not a legacy workaround

`api.pgammedia.com` already has exactly the endpoint the legacy API lacks:

    POST /demand-sources/{id}/status

So automating this needs no favour from Teqblaze and no reverse-engineering. It
needs the two things already on the list: the `TBX_EMAIL` / `TBX_PASSWORD` repo
secrets, and the ID mapping (`docs/teqblaze-new-platform.md` §8.1.10b) so a
legacy endpoint number resolves to a new-platform demand source. `set_demand_source_status()`
in `core/tbx_mgmt.py` is already written and already gated.

### Fastest manual route: the help centre's "Try it out" console

`https://ssp-new.pgammedia.com/help-center/management-api` has an interactive
console per endpoint, authenticated as the logged-in user. So the pause can be
executed as an API call by hand, with no credential shared and no clicking
through UI screens:

1. `POST /demand-sources` with `{"filter": {"search": "Illumin"}}` — does the
   new platform even have these endpoints, and under what IDs? This answers the
   ID-mapping question for the five that matter, without waiting on Teqblaze.
2. If they are there: `POST /demand-sources/{id}/status` with the status field
   set false, one per endpoint.
3. If they are not there, the endpoints exist only on the legacy platform and
   the pause has to happen in the legacy UI.

Either way the +48h / +7d verification above still applies.

Until then: UI or the console for this one-off, and the runbook above.

## What the platform can't tell us

Zero wins is consistent with two different faults, and the legacy tables cannot
separate them:

- the endpoint **responds and always loses** → priced below our floor
- the endpoint **never responds** → broken integration

`bids-overview` on the new platform names the drop reason directly. Until those
credentials exist this stays a question for Illumin rather than a measurement.
