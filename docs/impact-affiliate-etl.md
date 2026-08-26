# impact.com affiliate leg

Notes on PGAM's affiliate revenue ETL: what it is, what is verified, and what
has to happen before its numbers can be quoted at anyone.

Written 2026-08-26.

---

## 1. Why this exists, and why it is not the MCP server

impact.com ships a remote MCP server
(<https://integrations.impact.com/ai-solutions/mcp-quick-start>) — OAuth 2.1,
per-client token, respects the account's own permissions, exposes reports,
partners, campaigns, deals and invoices to any MCP-capable AI tool. It is a
good thing and worth connecting.

It is also not a substitute for this. An MCP server answers a question asked
by a person in a session. It puts no row in Neon, so:

- no dashboard can chart affiliate revenue next to programmatic revenue,
- no scheduled agent can alert on it,
- and next month it cannot tell you what last month did unless somebody asks
  it again, by hand.

This leg is the durable half. The two are complements: connect the MCP server
for ad-hoc questions, run this for everything that has to survive the session.

Nothing about affiliate revenue was in this repo before — every prior mention
of Impact was prose in strategy docs (`08_monetization_strategy.md` lines 186
and 244, `docs/expedia-affiliate-decision.md` line 97), naming it as a network
PGAM *could* use.

## 2. Credentials

Two values, from the impact.com UI under **Settings → API Access**:

| Env var | UI field |
|---|---|
| `IMPACT_ACCOUNT_SID` | Account SID |
| `IMPACT_AUTH_TOKEN` | Auth Token |

Auth is HTTP Basic (SID as username, token as password). Prod values go in the
**Render** dashboard on `pgam-intelligence-scheduler` — see CLAUDE.md; this
repo does not deploy to Vercel. `render.yaml` declares both with
`sync: false`.

Optional: `IMPACT_ACCOUNT_TYPE` selects which side of the marketplace the
credentials address — `Mediapartners` (publisher; the default and what PGAM is)
or `Advertisers`. An advertiser account would be a **second credential pair**,
not a flag flip: the SID differs per account.

No new Python dependencies. `requests`, `psycopg`, `python-dotenv` were
already in `requirements.txt`.

## 3. What is built

| File | Role |
|---|---|
| `core/impact_api.py` | Transport + reads. Basic auth, retry/throttle, `@nextpageuri` pagination, field-name tolerance. Read-only — there is no write layer. |
| `migrations/2026_08_26_impact_affiliate.sql` | `pgam_direct.impact_actions` ledger + two rollup views. |
| `agents/etl/impact_revenue_etl.py` | The ETL. Two passes, UPSERT by action id. |
| `scripts/impact_probe.py` | Read-only probe. **Run this first.** |
| `tests/test_impact_etl.py` | 23 offline checks over the pure layer. |
| `scheduler.py` | Hourly at `:58`, plus a daily deep pass at 05:20 ET. |

The hourly job is registered explicitly rather than through the `_hourly()`
helper: that helper's fifteen four-minute slots (`:00`…`:56`) are all taken,
and a sixteenth call wraps to `:00`, which would silently double up with
`partner_revenue_etl`. `:58` was the last free minute on the hour.

## 4. The one way affiliate data is not like ad data

**It goes backwards.**

An ad impression is final the moment it is counted. An affiliate action is
not: a conversion recorded in March can be REVERSED in June, when the shopper
returns the item or the advertiser rejects the sale.

Every other ETL in this repo writes pre-aggregated daily rows and refreshes a
trailing window — correct for impressions, because nothing older than the
window ever changes. Applied here that shape **ratchets revenue permanently
upward**: the reversal arrives after the window has moved past the action's
date, so the day keeps money it lost. Understating revenue gets noticed;
overstating it does not.

So the storage shape is different on purpose:

```
pgam_direct.impact_actions                    -- one row per ACTION, PK action_id
pgam_direct.impact_daily_campaign_revenue     -- VIEW over the ledger
pgam_direct.impact_daily_property_revenue     -- VIEW over the ledger
```

A reversal of any age UPSERTs one ledger row and every rollup corrects itself
in the same instant, because there is only one copy of each number. Volume
makes that affordable — affiliate conversions are thousands per month, not the
400k rows/hour the LL ETL moves.

Seeing the reversal is a separate problem from storing it, hence two passes
per run:

1. **Event-date window** — 45 trailing days hourly, 365 on the daily deep
   pass. Wider than the programmatic ETLs' 14 because many programs hold
   actions PENDING through a 30-day return window and only then lock them; a
   window that ends before the hold does never sees an action's final state.
2. **Modification sweep** — 7 trailing days of *lifecycle* changes, at any
   event date, including years back. This is the reversal catcher. If PGAM's
   account rejects the `ModificationDateStart` parameter the ETL logs a
   warning and carries on, and the daily 365-day pass becomes the only thing
   catching reversals — **do not narrow `DEEP_WINDOW_DAYS` while that warning
   is showing.**

### State meanings

`PENDING` and `APPROVED` can still reverse. `LOCKED` cannot. The views expose
both, and **`payout_locked` is the only column safe to invoice against**;
`payout_net` is current belief. `payout_reversed` is kept visible rather than
netted away, because a rising reversal rate on a program is a reason to stop
promoting it and is invisible if the only number carried is the net.

### Currency

Payouts are stored **unconverted** and every view carries `payout_currency` in
its grain. This repo has no FX source, and a hardcoded rate would turn a
reporting question into a silent accounting error. Summing across currencies
is wrong; the grain makes it hard to do by accident, and the ETL logs a NOTE
when an account is mixed.

### Day boundaries

`event_date` is the date portion of the vendor's timestamp **as the vendor
expressed it** — deliberately not shifted to UTC or ET. impact.com totals its
UI on its own day boundaries; shifting here would put our per-day numbers a
few hours out of step with the dashboard every day, which looks like data loss
and cannot be reconciled against an invoice.

### Property attribution

`SubId1` is how a publisher with several sites splits revenue, and it is set
by the **tracking link**, not by impact.com. `impact_daily_property_revenue`
groups on it, with unset values bucketed as `'(unset)'` rather than dropped —
unattributed revenue is a tracking-link gap worth seeing, not zero. The probe
measures coverage; if it is low, the fix is in the links, not here.

## 5. Verification status — read before quoting a number

**The client was written without a live account.** `api.impact.com` and
`integrations.impact.com` are both blocked by the egress proxy on the network
this repo's cloud sessions run on (403 at CONNECT), and no `IMPACT_*`
credential existed anywhere at the time.

What that means concretely:

- **Transport is standard** across the API — Basic auth, the JSON `Accept`
  header, the `@page`/`@numpages`/`@nextpageuri` envelope, `Page`/`PageSize`
  paging. Most likely correct.
- **Per-account field names are the risk.** impact.com exposes
  account-specific custom fields alongside the standard set, and a field this
  mapping gets wrong reads as *zero revenue*, not as an error.

Two mitigations are built in:

1. Every field is read through `ACTION_FIELDS` / `action_field()`, which try
   several spellings per logical field.
2. The ledger stores the **whole vendor payload** in `raw jsonb`, so a
   mis-mapped column is a SQL fix over data already landed, not a re-pull of
   months.

### What has actually been tested

- `tests/test_impact_etl.py` — 23 checks, all passing: amount/id/date parsing,
  the drop rules, state uppercasing, alternate field spellings, the dedupe
  rule between the two passes, and the probe's own mapping audit.
- **End-to-end against a real PostgreSQL 16**, with a faked impact.com: the
  migration applies clean and twice (idempotent); 7 actions UPSERT; the
  id-less row is dropped and reported; a lowercase `"Reversed"` normalises; a
  second currency splits into its own view row; `'(unset)'` attribution
  appears. Then a reversal was injected — one inside the window and one on a
  **200-day-old** action reachable only via the modification sweep — and USD
  `payout_net` fell from 147.50 to 47.50 while `payout_reversed` rose to
  match, with the old day corrected too. Re-running left the row count
  unchanged and preserved `first_seen_at`.
- **Not tested:** anything requiring the real API. Field names, report ids,
  whether `ModificationDateStart` is accepted, rate limits, actual page sizes.

## 6. First-run checklist

```bash
export IMPACT_ACCOUNT_SID=...
export IMPACT_AUTH_TOKEN=...

# 1. Do the credentials work at all?
python3 scripts/impact_probe.py

# 2. The one that matters: what does a real action look like, and does the
#    mapping resolve against it? Exits non-zero if a critical field is
#    unresolved, and names it.
python3 scripts/impact_probe.py --actions --days 30

# 3. Fix core/impact_api.py:ACTION_FIELDS for anything the probe flagged.
#    Add any unexpected state to the FILTER clauses in the migration.

# 4. Pull without writing — check the totals against the impact.com UI for
#    the same dates before trusting anything.
python3 -m agents.etl.impact_revenue_etl --dry-run

# 5. Land it, then backfill history.
python3 -m agents.etl.impact_revenue_etl
python3 -m agents.etl.impact_revenue_etl --backfill 365
```

Step 4 is not optional. Comparing a dry-run total against the vendor's own
dashboard for the same dates is the only independent check that the field
mapping is right — a wrong mapping produces a plausible smaller number, not an
error.

Until the credentials are set, the scheduled job no-ops each hour with a log
line naming what is missing. Deploying ahead of them is deliberate: the day
they land in Render, data flows with no redeploy.

## 7. Open questions

- **Report catalog.** Nothing here hardcodes a report id, because they are
  account-specific. `scripts/impact_probe.py --reports` lists what PGAM's
  account can actually run. Clicks and impressions — needed for EPC and
  conversion rate — live in those reports, not in `/Actions`, so a second
  grain can be added once the real ids are known. Deliberately not guessed.
- **Is `ModificationDateStart` accepted?** Decides whether reversals are
  caught hourly or daily. The probe answers it.
- **SubId1 coverage.** Decides whether per-property revenue is answerable at
  all from this data.
- **A PGAM advertiser account**, if one exists, is a separate credential pair
  and a separate leg.

## 8. Queries

```sql
-- Last 30 days by program. payout_locked is the invoiceable number.
SELECT report_date, campaign_name, payout_currency,
       payout_net, payout_locked, payout_reversed, actions_total
FROM pgam_direct.impact_daily_campaign_revenue
WHERE report_date >= current_date - 30
ORDER BY report_date DESC, payout_net DESC;

-- Which of our sites is earning.
SELECT property, payout_currency,
       sum(payout_net) AS net, sum(actions_total) AS actions
FROM pgam_direct.impact_daily_property_revenue
WHERE report_date >= current_date - 30
GROUP BY property, payout_currency
ORDER BY net DESC;

-- Reversal rate by program: a program that reverses a third of its
-- conversions is not earning what its gross suggests.
--
-- payout_currency is in the GROUP BY, not because the ratio needs it, but
-- because the sums it is built from do. Drop it and a program paying in two
-- currencies gets a rate computed from added-together money.
SELECT campaign_name, payout_currency,
       sum(payout_reversed) AS reversed,
       sum(payout_net)      AS net,
       round(100.0 * sum(payout_reversed)
             / nullif(sum(payout_net) + sum(payout_reversed), 0), 1) AS reversed_pct
FROM pgam_direct.impact_daily_campaign_revenue
WHERE report_date >= current_date - 90
GROUP BY campaign_name, payout_currency
ORDER BY reversed_pct DESC NULLS LAST;

-- Freshness: is the ETL running, and is the sweep seeing changes?
SELECT max(updated_at) AS last_write,
       max(modification_date) AS newest_vendor_change,
       count(*) AS actions
FROM pgam_direct.impact_actions;
```
