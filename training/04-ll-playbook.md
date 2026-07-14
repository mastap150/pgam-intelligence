# LL Playbook

LL is one of the two supply platforms PGAM runs. Target: **$10K/day**. Data does **not** live in Neon — LL has its own backend.

## What LL is

LL is a supply-side platform that connects PGAM publishers to demand partners. We manage floors, allowlists, and demand connections via LL's UI and API. Our automation (in `pgam-intelligence/scheduler.py` + `core/`) reads state and writes changes on a schedule.

## Mental model — floors and freezes

**Floors** are the minimum CPM we accept from a given demand partner on a given supply path. Floors are the main lever: too low and margin evaporates; too high and demand walks.

**Freezes** are choke-points in `core/partner_freeze.py` that prevent automated writes to specific partners. Freezes exist because a partner has broken trust or we're in the middle of a compliance investigation. **Do not remove a freeze without Priyesh sign-off.**

Current LL freezes to know:
- **Unruly (dp=5)** — all automated writes off. Compliance root-cause pending. See security doc.

## Non-negotiable partner rules

- **9 Dots (demands 692 / 693 / 955)** — contract floor **$1.70 minimum**. Floors may go higher, never lower. Contractual.
- **BidMachine (dp=40)** — QPS cap is partner-mandated. 99% utilization is expected, not opportunity.
- **Cas.ai / Nimpha LTD** — dual-role partner (supply and demand). Billing entity is Nimpha LTD (Cyprus), QBO customer 155, Net 60. Handle their supply and demand relationships as separate books.

## LL UI access

Sagar (and possibly others) has LL UI access. This means **manual UI changes bypass our ledger**. If you see unexplained state drift that our ledger didn't produce, ask the team before assuming an automation bug.

## The reporting dashboard

- **Partner Revenue Dashboard** — lives at `admin.pgammedia.com`, unified LL + TB view. Domo replacement. Started 2026-04-28.
- LL data source is LL's backend, not Neon
- TB data source is Neon (`pgam_direct` schema)

## Scheduler jobs that touch LL

Located in `pgam-intelligence/scheduler.py` + `core/`:

- Floor optimization (dry-run by default; ships changes on approve)
- Ledger writes
- Partner health checks
- Reconciliation nudges

Never run a scheduler job that writes to LL from a local shell without knowing exactly what it does. Read the job first.

## Common tasks

- **Check a partner's current floor:** query LL API via our wrappers in `core/ll_client.py` (verify path)
- **Adjust a floor:** normally scheduler decides; manual overrides go through `core/` helper functions with `--dry-run` first
- **Investigate a partner outage:** check LL UI first, then our ledger, then compare
- **Add a new demand connection:** LL UI + confirm freeze list doesn't already block them

## Escalation

- **Partner-side outage / relationship issue** → Priyesh
- **Contract terms question** (floors, minimums, exclusivity) → Priyesh, do not guess
- **Automation writing wrong values** → stop the scheduler job, check `core/partner_freeze.py`, ask before restarting
