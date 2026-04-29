# Autonomous Revenue Loop — Handoff for TB (TechBid) Platform

**Source**: pgam-intelligence repo (LL/Limelight stack)
**Target**: TB programmatic platform
**Updated**: 2026-04-26
**Reference session**: Built over 2026-04-18 to 2026-04-26 in pgam-intelligence

This doc captures the patterns, agents, and lessons-learned so the same
autonomous revenue-optimization architecture can be replicated on the TB
platform. **It's architecture-first, not code-first** — copying code wholesale
won't work because TB's data model, API, and seat structure differ from LL.
The patterns transfer; the implementations need adaptation.

---

## Why this exists

PGAM had three compounding problems that the architecture below was built
to solve:

1. **Autonomous agents making bad floor changes** — the ML portfolio
   optimizer dropped a contracted demand's floor from $1.80 to $0.00 with
   no safety net, costing ~$900 in one Saturday. Floor changes were
   firing every 2h with ±39% step sizes, thrashing DSP equilibrium.

2. **Silent ghost writes** — for months, a legacy floor agent was writing
   to a broken API endpoint. The ledger said "applied" but live state never
   changed. False sense of optimization happening.

3. **No accountability** — when revenue dropped, there was no way to
   attribute cause. Was it our changes? Partner-side? Market? Nobody knew.

The architecture below ensures: every change is **safe** (write-path
clamps), **ledgered** (every write recorded), **graded** (post-hoc A/B
verdict), **reverted** when wrong, and **reported** (daily Slack digest).
Plus it actively hunts new revenue patterns and acts on the safe ones.

---

## The architecture in one diagram

```
                 ┌─────────────────────────────────────────────┐
                 │  TRIGGERS (cron + real-time)                │
                 └──────────┬──────────────────────────────────┘
                            │
       ┌────────────────────┼────────────────────┐
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────┐   ┌──────────────┐    ┌──────────────────┐
│ DETECTION    │   │ ACTION       │    │ ACCOUNTABILITY   │
│              │   │              │    │                  │
│ trend_hunter │   │ auto_wire    │    │ change_outcome   │
│ pacing_dev   │   │ auto_unpause │    │   _digest        │
│ partner_     │   │ auto_revert_ │    │ intervention_    │
│   churn      │   │   harmful    │    │   journal        │
│              │   │ auto_adjust_ │    │ pacing alerts    │
│              │   │   wirings    │    │                  │
│              │   │ trend_hunter │    │                  │
│              │   │   auto-raise │    │                  │
└──────────────┘   └───────┬──────┘    └──────────────────┘
                           │
                           ▼
                 ┌──────────────────────────────────┐
                 │ SAFETY LAYER (every write)       │
                 │                                  │
                 │ - write_path_clamp (contracts)   │
                 │ - multi_pub_guard                │
                 │ - verify (re-GET after PUT)      │
                 │ - ledger.record() audit trail    │
                 └──────────────────────────────────┘
                           │
                           ▼
                 ┌──────────────────────────────────┐
                 │ TB API / TB DASHBOARD            │
                 └──────────────────────────────────┘
```

Layers above the API call are the autonomous loop. Each layer can fire
without the others; their value compounds.

---

## The 12 agents — what each does

### Safety / enforcement (always-on, defensive)

| Agent | Cadence | Role | TB equivalent design |
|---|---|---|---|
| `write_path_clamp` (in `core/ll_mgmt.py`) | real-time | Clamps any sub-contract floor write to the contract minimum (e.g. 9 Dots ≥ $1.70). Catches code-path violations regardless of which agent attempts. | Wrap TB's equivalent of `set_demand_floor`. Maintain a `PROTECTED_FLOOR_MINIMUMS` list keyed on TB seat/campaign names. |
| `contract_floor_sentry` | hourly | Daily defense-in-depth scan: walks every demand, restores any whose live floor has slipped below contract. Catches UI edits / external changes the clamp can't see. | Walk TB's seat/campaign list, fetch live rate, restore if below contract. |
| `multi_pub_guard` (in `set_demand_floor`) | real-time | Refuses demand-global floor writes that would change >1 publisher's behavior unless caller passes `allow_multi_pub=True` after aggregating per-pub recommendations. | If TB has demand-global writes, mirror this. If TB writes are per-seat, this isn't needed. |

### Reversion (post-hoc, defensive)

| Agent | Cadence | Role | TB equivalent design |
|---|---|---|---|
| `auto_revert_harmful` | every 4h | Scans last 48h of ledger writes. For each, compares pre vs post per-demand revenue. Reverts anything causing >20% drop. Hard threshold; fast. | Same algorithm. TB's equivalent of demand_id is the seat or campaign ID. |
| `intervention_journal` | every 4h | Per-write A/B grader. For each ledger entry 48h+ old, computes pre-rate vs post-rate, assigns WINNER (>110%) / NEUTRAL / LOSER (<85%). Auto-reverts losers. More precise than `auto_revert_harmful` (uses baseline comparison, not fixed threshold). | Direct port. Adjust thresholds per TB's revenue volatility. |
| `auto_adjust_wirings` | every 6h | After a wiring is 48h+ old: if it accumulated >50k bid requests but <$0.50 revenue, remove the wiring. The DSP doesn't value that pub's inventory. | If TB has "wirings" (seat × inventory associations), same pattern. |

### Additive optimization (autonomous, opportunity-seeking)

| Agent | Cadence | Role | TB equivalent design |
|---|---|---|---|
| `auto_wire_gaps` | daily | Reads gap report (qualifying pub × demand pairs not currently wired but should be). Auto-wires up to 5/day above threshold. | TB's equivalent: which seat/campaign combinations exist on peer pubs but not target pub. |
| `auto_unpause` | daily | Re-enables silently-paused inventory: a pub × demand that was active, is now paused, has historical revenue, AND no ledger-explained pause reason. | Direct port. |
| `config_health_scanner` | daily | Scans for config blockers: `supplyChainEnabled=False`, `lurlEnabled=False`, `qpsLimit` at 90%+ utilization. Auto-fixes safe ones. Slacks low-margin demands as renegotiation candidates. | TB has its own config flags — identify the equivalent revenue-blocking settings (likely there are TB-specific ones). |

### Aggressive optimization (autonomous, growth-seeking)

| Agent | Cadence | Role | TB equivalent design |
|---|---|---|---|
| `trend_hunter` | every 6h | Detects: underpriced demands (WR ≥50% + eCPM > $0.50), DSP-declining demands, WoW drops. **Auto-raises floors +10% on underpriced demands** (cap 3/run, ±10% step, intervention_journal catches losers). Slacks investigative findings. | Same algorithm. Re-tune thresholds for TB's revenue distribution. |
| `dayparting` | hourly | Per-hour-of-week floor schedule for high-variance demands. Predictable pattern (DSPs can learn it) vs random nudges (DSPs play defensive). Gated by env flag. | Direct port. Predictable patterns work with platform ML, random ones fight it. |

### Reporting (read-only, accountability)

| Agent | Cadence | Role | TB equivalent design |
|---|---|---|---|
| `change_outcome_digest` | daily 09:15 ET | Single morning Slack post: "what got changed in last 24h" + "outcomes from changes 48-72h ago" + "weekly tally with $/wk impact estimate". Closes the feedback loop. | Direct port. |
| `pacing_deviation` | every 2h | Slack alert when today's pace is <80% of 4-week same-DOW same-hour median. | Direct port. |
| `weekly_review_digest` | Mon 09:00 ET | Single Slack digest of equilibrium changes needing human approval (floor raises/drops on active demands, anything >5% total-book impact). | Direct port. |
| `advisory_verifier` (in `intelligence/`) | wraps LLM outputs | Reality-checks LLM-generated Slack memos against live state before publishing. Replaces stale advisories with a discrepancy alert. | Direct port — wherever TB has LLM-powered advisory output. |

---

## The autonomy bar (CRITICAL — read this twice)

The single most important architectural decision, drawn from the 9 Dots
incident: **autonomous agents auto-execute additive/restorative actions;
equilibrium changes need human approval.**

### Auto-execute (no human in the loop)

- **Wire NEW** (pub × demand additions) — purely additive, can't break existing
- **Restore contract floor** — strictly safer than the broken state
- **Revert harmful change** — restoring known-good prior state
- **Re-enable silently-paused** — restoring known-good prior state
- **Enable schain/LURL/QPS** — config best practices, no equilibrium impact
- **Floor RAISES on underpriced demands** (only with intervention_journal as safety net + cap of 3/run + ±10% step + 24h cooldown + 2% of book cap)

### Human-approve via weekly digest

- Floor raises/drops on active high-revenue demands (without the safety net above)
- Pause/disable an active demand
- Anything estimated >5% total-book impact
- Partner conversations (Pubmatic, BidMachine reps)

### Why this bar matters

Equilibrium changes are what blew up on 04-18 (9 Dots) and 04-22→04-24
(`floor_optimizer` thrashing). Auto-executing them once was acceptable
in theory but catastrophic in practice. The autonomy bar moved them to
human-approved-by-default, then graduated SOME of them back to autonomous
once the safety net (intervention_journal) was proven.

**On TB**: start with the bar at "auto-execute only additive things."
Graduate equilibrium changes to autonomous after 30 days of clean
intervention_journal data showing it correctly catches losers.

---

## Why this doesn't conflict with TB's own ML / traffic shaping

Industry pattern: SSP/DSP platforms have their own bid-shading ML that
adapts to floor signals. **Constant nudging fights that ML.** Our system:

1. **Per-demand 24h cooldown** on auto-raises → DSPs see steady signal
2. **Step size cap** ±10% (vs the broken floor_optimizer's ±39%) → smaller
   adaptations DSPs can follow
3. **Cap on writes per run** (3 raises max) → bounded rate of change
4. **48h evaluation window** before grading a change → DSPs have adapted
5. **Predictable patterns** (dayparting on hour-of-week) > random tweaks
6. **One-shot decisions** (revert OR keep) → no oscillation

When evaluating revenue lift on TB after deployment, give changes
**≥48h of post-change data** before declaring a verdict. Anything earlier
is noise.

---

## Rollout sequence (order matters)

When implementing on TB, ship in this order. Each layer protects against
the failures that the next layer introduces.

1. **Ledger** — `core/floor_ledger.py` equivalent. Append-only audit log
   of every revenue-affecting change, with `actor`, `reason`, `dry_run`,
   `applied`, `old_value`, `new_value`. Without this, nothing else has
   data to evaluate.

2. **Write-path clamps** — `core/ll_mgmt.set_demand_floor` equivalent.
   Single chokepoint for floor writes. Includes:
   - Contract minimum enforcement (`PROTECTED_FLOOR_MINIMUMS`)
   - Multi-entity guard (refuse writes that affect >1 entity unless ack'd)
   - `verify=True` re-GET after PUT (catches silent endpoint failures —
     this caught the months-long ghost-write bug)
   - Ledger record on every call

3. **Defensive agents** (catch what already broke):
   - `contract_floor_sentry`
   - `auto_revert_harmful`

4. **Reporting** (so you know what's happening):
   - `change_outcome_digest`
   - `pacing_deviation`

5. **Additive agents** (start growing carefully):
   - `auto_wire_gaps`
   - `auto_unpause`
   - `config_health_scanner`

6. **A/B grader** (precision reversion):
   - `intervention_journal`

7. **Aggressive agents** (only after #6 has 30 days of clean data):
   - `trend_hunter` with auto-raise enabled
   - `dayparting`

8. **LLM advisory layer** (only after #1–7 are stable):
   - LLM analyst (e.g. `claude_analyst.write_revenue_gap_memo`)
   - **`advisory_verifier` BEFORE the LLM ships** — never deploy an LLM
     advisory without the reality-check wrapper

---

## Things that will trip you up (lessons from PGAM)

### 1. Silent endpoint bugs

LL's UI exposed two write paths for floor changes. One worked. The other
returned 200 OK but silently discarded the change. For months, agents
wrote to the broken one. The ledger said "applied=true" but live state
was unchanged.

**Fix**: every write must be followed by a `verify=True` re-GET that
fails loudly if live ≠ written. If you can't trust the API, trust the
re-read.

### 2. OOM on data-pull agents

The hourly collector (`intelligence/collector.py`) pulled 30 days of
hourly funnel data in a single API call. Response.json() spiked memory
past 512 MB instance limit. Worker died every hour, daily-cron jobs
never fired.

**Fix**: split heavy data pulls into chunks. Run on a plan with adequate
RAM (Render Standard 2GB minimum for our scale). Use `PYTHONUNBUFFERED=1`
so logs flush in real time and you can diagnose.

### 3. Floor thrashing

A legacy `floor_optimizer.py` ran every 2h with ±39% step sizes. When
its broken endpoint was fixed, the writes started landing. DSPs had no
chance to adapt → bid responses dropped → revenue tanked.

**Fix**: cap step size (±10%), cap writes per run (3), cap cadence
(daily NOT hourly for equilibrium changes). Plus intervention_journal
catches losers in 48h.

### 4. Stale LLM advisories

An LLM advisory recommended cutting "$3.25 floors" that didn't exist.
Almost merged a floor-cut PR that would have been a no-op (best case)
or misleading attention (worst).

**Fix**: `advisory_verifier` — every LLM Slack post passes through a
fact-check that pulls live state for entities mentioned and replaces
the post with a "stale data" alert if claims don't verify.

### 5. Render redeploys reset cron timers

Every PR merge triggers Render autoDeploy. The Python `schedule` library
fires jobs N hours after registration, not on absolute clock times. If
deploys happen every <N hours, the agent never fires.

**Fix**: spread deploys, OR use absolute-time scheduling (`schedule.every().day.at("09:30")`).
Watch the Schedule Registered log block on each boot.

### 6. The contract violation (the big one)

ML portfolio optimizer dropped contracted demand from $1.80 to $0.00
because it didn't know about the contract floor.

**Fix**: write-path clamp + contract sentry + `PROTECTED_FLOOR_MINIMUMS`
list. Architecturally impossible to violate now. **Make sure TB has the
same. Find every contract minimum BEFORE turning on autonomy.**

---

## File-level pointers (for a Claude session implementing TB)

The reference implementation lives in `pgam-intelligence` at
`~/Desktop/pgam-intelligence`. Read in this order:

```
core/ll_mgmt.py                          # write_path_clamp, set_demand_floor, multi_pub_guard
core/floor_ledger.py                     # the append-only audit trail
agents/optimization/contract_floor_sentry.py
agents/optimization/auto_revert_harmful.py
agents/optimization/intervention_journal.py
agents/optimization/auto_wire_gaps.py
agents/optimization/auto_unpause.py
agents/optimization/auto_adjust_wirings.py
agents/optimization/config_health_scanner.py
agents/optimization/trend_hunter.py
agents/alerts/pacing_deviation.py
agents/reports/change_outcome_digest.py
agents/reports/weekly_review_digest.py
intelligence/advisory_verifier.py        # LLM advisory reality-check
intelligence/dayparting.py               # predictable hour-of-week schedule
scheduler.py                             # the cron registry — see Schedule Registered block
```

Each of those files has a docstring at the top explaining the design.
Read the docstrings before reading the code.

---

## TB-specific implementation checklist — ANSWERED 2026-04-26

### 1. TB's equivalent of `set_demand_floor`
`core.tb_mgmt.set_floor(placement_id, price=X, price_country=..., is_optimal_price=...)`.
Backed by `POST /api/{token}/edit_placement_banner|video|native` (deployed 2026-04-24).
Type-aware dispatch inside `set_floor()` based on placement's `type` field.

### 2. TB's equivalent of `biddingpreferences[].value[]`
**No direct equivalent.** TB's per-publisher demand entry is
`inventory_dsp[white][]` — list of DSP endpoint IDs (from `reference_dsp_list`)
allowed to bid on that inventory. There's no per-DSP floor on the SSP side
(TB admin UI only). Floor lives on the *placement* (`price`), not the demand line.

### 3. TB's equivalent of `demand_id`
No 1:1. TB splits the concept:
- For **floor writes**: unit is `placement_id` (numeric, account-wide).
- For **demand identity**: `company_dsp` from `/report` (e.g. `"Pubmatic #2"`
  where `2` matches the catalog `key` in `reference_dsp_list`).
- For **whitelist edits**: the partner ID (catalog `key`).
A floor write affects a single placement and serves all currently
whitelisted DSPs uniformly — no per-DSP floor.

### 4. TB's equivalent of `publisher_id`
Two layers:
- `userId` — TB account (e.g. `45=PGAM`, `36=Rough Maps`, `60=Aditude`, `32=RevIQ`).
- `inventory_id` — single site/app under a user (e.g. `441=BoxingNews`,
  `544=Modrinth`, `64=OP.GG`).
For Slack alerts/grouping, prefer `inventory_id`; for partner-level
churn/concentration, use the `publisher` attribute from `/report` (encodes
user as `"PublisherName #userId"`).

### 5. Contract minimums on TB
**None enforced on TB side as of 2026-04-26 — this is a gap.**
Known LL contract floors that may carry to TB if same partners are plumbed:
- 9 Dots demands (LL ids 692/693/955) — $1.70 minimum
- See memory files `pgam_recon` and `9 dots contract floor`.
**Action**: enumerate which TB inventories/seats correspond to contract-floor
partners; seed `PROTECTED_FLOOR_MINIMUMS` in `tb_mgmt.set_floor`. Until the
mapping exists, the clamp falls back to a global `MIN_FLOOR=$0.01` to
prevent obvious zero-out attacks.

### 6. Multi-entity floor writes on TB
**No.** Every floor write is `placement_id` scoped, one-to-one. A single
write cannot affect more than one placement. So `multi_pub_guard` isn't
needed — the architecture is naturally per-entity. The closest "multi"
write is `edit_inventory` (DSP whitelist for an inventory), still scoped
to one inventory.

### 7. Verifiable read-after-write API on TB
**Yes, fully supported.** `GET /api/{token}/placement?placement_id=X` returns
the full state including `price`, `price_country`, `is_optimal_price`,
`status`, `banner.sizes`. Confirmed cross-account in 2026-04-24 testing.
`verify=True` should: write → 0.3s pause → re-GET → assert price match → raise on mismatch.
Catches silent endpoint failures like LL's ghost-write bug.

### 8. TB's revenue/funnel data source
`tbm.partner_report()` and `/report` endpoint. Attributes:
`placement, country, traffic, ad_format, inventory, domain, company_dsp,
publisher, placement_name, size`. Pagination via `offset` (max `limit=5000`/page).
`day_group=hour` for hourly. Data freshness ~15-30 min lag, no streaming.
Cache: live placement state on demand; revenue stats 1-2h; DSP catalog 24h.

### 9. TB's deploy target
**Render** (same as LL) — single service `scheduler.py` in `pgam-intelligence`.
Same OOM lesson: collector agents must use chunked pulls or `PYTHONUNBUFFERED=1`.

### 10. TB's Slack webhook
**Shared** with LL — `core.slack.post_message` reads `SLACK_WEBHOOK` env var.
Already wired into all TB agents shipped 2026-04-24+. To split channels later,
add `SLACK_WEBHOOK_TB` and route by agent module path.

### 11. TB LLM advisory output today
**None.** No TB LLM advisory shipped. When one is added (e.g.
`intelligence/tb_analyst.py`), wrap in `advisory_verifier` *before* first
Slack post. Don't repeat the LL stale-advisory near-miss.

### 12. Env var pattern
TB agents currently use `--apply` flag, not env vars. Phase-2 plan:
- `TB_AUTO_APPLY_ENABLED=true` — global kill switch
- `TB_FLOOR_NUDGE_ENABLED=true` — per-agent gate (default off)
- `TB_AGGRESSIVE_LIFT_ENABLED=false` — high-risk default off
- `TB_GUARDIAN_AUTOREVERT_ENABLED=true` — defensive default on
Implemented as `core.tb_mgmt._check_enabled(agent_name)` helper.

---

## Rollout status (TB) — 2026-04-26

| Layer | Status | File |
|---|---|---|
| 1. Unified ledger | ✅ | `core/tb_ledger.py` |
| 2. Write-path clamp + verify | ✅ | `core/tb_mgmt.set_floor` (PROTECTED_FLOOR_MINIMUMS + verify=True) |
| 3. Defensive: contract sentry | ✅ | `agents/optimization/tb_contract_floor_sentry.py` |
| 3. Defensive: auto-revert | ✅ | `revenue_guardian.py` verify phase |
| 4. Reporting: churn / concentration / compression | ✅ | `agents/alerts/partner_churn_radar`, `demand_concentration`, `yield_compression` |
| 5. Additive: floors | ✅ | `tb_floor_nudge`, `aggressive_floor_lift`, `min_floor_sweep`, `optimal_price_sweep` |
| 5. Additive: traffic hygiene | ✅ | `blocked_domains_agent`, `brand_safety_sweep` |
| 5. Additive: dead pause | ✅ | `placement_status_agent` |
| 5. Additive: opportunity | ✅ | `size_gap_agent`, `placement_autocreate_agent` |
| 6. A/B grader | ✅ | `revenue_guardian` (24h warm-up) |
| 7. Aggressive | ✅ live | `tb_floor_nudge` 4h, `aggressive_floor_lift` one-shot |
| 8. LLM advisory | ⏸️ deferred | — |

**Open items**:
- Enumerate contract-floor partner IDs on TB; populate `PROTECTED_FLOOR_MINIMUMS`
- Build `change_outcome_digest` for morning Slack summary
- Build TB-specific `pacing_deviation`

**TB platform-side roadmap (confirmed by TB team 2026-04-29)**:
- **SSP Company / AdX API** — TB acknowledged the gap. Misha is scoping
  the build, ETA pending. Once shipped, we can drop the
  `ssp_company_optimizer` catalog-name reverse-parse and get canonical
  SSP Company P&L direct from the API.
- **`price_country` on banner placements** — documented in
  BannerModification but silently drops on write. Flagged to Vadym.

---

## Bottom line

This isn't a code drop — it's a transferable architecture. The agents
work because they compose:

- Detection finds patterns
- Action acts only on safe ones
- Safety prevents catastrophes
- Reversion catches the rest
- Accountability surfaces truth

Each layer is bounded (per-run caps, per-step caps, cooldowns) so even
a misbehaving agent has a small blast radius. Each layer is observable
(ledger entries, Slack posts) so you can audit and improve.

**On TB**: don't shortcut the safety layer. The 9 Dots incident cost
real money because the optimizer didn't know about the contract. Build
the clamp + sentry first. Everything else compounds on top.

---

*Generated 2026-04-26 from pgam-intelligence session. Reach the architecture
author through the floor_ledger entries and PR history (PRs #7, #9, #10,
#11, #12, #13, #14, #15, #16, #17, #18, #19, #21, #22, #23 in
mastap150/pgam-intelligence).*
