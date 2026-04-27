# Guardrail agents (pgam-intelligence)

Four scheduled agents that protect contract-floor compliance, ads.txt
integrity for PGAM-owned O&O sites, overall LL/TB configuration health, and
per-partner revenue optimization (with strict RON-isolation safety).
All run inside the existing `scheduler.py` worker on Render and post to the
system Slack webhook.

## 1. Contract Floor Sentry (9 Dots and friends)

| Field | Value |
|---|---|
| Module | [`agents/optimization/contract_floor_sentry.py`](../agents/optimization/contract_floor_sentry.py) |
| Cadence | **Hourly** at :15 (changed from daily 06:00 ET on 2026-04-25) |
| Severity | P3 (normal restoration, deduped daily per demand) → P1 (repeat offender) |
| Slack | `SLACK_WEBHOOK` (system-level via `core.slack`) |

### What it does

Scans every active LL demand whose name matches a contract-protected token
(currently `9 dots` / `9dots` → $1.70 minimum, defined in
`core.ll_mgmt.PROTECTED_FLOOR_MINIMUMS`). Any demand whose live
`minBidFloor` has slipped below the contract minimum gets restored to the
minimum and ledgered with `actor="contract_floor_sentry"`.

This is the *defense-in-depth* layer behind the write-path clamp in
`ll_mgmt.set_demand_floor()` — the clamp catches API-side drops, the
sentry catches everything the clamp can't see (manual UI edits, archived-
and-recreated demands, third-party LL config changes).

### What changed (2026-04-25 hardening)

1. **Hourly cadence** — was daily 06:00 ET. Maximum exposure window between
   a UI-side floor drop and restoration shrunk from ≤24h to ≤1h.
2. **Slack alerts on every restoration** — was silent. Posts a P3 with the
   demand id, old floor, and restored value. Deduped to one post per demand
   per UTC day.
3. **Repeat-offender escalation** — if the floor_ledger shows the same
   demand has been restored ≥2 times in the trailing 7 days,
   the alert is upgraded to P1 and bypasses dedup. That pattern means
   *something upstream keeps dropping the floor* and the sentry alone is
   masking the real bug.
4. **Restoration-failure alerts** — if `set_demand_floor()` itself raises,
   a P1 alert fires immediately so we don't quietly leave a violation in
   place.

### How to see results

- **Slack:** P3 / P1 alerts as described above
- **Floor ledger:** `data/floor_ledger.jsonl.gz` — every restoration is
  appended with actor `contract_floor_sentry`. Inspect with:
  ```bash
  python -m core.floor_ledger --show
  ```
- **Render logs:** stdout from `scheduler.py` shows
  `[contract_floor_sentry] restored demand … (was X, prior_restorations_7d=N)`

### Manual trigger

```bash
cd ~/Desktop/pgam-intelligence
python -m agents.optimization.contract_floor_sentry
```

Set `LL_DRY_RUN=true` to preview restorations without writing.

### Adding a new contract floor

Edit `PROTECTED_FLOOR_MINIMUMS` in `core/ll_mgmt.py`:

```python
PROTECTED_FLOOR_MINIMUMS = [
    (("9 dots", "9dots"), 1.70),
    (("new partner",), 2.50),  # ← add here
]
```

Both the write-path clamp and the sentry pick this up automatically.

---

## 2. ads.txt Monitor

| Field | Value |
|---|---|
| Module | [`agents/alerts/adstxt_monitor.py`](../agents/alerts/adstxt_monitor.py) |
| Cadence | Daily 09:00 ET |
| Severity | P1 (missing/wrong-relationship), P2 (fetch error), P3 (unexpected entry) |
| Slack | `SLACK_WEBHOOK` |

### What it does

For every PGAM-owned O&O site (currently `destination.com` and
`boxingnews.com`), fetches `https://<site>/ads.txt` over HTTPS and verifies
that PGAM's own seats are present and set to `DIRECT`:

- `pgammedia.com, pgam-{site}-001, DIRECT`
- `limelight.com, ll-pgam-{site}-001, DIRECT`
- `teqblaze.com, tb-pgam-{site}-001, DIRECT`

Source-of-truth for the full ads.txt content lives in `pgam-wrapper`
(`configs/ads.txt.<site>`) — the monitor enforces only the contract-critical
PGAM-owned subset, so adding a new SSP partner doesn't require a code change here.

### Severities

| Severity | Trigger | Why |
|---|---|---|
| **P1** | Required line missing OR present but not `DIRECT` | Demand partners stop bidding into that seat → silent revenue drop |
| **P2** | ads.txt unreachable / non-200 / non-text | Site may be down or ads.txt removed |
| **P3** | DIRECT entry on a PGAM-owned domain we don't recognize | Someone added a seat without updating pgam-wrapper source-of-truth — likely benign, worth investigating |

### How to see results

- **Slack:** P1 + P2 always page; P3 is deduped per-day-per-site
- **Snapshots:** `logs/adstxt_snapshots.json` — last seen sha + entry count
  per site. Useful for change detection across runs.
- **Render logs:** one summary line per site:
  ```
  [adstxt_monitor] destination.com: status=200 missing=0 wrong_rel=0 unexpected_direct=0
  ```

### Manual trigger

```bash
cd ~/Desktop/pgam-intelligence
python -m agents.alerts.adstxt_monitor
```

Sample output (success):
```json
{
  "ran_at": "2026-04-25T13:00:00+00:00",
  "sites_scanned": 2,
  "reports": [
    { "site": "destination.com", "status": 200, "sha": "ab12cd34…",
      "entry_count": 18, "missing": [], "wrong_relationship": [],
      "unexpected_direct": [] }
  ]
}
```

### Adding a new site

Append to `REQUIRED_ADSTXT` in `agents/alerts/adstxt_monitor.py`:

```python
REQUIRED_ADSTXT = {
    "destination.com": [...],
    "boxingnews.com":  [...],
    "newsite.com": [
        ("pgammedia.com",  "pgam-newsite-001", "DIRECT"),
        ("limelight.com",  "ll-pgam-newsite-001", "DIRECT"),
        ("teqblaze.com",   "tb-pgam-newsite-001", "DIRECT"),
    ],
}
```

Also add the matching ads.txt file in `pgam-wrapper/configs/ads.txt.newsite.com`
so the source-of-truth and monitor stay in sync.

### Action when it fires

| Alert | Investigation steps |
|---|---|
| P1 missing | 1) Check `https://<site>/ads.txt` directly. 2) Check pgam-wrapper `configs/ads.txt.<site>` for the line. 3) Check static-site build logs — was the latest ads.txt deploy successful? 4) Re-deploy from pgam-wrapper. |
| P1 wrong relationship | Someone (or some build) changed DIRECT → RESELLER. Find the diff via `git log -p configs/ads.txt.<site>` in pgam-wrapper. |
| P2 fetch error | Confirm site is up. Check DNS / CDN / hosting. |
| P3 unexpected | Either add the new seat to source-of-truth (legitimate) or remove it from live (rogue edit). |

---

## 3. LL + TB Config Auditor

| Field | Value |
|---|---|
| Module | [`agents/alerts/config_auditor.py`](../agents/alerts/config_auditor.py) |
| Cadence | Daily 06:45 ET (after `config_health_scanner` at 06:30) |
| Severity | P1 (contract breach / TB unexpectedly live), P2 ($0 or outlier floor), P3 (orphan / zombie wiring) |
| Slack | `SLACK_WEBHOOK` (digest, deduped daily) |

> **Relationship to `config_health_scanner`** — disjoint sibling. The scanner
> auto-fixes known-good defaults (`supplyChainEnabled`, `lurlEnabled`,
> `qpsLimit` util) and runs first at 06:30. This auditor runs 15 min later
> and reads the post-fix state, flagging only things that need human
> judgment (floors, wirings, TB shadow). No field overlap.

### What it does

Walks every active LL demand + publisher and flags configurations that look
off. This is the broad sweep that complements the per-domain agents
(`contract_floor_sentry`, `floor_gap`, `dead_demand`) — they each watch one
failure mode, this one is the "are we set up correctly?" daily check.

The TB section inverts the usual logic per the LL-only memory: any signs of
TB activity (reachable creds + active inventories / placements / non-zero
floors) get flagged P1, since TB is supposed to be dormant. If TB auth fails
outright, that's the expected steady state and we report `dormant`.

### Checks

| Severity | Kind | Trigger |
|---|---|---|
| P1 | `contract_floor_below_min` | Active demand whose name matches a contract token has live floor below `PROTECTED_FLOOR_MINIMUMS` (defense-in-depth on `contract_floor_sentry`) |
| P2 | `zero_floor_active_demand` | `status=1` demand with `minBidFloor` of 0 / null — any bid wins regardless of margin |
| P2 | `outlier_high_floor` | Floor > $15 — almost always a typo (e.g. $35 vs $3.50), blocks fill |
| P3 | `orphan_active_demand` | Active demand with zero publisher wirings — either wire it or pause it |
| P3 | `zombie_wiring_paused_demand` | Paused demand still wired to one or more publishers |
| P1 | `tb_unexpectedly_live` | TB API reachable AND any active inventory/placement OR non-zero floor |
| P3 | `tb_reachable_but_idle` | TB API reachable but everything is zeroed out — consider revoking creds |

### How to see results

- **Slack:** one digest per UTC day summarising P1 / P2 / P3 counts and up to 6 examples each. Suppressed when zero findings.
- **JSON report:** `data/config_audit_report.json` — full per-finding detail with proposed fix. Useful for re-runs and ticket creation.
- **Render logs:** one summary line per run:
  ```
  [config_auditor] done — 7 findings (LL: 6, TB: 1); report at data/config_audit_report.json
  ```

### Manual trigger

```bash
cd ~/Desktop/pgam-intelligence
python -m agents.alerts.config_auditor
```

### Action when it fires

| Kind | Investigation steps |
|---|---|
| `contract_floor_below_min` | Should self-heal within 1h via `contract_floor_sentry`. If it persists, check write-path / UI edits / archived-and-recreated demand. |
| `zero_floor_active_demand` | Set a real floor in LL UI. $0 floors pass any bid through and erode margin. |
| `outlier_high_floor` | Verify the floor is intentional. Common cause: missed decimal point. |
| `orphan_active_demand` | Either wire the demand to ≥1 publisher or pause/archive it. |
| `zombie_wiring_paused_demand` | Either re-activate the demand or unwire it from publishers. |
| `tb_unexpectedly_live` | Disable TB state via TB UI, or update memory if PGAM is intentionally re-enabling TB. |

### Tuning

- `HIGH_FLOOR_THRESHOLD` (default `$15.00`) — bump if a legitimate CTV demand uses high floors.
- New contract floors — add to `core.ll_mgmt.PROTECTED_FLOOR_MINIMUMS`; auditor picks them up automatically.

---

## 4. Partner Revenue Optimizer

| Field | Value |
|---|---|
| Module | [`agents/optimization/partner_revenue_optimizer.py`](../agents/optimization/partner_revenue_optimizer.py) |
| Cadence | Every 4 hours (aligned with `auto_revert_harmful`) |
| Severity | Slack post per change (no severity tier — these are intentional optimization writes) |
| Slack | `SLACK_WEBHOOK` |
| Kill switch | `PARTNER_OPTIMIZER_ENABLED=1` required to write — otherwise dry-run only |

### What it does

Per-partner floor-lift optimizer for a small whitelist of partner publishers
(AppStock, Start.IO Video Magnite, Start.IO Display Magnite, PubNative In-App
Magnite). Lifts the floor on partner-UNIQUE low-yield demands so the freed
impressions can flow to higher-eCPM unique demands already wired on the same
publisher.

### Hard safety contract

| Rule | Mechanism |
|---|---|
| Never touches RON/shared demands | Filters to demands wired to exactly **1** publisher |
| Never touches non-whitelisted publishers | `PARTNER_PUBS` dict — explicit code change to extend |
| Never lowers floors | Skips any candidate where current floor ≥ proposed |
| Cap 3 changes/run, 1/partner/day | Per-run + ledger-based per-partner-per-day check |
| Defaults to dry-run | `PARTNER_OPTIMIZER_ENABLED=1` env var required to write |
| Auto-rollback on >20% revenue drop in 48h | Existing `auto_revert_harmful` (every 4h) handles this |
| Contract floor minimums respected | `set_demand_floor()` clamp — sentinel demands skip-restored |

### How to enable

In Render dashboard → Environment → add:
```
PARTNER_OPTIMIZER_ENABLED=1
```

Then redeploy. The agent will start applying changes on its next 4-hour tick.
Until that var is set, the agent runs every 4h in dry-run mode and ledgers
proposed changes (with `dry_run=True`) so you can review what it would do.

### How to see results

- **Slack:** one message per applied change (or per dry-run preview), with
  demand id, old → new floor, eCPM, imp share, estimated 7d upside, and
  the auto-rollback contract.
- **Floor ledger:** `data/floor_ledger.jsonl.gz` — every change appended
  with `actor=partner_revenue_optimizer_<YYYYMMDD>`. Inspect with:
  ```bash
  python -m core.floor_ledger --show | grep partner_revenue
  ```
- **Render logs:** stdout summary per run:
  ```
  [partner_revenue_optimizer] enabled=True ll_dry_run=False → effective_dry=False
  [partner_revenue_optimizer] 2 candidate floor lifts (across 4 partners)
  [partner_revenue_optimizer] changes today by partner: {290115373: 0}
  ```

### Manual trigger

```bash
# Dry-run (safe; PARTNER_OPTIMIZER_ENABLED unset)
cd ~/Desktop/pgam-intelligence
python -m agents.optimization.partner_revenue_optimizer

# Apply mode (writes!)
PARTNER_OPTIMIZER_ENABLED=1 python -m agents.optimization.partner_revenue_optimizer
```

### Why this isn't a re-run of the deprecated `floor_optimizer`

`scripts/floor_optimizer.py` was unregistered 2026-04-25 because its kill
switch was being bypassed and writes landed every 2h without oversight. This
agent is intentionally different:

1. **Partner whitelist** — touches only the 4 pubs in `PARTNER_PUBS`.
2. **Unique-only filter** — never touches a demand wired to ≥2 publishers,
   even if someone widens the whitelist by mistake.
3. **Strict caps** — 3 changes/run + 1/partner/day = at most 4 changes/day total.
4. **Belt-and-suspenders kill switch** — the env-var gate AND `LL_DRY_RUN`
   support AND the existing safety nets (`auto_revert_harmful`,
   `revenue_guardian`) catch any misbehavior.

### Adding a new partner

Edit `PARTNER_PUBS` in `agents/optimization/partner_revenue_optimizer.py`:

```python
PARTNER_PUBS = {
    290115377: "AppStock",
    # ... existing partners ...
    99999999: "New Partner Pub Name",  # ← add here
}
```

Tune thresholds (`LOW_YIELD_ECPM_CEILING`, `LOW_YIELD_MIN_IMP_SHARE`,
`NEW_FLOOR_ON_LIFT`) if the new partner has a different yield profile.

---

## Required env vars (all four agents)

| Var | Purpose |
|---|---|
| `LL_API_BASE_URL` / `LL_CLIENT_KEY` / `LL_SECRET_KEY` / `LL_UI_EMAIL` / `LL_UI_PASSWORD` | LL API auth (sentry only) |
| `LL_DRY_RUN` | Set `true` to preview floor changes without writing (sentry only) |
| `SLACK_WEBHOOK` | Posts via `core.slack` (both agents) |

All other config (TZ, PYTHONUNBUFFERED, autoDeploy) is already handled by
`render.yaml`.

## Disabling

Comment out the relevant `schedule.every(...).do(...)` line in
`scheduler.py`. The agent module remains importable for manual runs.
