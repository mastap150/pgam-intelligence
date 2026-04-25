# Guardrail agents (pgam-intelligence)

Two scheduled agents that protect contract-floor compliance and ads.txt
integrity for PGAM-owned O&O sites. Both run inside the existing
`scheduler.py` worker on Render and post to the system Slack webhook.

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

## Required env vars (both agents)

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
