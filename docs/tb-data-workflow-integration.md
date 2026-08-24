# TB data into `/admin/finance` and `/admin/pnl` — the runbook

Written 2026-08-24, from the vendor API reference Priyesh assembled against
the live platform. It supersedes nothing in `teqblaze-new-platform.md`; it is
the *operational* half — what to press, in what order, to get TB numbers onto
the two admin surfaces and keep them there, plus the first dynamic optimizer
on the new platform.

Read `teqblaze-new-platform.md` §1 first if you have not: **two Teqblaze APIs,
one marketplace.** Everything below turns on that fact.

---

## 1. What was actually broken

Not the code. The **credential path**, in three separate places, and each one
fails in a way that produces a plausible number rather than an error.

| Surface | Repo | Reads from | Why it stalls |
|---|---|---|---|
| `/admin/finance` — PGAM-side TB column | `pgam-recon` | legacy `ssp.pgammedia.com/api/{token}/adx-report` | Needs `TB_ACCESS_TOKEN`, a token a person mints by hand in the TB dashboard. TB suspended `/create_token` for our login on **2026-05-11** (403 "Account don't have access"), so it cannot be minted programmatically. Every lapse between hand-rotations is a day with no TB column. |
| `/admin/pnl` — `tb_gross_usd`, `tb_gross_profit_usd` | `pgam-recon` (`pnl_sync.py`) | same legacy endpoint, or `pgam_direct.tbx_daily_supply_revenue` | The TBX path needed *two* things configured — Render credentials for the hourly ETL **and** a `PGAM_DIRECT_DATABASE_URL` secret in `pgam-recon`. Neither was set, so `PNL_TB_SOURCE` could not be moved off `legacy`. |
| `pgam_direct.tbx_daily_*` — the rollups everything else reads | `pgam-intelligence` | `api.pgammedia.com` | `TBX_EMAIL` / `TBX_PASSWORD` were never added to Render, so the hourly ETL has been no-opping since it shipped. Separately, the client did `POST /share/report` → `POST /report/{hash}`, which is two calls and stakes the run on an undocumented TTL. |

The new platform authenticates with `POST /login` → JWT. **That is the fix for
all three**: no token to rotate by hand, no lapse.

---

## 2. What changed in this branch

### `pgam-intelligence`

| File | Change |
|---|---|
| `core/tbx_api.py` | `_report_call` now prefers `POST /report/` — trailing slash, empty hash. One call per page instead of mint-then-read, and no dependence on a hash TTL. Mint-then-read stays as a fallback, chosen once per process on a hash-shaped rejection. A 422 naming a bad metric is *not* a hash-shaped rejection and propagates unchanged. |
| `agents/optimization/tbx_demand_geo_floor.py` | **New.** Per-DSP × country bid-floor optimizer on `geo_settings.bid_floor`. Propose-only. |
| `scheduler.py` | Runs it daily at 09:45 ET, propose-only, no-op without credentials. |
| `tests/test_tbx.py` | Transport tests rewritten for the empty-hash preference; 3 new sections for the optimizer. |

### `pgam-recon` (PR #2)

| File | Change |
|---|---|
| `pgam_recon/fetchers/tbx.py` | **New.** The TBX client for the recon: JWT auth with a per-account token cache, `POST /report/`, pagination, demand-endpoint → canonical-partner rollup. |
| `pgam_recon/cli.py` | `RECON_TB_SOURCE` = `legacy` \| `compare` \| `tbx`. |
| `pgam_recon/reconcile.py` | `contributes = False` — run a fetcher without counting it. |
| `pgam_recon/pnl_sync.py` | The TBX reading gains a second transport: the platform API answers when the Neon rollup cannot. |
| `pgam_recon/doctor.py` | TBX login **and** a real report pull. |
| `.github/workflows/_runner.yml` | `TBX_EMAIL`, `TBX_PASSWORD`; `RECON_TB_SOURCE`, `TBX_TIMEZONE`. |

**Nothing changes until a switch is set.** Both default to `legacy`.

### `pgam-dsp-dashboard` — untouched, deliberately

`/admin/finance` and `/admin/pnl` read `finance.ssp_recon_daily` and
`finance.daily_pnl_inputs`. Repointing the *source* of those columns changes
no column name, so the UI needs no change. If a diff ever appears in that repo
for this work, something has been misunderstood.

---

## 3. The one mistake that matters

**Both hosts report the same marketplace. They are alternatives, never
addends.**

Summing them doubles every impression. On `/admin/finance` that shows as the
PGAM-side column at roughly 2× and every partner appearing to under-invoice
us; on `/admin/pnl` it doubles gross and gross profit. This is why
`compare` mode reads two hosts and counts one, and why the tables in Neon are
`tbx_daily_*` separate from `tb_daily_*` rather than extra rows.

And the money mapping, because getting it backwards yields a constant ~22–31%
offset that reads exactly like a fee applied at a different stage:

```
dsp_price_sum  = demand spend, what buyers pay      -> gross, the top line
ssp_price_sum  = publisher payout                   -> cost
profit         = dsp_price_sum - ssp_price_sum
```

The `ssp_`/`dsp_` prefixes read backwards from the intuition. Verified on
every row.

---

## 4. Credentials — where they go, and where they must not

A credential was pasted into a Claude Code session to test with. **It cannot
be used from there**: this environment's network policy denies egress to
`api.pgammedia.com` (`CONNECT tunnel failed, response 403`, re-verified
2026-08-24). Nothing in this branch has touched the live platform.

Two secret stores, and nowhere else:

| Where | Powers | How |
|---|---|---|
| `pgam-intelligence` **Render** env | hourly `tbx_revenue_etl`, daily `tbx_demand_geo_floor` | Environment → Add Environment Variable. Declared `sync: false` in `render.yaml`. |
| `pgam-recon` **Actions secrets** | the `/admin/finance` fetcher, `pnl_sync`'s API transport, `doctor` | Settings → Secrets and variables → Actions |

Not the Claude cloud environment — its variables are readable by anyone using
the environment and it is not a secrets store. Not `.env` in a commit; the
playbook records a real leak from exactly that shortcut (2026-07-02).

**Rotate the credential that was pasted into the session** once the path is
proven — it now exists in a conversation transcript. That was always the plan;
this is the reminder of when.

Two more points, both from §5.5 of `teqblaze-new-platform.md` and both still
right:

- `TBX_EMAIL` is one character from `TB_EMAIL`, and they are different hosts
  with different user stores. Entering the new login under the legacy names
  breaks the legacy leg *and* leaves the new one idle. The ETL's not-configured
  message checks for exactly this and says so.
- A **read-only** user is the correct start. Everything in §5 and §6 below is
  read-only. Only §7 needs write scope, and it should be a **second** user
  when it comes to that — not an upgrade of the first.

---

## 5. Prove the reads — in this order

Do not skip ahead. Each step is the precondition for trusting the next.

**1. `pgam-recon` → `doctor`.** Add `TBX_EMAIL` / `TBX_PASSWORD` as Actions
secrets, then run any workflow that reaches the CLI, or run locally:

```
python -m pgam_recon.cli doctor
```

Two new lines. `tbx: auth` failing means the credential is wrong or is an
`ssp.pgammedia.com` login — the platform's 422 for that case reads
"credentials do not match our records", which sends people to reset a password
that was never the problem. `tbx: yesterday's report` failing with auth passing
means the account authenticates but lacks reporting scope.

**2. `pgam-intelligence` → Actions → TBX Data Pull.** Read the capability
matrix in the job log: it says which modules the account actually licenses,
and diffs the account's live `report/columns-list` against this client's
constants.

**3. Credentials into Render.** This starts the hourly `tbx_revenue_etl`, which
fills `pgam_direct.tbx_daily_*`. Give it a few hours.

**4. Actions → TBX Neon reports → `recon`.** Does the new platform agree with
the legacy one? This is the trust gate, and it comes *before* anything
touching the P&L. Exit 0 = agree, 1 = look at it, 2 = misconfigured.

**5. Actions → TBX Neon reports → `pnl`.** Would repointing change a number
already in the P&L? A disagreement here *with* agreement in step 4 means the
P&L row is stale or hand-entered — a different problem with a different fix.

---

## 6. Flip the surfaces — independently, `compare` first

The two switches are separate on purpose: different audiences, different blast
radii.

### `/admin/finance`

Set the repository **variable** `RECON_TB_SOURCE=compare` in `pgam-recon`.
The nightly recon then reads both hosts, counts legacy, and prints:

```
[tb-compare 2026-08-23] legacy $41,203.55 vs tbx $41,198.02 (-0.01%)
    magnite          legacy $ 12,004.11  tbx $ 12,004.11    +0.00%
    …
```

Leave it a week. What you are looking for:

- **A constant small offset on every partner** → almost certainly a timezone
  mismatch, not a data problem. The legacy report's timezone is not documented
  anywhere we control; TBX defaults to `US/Eastern` here. Try `TBX_TIMEZONE=UTC`
  before concluding the platforms disagree.
- **One partner at ±100%** → a demand endpoint whose name does not match the
  alias list on one host. Fix `partner_aliases` in `config.yaml`; the
  unmatched names are logged with their revenue.
- **Everything agreeing** → set `RECON_TB_SOURCE=tbx` and the hand-rotated
  `TB_ACCESS_TOKEN` stops mattering.

### `/admin/pnl`

Same shape: `PNL_TB_SOURCE=compare`, read the delta in the run notes, then
`tbx`. The playbook's rule applies — **do not guess on P&L**. Run the recon
gate (§5 step 4) before this one, every time.

Keep the legacy leg running through all of it. It is the only independent
check on TBX's numbers, Teqblaze will keep it alive as long as we ask, and
there is a permanent cost to shutting it off early.

---

## 7. The dynamic optimizer

`agents/optimization/tbx_demand_geo_floor.py` — per-DSP × country bid floors
on `geo_settings.bid_floor`, the LL-style dynamic optimization applied to the
new platform.

**Why this lever first.** Demand-side floors carry no publisher contract
exposure. `tbm.PROTECTED_FLOOR_MINIMUMS` is still empty on TBX, so a
supply-side floor writer has nothing enforcing the 9 Dots $1.70 minimum — that
is the April incident's exact precondition. A demand-side floor written wrong
costs fill on one DSP and reverses in one call. The lever also has no legacy
equivalent, so no agent on the other host can fight it.

**What it does.** Pulls `demand_source × country` over 14 days, computes each
pair's observed eCPM from `dsp_price_sum / imps`, and proposes
`floor = 0.85 × eCPM` where the pair clears volume and quality bars **and**
that is a material increase on what is set today. Never a cut.

**Three gates, all of which must be open to write:**

```
--apply                      on the command line   (default: propose only)
TBX_ALLOW_WRITES=1           the platform write gate      (render.yaml: 0)
PGAM_OPTIMIZER_AUTO_APPLY=1  the fleet autonomy gate      (render.yaml: 0)
```

The scheduled 09:45 run passes none of them. It posts proposals to Slack and
writes nothing — which is how the LL optimizers were introduced, and what the
April incidents argued for.

**What it refuses.** A frozen partner, via `core.partner_freeze`. A report
name that resolves to zero or more than one demand source — a floor on the
wrong DSP is silent, and an ambiguous name is not worth guessing at. And a DSP
with `is_smart_floor` on, which needs a caveat: the vendored spec declares
that field on `SupplySource_IndirectSuppliersResource` and **not** on
`DemandSourceResource`, so as documented it cannot appear here. The check
stays because the live account, not the spec, is the authority on what comes
back — the vendored copy has already been caught behind the platform once
(`uuid`, 2026-08-21). The demand side's own vendor automation is
`qps_limit.qps_limit_type = "dynamic"`, a QPS lever rather than a floor one,
which does not conflict with this agent. **The one-owner-per-lever rule bites
on the supply side**, and it is one more reason a supply-side floor writer
comes last.

**Before `--apply` is ever justified**, `teqblaze-new-platform.md` §6's round
trip has to pass: `python3 scripts/tbx_probe.py --diff-shape demand:<id>`,
with no findings under either heading. That check is what confirms the
platform accepts a read-modify-write at all.

Tunables, all env, all with defaults: `TBX_GEO_FLOOR_WINDOW_DAYS`,
`TBX_GEO_FLOOR_COUNTRIES`, `TBX_GEO_FLOOR_PCT`, `TBX_GEO_FLOOR_MIN_IMPS`,
`TBX_GEO_FLOOR_MIN_ECPM`, `TBX_GEO_FLOOR_MIN_UPLIFT`,
`TBX_GEO_FLOOR_MAX_COUNTRIES`, `TBX_GEO_FLOOR_MAX_SOURCES`.

### What comes after it

The LL fleet these mirror, in the order the risk argues for. None of these
exist on TBX yet:

1. **Geo blacklist** (`set_demand_geo_blacklist`) — countries where a DSP
   spends but never clears to margin. Read-only detection is already possible
   today; the writer is one call.
2. **QPS waste** (`set_demand_qps_limit`) — but check
   `qps_limit.qps_optimization_by` first: that is Teqblaze's own QPS tuner and
   the same one-owner-per-lever rule applies.
3. **Dead demand** — pause sources with sustained zero fill.
4. **Auto-revert** — the LL side's `auto_revert_harmful` is what makes the
   rest of its fleet safe to run unattended. Nothing here should move to
   `PGAM_OPTIMIZER_AUTO_APPLY=1` before its TBX equivalent exists.
5. **Supply-side floors** — last, and not before `PROTECTED_FLOOR_MINIMUMS` is
   populated from the contract sheets.

---

## 8. Known limits of this branch

- **Nothing here has run against the live platform.** No egress from the
  session that wrote it. Every test is offline. §5 is the first real contact,
  and it is deliberately the cheapest, most reversible thing to try.
- **`POST /report/` with an empty hash is verified live** per the vendor
  reference, but not by us, and not on this account. The fallback exists for
  exactly that reason.
- **Silent date truncation.** A 21-day request came back holding only the most
  recent 5 days — no error, no flag. The recon fetcher asks one day at a time
  and checks the `date` column on every row. `core/tbx_api.py`'s `report()`
  still takes a range, so **historical backfills through it are not currently
  trustworthy**; group by `date` and check what came back.
- **All-zero rows are dropped by the platform.** Two reports built with
  different metric sets are not row-comparable. Grand totals are unaffected.
- **`PROTECTED_FLOOR_MINIMUMS` is empty.** Until it is populated with TBX's own
  IDs, only the $0.01 zero-out guard and the ±25% delta cap apply — enough to
  stop a zero-out, not enough to stop a $0.05 write on a $1.70 contract floor.
