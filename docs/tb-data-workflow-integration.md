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

> **Superseded by §11.** This section describes the plan while both hosts were
> live. The legacy host has since gone quiet, which turned the switch from a
> whole-surface mode into a per-day decision. Read §11 for what shipped; this
> stays because the failure modes it lists are still the ones to look for.

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
4. ~~**Auto-revert**~~ — **built**, see §9. It is what makes the rest of the
   fleet safe to run unattended, and nothing here should move to
   `PGAM_OPTIMIZER_AUTO_APPLY=1` before it has been watched through at least
   one real write-and-review cycle.
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
  recent 5 days — no error, no flag. Every caller that lands data now chunks
  by day and checks the `date` on each row: the recon fetcher, and as of this
  branch `agents/etl/tbx_revenue_etl` too (it previously asked for its whole
  14-day window in one call, so it would have landed 5 days and reported
  success). **`core/tbx_api.report()` still takes a range and is still
  truncated** — it is the raw client and the range is the platform's own
  parameter. Anything built on it that spans more than a couple of days must
  do its own chunking; group by `date` and check what came back.
- **All-zero rows are dropped by the platform.** Two reports built with
  different metric sets are not row-comparable. Grand totals are unaffected.
- **`PROTECTED_FLOOR_MINIMUMS` is empty.** Until it is populated with TBX's own
  IDs, only the $0.01 zero-out guard and the ±25% delta cap apply — enough to
  stop a zero-out, not enough to stop a $0.05 write on a $1.70 contract floor.

---

## 9. Auto-revert — the net under the optimizer

`agents/optimization/tbx_auto_revert.py`. Scheduled daily at 10:15, half an
hour after the optimizer, so a write and its review never land in one tick.

It re-reads the geo-floor writes `tbx_demand_geo_floor` made, measures what
happened to each DSP afterwards, and restores the pre-change floors if the
change did harm.

### The measurement grain is the whole design

The LL agent this mirrors compares **hourly** revenue and can act six hours
after a bad write. The TBX report has no `hour` attribute — `date` is the
finest grain the platform offers — and today is never settled. So:

```
write lands on day D
day D is partial            -> unusable
day D+1 settles overnight   -> first usable post-day
run on day D+2              -> earliest possible revert
```

**Two days, against six hours on LL.** Three things follow, and they are the
reason this section is longer than the agent deserves:

- `MIN_POST_DAYS` is 2, not 1. A single day against a 7-day baseline is mostly
  day-of-week noise, and a false revert is itself a harmful write.
- **The forward agent's caps are what actually bound the damage** — the ±25%
  delta cap, `FLOOR_PCT` below 1.0, the per-run source cap. Do not loosen any
  of them on the theory that auto-revert will catch it. Over a two-day
  detection window it will not catch it; it will only end it.
- If Teqblaze ever exposes an hour attribute, the day constants become hour
  constants and this gets much sharper. That is a question worth adding to
  `teqblaze-new-platform.md` §8.1.

### What counts as harm

Either trigger is enough, both measured as per-settled-day rates so an uneven
window length cannot skew them:

| trigger | threshold | why this one |
|---|---|---|
| profit rate drop | >20% below pre | profit is what the floor is *for* — `dsp_price_sum − ssp_price_sum` |
| impression rate drop | >50% below pre | a floor that zeroes a DSP is harm even if the survivors are profitable |

Sources with less than $50 of profit across the 7-day baseline are left alone
— too small to distinguish a real drop from noise.

A DSP that produces **no rows at all** post-change reads as *zero*, not as
missing data. That is deliberate and it is the case that matters most: the
platform drops all-zero rows, so a total wipeout looks exactly like an absent
partner. Treating it as no-data would make the worst outcome the one the agent
cannot see.

### What it refuses to do

- **Revert a write it did not make.** Only `tbx_demand_geo_floor` writes are
  candidates. A human's manual floor change is theirs.
- **Revert twice, or revert its own reverts.** Each revert writes an
  `auto_revert_link` ledger entry carrying `reverted_from`, and the next run
  reads it. That is also why `core/tb_ledger.record` now stamps an `id` —
  entries written before it exists fall back to a fingerprint via
  `tb_ledger.entry_key`.
- **Clobber a third party.** The revert restores a whole snapshot
  (`replace=True`), which is the only way to undo a country the forward run
  *added*. That makes it dangerous if anyone else has written to the same
  demand source since — so if anyone has, the agent escalates to Slack instead
  of writing.
- **Override a partner freeze.** `set_demand_geo_bid_floors` refuses frozen
  partners and this agent does not route around it — but a freeze blocking a
  revert is reported loudly, because the harm is still live and now needs
  hands.

### Gates — deliberately not the optimizer's three

```
--apply              on the command line (default: propose only)
TBX_ALLOW_WRITES=1   the platform-wide write gate
```

**`PGAM_OPTIMIZER_AUTO_APPLY` intentionally does not gate this agent.** That
gate authorises taking *new* positions. A revert only restores one the
platform was already in and a human already lived with. Gating the net behind
the accelerator means that closing the accelerator mid-incident — the exact
reflex someone has on seeing a bad write — also disables the thing that undoes
it. `TBX_ALLOW_WRITES` stays the master switch: with it off, nothing here
writes either.

One property worth knowing rather than rediscovering: **the delta cap can
never trap a revert.** Undoing a raise of `(1+d)` requires a cut of
`d/(1+d)`, which is strictly smaller than `d` for any positive `d`. A
single-step revert of a capped raise always fits inside the same cap.
`test_tbx_auto_revert_is_always_within_the_delta_cap` pins it. The one clamp
that *can* bite on the way back is `GLOBAL_MIN_FLOOR` raising a prior $0.00 to
$0.01; the agent flags that run as `inexact` rather than reporting a clean
revert.

---

## 10. Backfilling TB data from 21 Aug

Both `/admin/pnl` and the SSP recon sheet lose their TB column from **21
August**. Measured on 2026-08-24, the three missing days hold about **$20.1k
of gross** on the new platform — see the reachability table below. The data is
there; nothing has been reading it.

### Why it stopped on the 21st

**The traffic moved and nothing was reading where it moved to.** The legacy
host is retired, `api.pgammedia.com` carries the marketplace now, and
`tbx_revenue_etl` — the one job pointed at the new platform — crashes on every
grain (see the two bugs below). Legacy runs full through the 20th, TBX runs
full from the 20th; the column went blank at the seam.

An earlier draft of this section blamed `TB_ACCESS_TOKEN` ageing out, on the
strength of the date matching. That was wrong, and it is a useful wrong: the
static-token path is genuinely fragile (Teqblaze suspended `POST /create_token`
for our login on 2026-05-11, so it is hand-minted roughly monthly —
`core/tb_api.py`, `get_token`), which made it a plausible culprit. But a dead
token and a completed cutover produce the same blank column on the same day,
and only the reachability data separates them.

**There is no legacy repair to do.** Do not mint a token and do not run the
`tb_*` ETLs against `ssp.pgammedia.com` — that host is gone, and the days in
question were never on it. The whole repair is on the TBX side.


### TBX backfill — the whole repair

```bash
# prove the days are reachable first — writes nothing
python3 -m agents.etl.tbx_revenue_etl --from 2026-08-21 --dry-run

# then land them
python3 -m agents.etl.tbx_revenue_etl --from 2026-08-21
```

Or without a shell: **Actions → "TBX backfill (land daily revenue into Neon)"**,
`date_from = 2026-08-21`, `dry_run` on for the first pass. That workflow only
appears in the Actions tab once this branch is merged — GitHub offers
`workflow_dispatch` for workflows on the default branch only.

**This command did not work before this branch**, in two separate ways, and
both are worth knowing because both failed quietly:

1. It passed `tbx.report(...)`'s `(rows, totals)` tuple straight into
   `_aggregate`, which iterates it expecting dicts — `AttributeError: 'list'
   object has no attribute 'get'`, caught by the per-grain handler, logged as
   a grain failure. Every grain, every run. **The job has never landed a
   row.**

   This is the whole reason `pgam_direct.tbx_daily_*` is empty, and it is
   worth being precise about, because the record says otherwise: PR #106's
   commit message states "with TBX credentials now in Render the ETL is
   landing rows". The credentials part is right — see below — but the landing
   part was assumed, not checked. The rows never arrived, and the per-grain
   handler turned a hard crash into three log lines an hour that nobody was
   reading.

2. It asked for its whole 14-day window in one call. The platform answers 200
   and returns the most recent ~5 days, silently. So even once fixed, a
   `--backfill 30` would have landed 5 days and printed success, and the 25
   missing days would have read as 25 days of zero revenue.

3. **Every row was then dropped as unresolvable.** With the first two fixed,
   a dry run pulled 12,830 rows across the three grains and turned them into
   **zero** records. `_entity` looked for an id — an `{id, name}` object, a
   flattened `x_id` column, a numeric scalar — and the report has none of
   them. A row is:

   ```
   {'date': '2026-08-21', 'placement': '01net.it_300x250 #8766'}
   ```

   The dimension is a display **name** with the entity id appended as a
   `#NNNN` suffix. That is the vendor's own convention — the API reference
   shows the same form for a source, `"Magnite - RON Prebid Server In App
   #1752"` — so it was documented all along and simply not implemented.

All three are fixed: the tuple is unpacked, `_fetch_daily` asks one day at a
time and discards any row whose `date` is not the day requested rather than
attributing it to the wrong day, and `_entity` parses the trailing `#NNNN`.
Only a *trailing* suffix counts, so a `#` inside a partner name is not
mistaken for an id.

### Verified end to end, 2026-08-24

A dry run for 21 Aug on a runner, after the fixes:

```
[tbx_revenue_etl] placement: 2554 row(s) -> 2554 upserted, gross $5,586.71
```

Nothing dropped, and **$5,586.71** against the **$5,587.11** the reachability
probe independently reported for that day — 0.007%, which is rounding across
2,554 rows. Two different code paths agreeing on the platform's own total is
the check that the ids resolved and no row went missing; it costs no extra
call, which is why the dry run prints per-day gross.

Lesson worth keeping: each of these three bugs hid the next. The tuple crash
masked the truncation, and fixing both revealed the parse. Nothing short of
running it against the live account would have found them, and the first two
were each "obviously the bug" at the time.

### The credentials already exist

Verified 2026-08-24 by dispatching `tbx-probe.yml` on this branch: the
"Verify credentials are present" gate **passed**, and the connectivity probe
against `api.pgammedia.com` **succeeded**. So `TBX_EMAIL` and `TBX_PASSWORD`
are live GitHub Actions secrets, the login works, and the platform answers.

Earlier notes in this branch said the credentials were "in neither secret
store". That was wrong. It matters in the right direction: nothing is waiting
on a credential handover, and the ETL bug above was the only thing standing
between the platform and the warehouse.

### Measured 2026-08-24: what the platform will actually serve

`python3 scripts/tbx_probe.py --reach-from 2026-08-10 --reach-to 2026-08-23`,
run on a GitHub runner (`tbx-probe.yml`, `reach_from` input). One single-day
report per day. Result:

| day | rows | impressions | gross (`dsp_price_sum`) |
|---|---|---|---|
| 2026-08-10 … 08-16 | — | — | none |
| 2026-08-17 | yes | 40,166 | $5.79 |
| 2026-08-18 | yes | 57,763 | $29.91 |
| 2026-08-19 | yes | 33,436 | $13.76 |
| 2026-08-20 | yes | 3,966,503 | $2,605.45 |
| 2026-08-21 | yes | 6,325,162 | **$5,587.11** |
| 2026-08-22 | yes | 7,782,718 | **$10,796.13** |
| 2026-08-23 | yes | 6,470,029 | **$3,763.56** |

**21 August is reachable, and it holds real money.** The three missing days
are roughly **$20.1k of gross** sitting in the platform and absent from the
P&L. Nothing about the backfill is blocked; it just has to be run.

### What this says about the truncation window — and about the 21st

The earlier worry was that the ~5-day window might be anchored to *today*,
putting the 21st out of reach around the 26th. The measurement says the
reachable span is 17–23 Aug, seven days ending yesterday, with everything
before it empty. Taken at face value that is an anchored-to-today window of
about a week, and the 21st would age out around the 28th.

But the shape argues for a different reading. A truncation window has a hard
edge — full data, then nothing. What is actually there is three days of
*trickle* (40k, 58k, 33k impressions, single- and double-digit dollars)
followed by a 100× jump to millions of impressions on the 20th. That is not
an edge, it is a **ramp**, and it lines up exactly with the other half of the
picture: the legacy P&L column runs full until the 20th and goes blank from
the 21st.

So the better explanation is a **cutover**, not an expiry:

```
legacy  ████████████████████████░░░░░░░   full through 08-20, then nothing
TBX     ░░░░░░░░░░░░░░░░░░▁▁▁████████     trickle 08-17..19, full from 08-20
```

Traffic moved to `api.pgammedia.com` around 20–21 August. The 17th–19th are
migration trickle; the days before that are empty because there was no
traffic on this platform yet, not because the platform refuses to serve them.
Under that reading there is no deadline on single-day requests at all.

**Both readings point at the same action**, which is why this is recorded
rather than resolved: back the days up now. What separates them, if anyone
needs to know later, is one call — re-run the probe over the same range in a
week. If 17–19 Aug have gone empty, it is a rolling window; if they still
answer, it was a cutover and history is permanently available.

---

## 11. Repointing the surfaces — what the cutover actually forced

§6 above was written while both hosts served the same marketplace and the
question was *when* to switch. That question is closed: `ssp.pgammedia.com`
answers nothing, so `compare` has no second reading to compare against and
`legacy` names a host that is gone. Both are kept as modes — they still
describe something real about the history — but the default in both repos is
now `tbx`, and the interesting problem turned out to be a different one.

### One rule, three places

Flipping a mode switch resolves *the whole surface* to one host. The data
does not work that way. The cutover is a boundary inside the date range every
one of these surfaces reads, so the choice has to be made **per day**:

```
day <  2026-08-20   legacy only          (TBX has migration trickle, not revenue)
day == 2026-08-20   legacy + TBX summed  (real traffic on both)
day >= 2026-08-21   TBX only             (legacy served nothing)
```

That rule now exists in three implementations, which is two more than ideal
but each reads from a different store:

| Surface | Lives in | Reads |
|---|---|---|
| Slack revenue alert | `core/tb_unified.py` | both Neon rollups |
| `/admin/pnl` | `pgam-recon` `pnl_sync._resolve_tb` | both Neon rollups |
| `/admin/finance` | `pgam-recon` `fetchers/tb_legacy_rollup.py` + `fetchers/tbx.py` | legacy rollup + live TBX |

The window is env-overridable in all three (`TB_SPLIT_START`,
`TB_TBX_CUTOVER`) so a corrected boundary is a variable change, not a deploy.
They agree on the defaults; if you move one, move all three.

The recon is the odd one out because it fetches TBX **live** rather than from
the rollup — it needs demand-partner grain the hourly ETL does not land. So
its legacy leg reads `pgam_direct.tb_daily_demand_revenue` while its TBX leg
calls the platform. Same rule, two different sources, which is exactly why
`TBXFetcher.fetch()` had to learn to return `[]` before the split start
instead of asking a platform that would answer with trickle.

### The mistake worth not repeating

Repointing the P&L overwrote four days — 17–20 Aug — of real legacy figures
with TBX's migration trickle before anyone noticed. It was caught by the
operator looking at the sheet, not by anything in this repo.

The cause was reading a docstring instead of the SQL under it. `pnl_sync`'s
upsert is documented as filling NULL cells without overwriting existing
values; the statement is `COALESCE(EXCLUDED.x, existing)`, which means the
**new** value wins whenever it is non-null. A $40 trickle day is not null.

Two things follow, and both are now in the code:

- A day before the cutover never resolves to TBX, so there is nothing to
  overwrite it with. That is the guard.
- When the legacy leg is unreachable for a day it should have served, the
  row is flagged `⚠ LEGACY LEG MISSING — understated` rather than written
  quietly. A short number that says it is short is recoverable; a short
  number that looks complete is not.

All four days were restored and re-verified. 20 Aug reads $7,505.66 — the sum
of both legs, not either one.

### Still open

- **`ent_payout`** fails with `password authentication failed for user
  'neondb_owner'`. That is a credential, not code; no change in either repo
  reaches it. It needs a fresh Neon DSN.
- **Revenue and margin both step down at the boundary — and as of 2026-08-25
  the pipeline is ruled out as the cause.** See §12.

---

## 12. The step-down is real — measured against the platform, 2026-08-25

§11 left two readings open: a genuine post-migration decline, or a TBX ETL
missing supply sources. A `tbx-probe --reach-from 2026-08-17 --reach-to
2026-08-25` read the platform directly, one request per day, and settles it.

| day | platform gross | Neon holds | platform imps |
|---|---|---|---|
| 2026-08-20 | 2,605.45 | 2,605.46 | 3,966,503 |
| 2026-08-21 | 5,587.11 | 5,587.06 | 6,325,162 |
| 2026-08-22 | 10,796.13 | 10,796.09 | 7,782,718 |
| 2026-08-23 | 3,763.56 | 3,763.55 | 6,470,029 |
| 2026-08-24 | 3,742.93 | 3,742.75 | 6,752,225 |

**The ETL is sound.** Five settled days agree to the cent. Nothing is being
dropped, and the decline downstream of it is the marketplace, not the
pipeline.

### Correction: a day is not settled at midnight UTC

§11 as first written called 2026-08-24 "a complete day" at $2,863.54. It was
not complete — that reading was taken at 01:27 UTC, and the report timezone
is `US/Eastern`, so the day had four hours left to run. The hourly ETL
restated it to $3,742.75 as the remaining hours landed, which is the correct
behaviour.

The mistake is worth keeping because it is built into the schedule, not into
one bad reading. `recon-daily` fires at 10:13 UTC and chains `pnl-sync`; that
is safely past the 04:00/05:00 UTC close. But nothing *enforces* it, any
manual run before the close silently books a partial day as final, and the
figure is 24% low at 21:00 ET. See §13 item 3.

### What the numbers actually say

| day | source | gross | profit | margin | imps | CPM |
|---|---|---|---|---|---|---|
| 08-17 | legacy | 8,077.50 | 2,463.36 | 30.5% | 9,768,515 | $0.83 |
| 08-18 | legacy | 7,975.46 | 2,473.11 | 31.0% | 9,686,876 | $0.82 |
| 08-19 | legacy | 8,011.13 | 2,464.24 | 30.8% | 8,713,946 | $0.92 |
| 08-20 | both | 7,505.66 | 2,115.98 | 28.2% | 9,689,743 | $0.77 |
| 08-21 | tbx | 5,587.06 | 1,232.53 | 22.1% | 6,325,162 | $0.88 |
| 08-22 | tbx | 10,796.09 | 2,201.91 | 20.4% | 7,782,718 | $1.39 |
| 08-23 | tbx | 3,763.55 | 905.39 | 24.1% | 6,470,029 | $0.58 |
| 08-24 | tbx | 3,742.75 | 781.56 | 20.9% | 6,752,225 | $0.55 |

Daily profit fell from ~$2,467 (17–19 Aug) to ~$840 (23–24). Decomposing the
$1,685 gap on the 24th against the legacy baseline:

| effect | contribution | share |
|---|---|---|
| volume — 9.39M → 6.75M imps | −$694 | 41% |
| price — $0.859 → $0.554 CPM | −$630 | 37% |
| margin — 30.6% → 20.9% | −$363 | 22% |

Each points somewhere different, which is why they are worth separating:

- **Volume** is 2.6M impressions a day that stopped arriving at the cutover.
  Supply that did not survive the migration is the first thing to rule out —
  it is the most recoverable of the three.
- **Price** is the marketplace, and the hardest to act on directly. Floors are
  the lever, and the tooling for that already exists and is gated shut.
- **Margin** is the interesting one. It fell ~10 points and *stayed* there,
  and it does not track price or volume — 08-22 had the best CPM of the period
  and the second-worst margin. A take rate that moves independently of what
  the inventory sells for points at revenue-share configuration on the new
  platform, not at market conditions. That is a settings question with a
  definite answer, worth ~$364/day at current volume.

**08-22 is unexplained and should not be averaged away.** 7.78M impressions
at $1.39 CPM against $0.55–0.62 on either side. Either a repeatable demand
pattern worth finding, or a reporting artifact worth discounting; nothing
currently distinguishes them.

---

## 13. What to automate next

Ordered by what the numbers in §12 actually justify, not by what is easiest.
Each names the existing piece it builds on — none of this is greenfield.

### 1. Migration-gap watchdog — BUILT 2026-08-25, and it found something

`scripts/tbx_supply_gap.py` + `.github/workflows/tbx-supply-gap.yml`. First
clean run over 17–20 Aug against 21–24 Aug:

| publisher | imps/day before | after | gross/day lost |
|---|---|---|---|
| Dexerto Display | 2,285,965 | 30,429 | $1,916.49 |
| Illumin Display and Video | 449,044 | 70 | $358.38 |
| Smaato - Display and Video | 229,245 | 33,280 | $323.09 |
| Illumin Display 3 nodes | 177,732 | 28 | $177.33 |
| Smaato - Zeta Display and Video | 142,875 | 11,337 | $170.34 |
| Illumin Zeta Display and Video | 58,145 | 0 | $80.56 |
| Start.IO Video | 92,200 | 4,477 | $74.57 |
| Cas.ai Display | 11,956 | 2,242 | $43.59 |
| Dexerto Video | 34,639 | 341 | $21.34 |

20 sources carried over cleanly, 1 gone, 9 collapsed. **$3,877/day gross,
~$1,186/day profit** — and **Dexerto Display alone is ~85% of the lost
impressions.** That is one name to chase, not a diffuse decline.

It also answers the supply half of §8.1.10d empirically: **29 matched, 0
moved.** Supply ids are stable across the two hosts. Teqblaze never committed
to that, so it is evidence rather than a guarantee, but it is more than we
had.

Two things this run taught, both now in the script's docstring:

- **The legacy report hides the id in the display name.** `publisher_name` is
  `'Smaato - Display Stirista Premium #190'` and the legacy ETL stores that
  same string in `publisher_id`, so that column carries no id at all. TBX
  splits them properly. The first version of this script matched zero names
  and confidently reported 30 publishers gone and $7,241/day at risk; both
  numbers were artifacts of the failed join. Establish the key from the data
  before believing anything built on it.
- **The 357 "new on TBX" names are not 357 new publishers.** Most are
  domain-level entries (`decoist.com`, `outdoorrevival.com`) — TBX breaks
  supply out finer than legacy did. Before chasing any of the collapsed
  sources above, rule out that its traffic reappeared under a
  finer-grained name.

### 1b. Original reasoning — the 41% of the loss with a recoverable cause

2.6M impressions a day stopped arriving at the cutover. The question nobody
has asked in a form a machine can answer is *which publishers*. Every supply
source that earned on `tb_daily_publisher_revenue` before 08-21 and has no
row in `tbx_daily_supply_revenue` after it is inventory that did not survive
the migration.

`scripts/tbx_recon.py` already joins the two legs on name — this is that join
with the direction reversed, asking who is missing rather than who disagrees.
Schedule it daily until it reads clean, then delete it.

Worth roughly $700/day of profit at legacy economics, and unlike the other
two effects it is a fix rather than a negotiation. **Do this one first.**

### 2. Take-rate sentry, per supply source

Margin fell ~10 points at the cutover and held, independent of price and
volume (§12). Whole-book margin cannot say whether that is every publisher
moving a little or a few moving a lot, and those need different responses.

Alert when a supply source's daily margin deviates more than 3pp from its own
trailing 14-day median. `tbx_daily_supply_revenue` already carries gross and
payout per source per day, so this is a query and a Slack post. It would have
fired on 2026-08-21.

### 3. Refuse to book an unsettled day

The §12 correction, made structural. A day is not final until the
`US/Eastern` close, and at 21:00 ET it reads ~24% low.

- `pnl_sync` should decline to write a day whose ET close has not passed,
  rather than booking a partial as final.
- The Slack alert should label a same-day figure `partial`.
- `tb_unified` should expose settled-ness so both get it from one place, the
  way they already get the per-day host rule from one place.

No revenue in this one — it is the difference between a number being wrong
and a number saying it is provisional. It caught me out yesterday and the
schedule only avoids it by luck.

### 4. Daily platform-vs-warehouse assertion

§12 took a manual probe dispatch and a wait. It should be a line in a log
every morning: one single-day report per settled day, compared against Neon,
alerting past 1%. `--reach-from` is already exactly this call; it needs the
comparison and a schedule.

The value is not catching a broken ETL — it is being able to tell "the
pipeline broke" from "revenue fell" in seconds. Those look identical on a
dashboard and have nothing in common as problems.

### 5. Explain the outliers automatically

`scripts/tb_whatchanged.py` does revenue-change attribution and nothing calls
it on a schedule. Trigger it on any day more than 40% from the trailing
median and post the top movers. 08-22 has been sitting unexplained since it
happened.

### 6. Floors — later, deliberately

`tbx_demand_geo_floor` and `tbx_auto_revert` are built and gated shut, and
CPM being down 35% is exactly the case floors address. Do not open the gate
yet:

- The auto-revert needs two settled days to measure harm (the TBX report has
  no `hour` attribute), so the blast radius of a bad write is two days wide.
- The book is still moving from the migration. Tuning floors against a
  marketplace that is changing underneath the agent means the baseline it
  measures against is not a baseline.

Revisit once item 1 has landed and the book has been stable for a week. The
prerequisites for `TBX_ALLOW_WRITES=1` are in
`docs/teqblaze-new-platform.md` §6.
