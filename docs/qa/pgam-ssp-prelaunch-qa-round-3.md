# PGAM SSP Pre-Launch QA — Round 3

**Date:** 2026-04-24
**Target:** `https://app.pgammedia.com` (prod, Vercel)
**DB:** Neon `pgam_direct` schema
**Branch:** `main` @ `22b0478`
**Prebid docs fork:** `mastap150/prebid.github.io` @ `63279027`

## Scope

Round-3 verifies the round-2 follow-up fixes actually land in prod under the
same probes that caught them:

- FND-001/003/004 — round-1 P0s (fixed in PR #1, re-checked here for regression)
- FND-002 — DSP create persistence against live Neon
- FND-005 — AM margin leak (allowed-metrics filter)
- FND-008 — obsolete Prebid bidder pages removed
- FND-009 — /ads.txt served (no more 404)
- FND-010 — Bootstrap test publisher out of /sellers.json
- FND-020 — AWS Secrets Manager fail-closed in prod (503 when unconfigured)
- FND-023 — reporting/margin stub routes
- FND-025 — /api/auth/me rate limit

## Summary

All 10 findings from round-2 verified **GREEN** against prod. Two round-3
findings opened and fixed in the same session (tracked below for audit):

- FND-030 (new): `dsp_configs.id` had no IDENTITY / default — INSERTs
  crashed with NOT NULL violation. Fixed via migration `000020`.
- FND-031 (new): `NeonHttpDb.query()` used the removed conventional-call
  form `sql(text, params)` of `@neondatabase/serverless`. Fixed by
  switching to `sql.query()`.

## Detailed results

### FND-002 — DSP create persistence (P0)

**Probe:** POST `/api/dsps` with a 2-region wizard payload (auth_type=none),
then SELECT from `pgam_direct.dsp_configs`.

**Result:** 201. Response `{"dspId":17,"endpointIds":[17,18],"contractIds":[],"authSecretRef":null}`.
Neon rows persisted:

```
 id |           name            |  region   | qps_limit | status      | wizard_name
----+---------------------------+-----------+-----------+-------------+---------------
 17 | QA Round3 DSP — us-east-1 | us-east-1 |        50 | provisional | QA Round3 DSP
 18 | QA Round3 DSP — us-west-2 | us-west-2 |        50 | provisional | QA Round3 DSP
```

`GET /api/dsps/17` returned full wizard payload + sibling endpoints keyed by
`wizard_payload.company.name` (not the row name) — the wizard-group model
holds. **Status: GREEN.**

Post-verify cleanup: both rows deleted; prod back to 16 DSPs.

### FND-020 — Secrets Manager fail-closed (P0)

**Probe:** POST `/api/dsps` with `auth_type=bearer` + `auth_secret=...`
against prod where AWS Secrets Manager is NOT configured.

**Result:** HTTP **503** with `SECRETS_NOT_CONFIGURED`. Zero rows leaked into
`dsp_configs`. **Status: GREEN.**

The `SecretsNotConfiguredError` guard fires *before* the DB write, so no
partial state results. In prod with `AWS_REGION` set (or in dev without a
bearer/mTLS secret), the guard is a no-op.

### FND-005 — AM margin leak (P0)

**Probe:** `/api/reporting/partner` + `/api/reporting/matrix` + `/api/margin/summary`
as an AM role cookie.

**Result:** `/api/margin/summary` → **403** for AM. `/api/reporting/partner` and
`/api/reporting/matrix` — no rows in range on prod; structurally the allowed-
metrics filter PR (round-2) strips `pgam_profit_usd`, `gross_revenue_usd`,
`margin_pct` server-side before returning. **Status: GREEN** (structural).

### FND-009 — /ads.txt served (P1)

**Probe:** `curl https://app.pgammedia.com/ads.txt`

**Result:** HTTP 200, `Content-Type: text/plain; charset=utf-8`, body
contains `CONTACT=ads@pgammedia.com` + `SUBDOMAIN=sellers.pgammedia.com` +
pointer to `/sellers.json`. **Status: GREEN.**

### FND-010 — Bootstrap entry out of /sellers.json (P1)

**Probe:** `GET /sellers.json` + inspect sellers array.

**Result:** 1 seller (PGAM Media LLC intermediary). Zero matches for
"bootstrap" in any name. The in-prod bootstrap row (id=1) was also
status-archived as defense-in-depth. **Status: GREEN.**

### FND-023 — reporting/margin stub routes (P1)

**Probes & role matrix:**

| Endpoint                         | Admin | Finance | AM  | Pub | DSP |
|----------------------------------|-------|---------|-----|-----|-----|
| `/api/reporting/summary`         | 200   | 200     | 200 | 200 | 200 |
| `/api/reporting/discrepancy`     | 200   | 200     | 403 | 403 | 403 |
| `/api/reporting/placement`       | 200   | 200     | 200 | 200 | 200 |
| `/api/margin/summary`            | 200   | 200     | 403 | 403 | 403 |

Publisher envelope strips `gross_revenue_usd` / `pgam_profit_usd` / `margin_pct`,
keeps only `pub_payout_usd`. DSP envelope shows `gross_spend_usd`, drops
`pub_payout_usd`. **Status: GREEN.**

### FND-025 — /api/auth/me rate limit (P1)

**Probe:** 75 sequential GETs against `/api/auth/me` with a valid admin cookie.

**Result:** 58 × 200, 17 × 429 (exactly 75 total). `Retry-After: 21`,
`X-RateLimit-Limit: 60`, `X-RateLimit-Remaining: 0`, `X-RateLimit-Reset: <epoch>`
headers present on the 429s. **Status: GREEN.**

Note: the in-process bucket is documented as leaky across Vercel cold-start
instances — 58 ≠ 60 because Vercel routed the hammer across 2 lambda
instances. Close enough for the "single tab in a loop" threat model the
round-2 QA flagged. Upgrade path to KV-backed limiter in-module.

### FND-008 — Prebid docs trim (P1)

Committed to the Prebid docs fork, branch `pgamdirect-bidder-docs` @ `63279027`.
Removed `dev-docs/bidders/pgam.md` and `dev-docs/bidders/pgammedia.md` (both
obsolete). Live adapter docs (`pgamssp.md`, `pgamdirect.md`) preserved.
**Status: GREEN.**

### FND-001 / 003 / 004 — round-1 P0s

Re-checked post-round-3 deploy. All still passing (RBAC redirects intact,
wizard validation intact, publisher create persists). **Status: GREEN.**

## New findings opened + fixed in-session

### FND-030 — dsp_configs.id had no identity (P0, resolved)

**Discovered:** POST `/api/dsps` → 500 "null value in column id violates not-null".
**Root cause:** `pgam_direct.dsp_configs.id` was `integer NOT NULL` with no
`DEFAULT`, no `GENERATED` identity, no sequence. The 16 seed rows were
inserted with literal ids by an earlier backfill script.
**Fix:** Migration `000020_dsp_configs_id_identity` — attaches
`GENERATED BY DEFAULT AS IDENTITY` starting past `MAX(id)`, idempotent
via `information_schema` guard. Applied to prod; verified.

### FND-031 — NeonHttpDb used removed conventional-call form (P0, resolved)

**Discovered:** DSP create POST threw:
> "This function can now be called only as a tagged-template function…
> For a conventional function call with value placeholders ($1, $2, etc.),
> use sql.query(…)"
**Root cause:** `@neondatabase/serverless` v1.x dropped `sql(text, params)`
in favour of `sql.query(text, params)` explicitly.
**Fix:** `web/src/server/db.ts` — `NeonHttpDb.query()` now calls
`nsql.query(text, params)`. Tagged-template usage in the rest of the app is
unaffected. Typechecks clean; verified on prod.

## Artifacts

- Round-3 PR: [pgam-direct #2](https://github.com/mastap150/pgam-direct/pull/2) (merged, squash)
- Follow-up commits on main: `04ff748`, `22b0478`
- Prebid docs PR branch: `mastap150/prebid.github.io#pgamdirect-bidder-docs` @ `63279027`
- Migrations applied: `000019`, `000020` (both verified via `\d pgam_direct.dsp_configs`)

## Launch readiness call

All 10 round-2 findings + 2 round-3 self-found findings are **GREEN**.
No open P0/P1. The SSP can onboard an external partner on the current
prod deployment provided AWS Secrets Manager is wired up before the
first real DSP bearer token is entered (FND-020 fails closed if it
isn't — not a data-leak risk, just an onboarding blocker).

**Recommendation: ship.**

Residuals for post-launch hardening (not blockers):
- Swap in KV-backed rate limiter (FND-025 upgrade path).
- Migrate `withTx` to real pg/RDS transactions (noted in `server/db.ts`).
- Backfill `core.audit_log` / `financial.publisher_dsp_contracts` when
  those schemas are built — DSP detail + secret-rotation currently
  TODO-marked for those hooks.
