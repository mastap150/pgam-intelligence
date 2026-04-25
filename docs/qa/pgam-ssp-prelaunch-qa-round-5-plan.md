# PGAM SSP Pre-Launch QA — Round 5 Plan

**Date:** 2026-04-24
**Scope:** Full pre-launch QA cycle. Rounds 2/3/4 closed all P0/P1 blockers; Round 5 is a wider, denser pass intended to catch what 2-rounds-of-fixes plus a same-day deploy of new code (KV limiter `b61660f`, auction shading `bd59d8c`/`049abc8`) might have surfaced or regressed.
**Target:** `https://app.pgammedia.com` (prod, Vercel)
**DB:** Neon `pgam_direct` (unpooled for DDL, pooled for app)

---

## What's different from Round 4

Round 4 left 4 P2 observations open and had two same-day fixes (FND-040 SQL, FND-041 partner-id resolver). Round 5 widens coverage along three axes:

1. **Route inventory** — Round 4 directly probed ~25 of 67 API routes. Round 5 sweeps **all 67** with a baseline RBAC matrix and goes deep on 12 surfaces never directly probed: `bidder-events/*`, `agentic/v1/tools/*`, `admin/deals`, `admin/blocklist`, `admin/floors/snapshot`, `admin/traffic`, `admin/infra`, `admin/health`, `discrepancy/import`, `discrepancy/our-counts`, `dsps/draft`, `dsps/[id]/secrets/rotate`, `users/*`, `rules/*`.
2. **End-to-end auction lifecycle** — Round 4 verified each layer in isolation. Round 5 runs a full ingestion pipeline: `bidder-events/bid-outcome` → `bidder-events/financial` → `bidder-events/impression` → `/api/reporting/*` and verifies the math identities (`gross ≈ payout + profit`, `margin_pct = profit/gross`) on real ingested rows, not zero-row stubs.
3. **In-flight fix** — close FND-043 (duplicate placement_ref dedup) by adding a Zod `.superRefine` rejection. Round 5 verifies the fix in the same pass.

## Test plan — 19 categories

Each category lists scenario IDs only — execution + evidence go into the Round-5 results report.

### 1. Partner creation from scratch
- 1.1 Publisher create (prebid_s2s, slug + numeric source_kind)
- 1.2 Publisher create (direct_rtb + IP allow-list)
- 1.3 Publisher create (direct_rtb + HMAC) — expect 503 SECRETS_NOT_CONFIGURED
- 1.4 Publisher create with **duplicate placement_ref** — expect 400 after FND-043 fix lands
- 1.5 Publisher create as AM/finance/dsp/anon — expect 403/401
- 1.6 DSP create (auth=none, multi-region)
- 1.7 DSP create (auth=bearer, AWS unwired) — expect 503
- 1.8 DSP create as non-admin — expect 403
- 1.9 Concurrent parallel publisher creates (5 in parallel) — expect all 201, distinct ids, no row corruption

### 2. Partner profile config
- 2.1 GET publisher by id → full wizard payload round-trips
- 2.2 PATCH publisher metadata
- 2.3 GET DSP by id with sibling endpoints
- 2.4 DSP list pagination stable across 3 page boundaries
- 2.5 publisher list as DSP/AM
- 2.6 `/api/admin/publishers/[id]/dsp-policy` GET + PATCH
- 2.7 `/api/admin/publishers/[id]/overrides` GET + POST
- 2.8 `/api/admin/publishers/[id]/placements` add-placement-after-create

### 3. Placement-ID lifecycle
- 3.1 Placement rows written alongside publisher
- 3.2 PATCH placement floor (admin / owning publisher / cross-publisher)
- 3.3 PATCH bad id, missing id (FND-042 regression)
- 3.4 Add placement post-create via dedicated endpoint
- 3.5 placement_ref uniqueness within publisher (post FND-043 fix)
- 3.6 placement_ref same value across different publishers — expect 201 (different publisher_id namespaces)

### 4. Ad-serving readiness
- 4.1 sellers.json round-trip (active publisher → entry; archive → removed; 60-300s edge propagation per FND-045)
- 4.2 `/ads.txt` headers + IAB record format
- 4.3 imp.tagid / imp.ext.pgam.orgId derivation from placement_ref + org_id
- 4.4 `/api/sellers-migration-status` content
- 4.5 `/api/prebid-release-status` content

### 5. Bid request/response validation
- 5.1 `/api/dsps/[id]/test-bid` happy path
- 5.2 `/api/rtb/test-bid` payload validation
- 5.3 Test bid as non-admin — expect 403
- 5.4 Test bid with deliberately failing endpoint URL → catches upstream error gracefully
- 5.5 `/api/dsps/draft` (wizard pre-validation)

### 6. Impression + click tracking
- 6.1-4 All 4 `FORWARDED_EVENT_TYPES` round-trip (Round 4 regression)
- 6.5 `/api/bidder-events/impression` server-side ingest (new — Round 5 first-time)
- 6.6 `/api/bidder-events/bid-outcome` ingest with valid OpenRTB-shaped payload
- 6.7 `/api/bidder-events/financial` ingest → financial_events row
- 6.8 `/api/bidder-events/attention-score` ingest
- 6.9 OPTIONS preflight on each bidder-events endpoint
- 6.10 Bidder-events with bad shape / wrong content-type / bad token

### 7-9. Reporting (partner / placement / DSP / matrix / partner-health)
- 7.1 RBAC matrix across all 11 reporting endpoints × 6 cookie forms
- 7.2 With **real ingested data** from §6 — non-zero `row_count`, `cell_count`
- 7.3 `?from=&to=` window math (1d, 7d, 30d, 90d, future-dated, inverted)
- 7.4 `?metric=` allow-list enforcement against `/api/rbac/allowed-metrics`
- 7.5 Deep-link to `/api/reporting/refresh` (admin-only refresh trigger)
- 7.6 `/api/lld` CSV streaming with large window

### 10. Revenue / CPM / payout / margin
- 10.1 With ingested data: assert `gross ≈ payout + profit` per partner
- 10.2 `blended_margin_pct = pgam_profit / gross_revenue * 100` numerical verify
- 10.3 Margin redaction for AM role (FND-005 regression)
- 10.4 `/api/margin/summary` `?days=` boundaries (1, 7, 30, 90, 0, -1, 1000)

### 11. Discrepancy + reconciliation
- 11.1 `/api/discrepancy/import` POST with valid CSV → reconciliation row
- 11.2 `/api/discrepancy/compare` after import → diff payload
- 11.3 `/api/discrepancy/our-counts` for the same window
- 11.4 RBAC: only admin/finance can import; AM/pub/dsp 403

### 12. Troubleshooting / logs
- 12.1 `/live` page renders + `/api/live/snapshot` fresh data
- 12.2 `/api/admin/health` returns DB ping + lambda warm/cold flag
- 12.3 `/api/admin/infra` returns env hints
- 12.4 `/auctions/[id]` with valid + invalid id (anti-enumeration check)
- 12.5 `/api/dsps/health` and `/api/dsps/[id]/health-timeseries`

### 13. Error-message clarity
- 13.1 Each error code surfaced this round has a human-readable detail field
- 13.2 No stack traces leak through 500 responses

### 14. RBAC / permissions
- 14.1 6×67 matrix (admin/finance/am/pub/dsp/anon × 67 routes) — capture every status code
- 14.2 MFA-required surfaces honored
- 14.3 Cross-tenant probes (manufacture cookie with tenantId=99) — expect 403/404, never 500

### 15. Edge cases + invalid setups
- 15.1 Boundary validations on every numeric field (rev_share, floor, qps_limit, take_rate_pct)
- 15.2 placement_ref dedup post-fix
- 15.3 Cross-publisher placement_ref reuse (different publishers, same ref → OK)
- 15.4 SQL-injection regression
- 15.5 Unicode in name (FND-044 regression)
- 15.6 Bad signature cookie / expired cookie / no cookie
- 15.7 Concurrency: parallel PATCH on same placement
- 15.8 Body-size cap (10 MB+ payload) → expect 413 or graceful 400
- 15.9 Long URL query string (50 KB) → expect 414 or 400
- 15.10 `?days=NaN` / `?days=abc` / `?days=999999`
- 15.11 Cookie tampering: alter role to `internal_admin` and re-submit (HMAC must reject)

### 16. Dashboard data flow + exports
- 16.1 All 30 UI pages 200 as admin (no console errors)
- 16.2 LLD CSV download integrity (header row + content-disposition)
- 16.3 Reports page exports
- 16.4 Combo-lists, prebid-test, agentic UI pages

### 17. Reporting latency
- 17.1 Wall-clock budgets per endpoint with non-zero rows
- 17.2 Cold-start p95 vs warm p50

### 18. External integrations
- 18.1 sellers.json + ads.txt regression
- 18.2 AWS Secrets Manager fail-closed (FND-020 regression)
- 18.3 Prebid docs PR status (informational)
- 18.4 Upstash KV runbook lint (no broken commands)

### 19. Public surfaces & security
- 19.1 HTTPS enforce + HSTS + 308 from http
- 19.2 Cookie attributes: `Secure`, `HttpOnly`, `SameSite=Lax`/`Strict`, `Path=/`
- 19.3 Rate-limit headers on `/api/auth/me` (KV-backed post `b61660f`)
- 19.4 sellers.json cache-control (FND-045 regression)
- 19.5 Open-graph / favicon / robots.txt sanity

### 20. New for Round 5 — auction shading regression
- 20.1 With shading code (`bd59d8c`, `049abc8`) deployed, verify it's still inert (no behavior change vs Round 4)
- 20.2 `/api/admin/floors/snapshot` returns shading data without error

---

## Execution order

1. Plan committed (this doc)
2. Fix FND-043 in flight (small Zod superRefine + test)
3. Reactivate test publisher + seed bidder-events ingestion data
4. RBAC matrix sweep (cheap broad coverage)
5. Deep probes on never-tested surfaces
6. Math-identity verification on ingested data
7. Discrepancy import flow
8. Concurrency / boundary / cookie tampering
9. Regression sweep on all 24 prior FNDs
10. Cleanup
11. Final report at `docs/qa/pgam-ssp-prelaunch-qa-round-5.md`

## Out of scope

- Prebid.js adapter regression (no behavior change since Round 3 lock)
- Prebid docs PR (#6543) is upstream-blocked, no action this round
- AWS Secrets Manager wire-up itself (covered by `wire-aws-secrets-manager.md` runbook; Round 5 only verifies the fail-closed path)
- Upstash KV wire-up itself (covered by `wire-upstash-rate-limiter.md` runbook)

## Acceptance criteria

- All 24 prior FNDs remain GREEN
- No new P0 or P1 findings opened (or any opened are fixed in-session)
- Math identities hold on at least one fully ingested auction
- 67-route RBAC matrix has zero 500s (every status code is explainable)
- Test data fully purged, sellers.json back to 1 INTERMEDIARY row
- Final report shipped at `docs/qa/pgam-ssp-prelaunch-qa-round-5.md`

If any of the above fail, recommendation flips from "ship" to "block until resolved."
