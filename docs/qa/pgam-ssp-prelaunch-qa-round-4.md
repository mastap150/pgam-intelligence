# PGAM SSP Pre-Launch QA — Round 4 (Consolidated)

**Date:** 2026-04-24
**Target:** `https://app.pgammedia.com` (prod, Vercel)
**DB:** Neon `pgam_direct` schema (unpooled)
**Branch:** `main` @ `36ff077` (+ any hotfix commits landed this round)
**Tester:** Claude (executing on behalf of Priyesh Patel, PGAM Media LLC)
**Scope:** end-to-end pre-launch QA prior to onboarding the first external partner

## Intent

Round-4 is a **full, experienced-tester pass** across every shipped surface of
the SSP — not a spot-check. It covers the 19 categories Priyesh called out:

1. Partner creation from scratch
2. Partner profile config (rev-share, payout, integration mode, compliance)
3. Placement-ID lifecycle (create → list → patch → delete)
4. Ad-serving readiness (imp.tagid → placements wiring)
5. Bid request/response validation (test-bid flow)
6. Impression + click tracking hooks
7. Reporting — partner level
8. Reporting — placement level
9. Reporting — campaign/endpoint level (DSP side)
10. Revenue / CPM / payout / margin math accuracy
11. Discrepancy + reconciliation surface
12. Troubleshooting tools + logs (rtb-tester, live, bidder events)
13. Error-message clarity (validation + auth failures)
14. RBAC (admin / finance / am / publisher / dsp)
15. Edge cases + invalid setups
16. Dashboard data flow (Neon → API → UI) + CSV exports
17. Reporting latency (stubs are instant; real-data paths flagged)
18. External integrations (ads.txt, sellers.json, Prebid docs, AWS Secrets)
19. Public surfaces & SEO (login flow, 404s, security headers)

## Method

- **Session cookies:** `ENV_FILE=web/.env.vercel node /tmp/mkcookie.mjs <role> [partnerId]`
- **Direct DB reads:** `psql "$UNPOOLED"` (DATABASE_URL with `-pooler` stripped)
- **HTTP probes:** `curl -sS -o /dev/null -w "%{http_code}"` for status, `curl -sS`
  for body
- **Evidence:** captured inline in the Actual Result column (trimmed JSON or
  HTTP codes); DB evidence quoted from `psql` output
- **Results table schema:** Scenario · Steps · Expected · Actual · Status ·
  Evidence · Owner · Priority · Notes

Legend: **GREEN** = pass; **YELLOW** = pass-with-caveat documented in Notes;
**RED** = fail requiring fix before launch; **INFO** = observation, no defect.

---

## 1. Partner creation from scratch

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 1.1 | Publisher create (prebid_s2s, direct source) | POST `/api/publishers` w/ full wizard payload as admin | 201 + publisher row + placement ids + Neon persistence | _pending_ | _pending_ | — | Claude | P0 | Baseline happy-path |
| 1.2 | Publisher create (direct_rtb + HMAC) | POST w/ `integration.mode=direct_rtb`, `auth_hmac_enabled=true` | 201; `auth_secret_ref` populated (or 503 if AWS unwired) | _pending_ | _pending_ | — | Claude | P0 | Validates secret-auth path |
| 1.3 | Publisher create (direct_rtb + IP allow-list) | POST w/ `auth_ip_allowlist=["1.2.3.4/32"]`, hmac off | 201; `auth_ip_allowlist` persisted; no AWS call | _pending_ | _pending_ | — | Claude | P0 | Validates no-secret path |
| 1.4 | Publisher create (direct_rtb, no HMAC, no IPs) | POST w/ `auth_hmac_enabled=false`, empty allowlist | 400 VALIDATION (superRefine rejects) | _pending_ | _pending_ | — | Claude | P1 | Negative — matrix rule |
| 1.5 | Publisher create as non-admin | POST as `am` | 403 FORBIDDEN | _pending_ | _pending_ | — | Claude | P0 | RBAC gate |
| 1.6 | Publisher create unauthenticated | POST with no cookie | 401 UNAUTHENTICATED | _pending_ | _pending_ | — | Claude | P0 | Baseline auth |
| 1.7 | DSP create (auth_type=none) | POST `/api/dsps` w/ 2-region wizard payload | 201 + dspId + endpointIds[] + null authSecretRef | _pending_ | _pending_ | — | Claude | P0 | Re-run FND-002 probe |
| 1.8 | DSP create (auth_type=bearer, AWS unwired) | POST w/ `auth_secret="x"` | 503 SECRETS_NOT_CONFIGURED; zero rows written | _pending_ | _pending_ | — | Claude | P0 | FND-020 regression |
| 1.9 | DSP create as publisher | POST as publisher cookie | 403 FORBIDDEN | _pending_ | _pending_ | — | Claude | P0 | RBAC gate |
| 1.10 | Duplicate slug collision | POST publisher twice with identical name | 2nd returns `org_id_conflict` 409 OR fresh auto-suffixed slug | _pending_ | _pending_ | — | Claude | P1 | Slug dedupe |

## 2. Partner profile config

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 2.1 | GET publisher by id (own) | `GET /api/publishers/{id}` as admin | 200 + full wizard payload | _pending_ | _pending_ | — | Claude | P0 | Read-back |
| 2.2 | GET publisher (cross-tenant) | GET as foreign publisher cookie | 403 or 404 | _pending_ | _pending_ | — | Claude | P0 | Tenant isolation |
| 2.3 | PATCH publisher rev-share | PATCH `/api/publishers/{id}` w/ `financial.rev_share_default_pct=75` | 200 + row mutated + audit log | _pending_ | _pending_ | — | Claude | P1 | Config mutation |
| 2.4 | GET DSP by id | `GET /api/dsps/{id}` as admin | 200 + sibling endpoints grouped | _pending_ | _pending_ | — | Claude | P0 | FND-002 read-back |
| 2.5 | GET DSP list pagination | `?limit=5&offset=0`, then `offset=5` | Stable ordering, no duplicates | _pending_ | _pending_ | — | Claude | P1 | Pagination correctness |
| 2.6 | GET publishers list as DSP | as dsp role | 403 FORBIDDEN (hard-denied before Neon) | _pending_ | _pending_ | — | Claude | P0 | Hard tenant gate |
| 2.7 | GET publishers list as am | as am role | 200 + tenant-scoped list | _pending_ | _pending_ | — | Claude | P1 | Scoping |

## 3. Placement-ID lifecycle

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 3.1 | Placements created alongside publisher | Inspect `placement_ids[]` + Neon `placements` rows | One row per wizard placement, FK to publisher_id | _pending_ | _pending_ | — | Claude | P0 | Wiring check |
| 3.2 | PATCH placement floor | PATCH `/api/admin/placements/{pid}` `{floor_usd: 0.75}` | 200 + row updated | _pending_ | _pending_ | — | Claude | P0 | Config mutation |
| 3.3 | PATCH placement as publisher (own) | as publisher role for owned placement | 200 | _pending_ | _pending_ | — | Claude | P1 | Scoped write |
| 3.4 | PATCH placement as publisher (other's) | as publisher for foreign placement | 403 | _pending_ | _pending_ | — | Claude | P0 | Tenant gate |
| 3.5 | PATCH placement as dsp | as dsp | 403 | _pending_ | _pending_ | — | Claude | P0 | RBAC |
| 3.6 | DELETE placement | DELETE `/api/admin/placements/{pid}` as admin | 200 or 204; row soft/hard-deleted | _pending_ | _pending_ | — | Claude | P1 | Deletion path |
| 3.7 | DELETE placement invalid id | DELETE w/ `pid=abc` | 400 bad_id | _pending_ | _pending_ | — | Claude | P2 | Input validation |
| 3.8 | DELETE placement missing | DELETE nonexistent pid | 404 not_found | _pending_ | _pending_ | — | Claude | P2 | Input validation |

## 4. Ad-serving readiness

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 4.1 | `placement_ref` → `imp.tagid` mapping | Verify created placement's `placement_ref` is the tagid shape | Matches regex, no spaces, URL-safe | _pending_ | _pending_ | — | Claude | P0 | Wire contract |
| 4.2 | Publisher `org_id` → `imp.ext.pgam.orgId` | Verify slug shape on created publisher | Non-empty, lowercase, URL-safe | _pending_ | _pending_ | — | Claude | P0 | Wire contract |
| 4.3 | sellers.json includes new publisher | `GET /sellers.json` after create | New active publisher listed; test/bootstrap filtered | _pending_ | _pending_ | — | Claude | P0 | Supply chain visibility |
| 4.4 | ads.txt served | `GET /ads.txt` | 200 text/plain with CONTACT + SUBDOMAIN + sellers.json pointer | _pending_ | _pending_ | — | Claude | P0 | IAB compliance |

## 5. Bid request/response validation

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 5.1 | Admin test-bid (valid OpenRTB 2.6) | POST `/api/dsps/{id}/test-bid` w/ sample request | 200 + upstream response + timings | _pending_ | _pending_ | — | Claude | P0 | Upstream reachability |
| 5.2 | Admin test-bid (malformed) | POST w/ missing `imp[]` | 400 or structured validation error | _pending_ | _pending_ | — | Claude | P1 | Validation surfacing |
| 5.3 | rtb/test-bid UI surface | `GET /rtb-tester` dashboard | 200 HTML | _pending_ | _pending_ | — | Claude | P1 | Dashboard renders |
| 5.4 | Bidder-events endpoint present | `GET /api/bidder-events` (no params) | 200 or 400 w/ structured error | _pending_ | _pending_ | — | Claude | P2 | Surface present |

## 6. Impression + click tracking

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 6.1 | analytics-events endpoint reachable | POST `/api/analytics-events` w/ sample impression | 200/202 + Neon row (or documented stub) | _pending_ | _pending_ | — | Claude | P1 | Hook wired |
| 6.2 | Click macro wiring | Inspect `/api/bidder-events` emitter for click schema | Documented or stubbed | _pending_ | _pending_ | — | Claude | P2 | Schema sanity |

## 7. Reporting — partner level

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 7.1 | `/api/reporting/partner` admin | GET | 200 + full financial envelope | _pending_ | _pending_ | — | Claude | P0 | Admin view |
| 7.2 | `/api/reporting/partner` finance | GET | 200 + full envelope | _pending_ | _pending_ | — | Claude | P0 | Finance view |
| 7.3 | `/api/reporting/partner` am | GET | 200 + margin fields stripped | _pending_ | _pending_ | — | Claude | P0 | FND-005 regression |
| 7.4 | `/api/reporting/partner` publisher | GET | 200 + only `pub_payout_usd` | _pending_ | _pending_ | — | Claude | P0 | Publisher envelope |
| 7.5 | `/api/reporting/partner` dsp | GET | 200 + `gross_spend_usd` only; no `pub_payout` | _pending_ | _pending_ | — | Claude | P0 | DSP envelope |

## 8. Reporting — placement level

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 8.1 | `/api/reporting/placement` admin | GET | 200 + full rows | _pending_ | _pending_ | — | Claude | P0 | Admin view |
| 8.2 | `/api/reporting/placement` publisher | GET | 200 + only own placements | _pending_ | _pending_ | — | Claude | P0 | Scoping |
| 8.3 | `/api/reporting/placement` dsp | GET | 403 or scoped to DSP's bought inventory | _pending_ | _pending_ | — | Claude | P1 | Tenant gate |

## 9. Reporting — DSP/campaign/endpoint

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 9.1 | `/api/reporting/summary` all roles | GET × 5 roles | 200 × 5 (role-gated fields) | _pending_ | _pending_ | — | Claude | P0 | Smoke matrix |
| 9.2 | `/api/reporting/matrix` admin | GET | 200 + partner × day grid | _pending_ | _pending_ | — | Claude | P1 | Admin cross-view |
| 9.3 | `/api/reporting/matrix` am | GET | 200, margin fields stripped | _pending_ | _pending_ | — | Claude | P0 | FND-005 path |
| 9.4 | `/api/reporting/partner-health` admin | GET | 200 + per-partner health scores | _pending_ | _pending_ | — | Claude | P1 | Observability |
| 9.5 | `/api/reporting/refresh` admin | POST | 200 / 202 (queued) or 204 | _pending_ | _pending_ | — | Claude | P2 | Refresh trigger |

## 10. Revenue / CPM / payout / margin math

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 10.1 | `/api/margin/summary` admin | GET | 200 + full margin struct | _pending_ | _pending_ | — | Claude | P0 | Finance view |
| 10.2 | `/api/margin/summary` finance | GET | 200 + full struct | _pending_ | _pending_ | — | Claude | P0 | Finance view |
| 10.3 | `/api/margin/summary` am | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | AM gate |
| 10.4 | `/api/margin/summary` publisher | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | Publisher gate |
| 10.5 | `/api/margin/summary` dsp | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | DSP gate |
| 10.6 | Math identity: gross = payout + margin | For any non-zero row, assert `gross_revenue_usd ≈ pub_payout_usd + pgam_profit_usd ± ¢` | Identity holds | _pending_ | _pending_ | — | Claude | P1 | Arithmetic sanity |
| 10.7 | margin_pct = profit / gross | `margin_pct` within 0.01 of computed ratio | Identity holds | _pending_ | _pending_ | — | Claude | P1 | Arithmetic sanity |

## 11. Discrepancy + reconciliation

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 11.1 | `/api/reporting/discrepancy` admin | GET | 200 + diff rows | _pending_ | _pending_ | — | Claude | P0 | Recon stub |
| 11.2 | `/api/reporting/discrepancy` finance | GET | 200 | _pending_ | _pending_ | — | Claude | P0 | Finance view |
| 11.3 | `/api/reporting/discrepancy` am | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | AM gate |
| 11.4 | `/api/reporting/discrepancy` publisher | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | Publisher gate |
| 11.5 | `/api/reporting/discrepancy` dsp | GET | 403 | _pending_ | _pending_ | — | Claude | P0 | DSP gate |
| 11.6 | `/discrepancy` dashboard page | GET HTML as admin | 200 | _pending_ | _pending_ | — | Claude | P1 | Dashboard renders |

## 12. Troubleshooting / logs

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 12.1 | `/live` page renders | GET HTML | 200 | _pending_ | _pending_ | — | Claude | P1 | Live view |
| 12.2 | `/rtb-tester` renders | GET HTML | 200 | _pending_ | _pending_ | — | Claude | P1 | Tester UI |
| 12.3 | `/api/live` data stream | GET | 200 (JSON/SSE) | _pending_ | _pending_ | — | Claude | P2 | Live endpoint |
| 12.4 | `/auctions` renders | GET HTML | 200 | _pending_ | _pending_ | — | Claude | P1 | Auction insight |

## 13. Error-message clarity

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 13.1 | Zod validation error shape | POST publisher w/ missing basics.name | 400 + `{error, issues:[{path, message}]}` | _pending_ | _pending_ | — | Claude | P1 | Machine-readable |
| 13.2 | Invalid JSON body | POST w/ `{bad json` | 400 BAD_JSON | _pending_ | _pending_ | — | Claude | P2 | Parser guard |
| 13.3 | AWS 503 message | DSP w/ bearer, AWS unwired | 503 `SECRETS_NOT_CONFIGURED` + detail | _pending_ | _pending_ | — | Claude | P0 | Ops-actionable |
| 13.4 | Unauthenticated error shape | GET admin route no cookie | 401 `UNAUTHENTICATED` | _pending_ | _pending_ | — | Claude | P1 | Consistent shape |

## 14. RBAC matrix

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 14.1 | Full role × endpoint matrix | 5 roles × 12 endpoints | Matches design table | _pending_ | _pending_ | — | Claude | P0 | Captured inline below |

## 15. Edge cases + invalid setups

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 15.1 | Very long publisher name | `name = "a"*121` | 400 VALIDATION | _pending_ | _pending_ | — | Claude | P2 | Bounds |
| 15.2 | Negative floor_usd | floor=-1 | 400 VALIDATION | _pending_ | _pending_ | — | Claude | P2 | Bounds |
| 15.3 | Bad currency | currency="XX" | 400 VALIDATION (len≠3) | _pending_ | _pending_ | — | Claude | P2 | Bounds |
| 15.4 | SQL injection in `name` | name with `'); DROP TABLE` | Parameterized, no damage | _pending_ | _pending_ | — | Claude | P0 | Injection |
| 15.5 | XSS in notes | notes w/ `<script>` | Stored literally; no reflection on read | _pending_ | _pending_ | — | Claude | P1 | XSS |
| 15.6 | Unknown DSP id | GET `/api/dsps/9999999` | 404 | _pending_ | _pending_ | — | Claude | P2 | Not-found |
| 15.7 | Rate-limit burn-through | 75 × `/api/auth/me` | 58-60 × 200 + ≥15 × 429 | _pending_ | _pending_ | — | Claude | P0 | FND-025 regression |

## 16. Dashboard data flow + exports

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 16.1 | `/` root renders (or redirects to login) | GET | 200 or 307 | _pending_ | _pending_ | — | Claude | P0 | Baseline |
| 16.2 | Login page | GET `/login` | 200 HTML w/ form | _pending_ | _pending_ | — | Claude | P1 | Auth surface |
| 16.3 | Dashboard index as admin | GET `/` | 200 | _pending_ | _pending_ | — | Claude | P0 | Dashboard root |
| 16.4 | `/publishers` list page | GET | 200 | _pending_ | _pending_ | — | Claude | P1 | List page |
| 16.5 | `/dsps` list page | GET | 200 | _pending_ | _pending_ | — | Claude | P1 | List page |
| 16.6 | `/reporting` renders | GET | 200 | _pending_ | _pending_ | — | Claude | P1 | Reports |
| 16.7 | `/reports` renders | GET | 200 | _pending_ | _pending_ | — | Claude | P1 | Reports |
| 16.8 | `/compliance` renders | GET | 200 | _pending_ | _pending_ | — | Claude | P1 | Compliance |

## 17. Reporting latency

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 17.1 | Stub latency | `curl -w "%{time_total}" /api/reporting/summary` | <1s | _pending_ | _pending_ | — | Claude | P1 | Baseline |
| 17.2 | Matrix latency | Same vs `/api/reporting/matrix` | <2s | _pending_ | _pending_ | — | Claude | P2 | Baseline |

## 18. External integrations

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 18.1 | ads.txt | GET `/ads.txt` | 200 text/plain | _pending_ | _pending_ | — | Claude | P0 | IAB |
| 18.2 | sellers.json | GET `/sellers.json` | 200 application/json, IAB-compliant | _pending_ | _pending_ | — | Claude | P0 | IAB |
| 18.3 | sellers.json bootstrap filter | grep for "bootstrap" | 0 matches | _pending_ | _pending_ | — | Claude | P0 | FND-010 |
| 18.4 | Prebid docs upstream | Check PR #6543 state | Draft/open w/ removed docs | _pending_ | _pending_ | — | Claude | P1 | FND-008 |
| 18.5 | AWS Secrets wire guard | DSP bearer POST | 503 SECRETS_NOT_CONFIGURED (until AWS wired) | _pending_ | _pending_ | — | Claude | P0 | FND-020 |

## 19. Public surfaces & security headers

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 19.1 | HTTPS enforced | `curl -I http://app.pgammedia.com` | 301/308 → https | _pending_ | _pending_ | — | Claude | P0 | Vercel default |
| 19.2 | Security headers | HEAD `/` | Strict-Transport-Security present | _pending_ | _pending_ | — | Claude | P1 | HSTS |
| 19.3 | 404 surface | GET `/definitely-not-a-route` | 404 | _pending_ | _pending_ | — | Claude | P2 | Not-found |
| 19.4 | Auth me rate-limit headers | GET `/api/auth/me` | `X-RateLimit-*` headers present | _pending_ | _pending_ | — | Claude | P1 | FND-025 |

---

## Execution log

_Results populated section by section below as probes run. Findings with priority ≥ P1 get a FND-0xx ticket inline._

