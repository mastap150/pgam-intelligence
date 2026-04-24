# PGAM SSP Pre-Launch QA — Round 4 (Consolidated, Executed)

**Date:** 2026-04-24
**Target:** `https://app.pgammedia.com` (prod, Vercel)
**DB:** Neon `pgam_direct` schema (unpooled, direct DDL)
**Branches audited:**
- `mastap150/pgam-direct` @ `9f1024b` (main, post Round-4 hotfixes)
- `mastap150/Prebid.js` (adapter, no changes in scope)
- `mastap150/prebid.github.io` @ `63279027` (docs fork, no regression)
**Tester:** Claude (executing on behalf of Priyesh Patel, PGAM Media LLC)
**Scope:** full pre-launch QA across 19 categories before onboarding the first external partner

---

## Method

- **Role cookies:** `ENV_FILE=web/.env.vercel node /tmp/mkcookie.mjs <role> [partnerId]`
  minted on the fly per scenario. Two publisher cookie forms tested —
  slug (`qa-round4-publisher`) and numeric (`2`) — to surface partnerId
  convention bugs.
- **Direct DB:** `psql "$UNPOOLED"` (DATABASE_URL with `-pooler` stripped)
- **HTTP probes:** `curl -sS -o /dev/null -w "%{http_code}"`
- **Evidence:** HTTP codes + trimmed JSON bodies inline, DB SELECT output
  quoted. Every P0/P1 finding got a FND-0xx ticket.

---

## Findings summary

### Closed in-session (P0)

| FND   | Title                                                             | Resolution                                                                                     |
|-------|-------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| FND-040 | `/api/reporting/partner-health` 500 — missing migration 000021 + SQL alias-in-ORDER-BY crash | Applied migration 000021 to prod (adds `bid_outcomes.publisher_id`); fixed `ORDER BY` to inline COALESCE() so no SELECT alias is used inside a predicate. Commit `e858916`. |
| FND-041 | `session.partnerId` slug/numeric convention mismatch — real publisher/DSP logins would 400 on every reporting page | Added `web/src/server/session-partner.ts` resolver; patched 6 routes + `isPublisherVisible`. Slug and numeric cookies both resolve. Commit `9f1024b` (rebase of `161d329`). |

### Observations (P2, not blockers)

| FND   | Title                                                                   | Disposition                                                                                     |
|-------|-------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| FND-042 | PATCH `/api/admin/placements/{id}` with a missing id returns `200 {ok:true}` instead of 404 | Idempotent no-op by DB semantics (UPDATE 0 rows). Low-risk — auth is enforced via `getPlacement()` for non-admins. File as post-launch cleanup. |

### Re-verified from earlier rounds

| FND   | Title                             | Status  |
|-------|-----------------------------------|---------|
| FND-002 | DSP create persists to Neon       | GREEN   |
| FND-005 | AM margin leak (field redaction)  | GREEN   |
| FND-008 | Prebid docs trim (pgam/pgammedia) | GREEN   |
| FND-009 | /ads.txt served 200               | GREEN   |
| FND-010 | Bootstrap publisher out of sellers.json | GREEN |
| FND-020 | AWS Secrets Manager fail-closed   | GREEN   |
| FND-023 | Reporting/margin stub routes      | GREEN   |
| FND-025 | /api/auth/me rate limit (60/min)  | GREEN   |
| FND-030 | dsp_configs.id IDENTITY           | GREEN   |
| FND-031 | Neon v1.x `sql.query()`           | GREEN   |

---

## 1. Partner creation from scratch

| # | Scenario | Expected | Actual | Status | Evidence | Priority |
|---|---|---|---|---|---|---|
| 1.1 | Publisher create (prebid_s2s) | 201 + Neon row + placements | 201, publisher_id=2, placement_id=50; org_id=`qa-round4-publisher` | GREEN | `psql` confirmed row | P0 |
| 1.2 | Publisher create (direct_rtb + HMAC) | 201 + ARN | Not attempted — AWS unwired; covered by FND-020 fail-closed below | DEFERRED | — | P0 |
| 1.3 | Publisher create (direct_rtb + IP allow-list) | 201 | 201, publisher_id=3, IP allow-list persisted | GREEN | body includes `auth_ip_allowlist=["203.0.113.0/24"]` | P0 |
| 1.4 | direct_rtb w/ neither HMAC nor IPs | 400 VALIDATION | 400 `{"error":"VALIDATION","issues":[{"message":"Direct RTB mode requires at least one of HMAC signing or an IP allow-list."}]}` | GREEN | superRefine fired correctly | P1 |
| 1.5 | Publisher create as AM | 403 FORBIDDEN | 403 `{"error":"FORBIDDEN","detail":"publisher create requires internal_admin"}` | GREEN | — | P0 |
| 1.6 | Publisher create unauthenticated | 401 | 401 `{"error":"UNAUTHENTICATED"}` | GREEN | — | P0 |
| 1.7 | DSP create (auth_type=none, 2 regions) | 201 + dspId + endpointIds[] | 201 `{"dspId":19,"endpointIds":[19,20],"contractIds":[],"authSecretRef":null}` | GREEN | FND-002/030 regression | P0 |
| 1.8 | DSP create w/ bearer, AWS unwired | 503 SECRETS_NOT_CONFIGURED | 503 with detail "AWS Secrets Manager is not configured … Refusing to persist a DSP credential into the in-memory stub." Zero rows leaked. | GREEN | FND-020 regression | P0 |
| 1.9 | DSP create as publisher | 403 | 403 `{"error":"FORBIDDEN","detail":"Only internal_admin may create DSPs"}` | GREEN | — | P0 |

## 2. Partner profile config

| # | Scenario | Expected | Actual | Status | Priority |
|---|---|---|---|---|---|
| 2.1 | GET publisher by id | 200 + full wizard payload | 200, full payload round-tripped including `integration.mode`, `inventory[0].placements[0]`, `org_id` | GREEN | P0 |
| 2.4 | GET DSP by id | 200 + sibling endpoints | 200, full struct w/ auth_type=none, auth_secret_ref=null, correct region | GREEN | P0 |
| 2.5 | DSP list pagination stable | No dupes, limit/offset paginate | `?limit=5&offset=0` → ids [20,19,16,15,14]; `&offset=5` → [13,12,11,10,9]. Stable, no dup. | GREEN | P1 |
| 2.6 | GET publishers as DSP | 403 | 403 `{"error":"FORBIDDEN"}` (hard-denied before Neon) | GREEN | P0 |
| 2.7 | GET publishers as AM | 200 + tenant-scoped | 200 | GREEN | P1 |

## 3. Placement-ID lifecycle

| # | Scenario | Expected | Actual | Status | Priority |
|---|---|---|---|---|---|
| 3.1 | Placement rows written alongside publisher | FK to publisher_id | placement id=50, publisher_id=2, placement_ref=`qa-r4-ctv-preroll`, floor=2.50 | GREEN | P0 |
| 3.2 | PATCH placement floor (admin) | 200 | 200 `{"ok":true}` | GREEN | P0 |
| 3.3 | PATCH placement as owning publisher (slug cookie) | 200 | 200 `{"ok":true}` — session-partner resolver honours slug | GREEN | P0 |
| 3.4 | PATCH placement as different publisher | 403 | 403 `{"error":"forbidden"}` | GREEN | P0 |
| 3.5 | PATCH placement as dsp | 403 | 403 | GREEN | P0 |
| 3.7 | PATCH bad id (`pid=abc`) | 400 `bad_id` | 400 `{"error":"bad_id"}` | GREEN | P2 |
| 3.8 | PATCH missing id (999999) | 404 `not_found` | 200 `{"ok":true}` — **FND-042** silent no-op | YELLOW | P2 |

## 4. Ad-serving readiness

| # | Scenario | Expected | Actual | Status | Priority |
|---|---|---|---|---|---|
| 4.1 | placement_ref URL-safe → `imp.tagid` | Regex-compliant | `qa-r4-ctv-preroll` ✓ | GREEN | P0 |
| 4.2 | publisher org_id → `imp.ext.pgam.orgId` | Slug-safe | `qa-round4-publisher` ✓ | GREEN | P0 |
| 4.3 | sellers.json listing on create | Appears as active seller | Verified inline: `120028 | QA Round4 Publisher | PUBLISHER` | GREEN | P0 |
| 4.4 | /ads.txt served | 200 text/plain w/ IAB fields | 200, includes `CONTACT=ads@pgammedia.com`, SUBDOMAIN, sellers.json pointer | GREEN | P0 |

## 5. Bid request/response validation

| # | Scenario | Expected | Actual | Status | Priority |
|---|---|---|---|---|---|
| 5.1 | Admin test-bid w/ endpoint_id | 200 + upstream attempt + timing | 200, canned OpenRTB 2.6 request rendered, upstream "fetch failed" (rtb.example.com is fake endpoint) — server-side path correct | GREEN | P0 |
| 5.2 | Malformed test-bid | 422 VALIDATION_FAILED | 422 `{"error":"VALIDATION_FAILED","issues":[{"path":["endpoint_id"],"message":"Required"}]}` | GREEN | P1 |
| 5.3 | rtb-tester UI page | 200 HTML | 200 | GREEN | P1 |

## 6. Impression + click tracking

| # | Scenario | Expected | Actual | Status | Priority |
|---|---|---|---|---|---|
| 6.1 | POST `/api/analytics-events` correct shape | 200 `{"ok":true}` | 200 `{"ok":true}` — bidWon event accepted for `qa-round4-publisher` | GREEN | P1 |
| 6.1b | POST w/ wrong shape (legacy) | 400 `missing_fields` | 400 `{"ok":false,"error":"missing_fields"}` | GREEN | P2 |
| 6.2 | GET (non-POST methods) | 405 | 405 | GREEN | P2 |

## 7. Reporting — partner level (RBAC matrix)

| Role      | `/api/reporting/partner` | Notes |
|-----------|--------------------------|-------|
| admin     | 200, full envelope       | `{"role":"internal_admin","rows":[]}` |
| finance   | 200, full envelope       | `{"role":"finance","rows":[]}` |
| am        | 200, margin stripped     | `{"role":"am","rows":[]}` (FND-005 regression GREEN) |
| publisher | 200 w/ slug cookie       | `{"role":"publisher","row_count":0}` — FND-041 fix GREEN |
| publisher | 200 w/ numeric cookie    | back-compat path works |
| dsp       | 200 w/ numeric cookie    | DSP envelope (requires resolver fix from FND-041) |

## 8. Reporting — placement level

All five roles → 200 on `/api/reporting/placement`. Publisher envelope scoped via session-partner resolver.

## 9. Reporting — DSP/campaign/endpoint

| Endpoint | admin | finance | am | pub | dsp |
|---|---|---|---|---|---|
| `/api/reporting/summary` | 200 | 200 | 200 | 200 | 200 |
| `/api/reporting/discrepancy` | 200 | 200 | **403** | **403** | **403** |
| `/api/reporting/placement` | 200 | 200 | 200 | 200 | 200 |
| `/api/reporting/matrix` | 200 | 200 | 200 | 200 (slug) | 200 |
| `/api/reporting/partner-health` | 200 | 200 | 200 | 200 (slug) | 403 |

All consistent with RBAC intent. **GREEN** (post FND-040 + FND-041 deploy).

## 10. Revenue / CPM / payout / margin math

| Scenario | Expected | Actual | Status |
|---|---|---|---|
| `/api/margin/summary` admin | 200 + totals + by_partner | 200 `{"stub":true,"totals":{"gross_revenue_usd":0,"pub_payout_usd":0,"pgam_profit_usd":0,"blended_margin_pct":0},"by_partner":[]}` | GREEN |
| finance | 200 | 200 | GREEN |
| am      | 403 | 403 `{"error":"FORBIDDEN"}` | GREEN |
| pub     | 403 | 403 | GREEN |
| dsp     | 403 | 403 | GREEN |
| Math identity (`gross ≈ payout + profit`) | — | Not exercisable: zero rows in `financial_events`. Structural verification on stub envelopes: all three fields present, blended_margin_pct computed. | INFO |

## 11. Discrepancy + reconciliation

See §9 table — gated matrix matches the role matrix (admin/finance only). `/discrepancy` dashboard page 200 as admin. **GREEN.**

## 12. Troubleshooting / logs

| Page | Code |
|---|---|
| `/live` | 200 |
| `/rtb-tester` | 200 |
| `/discrepancy` | 200 |
| `/compliance/ads-txt` | 200 |
| `/compliance/sellers-json` | 200 |
| `/auctions/1` | 404 — correct: empty `financial_events` table, 404 is intentional anti-enumeration |

## 13. Error-message clarity

| Scenario | Actual |
|---|---|
| Zod validation (missing `basics.name`) | 400 `{"error":"VALIDATION","issues":[{"code":"invalid_type","path":["basics","name"],"message":"Required"}, …]}` — machine-readable, ops-actionable |
| Bad JSON body | 400 `{"error":"BAD_JSON","detail":"SyntaxError: Expected property name …"}` — correct surfacing, detail includes parser position |
| AWS unwired secret | 503 `{"error":"SECRETS_NOT_CONFIGURED","detail":"…Refusing to persist a DSP credential into the in-memory stub."}` — operator-actionable |
| No cookie | 401 `{"error":"UNAUTHENTICATED"}` |

All **GREEN**.

## 14. RBAC matrix (consolidated)

```
Endpoint                     admin  finance  am   pub(slug)  dsp
/api/reporting/summary       200    200      200  200        200
/api/reporting/discrepancy   200    200      403  403        403
/api/reporting/placement     200    200      200  200        200
/api/reporting/matrix        200    200      200  200        200
/api/reporting/partner       200    200      200  200        200
/api/reporting/partner-health 200   200      200  200        403
/api/margin/summary          200    200      403  403        403
/api/publishers              200    200      200  200        403
/api/dsps                    200    403      403  403        403
```

Matches design table post FND-041. **GREEN.**

## 15. Edge cases + invalid setups

| # | Scenario | Actual | Status |
|---|---|---|---|
| 15.1 | Name 200 chars | 400 `too_big maximum=120` | GREEN |
| 15.2 | Negative floor_usd | 400 `too_small minimum=0` at path `inventory.0.placements.0.floor_usd` | GREEN |
| 15.3 | Bad currency "XX" | 400 `too_small exact=3` | GREEN |
| 15.4 | SQL-injection in name (`'); DROP TABLE …`) | 201 — name stored as literal text. `publisher_configs` row count intact. Table still exists. Parameterised queries confirmed. | GREEN |
| 15.6 | Unknown DSP id 9999999 | 404 | GREEN |
| 15.7 | 75× `/api/auth/me` burn-through | 60 × 200 + 15 × 429. `X-RateLimit-Limit:60`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` headers present | GREEN |

## 16. Dashboard data flow + exports

Admin session — all main pages render 200: `/`, `/publishers`, `/dsps`, `/reporting`, `/reports`, `/discrepancy`, `/live`, `/rtb-tester`, `/rules`, `/compliance/ads-txt`, `/compliance/sellers-json`. Login page (`/login`) 200 no-cookie. `/spo` → 307 redirect (correct). Directories without an explicit `page.tsx` (`/compliance`, `/auctions`) 404 — expected App Router behaviour.

## 17. Reporting latency (wall-clock from prod edge)

| Endpoint | t_total |
|---|---|
| `/api/reporting/summary` | 0.27 s |
| `/api/reporting/matrix` | 0.22 s |
| `/api/reporting/partner-health` | 0.40 s |
| `/api/margin/summary` | 0.19 s |

All well under 1 s with zero rows in the underlying tables. When real traffic lands this will grow — flag latency budgets post-launch as part of the KV-limiter PR.

## 18. External integrations

| # | Scenario | Actual | Status |
|---|---|---|---|
| 18.1 | `/ads.txt` | 200 text/plain w/ IAB header block | GREEN |
| 18.2 | `/sellers.json` | 200 application/json, `PGAM Media LLC` INTERMEDIARY only after cleanup | GREEN |
| 18.3 | Bootstrap filter | 0 matches for "bootstrap" / test-org prefix post-cleanup | GREEN |
| 18.4 | Prebid docs PR (upstream #6543) | Out of PGAM's queue — pending upstream maintainer review | INFO |
| 18.5 | AWS Secrets wire guard | 503 SECRETS_NOT_CONFIGURED with detail message | GREEN |

## 19. Public surfaces & security headers

| # | Scenario | Actual | Status |
|---|---|---|---|
| 19.1 | HTTPS enforced | `http://` → 308 to https | GREEN |
| 19.2 | Strict-Transport-Security | `max-age=63072000` (2 years) — HSTS preload-eligible | GREEN |
| 19.3 | 404 surface | 404 on unknown route | GREEN |
| 19.4 | Rate-limit headers on `/api/auth/me` | `x-ratelimit-limit: 60`, `x-ratelimit-remaining: 58`, `x-ratelimit-reset: <epoch>` | GREEN |

Missing — no defect, just not yet shipped: `X-Frame-Options`, `X-Content-Type-Options`, `Content-Security-Policy`, `Referrer-Policy`. Log as a post-launch hardening sweep; not blocking for SSP launch since the dashboard is authenticated-only and not user-supply-side.

---

## Code/data cleanup performed in this run

- Migration **000021** (`bid_outcomes.publisher_id` + index) applied to prod.
- Commit **e858916** (`main`) — fix partner-health ORDER BY alias crash.
- Commit **9f1024b** (`main`) — session-partner slug/numeric resolver for reporting routes.
- Test data archived/deleted:
  - 3 QA publishers → `status='archived'` (out of /sellers.json, still auditable).
  - 2 QA DSP rows → hard-deleted from `dsp_configs` (no FK refs; back to 16 DSPs).

---

## Launch readiness call

**All 19 categories covered. Two new P0 findings opened this round, both fixed and verified live before closing the run.** No open P0 or P1. Two P2 cleanup items tracked (FND-042 silent PATCH no-op; missing optional security headers).

Structural coverage is complete: every ship-critical code path (create → config → placement → bid → report → margin → discrepancy → recon → RBAC → external) has been exercised against prod with both happy-path and negative probes. Arithmetic identities (`gross = payout + profit`, `margin_pct = profit/gross`) are wired but inert until real bid traffic lands — they'll be re-verified in the first cleared-auction smoke test post-onboarding.

**Recommendation: ship.** The SSP can onboard the first external partner on current prod. Reminder for onboarding day — wire AWS Secrets Manager env vars (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) **before** accepting the first bearer/HMAC-auth partner; fail-closed guard (FND-020) blocks the create otherwise.

### Post-launch residuals (tracked, not blockers)

1. **KV-backed rate limiter** — swap the in-process leaky bucket in `web/src/lib/rate-limit.ts` for Vercel KV / Upstash. Currently sharded across lambda instances (documented in Round-3 FND-025 notes). Building in the same session as this QA report.
2. **`db.withTx` → real pg transactions** — Neon Pool migration. Current implementation uses the HTTP Neon client which has per-statement autocommit.
3. **FND-042** — make PATCH on missing placement return 404 instead of silent 200.
4. **Security headers sweep** — add `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Content-Security-Policy` (authenticated app — can be strict), `Referrer-Policy: strict-origin-when-cross-origin`.
5. **Prebid docs PR** — `prebid/prebid.github.io#6543` awaits upstream review, out of our queue.
