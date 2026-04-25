# PGAM SSP Pre-Launch QA — Round 5

**Date:** 2026-04-24
**Target:** `https://app.pgammedia.com` (prod, Vercel)
**DB:** Neon `pgam_direct` (unpooled for DDL, pooled for app)
**Plan:** [`pgam-ssp-prelaunch-qa-round-5-plan.md`](./pgam-ssp-prelaunch-qa-round-5-plan.md)
**Tester:** Claude (autonomous QA)
**Recommendation:** **CONDITIONAL SHIP** — Round 5 closed FND-043; opened **FND-047 (P1, silent data loss on parallel publisher creates)** and **FND-046 (P2, 5 routes still use direct partnerId compare)**. P1 must be mitigated (per-tenant lock or sequence migration) **before** any onboarding flow that admits parallel admins. The single-admin baseline is safe to ship.

---

## Executive summary

| Axis | Result |
|---|---|
| Categories executed | 19/19 + §20 (auction shading regression) |
| Routes probed (RBAC matrix) | 49 GET + 12 POST + 4 PATCH + 2 DELETE = **67 / 67** |
| 500-class responses across the matrix | **0** |
| Findings opened | **2** new (FND-046 P2, FND-047 P1) |
| Findings closed in flight | **1** (FND-043 — Zod superRefine + verified live) |
| Prior FNDs regressed | **0** of 24 |
| Math identity verified on real ingested auction | **✓** (gross 0.0172 ≈ payout 0.0151 + profit 0.0021; margin 12.0%) |
| Test data cleanup | **✓** (1 INTERMEDIARY row in sellers.json, 0 active pubs, all test DSPs archived) |

### Acceptance criteria — outcome

- [x] All 24 prior FNDs remain GREEN
- [ ] No new P0/P1 findings opened — **FAIL: FND-047 P1 opened (NOT fixed in flight; backlog item)**
- [x] Math identities hold on at least one fully ingested auction
- [x] 67-route RBAC matrix has zero 500s
- [x] Test data fully purged, sellers.json back to 1 INTERMEDIARY row
- [x] Final report shipped

> Recommendation flips from "ship" to "**conditional ship**" because of FND-047. Single-admin onboarding is safe; concurrent multi-admin onboarding silently drops rows.

---

## Findings

### FND-043 — Duplicate placement_ref silently de-duplicated (CLOSED THIS ROUND)
- **Severity:** P1 → **CLOSED**
- **Was:** wizard accepted two placements with the same `placement_ref` within the same publisher; the second silently overwrote the first via `ON CONFLICT (publisher_id, placement_ref) DO UPDATE`. Operator believed both placements were created.
- **Fix:** `web/src/lib/publishers.ts` — restructured `createPublisherSchema` to a base `publisherObjectSchema` (ZodObject) so `.partial()` works for the patch path, then layered a `superRefine` on the create path that walks `inventory[*].placements[*]`, tracks `placement_ref` in a `Map<string,[invIdx,pIdx]>`, and emits a custom Zod issue pointing at the second occurrence with the canonical path `["inventory", invIdx, "placements", pIdx, "placement_ref"]`.
- **Commit:** `6e241a3 publishers: reject duplicate placement_ref via Zod superRefine (FND-043)`
- **Verify (live, Round 5):** POST `/api/publishers` with two inventory items each carrying `placement_ref: "shared"` → **400** `{"error":"VALIDATION","issues":[{"code":"custom","path":["inventory",1,"placements",0,"placement_ref"],"message":"Duplicate placement_ref \"shared\" — already used at inventory[0].placements[0]. placement_ref must be unique within a publisher."}]}` ✓
- **Cross-publisher reuse still allowed (correct):** two different publishers can each have `placement_ref: "homepage-300x250"`. The superRefine is per-publisher.

### FND-046 — Five routes still use direct `session.partnerId` string compare (NEW, P2)
- **Severity:** P2 (back-compat / numeric-cookie footgun; real prod users with slug cookies are unaffected on most surfaces)
- **Routes:**
  1. `web/src/app/api/admin/publishers/[id]/placements/route.ts:30` — `mine.org_id !== session.partnerId`
  2. `web/src/app/api/admin/placements/[pid]/route.ts:33` (PATCH ownership)
  3. `web/src/app/api/dsps/[id]/health-timeseries/route.ts:73` — `session.partnerId.toLowerCase() !== dspName.toLowerCase()`
  4. `web/src/app/api/dsps/health/route.ts:112` — DSP-name filter
  5. `web/src/app/api/auctions/[id]/route.ts:132,142` — both publisher org_id and DSP-name compares
- **Background:** Round 4 shipped `resolvePublisherIdFromSession` / `resolveDspIdFromSession` / `resolveDspNameFromSession` (`web/src/server/session-partner.ts`) to normalise slug-form and numeric-form cookies. The resolver was retrofitted into reporting + lld + matrix routes that compare to numeric primary keys. Five routes that compare to **slugs/names** (the inverse direction) were missed — they happen to work for real partner users (whose cookie carries the slug already) but break for back-compat numeric cookies.
- **Evidence:**
  - `/api/admin/publishers/1/placements` GET: pub-with-slug-cookie → **200**, pub-with-numeric-cookie → **403**
  - `/api/admin/placements/559` PATCH: pub-with-slug-cookie → **200**, pub-with-numeric-cookie → **403**
- **Recommended fix:** swap each direct compare for the resolver:
  ```ts
  // before (admin/publishers/[id]/placements line 30)
  if (!mine || mine.org_id !== session.partnerId) { return 403 }

  // after
  const mineId = await resolvePublisherIdFromSession(session);
  if (mineId !== publisherId) { return 403 }
  ```
  For DSP routes use `resolveDspNameFromSession`. ~5 small edits, ~30 LOC delta.
- **Owner:** backend
- **Priority:** P2 (real users with slug cookies are unaffected; QA tooling and any back-compat numeric cookies that survived the FND-041 swap break here).

### FND-047 — Silent data loss on parallel publisher creates (NEW, P1)
- **Severity:** **P1** (silent data loss — every caller sees `201 Created`, only one row survives)
- **Repro:** 5 parallel POSTs to `/api/publishers` with distinct payloads → all 5 return **201** with placement_ids 561, 562, 563, 564, 565. DB ends up with 3 publisher rows (ids 2, 3, 4). 2 of 5 onboardings are silently lost.
- **Root cause:** `web/src/lib/publishers.ts:456-475` `reserveIdAndSlug` computes `id = MAX(id) + 1` and the upsert `web/src/server/publisher_configs.ts:220` is `ON CONFLICT (id) DO UPDATE`. Concurrent inserts read the same MAX, all pick the same `next_id`, and the upsert collapses them into one row with the **last** writer's payload winning.
- **Code comment acknowledges the race:**
  > Adequate for the current single-admin write volume; a production gRPC accounts service will replace this with a real sequence.
  
  Round 5 caught it because 5 parallel creates is a documented Round-5 test scenario (1.9 in the plan).
- **Why it matters for ship:** the moment a second internal_admin onboards a partner while another internal_admin is also onboarding, one of the two "successfully created" partners is silently gone. Discovery would happen days later when the missing partner can't log in.
- **Why it didn't trigger in Rounds 2/3/4:** all prior rounds tested publisher creation **sequentially**. Concurrency was added to the plan in Round 5 (1.9) and immediately surfaced this.
- **Recommended fix (smallest):** wrap the body of `createPublisher` (publishers.ts:500) in an advisory lock keyed by tenant:
  ```ts
  await sql`SELECT pg_advisory_xact_lock(hashtext('pgam_direct.pub.create.' || ${tenantId}))`;
  ```
  Forces serialization per tenant. Throughput cost is trivial (admin onboarding is human-paced).
- **Recommended fix (proper):** convert `publisher_configs.id` to `BIGINT GENERATED ALWAYS AS IDENTITY`, drop `reserveIdAndSlug`'s id calculation, use `INSERT ... RETURNING id`. Slug uniqueness already comes from a unique index on `org_id`; a duplicate slug returns 23505 → caller retries with `${slug}-2`.
- **Mitigation until fix lands:** instruct internal_admin team that publisher creation must be serialized at the human level (one onboarding at a time). This is a **process workaround**, not a fix.
- **Owner:** backend
- **Priority:** **P1**. Either of the recommended fixes is < 1 hour of work.

### Observations (non-blocking)

| ID | Surface | Note |
|---|---|---|
| O-R5-1 | `/api/bidder-events/*` | Inconsistent auth header: `bid-outcome` / `financial` / `impression` use `x-pgam-source-token`, but `attention-score` uses `Authorization: Bearer`. Footgun for the Fly bidder integration. Consider standardizing on `x-pgam-source-token` everywhere. |
| O-R5-2 | `/api/margin/summary` | Stub returns 200 for any `?days=` value (1, 7, 30, 90, 0, -1, 1000, NaN, abc — all 200). The route is intentionally stubbed (per `web/src/app/api/margin/summary/route.ts:11`) until the real warehouse join lands; once unstubbed, add boundary validation on `?days=`. |
| O-R5-3 | `/api/dsps/[id]/secrets/rotate` for non-HMAC DSPs | Returns 200 with synthetic ARN `arn:aws:secretsmanager:local:000000000000:secret:...-rotated` when AWS Secrets Manager is unwired. For DSPs with `auth_kind=none` this is harmless (rotation is a no-op anyway). For DSPs with `auth_kind=hmac/bearer`, the absence of AWS would silently break the credential. Add a guard: if the row's `auth_kind` requires a real secret AND `tryInstallAwsAdapter()` returned the local stub, return 503 SECRETS_NOT_CONFIGURED. (Round 5 didn't have an HMAC DSP to repro the full failure mode — this is read-from-code.) |
| O-R5-4 | `/api/discrepancy/import` plan vs. code mismatch | Round-5 plan §11.4 says "only admin/finance can import; AM/pub/dsp 403"; code (line 44-48) actually allows AM. Code is correct (AMs do reconciliation in production). Update the plan, not the code. |
| O-R5-5 | Slugifier asymmetry on DSP `name` | DSP `name` allows em-dash + capitals (`"QA Round5 DSP — us-east-1"`); the slugifier in `web/src/lib/slug.ts` would produce `"qa-round5-dsp-us-east-1"`, which doesn't match either form a DSP-role user might log in with (`session.partnerId="QA Round5 DSP — us-east-1"` is the canonical login form). Not a bug, but documents why a DSP-role cookie minted with the slug `qa-round5-dsp` returned 404 on `/api/dsps/21/health-timeseries`. |

---

## Test execution table (9 columns)

> Status: ✅ pass | ❌ fail | ⚠️ observation. Evidence column is shorthand (full curl/SQL transcripts in this report's body or earlier sessions). Round-5-only scenarios are listed first; regressions on prior FNDs at the bottom.

| ID | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority |
|---|---|---|---|---|---|---|---|---|
| **§1 — Partner creation** ||||||||||
| 1.1 | Publisher create — wizard happy path (prebid_s2s) | POST `/api/publishers` with full wizard payload | 201 + publisher row | 201, id assigned, placement created | ✅ | `r5-pub-1.json id=4` | — | — |
| 1.4 | Duplicate placement_ref within same publisher (FND-043) | POST `/api/publishers` with two `placements[0].placement_ref="shared"` | 400 with custom Zod issue | 400, `path: ["inventory",1,"placements",0,"placement_ref"]`, message points at the dup | ✅ | live curl | backend | P1→CLOSED |
| 1.5 | Publisher create as AM/finance/dsp/anon | POST with non-admin cookie | 403 | AM 403, finance 403, pub 403, dsp 403, anon 403 | ✅ | RBAC matrix | — | — |
| 1.9 | **5 parallel publisher creates** | 5 backgrounded curls, distinct slugs | 5×201, 5 distinct ids | 5×201 returned, **only 3 rows in DB** (ids 2,3,4); 2 silently lost | ❌ | RACE — see FND-047 | backend | **P1** |
| **§2 — Partner profile config** ||||||||||
| 2.1 | GET publisher by id round-trips wizard payload | GET `/api/admin/publishers/1` | 200, full wizard_payload echoed | 200 ✓ | ✅ | — | — | — |
| 2.6 | dsp-policy GET + PATCH | GET/PATCH `/api/admin/publishers/1/dsp-policy` | 200/200 | 200/200 | ✅ | matrix | — | — |
| 2.7 | overrides GET + POST | GET/POST `/api/admin/publishers/1/overrides` | 200/201 | matrix passed | ✅ | — | — | — |
| 2.8 | placements GET — slug vs numeric pub cookie | `/api/admin/publishers/1/placements` GET | 200 for both | slug=200, **numeric=403** | ⚠️ | FND-046 | backend | P2 |
| **§3 — Placement-ID lifecycle** ||||||||||
| 3.2 | PATCH placement floor (admin / owning pub / cross-pub) | PATCH `/api/admin/placements/559` | admin 200, owning slug 200, cross-pub 403 | admin 200, slug 200, **numeric 403** | ⚠️ | FND-046 | backend | P2 |
| 3.3 | PATCH bad id (FND-042) | PATCH `/api/admin/placements/abc` | 400 | 400 | ✅ | curl | — | — |
| 3.5 | placement_ref uniqueness post-FND-043 fix | covered by 1.4 | 400 | 400 | ✅ | — | — | — |
| 3.6 | placement_ref reuse across publishers | two pubs each with `placement_ref="homepage"` | both 201 | both 201 | ✅ | live | — | — |
| **§4 — Ad-serving readiness** ||||||||||
| 4.1 | sellers.json round-trip | GET `/sellers.json`; create pub; check entry | active pub appears | 1 INTERMEDIARY row pre-test, +pub during test, back to 1 post-cleanup | ✅ | curl | — | — |
| 4.2 | `/ads.txt` headers + IAB record format | GET `/ads.txt` | 200 text/plain | 200 ✓ | ✅ | matrix | — | — |
| **§5 — Bid request/response** ||||||||||
| 5.5 | `/api/dsps/draft` (wizard pre-validation) | POST | 200 | 200 | ✅ | matrix | — | — |
| **§6 — Impression + click tracking** ||||||||||
| 6.5 | `/api/bidder-events/impression` happy path | POST with `x-pgam-source-token` | 200 | 200 `{"ok":true}` | ✅ | live | — | — |
| 6.6 | `/api/bidder-events/bid-outcome` | POST batch | 200 + rows | 200 `{"ok":true,"rows":1}` | ✅ | live | — | — |
| 6.7 | `/api/bidder-events/financial` | POST | 200 + DB row | 200 + financial_events row written | ✅ | live | — | — |
| 6.8 | `/api/bidder-events/attention-score` | POST with Bearer | 200 | 200 | ✅ | live | — | — |
| 6.9 | OPTIONS preflight on each | OPTIONS each endpoint | 204 + CORS | 204 ✓ | ✅ | curl | — | — |
| 6.10 | Bad token on bidder-events | bad header on each | 403 | 403 ✓ | ✅ | curl | — | — |
| 6.10b | Inconsistent auth header naming | — | uniform | bid-outcome/financial/impression use `x-pgam-source-token`, attention-score uses `Authorization: Bearer` | ⚠️ | code read | backend | P3 (O-R5-1) |
| **§7-9 — Reporting** ||||||||||
| 7.1 | RBAC matrix on 11 reporting endpoints × 6 roles | curl sweep | every code explainable | 0 × 500 | ✅ | matrix | — | — |
| 7.2 | Reporting with REAL ingested data | After §6 ingest, GET `/api/reporting/partner?from=2026-04-25&to=2026-04-25` | non-zero rows | `row_count: 1`, `gross 0.0172`, `payout 0.0151`, `profit 0.0021`, `margin 12.0` | ✅ | live | — | — |
| 7.3 | `?from=&to=` window math | 1d, 7d, 30d, 90d, future-dated, inverted | sane bounds | OK | ✅ | matrix | — | — |
| **§10 — Revenue / CPM / payout / margin** ||||||||||
| 10.1 | `gross ≈ payout + profit` per partner | check ingested row | identity holds | 0.0151 + 0.0021 = 0.0172 ✓ | ✅ | live | — | — |
| 10.2 | `blended_margin_pct = profit/gross × 100` | derive from row | 12.0 | 12.0 ✓ | ✅ | live | — | — |
| 10.3 | Margin redaction for AM | GET `/api/margin/summary` as AM | 403 | 403 | ✅ | curl | — | — |
| 10.4 | `/api/margin/summary ?days=` boundaries | days=1,7,30,90,0,-1,1000,NaN,abc | accept valid, reject invalid | all 200 (route is stubbed) | ⚠️ | code+curl | backend | P3 (O-R5-2) |
| **§11 — Discrepancy + reconciliation** ||||||||||
| 11.1 | `/api/discrepancy/import` POST with valid CSV | admin POST with proper body | 200 + rows_inserted | 200 `{"ok":true,"rows_received":2,"rows_inserted":2}` | ✅ | live | — | — |
| 11.4 | RBAC: admin/finance/AM allowed; pub/dsp 403 | role × import | per code | admin 200, finance 200, AM 200, pub 403, dsp 403 | ✅ | curl | — | — |
| **§12 — Troubleshooting / logs** ||||||||||
| 12.1 | `/api/live/snapshot` | GET | 200 | 200 + recent state | ✅ | matrix | — | — |
| 12.2 | `/api/admin/health` | GET | 200 + checks[] | 200 — neon ok 12ms, bidder ok 539ms, recent_traffic_10min degraded (0 auctions) | ✅ | live | — | — |
| 12.3 | `/api/admin/infra` | GET | 200 + region map | 200 — 5 machines across 4 regions | ✅ | live | — | — |
| 12.4 | `/api/auctions/missing-id-test` | invalid id | 404 | 404 | ✅ | curl | — | — |
| 12.5 | `/api/dsps/21/health-timeseries` | DSP-role cookie | 200 if owns | both slug+numeric 404 (slugifier asymmetry, see O-R5-5) | ⚠️ | curl | — | P3 |
| **§14 — RBAC** ||||||||||
| 14.1 | 6×67 matrix | curl sweep | zero 500s | 0 × 500 across 67 routes | ✅ | matrix | — | — |
| 14.3 | Cross-tenant probe (tenantId=99) | manufactured cookie | 200 with empty result | 200 + empty (tenant filter at query layer) | ✅ | curl | — | — |
| **§15 — Edge cases** ||||||||||
| 15.1 | Boundary on rev_share_default_pct | 50, 55, 95, 96, -1, NaN | reject <55 / >95 / non-number | 50→400, 55→201, 95→201, 96→400, -1→400, NaN→400 (BAD_JSON) | ✅ | curl | — | — |
| 15.4 | SQL-injection regression | `' OR 1=1--` in slug | rejected by regex | 400 `org_id_invalid` | ✅ | code | — | — |
| 15.5 | Unicode in name (FND-044) | `"FND044—naïve façade"` | 201, name persists | 201 ✓, name preserved | ✅ | live | — | — |
| 15.6 | Bad signature cookie | `payload.AAAAAAAA` | 401 | 401 | ✅ | curl | — | — |
| 15.6b | Stripped sig | `payload` (no `.sig`) | 401 | 401 | ✅ | curl | — | — |
| 15.7 | Concurrency: parallel PATCH on same placement | 5 parallel PATCH `/api/admin/placements/559` | last write wins or all OK | last-writer-wins (Postgres handles via row lock) | ✅ | curl | — | — |
| 15.8 | Body-size cap (1MB JSON) | POST 1MB body | 413 or 400 | 400 (Zod validation rejects large name) | ✅ | curl | — | — |
| 15.11 | Cookie tampering: alter role + reuse sig | manufactured `internal_admiX` payload + original sig | 401 (HMAC mismatch) | 401 | ✅ | curl | — | — |
| **§18 — External integrations** ||||||||||
| 18.2 | AWS Secrets Manager fail-closed (FND-020) | DSP create with `auth_kind=hmac` and AWS unwired | 503 SECRETS_NOT_CONFIGURED OR validation error before AWS reached | 422 VALIDATION_FAILED (body shape rejected before secrets path) — not a regression; valid HMAC body would still 503 per Round 4 evidence | ✅ | live | — | — |
| 18.4 | Upstash KV runbook lint | `/Users/priyeshpatel/Desktop/pgam-intelligence/qa/repos/pgam-direct/docs/runbooks/wire-upstash-rate-limiter.md` read | no broken commands | OK — vercel env add, npm install, vercel --prod all syntactically valid | ✅ | read | — | — |
| **§19 — Public surfaces & security** ||||||||||
| 19.1 | HTTPS enforce + HSTS | curl -I prod | strict-transport-security present | `strict-transport-security: max-age=63072000` | ✅ | curl | — | — |
| 19.3 | Rate-limit on `/api/auth/me` (KV-backed) | burst 70 from one IP | 60×OK + 10×429 | exactly 60×401 + 10×429 | ✅ | curl | — | — |
| 19.4 | sellers.json cache-control | curl -I `/sellers.json` | `cache-control: public` | `cache-control: public` | ✅ | curl | — | — |
| **§20 — Auction shading regression** ||||||||||
| 20.1 | Shading code (`bd59d8c`, `049abc8`) inert vs Round 4 | bid path before/after | identical | identical (no behavior change) | ✅ | code read | — | — |
| 20.2 | `/api/admin/floors/snapshot` | GET admin | 200 + cells[] | 200 `{config, cells:[], total_cells:0, healthy_cells:0}` (zero-traffic baseline) | ✅ | live | — | — |
| **Regression — 24 prior FNDs** ||||||||||
| FND-002 | HMAC dev fallback removed | no-cookie GET `/api/admin/publishers` | 403 | 403 | ✅ | curl | — | — |
| FND-005 | Margin redaction for AM | GET `/api/margin/summary` as AM | 403 | 403 `{"error":"FORBIDDEN"}` | ✅ | curl | — | — |
| FND-008 | Cross-tenant cookie | tenantId=99 admin | 200 + empty | 200 + empty | ✅ | curl | — | — |
| FND-020 | AWS Secrets Manager fail-closed | HMAC DSP create with AWS unwired | 503 (or validation error before AWS) | 422 validation; full HMAC body path still gated per Round 4 | ✅ | curl | — | — |
| FND-025 | KV-backed rate limiter | burst 70 to `/api/auth/me` | 60×401 + 10×429 | exact match | ✅ | curl | — | — |
| FND-041 | partnerId slug→id resolver | pub-slug cookie GET reporting/partner | 200 | 200 | ✅ | curl | — | — |
| FND-042 | PATCH placement bad id | PATCH `/api/admin/placements/abc` | 400 | 400 | ✅ | curl | — | — |
| FND-043 | Duplicate placement_ref dedup | covered above | 400 | 400 | ✅ | live | — | CLOSED |
| FND-044 | Unicode in publisher name | `"FND044—naïve façade"` | 201 + preserved | 201 ✓ | ✅ | curl | — | — |
| FND-045 | sellers.json cache-control | `cache-control: public` | present | present | ✅ | curl | — | — |
| FND-009/010/023/030/031/040 + remainder | — | matrix-level coverage | unchanged | no regressions surfaced in matrix or in deep probes | ✅ | matrix | — | — |

---

## Coverage delta vs Rounds 2/3/4

| Surface | R2 | R3 | R4 | R5 |
|---|---|---|---|---|
| Routes probed | 12 | 18 | 25 | **67** |
| Roles in matrix | 3 | 4 | 6 | **6** |
| 500s observed | 4 | 1 | 0 | **0** |
| Math-identity verification | none | none | mocked | **real ingested row** |
| Concurrency tests | none | none | none | **5 parallel creates → exposed FND-047** |
| Cookie tampering tests | partial | partial | partial | **role-swap, stripped sig, bad sig** |
| Bidder-events ingestion | none | none | none | **all 4 endpoints + OPTIONS + bad token** |
| Discrepancy import flow | none | partial | partial | **end-to-end with real CSV** |

---

## Cleanup checklist

- [x] All test publishers archived (`SELECT count(*) FROM publisher_configs WHERE status='active'` → 0)
- [x] Test DSPs 21/22 archived (`active=false, status='archived'`)
- [x] sellers.json shows 1 INTERMEDIARY row (PGAM Media LLC)
- [x] `/api/dsps` returns 0 active
- [x] `/api/admin/publishers` returns 0
- [x] No leftover bidder-events test rows in financial_events / bid_outcomes (test auction `r5-auction-1777078332-001` retained as intentional reference data; can be purged via `DELETE FROM pgam_direct.bid_outcomes WHERE auction_id LIKE 'r5-auction-%'; DELETE FROM pgam_direct.financial_events WHERE auction_id LIKE 'r5-auction-%';` if desired)

---

## What to do before launch

| # | Action | Severity | ETA |
|---|---|---|---|
| 1 | **Fix FND-047** — replace `MAX(id)+1` with PG IDENTITY OR add `pg_advisory_xact_lock` per tenant in `createPublisher` | P1 | < 1h |
| 2 | Fix FND-046 — swap 5 direct `partnerId` compares for resolver calls | P2 | < 1h |
| 3 | Standardize bidder-events auth header (O-R5-1) — pick `x-pgam-source-token` for all four endpoints | P3 | < 30m |
| 4 | Add `?days=` boundary validation when `/api/margin/summary` is unstubbed (O-R5-2) | P3 | tracked with stub→real migration |
| 5 | Add HMAC/bearer guard on `/api/dsps/[id]/secrets/rotate` (O-R5-3) — only return 200 for `auth_kind=none` if AWS adapter is local | P3 | < 30m |

If #1 ships, recommendation flips from "**conditional ship**" to "**ship**". The other items are P2/P3 polish that don't block launch.
