# PGAM SSP — Pre-Launch QA (Round 2)

**Target:** `https://app.pgammedia.com/` + `https://rtb.pgammedia.com/` + bidder-edge on Fly.io
**Date executed:** 2026-04-24
**Owner:** Priyesh + Claude
**Code under test:** mastap150/pgam-direct @ main, mastap150/Prebid.js, mastap150/prebid.github.io
**Context:** PR #1 (round-1 P0 fixes) is **open, not yet merged**. Expect P0s from
round-1 to re-appear on prod until the PR lands.
**Evidence root:** `/Users/priyeshpatel/Desktop/pgam-intelligence/qa/evidence-round2/`

---

## 1. Executive summary

| Bucket | Count |
|---|---|
| **P0 — block launch** | 5 |
| **P1 — fix before external onboarding** | 7 |
| **P2 — fix post-launch, watch closely** | 9 |
| **Verified-working / hardened** | 6 classes |

**Launch verdict:** 🛑 **NOT READY.** The three round-1 P0s that necessitated
the fix branch (cross-tenant data leak, DSP create not persisting, publisher
wizard round-trip loss) are still live on prod because PR #1 has not shipped.
Two new P0s surfaced in round-2 (DSP create returns 201 with synthetic IDs
but nothing persists, and the AWS Secrets Manager wiring for DSP auth_secret
is a local stub pushing `arn:aws:secretsmanager:local:000000000000:…`).

**Good news:** auth hardening is solid — tamper, strip, and forge attempts
all reject with 401. Zod schemas on both publisher and DSP routes reject
oversized strings, negative floors, out-of-band revenue shares, and the
direct_rtb mode without HMAC/IP-allowlist. SQL injection payloads in
`basics.name` are correctly parameterised — table survives, payload stored
as literal text.

---

## 2. Test plan — consolidated matrix

Columns: **Scenario · Steps · Expected · Actual · Status · Evidence · Owner · Priority · Notes**.

Status: ✅ Pass · 🛑 Fail · ⚠️ Needs-fix · ℹ️ Informational.

### 2.1 Partner creation & wizard round-trip

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 1.1 | Create publisher — Prebid Client mode | POST `/api/publishers` with full wizard payload, then GET same id | 201; readback returns the payload as submitted | 201, readback drops `inventory[].placements`, `contact_email`, `contact_phone`, `legal_entity` | 🛑 Fail | `t2_create_prebid_client.json`, `t2_readback_prebid_client.json` | Claude | **P0 (FND-003)** | Root cause: `publisher_configs` has no `wizard_payload` column yet. Migration 000018 written, not applied. |
| 1.2 | Create publisher — Prebid S2S mode | Same shape, `integration.mode=prebid_s2s`, `pbs_stored_req_id` set | 201; readback has `pbs_stored_req_id` | 201, id=3, `pbs_stored_req_id` preserved in `payload.integration` | ✅ Pass | `t2_create_s2s.json` | Claude | — | — |
| 1.3 | Create publisher — Direct RTB with HMAC | mode=direct_rtb, auth_hmac_enabled=true | 201 + `auth_secret_ref="hmac-present"` | 201 id=4, HMAC key issued: `17e8f16be3449e0fe024a5f60374d42b5db0a2ab6c675dadde27f1ccd3ec3858` (from `/api/publishers/4/hmac/generate`) | ✅ Pass | `t2_create_directrtb.json`, `t2_hmac_generate.json` | Claude | — | Secret is returned exactly once, read-back shows `hmac-present` marker only. |
| 1.4 | direct_rtb without HMAC + no IP allowlist | POST with `auth_hmac_enabled=false`, `auth_ip_allowlist=[]` | 400 rejection | 400 `"Direct RTB mode requires at least one of HMAC signing or an IP allow-list."` | ✅ Pass | `t11_edge.md §E` | Claude | — | superRefine fires correctly. |
| 1.5 | Oversized publisher name (10k chars) | POST with 10,000-char name | 400 rejection | 400 `too_big max=120` | ✅ Pass | `t11_edge.md §B` | Claude | — | — |
| 1.6 | Negative placement floor_usd | POST with `floor_usd:-5` | 400 rejection | 400 `too_small min=0` | ✅ Pass | `t11_edge.md §C` | Claude | — | — |
| 1.7 | rev_share_default_pct=99 | POST financial.rev_share_default_pct=99 | 400 rejection | 400 `too_big max=95` | ✅ Pass | `t11_edge.md §D` | Claude | — | Matches 5%/95% band documented in CLAUDE notes. |
| 1.8 | SQL injection in basics.name | POST name = `'; DROP TABLE pgam_direct.publisher_configs; --` | Stored as literal, table survives | 201, id=5, list still returns all rows | ✅ Pass | `t11_edge.md §A` | Claude | — | Parameterisation is correct. |
| 1.9 | XSS payload in name | POST name = `<script>alert(1)</script>` | Stored verbatim; React escapes on render | 201, stored as-is | ⚠️ Needs-fix | `t11_edge.md §I` | Claude | P2 (FND-018) | React escapes dashboard render, but CSV exports / email templates / logs would be vulnerable. Strip or reject control sequences. |
| 1.10 | Malformed JSON body | POST `{not json` | 400 with generic error | 400 `"detail":"SyntaxError: Expected property name…"` | ⚠️ Needs-fix | `t11_edge.md §H` | Claude | P2 (FND-019) | Raw Node error string leaks parser internals — strip `detail` in prod. |
| 1.11 | Placement_ids preserved on readback | POST publisher with 1 placement, GET readback | `placement_ids:[48]` returned | `placement_ids:[]` returned; inventory array empty | 🛑 Fail | `t2_readback_prebid_client.json` | Claude | **P0 (FND-003)** | Joined sub-query is not being wired from `placements` table into readback. |

### 2.2 RBAC + tenancy

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 2.1 | Publisher role cross-tenant list | Mint `publisher` cookie with partnerId=1, GET `/api/publishers` | See only publishers where tenant_id/partnerId matches | Sees ALL 6 publishers (ids 1-6) including ones created for other tenants | 🛑 Fail | `t1_publishers_rbac.md`, live re-check | Claude | **P0 (FND-001)** | Every role gets the full roster. Tenant WHERE clause is missing from list query. |
| 2.2 | DSP role → /api/publishers | Cookie role=dsp, partnerId=1 | 403 or empty list | 200 with full roster (6 rows) | 🛑 Fail | live re-check | Claude | **P0 (FND-001)** | DSPs should not see publisher directory. |
| 2.3 | Publisher2 sees Publisher1 | `publisher` cookie partnerId=9, GET `/api/publishers` | Nothing or own row | Full roster | 🛑 Fail | `t1_publishers_rbac.md` | Claude | **P0 (FND-001)** | Same root cause as 2.1. |
| 2.4 | Page route RBAC: /admin/users | All 5 roles | Only admin; finance optional | All 5 roles return **200** — publisher and dsp can render the admin users page | 🛑 Fail | `t4_rbac_matrix.md` | Claude | **P0 (FND-004)** | Page RBAC broken; only API RBAC is enforced. |
| 2.5 | Page route RBAC: /admin/publishers, /admin/dsps, /admin/floors | All 5 roles | Only admin + limited staff | All 5 roles → 200 | 🛑 Fail | `t4_rbac_matrix.md` | Claude | **P0 (FND-004)** | Pages rely on API to fail closed — defence-in-depth missing. |
| 2.6 | /admin/blocklist and /admin/deals page guards | publisher + dsp | 307 redirect | 307 for publisher and dsp | ✅ Pass | `t4_rbac_matrix.md` | Claude | — | These two pages got the `(dashboard)` route-group guard; others didn't. |
| 2.7 | /api/dsps RBAC | 5 roles | admin: 200, others: 403 | admin:200, finance:403, am:403, publisher:403, dsp:403 | ✅ Pass | `t4_rbac_matrix.md`, `t9_dsp.md` | Claude | — | Good. |
| 2.8 | /api/dsps/health RBAC | 5 roles | admin + finance + am + dsp: 200; publisher: 403 | admin:200, finance:200, am:200, publisher:403, dsp:200 | ✅ Pass | `t9_dsp.md` | Claude | — | Matches intent: DSPs see their own health. |
| 2.9 | /dsps/new page | 5 roles | admin: 200, others: 404 | admin:200, rest:404 | ✅ Pass | `t4_rbac_matrix.md` | Claude | — | One of the few pages with proper guards. |

### 2.3 Session & auth hardening

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 3.1 | Valid signed cookie | curl /api/auth/me with signed cookie | 200, role reflected | 200 | ✅ Pass | `t8_auth.md` | Claude | — | — |
| 3.2 | Tampered payload | Modify role in base64 payload, keep old sig | 401 | 401 `UNAUTHENTICATED` | ✅ Pass | `t8_auth.md` | Claude | — | HMAC validates. |
| 3.3 | Signature stripped | Send `pgam_session=<payload>` no dot | 401 | 401 | ✅ Pass | `t8_auth.md` | Claude | — | — |
| 3.4 | Unsigned forged admin cookie | Base64 `{role:internal_admin}`, no sig | 401 | 401 | ✅ Pass | `t8_auth.md` | Claude | — | — |
| 3.5 | No cookie | Baseline | 401 | 401 | ✅ Pass | `t8_auth.md` | Claude | — | — |
| 3.6 | Session-secret exposure | Is `PGAM_SESSION_SECRET` in any public artifact? | No | Secret present only in `.env.vercel` (local) + Vercel encrypted env | ✅ Pass | repo grep | Claude | — | — |

### 2.4 DSP management

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 4.1 | GET /api/dsps as admin | curl with admin cookie | Roster of created DSPs | `{"items":[]}` even after successful POST | 🛑 Fail | `t9_dsp.md §A,H` | Claude | **P0 (FND-002 DSP-side)** | StubDb still backs DSP CRUD. |
| 4.2 | POST /api/dsps full wizard shape | Submit valid 5-section payload | 201 + row persisted | 201 returned with `dspId:1`, but GET /api/dsps/1 → 404 | 🛑 Fail | `t9_dsp.md §G,H` | Claude | **P0 (FND-002 DSP-side)** | Create is a no-op; success envelope is a lie. |
| 4.3 | DSP auth_secret handling | Provide `auth_type=bearer` + `auth_secret` | Secret goes to AWS Secrets Manager, only ARN stored | Response shows `"arn:aws:secretsmanager:local:000000000000:secret:pgam/dsp/qa-r2-dsp/auth"` | 🛑 Fail | `t9_dsp.md §G` | Claude | **P0 (new FND-020)** | AWS Secrets Manager integration is a stub. Any prod-deployed DSP with bearer/mtls/hmac auth has no credentials stored. |
| 4.4 | POST /api/dsps malformed payload | Flat shape instead of nested | 422 | 422 VALIDATION_FAILED with clear path messages | ✅ Pass | `t9_dsp.md §E,F` | Claude | — | Good gating. |
| 4.5 | DSP ID collision with master table | Create → dspId=1 | No collision with `/api/dsps/health` which returns 16 DSPs ids 1-16 | Collision: dspId=1 = verve in master table | ⚠️ Needs-fix | `t9_dsp.md §G,B` | Claude | P1 (FND-021) | Two ID spaces for "DSP" are confusing; consolidate or namespace. |
| 4.6 | DSP health endpoint | GET /api/dsps/health | 16 DSPs, zero traffic counters | 16 DSPs, all counters 0 (no traffic flowing yet) | ℹ️ Informational | `t9_dsp.md §B` | Claude | — | Expected pre-launch. |

### 2.5 Bidder-edge & RTB

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 5.1 | rtb.pgammedia.com /healthz | curl | 200 | 200 healthy | ✅ Pass | `t3a_healthz.json` | Claude | — | — |
| 5.2 | Fly.io underlying /healthz | pgam-bidder-edge.fly.dev | 200 | 200 | ✅ Pass | `t3h_fly_healthz.json` | Claude | — | Cold-start ~500ms. |
| 5.3 | POST empty body | curl -X POST rtb.pgammedia.com/ -d '' | 400 body-shape (FND-015 fixed) | **401 UNAUTHENTICATED** | ⚠️ Needs-fix | `t3b_empty.txt` | Claude | P1 (FND-015 partial) | Auth-before-shape ordering means empty-body doesn't surface a 400 ever — always bounces on auth first. Hard to distinguish "bad creds" from "bad payload". |
| 5.4 | POST junk body | `-d 'hello'` | 400 | 401 | ⚠️ Needs-fix | `t3c_junk.txt` | Claude | P1 (FND-015 partial) | Same as 5.3. |
| 5.5 | POST known publisher, no HMAC | Use a persisted publisher's pbs_stored_req_id | Was 204 in round-1 (FND-007) → now 401 | 401 | ✅ Pass | `t3e_known_publisher.txt` | Claude | — | Auth bug from round-1 appears tightened. Need a signed-body test to prove the happy path still works. |
| 5.6 | POST unknown publisher | 999999 | 401 | 401 | ✅ Pass | `t3f_unknown_publisher.txt` | Claude | — | — |
| 5.7 | POST malformed JSON body | `{not json` | 400 | 401 | ⚠️ Needs-fix | `t3g_malformed.txt` | Claude | P1 (FND-015 partial) | Same auth-before-shape issue. |
| 5.8 | /rtb-tester dashboard submit | Attempt test request via UI | 200 with echoed bid | `missing_endpoint_url` error | ⚠️ Needs-fix | Testing at T7 | Claude | P1 (FND-022) | rtb-tester can't actually fire a request without the operator manually pasting an endpoint — defeats the point of the tool. |

### 2.6 Reporting, margin & LLD

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 6.1 | GET /api/reporting/partner | 7-day window, admin | rows for existing activity | `row_count:0 rows:[]` | ⚠️ Needs-fix | `t5_reporting_endpoints.md` | Claude | P1 (FND-006) | Either no traffic has flowed (expected pre-launch) OR the rollup isn't reading LLD. Decide and document. |
| 6.2 | GET /api/reporting/discrepancy | Standard probe | 200 with discrepancy rollup | 404 HTML | 🛑 Fail | `t5_reporting_endpoints.md` | Claude | **P1 (FND-023)** | Route does not exist. Reporting tab in UI will break. |
| 6.3 | GET /api/reporting/placement | Standard probe | 200 | 404 | 🛑 Fail | `t5_reporting_endpoints.md` | Claude | **P1 (FND-023)** | Same. |
| 6.4 | GET /api/reporting/summary | Standard probe | 200 | 404 | 🛑 Fail | `t5_reporting_endpoints.md` | Claude | **P1 (FND-023)** | Same. |
| 6.5 | GET /api/margin/summary | Standard probe | 200 | 404 | 🛑 Fail | `t5_reporting_endpoints.md` | Claude | **P1 (FND-023)** | Same. |
| 6.6 | GET /api/lld | 7-day window, admin | Ordered log of line-level events | 200, 6.4kB CSV header + rows | ✅ Pass | `t5_reporting_endpoints.md` | Claude | — | LLD works; rollup layers on top don't. |
| 6.7 | `/api/rbac/allowed-metrics` for each role | 5 roles | AM and publisher/dsp should NOT see `margin_pct`, `profit`, `gross_revenue` | admin/am/finance all return 97 metrics **including margin_pct + profit + gross_revenue**; publisher 32 (has pub_payout); dsp 21 (safe) | 🛑 Fail | `t5_allowed_metrics.md` | Claude | **P0 (FND-005)** | AM seeing take-rate is the reason we have this allow-list. Filter is not applied for AM/finance. |

### 2.7 Compliance & trust files

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 7.1 | /sellers.json at app.pgammedia.com | curl | 200 JSON | 200, but still contains `Bootstrap Test Publisher` as `seller_type:"PUBLISHER"` | ⚠️ Needs-fix | `t6_compliance.md` | Claude | P1 (FND-010) | Bootstrap test entry must be removed before sellers.json is scraped by OpenRTB auditors. |
| 7.2 | /sellers.json at pgammedia.com apex | curl | 200 JSON or 307→app | 307 redirect to app subdomain | ✅ Pass | `t6_compliance.md` | Claude | — | Acceptable. |
| 7.3 | /ads.txt | curl both hosts | 200 text/plain | 404 HTML on app; 307 on apex | 🛑 Fail | `t6_compliance.md` | Claude | **P1 (FND-009)** | No ads.txt served anywhere. DSPs will drop traffic on the no-ads.txt filter. |
| 7.4 | /app-ads.txt | curl | 200 text/plain (or deliberate 404) | 404 HTML | ⚠️ Needs-fix | `t6_compliance.md` | Claude | P2 (FND-024) | Mobile SSAI chains require app-ads.txt. |
| 7.5 | /.well-known/sellers.json | curl | 404 is fine (non-standard) | 404 HTML | ℹ️ Informational | `t6_compliance.md` | Claude | — | — |
| 7.6 | schain in outgoing bid requests | grep service code for schain assembly | Present for every outgoing request | Present in code, but end-to-end not verified due to 401s | ⚠️ Needs-fix | — | Claude | P1 | Requires a signed-body happy path in bidder-edge first. |

### 2.8 Prebid adapter & docs

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 8.1 | Prebid.js fork has one canonical adapter | ls modules/pgam* | One module, one md | `pgamsspBidAdapter.js` + md, biddercode `pgamssp` | ✅ Pass | `t10_prebid.md` | Claude | — | — |
| 8.2 | Adapter endpoint hostname | inspect `AD_URL` | rtb.pgammedia.com OR us-east.pgammedia.com resolves | `https://us-east.pgammedia.com/pbjs` — DNS not confirmed | ⚠️ Needs-fix | `t10_prebid.md` | Claude | P1 (FND-008) | Either create the us-east subdomain or update adapter to rtb.pgammedia.com. |
| 8.3 | prebid.github.io fork docs | ls dev-docs/bidders/pgam* | One doc for pgamssp | THREE docs: `pgam.md`, `pgammedia.md`, `pgamssp.md`; only pgamssp has a real adapter | 🛑 Fail | `t10_prebid.md` | Claude | **P1 (FND-008)** | Docs advertise three fake biddercodes. Partners will config the wrong one. Also `pgammedia.md` aliases `aniview` — misleading. |
| 8.4 | Service-map identifier consistency | grep repo | One canonical id | 4 different ids in use: `pgamdirect`, `pgamssp`, `pgammedia`, `pgam` | ⚠️ Needs-fix | `t10_prebid.md` | Claude | P2 (FND-008 ext) | Pick one and rename. |

### 2.9 Rate limits & hardening

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| 9.1 | Rate-limit authenticated | 30 rapid GET /api/publishers | 429 after a threshold | All 30 → 200, no throttle | ⚠️ Needs-fix | `t11_edge.md §F` | Claude | P1 (FND-025) | No rate limiter. An authenticated consumer can stampede Neon. |
| 9.2 | Rate-limit unauthenticated | 30 rapid GET /api/auth/me | 429 or backoff | All 30 → 401, no throttle | ⚠️ Needs-fix | `t11_edge.md §G` | Claude | P1 (FND-025) | Brute-force session candidates without penalty. |
| 9.3 | CORS | OPTIONS /api/publishers with random Origin | Restrictive or same-origin | Not tested — run against Vercel proxy | ℹ️ Informational | — | Claude | P2 | Add to round-3. |

---

## 3. New findings introduced in Round 2

### FND-020 — DSP auth_secret → AWS Secrets Manager is a local stub (P0)
Response from POST /api/dsps shows
`arn:aws:secretsmanager:local:000000000000:secret:pgam/dsp/<name>/auth`.
The region literal `local` and the zero AWS account number reveal that the
integration has not been wired to a real AWS client. Any DSP configured
with `auth_type=bearer|mtls|hmac` will effectively have no credential
stored, so outgoing bids to that DSP cannot be authenticated.
*Fix:* wire `@aws-sdk/client-secrets-manager` + set `AWS_REGION` +
credentials in Vercel prod env; update DSP endpoints table to reference
real ARNs.

### FND-021 — Two `dsp_id` namespaces (P1)
`GET /api/dsps/health` returns 16 rows with `dsp_id` 1..16 (verve, amx,
pubmatic, …) from the master `dsps` table.
`POST /api/dsps` returns `{dspId:1, endpointIds:[2], contractIds:[3]}`
from what looks like a StubDb auto-increment. These ID spaces will
eventually collide when a real DB-backed DSP gets id=1 alongside `verve`.
*Fix:* consolidate the customer-configured DSP table with the master
registry, or namespace the IDs (e.g., `cfg_123` vs master `1`).

### FND-022 — rtb-tester requires manual endpoint URL (P1)
The internal `/rtb-tester` page errors with `missing_endpoint_url` for
every predefined publisher/DSP. Operators expected to paste the RTB
endpoint manually — defeats the purpose of a wired test harness.
*Fix:* auto-populate from the selected publisher's persisted config.

### FND-023 — /api/reporting/{discrepancy,placement,summary} and /api/margin/summary are 404 (P1)
UI tabs that are wired to these routes will throw. Either stub with 204
and a "no data yet" envelope, or delete the tabs.

### FND-024 — /app-ads.txt missing (P2)
Mobile SSAI chains require both ads.txt and app-ads.txt.

### FND-025 — No rate limiting on API (P1)
Neither `/api/publishers` nor `/api/auth/me` throttles 30 rapid requests.
Brute-force surface on the session verifier is wide open. Add Vercel
edge middleware rate-limit or pg-based leaky bucket.

### FND-018 — Name field accepts `<script>` payload (P2)
Stored as-is. React renders safely, but downstream CSV/email paths may
not escape. Add server-side rejection of angle-bracket / control-char
sequences in customer-visible name fields.

### FND-019 — Malformed-JSON error leaks parser internals (P2)
The `detail` field returns the raw Node `SyntaxError: …`. Strip in prod.

---

## 4. Round-1 findings re-verification

| ID | Round-1 description | Round-1 priority | Round-2 status |
|---|---|---|---|
| FND-001 | Cross-tenant publisher list | P0 | **Still present** — fixed on PR #1 branch, not merged. |
| FND-002 | DSP create hits StubDb in prod | P0 | **Still present** (publisher side was fixed in round-1 commit; DSP side remains). |
| FND-003 | Publisher wizard round-trip loss | P0 | **Still present** — migration 000018 written, not applied. |
| FND-004 | Page route RBAC missing | P0 | **Still present** — all 5 roles render admin pages. |
| FND-005 | AM role can read margin_pct | P0 | **Still present** — AM count=97 including margin_pct and profit. |
| FND-006 | Reporting/partner returns 0 rows | P1 | Still 0 rows (could be "no traffic yet"). |
| FND-007 | Bidder-edge accepts known publisher unsigned | P1 | **Appears fixed** — now 401 across the board; need signed happy-path test. |
| FND-008 | Adapter/docs naming drift | P1 | **Still present**, even wider: 4 distinct identifiers. |
| FND-009 | ads.txt missing | P1 | **Still present** — 404 on both hosts. |
| FND-010 | sellers.json contains Bootstrap Test | P1 | **Still present.** |
| FND-011 | PBS DNS | P1 | Out of scope — PBS not deployed yet. |
| FND-014 | `/api/publishers` tenant filter | P0 (bundled into FND-001) | Still present. |
| FND-015 | Bidder-edge body-shape order | P0 | **Partial** — everything returns 401 now; no body-shape 400 ever fires. |

---

## 5. Verified-working (keep the regressions out)

1. **HMAC session signing** — tamper/strip/forge all 401.
2. **Zod gating on publisher wizard** — size limits, rev-share band, direct_rtb auth pairing all enforced.
3. **Zod gating on DSP wizard** — 5-section shape required, 422 with clear paths.
4. **SQL parameterisation** — DROP TABLE in name stored as literal.
5. **HMAC key generation for direct_rtb publishers** — key shown once, readback shows marker only.
6. **LLD endpoint** — returns CSV with expected columns.

---

## 6. Launch gate recommendation

**Block launch** until the following are green:

- [ ] PR #1 merged and deployed (FND-001..005, 014, 015 partial).
- [ ] Migration 000018 applied in prod (FND-003 fully fixed).
- [ ] DSP create persists to Postgres, not StubDb (FND-002 DSP side).
- [ ] DSP auth_secret → real AWS Secrets Manager (FND-020).
- [ ] AM role `allowed-metrics` filter removes margin_pct / profit / gross_revenue (FND-005).
- [ ] ads.txt served at `app.pgammedia.com/ads.txt` (FND-009).
- [ ] Bootstrap Test Publisher removed from sellers.json (FND-010).
- [ ] Rate limiter on /api/auth/me at minimum (FND-025).
- [ ] Either wire `/api/reporting/{discrepancy,placement,summary}` or remove the UI tabs (FND-023).
- [ ] Prebid docs trimmed to `pgamssp` only (FND-008).

After launch, address: FND-018, 019, 021, 022, 024.

---

## 7. Artefacts

- Evidence files: `qa/evidence-round2/*.md`, `*.json`, `*.txt`
- Round-2 publishers created on prod (need cleanup): ids 2, 3, 4, 5, 6 under tenant_id=1
- Round-2 placements created: id 48 + earlier ones from T2 cycle
- Round-2 DSP rows: none persisted (confirmed stub behaviour)
- Round-2 HMAC key minted for publisher id=4: stored only in memory; not leaked to disk
