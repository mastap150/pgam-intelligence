# PGAM SSP Pre-Launch QA Cycle

**Target:** `https://app.pgammedia.com/` (PGAM Direct SSP — admin + partner portal)
**Code:** [mastap150/pgam-direct](https://github.com/mastap150/pgam-direct) · [mastap150/Prebid.js](https://github.com/mastap150/Prebid.js) · [mastap150/prebid.github.io](https://github.com/mastap150/prebid.github.io)
**Date of test cycle:** 2026-04-23 → 2026-04-24
**Tester:** Claude (automated pre-launch QA under supervision of Priyesh Patel)
**Build phase observed:** Phase 1 foundation scaffold

---

## 1. Executive summary — launch readiness

| Question | Verdict |
|---|---|
| Can we confidently onboard an external partner today? | **NO.** |
| Showstoppers found? | **Yes — 5 P0, 6 P1, 10+ P2.** |
| Recommended action | **Fix all P0 + P1 items below before first partner touches the platform.** Additional smoke cycle after fixes. |

**P0 showstoppers (block launch):**

1. **Cross-tenant publisher data leak** — any authenticated role (publisher, DSP, AM, finance) can read *any other* publisher's full config via `GET /api/publishers` and `GET /api/publishers/{id}`, including `auth_secret_ref`, `pbs_stored_req_id`, financial terms, and integration payload. See §6.1 finding FND-001.
2. **DSP create writes to one table, reads from another** — `POST /api/dsps` returns 201 with an ID, but the new row is invisible in the list (`GET /api/dsps`, `GET /api/dsps/health`, `/api/admin/dsps`). Effectively a no-op from the operator's perspective. FND-002.
3. **Publisher detail drops fields on read-back** — immediately after create returns `placement_ids:[41,42]` and the full basics payload, `GET /api/publishers/{id}` returns `placement_ids:[]` and only `name`, `timezone`, `currency`. Other fields (legal_entity, contact_email, financial terms, integration config) are silently dropped on the Neon round-trip. FND-003.
4. **Page-level RBAC is missing on most `/admin/*` routes** — publisher and DSP sessions get HTTP 200 and the page shell for `/admin/users`, `/admin/floors`, `/admin/blocklist`, `/admin/traffic`, `/admin/health`, `/admin/infra`, `/admin/publishers`, `/admin/dsps`, `/publishers/new`, `/rtb-tester`. Only `/admin/deals`, `/admin/agentic` and `/dsps/new` redirect or 404 correctly. FND-004.
5. **AM role allowed-metrics set is wrong** — `GET /api/rbac/allowed-metrics` as `am` returns the same 97-metric whitelist as `internal_admin`/`finance`, including `margin_pct`. Per the design (docs + code comments), AM is supposed to see everything **except** `margin_pct`. FND-005.

**P1 important:**

6. **`/api/reporting/partner` vs `/api/lld` data-source mismatch** — LLD endpoint returns 6.4 KB of CSV bid-outcome data; `reporting/partner` for the same date window returns `row_count:0`. Partners looking at their partner reporting will see empty while LLD shows activity. FND-006.
7. **Bidder authentication model is unclear / may be open** — `https://rtb.pgammedia.com/rtb/v1/auction` returns 401 `unauthenticated` for `{}` and junk bodies but 204 (no-bid) for a well-formed OpenRTB 2.6 request with just `site.publisher.id` set — no HMAC, no bearer token supplied. Needs confirmation whether the endpoint is actually enforcing partner auth or merely body-shape validation disguised as auth. FND-007.
8. **Prebid.js adapter identity mismatch between repos** — the Go backend service map references adapter `pgamdirect` → `rtb.pgammedia.com/rtb/v1/auction`. The Prebid.js fork actually ships `pgamssp` → `us-east.pgammedia.com/pbjs`. The docs fork additionally has `pgam` (removed in 8.13, obsolete) and `pgammedia` (aniview alias). Three documented names, one current shipped name, and none match the backend reference. FND-008.
9. **`/sellers.json` on app.pgammedia.com contains a bootstrap test entry** — production sellers.json lists a `"Bootstrap Test Publisher"` with seller_id `pgam-test-1`. This must be removed / replaced with real sellers before any DSP scrapes it. Also there's a second copy on `pgammedia.com/sellers.json` (25 KB) that times out under 8s — two independent sellers.json sources. FND-009.
10. **`pgammedia.com/ads.txt` returns HTML "Redirecting…" instead of text** — ads.txt on the apex domain is not being served as `text/plain` and has body `"Redirecting..."` (2 KB). Any ads.txt monitor or crawler will treat the site as having no ads.txt. FND-010.
11. **`pbs.pgammedia.com` DNS does not resolve** — the backend service map documents PBS as a first-class integration at `pbs.pgammedia.com/openrtb2/auction`; hostname currently fails DNS. Either remove the doc or provision the host before promising s2s to partners. FND-011.

**P2 notable (list in §6).**

---

## 2. Environment & access

| Item | Value |
|---|---|
| Primary app | `https://app.pgammedia.com` (Vercel, Next.js 14) |
| Login mode | Passwordless dev — any email + role picker (`/login`). POST `/api/auth/dev-login` issues a 12h `pgam_session` HMAC-signed cookie. |
| Auth roles observed | `internal_admin`, `finance`, `am`, `publisher`, `dsp` (publisher/dsp need `partner_id`) |
| Bidder edge | `https://rtb.pgammedia.com/rtb/v1/auction` (Fly.io, iad region). `/healthz` 200. |
| Prebid.js adapter endpoint | `https://us-east.pgammedia.com/pbjs` (nginx, returns 204 for all probes) |
| User sync | `https://cs.pgammedia.com/iframe?pbjs=1` — 200 HTML |
| Sellers.json | `https://app.pgammedia.com/sellers.json` (472 B) **and** `https://pgammedia.com/sellers.json` (25 KB, slow) |
| PBS s2s | `https://pbs.pgammedia.com/openrtb2/auction` — **DNS fails** |
| ads.txt apex | `https://pgammedia.com/ads.txt` — returns HTML redirect (broken) |
| Backend stack | Go services (bidder-edge, pbs, pixel, api, config-pusher, event-gateway, rule-evaluator, revert-worker, ml-serving, sellers-json-publisher, webhook-worker) + Next.js web + Neon Postgres + ClickHouse + Redis + Kafka |

**Cookie capture commands used** (for re-running any test):
```bash
# as internal_admin
curl -si -X POST https://app.pgammedia.com/api/auth/dev-login \
  -H 'content-type: application/json' \
  -d '{"email":"qa-admin@pgammedia.com","role":"internal_admin"}' \
  | grep -i '^set-cookie: pgam_session=' | sed -E 's/^.*: (pgam_session=[^;]+).*/\1/'

# as publisher (scoped to a partner_id)
curl -si -X POST https://app.pgammedia.com/api/auth/dev-login \
  -H 'content-type: application/json' \
  -d '{"email":"qa-publisher@pgammedia.com","role":"publisher","partner_id":"qa-partner-1"}' \
  | grep -i '^set-cookie: pgam_session='
```

All session cookies captured in `qa/evidence/*_session.txt` (gitignored).

**Test artifacts left in prod DB** (please clean up before a real partner onboards):
- Publisher #2 `qa-test-publisher-pre-launch` (org_id slug) with placements #41, #42 and an HMAC key that was rotated into Neon.
- DSP create attempts (IDs reported as 1 by the API but possibly orphan rows).

---

## 3. Test methodology

1. **Architecture mapping** — three parallel subagent explorations of the repos (backend services, web frontend, Prebid forks). Output in this doc's §A-C appendix.
2. **Public surface probe** — curl every documented endpoint, record HTTP + content-type + size.
3. **Auth** — captured one `pgam_session` cookie per role via dev-login; all role tests use these cookies.
4. **CRUD flows** — create publisher, create DSP, generate HMAC, list, detail, RBAC.
5. **Bid flow** — fire real OpenRTB 2.6 request at `rtb.pgammedia.com/rtb/v1/auction` via both the server-side test-bid proxy and direct curl; fire teqblaze-format payload at `us-east.pgammedia.com/pbjs`.
6. **Reporting** — compare `/api/reporting/partner`, `/api/reporting/matrix`, `/api/lld`, `/api/live/snapshot`, `/api/dsps/health`, `/api/analytics/attention` under admin session.
7. **UI smoke** — HTTP-level GET of every route for every role (admin full matrix; publisher/dsp spot-check on admin pages).
8. **Bugs captured as findings** (FND-###) in §6 with severity + recommended fix.

Volume discipline — per user instruction, bid requests sent at single-digit volume against RTB + pbjs endpoints, well below anything any SSP abuse monitor would flag.

---

## 4. Test matrix

Columns: Scenario · Steps · Expected · Actual · Status · Evidence · Owner · Priority · Notes.

Legend: ✅ Pass, ❌ Fail, ⚠️ Partial, 🛑 Blocked.
Priority: P0 launch blocker, P1 ship-critical, P2 important, P3 polish.

### 4.1 Authentication & session

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| A01 | Login page renders | `GET /login` | 200, HTML | 200, 12.8 KB HTML | ✅ | §2 curl | Web | P3 | — |
| A02 | Dev-login issues session cookie | `POST /api/auth/dev-login` with email+role | 303 to `/`, Set-Cookie `pgam_session=...`, HttpOnly + Secure + 12h max-age | 303, HttpOnly, Secure, SameSite=lax, Max-Age=43200 | ✅ | evidence/admin_session.txt | Web | P2 | — |
| A03 | `/api/auth/me` reflects role | GET with cookie | JSON with email, role, tenantId | `{"userId":"dev-user","email":"qa-admin...","role":"internal_admin","tenantId":"1","partnerId":null,"mfaVerified":true}` | ✅ | transcript | Web | P2 | — |
| A04 | Dev-mode on prod | `NEXT_PUBLIC_ALLOW_DEV_LOGIN` | Should be OFF in prod | **ON in prod** — any email wins any role | ❌ | transcript | Web | P0 | Do not go live with partners while dev-login works. Gate on env var + kill switch. |
| A05 | Session cookie is HMAC-signed (not just base64) | Decode payload, verify `.sig` | Cookie format `base64(json).base64(HMAC-SHA256)` | Confirmed in code (`lib/auth.ts`), session parses with sig | ✅ | code audit | Web | P2 | — |
| A06 | `/api/auth/me` 401 without cookie | GET no cookie | 401 | 401 `{"error":"unauthenticated"}` | ✅ | §2 | Web | P2 | — |

### 4.2 Partner onboarding — publishers

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| P01 | Admin creates publisher (prebid_s2s, 2 placements) | POST `/api/publishers` with wizard payload | 201 + publisher row + placement_ids populated | 201, id=2, org_id=`qa-test-publisher-pre-launch`, placement_ids=[41,42] | ✅ | evidence/publisher_create_response.txt | Web+Backend | P1 | — |
| P02 | Publisher appears in list after create | GET `/api/publishers` | Contains new row | Row #2 present, status active | ✅ | transcript | Web | P1 | — |
| P03 | GET publisher detail rehydrates placements + basics | GET `/api/publishers/2` | placement_ids=[41,42], full basics echoed | `placement_ids:[]`, only `name,timezone,currency` back — legal_entity, contact_email, contact_phone, full financial, integration stripped | ❌ | transcript | Web+Backend | **P0** (FND-003) | DB denormalisation of placement_ids and payload round-trip both broken. |
| P04 | Placements persisted separately | GET `/api/admin/publishers/2/placements` | Both placements with correct refs/floors | ✅ Both returned correctly | ✅ | transcript | Backend | P2 | Confirms write is fine, read-back denorm is broken. |
| P05 | HMAC key generation | POST `/api/publishers/2/secrets/generate-hmac` | One-time 64-hex key + warning, auth_secret_ref populated | ✅ key `6d8cbbdf...a78336`, auth_secret_ref `neon://publisher_configs/2/hmac_key` on re-fetch | ✅ | transcript | Backend | P2 | — |
| P06 | HMAC gen is internal_admin-only | POST with publisher cookie | 403 | 403 FORBIDDEN | ✅ | transcript | Web | P1 | — |
| P07 | Publisher create is internal_admin-only (API) | POST as publisher/dsp/am/finance | 403 | All 403 with correct error | ✅ | transcript | Web | P1 | AM being blocked disagrees with the route.ts doc comment ("internal_admin + am only") but matches the code — doc/comment is stale (minor P3). |
| P08 | Invalid placement_ref (spaces) rejected | POST with `"has spaces"` | 400 VALIDATION, path `inventory[0].placements[0].placement_ref` | ✅ 400 with exact zod issue | ✅ | transcript | Web | P2 | — |
| P09 | direct_rtb without HMAC or CIDR rejected | POST with both auth fields empty | 400 VALIDATION | ✅ 400 with custom message | ✅ | transcript | Web | P2 | — |
| P10 | Duplicate org_id handling | POST same payload twice | 409 `org_id_conflict` OR new slug | Got 201 with new `pre-launch-2` slug — good | ✅ | (not captured, behavior of slug reserve) | Backend | P3 | — |
| P11 | Publisher wizard UI accessible | GET `/publishers/new` as admin | 200 HTML | 200, 43 KB | ✅ | §4.7 | Web | P3 | UI-level steps not clicked through — API exercised. Needs a manual walk-through before launch. |

### 4.3 Partner onboarding — DSPs

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| D01 | Admin creates DSP | POST `/api/dsps` with wizard payload (net take_rate 20%, qps 10, us-east-1) | 201 + dspId + endpointIds + contractIds | 201 `{"dspId":1,"endpointIds":[2],"contractIds":[3]}` | ⚠️ | evidence/dsp_create_response.txt | Backend | **P0** (FND-002) | Created but not queryable — see D02. |
| D02 | New DSP appears in list | GET `/api/dsps` | Contains dspId=1 | `{"items":[]}` — empty | ❌ | transcript | Backend | **P0** (FND-002) | Write goes to `accounts.dsps`, read queries return empty. GET probably has a tenant_id type mismatch OR deleted_at is being set. |
| D03 | New DSP appears in health | GET `/api/dsps/health` | Contains qa-test-dsp | 16 seed DSPs (verve, pubmatic, magnite, 33across, amx, illumin, loopme, onetag, zmaticoo, sovrn, openweb, perion, synatix, stirista, growintech, unruly) — none are mine | ❌ | transcript | Backend | **P0** (FND-002) | `/api/dsps/health` reads `pgam_direct.dsp_configs` (legacy schema). Creates go to `accounts.dsps` (new schema). Pick one and migrate. |
| D04 | `/api/admin/dsps` reads legacy table | GET `/api/admin/dsps` | Same seed list | id=1 verve, 2 amx, etc. | ⚠️ | transcript | Backend | P1 | Adds to schema confusion — admin-list, admin-dsps, /api/dsps all touch different tables. |
| D05 | Take-rate <5% rejected | POST with `take_rate_pct:2` | 422 VALIDATION_FAILED at `commercial.take_rate_pct` | ✅ 422 with exact zod issue | ✅ | transcript | Backend | P1 | 5% hard floor correctly enforced at wizard + API. |
| D06 | DSP create is admin-only | POST as dsp/publisher/am/finance | 403 | ✅ 403 for all | ✅ | transcript | Web | P1 | — |
| D07 | DSP list is admin-only | GET `/api/dsps` as dsp | 403 FORBIDDEN | ✅ 403 | ✅ | transcript | Web | P1 | Good — but admin's own list is empty (D02). |
| D08 | Non-HTTPS endpoint rejected | POST with `http://...` endpoint_url | 422 on `technical.endpoint_url` | Not tested — covered by zod `.refine(startsWith("https://"))` | 🛑 | code read | Backend | P2 | Recommend add as auto-test. |
| D09 | Multi-region QPS split | POST with 2+ regions | Endpoints split QPS proportionally | Not tested with multi-region | 🛑 | — | Backend | P2 | `splitQps` utility exists; needs explicit integration test. |

### 4.4 Placements & tag generation

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| T01 | Placement IDs auto-generated | Create publisher with 2 placements | Two numeric IDs returned, both referenced on `placement_ref` | 41, 42 | ✅ | P01 | Backend | P1 | — |
| T02 | `placement_ref` becomes `imp.tagid` on wire | Confirm from adapter code | placement_ref is the tagid the DSP sees | Confirmed in `teqblazeUtils/bidderUtils.ts:188-198`: type=publisher when placementId set | ✅ | code audit | Prebid | P2 | — |
| T03 | Integration-mode-specific snippet displayed | UI `/publishers/2` detail → Integration tab | Wizard renders snippet/endpoint per mode | UI page loads (200) but this tab's rendering not pixel-verified (preview tools require local server) | 🛑 | — | Web | P1 | Needs manual walk-through. Code comment at web summary notes "Partial — Integration tab rendering not fully built". |
| T04 | Generated tag round-trips to backend | Use placement_ref in a bid request | Bidder resolves placement, returns response | 204 no-bid (no DSPs wired to this new publisher yet) | ⚠️ | evidence/rtb_test_bid.txt | Backend | P1 | Need at least one `accounts.dsps` → `pgam_direct.dsp_configs` bridge or re-seed before you can actually auction a new publisher. |
| T05 | HMAC display-once buffer | Request generate-hmac twice | First returns key; second returns warning | Not tested — requires two live requests in same cold-start window | 🛑 | — | Backend | P3 | — |

### 4.5 Live bid flow

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| B01 | `/healthz` | curl GET | 200 text/plain | 200, body "ok" (15 B) | ✅ | §2 | Backend | P3 | — |
| B02 | `POST /rtb/v1/auction` valid OpenRTB 2.6, no auth header | curl with QA publisher org_id + placement | Either 204 no-bid OR 200 bidresponse OR 401 | **204 no-bid, 289 ms** via proxy, 866 ms direct | ⚠️ | evidence/rtb_test_bid.txt | Backend | **P1** (FND-007) | Concerning: accepted anonymously. Need clarity on auth model. |
| B03 | `POST /rtb/v1/auction` `{}` body | curl with empty JSON | 400 malformed | 401 `unauthenticated` | ❌ | transcript | Backend | P2 | Wrong error code — auth runs after shape check should be the other way. |
| B04 | `POST /rtb/v1/auction` `not-json` body | curl plain text | 400 invalid json | 401 `unauthenticated` | ❌ | transcript | Backend | P2 | Same as B03. |
| B05 | `/api/rtb/test-bid` echoes request+response (admin only) | POST via tester | 200 with headers, body, duration | 200, duration=289 ms, response=204 from upstream | ✅ | evidence/rtb_test_bid.txt | Web | P2 | — |
| B06 | `us-east.pgammedia.com/pbjs` teqblaze-format POST | POST with placements[] including `qa_leaderboard_top` | 200 JSON seatbid or 204 no-bid | **204 for everything** including GET; no content-type on 204 | ⚠️ | evidence/pbjs_resp.txt | Prebid | **P1** (FND-012) | 204 on GET is suspicious; confirm whether the endpoint has any actual bidder wired behind nginx, or it's a terminating stub. |
| B07 | CORS on RTB endpoint | OPTIONS preflight | ACAO + allowed headers/methods | access-control-allow-origin: *, allow-methods POST,OPTIONS | ✅ | §2 | Backend | P3 | — |
| B08 | Schain on outbound | Confirm from code + wire | schain complete=1 enforced; BuildSchain refuses incomplete | Confirmed in code; not exercised on wire | ✅ | code audit | Backend | P1 | Needs one real end-to-end auction trace to confirm schain reaches the DSP response. |
| B09 | Impression pixel endpoint | GET `/rtb/v1/imp?...&t=<hmac>` | 204 with 1×1 GIF semantics | Not tested — requires HMAC-signed token from a real auction | 🛑 | — | Backend | P1 | Cannot be exercised until we can actually run an auction end-to-end. |
| B10 | Click pixel endpoint | GET `/rtb/v1/click?r=...&t=...` | 302 to landing URL | Not tested | 🛑 | — | Backend | P1 | Same gating as B09. |
| B11 | VAST event pixel | GET `/rtb/v1/vast_event?event_type=start&t=...` | 204 No Content | Not tested | 🛑 | — | Backend | P1 | Same. |

### 4.6 Reporting & analytics

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| R01 | `/api/reporting/partner` default window | GET admin | JSON with rows | `row_count:0, rows:[]` for 2026-03-25 → 2026-04-24 | ⚠️ | transcript | Backend | **P1** (FND-006) | Conflicts with R03. |
| R02 | `/api/reporting/matrix` | GET admin | Pivot cells | `cell_count:0` | ⚠️ | transcript | Backend | P1 | Same pipeline gap. |
| R03 | `/api/lld` (low-level detail) | GET admin | CSV bid outcomes | 6.4 KB CSV with real rows | ✅ | transcript | Backend | P1 | Activity exists — so R01/R02 are definitely wired wrong. |
| R04 | `/api/dsps/health` | GET admin | 24h metrics per DSP | 16 DSPs listed, all with 0 calls | ⚠️ | transcript | Backend | P2 | All zero calls indicates the health rollup isn't populated by recent traffic. |
| R05 | `/api/live/snapshot` | GET admin | Last 50 auctions | `last_auctions:[], wins_60m:0, total_revenue_60m:"0"` | ⚠️ | transcript | Backend | P2 | Same pipeline concern. |
| R06 | `/api/analytics/attention` | GET admin | Attention-index rollup | `count:17, avg_com...` OK | ✅ | transcript | Backend | P3 | CTV attention engine rollup is live. |
| R07 | `/api/admin/health` | GET admin | status=ok | `status: degraded` — recent_traffic_10min 0 auctions; bidder_traffic_state 0 endpoints in breaker state | ⚠️ | transcript | Backend | P2 | Health rollup flagging, confirming the traffic pipeline is quiet. |
| R08 | `/api/admin/infra` | GET admin | Fleet summary | `machines_total:5, machines_running:5, regions:4, auctions_24h:3` | ✅ | transcript | Ops | P3 | Only 3 auctions in 24 h — we have not been driving real bid volume. |
| R09 | CSV export on reporting pages | — | Download CSV | Not implemented in UI per web explore report | ❌ | code audit | Web | P2 | Spec says "CSV download" everywhere; buttons/logic missing. |
| R10 | Allowed-metrics per role | GET `/api/rbac/allowed-metrics` as each role | publisher 32, dsp 21, am=?, finance 97, admin 97 | admin 97, finance 97, am **97**, publisher 32, dsp 21 | ❌ | transcript | Backend | **P0** (FND-005) | AM should not see margin_pct. |

### 4.7 UI smoke (HTTP-level) + page-level RBAC

Every page below was GETed with a valid admin `pgam_session` cookie. All returned 200 HTML.

| Path | Admin | Publisher | DSP | Status | Notes |
|---|---|---|---|---|---|
| `/` | 200 (46 KB) | 200 (expected — home) | 200 | ✅ | — |
| `/login` | 200 | 200 | 200 | ✅ | — |
| `/publishers` | 200 | 200 (list leaks — FND-001) | 200 (list leaks) | ⚠️ | — |
| `/publishers/new` | 200 | **200 reachable by publisher** | **200 reachable by dsp** | ❌ | P0 — see FND-004 |
| `/publishers/2` | 200 (58 KB) | — | — | ✅ | — |
| `/dsps` | 200 (79 KB — renders seed DSPs) | — | — | ✅ | — |
| `/dsps/new` | 200 | 404 (correct) | 200 (leaks) | ❌ | — |
| `/reporting` | 200 | 200 | 200 | ✅ | Role-scoping must happen server-side in API; page ok. |
| `/reporting/matrix` | 200 | — | — | ✅ | — |
| `/reporting/lld` | 200 | — | — | ✅ | — |
| `/admin/users` | 200 | **200 reachable** | **200 reachable** | ❌ | P0 — see FND-004 |
| `/admin/deals` | 200 | 302→`/` ✅ | 302→`/` ✅ | ✅ | Proper gate. |
| `/admin/floors` | 200 | **200 reachable** | — | ❌ | Leaks |
| `/admin/blocklist` | 200 | — | — | ✅ | — |
| `/admin/health` | 200 | — | — | ✅ | — |
| `/admin/infra` | 200 | — | — | ✅ | — |
| `/admin/traffic` | 200 | — | — | ✅ | — |
| `/admin/agentic` | 200 | 302→`/` ✅ | — | ✅ | Proper gate. |
| `/admin/publishers` | 200 | **200 reachable** | — | ❌ | Leaks |
| `/admin/dsps` | 200 | — | — | ✅ | — |
| `/rules` | 200 | — | — | ✅ | — |
| `/combo-lists` | 200 | — | — | ✅ | — |
| `/rtb-tester` | 200 | **200 reachable** | **200 reachable** | ❌ | Leaks — tool is admin-only per code; page shell not gated. |
| `/live` | 200 | — | — | ✅ | — |
| `/discrepancy` | 200 | — | — | ✅ | — |
| `/discrepancy/import` | 200 | — | — | ✅ | — |
| `/compliance/sellers-json` | 200 | — | — | ✅ | — |
| `/compliance/ads-txt` | 200 | — | — | ✅ | Body empty-state per code. |
| `/spo` | **307** redirect | — | — | ⚠️ | admin redirected somewhere; code says DSP-only. |

### 4.8 Discrepancy & reconciliation

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| DC01 | `/discrepancy/import` page loads | GET admin | 200 HTML | 200, 55 KB | ✅ | §4.7 | Web | P3 | UI path confirmed. |
| DC02 | `/api/discrepancy/our-counts` | GET | Our-side rollups | 405 (POST only) | ⚠️ | transcript | Backend | P2 | Route exists but GET not supported; confirm UI calls POST correctly. |
| DC03 | `/api/discrepancy/compare` | GET | JSON reconciliation | Not tested (requires partner-report import first) | 🛑 | — | Backend | P1 | — |
| DC04 | Import CSV end-to-end | Upload CSV, compare | Rows land in reconciliation | Not tested | 🛑 | — | Backend | P1 | — |

### 4.9 Compliance

| # | Scenario | Steps | Expected | Actual | Status | Evidence | Owner | Priority | Notes |
|---|---|---|---|---|---|---|---|---|---|
| C01 | `app.pgammedia.com/sellers.json` | curl | IAB sellers.json with real sellers only | Contains bootstrap test publisher entry | ⚠️ | §2 | Compliance | **P1** (FND-009) | Remove `pgam-test-1` before DSPs crawl. |
| C02 | `pgammedia.com/sellers.json` | curl | Same content as app | Different content (25 KB vs 472 B), timed out under 8 s | ❌ | §2 | Compliance | **P1** (FND-009) | Two independent sellers.json. Consolidate. |
| C03 | `pgammedia.com/ads.txt` | curl | text/plain ads.txt | Returns "Redirecting…" HTML 2 KB | ❌ | §2 | Compliance | **P1** (FND-010) | — |
| C04 | `/compliance/ads-txt` dashboard | GET | Monitoring state | "No ads.txt checks yet" empty state | ⚠️ | web explore | Compliance | P2 | Monitoring not wired. |
| C05 | `/compliance/sellers-json` page | GET admin | Admin sellers.json authorship | 200, 40 KB HTML | ✅ | §4.7 | Compliance | P3 | UI path reachable; authoring flow not exercised. |
| C06 | `/api/sellers-migration-status` | GET admin | TB URL + fetch status | `tb_url: sellers.pgamssp.com/.../sellers.json, tb_reachable:true` | ✅ | transcript | Compliance | P2 | Old TeqBlaze sellers.json still live — confirm this is intentional during cutover. |

### 4.10 Permissions matrix (API)

| API path | admin | finance | am | publisher | dsp | Expected | Notes |
|---|---|---|---|---|---|---|---|
| GET `/api/publishers` | 200 (all) | — | 200 (all) | **200 (all)** ❌ | **200 (all)** ❌ | publisher: own only; dsp: 403 | FND-001 |
| GET `/api/publishers/{id}` | 200 | — | — | **200 (any)** ❌ | — | publisher: own only; else 403/404 | FND-001 |
| POST `/api/publishers` | 201 | 403 | 403 | 403 | 403 | admin only | ✅ correct |
| POST `/api/publishers/{id}/secrets/generate-hmac` | 200 | — | — | 403 | — | admin only | ✅ |
| GET `/api/dsps` | 200 | — | — | — | 403 | admin only | ✅ |
| POST `/api/dsps` | 201 (but not visible) | — | — | — | 403 | admin only | ✅ auth, ❌ persistence |
| GET `/api/users` | 200 | 403 | 403 | 403 | 403 | admin only | ✅ |
| GET `/api/admin/deals` | 200 | — | — | 403 | — | admin only | ✅ |
| GET `/api/admin/infra` | 200 | — | — | 403 | 403 | admin only | ✅ |
| GET `/api/admin/publishers/{id}/placements` | 200 | — | — | 403 | — | admin only | ✅ |
| GET `/api/config/*` | **403** | — | — | — | — | admin (expected) | ⚠️ — even admin can't read? These routes exist in code. See FND-013. |
| GET `/api/rbac/allowed-metrics` | 200 (97) | 200 (97) | **200 (97)** ❌ | 200 (32) | 200 (21) | AM ≠ admin/finance set | FND-005 |

### 4.11 API integrations & external

| # | Scenario | Steps | Expected | Actual | Status | Priority | Notes |
|---|---|---|---|---|---|---|---|
| I01 | `rtb.pgammedia.com/healthz` | curl | 200 "ok" | ✅ 200, 15 B, 572 ms | ✅ | P2 | — |
| I02 | `us-east.pgammedia.com/pbjs` | POST with prebid body | 200 seatbid OR 204 | 204 for all inputs incl. GET | ⚠️ | P1 | FND-012 |
| I03 | `cs.pgammedia.com/iframe` | GET | HTML sync page | 200, 9.6 KB HTML | ✅ | P2 | — |
| I04 | `pbs.pgammedia.com` | DNS resolve | Resolves | **DNS fail** | ❌ | P1 | FND-011 |
| I05 | `/api/prebid-release-status` | GET admin | latest prebid version info | `state:"shipped", latest_tag:"11.8.0"` | ✅ | P3 | — |
| I06 | `/api/sellers-migration-status` | GET admin | TB migration status | `tb_reachable:true` | ✅ | P2 | — |

---

## 5. Bugs / gaps / improvements — consolidated register

Priority: **P0** = blocks launch, **P1** = ship-critical (fix before partner onboards), **P2** = important, **P3** = polish.

| ID | Title | Priority | Area | Repro | Recommended fix |
|---|---|---|---|---|---|
| FND-001 | Cross-tenant publisher data leak via `/api/publishers[/{id}]` | **P0** | Web/API | `POST /api/auth/dev-login` as `{"role":"publisher","partner_id":"qa-partner-1"}`, then `GET /api/publishers/2` → returns full QA Publisher incl `auth_secret_ref`. Likewise `role:"dsp"` can GET the list. | Add scoping middleware: `publisher` → filter WHERE `publishers.id = session.partner_id` (and 403 on cross-ID `/publishers/{other}`); `dsp` → 403 on all `/api/publishers*`; `am`/`finance` OK tenant-wide but never across tenants. Assert in tests. |
| FND-002 | DSP create succeeds but is invisible afterwards (read/write schema split) | **P0** | Backend | `POST /api/dsps` 201 with `dspId:1`; `GET /api/dsps` empty, `/api/dsps/health` shows 16 unrelated DSPs. | `POST /api/dsps/route.ts` writes `accounts.dsps`; `/api/dsps/health` and `/api/admin/dsps` query `pgam_direct.dsp_configs`. Pick one canonical table, migrate the other, add integration test that list-after-create always sees the new row. |
| FND-003 | Publisher detail read-back drops fields + placement_ids | **P0** | Backend | `POST /api/publishers` returns `placement_ids:[41,42]` and full basics. `GET /api/publishers/2` returns `placement_ids:[]` and only name/timezone/currency. | `listPublishersAsync` / `getPublisher` likely select minimum columns. Rehydrate the full payload column, join to `pgam_direct.placements` for the ID array. Snapshot test on create→get round-trip. |
| FND-004 | Page-level RBAC missing on most `/admin/*` + `/publishers/new`, `/rtb-tester` | **P0** | Web | `GET /admin/users` with publisher or dsp cookie → 200 HTML. | Add a shared `requireRole(["internal_admin"])` guard in the `(dashboard)/admin/*` layout (or a middleware.ts matcher) that redirects any non-admin session to `/`. Currently only `/admin/deals` and `/admin/agentic` are protected; do all of `/admin/*`, `/publishers/new`, `/dsps/new`, `/rtb-tester`. |
| FND-005 | AM allowed-metrics whitelist is wrong (includes margin_pct) | **P0** | Web | `GET /api/rbac/allowed-metrics` as `am` → 97 metrics incl. `margin_pct`, `take_rate`, `adaptive_margin_delta_pct`. | Update `lib/role-metrics.ts`'s AM list to mirror the ClickHouse column projection (all financials *except* `margin_pct` per v2 §8.4). Add a test comparing allowed set by role to a fixture. |
| FND-006 | `/api/reporting/partner` + `/reporting/matrix` empty while `/api/lld` has rows | **P1** | Backend | Same 30-day window: `/api/lld` returns 6.4 KB CSV; `/api/reporting/partner` returns `row_count:0`. | The reporting endpoints probably still read an un-refreshed ClickHouse MV or the wrong Postgres rollup. Point them at the same underlying `financial_events` / `bid_outcomes` source the LLD uses, or run `refresh materialized view` on deploy. |
| FND-007 | RTB endpoint appears to authenticate by body shape, not credentials | **P1** | Backend | Empty body → 401; valid OpenRTB body with no auth headers → 204 no-bid (accepted). | Confirm whether `CompositeVerifier` actually enforces HMAC/bearer on `/rtb/v1/auction`. If anonymous requests are acceptable because auth = publisher_id lookup, document it explicitly. Otherwise add a required `X-PGAM-Signature` or bearer per the contract. |
| FND-008 | Prebid adapter identity mismatch across backend ↔ Prebid.js ↔ docs | **P1** | Prebid | Backend svc map says `pgamdirect` → `rtb.pgammedia.com/rtb/v1/auction`. Prebid.js fork ships `pgamssp` → `us-east.pgammedia.com/pbjs`. Docs fork has `pgam` (removed 8.13), `pgammedia` (aniview alias), `pgamssp`. | Pick ONE bidder code, ship one adapter that matches the backend endpoint, delete stale doc pages, re-submit to prebid/Prebid.js upstream. Until fixed, publishers integrating via Prebid.js will hit a different endpoint than the one the backend expects. |
| FND-009 | Sellers.json has bootstrap test entry + two independent copies | **P1** | Compliance | `app.pgammedia.com/sellers.json` → `pgam-test-1` bootstrap entry. `pgammedia.com/sellers.json` → larger 25 KB file, 8 s timeout. | Authoritative should be the Go `sellers-json-publisher` service; remove test row from `compliance.sellers_json_entries`, collapse the two URLs to one canonical (`sellers.pgammedia.com`?), redirect the other. |
| FND-010 | `pgammedia.com/ads.txt` returns HTML "Redirecting…" | **P1** | Compliance | `curl https://pgammedia.com/ads.txt` → 200 text/html "Redirecting…" 2 KB. | Serve a real ads.txt as text/plain at the apex. Include the SSP's own line so publishers know what to put in theirs. |
| FND-011 | `pbs.pgammedia.com` DNS does not resolve | **P1** | Ops | Backend docs reference it; DNS lookup fails. | Either provision the host + deploy the `pbs` service, or remove the promise from the partner matrix / onboarding wizard's `prebid_s2s` flow. Publishers picking s2s will ask "where do I point my stored request?" and we don't have an answer. |
| FND-012 | `us-east.pgammedia.com/pbjs` returns 204 for everything | **P1** | Prebid/Ops | Any POST (valid teqblaze body) or GET → 204 no content. | Confirm the upstream is actually routed to the bidder. If it is, a 204 on GET is fine (teqblaze convention) but we need a real seatbid on a valid POST for any DSP wired to the placement. Tie into the DSP-visibility fix (FND-002). |
| FND-013 | `/api/config/*` all 403 for internal_admin | **P2** | Web | `GET /api/config/dsps`, `/api/config/publishers`, `/api/config/deals`, `/api/config/rules`, `/api/config/bid-outcomes` → 403 even with admin session. | Either these require a separate API-key auth (not session) by design — document it — or the role check is wrong. Confirm and fix consistently. |
| FND-014 | Dev login active on prod | **P0** | Web | `/api/auth/dev-login` works at app.pgammedia.com without env flag check at runtime. | Gate `/api/auth/dev-login` behind `process.env.NEXT_PUBLIC_ALLOW_DEV_LOGIN === "1"` with a hard 404 otherwise; confirm prod Vercel env has this variable **unset**. Add a CI smoke: `curl POST prod/api/auth/dev-login` must 404. |
| FND-015 | 401 returned for malformed body on RTB (wrong error code) | **P2** | Backend | `POST /rtb/v1/auction {}` → 401. | Run JSON schema validation first; return 400 for malformed OpenRTB, 401 only when the shape is valid but auth fails. |
| FND-016 | CSV export not implemented anywhere in UI | **P2** | Web | No download buttons on `/reporting`, `/reporting/matrix`, `/reporting/lld`, `/live`. | Partners will ask. Minimum: wire `/api/lld` GET's CSV output to a download link; add JSON→CSV client-side serializer for the other endpoints. |
| FND-017 | ads.txt monitoring "No ads.txt checks yet" (empty state) | **P2** | Compliance | `/compliance/ads-txt` page renders empty. | Wire the Postgres `compliance.ads_txt_cache` read; add a scheduled fetcher. Without this, publishers' sellers.json gates are unmonitored. |
| FND-018 | `/api/admin/alerts-ping` and `/api/discrepancy/our-counts` 405 on GET | **P3** | Backend | Both return `405` GET. | Either intentional (POST-only) — if so, document it in the page that calls them. |
| FND-019 | Only 3 auctions in 24 h across 5 machines, 4 regions | **P2** | Ops | `/api/admin/infra` → `auctions_24h:3`. | Either zero traffic is expected and we should suppress the "degraded" traffic check during this phase, or there is a publisher-→-bidder routing problem. Flag so it doesn't mask a real regression. |
| FND-020 | Multiple sellers-json listed bootstrap seller without `is_confidential`/`domain` redaction rules exercised | **P3** | Compliance | sellers.json shows `PGAM Media LLC` as INTERMEDIARY with domain, plus test publisher. | Walk the sellers-json publisher through the "confidential seller" test case before first publisher onboards. |
| FND-021 | Publisher detail `payload.integration` stripping `enable_hmac_signing` on read-back | **P3** | Backend | Payload echo shows `enable_hmac_signing` intact on create, missing on re-fetch. | Part of FND-003. |
| FND-022 | `/spo` redirects (307) even for internal_admin | **P3** | Web | `GET /spo` as admin → 307. | Probably a hardcoded DSP-role redirect on the page; admin needs a separate route or a role-based branch. |

---

## 6. Findings in depth (narrative)

### FND-001 — Cross-tenant data leak

The most severe finding. In 2 lines of code in [publishers/route.ts:23-34](https://github.com/mastap150/pgam-direct/blob/main/web/src/app/api/publishers/route.ts#L23-L34):

```ts
export async function GET() {
  const session = await getSession();
  if (!session) return NextResponse.json({ error: "UNAUTHENTICATED" }, { status: 401 });
  const publishers = await listPublishersAsync();
  return NextResponse.json({ publishers });
}
```

No role check. No tenant-scoping. No partner-scoping for publisher/dsp sessions. Any authenticated session, regardless of role, receives every publisher in the DB. The `/api/publishers/{id}` handler has the same problem.

Real impact: a publisher who legitimately owns partner_id=`acme` logs in and can read `costco`'s `pbs_stored_req_id`, financial terms, HMAC presence, and integration strategy. This is a cross-publisher intelligence leak and, if `auth_ip_allowlist` ever contained addresses, an attack surface.

**Fix direction** (consistent with the 5-role matrix in the web explore doc):
- `internal_admin`, `finance`, `am` → tenant-scoped list
- `publisher` → list of one (their own partner_id) OR 403
- `dsp` → 403

### FND-002 — DSP create/read schema split

The `POST /api/dsps` handler inserts into `accounts.dsps` and `accounts.dsp_endpoints` (new schema, per `services/dsps.ts`). `GET /api/dsps` queries the same table — and returns empty. Meanwhile `/api/dsps/health` and `/api/admin/dsps` read `pgam_direct.dsp_configs` (legacy schema) and return 16 seed rows.

The create + list query against the same table ought to be symmetric. Most likely candidates:
- `session.tenantId` is the string `"1"`; the `WHERE tenant_id = $1` clause probably expects a bigint — implicit cast could zero out.
- `deleted_at` column may have a trigger defaulting it to NOW() on insert.
- A different `tenant_id` is seeded at create time (see `createDspWithEndpoints` — it does pass `actor.tenantId` as a string).

Because a second DSP create returned the same `dspId:1` (not 2), it's also possible the sequence itself is not advancing — pointing to a transaction rollback that doesn't surface as an error.

**Fix direction:** reproduce locally, verify the row actually commits, fix the query type mismatch, add a create→list snapshot test.

### FND-004 — UI route RBAC

Per `/api/*` RBAC is mostly correct (§4.10). But the **pages** themselves — Next.js App Router under `web/src/app/(dashboard)/*` — do not all enforce the role check at render time. Pages for publisher-role users successfully SSR admin shells for `/admin/users`, `/admin/floors`, `/admin/blocklist`, `/admin/traffic`, `/admin/health`, `/admin/infra`, `/admin/publishers`, `/admin/dsps`, `/publishers/new`, `/rtb-tester`, `/dsps/new` (DSP can reach).

The most surgical fix is a single `middleware.ts` at the web root that:
1. Reads the `pgam_session` cookie
2. For any path matching `/admin/:path*`, `/publishers/new`, `/dsps/new`, `/rtb-tester`: require `role === "internal_admin"`
3. Redirect to `/` (or `/login`) on mismatch

Alternative: a shared `app/(dashboard)/(admin)/layout.tsx` that calls `getSession()` + `redirect('/')` on non-admin, and move all admin pages under that group.

### FND-007 + FND-008 — Bid flow integrity

The most important point for partner launch: **we currently can't demonstrate an end-to-end successful auction**. The RTB endpoint accepts a well-formed request and returns 204 no-bid because (a) the QA publisher I created isn't in whatever table the bidder actually polls, or (b) no DSPs are wired to bid on it. The pbjs endpoint returns 204 for everything, valid or not.

Before launch we need:
1. One fully-wired QA publisher + QA DSP pair that produces a **real** seatbid in a bid response.
2. End-to-end trace through pixel + click + VAST event pipelines landing in ClickHouse.
3. Partner reporting numbers visible via `/api/reporting/partner` that match `/api/lld`.
4. Resolve the `pgamdirect` vs `pgamssp` adapter naming + point at the correct host.

---

## 7. Recommendations — fix-first order

1. **Security + leakage** (today): FND-001, FND-004, FND-005, FND-014.
2. **Data correctness** (next 48h): FND-002, FND-003, FND-006.
3. **Partner-facing integration** (before first partner): FND-007, FND-008, FND-011, FND-012.
4. **Compliance surface** (before first DSP crawls): FND-009, FND-010, FND-020.
5. **Operator UX** (in parallel): FND-016, FND-017.
6. **Logging/observability cleanup**: FND-013, FND-015, FND-018, FND-019, FND-022.

Re-run this QA plan against staging after each fix batch. Add the APIs exercised here as CI smoke tests (`scripts/qa-smoke.sh` that runs §4.10 + §4.2 + §4.3 against `staging.pgammedia.com`).

---

## 8. What was NOT covered (honest gaps in this cycle)

- **Pixel / click / VAST event hot path** (B09-B11) — requires a successful auction to generate a valid HMAC token; couldn't be triggered.
- **Webhooks end-to-end** (code review only) — no partner webhook URL to target.
- **Rule-evaluator engine** — `/rules` empty; engine untested. Spawn a rule via seed then verify it lands in Redis.
- **Adaptive margin / take-rate application** — no live auctions to observe.
- **Multi-region DSP endpoint QPS split** (D09) — not tested live.
- **Discrepancy import + compare** (DC03/DC04) — would need real partner-report CSV.
- **MFA, SSO, TOTP** — not live yet (phase 2).
- **Load/perf** — deliberately avoided per "don't make SSPs flag us" constraint. Run a proper perf cycle against staging with synthetic publishers before first external traffic.
- **UI click-path verification** — preview tools require a local dev server; remote SaaS was probed at HTTP level only. A manual QA walk-through of the publisher + DSP wizards (every step, every field, every validation) is still owed.
- **Invoices + payment_runs + contracts UI** — code has tables and schemas (`financial.invoices`, `payment_runs`), but no routes were surfaced in the web app. Flag for clarification.

---

## Appendix A — architecture map (backend)

See `qa/notes/backend-services.md` for the full subagent-produced map. Key points:

- 11 Go services: `bidder-edge` (auction), `pbs` (prebid-server wrapper, **not reachable today**), `pixel` (tracking), `api` (REST reporting + GraphQL stub), `config-pusher` (gRPC snapshots), `event-gateway` (Kafka→ClickHouse, two pipelines for financial isolation), `rule-evaluator` (cron rule engine), `revert-worker` (expired action cleanup), `ml-serving` (LightGBM shadow), `sellers-json-publisher`, `webhook-worker`.
- Bid flow: Prebid/PBS → `bidder-edge:8080/rtb/v1/auction` → DSP fan-out → OpenRTB response with redaction → Kafka → `event-gateway` → ClickHouse.
- Financial isolation: Kafka `financial_events` topic has its own ACL; ClickHouse `auction_financials` REVOKEd from publisher+dsp roles; a response serializer strips fields as Layer 3.
- Margin hard floor: 5% compile-time constant in `bidder-edge/internal/margin/floor.go:27`.
- Schain complete=1 enforced in `BuildSchain`.

## Appendix B — architecture map (web frontend)

See `qa/notes/frontend-map.md`. Key points:

- Next.js 14 App Router, TS 5.6, Tailwind + shadcn/ui, TanStack Query.
- Session = signed `pgam_session` cookie (base64 JSON + HMAC-SHA256), 12h TTL, HttpOnly + Secure + SameSite=lax.
- Roles: `internal_admin`, `finance`, `am`, `publisher`, `dsp`.
- Routes grouped by role in `role-aware-sidebar.tsx`.
- RBAC currently enforced in API routes individually (inconsistent) and partially at page level (see FND-004).
- `listPublishers` v sync/async bug (fixed 2026-04-22 per code comment) — current impl reads Neon via `listPublishersAsync`.
- HMAC display-once buffer is process-local by design.

## Appendix C — Prebid repos

See `qa/notes/prebid-audit.md`. Key points:

- **Prebid.js fork (mastap150/Prebid.js)**: ships `modules/pgamsspBidAdapter.js` + `libraries/teqblazeUtils/bidderUtils.ts` (20 LOC adapter, 373 LOC shared util). Bidder code `pgamssp`. Endpoint `https://us-east.pgammedia.com/pbjs`. Sync `https://cs.pgammedia.com`. No analytics adapter. No custom ID module.
- **Prebid docs fork**: `dev-docs/bidders/pgamssp.md` (GVL 1353, maintainer info@pgammedia.com, supports banner/video/native), `pgam.md` (legacy, removed 8.13), `pgammedia.md` (aniview alias). None of these match the backend-referenced `pgamdirect`.
- Git log shows no recent PGAM-specific commits on HEAD — fork tracks upstream.

## Appendix D — commands to re-run / reproduce

All evidence + commands land in `qa/evidence/`. Cookie capture, create calls, RBAC probe, and bid tests are scripted — see the transcripts embedded in each §4 section.
