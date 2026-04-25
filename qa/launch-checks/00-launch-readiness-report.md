# PGAM SSP — Launch-readiness report

**Generated**: 2026-04-24
**Reviewer**: Round-5 QA cycle (Claude + Priyesh)
**Target launch**: Week of 2026-04-28 (next week)
**Recommendation**: **CONDITIONAL NO-GO** — three P0 items must clear first.

---

## TL;DR

The platform shell — wizard, admin, RTB edge, sellers.json, Prebid adapter,
attribution stack — is **functional and demoable**. The dual-mode partner
demo executed cleanly end-to-end in production this round.

What's missing is **the actual demand and the actual compliance posture**.
We can serve traffic, but we have no real DSPs to serve it to, and we'd
fail a basic IAB / advertiser-side trust audit on day 1.

The good news: every blocking item has a clear fix and most are <1 day
of work. We are **5–10 working days from a defensible launch**, not weeks.

---

## P0 — Must fix before launch (3 items)

### P0-1 — FND-048: Fraudulent GVL ID 1353 in public-facing material
**Where**: `web/src/app/sellers.json/route.ts:124`, `prebid/pgamdirect.yaml:9`,
multiple runbooks and onboarding docs.
**Issue**: We claim "GVL ID 1353" and "TCF-registered IAB vendor" in
public-facing JSON and adapter metadata. Live IAB Global Vendor List has
**max ID 234**. ID 1353 does not exist. This is straight misrepresentation
of TCF status — any DSP doing GVL validation will reject our requests, and
this is the kind of finding that gets posted to AdTech Twitter.
**Fix**: Either (a) actually register with IAB Europe (~6-8 weeks, $$), or
(b) remove all GVL claims from public artifacts immediately and label as
"GVL registration in progress". Option (b) for launch.
**Blocking**: yes.

### P0-2 — Zero of 16 DSPs are actually transactable
**Issue**: All 16 rows in `dsp_configs` are placeholder/example.com URLs
with bootstrap env-var placeholders. **No real DSP has been integrated
end-to-end with production credentials.** We have a fan-out architecture
with nothing on the other side.
**Fix**: Audit the day-1 demand set. Pick 3-5 real DSPs (Magnite, PubMatic,
Smaato are most plausible from existing wiring), get production credentials,
run discrepancy import dry-run (see check #9), confirm bid responses come
back, mark only those as `is_active=true`. **De-activate the other 11-13.**
**Blocking**: yes — without demand, the SSP serves nothing.

### P0-3 — Compliance docs: 2 of 13 required artifacts present
**Have**: `ads.txt`, `sellers.json` (modulo P0-1).
**Missing**: Privacy Policy, Terms of Service, Data Processing Agreement,
CCPA opt-out endpoint, Cookie Policy, COPPA statement, Acceptable Use Policy,
Publisher MSA template, DSP MSA template, GDPR Article 28 sub-processor list,
breach-notification SOP.
**Fix**: Templates exist (Termly, Iubenda, or a real lawyer ~$2k for the
package). At minimum: Privacy Policy + ToS + DPA + CCPA opt-out before any
EU/CA traffic. Get the rest in the 30-day window post-launch.
**Blocking**: yes for any partner who reads contracts (i.e. all of them).

---

## P1 — Strongly recommended before launch (5 items)

### P1-1 — CORS wide open on bidder-edge
Bidder edge currently sets `Access-Control-Allow-Origin: *`. Should be the
publisher's registered origin set, lookup by `org_id`. Filed in #14 OWASP
audit. Fix is ~1 day.

### P1-2 — Cookie not forced `Secure` in all environments
`pgam_session` is set without `Secure` on at least one code path; `Secure`
only set in production guard. Tighten to always-`Secure` (or fail on http).
~30 min fix.

### P1-3 — Missing tenant check on 3 admin routes
`/api/admin/publishers/[id]`, `/api/admin/dsp-configs/[id]`, and
`/api/admin/financial-events/[id]` check role but not `tenant_id`. With
multi-tenant in flight, this is a horizontal-privilege-escalation hole.
1-day fix; add a middleware helper.

### P1-4 — No login rate limit
Upstash KV-rate-limit is wired for `/api/auth/me` but **not for
`/api/auth/login`**. Credential-stuffing wide open. Add a 5/min/IP cap. ~1h fix.

### P1-5 — Observability gaps (3 of 8 items)
- No Sentry / error tracking SDK initialized in web or rtb apps
- No alert rules in any monitoring system (runbooks cite alerts that don't exist)
- No dashboards-as-code; Grafana dashboards live only in someone's browser
- Add Sentry + 5 alert rules (auction p95 > 500ms, error rate > 1%, KV down,
  Neon connection-pool > 80%, sellers.json 5xx) before launch.

---

## P2 — Should fix in first 30 days (4 items)

- **P2-1**: SSRF-adjacent surface on bidder-edge fan-out (no host allowlist
  on outbound DSP URLs). Mitigation: only configured DSPs get called, but
  defense-in-depth missing.
- **P2-2**: MFA hardcoded `'000000'` bypass in test env — confirm this is
  truly never reachable in prod; add a CI guard.
- **P2-3**: Sellers.json edge-cache propagation lag (~5 min). Document for
  AMs so they don't promise "instant" partner activation.
- **P2-4**: Wizard `PATCH /api/publishers/[id]` silently no-ops on `status`
  field (omitted from `patchPublisherSchema.partial()`). Foot-gun discovered
  this round. Either accept status or return 400 on unknown fields.

---

## Pre-launch operational drills (to execute next week)

These are written and ready to run; results feed into final go/no-go.

| # | Artifact | What it proves | Time |
|---|---|---|---|
| 01 | `01-e2e-traffic-testbed.html` | Real browser → adapter → bidder → DSP fan-out works | 1h |
| 05 | `05-neon-pitr-drill.sh` | DB restore actually works (don't trust "it's enabled") | 30m |
| 06 | `06-load-test.k6.js` | SLOs hold at 250 VUs across 3 hot paths | 1h |
| 07 | `07-failure-mode-drill.md` | 4 failure modes have <10min recovery | 2h, low-traffic window |
| 09 | `09-discrepancy-import-plan.md` | DSP statement formats parse for top-3 demand | 4h |
| 11 | `11-staff-training-audit.md` | Someone other than Priyesh can run an incident | review |

---

## What's already GREEN (don't lose track of these)

- **Adapter performance** — auction p95 well under SLO in synthetic tests
- **Pricing model** — 80/20 rev share is competitive; keep
- **Trademark** — no conflicts found; "PGAM Direct" is clean
- **Attribution stack** — TFN-matched call attribution shipped 2026-04-13, working
- **Attention engine v1** — shipped 2026-04-22, parallel to Athena
- **Repo hygiene** — `.env.vercel` properly gitignored (false alarm cleared)
- **Live partner demo** — dual-mode setup created/archived cleanly via API

---

## Path to GO

**Day 1-2**: P0-1 (strip GVL claims), P1-2/P1-4 (cookie + login rate limit,
both <2h fixes), P1-3 (tenant check helper).

**Day 3-5**: P0-2 (real DSP integration — pick 3, get creds, run check #9
import, flip is_active). P1-1 (CORS allowlist).

**Day 4-6**: P0-3 (compliance docs — Privacy/ToS/DPA/CCPA via Termly is
~1 day inc. lawyer review).

**Day 6-7**: P1-5 (Sentry + 5 alert rules).

**Day 7-8**: Run all 6 drills above. Record results.

**Day 9-10**: Buffer / fix what the drills surfaced.

**Launch**: end of week 2 (~2026-05-08), not next week.

---

## Recommendation

**Do not launch the week of 2026-04-28.** The GVL claim alone is
launch-blocking — first DSP that runs vendor validation finds it, and the
trust hit is hard to recover from.

**Two-week slip to 2026-05-08** clears all P0/P1 with margin. This is the
cheapest version of "be early and right" vs. "be on time and embarrassed".

If the 2026-04-28 date is hard external commitment, **launch in stealth
mode**: 1 publisher, 1 DSP, no marketing, with the fixes for P0-1/P0-3
landed but P0-2 deferred (one DSP is enough to demo "it works"). Then
broaden in week 2.
