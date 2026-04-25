# Failure-mode rehearsal — pre-launch drill

Before going live with paying partners, **rehearse the four most likely
failure modes** and confirm each has a recovery path you can execute in
under 10 min. Pick a low-traffic window (Sat morning typically), have a
second person on a call as observer.

---

## Drill 1 — A DSP starts returning 500s mid-auction

**Setup**: Pick one inactive DSP from the catalog (e.g. id=21, archived QA
DSP). Toggle it `is_active=true` via SQL, with an endpoint URL that we
know returns 500 (`https://httpbin.org/status/500` or similar).

**Run**: Generate ~30 auctions through the bidder-edge.

**Watch for**:
- Does the bidder-edge circuit-breaker trip and stop calling the bad DSP?
- Does the auction still succeed for the publisher (other DSPs win)?
- Does `pgam_circuit_breaker_transitions_total` increment?
- Does the runbook `alert-bidder-latency.md` actually trigger? (or are alerts unwired — see #8)

**Pass condition**: Auction p95 latency rises by < 50ms; auctions still complete; circuit-breaker opens within 10 calls.

**Cleanup**: Set `is_active=false` on the bad DSP.

---

## Drill 2 — Neon connection-pool exhaustion

**Setup**: From a separate machine, hold open 100 idle Neon connections
(simple psql x 100 in a loop with `SELECT pg_sleep(600)`).

**Run**: Try to use the app normally — load /admin/publishers, run reports.

**Watch for**:
- Do API routes return graceful 503s, or do they hang for 60s and timeout?
- Does the `/api/admin/health` endpoint flag the pool as exhausted?
- Does Vercel's request queue back up?

**Pass condition**: API routes return 503 within 5s, not a 60s hang.
Health endpoint shows degraded.

**Cleanup**: Kill the holding connections.

**If this fails**: Add explicit pool wait timeout in `web/src/server/pg.ts`.

---

## Drill 3 — Vercel deployment rollback under live traffic

**Setup**: Deploy a known-broken version (introduce a 500 in `/api/auth/me`).

**Run**: Watch error rate spike, run `vercel rollback` to previous
production deployment.

**Watch for**:
- Time from broken-deploy → traffic-restored.
- Does `vercel rollback` actually revert all routes, or only some?
- Are any partner sessions invalidated by the rollback (cookie secret rotation)?

**Pass condition**: Recovery in < 3 min, no session loss.

**If this fails**: Document rollback runbook with explicit steps; consider
canary deploys.

---

## Drill 4 — Upstash KV rate-limiter outage

**Setup**: Temporarily set `UPSTASH_REDIS_REST_URL` to a non-existent URL
in the Vercel env. Trigger redeploy.

**Run**: Hit `/api/auth/me` 100 times in 30s.

**Watch for**:
- Does the rate-limiter fail closed (deny all) or fail open (allow all)?
  Either is defensible but the choice should be intentional.
- Are errors in handler logs that name "Upstash" or "KV"?
- Does the limiter recovery happen automatically when KV is restored, or
  does Vercel need a redeploy?

**Pass condition**: System remains usable (fail-open is acceptable for a
rate limiter — denying all auth would be worse). Recovery within 1 min of
KV restore.

**Cleanup**: Restore real KV URL, redeploy.

---

## Acceptance criteria for "ready to launch"

You've run all 4 drills and recorded:
- Recovery time for each
- Whether alerts actually fired (currently most won't — see #8 audit)
- Whether on-call documentation worked

Drill blocking issues that came up → file as findings, fix or accept-with-mitigation before launch.
