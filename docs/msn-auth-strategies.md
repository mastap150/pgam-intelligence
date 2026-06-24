# MSN Partner Hub authentication strategies

The MSN Partner Hub API at `api.msn.com/msn/v0/pages/ugc/insights/...` authenticates on a **Bearer JWE** that the SPA's MSAL/RPS stack mints during login. Keeping that Bearer valid long-term without a human at the keyboard is non-trivial because MSN enforces:

- Session cookies (`_C_Auth`) with `exp=-1` — explicitly designed not to persist
- 1-hour access-token lifetime
- 24-hour refresh-token lifetime (consumer accounts)
- MFA on initial login
- "Trusted device" cookies that can lapse

This doc enumerates the strategies we evaluated and which one is currently in use.

## Strategies

### Strategy A — Local LaunchAgent on the user's Mac (initial approach, 2026-05-18)

- Three `~/Library/LaunchAgents/com.pgam.msn-*.plist` agents running the puller, resolver, and health-alert every 15/30/60 min
- Used Playwright with a persistent Chromium profile at `~/.pgam/msn-session`
- **Failure mode:** Mac sleep + ~16-24h cookie expiry. We observed silent 401s starting 16h after a working login.
- **Verdict:** ❌ not viable for traveling user

### Strategy B — Playwright on GitHub Actions with session port (2026-05-19, failed)

- `scripts/session_backup_restore.py` chunks the user-data-dir into Neon BYTEA
- GH Actions restores at start, runs puller, backs up at end
- **Failure mode:** Chromium profile dirs don't port cross-OS (cookies are OS-keychain-encrypted). Even storage_state JSON didn't survive the ~16-24h session window. Test runs landed on `/partnerhub/login`.
- **Verdict:** ❌ doesn't solve the fundamental session-expiry issue

### Strategy C — Refresh-token chain (current, recommended)

- `scripts/msn_oauth_capture.py` does a ONE-TIME interactive login on the user's Mac, intercepts the OAuth token exchange, saves `refresh_token` + `client_id` + scope into `pgam_direct.msn_oauth_token`.
- `scripts/msn_refresh_puller.py` runs anywhere (GH Actions, Render, locally) — reads the stored refresh_token, calls Microsoft's OAuth endpoint to mint a new access_token (and a **rotated** refresh_token), uses the access_token directly as Bearer for `api.msn.com/realtime`.
- Each refresh issues a new 24h refresh_token, so the chain is indefinite as long as we refresh within 24h.
- **No browser, no Chromium, no Playwright** at runtime. Just `requests` + `psycopg`.
- `.github/workflows/msn-insights.yml` is wired to run this every 15 min.
- **Failure mode:** If the chain breaks (no successful refresh in >24h, or user re-auths elsewhere invalidating our chain), we need a fresh bootstrap.
- **Verdict:** ✅ best autonomous option. Re-bootstrap ~weekly to be safe (chain might survive longer; needs observation).

### Strategy D — Service account (fallback / belt-and-suspenders)

If the refresh-token chain proves fragile (e.g., chain breaks more than once a week), set up a dedicated Microsoft 365 service account:

1. **Microsoft 365 Admin Center** → Users → Add a user → e.g. `partnerhub-bot@pgammedia.com`
2. License: any cheap one (Business Basic works; ~$6/month)
3. **Security defaults** → exclude this user from MFA, OR create a Conditional Access policy that excludes this user from MFA when authenticating from a trusted IP range
4. **Microsoft Partner Hub** → Add team member → invite the bot user with editor access to the BoxingNews partner (AA1lKiff)
5. Use the bot creds with `scripts/msn_oauth_capture.py` to bootstrap a refresh_token

The service account never logs into anything interactively, so its refresh chain stays alive forever (modulo Microsoft's normal 90-day refresh-token caps for managed accounts).

This is the standard pattern for service automation but requires the user's 30-min setup in MS Admin Center.

## Current operational status (2026-05-19)

- Strategy C is fully implemented but **not yet bootstrapped** — needs one interactive run of `scripts/msn_oauth_capture.py` on Priyesh's Mac
- The local LaunchAgents (Strategy A) are still loaded but silently failing since the session expired
- GH Actions workflow `.github/workflows/msn-insights.yml` is on branch `feat/msn-puller-on-gh-actions`, switched to refresh-token mode, not yet merged

## Bootstrap procedure (when ready to go live)

```bash
cd ~/Desktop/pgam-intelligence

# 1. One-time interactive bootstrap. Visible Chromium opens; sign in
#    (auto-fill works if MSN_EMAIL/MSN_PASSWORD set); MFA tap on phone
#    when prompted. The script intercepts the OAuth response silently
#    and writes refresh_token to Neon.
PGAM_DIRECT_DATABASE_URL="$(grep ^PGAM_DIRECT_DATABASE_URL .env | cut -d= -f2-)" \
  python3 scripts/msn_oauth_capture.py

# 2. Smoke test the refresh puller locally
PGAM_DIRECT_DATABASE_URL="$(grep ^PGAM_DIRECT_DATABASE_URL .env | cut -d= -f2-)" \
  python3 scripts/msn_refresh_puller.py

# 3. Merge the PR + verify GH Actions takes over
gh pr merge --squash --delete-branch  # on feat/msn-puller-on-gh-actions

# 4. Unload local LaunchAgents (they're failing anyway)
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-puller.plist
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-resolver.plist
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-health.plist
```

## Recovery procedure (when the chain breaks)

`pgam_direct.msn_pull_runs` will show consecutive failures with error message `OAuth refresh failed: HTTP 400` or `refresh_token expired`. When that happens:

1. Re-run `scripts/msn_oauth_capture.py` — same bootstrap as initial
2. The chain restarts from there

The puller-health alert (`agents.alerts.msn_puller_health`) will fire a Slack message after a few consecutive failures, so we'll notice within an hour.
