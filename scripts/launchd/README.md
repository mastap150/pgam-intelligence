# Local macOS LaunchAgents for the MSN Insights stack

These three LaunchAgents run the MSN-specific agents on the user's local
Mac at fixed intervals. They exist because:

- **Render can't host Chromium** — the Playwright dep is too heavy for
  the free Python tier
- **Vercel cron can't host Chromium** either — same reason
- The full `scheduler.py` already runs on Render (no Chromium agents)
- So the MSN puller, docID resolver, and health alert run **here**
  via launchd, sharing the authenticated Playwright session at
  `~/.pgam/msn-session/`

| Agent | Cadence | What it does |
|---|---|---|
| `com.pgam.msn-puller` | every 15 min | Pulls per-article snapshots + 15-min traffic buckets from MSN Partner Hub → shared Neon |
| `com.pgam.msn-resolver` | every 30 min | Fetches msn.com public URLs for new docIDs, parses canonical boxingnews.com URLs into `msn_article_meta` |
| `com.pgam.msn-health` | every 60 min | Slack-alerts if no successful pull in 45 min or 3+ consecutive failures |

## Install (one-time, on the Mac that runs the puller)

```bash
# 1. Copy the wrapper scripts to ~/.pgam/ (where they're invoked from)
mkdir -p ~/.pgam
cp scripts/launchd/run-msn-*.sh ~/.pgam/
chmod +x ~/.pgam/run-msn-*.sh

# 2. Copy the plists to ~/Library/LaunchAgents/
cp scripts/launchd/com.pgam.msn-*.plist ~/Library/LaunchAgents/

# 3. Load each agent (the -w flag persists the registration across reboots)
launchctl load -w ~/Library/LaunchAgents/com.pgam.msn-puller.plist
launchctl load -w ~/Library/LaunchAgents/com.pgam.msn-resolver.plist
launchctl load -w ~/Library/LaunchAgents/com.pgam.msn-health.plist
```

## Verify

```bash
# All three should show up. Status code 0 = last run succeeded.
launchctl list | grep com.pgam.msn

# Tail the puller log (or msn-resolver.log / msn-health.log):
tail -f ~/.pgam/msn-puller.log

# Check what landed in Neon:
psql "$(grep '^PGAM_DIRECT_DATABASE_URL=' ~/Desktop/pgam-intelligence/.env | cut -d= -f2- | tr -d '"')" \
  -c "SELECT started_at, ok, realtime_rows_seen FROM pgam_direct.msn_pull_runs ORDER BY started_at DESC LIMIT 5"
```

## Stop / disable

```bash
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-puller.plist
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-resolver.plist
launchctl unload -w ~/Library/LaunchAgents/com.pgam.msn-health.plist
```

## Prerequisites

- `~/.pgam/msn-session/` must contain an authenticated MSN Partner Hub
  session (one-time interactive login — see
  `docs/msn_insights.md` "First-run bootstrap")
- Python 3.x with playwright + psycopg installed (already done if
  `pip install -r requirements.txt` has run)
- `PGAM_DIRECT_DATABASE_URL` in `pgam-intelligence/.env`
- `SLACK_WEBHOOK` in `.env` (optional — only used by the health alert)

## Mac sleep behavior

LaunchAgents pause when the Mac sleeps. When it wakes, missed
intervals fire as soon as possible. For an overnight gap (Mac
sleeping 8h), one or two missed pulls is fine — MSN's own rolling
24h surface absorbs the gap, and the next puller run picks up where
we left off. No special configuration needed.
