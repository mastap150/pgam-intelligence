#!/bin/bash
#
# ~/.pgam/run-msn-puller.sh
#
# Wrapper invoked every 15 minutes by the com.pgam.msn-puller LaunchAgent.
# Loads PGAM_DIRECT_DATABASE_URL from pgam-intelligence's .env (no secrets
# baked into the LaunchAgent plist itself), then runs the puller.
#
# Logs to /Users/priyeshpatel/.pgam/msn-puller.log (rotates daily by
# size; we don't enforce a strict log rotation — disk pressure on the
# Mac will never become an issue at this volume).

set -u
set -o pipefail

PGAM_DIR="/Users/priyeshpatel/Desktop/pgam-intelligence"
LOG="/Users/priyeshpatel/.pgam/msn-puller.log"
ENV_FILE="${PGAM_DIR}/.env"

# Source PGAM_DIRECT_DATABASE_URL (handles quoted/unquoted, ignores comments)
if [ -f "${ENV_FILE}" ]; then
  PGAM_DIRECT_DATABASE_URL=$(grep -E "^PGAM_DIRECT_DATABASE_URL=" "${ENV_FILE}" | cut -d= -f2- | tr -d '"' | tr -d "'")
  export PGAM_DIRECT_DATABASE_URL
fi

# Always run headless — the saved session at ~/.pgam/msn-session/ has
# the authenticated cookies from the manual login on 2026-05-18.
export MSN_HEADLESS=1
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "${PGAM_DIR}" || exit 1

{
  echo ""
  echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) — MSN puller run ==="
  /opt/homebrew/bin/python3 -m agents.etl.msn_insights_etl 2>&1
  echo "=== exit code: $? ==="
} >> "${LOG}" 2>&1
