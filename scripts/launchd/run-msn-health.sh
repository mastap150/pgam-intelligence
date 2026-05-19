#!/bin/bash
#
# ~/.pgam/run-msn-health.sh
#
# Wrapper for the puller-health alert agent. Runs hourly via
# LaunchAgent. Slack-pages once/day if pulls go stale (no successful
# run in 45 min) or hit a failure streak.

set -u
set -o pipefail

PGAM_DIR="/Users/priyeshpatel/Desktop/pgam-intelligence"
LOG="/Users/priyeshpatel/.pgam/msn-health.log"
ENV_FILE="${PGAM_DIR}/.env"

if [ -f "${ENV_FILE}" ]; then
  PGAM_DIRECT_DATABASE_URL=$(grep -E "^PGAM_DIRECT_DATABASE_URL=" "${ENV_FILE}" | cut -d= -f2- | tr -d '"' | tr -d "'")
  SLACK_WEBHOOK=$(grep -E "^SLACK_WEBHOOK=" "${ENV_FILE}" | cut -d= -f2- | tr -d '"' | tr -d "'")
  export PGAM_DIRECT_DATABASE_URL SLACK_WEBHOOK
fi
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "${PGAM_DIR}" || exit 1

{
  echo ""
  echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) — health check ==="
  /opt/homebrew/bin/python3 -m agents.alerts.msn_puller_health 2>&1
  echo "=== exit code: $? ==="
} >> "${LOG}" 2>&1
