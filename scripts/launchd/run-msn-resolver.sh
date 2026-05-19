#!/bin/bash
#
# ~/.pgam/run-msn-resolver.sh
#
# Wrapper for the docID → boxingnews.com URL resolver. Runs every
# 30 min via LaunchAgent. Just a thin shell over
#   python3 -m agents.enrichment.msn_doc_resolver

set -u
set -o pipefail

PGAM_DIR="/Users/priyeshpatel/Desktop/pgam-intelligence"
LOG="/Users/priyeshpatel/.pgam/msn-resolver.log"
ENV_FILE="${PGAM_DIR}/.env"

if [ -f "${ENV_FILE}" ]; then
  PGAM_DIRECT_DATABASE_URL=$(grep -E "^PGAM_DIRECT_DATABASE_URL=" "${ENV_FILE}" | cut -d= -f2- | tr -d '"' | tr -d "'")
  export PGAM_DIRECT_DATABASE_URL
fi
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

cd "${PGAM_DIR}" || exit 1

{
  echo ""
  echo "=== $(date -u +%Y-%m-%dT%H:%M:%SZ) — resolver run ==="
  /opt/homebrew/bin/python3 -m agents.enrichment.msn_doc_resolver 2>&1
  echo "=== exit code: $? ==="
} >> "${LOG}" 2>&1
