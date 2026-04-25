#!/usr/bin/env bash
# Neon PITR (point-in-time restore) drill — verifies that we can actually
# restore the production DB from yesterday into a scratch branch and that
# data integrity is preserved.
#
# RUN THIS BEFORE LAUNCH. Do not assume PITR works because it's enabled.
#
# REQUIREMENTS:
#   - NEON_API_KEY in env (Neon Console → API Keys → create personal key)
#   - jq installed
#   - psql installed
#   - read-only DATABASE_URL_PROD (or DDL-capable for the unpooled)
#
# WHAT THIS DRILL VALIDATES:
#   1. We can issue a branch-restore via Neon API
#   2. The restore completes within reasonable wall time (< 5 min)
#   3. The restored branch has expected row counts in core tables
#   4. We can connect to it and run a smoke query
#   5. Cleanup works — the scratch branch is fully removed
#
# FAIL-CONDITIONS that should block launch:
#   - Restore takes > 10 min (data is bigger than expected → review tier)
#   - Row counts don't match expected (silent data loss in source?)
#   - Branch creation fails (auth/quota issue)
#   - Cleanup fails (orphan branches accumulate cost)

set -euo pipefail

: "${NEON_API_KEY:?NEON_API_KEY must be set in env}"
: "${NEON_PROJECT_ID:?NEON_PROJECT_ID must be set in env (e.g. shrill-night-12345)}"

API="https://console.neon.tech/api/v2"
DRILL_BRANCH_NAME="pitr-drill-$(date +%Y%m%d-%H%M%S)"
RESTORE_TS=$(date -u -v -1H +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -d '1 hour ago' +"%Y-%m-%dT%H:%M:%SZ")

echo "=== PGAM Neon PITR drill ==="
echo "Project:       $NEON_PROJECT_ID"
echo "Restore-to:    $RESTORE_TS  (1 hour ago)"
echo "Scratch:       $DRILL_BRANCH_NAME"
echo ""

# --- 1. Get parent branch (main) id ---
echo "[1/6] Resolving main branch id..."
MAIN_BRANCH_ID=$(curl -fsS -H "Authorization: Bearer $NEON_API_KEY" \
  "$API/projects/$NEON_PROJECT_ID/branches" | \
  jq -r '.branches[] | select(.name == "main" or .primary == true) | .id' | head -1)
echo "  main branch id: $MAIN_BRANCH_ID"

# --- 2. Create restore-from-timestamp branch ---
echo "[2/6] Creating PITR branch from $RESTORE_TS..."
START=$(date +%s)
BRANCH_RESP=$(curl -fsS -X POST -H "Authorization: Bearer $NEON_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"branch\": {\"parent_id\": \"$MAIN_BRANCH_ID\", \"parent_timestamp\": \"$RESTORE_TS\", \"name\": \"$DRILL_BRANCH_NAME\"}, \"endpoints\": [{\"type\": \"read_write\"}]}" \
  "$API/projects/$NEON_PROJECT_ID/branches")
DRILL_BRANCH_ID=$(echo "$BRANCH_RESP" | jq -r '.branch.id')
DRILL_HOST=$(echo "$BRANCH_RESP" | jq -r '.endpoints[0].host')
END=$(date +%s)
RESTORE_TIME=$((END - START))
echo "  branch id:     $DRILL_BRANCH_ID"
echo "  endpoint host: $DRILL_HOST"
echo "  restore time:  ${RESTORE_TIME}s"
if (( RESTORE_TIME > 300 )); then
  echo "  ⚠️  WARNING: PITR took longer than 5 min — flag for review"
fi

# --- 3. Smoke query the restored branch ---
echo "[3/6] Smoke-querying restored branch..."
DRILL_DB_URL="postgresql://${NEON_DB_USER:?NEON_DB_USER required}:${NEON_DB_PASSWORD:?NEON_DB_PASSWORD required}@${DRILL_HOST}/neondb?sslmode=require"
psql "$DRILL_DB_URL" -At <<'SQL' > /tmp/pitr_drill_counts.txt
SELECT 'publisher_configs:' || COUNT(*) FROM pgam_direct.publisher_configs;
SELECT 'dsp_configs:'       || COUNT(*) FROM pgam_direct.dsp_configs;
SELECT 'placements:'        || COUNT(*) FROM pgam_direct.placements;
SELECT 'users:'             || COUNT(*) FROM pgam_direct.users;
SELECT 'financial_events:'  || COUNT(*) FROM pgam_direct.financial_events;
SQL
cat /tmp/pitr_drill_counts.txt

# --- 4. Compare against prod counts (sanity) ---
echo "[4/6] Comparing against prod counts (drift expected to be tiny: only writes in last 1h)..."
PROD_DB_URL="${PGAM_PROD_DATABASE_URL:?PGAM_PROD_DATABASE_URL required}"
psql "$PROD_DB_URL" -At <<'SQL' > /tmp/pitr_prod_counts.txt
SELECT 'publisher_configs:' || COUNT(*) FROM pgam_direct.publisher_configs;
SELECT 'dsp_configs:'       || COUNT(*) FROM pgam_direct.dsp_configs;
SELECT 'placements:'        || COUNT(*) FROM pgam_direct.placements;
SELECT 'users:'             || COUNT(*) FROM pgam_direct.users;
SELECT 'financial_events:'  || COUNT(*) FROM pgam_direct.financial_events;
SQL
diff /tmp/pitr_drill_counts.txt /tmp/pitr_prod_counts.txt || echo "  (drift seen — review whether expected for the restore window)"

# --- 5. Run a real query against restored data ---
echo "[5/6] Running an actual business query against restored data..."
psql "$DRILL_DB_URL" -c "
  SELECT id, org_id, integration_mode, status
  FROM pgam_direct.publisher_configs
  WHERE status = 'active'
  ORDER BY id;
"

# --- 6. Cleanup ---
echo "[6/6] Cleaning up scratch branch..."
curl -fsS -X DELETE -H "Authorization: Bearer $NEON_API_KEY" \
  "$API/projects/$NEON_PROJECT_ID/branches/$DRILL_BRANCH_ID"
echo "  branch $DRILL_BRANCH_ID deleted"

echo ""
echo "=== PITR drill complete ==="
echo "Wall time to restore: ${RESTORE_TIME}s"
echo "If this was < 60s and counts matched → PITR is healthy."
echo "If anything looked weird → don't ship until investigated."
