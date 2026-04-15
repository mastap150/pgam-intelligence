#!/bin/bash
# Run this once to print the values you need to add as GitHub Secrets.
# Usage:  bash generate_secrets.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ""
echo "══════════════════════════════════════════════════════"
echo "  GitHub Secrets for daily_report workflow"
echo "══════════════════════════════════════════════════════"
echo ""

echo "── Secret 1: GOOGLE_CREDENTIALS_JSON ─────────────────"
cat "$SCRIPT_DIR/credentials.json"
echo ""
echo ""

echo "── Secret 2: GOOGLE_TOKEN_PICKLE_B64 ─────────────────"
base64 -i "$SCRIPT_DIR/token.pickle"
echo ""
echo ""

echo "── Secret 3: GH_PAT ───────────────────────────────────"
echo "Create a Personal Access Token at:"
echo "  https://github.com/settings/tokens/new"
echo "  → Expiration: No expiration"
echo "  → Scopes:     repo (tick the top-level 'repo' checkbox)"
echo "Then paste that token as the value for GH_PAT."
echo ""
echo "══════════════════════════════════════════════════════"
echo "Add each secret at:"
echo "  https://github.com/YOUR_ORG/YOUR_REPO/settings/secrets/actions"
echo "══════════════════════════════════════════════════════"
