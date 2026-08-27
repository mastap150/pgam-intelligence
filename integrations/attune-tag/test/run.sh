#!/usr/bin/env bash
# Attune Tag test suite.
#
# Runs the tag in headless Chromium against a STUBBED vendor script. It never
# touches the real tracker — pointing these at production would fire junk
# events into a live pixel.
set -euo pipefail
cd "$(dirname "$0")"

CHROME="${CHROME:-}"
if [ -z "$CHROME" ]; then
  for c in /opt/pw-browsers/chromium-*/chrome-linux/chrome \
           "$(command -v chromium || true)" \
           "$(command -v google-chrome || true)"; do
    [ -x "$c" ] && CHROME="$c" && break
  done
fi
[ -x "${CHROME:-}" ] || { echo "No Chromium found. Set CHROME=/path/to/chrome"; exit 1; }

# Build a test copy with the vendor host swapped for the local stub.
sed 's|https://tracker.vibe.co/vbpx.js|./vendor-stub.js|' ../attune-tag.js > attune-test.js
if grep -q 'tracker\.vibe\.co' attune-test.js; then
  echo "FATAL: test build still points at the real tracker"; exit 1
fi

pass=0; fail=0
for page in a b c; do
  dom=$(mktemp)
  "$CHROME" --headless=new --disable-gpu --no-sandbox --allow-file-access-from-files \
            --virtual-time-budget=8000 --dump-dom "file://$PWD/$page.html" 2>/dev/null > "$dom"
  out=$(python3 -c "
import re,html,sys
d=open('$dom').read()
m=re.search(r'<pre id=\"out\">(.*?)</pre>', d, re.S)
print(html.unescape(m.group(1)).strip() if m else 'FAIL :: $page produced no output')
")
  echo "$out"
  pass=$((pass + $(grep -c '^PASS' <<<"$out" || true)))
  fail=$((fail + $(grep -c '^FAIL' <<<"$out" || true)))
  rm -f "$dom"
done
rm -f attune-test.js

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
