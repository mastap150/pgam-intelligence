#!/usr/bin/env python3
"""
scripts/msn_session_capture.py

Passive-observer mode for capturing an authenticated MSN Partner Hub
session into ~/.pgam/msn-session/ so the puller can run headless from
then on.

The visible Chromium window opens on Partner Hub. The script doesn't
try to drive the login form — it just waits, polling the /realtime
API every 5 seconds. Once the API returns 200, we know the user has
authenticated successfully, the session cookies are saved by the
persistent context, and the script exits cleanly.

The user can authenticate however they want:
  - Let the auto-fill submit credentials + tap MFA when prompted
  - Manually log in via the Sign-in flow inside the window
  - Paste auth cookies from another browser via DevTools

Run:
    python3 scripts/msn_session_capture.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

# Best-effort .env loading. We need MSN_EMAIL/MSN_PASSWORD for the
# auto-fill convenience, but the script also works without them
# (user logs in manually).
try:
    from dotenv import load_dotenv
    # Load from the worktree env first (where Priyesh keeps creds), then main.
    here = Path(__file__).resolve().parent.parent
    for candidate in [
        here / '.claude/worktrees/eloquent-chatelet-86f9f6/.env',
        here / '.env',
    ]:
        if candidate.exists():
            load_dotenv(dotenv_path=str(candidate), override=False)
except Exception:
    pass

from playwright.sync_api import sync_playwright

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
SESSION_DIR.mkdir(parents=True, exist_ok=True)

PARTNER_HUB_URL = 'https://www.msn.com/en-us/partnerhub/analytics/realtime/headline'
API_HOST = 'https://api.msn.com'
API_PATH = '/msn/v0/pages/ugc/insights/content/realtime'
APIKEY = 'tfFF5vu2Sk8ndqqn6je2Vo4qOFve5LeicxEpNSnoZK'
PARTNER_ID = 'AA1lKiff'

WAIT_TIMEOUT_SEC = 5 * 60  # 5 min max
POLL_INTERVAL_SEC = 5


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%MZ')


def main() -> int:
    print(f'Session dir: {SESSION_DIR}')
    print(f'Will poll for {WAIT_TIMEOUT_SEC}s. Complete the MSN login in the popup window.')
    print()

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,
            viewport={'width': 1400, 'height': 900},
            user_agent=(
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/148.0.0.0 Safari/537.36'
            ),
        )
        page = ctx.new_page()
        print('  [1/3] Opening Partner Hub...')
        page.goto(PARTNER_HUB_URL, wait_until='domcontentloaded')
        print('  [2/3] Waiting for you to complete login (any method).')
        print('        - Auto-fill might run automatically.')
        print('        - Or click "Sign in", enter credentials, complete MFA.')
        print('        - Or paste auth cookies from another browser via DevTools.')
        print('  [3/3] Polling /realtime every 5s — will exit on first 200.')
        print()

        end_dt = datetime.now(tz=timezone.utc)
        start_dt = end_dt - timedelta(hours=1)  # small window, just need to probe auth
        params = {
            'apikey': APIKEY,
            'brandId': '-2', 'clickSource': '-2', 'contentType': '-2',
            'date': '-2', 'device': '-2',
            'endDate': iso_z(end_dt),
            'fdhead': 'prg-ugc-benchmark', 'lang': '-2', 'mkt': '-2', 'ocid': 'msph',
            'partnerId': PARTNER_ID, 'partnerType': '2',
            'scn': 'MSNRPSAuth', 'skipaadal': 'true',
            'startDate': iso_z(start_dt),
            'timeout': '30000', 'title': '-1',
            'ugc-flights': 'prg-ugc-benchmark', 'vertical': '-2', 'wrapodata': 'false',
            '$orderBy': 'view', '$skip': '0', '$top': '5',
        }
        url = f'{API_HOST}{API_PATH}?{urlencode(params, safe=",-")}'
        js_template = """
        async (url) => {
            const r = await fetch(url, {
                method: 'GET',
                credentials: 'include',
                headers: { 'accept': '*/*', 'content-type': 'application/json' }
            });
            return { status: r.status, ok: r.ok };
        }
        """

        deadline = time.time() + WAIT_TIMEOUT_SEC
        last_status: int | None = None
        while time.time() < deadline:
            try:
                result = page.evaluate(js_template, url)
                status = int(result.get('status') or 0)
                if status == 200:
                    print()
                    print(f'  ✓ /realtime returned 200 — session is authenticated.')
                    print(f'  ✓ Session saved at {SESSION_DIR}')
                    print(f'  ✓ Future puller runs can use MSN_HEADLESS=1.')
                    ctx.close()
                    return 0
                if status != last_status:
                    print(f'  ... /realtime returned {status} (will retry in {POLL_INTERVAL_SEC}s)')
                    last_status = status
            except Exception as exc:
                msg = str(exc)
                # Page might be on login.microsoftonline.com which blocks the
                # cross-origin fetch — that's normal during the login dance.
                if 'navigation' in msg.lower() or 'closed' in msg.lower():
                    print(f'  ... (still navigating: {msg[:80]})')
                else:
                    print(f'  ... (probe exception: {msg[:120]})')
            time.sleep(POLL_INTERVAL_SEC)

        print()
        print(f'  ✗ Timed out after {WAIT_TIMEOUT_SEC}s without seeing a 200.')
        print(f'  ✗ Last /realtime status: {last_status}')
        print(f'  ✗ Session dir state was preserved at {SESSION_DIR}; you can re-run.')
        ctx.close()
        return 1


if __name__ == '__main__':
    sys.exit(main())
