#!/usr/bin/env python3
"""
scripts/msn_moderation_capture.py

Network-sniff variant of msn_session_capture.py focused on the
*Content* / *Moderation* views of Partner Hub — where rejected and
under-review articles show up. The `/realtime` endpoint (covered by
the existing puller) only returns articles MSN has already published,
so it can't tell us anything about moderation outcomes.

Goal: decode the int-coded moderation states so we can mirror them
into the boxingnews `articles` table and (a) stop re-feeding rejected
items, (b) surface the 60-day appeal window in the admin dashboard,
(c) skip churn-tuning under-review items for 48h.

How to use:

  1. Run:   python3 scripts/msn_moderation_capture.py
  2. The visible window opens on Partner Hub. Sign in if prompted
     (existing persistent session usually still valid).
  3. Manually navigate through:
        - Content / All
        - Content / Under review        (filter the list)
        - Content / Rejected            (filter the list — pin at least
                                         one known-rejected article)
        - Open one rejected article's detail page
        - Open one published article's detail page (for control)
  4. Hit Ctrl-C when done. Script writes every JSON response to
     `~/.pgam/msn-session/moderation-capture-{ts}.jsonl` for offline
     analysis.

The next session reads that JSONL, decodes the status enum, and adds
the migration + ETL changes that the moderation-response protocol
requires.
"""

from __future__ import annotations

import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Response

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
SESSION_DIR.mkdir(parents=True, exist_ok=True)

START_URL = 'https://www.msn.com/en-us/partnerhub/content'

# Only capture API-y traffic — partnerhub fetches a lot of static junk
# we don't care about (telemetry pixels, CSS, etc).
CAPTURE_HOST_HINTS = (
    'api.msn.com',
    'partner.api.msn.com',
    'msn.com/msn/v0',
    'msn.com/msn/v1',
    'partnerhub',
)


def _interesting(url: str) -> bool:
    lo = url.lower()
    if not any(h in lo for h in CAPTURE_HOST_HINTS):
        return False
    # Skip noisy telemetry/log endpoints.
    if any(skip in lo for skip in ('/log/', '/telemetry', '/clientlog', '/oneds')):
        return False
    return True


def main() -> int:
    ts = datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_path = SESSION_DIR / f'moderation-capture-{ts}.jsonl'
    print(f'Session dir:   {SESSION_DIR}')
    print(f'Output JSONL:  {out_path}')
    print(f'Start URL:     {START_URL}')
    print()
    print('Drive the browser manually:')
    print('  - Go to Content / All → switch filter to Rejected → open one item')
    print('  - Switch filter to Under review → open one item')
    print('  - Open one Published item (control)')
    print('Hit Ctrl-C when done. Every JSON response is appended to the JSONL.')
    print()

    out_fh = out_path.open('a', encoding='utf-8')
    captured = {'n': 0}

    def on_response(resp: Response) -> None:
        try:
            url = resp.url
            if not _interesting(url):
                return
            ct = (resp.headers.get('content-type') or '').lower()
            # We want JSON; many partnerhub endpoints reply application/json.
            if 'json' not in ct and not url.endswith('.json'):
                return
            try:
                body = resp.json()
            except Exception:
                # Some endpoints reply text/plain with JSON-looking content
                txt = resp.text()
                try:
                    body = json.loads(txt)
                except Exception:
                    return
            row = {
                'captured_at': datetime.now(tz=timezone.utc).isoformat(),
                'status':      resp.status,
                'url':         url,
                'method':      resp.request.method,
                'body':        body,
            }
            out_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
            out_fh.flush()
            captured['n'] += 1
            # Compact stderr feedback so the user knows it's working.
            short = url.split('?', 1)[0]
            print(f'  + [{captured["n"]:04d}] {resp.status} {short[-90:]}', file=sys.stderr)
        except Exception as exc:
            print(f'  ! (response handler error: {exc})', file=sys.stderr)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,
            viewport={'width': 1500, 'height': 950},
            user_agent=(
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/148.0.0.0 Safari/537.36'
            ),
        )
        page = ctx.new_page()
        page.on('response', on_response)
        page.goto(START_URL, wait_until='domcontentloaded')

        # Hold the browser open until the user Ctrl-Cs.
        def _bail(*_a):
            print()
            print(f'  Captured {captured["n"]} JSON responses → {out_path}')
            print('  Closing browser...')
            try:
                out_fh.close()
            finally:
                ctx.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, _bail)
        signal.signal(signal.SIGTERM, _bail)

        while True:
            time.sleep(1)


if __name__ == '__main__':
    sys.exit(main())
