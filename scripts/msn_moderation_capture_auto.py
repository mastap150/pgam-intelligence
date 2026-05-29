#!/usr/bin/env python3
"""
scripts/msn_moderation_capture_auto.py

Autonomous variant of msn_moderation_capture.py. Drives Partner Hub
itself rather than waiting for the user to click.

v2 (2026-05-29):
  First v1 run confirmed aggregate-only endpoints
  (partnerdocstats: contentSubmitted=714 / contentPublished=696 /
  contentRejected=17) but did NOT trigger the per-doc rejected list.
  The SPA ignored URL filter params. v2 also tries clicking text-based
  filter controls ("Rejected", "Under review", "Published", "All") to
  force the SPA to fire the per-doc XHR we actually need.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Response, TimeoutError as PWTimeout

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
SESSION_DIR.mkdir(parents=True, exist_ok=True)

START_URLS = [
    'https://www.msn.com/en-us/partnerhub/content',
]

# Texts we'll try to click in the SPA, in order.
FILTER_TEXTS = ['All', 'Published', 'Under review', 'Rejected']

CAPTURE_HOST_HINTS = (
    'api.msn.com',
    'partner.api.msn.com',
    'msn.com/msn/v0',
    'msn.com/msn/v1',
    'partnerhub',
)
DWELL_AFTER_CLICK_SEC = 6
TOTAL_BUDGET_SEC = 180


def _interesting(url: str) -> bool:
    lo = url.lower()
    if not any(h in lo for h in CAPTURE_HOST_HINTS):
        return False
    if any(skip in lo for skip in ('/log/', '/telemetry', '/clientlog', '/oneds',
                                    '.png', '.jpg', '.svg', '.css', '.js?')):
        return False
    return True


def main() -> int:
    ts = datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out_path = SESSION_DIR / f'moderation-capture-{ts}.jsonl'
    print(f'Output JSONL: {out_path}', file=sys.stderr)

    out_fh = out_path.open('a', encoding='utf-8')
    counters = {'captured': 0}

    def on_response(resp: Response) -> None:
        try:
            url = resp.url
            if not _interesting(url):
                return
            ct = (resp.headers.get('content-type') or '').lower()
            if 'json' not in ct and not url.endswith('.json'):
                return
            try:
                body = resp.json()
            except Exception:
                try:
                    body = json.loads(resp.text())
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
            counters['captured'] += 1
            short = url.split('?', 1)[0]
            print(f'  + [{counters["captured"]:04d}] {resp.status} {short[-90:]}', file=sys.stderr)
        except Exception as exc:
            print(f'  ! response handler error: {exc}', file=sys.stderr)

    deadline = time.time() + TOTAL_BUDGET_SEC

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

        for url in START_URLS:
            print(f'\n--- goto {url}', file=sys.stderr)
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=25_000)
            except Exception as exc:
                print(f'  goto exception: {exc}', file=sys.stderr)
                continue

            # Wait for SPA to settle so its initial XHRs fire.
            try:
                page.wait_for_load_state('networkidle', timeout=20_000)
            except PWTimeout:
                pass

            time.sleep(3)

            # Try each filter by visible text. Many tab UIs respond to a
            # plain text-based click; if multiple matches, take the first.
            for label in FILTER_TEXTS:
                if time.time() > deadline:
                    break
                print(f'  ↳ try click "{label}"', file=sys.stderr)
                clicked = False
                # Try a few common selector idioms in order.
                for selector_fn in (
                    lambda: page.get_by_role('tab', name=label, exact=False),
                    lambda: page.get_by_role('button', name=label, exact=False),
                    lambda: page.get_by_text(label, exact=True).first,
                    lambda: page.locator(f'text="{label}"').first,
                ):
                    try:
                        loc = selector_fn()
                        loc.click(timeout=4_000)
                        clicked = True
                        break
                    except Exception:
                        continue
                if not clicked:
                    print(f'    (no element matched "{label}")', file=sys.stderr)
                    continue
                # Wait for the filter XHR to fire.
                time.sleep(DWELL_AFTER_CLICK_SEC)
                # Scroll to force pagination of the filtered list.
                try:
                    page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                except Exception:
                    pass
                time.sleep(2)

        out_fh.close()
        ctx.close()

    print(f'\nDone. Captured {counters["captured"]} responses → {out_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
