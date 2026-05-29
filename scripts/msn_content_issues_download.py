#!/usr/bin/env python3
"""
scripts/msn_content_issues_download.py

The Partner Hub home page shows a "Resolve content issues" card with a
Download button. Clicking it (per UI text) downloads a CSV of articles
that need updates — i.e., the rejected/under-review per-doc list we
need to decode moderation state.

This script clicks Download, captures both the CSV file and the XHR
that fired (which gives us the API endpoint + response shape).
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Response

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
OUT_DIR = SESSION_DIR / 'probe'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> int:
    ts = datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    jsonl_path = OUT_DIR / f'content-issues-xhr-{ts}.jsonl'
    download_dir = OUT_DIR / f'downloads-{ts}'
    download_dir.mkdir(exist_ok=True)
    out_fh = jsonl_path.open('a', encoding='utf-8')

    def on_response(resp: Response) -> None:
        try:
            url = resp.url
            if 'api.msn.com' not in url and 'partner' not in url.lower():
                return
            ct = (resp.headers.get('content-type') or '').lower()
            body_repr = None
            if 'json' in ct:
                try:
                    body_repr = resp.json()
                except Exception:
                    pass
            elif 'csv' in ct or 'text' in ct or 'octet' in ct:
                try:
                    txt = resp.text()
                    body_repr = {'__text_preview__': txt[:2000], '__len__': len(txt)}
                except Exception:
                    pass
            if body_repr is None:
                return
            row = {
                'captured_at': datetime.now(tz=timezone.utc).isoformat(),
                'status':      resp.status,
                'url':         url,
                'method':      resp.request.method,
                'content_type': ct,
                'body':        body_repr,
            }
            out_fh.write(json.dumps(row, ensure_ascii=False) + '\n')
            out_fh.flush()
            short = url.split('?', 1)[0]
            print(f'  + {resp.status} {short[-100:]}', file=sys.stderr)
        except Exception as exc:
            print(f'  ! handler err: {exc}', file=sys.stderr)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,
            viewport={'width': 1500, 'height': 950},
            accept_downloads=True,
            user_agent=(
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/148.0.0.0 Safari/537.36'
            ),
        )
        page = ctx.new_page()
        page.on('response', on_response)
        page.goto('https://www.msn.com/en-us/partnerhub/home',
                  wait_until='domcontentloaded', timeout=30_000)
        try:
            page.wait_for_load_state('networkidle', timeout=20_000)
        except Exception:
            pass
        time.sleep(3)

        # The "Resolve content issues" card has a Download link/button.
        # Try several selector idioms; capture the download.
        clicked = False
        for sel_fn in (
            lambda: page.get_by_role('link', name='Download', exact=False),
            lambda: page.get_by_role('button', name='Download', exact=False),
            lambda: page.get_by_text('Download', exact=True).first,
            lambda: page.locator('text=Download').first,
        ):
            try:
                with page.expect_download(timeout=15_000) as dl_info:
                    sel_fn().click(timeout=4_000)
                dl = dl_info.value
                target = download_dir / (dl.suggested_filename or 'download.bin')
                dl.save_as(str(target))
                print(f'  downloaded → {target}')
                clicked = True
                break
            except Exception as exc:
                print(f'  (selector miss / no download: {str(exc)[:120]})')
                continue

        if not clicked:
            print('  ! could not trigger Download click')

        time.sleep(5)
        out_fh.close()
        ctx.close()

    print(f'\nXHR log:  {jsonl_path}')
    print(f'Files:    {download_dir}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
