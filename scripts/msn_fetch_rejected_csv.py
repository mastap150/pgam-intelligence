#!/usr/bin/env python3
"""
scripts/msn_fetch_rejected_csv.py

The aggregate /partnerrejecteddocstats endpoint we already capture has
`isExportingCsv=false` in its query string. Try flipping that to true
(and/or contiguous variants like &exportType=csv) — most MS exports
follow this pattern. If the response is CSV, that's our per-doc list.

Runs from inside the persistent session so the request carries the
auth cookies. Saves every response body to disk for inspection.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from playwright.sync_api import sync_playwright

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
OUT_DIR = SESSION_DIR / 'probe' / f'csv-{datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")}'
OUT_DIR.mkdir(parents=True, exist_ok=True)

API = 'https://api.msn.com/msn/v0/pages/ugc/contents/report/partnerrejecteddocstats'
APIKEY = 'tfFF5vu2Sk8ndqqn6je2Vo4qOFve5LeicxEpNSnoZK'

# A handful of param variants to try. First should match what we already
# capture (aggregate), then variants likely to produce per-doc results.
VARIANTS = [
    {'name': 'baseline-aggregate', 'params': {'isExportingCsv': 'false'}},
    {'name': 'csv-export',         'params': {'isExportingCsv': 'true'}},
    {'name': 'csv-export-top500',  'params': {'isExportingCsv': 'true', '$top': '500', '$skip': '0'}},
    {'name': 'with-orderBy',       'params': {'isExportingCsv': 'true', '$orderBy': 'date', '$top': '500', '$skip': '0'}},
    {'name': 'no-csv-top500',      'params': {'isExportingCsv': 'false', '$top': '500', '$skip': '0'}},
]

COMMON_PARAMS = {
    'apikey':       APIKEY,
    'fdhead':       'prg-ugc-benchmark,prg-ugc-shortinsight,prg-ugc-pcm',
    'ocid':         'msph',
    'partnerId':    'AA1lKiff',
    'partnerType':  '2',
    'scn':          'MSNRPSAuth',
    'skipaadal':    'true',
    'timeout':      '30000',
    'ugc-flights':  'prg-ugc-benchmark,prg-ugc-shortinsight,prg-ugc-pcm',
    'wrapodata':    'false',
}


def main() -> int:
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=True,
            user_agent=(
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/148.0.0.0 Safari/537.36'
            ),
        )
        page = ctx.new_page()
        # Need to load a partnerhub page first to set up auth cookies for fetch
        page.goto('https://www.msn.com/en-us/partnerhub/home',
                  wait_until='domcontentloaded', timeout=30_000)
        try:
            page.wait_for_load_state('networkidle', timeout=20_000)
        except Exception:
            pass
        time.sleep(2)

        for v in VARIANTS:
            qs = {**COMMON_PARAMS, **v['params']}
            url = f'{API}?{urlencode(qs)}'
            print(f'\n--- {v["name"]}')
            print(f'    {url[:200]}')
            try:
                resp = page.evaluate(r"""
                    async (url) => {
                      const r = await fetch(url, {
                        method: 'GET',
                        credentials: 'include',
                        headers: { 'accept': '*/*' },
                      });
                      const ct = r.headers.get('content-type') || '';
                      const text = await r.text();
                      return { status: r.status, contentType: ct, text };
                    }
                """, url)
                status = resp.get('status')
                ct = resp.get('contentType', '')
                text = resp.get('text', '') or ''
                print(f'    HTTP {status}  CT={ct}  body={len(text)} bytes')
                preview = text[:500].replace('\n', ' \\n ')
                print(f'    preview: {preview}')
                # Save full body
                ext = 'csv' if 'csv' in ct.lower() else ('json' if 'json' in ct.lower() else 'txt')
                outp = OUT_DIR / f'{v["name"]}.{ext}'
                outp.write_text(text, encoding='utf-8')
                print(f'    saved → {outp}')
            except Exception as exc:
                print(f'    EXC: {exc}')

        ctx.close()
    print(f'\nAll results: {OUT_DIR}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
