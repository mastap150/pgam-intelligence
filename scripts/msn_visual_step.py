#!/usr/bin/env python3
"""
scripts/msn_visual_step.py

Single-step visual driver. Runs one ACTION on Partner Hub, screenshots
before + after, dumps current URL + clickable elements + captured XHRs
to a step folder. Idea: I run this with one action arg, read the
output, decide the next action, re-run.

Usage:
    python3 scripts/msn_visual_step.py <action>

Actions:
    home                       -- just load /partnerhub/home and probe
    click-content-issues       -- click the "Resolve content issues" card
    click-boxing-news          -- click the "Boxing News" brand row
    click-download-inline      -- click any 'Download' text inside an issues card
    click-text:<text>          -- generic: click any element whose visible text
                                  exactly matches <text>
    goto:<path>                -- navigate to https://www.msn.com<path>
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, Response

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'


def main(action: str) -> int:
    ts = datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    step_dir = SESSION_DIR / 'probe' / f'step-{ts}-{action[:30].replace(":", "_")}'
    step_dir.mkdir(parents=True, exist_ok=True)
    print(f'step dir: {step_dir}', file=sys.stderr)

    xhr_log = (step_dir / 'xhr.jsonl').open('a', encoding='utf-8')
    counters = {'xhr': 0}

    def on_response(resp: Response) -> None:
        try:
            url = resp.url
            if 'api.msn.com' not in url:
                return
            ct = (resp.headers.get('content-type') or '').lower()
            if 'json' not in ct:
                return
            try:
                body = resp.json()
            except Exception:
                return
            xhr_log.write(json.dumps({
                'captured_at': datetime.now(tz=timezone.utc).isoformat(),
                'status':      resp.status,
                'url':         url,
                'method':      resp.request.method,
                'body':        body,
            }, ensure_ascii=False) + '\n')
            xhr_log.flush()
            counters['xhr'] += 1
        except Exception:
            pass

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

        # Always start at home
        page.goto('https://www.msn.com/en-us/partnerhub/home',
                  wait_until='domcontentloaded', timeout=30_000)
        try:
            page.wait_for_load_state('networkidle', timeout=20_000)
        except Exception:
            pass
        time.sleep(3)

        page.screenshot(path=str(step_dir / 'before.png'), full_page=True)

        # Execute action
        action_result = 'no-op'
        try:
            if action == 'home':
                action_result = 'loaded home only'
            elif action == 'click-content-issues':
                # Find the card by its title text and click it
                page.locator('text=Resolve content issues').first.click(timeout=8_000)
                action_result = 'clicked card'
            elif action == 'click-boxing-news':
                page.locator('text=Boxing News').first.click(timeout=8_000)
                action_result = 'clicked Boxing News'
            elif action == 'click-download-inline':
                # Try multiple ways
                for fn in (
                    lambda: page.locator('a:has-text("Download")').first,
                    lambda: page.locator('button:has-text("Download")').first,
                    lambda: page.get_by_text('Download report', exact=False).first,
                ):
                    try:
                        with page.expect_download(timeout=10_000) as dl_info:
                            fn().click(timeout=4_000)
                        dl = dl_info.value
                        target = step_dir / (dl.suggested_filename or 'download.bin')
                        dl.save_as(str(target))
                        action_result = f'downloaded {target.name}'
                        break
                    except Exception as exc:
                        action_result = f'download miss: {str(exc)[:100]}'
                        continue
            elif action.startswith('click-text:'):
                txt = action.split(':', 1)[1]
                page.get_by_text(txt, exact=True).first.click(timeout=8_000)
                action_result = f'clicked text={txt!r}'
            elif action.startswith('goto:'):
                path = action.split(':', 1)[1]
                page.goto(f'https://www.msn.com{path}',
                          wait_until='domcontentloaded', timeout=20_000)
                action_result = f'goto {path}'
            else:
                action_result = f'unknown action: {action}'
        except Exception as exc:
            action_result = f'action exception: {exc}'

        # Wait for any XHR to settle.
        try:
            page.wait_for_load_state('networkidle', timeout=12_000)
        except Exception:
            pass
        time.sleep(2)

        page.screenshot(path=str(step_dir / 'after.png'), full_page=True)

        # Dump url + visible clickable elements
        final_url = page.url
        elements = page.evaluate(r"""
            () => {
              const out = [];
              const all = document.querySelectorAll('a, button, [role="tab"], [role="button"], [role="menuitem"], [data-testid], li[role], div[onclick]');
              all.forEach(el => {
                const r = el.getBoundingClientRect();
                if (r.width < 5 || r.height < 5) return;
                out.push({
                  tag: el.tagName.toLowerCase(),
                  role: el.getAttribute('role') || '',
                  text: (el.innerText || el.textContent || '').trim().slice(0, 100),
                  href: el.getAttribute('href') || '',
                  ariaLabel: el.getAttribute('aria-label') || '',
                  testId: el.getAttribute('data-testid') || '',
                });
              });
              return out;
            }
        """)
        with (step_dir / 'state.json').open('w') as f:
            json.dump({
                'action':       action,
                'action_result': action_result,
                'final_url':    final_url,
                'xhr_count':    counters['xhr'],
                'elements':     elements,
            }, f, indent=2, ensure_ascii=False)

        xhr_log.close()
        ctx.close()

    print(f'  action: {action}')
    print(f'  result: {action_result}')
    print(f'  url:    {final_url}')
    print(f'  xhr:    {counters["xhr"]}')
    print(f'  step:   {step_dir}')
    return 0


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
