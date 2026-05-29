#!/usr/bin/env python3
"""
scripts/msn_partnerhub_dom_probe.py

Visit Partner Hub /content, dump screenshot + every visible clickable
element (text + selector) so we can figure out what to click to reach
the rejected-articles list. One-shot diagnostic.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

SESSION_DIR = Path.home() / '.pgam' / 'msn-session'
OUT_DIR = SESSION_DIR / 'probe'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> int:
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
        page.goto('https://www.msn.com/en-us/partnerhub/content',
                  wait_until='domcontentloaded', timeout=30_000)
        try:
            page.wait_for_load_state('networkidle', timeout=20_000)
        except Exception:
            pass
        time.sleep(3)

        # Screenshot
        shot = OUT_DIR / 'content-page.png'
        page.screenshot(path=str(shot), full_page=True)
        print(f'screenshot: {shot}')

        # Dump every link, button, tab, anything with role
        elements = page.evaluate(r"""
            () => {
              const out = [];
              const all = document.querySelectorAll('a, button, [role="tab"], [role="button"], [role="menuitem"], [data-testid]');
              all.forEach(el => {
                const r = el.getBoundingClientRect();
                if (r.width === 0 && r.height === 0) return;
                out.push({
                  tag: el.tagName.toLowerCase(),
                  role: el.getAttribute('role') || '',
                  text: (el.innerText || el.textContent || '').trim().slice(0, 80),
                  href: el.getAttribute('href') || '',
                  ariaLabel: el.getAttribute('aria-label') || '',
                  testId: el.getAttribute('data-testid') || '',
                  classes: (el.className || '').toString().slice(0, 60),
                  visible: r.top >= 0 && r.top < window.innerHeight,
                });
              });
              return out;
            }
        """)
        out_txt = OUT_DIR / 'content-elements.txt'
        with out_txt.open('w', encoding='utf-8') as f:
            for e in elements:
                line = (f"[{e['tag']:7s}] role={e['role']:10s} "
                        f"vis={'Y' if e['visible'] else 'n'} "
                        f"text={e['text']!r:60s} href={e['href']!r:50s} "
                        f"aria={e['ariaLabel']!r:30s} testId={e['testId']!r}")
                f.write(line + '\n')
        print(f'elements:   {out_txt}  ({len(elements)} items)')

        # Also dump the page URL after settling (SPA may have rewritten it)
        print(f'final URL:  {page.url}')

        ctx.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
