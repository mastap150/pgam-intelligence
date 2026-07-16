#!/usr/bin/env python3
"""
scripts/extract_storage_state.py

Extract a platform-portable Playwright `storage_state` JSON from the
local persistent Chromium profile at ~/.pgam/msn-session.

Why: Chromium's on-disk profile encrypts cookies with an OS-specific
key (macOS Keychain, Linux libsecret/gnomekeyring, Windows DPAPI). A
profile dir created on macOS can be RESTORED on a Linux runner but
the cookies decrypt to garbage → MSN redirects to login. Playwright's
`storage_state` is plain JSON that survives the OS hop cleanly.

Usage:
    python3 scripts/extract_storage_state.py            # writes to /tmp/msn-storage-state.json
    python3 scripts/extract_storage_state.py --out X    # custom path

After extraction, upload to Neon via session_backup_restore.py
(json mode), then the GH Actions workflow can restore + use it.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

PARTNER_HUB_URL = "https://www.msn.com/en-us/partnerhub/analytics/realtime/headline"
DEFAULT_SESSION_DIR = Path.home() / ".pgam" / "msn-session"
DEFAULT_OUT_PATH = Path("/tmp/msn-storage-state.json")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--session-dir", default=str(DEFAULT_SESSION_DIR))
    p.add_argument("--out", default=str(DEFAULT_OUT_PATH))
    args = p.parse_args()

    session_dir = Path(args.session_dir).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()

    if not session_dir.exists():
        print(f"[extract] session dir does not exist: {session_dir}", file=sys.stderr)
        return 1

    print(f"[extract] launching Chromium with persistent context at {session_dir}")
    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(session_dir),
            headless=True,
            viewport={"width": 1400, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/148.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()
        print(f"[extract] navigating to {PARTNER_HUB_URL}")
        page.goto(PARTNER_HUB_URL, wait_until="domcontentloaded")
        # Let MSAL settle so any pending token refresh writes to localStorage
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        print(f"[extract] page url after settle: {page.url}")
        if "/login" in page.url:
            print(
                "[extract] WARNING: page redirected to a login URL — session "
                "may be stale. Re-run MSN_HEADLESS=0 puller to re-authenticate.",
                file=sys.stderr,
            )
        ctx.storage_state(path=str(out_path))
        ctx.close()

    size = out_path.stat().st_size
    print(f"[extract] wrote {size:,} bytes to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
