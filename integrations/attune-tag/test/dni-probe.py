#!/usr/bin/env python3
"""
DNI probe — is a site's call tracking visitor-level or source-level?

This is the test that decides whether calls can ever be attributed to a
household. It needs no cooperation from the call-tracking vendor: it just loads
the page repeatedly as a brand-new visitor and watches whether the phone number
changes.

    visitor-level (a per-session number pool) -> numbers ROTATE  -> IP available
    source-level  (one fixed number per source) -> numbers STATIC -> no IP

Most DNI only swaps the number when a visitor arrives with a marketing source,
so by default this sends a referrer and UTM parameters. Run --plain too and
compare: a site that rotates only with a source is still visitor-level, it just
gates on attribution.

Usage:
    ./dni-probe.py https://example.com
    ./dni-probe.py https://example.com --runs 12 --plain
    CHROME=/path/to/chrome ./dni-probe.py https://example.com

Run it from a machine that can actually reach the site — not a locked-down
build container.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from glob import glob

# Matches tel: hrefs and visible US numbers in a few common formats.
TEL_HREF = re.compile(r'href=["\']tel:([+0-9().\-\s]{7,})["\']', re.I)
# Guard only against adjacent DIGITS. Guarding against '<' and '>' as well
# silently skips every number that sits flush against a tag -- e.g.
# '<p>(405) 555-9999</p>' -- which is how most sites render one.
VISIBLE = re.compile(r'(?<!\d)(?:\+?1[\s.\-]?)?\(?([2-9]\d{2})\)?[\s.\-]?(\d{3})[\s.\-]?(\d{4})(?!\d)')


def find_chrome():
    if os.environ.get("CHROME"):
        return os.environ["CHROME"]
    for pat in ("/opt/pw-browsers/chromium-*/chrome-linux/chrome",
                "/opt/pw-browsers/chromium_headless_shell-*/chrome-linux/headless_shell"):
        hits = sorted(glob(pat))
        if hits:
            return hits[-1]
    for name in ("chromium", "chromium-browser", "google-chrome"):
        p = shutil.which(name)
        if p:
            return p
    sys.exit("No Chromium found. Set CHROME=/path/to/chrome")


def normalise(raw):
    """Reduce a number to its 10 digits so formatting differences don't count as rotation."""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None


def load_once(chrome, url, profile):
    cmd = [
        chrome, "--headless=new", "--disable-gpu", "--no-sandbox",
        f"--user-data-dir={profile}",           # fresh profile => brand-new visitor
        "--incognito",
        "--virtual-time-budget=9000",
        "--dump-dom", url,
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, timeout=90).stdout.decode("utf-8", "replace")
    except subprocess.TimeoutExpired:
        return None
    return out


SCRIPTY = re.compile(r"<(script|style|template)\b.*?</\1>", re.I | re.S)
COMMENT = re.compile(r"<!--.*?-->", re.S)


def strip_non_rendered(dom):
    """Drop script/style/template bodies and comments.

    Without this the probe scrapes a DNI vendor's own number pool straight out
    of its inline JavaScript and reports every pool number on every visit —
    which looks exactly like a static number and inverts the verdict.
    """
    return COMMENT.sub(" ", SCRIPTY.sub(" ", dom))


def numbers_in(dom):
    visible_dom = strip_non_rendered(dom)
    found = set()
    for raw in TEL_HREF.findall(visible_dom):
        n = normalise(raw)
        if n:
            found.add(n)
    for a, b, c in VISIBLE.findall(visible_dom):
        n = normalise(a + b + c)
        if n:
            found.add(n)
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--runs", type=int, default=8, help="fresh visits to make (default 8)")
    ap.add_argument("--plain", action="store_true",
                    help="visit with no referrer or UTM (tests whether swapping is source-gated)")
    args = ap.parse_args()

    chrome = find_chrome()
    url = args.url
    if not args.plain:
        sep = "&" if "?" in url else "?"
        url = (f"{url}{sep}utm_source=ctv&utm_medium=tv&utm_campaign=dni_probe"
               f"&gclid=dniprobe123")

    print(f"probing  : {url}")
    print(f"mode     : {'plain (no source)' if args.plain else 'with marketing source'}")
    print(f"visits   : {args.runs}\n")

    seen = Counter()
    per_visit = []
    for i in range(args.runs):
        profile = tempfile.mkdtemp(prefix="dniprobe-")
        try:
            dom = load_once(chrome, url, profile)
            if dom is None:
                print(f"  visit {i+1:>2}: timed out")
                per_visit.append(set())
                continue
            nums = numbers_in(dom)
            per_visit.append(nums)
            seen.update(nums)
            shown = ", ".join(sorted(nums)) if nums else "(none found)"
            print(f"  visit {i+1:>2}: {shown}")
        finally:
            shutil.rmtree(profile, ignore_errors=True)

    print()
    if not seen:
        print("VERDICT: no phone numbers found.")
        print("  The number may be inside an image, an iframe, or injected after our")
        print("  snapshot. Check the page by hand before concluding anything.")
        return 2

    # A number present on every visit is the static/fallback number.
    always = {n for n in seen if all(n in v for v in per_visit if v)}
    rotating = set(seen) - always

    print(f"distinct numbers seen: {len(seen)}")
    for n, c in seen.most_common():
        tag = "constant" if n in always else "ROTATES"
        print(f"  {n}  seen {c}/{args.runs}  [{tag}]")
    print()

    if rotating:
        print("VERDICT: VISITOR-LEVEL DNI (a number pool).")
        print("  The number changes per visitor, so the vendor is tracking individual")
        print("  sessions and will have an IP for each one. Attributed calls are possible.")
        print("  Next: confirm the webhook actually carries that IP (see Test B).")
        return 0

    print("VERDICT: STATIC / SOURCE-LEVEL.")
    print("  The same number is shown to every visitor, so no individual session is")
    print("  being identified and there is no visitor IP to attribute against.")
    if not args.plain:
        print("  You sent a marketing source and it still did not swap, which is the")
        print("  stronger form of this result.")
    else:
        print("  Re-run without --plain before concluding: some setups only swap for")
        print("  visitors arriving with a campaign source.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
