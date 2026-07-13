"""
agents/etl/msn_endpoint_sniffer.py

Ad-hoc XHR sniffer for the MSN Partner Hub SPA. Opens the same
authenticated Playwright session core.msn_partner_hub uses, then
LISTENS on every response hitting api.msn.com while you click
around Partner Hub. Writes each request to a JSONL log and prints
a compact summary at the end.

Purpose
-------
The msn_insights ETL currently reconstructs per-article peak reads
from realtime snapshots because we DON'T have MSN's daily-aggregate
endpoint (see core.msn_partner_hub.fetch_aggregate — the 3 candidate
paths all 404). Without the aggregate we can't measure impressions
(only reads), so we can't tell if the growth ceiling is CTR-side
(readers ignoring us) or reach-side (MSN not showing us).

Running this sniffer while manually clicking through Partner Hub's
"Overview" / "Content report" / "Aggregate" tabs should surface the
real endpoint path in ~30 seconds. Once discovered, we plug it into
core.msn_partner_hub.fetch_aggregate as a confirmed path and the
msn_daily_totals table starts filling.

Usage
-----
    python3 -m agents.etl.msn_endpoint_sniffer            # 90s default, manual clicking
    python3 -m agents.etl.msn_endpoint_sniffer --duration 120
    python3 -m agents.etl.msn_endpoint_sniffer --duration 60 --tabs
    python3 -m agents.etl.msn_endpoint_sniffer --auto-navigate  # no manual clicks

--auto-navigate visits a list of candidate Partner Hub URLs and
also programmatically clicks any nav-links it finds in the DOM,
so the whole flow can run headless (given a fresh session) without
a human in the loop.

Env
---
- MSN_EMAIL, MSN_PASSWORD — same vars core.msn_partner_hub uses
- MSN_HEADLESS defaults to "0" for the sniffer since manual clicking
  is the whole point.

Output
------
- JSONL log at ~/.pgam/msn-endpoint-sniff-<UTC-ISO>.jsonl (one row per
  api.msn.com response). The path is printed at the end.
- Compact summary of unique paths + status codes on stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from core.msn_partner_hub import (
        PartnerHubClient,
        PartnerHubError,
        API_HOST,
        DEFAULT_PARTNER_ID,
    )
except ImportError as exc:
    print(f"[sniffer] import failed: {exc}", file=sys.stderr)
    print("[sniffer] this module requires playwright + core.msn_partner_hub", file=sys.stderr)
    sys.exit(2)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _default_log_path() -> Path:
    return Path.home() / ".pgam" / f"msn-endpoint-sniff-{_ts()}.jsonl"


def _preview_body(body: str, max_chars: int = 400) -> str:
    """Trim response bodies to a preview — we're grepping shapes, not archiving."""
    if body is None:
        return ""
    b = body.strip()
    if len(b) <= max_chars:
        return b
    return b[:max_chars] + f"... ({len(b) - max_chars} more chars)"


# Candidate Partner Hub URLs the SPA might respond to. If any 404 or
# redirect, that's fine — the response listener still logs whatever
# XHRs fired. Ordered from most-likely to least-likely aggregate paths.
_CANDIDATE_URLS = (
    "https://www.msn.com/en-us/partnerhub/analytics/aggregate",
    "https://www.msn.com/en-us/partnerhub/analytics/overview",
    "https://www.msn.com/en-us/partnerhub/analytics/content",
    "https://www.msn.com/en-us/partnerhub/analytics/content-report",
    "https://www.msn.com/en-us/partnerhub/analytics/insights",
    "https://www.msn.com/en-us/partnerhub/analytics/realtime/traffic",
    "https://www.msn.com/en-us/partnerhub/analytics/daily",
    "https://www.msn.com/en-us/partnerhub/analytics/reports",
)


def _auto_navigate(page: Any, dwell_seconds: int = 6) -> None:
    """Visit candidate Partner Hub URLs and click discovered nav-links so
    the SPA fires its per-tab XHRs. Each URL gets `dwell_seconds` to
    resolve — enough for the initial data-load XHRs but not so much
    that the whole sweep takes forever."""
    from urllib.parse import urlsplit

    # Pass 1: direct URL navigation. Path-based SPAs (this one is)
    # generally re-render on real navigations to a router-owned URL.
    for url in _CANDIDATE_URLS:
        print(f"[sniffer] navigating: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            page.wait_for_timeout(dwell_seconds * 1000)
        except Exception as exc:
            print(f"[sniffer]   nav skipped: {type(exc).__name__}: {str(exc)[:120]}")

    # Pass 2: DOM sweep — any <a> in the Partner Hub nav that we
    # haven't already visited via direct URL. Catches tabs that use
    # SPA-internal routing (pushState + client-side handler).
    try:
        anchors = page.locator('a[href*="/partnerhub/"]').all()
        seen = {urlsplit(u).path for u in _CANDIDATE_URLS}
        clicked = 0
        for a in anchors:
            try:
                href = a.get_attribute("href")
                if not href:
                    continue
                path = urlsplit(href).path
                if path in seen:
                    continue
                seen.add(path)
                # Only click nav-shaped things, not action buttons.
                if not a.is_visible(timeout=500):
                    continue
                print(f"[sniffer] clicking nav-link: {href}")
                a.click(timeout=3_000)
                page.wait_for_timeout(dwell_seconds * 1000)
                clicked += 1
                if clicked >= 8:
                    break  # bounded — we're not crawling
            except Exception:
                continue
    except Exception as exc:
        print(f"[sniffer] nav-link sweep skipped: {type(exc).__name__}: {str(exc)[:120]}")


def sniff(duration_seconds: int, tabs_hint: bool, auto_navigate: bool) -> Path:
    """Run the sniffer for `duration_seconds` and return the log path."""
    if auto_navigate:
        # In auto-navigate mode we don't need a human, so headless is fine
        # (and much less disruptive on a workstation). Callers who want to
        # watch can still pass MSN_HEADLESS=0.
        os.environ.setdefault("MSN_HEADLESS", "1")
    else:
        # Manual-clicking mode needs a visible browser.
        os.environ.setdefault("MSN_HEADLESS", "0")

    log_path = _default_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[sniffer] logging to: {log_path}")
    print(f"[sniffer] listening for {duration_seconds}s on responses to {API_HOST}")
    if tabs_hint:
        print(
            "[sniffer] TAB HINTS — click through these tabs while the browser is open:\n"
            "          - Overview (top nav)\n"
            "          - Content report → Aggregate\n"
            "          - Realtime → Headline (already know this one)\n"
            "          - Realtime → Traffic\n"
            "          - Resolve content issues (rejections page)\n"
        )

    captured: list[dict[str, Any]] = []
    counter: Counter = Counter()
    per_path_statuses: dict[str, Counter] = defaultdict(Counter)

    with log_path.open("w") as f_log:

        def _on_response(response: Any) -> None:
            try:
                url = response.url
                if not url.startswith(API_HOST):
                    return
                path = url.split(API_HOST, 1)[-1].split("?", 1)[0]
                counter[path] += 1
                per_path_statuses[path][response.status] += 1

                body_text = ""
                try:
                    body_text = response.text()
                except Exception:
                    body_text = ""

                row = {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "url": url,
                    "path": path,
                    "method": response.request.method if response.request else "?",
                    "status": response.status,
                    "content_type": response.headers.get("content-type", ""),
                    "body_preview": _preview_body(body_text),
                }
                captured.append(row)
                f_log.write(json.dumps(row) + "\n")
                f_log.flush()
                print(f"  [{row['status']}] {row['method']} {path}")
            except Exception as exc:
                # A listener that throws would sever the response pipe —
                # swallow so a single bad response doesn't kill the sniffer.
                print(f"[sniffer] listener error (ignored): {exc}", file=sys.stderr)

        client = PartnerHubClient()
        try:
            client.start()
            # Attach our response listener AFTER the client's own
            # request listener is already in place, so we don't
            # interfere with bearer capture.
            assert client._page is not None, "PartnerHubClient.start() did not create a page"
            client._page.on("response", _on_response)

            if auto_navigate:
                # Kick off programmatic sweep. Response listener is already
                # attached, so every XHR fires goes to the log.
                _auto_navigate(client._page)
                print(f"[sniffer] auto-navigate complete — settling for {duration_seconds}s")
                time.sleep(duration_seconds)
            else:
                deadline = time.time() + duration_seconds
                while time.time() < deadline:
                    remaining = int(deadline - time.time())
                    if remaining % 10 == 0:
                        print(f"[sniffer] {remaining}s left — click around Partner Hub tabs")
                    time.sleep(1)
        finally:
            client.close()

    print()
    print(f"[sniffer] captured {len(captured)} responses across {len(counter)} unique paths")
    print()
    print(f"{'COUNT':>5}  {'PATH'}")
    for path, n in counter.most_common():
        statuses = ",".join(f"{s}×{c}" for s, c in per_path_statuses[path].most_common())
        print(f"{n:>5}  {path}  [{statuses}]")
    print()
    print(f"[sniffer] full log: {log_path}")
    return log_path


def main() -> None:
    parser = argparse.ArgumentParser(description="MSN Partner Hub XHR sniffer")
    parser.add_argument(
        "--duration",
        type=int,
        default=90,
        help="Seconds to keep the browser open and listen (default 90)",
    )
    parser.add_argument(
        "--tabs",
        action="store_true",
        help="Print a checklist of tabs to click while sniffing (manual mode only)",
    )
    parser.add_argument(
        "--auto-navigate",
        action="store_true",
        help=(
            "Programmatically visit candidate Partner Hub URLs and click "
            "discovered nav-links so no manual clicking is needed. Runs "
            "headless by default (set MSN_HEADLESS=0 to watch)."
        ),
    )
    args = parser.parse_args()
    try:
        sniff(
            duration_seconds=args.duration,
            tabs_hint=args.tabs,
            auto_navigate=args.auto_navigate,
        )
    except PartnerHubError as exc:
        print(f"[sniffer] partner hub error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
