#!/usr/bin/env python3
"""
Launch preflight — can this client go live, and if not, what is blocking?

    scripts/preflight.py clients/homebuyerforcash.json
    scripts/preflight.py clients/homebuyerforcash.json --no-network

Groups checks by who has to act, because the answer is almost always "waiting
on someone" rather than "broken". Exit 0 = clear to launch.

Network checks are best-effort and marked SKIP when a host is unreachable — a
locked-down build container cannot see the client's site, and that is not a
failure of the client's setup.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_campaign_payload as B  # noqa: E402

E164 = re.compile(r"^\+[1-9]\d{7,14}$")

OK, BLOCK, WARN, SKIP = "OK", "BLOCK", "WARN", "SKIP"
MARK = {OK: "  ok ", BLOCK: "BLOCK", WARN: " warn", SKIP: " skip"}

results = []


def record(owner, status, name, detail=""):
    results.append((owner, status, name, detail))


def http_ok(url, timeout=8):
    """(reachable, detail). Distinguishes 'said no' from 'could not ask'."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "attune-preflight/1"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return True, f"HTTP {r.status}"
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}"
    except Exception as e:                                  # noqa: BLE001
        return None, type(e).__name__


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config")
    ap.add_argument("--no-network", action="store_true")
    args = ap.parse_args()

    with open(args.config) as fh:
        cfg = json.load(fh)

    name = cfg.get("client", {}).get("name", args.config)

    # ---------------- campaign config ----------------
    rep = B.validate(cfg, B.Report())
    if rep.errors:
        for e in rep.errors:
            owner = "client" if "must come from the advertiser" in e else "us"
            record(owner, BLOCK, "campaign config", e)
    else:
        record("us", OK, "campaign config", "validates")
    for w in rep.warnings:
        record("us", WARN, "campaign config", w)

    v = cfg.get("vibe", {})
    record("us", OK if v.get("advertiser_id") else BLOCK, "advertiser record",
           "created" if v.get("advertiser_id") else "no API for this — create it in the dashboard")
    record("us", OK if v.get("pixel_id") else BLOCK, "pixel id",
           v.get("pixel_id") or "issued with the advertiser record")

    # ---------------- measurement ----------------
    m = cfg.get("measurement", {})

    pool = m.get("call_pool") or []
    if not pool:
        record("us", WARN, "call pool", "no numbers — calls will not be attributable")
    else:
        bad = [n for n in pool if not E164.match(str(n))]
        record("us", BLOCK if bad else OK, "call pool",
               f"{len(bad)} malformed: {bad}" if bad else f"{len(pool)} numbers, all E.164")

    record("us", OK if m.get("fallback_number") else WARN, "fallback number",
           m.get("fallback_number") or "pool exhaustion would leave no number to show")
    record("client", OK if m.get("tv_number") else WARN, "TV number",
           m.get("tv_number") or "decide before the video edit is locked")

    # ---------------- network ----------------
    if args.no_network:
        record("us", SKIP, "network checks", "--no-network")
    else:
        tag = m.get("tag_url")
        if tag:
            up, detail = http_ok(tag)
            if up is None:
                record("us", SKIP, "tag hosted", f"unreachable from here ({detail})")
            else:
                record("us", OK if up else BLOCK, "tag hosted", detail)
        else:
            record("us", BLOCK, "tag hosted", "measurement.tag_url is not set")

        bridge = m.get("bridge_url")
        if not bridge:
            record("us", WARN, "bridge", "not deployed — only needed if we run our own numbers")
        else:
            up, detail = http_ok(bridge.rstrip("/") + "/health")
            if up is None:
                record("us", SKIP, "bridge healthy", f"unreachable from here ({detail})")
            else:
                record("us", OK if up else BLOCK, "bridge healthy", detail)

        site = cfg.get("client", {}).get("website")
        if site:
            up, detail = http_ok(site)
            if up is None:
                record("client", SKIP, "site reachable", f"unreachable from here ({detail})")
            else:
                record("client", OK if up else WARN, "site reachable", detail)

    # ---------------- report ----------------
    width = max(len(r[2]) for r in results)
    print(f"\nPreflight — {name}\n")
    for owner in ("us", "client"):
        rows = [r for r in results if r[0] == owner]
        if not rows:
            continue
        print(f"  {'PGAM' if owner == 'us' else 'CLIENT'}")
        for _, status, label, detail in rows:
            print(f"    [{MARK[status]}] {label.ljust(width)}  {detail}")
        print()

    blockers = [r for r in results if r[1] == BLOCK]
    warns = [r for r in results if r[1] == WARN]
    skips = [r for r in results if r[1] == SKIP]

    if blockers:
        print(f"NOT READY — {len(blockers)} blocker(s):\n")
        for owner, _, label, detail in blockers:
            print(f"  • [{'PGAM' if owner == 'us' else 'CLIENT'}] {label}: {detail}")
        print()
    else:
        print("CLEAR TO LAUNCH" + (f" — {len(warns)} warning(s) worth reading first" if warns else ""))
    if skips:
        print(f"({len(skips)} check(s) skipped — run from a machine with network access "
              f"to the client's site to complete them)")

    return 1 if blockers else 0


if __name__ == "__main__":
    sys.exit(main())
