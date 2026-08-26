#!/usr/bin/env python3
"""Read-only capability probe for CJ Affiliate, plus a program shortlist.

Same posture as scripts/impact_probe.py: `core/cj_api.py` was written from
CJ's published REST overview, not against a live account — the CJ hosts are
unreachable from this repo's cloud sessions and no CJ credential existed when
it was written (2026-08-26). Transport is documented and probably right; the
per-account response shape is what needs confirming.

Run this first, on a machine that has the token.

Usage
-----
    # auth only — and it distinguishes CJ's three different 401s
    python3 scripts/cj_probe.py

    # current relationships and their states (what a status watcher will poll)
    python3 scripts/cj_probe.py --relationships

    # the point of the exercise: programs NOT yet joined, ranked
    python3 scripts/cj_probe.py --shortlist --keywords "hotel"

    # is the gated credit-card catalogue open to us yet?
    python3 scripts/cj_probe.py --offers

    python3 scripts/cj_probe.py --relationships --shortlist --json /tmp/cj.json

Nothing here writes. There is no CJ endpoint that joins a program, so nothing
here applies to anything — it tells you which programs are worth the click.
"""

from __future__ import annotations

import argparse
import json
import sys

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import cj_api as cj              # noqa: E402
from core.cj_api import CjError            # noqa: E402

OK, FAIL, SKIP = "✓", "✗", "–"

# Fields without which a shortlist row cannot be acted on.
CRITICAL = ("advertiser_id", "name", "relationship")


def _f(value) -> float:
    """CJ numerics arrive as strings, sometimes with % or $ attached."""
    if value in (None, "", "-"):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("%", "")
                     .replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0


def check_field_mapping(rows: list[dict]) -> dict:
    """Which vendor spelling actually resolved for each logical field."""
    print("\nField mapping (core.cj_api.ADVERTISER_FIELDS vs live rows)")
    print("-" * 70)
    report: dict = {}
    n = len(rows)
    for logical, candidates in cj.ADVERTISER_FIELDS.items():
        hits: dict[str, int] = {}
        for row in rows:
            for key in candidates:
                if key in row and row[key] not in (None, ""):
                    hits[key] = hits.get(key, 0) + 1
                    break
        resolved = sum(hits.values())
        mark = OK if resolved else (FAIL if logical in CRITICAL else SKIP)
        via = ", ".join(f"{k} ({v})" for k, v in sorted(hits.items(),
                                                        key=lambda kv: -kv[1]))
        note = ""
        if not resolved:
            note = ("  <-- CRITICAL" if logical in CRITICAL
                    else "  (will be blank)")
        print(f"  {mark} {logical:18} {resolved}/{n:<5} {via}{note}")
        report[logical] = {"resolved": resolved, "of": n, "via": hits}

    known = {k for c in cj.ADVERTISER_FIELDS.values() for k in c}
    unmapped: dict[str, int] = {}
    for row in rows:
        for key in row:
            if key in known:
                continue
            # _xml_to_records stores a nested leaf under both its bare tag and
            # its compound path. When the compound form is already mapped, the
            # bare one is the same value seen twice, not an unmapped field —
            # listing it would send someone hunting for a "parent" column.
            if any(k.endswith(f"-{key}") for k in known):
                continue
            unmapped[key] = unmapped.get(key, 0) + 1
    if unmapped:
        print("\nVendor keys NOT mapped (candidates for new fields):")
        for key, count in sorted(unmapped.items(), key=lambda kv: -kv[1])[:25]:
            print(f"    {key}  ({count}/{n})")
        report["_unmapped"] = unmapped

    missing = [f for f in CRITICAL if not report[f]["resolved"]]
    report["_missing_critical"] = missing
    if missing:
        print(f"\n{FAIL} CRITICAL field(s) unresolved: {', '.join(missing)} — "
              f"add the real spelling to ADVERTISER_FIELDS in core/cj_api.py")
    else:
        print(f"\n{OK} all critical fields resolve.")
    return report


def show_relationships(rows: list[dict]) -> dict:
    """
    Current relationships by state.

    This is the surface a status watcher polls: `pending -> joined` on
    Marriott / Hilton / IHG / Hyatt is the event that turns a `linkId: null`
    placeholder in destination.com's cj-advertisers.ts into revenue.
    """
    by_state: dict[str, list[str]] = {}
    for row in rows:
        state = str(cj.advertiser_field(row, "relationship", "(unknown)")).lower()
        by_state.setdefault(state, []).append(
            str(cj.advertiser_field(row, "name", "(unnamed)")))

    print("\nRelationships by state")
    print("-" * 70)
    for state in sorted(by_state, key=lambda s: -len(by_state[s])):
        names = by_state[state]
        known = state in cj.RELATIONSHIP_STATES
        print(f"  {OK if known else FAIL} {state:18} {len(names):>4}  "
              f"{', '.join(names[:6])}{' …' if len(names) > 6 else ''}")
        if not known:
            print(f"      not in RELATIONSHIP_STATES — add it before a watcher "
                  f"treats it as a change")
    return {state: len(names) for state, names in by_state.items()}


def score(row: dict) -> tuple[float, str]:
    """
    Rank an unjoined program on what CJ itself reports.

    Deliberately crude and deliberately transparent: EPC is the only number
    here that reflects money actually earned by publishers, so it dominates,
    and network rank breaks ties. No content-fit weighting — that needs
    destination.com's own top-page data, which is in another repo and not
    something to fake with a guess.

    Returns (score, why) so the shortlist explains itself rather than handing
    over an unauditable ordering.
    """
    epc3 = _f(cj.advertiser_field(row, "three_month_epc"))
    epc7 = _f(cj.advertiser_field(row, "seven_day_epc"))
    rank = _f(cj.advertiser_field(row, "network_rank"))

    # 3-month EPC is the steadier signal; 7-day catches something heating up.
    value = (epc3 * 2.0) + epc7
    # network-rank is a percentile where higher is better on CJ.
    value += rank / 100.0

    why = []
    if epc3:
        why.append(f"3mo EPC {epc3:.2f}")
    if epc7:
        why.append(f"7d EPC {epc7:.2f}")
    if rank:
        why.append(f"rank {rank:.0f}")
    return value, ", ".join(why) or "no performance data published"


def show_shortlist(rows: list[dict], top: int) -> list[dict]:
    """Rank programs not yet joined, and say why each is ranked there."""
    scored = []
    for row in rows:
        value, why = score(row)
        scored.append({
            "advertiser_id": cj.advertiser_field(row, "advertiser_id"),
            "name": cj.advertiser_field(row, "name"),
            "category": cj.advertiser_field(row, "category"),
            "relationship": cj.advertiser_field(row, "relationship"),
            "score": round(value, 3),
            "why": why,
        })
    scored.sort(key=lambda r: -r["score"])

    print(f"\nShortlist — programs not yet joined, ranked ({len(scored)} found)")
    print("-" * 70)
    for entry in scored[:top]:
        print(f"  {entry['score']:>8.2f}  {str(entry['name'])[:34]:34} "
              f"{str(entry['category'] or '')[:16]:16} {entry['why']}")
    if len(scored) > top:
        print(f"  … {len(scored) - top} more (raise --top to see them)")

    no_data = sum(1 for e in scored if e["why"].startswith("no performance"))
    if no_data:
        # Said out loud because an unranked program is not a bad program —
        # it is one CJ publishes no numbers for, and treating a zero score as
        # a judgement would bury every new advertiser on the network.
        print(f"\n  NOTE: {no_data} of {len(scored)} publish no EPC or rank, so "
              f"they score 0 — that is missing data, not a poor program.")

    print("\n  Applying is a UI act: CJ exposes no join endpoint, so open each "
          "\n  program in members.cj.com and apply there. Nothing here submits "
          "anything.")
    return scored


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only probe of CJ Affiliate for PGAM's publisher account")
    parser.add_argument("--relationships", action="store_true",
                        help="list current advertiser relationships by state")
    parser.add_argument("--shortlist", action="store_true",
                        help="rank programs NOT yet joined")
    parser.add_argument("--offers", action="store_true",
                        help="check whether the gated credit-card offer feed "
                             "is open to this account")
    parser.add_argument("--keywords", help="narrow the shortlist by keyword")
    parser.add_argument("--top", type=int, default=25,
                        help="shortlist rows to print (default 25)")
    parser.add_argument("--json", metavar="PATH", help="write findings as JSON")
    args = parser.parse_args()

    results: dict = {"endpoints": cj.ENDPOINTS}
    print(f"CJ probe — cid={cj.CJ_COMPANY_ID or '(unset)'} "
          f"pid={cj.CJ_WEBSITE_ID or '(unset)'}")

    if not cj.configured():
        print(f"\n{FAIL} not configured — missing {', '.join(cj.missing_env())}")
        print("    Token: members.cj.com -> Account -> Manage API Keys "
              "(shown once).")
        print("    export CJ_PERSONAL_ACCESS_TOKEN=...")
        print("    export CJ_COMPANY_ID=7112482      # destination.com")
        print("    export CJ_WEBSITE_ID=101849129")
        return 2

    print("\nAuth")
    conn = cj.test_connection()
    results["connection"] = conn
    if not conn.get("ok"):
        print(f"  {FAIL} {conn.get('error')}")
        # The verdict already names which of CJ's three 401s this is, so no
        # further guidance here would add anything.
        if args.json:
            _dump(args.json, results)
        return 1
    print(f"  {OK} authenticated — {conn['joined_advertisers']} joined "
          f"advertiser(s): {', '.join(str(s) for s in conn['sample'] if s)}")

    joined: list[dict] = []
    if args.relationships or args.shortlist:
        try:
            joined = cj.joined_advertisers()
        except CjError as exc:
            print(f"  {FAIL} advertiser lookup (joined) — {exc}")

    if joined:
        results["field_mapping"] = check_field_mapping(joined)

    if args.relationships and joined:
        results["relationships"] = show_relationships(joined)

    if args.shortlist:
        try:
            candidates = cj.advertiser_lookup(joined=False,
                                              keywords=args.keywords)
        except CjError as exc:
            print(f"\n{FAIL} advertiser lookup (not joined) — {exc}")
            candidates = []
        if candidates:
            results["shortlist"] = show_shortlist(candidates, args.top)
        else:
            print(f"\n{SKIP} no unjoined advertisers returned — narrow or drop "
                  f"--keywords, or the account may already be joined to "
                  f"everything matching.")

    if args.offers:
        print("\nAutomated Offer Feed (credit cards — the gated category)")
        try:
            offers = cj.offer_feed()
        except CjError as exc:
            print(f"  {FAIL} {exc}")
            results["offer_feed"] = {"ok": False, "error": str(exc)}
        else:
            if offers:
                print(f"  {OK} {len(offers)} offer(s) — the financial vertical "
                      f"is OPEN to this account")
                print(f"      sample keys: {sorted(offers[0].keys())[:12]}")
            else:
                # An empty feed is the documented shape of "not approved",
                # not of "no offers exist" — worth stating, because the two
                # look identical and imply completely different next steps.
                print(f"  {SKIP} empty. For this feed that means NOT APPROVED "
                      f"for the financial vertical, not that there are no "
                      f"offers. Applying for it is the highest-value item in "
                      f"08_monetization_strategy.md.")
            results["offer_feed"] = {"ok": True, "count": len(offers)}

    if args.json:
        _dump(args.json, results)

    fm = results.get("field_mapping") or {}
    return 1 if fm.get("_missing_critical") else 0


def _dump(path: str, results: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nwrote {path}")


if __name__ == "__main__":
    raise SystemExit(main())
