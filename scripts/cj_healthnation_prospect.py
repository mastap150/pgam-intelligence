#!/usr/bin/env python3
"""
cj_healthnation_prospect.py — find CJ advertisers that fit healthnation.com.

WHY THIS EXISTS
───────────────
The CJ publisher account (CID 7112482, live 2026-08-02) was opened for
destination.com and every advertiser on it today is travel: Hotels.com, Vrbo,
Casa Andina, EF Adventures, Club del Sole (see mastap150/destination-com
`docs/CJ_AFFILIATES.md` and `src/data/cj-advertisers.ts`).

healthnation.com has the opposite problem. Its 12-product catalog is wired to
`affiliate_network: "skimlinks"`, and Skimlinks was never installed — every
"Check current price" button earns $0 (docs/healthnation-amazon-affiliate-
review-2026-08-30.md §4.1). CJ is an already-open account that could carry
those clicks. The question is which CJ advertisers are worth applying to.

That question was previously answered from memory. It shouldn't be: CJ's
GraphQL catalog is browseable WITHOUT joining any program, so the real
advertiser roster for a given vertical can just be read off the API.

HOW IT WORKS
────────────
`products(companyId:..., keywords:[...])` on ads.api.cj.com searches the full
~2.8M-item catalog and returns `advertiserId` + `advertiserName` per row. This
script fires one probe per HealthNation content lane (taken from the nav in
02_healthnation_com_structure.md), aggregates the advertisers that come back,
and ranks them by how many distinct lanes they cover.

A separate `partnerStatus:JOINED` query marks advertisers we're already on, so
the output separates "apply to this" from "already have it, just wire it up".

WHAT IT CANNOT SEE  ← read this before trusting a short list
────────────────────
Only advertisers publishing a PRODUCT FEED appear in a `products` query.
CJ's health vertical has a large lead-gen/CPA half that publishes no feed at
all — lab testing (Function Health, LetsGetChecked, Everlywell), telehealth
and therapy, meal-delivery and diet programs, insurance. Those are invisible
here no matter how good the keyword is, exactly as the credit-card catalog
reads empty until an issuer approves us.

Find those by hand: members.cj.com → Advertisers → filter by category
(Health & Wellness / Family & Fitness) → sort by 7-day EPC / Network Earnings.
This script covers the supplement + gear + wearable half, which is the half
that maps onto the existing `healthnation.products` catalog.

THE WEBSITE-ID GATE
───────────────────
CJ publisher accounts have a company id (CID) and a per-property website id
(PID). destination.com is PID 101849129. Advertisers approve a PUBLISHER
PROPERTY, not a company — a travel site's approval does not carry to a health
site, and CJ errors with "cannot access requested publisherid" if you build a
link for a PID the token can't reach.

So healthnation.com must exist as its own website under CID 7112482 before any
of this is actionable. Set CJ_HEALTHNATION_WEBSITE_ID once it does and this
script will emit a real tracking URL per advertiser (proving the PID works);
leave it unset and it skips linkCode and says so.

USAGE
─────
  export CJ_PAT=...            # developers.cj.com → Authentication → PAT
  export CJ_CID=7112482
  # export CJ_HEALTHNATION_WEBSITE_ID=...   # once healthnation.com is added

  python3 scripts/cj_healthnation_prospect.py
  python3 scripts/cj_healthnation_prospect.py --lanes nutrition longevity
  python3 scripts/cj_healthnation_prospect.py --min-lanes 3
  python3 scripts/cj_healthnation_prospect.py --joined
  python3 scripts/cj_healthnation_prospect.py --json > prospects.json

Read-only. Nothing here joins a program or writes to healthnation.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict

import requests

ADS_URL = "https://ads.api.cj.com/query"
TIMEOUT = 30

# HealthNation content lanes → catalog probes.
#
# Lane names match the primary nav in 02_healthnation_com_structure.md so a
# hit is directly readable as "this advertiser has stock for that mega-menu".
# Probe terms are AND-ed by CJ, so they stay short and generic — a long phrase
# returns nothing and looks like an absent advertiser rather than a bad query.
LANES: dict[str, list[str]] = {
    "nutrition": [
        "magnesium glycinate",
        "omega 3 fish oil",
        "creatine monohydrate",
        "vitamin d3",
        "probiotic",
        "whey protein",
        "electrolyte powder",
        "multivitamin",
    ],
    "longevity": [
        "nicotinamide riboside",
        "collagen peptides",
        "berberine",
        "coq10 ubiquinol",
    ],
    "fitness": [
        "adjustable dumbbells",
        "resistance bands",
        "foam roller",
        "massage gun",
        "kettlebell",
        "exercise bike",
    ],
    "biomarkers": [
        "blood pressure monitor",
        "body composition scale",
        "fitness tracker",
        "pulse oximeter",
        "ketone meter",
    ],
    "sleep": [
        "weighted blanket",
        "sleep mask",
        "white noise machine",
        "magnesium sleep",
    ],
    "recovery": [
        "compression boots",
        "red light therapy",
        "sauna blanket",
        "ice bath",
    ],
}


def _quote_terms(phrase: str) -> str:
    """Render a probe phrase as a CJ keywords array literal.

    CJ takes `keywords:["a","b"]` and AND-s the terms. Anything that could
    close the string literal is stripped rather than escaped — these are our
    own probes, so a dropped character is a better failure than a malformed
    query that reads as an empty result.
    """
    terms = [re.sub(r'[^\w+.-]', "", t) for t in phrase.split()]
    terms = [t for t in terms if t]
    return "[" + ",".join(f'"{t}"' for t in terms) + "]"


def gql(pat: str, query: str) -> dict:
    res = requests.post(
        ADS_URL,
        headers={"Authorization": f"Bearer {pat}", "Content-Type": "application/json"},
        json={"query": query},
        timeout=TIMEOUT,
    )
    try:
        body = res.json()
    except ValueError:
        raise SystemExit(f"✗ Non-JSON from CJ (HTTP {res.status_code}): {res.text[:200]}")
    if "errors" in body:
        raise SystemExit("✗ GraphQL error: " + json.dumps(body["errors"], indent=2))
    return body["data"]


def probe(pat: str, cid: str, phrase: str, limit: int, website_id: str | None) -> dict:
    """One keyword search against the open catalog. No join required."""
    # linkCode is only requested when we have a website id the token can reach;
    # CJ fails the WHOLE query on a bad pid, so an unset var must not be guessed.
    link_field = f'linkCode(pid:"{website_id}") {{ clickUrl }}' if website_id else ""
    query = f"""{{
      products(companyId:"{cid}", limit:{limit}, keywords:{_quote_terms(phrase)}) {{
        totalCount
        count
        resultList {{
          title
          advertiserId
          advertiserName
          price {{ amount currency }}
          {link_field}
        }}
      }}
    }}"""
    return gql(pat, query)["products"]


def joined_advertisers(pat: str, cid: str) -> dict[str, str]:
    """Advertisers we already have an approved relationship with.

    CJ exposes no advertiser-lookup query; the joined list is derived from a
    product search filtered to partnerStatus:JOINED, same as destination-com's
    cj-pull.mjs `advertisers` subcommand.
    """
    query = f"""{{
      products(companyId:"{cid}", limit:100, partnerStatus:JOINED) {{
        resultList {{ advertiserId advertiserName }}
      }}
    }}"""
    out: dict[str, str] = {}
    for row in gql(pat, query)["products"]["resultList"] or []:
        out.setdefault(str(row["advertiserId"]), row["advertiserName"])
    return out


def collect(pat: str, cid: str, lanes: list[str], limit: int, website_id: str | None,
            delay: float) -> dict[str, dict]:
    advertisers: dict[str, dict] = {}
    for lane in lanes:
        for phrase in LANES[lane]:
            result = probe(pat, cid, phrase, limit, website_id)
            for row in result.get("resultList") or []:
                aid = str(row["advertiserId"])
                rec = advertisers.setdefault(
                    aid,
                    {
                        "advertiserId": aid,
                        "advertiserName": row["advertiserName"],
                        "lanes": set(),
                        "probes": set(),
                        "hits": 0,
                        "samples": [],
                        "clickUrl": None,
                    },
                )
                rec["lanes"].add(lane)
                rec["probes"].add(phrase)
                rec["hits"] += 1
                if len(rec["samples"]) < 3:
                    price = (row.get("price") or {}).get("amount")
                    rec["samples"].append(
                        {"title": row["title"][:80], "price": price, "lane": lane}
                    )
                if rec["clickUrl"] is None and row.get("linkCode"):
                    rec["clickUrl"] = row["linkCode"].get("clickUrl")
            print(
                f"  probe {lane:<11} {phrase:<26} → {result.get('totalCount', 0):>7} in catalog, "
                f"{result.get('count', 0):>3} sampled",
                file=sys.stderr,
            )
            if delay:
                time.sleep(delay)
    return advertisers


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--lanes", nargs="+", choices=sorted(LANES), default=sorted(LANES),
                    help="content lanes to probe (default: all)")
    ap.add_argument("--limit", type=int, default=50,
                    help="products sampled per probe (default 50)")
    ap.add_argument("--min-lanes", type=int, default=1,
                    help="only report advertisers covering at least N lanes")
    ap.add_argument("--joined", action="store_true",
                    help="only report advertisers we are already joined to")
    ap.add_argument("--delay", type=float, default=0.4,
                    help="seconds between probes (default 0.4)")
    ap.add_argument("--json", action="store_true", help="raw JSON instead of the table")
    args = ap.parse_args()

    pat = os.environ.get("CJ_PAT")
    cid = os.environ.get("CJ_CID", "7112482")
    website_id = os.environ.get("CJ_HEALTHNATION_WEBSITE_ID")
    if not pat:
        print("✗ CJ_PAT required (developers.cj.com → Authentication → Personal "
              "Access Token). CJ_CID defaults to 7112482.", file=sys.stderr)
        return 1

    if not website_id:
        print("• CJ_HEALTHNATION_WEBSITE_ID unset — skipping linkCode. Add "
              "healthnation.com as a website under the CID first; advertisers "
              "approve a property, not a company.\n", file=sys.stderr)

    print(f"Probing {len(args.lanes)} lanes against the open CJ catalog…", file=sys.stderr)
    advertisers = collect(pat, cid, args.lanes, args.limit, website_id, args.delay)
    joined = joined_advertisers(pat, cid)

    rows = []
    for rec in advertisers.values():
        rec["joined"] = rec["advertiserId"] in joined
        if args.joined and not rec["joined"]:
            continue
        if len(rec["lanes"]) < args.min_lanes:
            continue
        rows.append(rec)
    # Lane coverage first — an advertiser stocking four of HealthNation's six
    # mega-menus is worth more than one with more SKUs in a single lane.
    rows.sort(key=lambda r: (len(r["lanes"]), r["hits"]), reverse=True)

    serialisable = [
        {**r, "lanes": sorted(r["lanes"]), "probes": sorted(r["probes"])} for r in rows
    ]

    if args.json:
        print(json.dumps(
            {
                "companyId": cid,
                "websiteId": website_id,
                "lanesProbed": args.lanes,
                "joinedAdvertisers": joined,
                "prospects": serialisable,
            },
            indent=2,
        ))
        return 0

    print(f"\nCJ prospects for healthnation.com — CID {cid}")
    print(f"Lanes probed: {', '.join(args.lanes)}")
    print(f"Advertisers with catalog stock in ≥{args.min_lanes} lane(s): {len(rows)}\n")
    print(f"  {'':1} {'ADVERTISER':<34} {'ID':>8}  {'HITS':>5}  LANES")
    print(f"  {'-' * 78}")
    for r in rows:
        mark = "✓" if r["joined"] else " "
        print(f"  {mark} {r['advertiserName'][:34]:<34} {r['advertiserId']:>8}  "
              f"{r['hits']:>5}  {', '.join(sorted(r['lanes']))}")

    print("\n✓ = already joined on this CID (approval may still be property-scoped).")
    print("\nNot visible here: lead-gen/CPA programs with no product feed — lab")
    print("testing, telehealth, therapy, meal delivery, insurance. Browse those")
    print("in members.cj.com → Advertisers → Health & Wellness.")

    if rows and not website_id:
        print("\nNext gate: add healthnation.com as a website under the CID, then")
        print("re-run with CJ_HEALTHNATION_WEBSITE_ID set to confirm link minting.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
