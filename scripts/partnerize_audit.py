#!/usr/bin/env python3
"""Audit boxingnews.com's DAZN affiliate state in Partnerize.

WHY THIS EXISTS
---------------
On 2026-08-06, boxingnews commit fb59da1 ("retire Partnerize DAZN wrap")
tore out the `prf.hn` tracking-link builder and flagged the DAZN operator
`active: false`, on the stated premise that "Partnerize dropped
boxingnews.com's DAZN affiliate in 2026-08".

No evidence for that premise has been found: there is no termination
notice from Partnerize or DAZN in ppatel@pgammedia.com, and on
2026-08-21 Partnerize sent a "Funds are available to withdraw" alert for
the account. The DAZN Global Partners participation (camref 1101l3MQmm,
granted by DAZN 2024-06-05) appears to still be live.

This script answers, from the API rather than from a code comment:

  1. Is the DAZN participation still approved?         --participations
  2. Which camrefs does the account actually hold?     --camrefs
  3. What has converted, and WHEN was the click?       --conversions
  4. How many clicks did we actually send?             --clicks
  5. What is sitting unwithdrawn?                      --balance
  6. What else could we join (a sportsbook?)           --discover

(4) is the decisive one now. The console's aggregate export for
2026-01-01..2026-08-21 shows ONE conversion (2026-06-28, commission
14.65946) in 233 days. One conversion in eight months is either thin
placement or lost attribution, and those need different fixes. Compare
`--clicks` against the boxingnews ledger over the same window:

    SELECT count(*) FROM affiliate_clicks
     WHERE operator_id = 'dazn'
       AND clicked_at >= '2026-01-01' AND clicked_at < '2026-08-06';

Big gap -> attribution loss. Both small -> placement problem, not code.

(3) is the one that settles the revenue question. Every conversion row
carries both `conversion_time` and the originating `click.set_time`. A
click dated on/after 2026-08-06 could only have been attributed if the
`prf.hn` tunnel was still receiving traffic after the teardown — i.e. via
a tracked URL still set in the boxingnews Vercel env (`AFFILIATE_DAZN_URL`),
since the code itself stopped composing one. Clicks dated before the
teardown are pre-existing pipeline converting on DAZN's own lag. The
script labels each row accordingly.

AUTH
----
Partnerize uses HTTP Basic: base64("<user application key>:<user api key>").
Both are in the Partnerize console under Settings -> Account settings
(https://console.partnerize.com). Export them before running:

    export PARTNERIZE_APP_KEY=...
    export PARTNERIZE_API_KEY=...
    export PARTNERIZE_PUBLISHER_ID=...     # optional, see --publisher

Credentials are read from the environment or a local .env; they are never
printed. Every call this script makes is a read (GET) — nothing here
mutates campaign state, links, or payments.

USAGE
-----
    # everything, in one pass (recommended first run)
    python3 scripts/partnerize_audit.py --all

    # THE diagnostic: clicks vs conversions, scoped to before the teardown
    # (after 2026-08-06 the site stopped sending prf.hn traffic on purpose,
    #  so including those days makes Partnerize's click count look broken)
    python3 scripts/partnerize_audit.py --clicks --conversions \
        --start 2026-01-01 --end 2026-08-05

    # is the program actually still alive?
    python3 scripts/partnerize_audit.py --participations

    # what converted in the last 90 days, and when were the clicks?
    python3 scripts/partnerize_audit.py --conversions --days 90

    # include pending/rejected, not just approved
    python3 scripts/partnerize_audit.py --conversions --statuses approved,pending,rejected

    # what programs could we join? is there a sportsbook on Partnerize?
    python3 scripts/partnerize_audit.py --discover
    python3 scripts/partnerize_audit.py --discover --discover-keyword bet

    # find the publisher id if you don't know it
    python3 scripts/partnerize_audit.py --whoami
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

API = "https://api.partnerize.com"

# The camref DAZN issued to boxingnews.com on 2024-06-05 (Will
# Harbord-Hamond, DAZN affiliates). Used only to flag whether the
# account still holds it — never sent as a credential.
BOXINGNEWS_DAZN_CAMREF = "1101l3MQmm"

# The commit that removed the prf.hn wrap from boxingnews. Clicks dated
# on/after this are the interesting ones — see module docstring.
TEARDOWN_DATE = date(2026, 8, 6)

TIMEOUT = 30


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def auth_header() -> str:
    """Build the Basic auth header from the two console keys."""
    app_key = os.environ.get("PARTNERIZE_APP_KEY", "").strip()
    api_key = os.environ.get("PARTNERIZE_API_KEY", "").strip()
    if not app_key or not api_key:
        die(
            "set PARTNERIZE_APP_KEY and PARTNERIZE_API_KEY (Partnerize console "
            "-> Settings -> Account settings: 'User Application Key' and "
            "'User API Key')."
        )
    token = base64.b64encode(f"{app_key}:{api_key}".encode()).decode()
    return f"Basic {token}"


def get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET a Partnerize path, returning parsed JSON.

    Partnerize signals auth/permission problems with 401/403 and a JSON
    body; surface those verbatim rather than a bare status code, because
    the distinction matters here (403 on one endpoint is a permission
    quirk of the publisher user, not proof the program is gone — that
    exact confusion is what this script exists to resolve).
    """
    url = f"{API}{path}"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": auth_header(), "Accept": "application/json"},
            params=params,
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        die(f"request to {path} failed: {exc}")

    if resp.status_code in (401, 403):
        die(
            f"{resp.status_code} on {path}\n"
            f"  body: {resp.text[:400]}\n"
            "  A 403 here usually means the publisher user lacks a permission on "
            "that endpoint (a known quirk of this account) — it does NOT by "
            "itself mean the affiliate program ended. Check --participations."
        )
    if resp.status_code >= 400:
        die(f"HTTP {resp.status_code} on {path}: {resp.text[:400]}")

    try:
        return resp.json()
    except ValueError:
        die(f"non-JSON response from {path}: {resp.text[:200]}")


def resolve_publisher_id(explicit: str | None) -> str:
    """Publisher id from the flag, env, or the participations endpoint."""
    if explicit:
        return explicit
    from_env = os.environ.get("PARTNERIZE_PUBLISHER_ID", "").strip()
    if from_env:
        return from_env
    die(
        "publisher id unknown. Pass --publisher <id>, set "
        "PARTNERIZE_PUBLISHER_ID, or run --whoami to look it up."
    )
    raise AssertionError("unreachable")


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------


def cmd_whoami() -> None:
    """Print what the credentials can see, to locate the publisher id."""
    print("== networks visible to this user ==")
    data = get("/network")
    for entry in data.get("networks", []):
        net = entry.get("network", entry)
        print(f"  network_id={net.get('network_id')}  name={net.get('network_name')}")

    print("\n== brands / campaigns this partner is on ==")
    brands = get("/v3/partner/my-brands")
    payload = brands.get("data", brands) if isinstance(brands, dict) else brands
    print(json.dumps(payload, indent=2)[:3000])
    print(
        "\nThe partner/publisher id is the `1101l...`-shaped value in the above "
        "(or in the console URL after /publisher/). Export it as "
        "PARTNERIZE_PUBLISHER_ID."
    )


def cmd_participations(publisher_id: str) -> None:
    """The direct answer to 'did Partnerize drop us?'"""
    print("== campaign participations ==")

    # v1: approved / pending / rejected are separate calls, keyed a/p/r.
    for code, label in (("a", "approved"), ("p", "pending"), ("r", "rejected")):
        data = get(f"/user/publisher/{publisher_id}/campaign/{code}")
        campaigns = data.get("campaigns", []) if isinstance(data, dict) else []
        print(f"\n  -- {label} ({len(campaigns)}) --")
        for entry in campaigns:
            camp = entry.get("campaign", entry)
            title = camp.get("campaign_title") or camp.get("title") or "?"
            cid = camp.get("campaign_id") or "?"
            flag = "  <-- DAZN" if "dazn" in str(title).lower() else ""
            print(f"     {cid:16} {title}{flag}")

    # v3 gives participation status in one shot, including any that have
    # been suspended/ended by the advertiser — which is precisely the
    # state the 2026-08-06 commit assumed without checking.
    print("\n  -- v3 participations (authoritative on suspend/end) --")
    try:
        v3 = get(f"/v3/partner/{publisher_id}/participations")
        rows = v3.get("data", v3) if isinstance(v3, dict) else v3
        print(json.dumps(rows, indent=2)[:4000])
    except SystemExit:
        print("     (v3 participations unavailable for this user — rely on v1 above)")


def cmd_discover(publisher_id: str, keyword: str | None) -> None:
    """What else could we join? Answers "is there a sportsbook on here?".

    `discovery/advertisers` returns every campaign available to this
    partner, grouped by brand, each carrying a per-partner status:
    AVAILABLE / REQUESTED / INVITED / REJECTED. AVAILABLE and INVITED are
    the actionable ones.

    Pass a keyword (e.g. --discover-keyword bet) to filter to a vertical.
    Joining is a WRITE (`POST /v2/publishers/{id}/campaign-requests`) and
    this script deliberately does not do it — applying to a program is a
    commercial decision, not a side effect of an audit.
    """
    print("== brands & campaigns available to join ==")
    data = get(f"/v2/publishers/{publisher_id}/discovery/advertisers")

    # The v2 payload nests campaigns under brands; key naming has varied
    # across revisions, so probe the plausible containers rather than
    # assuming one and silently rendering nothing.
    brands = None
    if isinstance(data, dict):
        for key in ("advertisers", "brands", "data"):
            if isinstance(data.get(key), list):
                brands = data[key]
                break
    if brands is None:
        print(
            "  Could not locate a brand list in the response. Raw payload below —\n"
            "  if the shape has changed, the keys to look for are the campaign\n"
            "  `status` values (AVAILABLE / REQUESTED / INVITED / REJECTED).\n"
        )
        print(json.dumps(data, indent=2)[:4000])
        return

    needle = (keyword or "").strip().lower()
    shown = 0
    status_tally: dict[str, int] = {}

    for entry in brands:
        brand = entry.get("advertiser", entry.get("brand", entry)) or {}
        name = str(brand.get("name") or brand.get("title") or brand.get("advertiser_name") or "?")
        campaigns = brand.get("campaigns") or entry.get("campaigns") or []

        rows = []
        for camp in campaigns:
            c = camp.get("campaign", camp) or {}
            status = str(c.get("status") or "?").upper()
            status_tally[status] = status_tally.get(status, 0) + 1
            title = str(c.get("title") or c.get("name") or "?")
            cid = c.get("id") or c.get("campaign_id") or "?"
            region = c.get("region") or c.get("country") or ""
            rows.append((status, cid, title, region))

        haystack = (name + " " + " ".join(r[2] for r in rows)).lower()
        if needle and needle not in haystack:
            continue

        shown += 1
        print(f"\n  {name}")
        for status, cid, title, region in rows:
            marker = "  <<" if status in ("AVAILABLE", "INVITED") else ""
            print(f"     [{status:<9}] {str(cid):16} {title} {region}{marker}")

    print(f"\n  brands listed: {shown} of {len(brands)}"
          + (f" (filtered on {keyword!r})" if needle else ""))
    if status_tally:
        print("  campaign statuses across all brands: "
              + ", ".join(f"{k}={v}" for k, v in sorted(status_tally.items())))
    print(
        "\n  AVAILABLE / INVITED (marked <<) are the ones you can act on. To join,\n"
        "  use the console, or POST /v2/publishers/<id>/campaign-requests — this\n"
        "  script won't apply on your behalf."
    )


def cmd_camrefs(publisher_id: str) -> None:
    """Confirm the account still holds boxingnews's DAZN camref."""
    print("== campaign references (camrefs) held by this partner ==")
    data = get(f"/reference/publisher/camref/{publisher_id}")
    blob = json.dumps(data)
    print(json.dumps(data, indent=2)[:3000])
    if BOXINGNEWS_DAZN_CAMREF in blob:
        print(
            f"\n  OK: camref {BOXINGNEWS_DAZN_CAMREF} (boxingnews.com / DAZN) is "
            "still present on the account."
        )
    else:
        print(
            f"\n  NOT FOUND: camref {BOXINGNEWS_DAZN_CAMREF} did not appear in the "
            "response. That WOULD be consistent with the program having ended — "
            "confirm against --participations before concluding."
        )


def cmd_balance(publisher_id: str) -> None:
    """What is sitting unwithdrawn (matches the console's payout alert)."""
    print("== available commission ==")
    print(json.dumps(get(f"/user/publisher/{publisher_id}/available_commission"), indent=2)[:2000])

    print("\n== payment summary ==")
    print(json.dumps(get(f"/user/publisher/{publisher_id}/payment/summary"), indent=2)[:2000])


def _click_verdict(click_time: str | None) -> str:
    """Label a conversion by whether its click predates the teardown."""
    if not click_time:
        return "click date unknown"
    try:
        clicked = datetime.strptime(click_time[:10], "%Y-%m-%d").date()
    except ValueError:
        return "click date unparseable"
    if clicked >= TEARDOWN_DATE:
        return (
            f"CLICK {clicked} IS ON/AFTER THE {TEARDOWN_DATE} TEARDOWN "
            "-> a tracked URL is still live in Vercel env"
        )
    return f"click {clicked} predates the {TEARDOWN_DATE} teardown (pipeline lag)"


def resolve_window(start: str | None, end: str | None, days: int) -> tuple[date, date]:
    """Report window from explicit --start/--end, else a --days lookback.

    The click-vs-conversion comparison wants an explicit window ending
    2026-08-05: after the teardown the site stopped sending traffic into
    the prf.hn tunnel on purpose, so including post-teardown days makes
    Partnerize's click count look broken when it is merely correct.
    """
    try:
        end_d = date.fromisoformat(end) if end else date.today()
        start_d = date.fromisoformat(start) if start else end_d - timedelta(days=days)
    except ValueError as exc:
        die(f"bad --start/--end (expected YYYY-MM-DD): {exc}")
    if start_d > end_d:
        die(f"--start {start_d} is after --end {end_d}")
    return start_d, end_d


# Partnerize pages the reporting endpoints at 300 rows. Cap total pages so a
# high-volume window can't spin forever; the headline unique_click_count is
# returned on page 1 regardless, so a cap degrades detail, never the answer.
PAGE_SIZE = 300
MAX_PAGES = 40


def cmd_clicks(publisher_id: str, start: date, end: date) -> None:
    """Click volume — the number that separates 'thin placement' from
    'lost attribution'.

    One conversion in eight months means either we barely send DAZN
    clicks, or we send plenty and they aren't landing on our camref.
    Compare `unique_click_count` here against the boxingnews ledger:

        SELECT count(*) FROM affiliate_clicks
         WHERE operator_id = 'dazn'
           AND clicked_at >= <start> AND clicked_at < <end>;

    A large gap means attribution loss. Both small means the placements,
    not the code, are the problem.
    """
    print(f"== clicks {start} .. {end} ==")
    first = get(
        f"/reporting/report_publisher/publisher/{publisher_id}/click.json",
        params={"start_date": start.isoformat(), "end_date": end.isoformat()},
    )

    unique = first.get("unique_click_count")
    print(f"  unique_click_count: {unique}")
    print(f"  rows on page 1:     {first.get('count')} (page size {first.get('limit')})")

    # Walk the pages to bucket by month and by referring surface. This is
    # detail on top of the headline count above, so a truncated walk is
    # flagged rather than silently treated as complete.
    rows: list[dict[str, Any]] = list(first.get("clicks", []) or [])
    pages = 1
    truncated = False
    while len(rows) >= PAGE_SIZE * pages:
        if pages >= MAX_PAGES:
            truncated = True
            break
        page = get(
            f"/reporting/report_publisher/publisher/{publisher_id}/click.json",
            params={
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "offset": str(PAGE_SIZE * pages),
            },
        )
        batch = page.get("clicks", []) or []
        if not batch:
            break
        rows.extend(batch)
        pages += 1

    print(f"  rows walked:        {len(rows)} across {pages} page(s)")
    if truncated:
        print(
            f"  NOTE: stopped at the {MAX_PAGES}-page cap — the breakdowns below "
            "cover only the rows walked. unique_click_count above is still the "
            "full-window total."
        )

    if not rows:
        print("\n  no click rows returned for this window.")
        return

    by_month: dict[str, int] = {}
    by_referer: dict[str, int] = {}
    pre = post = unknown = 0
    for entry in rows:
        click = entry.get("click", entry)
        set_time = click.get("set_time") or ""
        month = set_time[:7] or "unknown"
        by_month[month] = by_month.get(month, 0) + 1

        ref = (click.get("referer") or "(none)")[:70]
        by_referer[ref] = by_referer.get(ref, 0) + 1

        try:
            clicked = datetime.strptime(set_time[:10], "%Y-%m-%d").date()
        except ValueError:
            unknown += 1
            continue
        if clicked >= TEARDOWN_DATE:
            post += 1
        else:
            pre += 1

    print("\n  by month:")
    for month in sorted(by_month):
        print(f"    {month}  {by_month[month]:>6}")

    print("\n  top referring surfaces:")
    for ref, n in sorted(by_referer.items(), key=lambda kv: -kv[1])[:12]:
        print(f"    {n:>6}  {ref}")

    print(
        f"\n  before {TEARDOWN_DATE}: {pre}   on/after: {post}"
        + (f"   undated: {unknown}" if unknown else "")
    )
    if post:
        print(
            "  >> Clicks recorded by Partnerize ON/AFTER the teardown means the "
            "prf.hn tunnel is still receiving traffic — i.e. a tracked "
            "AFFILIATE_DAZN_URL is still set in Vercel."
        )


def cmd_conversions(publisher_id: str, start: date, end: date, statuses: str) -> None:
    """The revenue question: what converted, and when was the click?"""
    params: dict[str, Any] = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    for status in [s.strip() for s in statuses.split(",") if s.strip()]:
        params.setdefault("statuses[]", []).append(status)

    print(f"== conversions {start} .. {end}  (statuses: {statuses}) ==")
    data = get(
        f"/reporting/report_publisher/publisher/{publisher_id}/conversion.json",
        params=params,
    )

    print(f"  total_conversion_count:     {data.get('total_conversion_count')}")
    print(f"  total_value:                {data.get('total_value')}")
    print(f"  total_publisher_commission: {data.get('total_publisher_commission')}")

    conversions = data.get("conversions", []) or []
    if not conversions:
        print("\n  no conversions in this window.")
        return

    print(f"\n  {len(conversions)} conversion(s):")
    for entry in conversions:
        conv = entry.get("conversion_data", entry)
        click = conv.get("click", {}) or {}
        value = conv.get("conversion_value", {}) or {}

        # `ar:` adrefs land in advertiser_reference; `pubref:` in
        # publisher_reference. boxingnews used `ar:` (per DAZN's own
        # 2024-06-24 guidance), so the adref identifying WHICH surface
        # drove the sale is advertiser_reference.
        adref = conv.get("advertiser_reference") or "(none)"
        pubref = conv.get("publisher_reference") or "(none)"

        print("\n  " + "-" * 66)
        print(f"    conversion_id:   {conv.get('conversion_id')}")
        print(f"    campaign:        {conv.get('campaign_title')} ({conv.get('campaign_id')})")
        print(f"    conversion_time: {conv.get('conversion_time')}")
        print(f"    click set_time:  {click.get('set_time')}")
        print(f"    adref (ar:):     {adref}")
        print(f"    pubref:          {pubref}")
        print(f"    referer:         {click.get('referer') or '(none)'}")
        print(f"    metric/type:     {conv.get('ref_conversion_metric')}")
        print(f"    country/device:  {conv.get('country')} / {conv.get('ref_device')}")
        print(
            f"    value:           {value.get('value')} {conv.get('currency')}"
            f"   commission: {value.get('publisher_commission')}"
            f"   status: {value.get('conversion_status')}"
        )
        print(f"    >> {_click_verdict(click.get('set_time'))}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Audit boxingnews.com's DAZN affiliate state in Partnerize.",
    )
    ap.add_argument("--publisher", help="Partnerize publisher/partner id")
    ap.add_argument("--whoami", action="store_true", help="locate the publisher id")
    ap.add_argument("--participations", action="store_true", help="is DAZN still approved?")
    ap.add_argument("--camrefs", action="store_true", help="camrefs held by the account")
    ap.add_argument("--discover", action="store_true", help="brands/campaigns available to join")
    ap.add_argument(
        "--discover-keyword",
        help="filter --discover to a vertical, e.g. bet / sportsbook / casino",
    )
    ap.add_argument("--conversions", action="store_true", help="conversions + click dates")
    ap.add_argument("--clicks", action="store_true", help="click volume (the key diagnostic)")
    ap.add_argument("--balance", action="store_true", help="unwithdrawn commission")
    ap.add_argument("--all", action="store_true", help="every check")
    ap.add_argument("--days", type=int, default=90, help="lookback in days (default 90)")
    ap.add_argument("--start", help="window start, YYYY-MM-DD (overrides --days)")
    ap.add_argument("--end", help="window end, YYYY-MM-DD (default today)")
    ap.add_argument(
        "--statuses",
        default="approved,pending",
        help="conversion statuses to include (default approved,pending)",
    )
    args = ap.parse_args()

    if args.whoami:
        cmd_whoami()
        return

    ran_any = args.all or any(
        (
            args.participations,
            args.camrefs,
            args.discover,
            args.conversions,
            args.clicks,
            args.balance,
        )
    )
    if not ran_any:
        ap.print_help()
        print(
            "\nNothing selected. Start with:\n"
            "  python3 scripts/partnerize_audit.py --all\n"
            "\nFor the click-vs-conversion diagnostic, scope to before the teardown:\n"
            "  python3 scripts/partnerize_audit.py --clicks --conversions \\\n"
            "      --start 2026-01-01 --end 2026-08-05"
        )
        return

    publisher_id = resolve_publisher_id(args.publisher)
    start, end = resolve_window(args.start, args.end, args.days)
    print(f"partner/publisher id: {publisher_id}")
    print(f"window: {start} .. {end}\n")

    if args.all or args.participations:
        cmd_participations(publisher_id)
        print()
    if args.all or args.camrefs:
        cmd_camrefs(publisher_id)
        print()
    if args.all or args.discover:
        cmd_discover(publisher_id, args.discover_keyword)
        print()
    if args.all or args.clicks:
        cmd_clicks(publisher_id, start, end)
        print()
    if args.all or args.conversions:
        cmd_conversions(publisher_id, start, end, args.statuses)
        print()
    if args.all or args.balance:
        cmd_balance(publisher_id)


if __name__ == "__main__":
    main()
