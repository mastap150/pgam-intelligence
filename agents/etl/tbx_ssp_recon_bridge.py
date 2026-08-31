"""
agents/etl/tbx_ssp_recon_bridge.py

Bridges `pgam_direct.tbx_daily_demand_revenue` (populated hourly by
`agents/etl/tbx_revenue_etl` from api.pgammedia.com) into the per-partner
`finance.ssp_recon_daily` table that drives `/admin/finance`.

Why this exists rather than a query at read time
-------------------------------------------------
Before TBX cutover, `pgam_recon/fetchers/teqblaze.py` hit
`ssp.pgammedia.com/api/{token}/adx-report` and wrote per-partner rows
to `finance.ssp_recon_daily` (source='pgam-authoritative'). That
legacy endpoint has returned zero rows since ~2026-08-01; every TB-only
demand partner (Stirista, Unruly OTTA, Synatix, etc.) sat at $0 on
`/admin/finance` while their TBX numbers were fine.

TBX gross totals flow into `finance.daily_pnl_inputs.tb_gross_usd` via
a separate path (see `scripts/tbx_pnl_check.py` for the read-only
audit that formalized the mapping), so the P&L Total Gross line has
been correct. But per-partner rows never got backfilled. This closes
that gap.

Aggregation shape
-----------------
TBX's `tbx_daily_demand_revenue` grain is one row per (report_date,
demand_id), where a partner like Stirista has 11 sub-tags (Verve Display
Banner Top 200 Properties, Illumin Display Banner, In App, etc.). Every
sub-tag rolls up into one canonical `partner_key` — the same key
pgam-recon's config.yaml uses — via prefix matching on `demand_name`.

The mapping is intentionally explicit rather than fuzzy: a new sub-tag
with an unusual name shows up in the unmapped log rather than silently
being credited to the wrong parent. Coverage across the last 14 days
of August 2026 was 99%+ (only "Adagio - CMG" needed adding on first
audit; now included).

Upsert semantics — GREATEST on pgam_ssp_dash
--------------------------------------------
Uses the same non-regression rules as `pgam_recon.neon.upsert_recon_rows`:

- `pgam_ssp_dash = GREATEST(existing, EXCLUDED)` — TBX pgam-side data
  can only lift the number up, never down. Prevents mid-ingest partial
  captures from clobbering fully-settled prior writes.
- `ssp_dash_net` — only overwritten when the current source is
  `mirror-fallback` or `pgam-authoritative` (i.e. we've never had SSP-
  side truth for this row). If a real SSP fetcher (Magnite email, Zeta
  API, LoopMe API, etc.) has already written a value we trust that
  more.
- `source` — promoted from `mirror-fallback` / `pgam-authoritative`
  to `tbx-etl` when we're the first authoritative writer. Otherwise
  left alone.
- `is_final=true` rows are never touched (mirrors upsert_recon_rows).

Read-only against TBX (via the warehouse table only, no live API call).
Writes to `finance.ssp_recon_daily` only, never to `daily_pnl_inputs`
— the P&L already has its own bridge.

Schedule (scheduler.py): every 60 minutes, right after tbx_revenue_etl.
Backfill: `python -m agents.etl.tbx_ssp_recon_bridge --backfill 30`
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_LOG = "[tbx_ssp_recon_bridge]"

# Import path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


# Mapping rules — first prefix match wins. Kept as an ordered list of
# (SQL fragment inserted into WHEN clause, canonical partner_key) pairs
# so a new partner is a one-line addition rather than a schema change.
# Compiled into a big CASE in _mapping_sql() below.
_RULES: list[tuple[str, str]] = [
    ("demand_name ILIKE 'stirista%'",                     "stirista"),
    ("demand_name ILIKE 'zmaticoo%'",                     "zmaticoo"),
    ("demand_name ILIKE 'magnite%'",                      "magnite"),
    ("demand_name ILIKE 'otta unruly%'",                  "unruly_otta"),
    ("demand_name ILIKE 'otta olv%'",                     "unruly_otta"),
    ("demand_name ILIKE 'synatix%'",                      "syntix"),
    ("demand_name ILIKE 'zeta%'",                         "zeta"),
    ("demand_name ILIKE 'verve%'",                        "verve"),
    ("demand_name ILIKE 'loopme%'",                       "loopme"),
    ("demand_name ILIKE 'illumin%'",                      "illumin"),
    ("demand_name ILIKE 'adnimation%'",                   "adnimation"),
    ("demand_name ILIKE 'bidmachine%'",                   "bidmachine"),
    ("demand_name ILIKE 'triplelift%'",                   "triplelift_blitz"),
    ("demand_name ILIKE 'xandr%'",                        "xandr_blitz"),
    ("demand_name ILIKE 'sharethrough%'",                 "sharethrough"),
    ("demand_name ILIKE 'adaptmx%' OR demand_name ILIKE 'amx%'",         "adaptmx"),
    ("demand_name ILIKE 'onetag%' OR demand_name ILIKE 'one tag%'",      "onetag"),
    ("demand_name ILIKE '%across%'",                      "across33"),
    ("demand_name ILIKE 'sovrn%'",                        "sovrn"),
    ("demand_name ILIKE 'pubmatic%'",                     "pubmatic"),
    ("demand_name ILIKE 'basis%'",                        "basis"),
    ("demand_name ILIKE 'trubid%'",                       "trubid"),
    ("demand_name ILIKE 'smilewanted%' OR demand_name ILIKE 'smiles wanted%'", "smilewanted"),
    ("demand_name ILIKE 'unruly%'",                       "unruly"),
    ("demand_name ILIKE 'perion%'",                       "perion"),
    ("demand_name ILIKE 'kueez%'",                        "kueez"),
    ("demand_name ILIKE 'epsilon%'",                      "epsilon"),
    ("demand_name ILIKE 'openweb%' OR demand_name ILIKE 'open web%'",    "openweb"),
    ("demand_name ILIKE 'sabio%'",                        "sabio"),
    ("demand_name ILIKE '9dots%' OR demand_name ILIKE 'ninedots%'",      "ninedots"),
    ("demand_name ILIKE 'cas.ai%' OR demand_name ILIKE 'casai%'",        "casai_supply"),
    ("demand_name ILIKE 'adprime%'",                      "adprime"),
    ("demand_name ILIKE 'adelement%'",                    "adelement"),
    ("demand_name ILIKE 'mobupps%'",                      "mobupps"),
    ("demand_name ILIKE 'growintech%'",                   "growintech"),
    ("demand_name ILIKE 'startio%' OR demand_name ILIKE 'start.io%'",    "startio"),
    ("demand_name ILIKE 'blasto%'",                       "blasto"),
    ("demand_name ILIKE 'lumeriq%'",                      "lumeriq"),
    ("demand_name ILIKE 'pubfusion%'",                    "pubfusion"),
    ("demand_name ILIKE 'adgrid%'",                       "adgrid"),
    ("demand_name ILIKE 'criteo%'",                       "criteo"),
    ("demand_name ILIKE 'axis%'",                         "axis"),
    ("demand_name ILIKE 'oveeo%'",                        "oveeo"),
    ("demand_name ILIKE 'performist%'",                   "performist"),
    ("demand_name ILIKE 'adform%'",                       "adform"),
    ("demand_name ILIKE 'exte%'",                         "exte"),
    ("demand_name ILIKE 'adagio%'",                       "adagio_cmg"),
    ("demand_name ILIKE 'pmp%'",                          "pmps"),
]

DISPLAY_NAMES = {
    "stirista": "Stirista", "zmaticoo": "Zmaticoo", "magnite": "Magnite",
    "unruly_otta": "Unruly OTTA", "syntix": "Synatix", "zeta": "Zeta",
    "verve": "Verve", "loopme": "LoopMe", "illumin": "Illumin",
    "adnimation": "Adnimation", "bidmachine": "BidMachine",
    "triplelift_blitz": "TripleLift-Blitz", "xandr_blitz": "Xandr-Blitz",
    "sharethrough": "Sharethrough", "adaptmx": "AdaptMX", "onetag": "OneTag",
    "across33": "33Across", "sovrn": "Sovrn", "pubmatic": "Pubmatic",
    "basis": "Basis", "trubid": "TruBid", "smilewanted": "SmileWanted",
    "unruly": "Unruly", "perion": "Perion", "kueez": "Kueez",
    "epsilon": "Epsilon", "openweb": "OpenWeb", "sabio": "Sabio",
    "ninedots": "9dots", "casai_supply": "cas.ai (supply)",
    "adprime": "AdPrime", "adelement": "AdElement", "mobupps": "Mobupps",
    "growintech": "GrowinTech", "startio": "Start.IO", "blasto": "Blasto",
    "lumeriq": "LumerIQ", "pubfusion": "PubFusion", "adgrid": "AdGrid",
    "criteo": "Criteo", "axis": "Axis", "oveeo": "Oveeo",
    "performist": "Performist", "adform": "Adform", "exte": "EXTE",
    "adagio_cmg": "Adagio", "pmps": "PMPs",
}


def _mapping_sql() -> str:
    """Compile the ordered rule list into a SQL CASE expression."""
    whens = "\n      ".join(f"WHEN {expr} THEN '{key}'" for expr, key in _RULES)
    return f"CASE\n      {whens}\n      ELSE NULL\n    END"


def _finance_url() -> str:
    url = os.environ.get("FINANCE_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("FINANCE_DATABASE_URL / DATABASE_URL not set")
    return url


def _pgam_direct_url() -> str:
    url = os.environ.get("PGAM_DIRECT_DATABASE_URL")
    if not url:
        raise RuntimeError("PGAM_DIRECT_DATABASE_URL not set")
    return url


def run(*, days: int = 3) -> dict:
    """Run the bridge for the trailing N days (default 3 — matches how
    often TBX corrects itself).

    Returns counters + coverage stats for logging / assertions.
    """
    import psycopg

    mapping = _mapping_sql()

    with psycopg.connect(_pgam_direct_url()) as pd, pd.cursor() as cur:
        cur.execute(f"""
            WITH mapped AS (
              SELECT report_date::date AS d,
                     {mapping} AS pk,
                     gross_revenue,
                     pub_payout,
                     demand_name
                FROM pgam_direct.tbx_daily_demand_revenue
               WHERE report_date BETWEEN CURRENT_DATE - INTERVAL '{days} days'
                                     AND CURRENT_DATE
            )
            SELECT d, pk,
                   SUM(gross_revenue)::numeric(14,4) AS gross,
                   SUM(pub_payout)::numeric(14,4)    AS payout,
                   COUNT(*)                           AS subtag_count
              FROM mapped
             WHERE pk IS NOT NULL
             GROUP BY d, pk
             ORDER BY d DESC, gross DESC
        """)
        aggregates = cur.fetchall()

        # Also collect unmapped for observability
        cur.execute(f"""
            WITH mapped AS (
              SELECT demand_name, gross_revenue,
                     {mapping} AS pk
                FROM pgam_direct.tbx_daily_demand_revenue
               WHERE report_date BETWEEN CURRENT_DATE - INTERVAL '{days} days'
                                     AND CURRENT_DATE
            )
            SELECT demand_name, SUM(gross_revenue)::numeric(10,2) AS gross
              FROM mapped WHERE pk IS NULL AND gross_revenue > 1
             GROUP BY demand_name ORDER BY gross DESC LIMIT 10
        """)
        unmapped = cur.fetchall()

    n_upserts = 0
    with psycopg.connect(_finance_url()) as fin, fin.cursor() as cur:
        for (d, pk, gross, payout, _subtag_count) in aggregates:
            disp = DISPLAY_NAMES.get(pk, pk.replace("_", " ").title())
            cur.execute("""
                INSERT INTO finance.ssp_recon_daily
                  (target_date, partner_key, partner_sheet_name,
                   ssp_dash_net, pgam_ssp_dash, source, written_at, status)
                VALUES (%s, %s, %s, %s, %s, 'tbx-etl', now(), 'provisional')
                ON CONFLICT (target_date, partner_key) DO UPDATE
                   SET pgam_ssp_dash = GREATEST(
                         finance.ssp_recon_daily.pgam_ssp_dash,
                         EXCLUDED.pgam_ssp_dash
                       ),
                       ssp_dash_net = CASE
                         WHEN finance.ssp_recon_daily.source IN ('mirror-fallback', 'pgam-authoritative')
                              AND EXCLUDED.pgam_ssp_dash > finance.ssp_recon_daily.ssp_dash_net
                         THEN EXCLUDED.pgam_ssp_dash
                         ELSE finance.ssp_recon_daily.ssp_dash_net
                       END,
                       source = CASE
                         WHEN finance.ssp_recon_daily.source IN ('mirror-fallback', 'pgam-authoritative')
                         THEN 'tbx-etl'
                         ELSE finance.ssp_recon_daily.source
                       END,
                       written_at = now(),
                       last_resync_at = now()
                 WHERE finance.ssp_recon_daily.is_final = false
            """, (d, pk, disp, gross, gross))
            n_upserts += 1
        fin.commit()

    print(f"{_LOG} bridged {n_upserts} (date, partner) rows across {days}d window")
    if unmapped:
        print(f"{_LOG} unmapped demand_names (gross > $1 last {days}d):")
        for name, gross in unmapped:
            print(f"{_LOG}   ${gross:>7,.2f}  {name}")

    return {"upserts": n_upserts, "unmapped": len(unmapped)}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--backfill", type=int, default=3,
                   help="Days to look back (default 3; use 30+ for one-shot backfill)")
    args = p.parse_args()

    # Load .env from repo root if present (dev mode; prod uses Render env)
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    except Exception:
        pass

    result = run(days=args.backfill)
    return 0 if result["upserts"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
