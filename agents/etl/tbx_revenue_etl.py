"""
agents/etl/tbx_revenue_etl.py

Lands TBX (new Teqblaze platform, api.pgammedia.com) daily revenue into Neon,
so PGAM reporting can read the new platform the same way it already reads the
legacy one.

Why this exists rather than an ad-hoc script
--------------------------------------------
Reporting needs the data on a schedule, in the warehouse, without a person or
a session in the loop. An interactive pull answers "what is today doing" once;
this answers it every hour, and it is the only shape that lets a dashboard
calculate revenue from the new platform at all.

Destination tables (created by migrations/2026_08_21_tbx_daily_revenue.sql):

  pgam_direct.tbx_daily_supply_revenue     (PK report_date, supply_id)
  pgam_direct.tbx_daily_demand_revenue     (PK report_date, demand_id)
  pgam_direct.tbx_daily_placement_revenue  (PK report_date, placement_id)

Separate from `tb_daily_*` on purpose. Both platforms serve the same
marketplace, so unioning them double counts every impression while both legs
run — and both run until the reconciliation passes
(docs/teqblaze-new-platform.md §7 tranche 1 step 2). Merging first would
destroy the only independent check we have.

Field mapping, settled against a live dashboard on 2026-08-21:

    dsp_price_sum  ("Demand Spend")   -> gross_revenue
    ssp_price_sum  ("Supply Revenue") -> pub_payout

Placement grain is here because Teqblaze confirmed placement ids are unchanged
across the two hosts while publisher / demand ids were not covered either way
(§8.1.10b, §8.1.10d) — so it is the one grain where a cross-platform join
rests on a commitment rather than a guess.

Safe to schedule before credentials exist: with TBX_EMAIL / TBX_PASSWORD
absent this no-ops and says so, rather than raising every hour. That is
deliberate — wiring it now means the day the credentials land in Render, data
starts flowing with no further deploy.

READ-only against the platform. It writes to our own warehouse and never
calls a TBX write endpoint; `core.tbx_mgmt` is not imported.

Schedule: every 60 minutes (scheduler.py). UPSERTs a trailing window so late
-arriving platform data is corrected rather than frozen at first read.
Backfill: `python -m agents.etl.tbx_revenue_etl --backfill 30`
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

_LOG = "[tbx_revenue_etl]"

# Trailing days re-read on every run. Matches tb_revenue_etl's 14 so the two
# platforms' tables cover the same span — a reconciliation over a window one
# side has not backfilled is a comparison of coverage, not of data.
WINDOW_DAYS = 14

# One request per grain PER DAY. The report endpoint accepts a date range and
# answers 200 for any span you ask for, but it silently returns only the most
# recent ~5 days of it — no error, no flag, no short-window marker (reference
# A5.1). A 14-day pull therefore lands 5 days and reports success, and the
# nine missing days look like nine days of zero revenue.
#
# So the range parameter cannot be trusted for anything wider than the
# truncation window, and the truncation window is undocumented and could
# narrow. Asking for one day at a time and checking the `date` on every row
# that comes back is the only form that cannot silently under-report. It is
# the same reason `tb_revenue_etl` sets CHUNK_DAYS = 1 against the legacy
# host, arrived at independently.
#
# Cost: N requests per grain for an N-day window instead of 1. A 14-day run
# is 42 calls. That is the price of a total that is either right or loud.
CHUNK_DAYS = 1

GRAINS: tuple[tuple[str, str, str], ...] = (
    # (attribute, destination table, id column)
    ("supply_source", "tbx_daily_supply_revenue", "supply_id"),
    ("demand_source", "tbx_daily_demand_revenue", "demand_id"),
    ("placement", "tbx_daily_placement_revenue", "placement_id"),
)

METRICS = (
    "dsp_price_sum",   # gross — what DSPs paid
    "ssp_price_sum",   # payout — what publishers get
    "imps_sum",
    "ssp_wins_sum",
    "requests_sum",
)


def _upsert_sql(table: str, id_col: str, name_col: str) -> str:
    return f"""
INSERT INTO pgam_direct.{table}
  (report_date, {id_col}, {name_col},
   impressions, requests, wins, gross_revenue, pub_payout, updated_at)
VALUES
  (%(report_date)s, %(entity_id)s, %(entity_name)s,
   %(impressions)s, %(requests)s, %(wins)s,
   %(gross_revenue)s, %(pub_payout)s, now())
ON CONFLICT (report_date, {id_col}) DO UPDATE SET
  {name_col}    = EXCLUDED.{name_col},
  impressions   = EXCLUDED.impressions,
  requests      = EXCLUDED.requests,
  wins          = EXCLUDED.wins,
  gross_revenue = EXCLUDED.gross_revenue,
  pub_payout    = EXCLUDED.pub_payout,
  updated_at    = now();
"""


def _f(value) -> float:
    """Platform numerics arrive as strings often enough to matter."""
    if value in (None, "", "-"):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return 0.0


def _i(value) -> int:
    return int(_f(value))


def _entity(row: dict, attribute: str) -> tuple[int | None, str | None]:
    """
    Pull (id, name) for `attribute` out of a report row.

    The report surface is not consistent about whether a dimension comes back
    as a scalar, an `{id, name}` object, or a pair of flattened `x_id` / `x`
    keys, and this has to survive all three rather than pick one and break on
    an account whose response differs. A row whose id cannot be resolved is
    dropped by the caller — an UPSERT keyed on a null id would collapse every
    such row onto one primary key and silently overwrite.
    """
    val = row.get(attribute)
    if isinstance(val, dict):
        raw_id = val.get("id") or val.get("value") or val.get(f"{attribute}_id")
        name = val.get("name") or val.get("title") or val.get("label")
    else:
        raw_id = (row.get(f"{attribute}_id") or row.get(f"{attribute}Id")
                  or (val if isinstance(val, (int, float)) else None))
        name = row.get(f"{attribute}_name") or (val if isinstance(val, str) else None)

    if raw_id is None and isinstance(val, str):
        # Some accounts return "1234 - Partner Name" in the dimension column.
        head = val.split("-", 1)[0].strip()
        if head.isdigit():
            raw_id = head
            name = val.split("-", 1)[1].strip() if "-" in val else val

    try:
        entity_id = int(str(raw_id).strip())
    except (TypeError, ValueError):
        return None, (str(name) if name else None)
    return entity_id, (str(name) if name else None)


def _aggregate(rows: list[dict], attribute: str) -> tuple[list[dict], int]:
    """
    Fold report rows to one record per (date, entity).

    Returns (records, dropped). `dropped` is surfaced rather than swallowed:
    rows silently discarded are how a total ends up quietly short.
    """
    grouped: dict[tuple, dict] = defaultdict(lambda: {
        "impressions": 0, "requests": 0, "wins": 0,
        "gross_revenue": 0.0, "pub_payout": 0.0, "entity_name": None,
    })
    dropped = 0

    for row in rows:
        raw_date = row.get("date") or row.get("report_date")
        if not raw_date:
            dropped += 1
            continue
        try:
            report_date = date.fromisoformat(str(raw_date)[:10])
        except ValueError:
            dropped += 1
            continue

        entity_id, entity_name = _entity(row, attribute)
        if entity_id is None:
            dropped += 1
            continue

        bucket = grouped[(report_date, entity_id)]
        bucket["impressions"]   += _i(row.get("imps_sum"))
        bucket["requests"]      += _i(row.get("requests_sum"))
        bucket["wins"]          += _i(row.get("ssp_wins_sum"))
        bucket["gross_revenue"] += _f(row.get("dsp_price_sum"))
        bucket["pub_payout"]    += _f(row.get("ssp_price_sum"))
        if entity_name and not bucket["entity_name"]:
            bucket["entity_name"] = entity_name

    records = [
        {
            "report_date": d,
            "entity_id": eid,
            "entity_name": v["entity_name"],
            "impressions": v["impressions"],
            "requests": v["requests"],
            "wins": v["wins"],
            "gross_revenue": round(v["gross_revenue"], 4),
            "pub_payout": round(v["pub_payout"], 4),
        }
        for (d, eid), v in grouped.items()
    ]
    return records, dropped


# Path to the DDL. Read from the migration file rather than duplicated here,
# so there is exactly one definition of these tables — a copy in this module
# would drift from migrations/ the first time a column is added, and the two
# would disagree about a revenue table.
_MIGRATION = (Path(__file__).resolve().parents[2]
              / "migrations" / "2026_08_21_tbx_daily_revenue.sql")


def ensure_tables() -> None:
    """
    Create the destination tables if they are absent.

    This repo has no migration runner — `migrations/*.sql` is a record, not
    something applied on deploy (see agents/etl/msn_insights_etl.py, which
    ensures its own schema for the same reason). Without this the first
    authenticated run dies on `relation "pgam_direct.tbx_daily_supply_revenue"
    does not exist`, which reads as a broken ETL rather than an unapplied
    migration — and the credentials would get blamed.

    Every statement in the migration is IF NOT EXISTS, so this is idempotent
    and cheap enough to run on each pass.
    """
    from core.neon import connect

    ddl = _MIGRATION.read_text(encoding="utf-8")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def _write(table: str, id_col: str, name_col: str, records: list[dict]) -> int:
    if not records:
        return 0
    # Imported here, not at module scope: the aggregation logic above is pure
    # and worth testing without a Postgres driver installed, and a run with no
    # records should not need one either.
    from core.neon import connect

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(_upsert_sql(table, id_col, name_col), records)
        conn.commit()
    finally:
        conn.close()
    return len(records)


def _fetch_daily(attribute: str, start: date, end: date) -> tuple[list[dict], list[str], int]:
    """
    Pull `[start, end]` one day at a time.

    Returns `(rows, days_with_no_rows, rows_discarded_off_window)`.

    Every row is checked against the day it was requested for. A row carrying
    another date is discarded and counted, never folded into the requested
    day — misattributing a day's revenue is worse than missing it, because a
    missing day is visible in the table and a misattributed one is not.

    A day with no rows is reported but is not an error: the platform drops
    all-zero rows (reference A5.2), so a genuinely dead day and a truncated
    response look the same from here. What separates them is that truncation
    cannot happen to a one-day request, which is the whole point of chunking.
    """
    from core import tbx_api as tbx

    rows: list[dict] = []
    missing: list[str] = []
    off_window = 0

    day = start
    while day <= end:
        iso = day.isoformat()
        day_rows, _totals = tbx.report(
            date_from=iso,
            date_to=iso,
            attributes=["date", attribute],
            metrics=list(METRICS),
        )
        kept = 0
        for row in day_rows:
            raw = str(row.get("date") or row.get("report_date") or "")[:10]
            if raw and raw != iso:
                off_window += 1
                continue
            rows.append(row)
            kept += 1
        if kept == 0:
            missing.append(iso)
        day += timedelta(days=CHUNK_DAYS)

    return rows, missing, off_window


def run(window_days: int = WINDOW_DAYS) -> dict:
    """Pull each grain from TBX and UPSERT it into Neon."""
    from core import tbx_api as tbx

    if not tbx.configured():
        # Not an error. This job is scheduled ahead of the credentials on
        # purpose, so that adding them to Render is the only step needed.
        #
        # The message names WHICH variable is missing, and checks for the
        # legacy TB_* equivalents, because TB_EMAIL vs TBX_EMAIL is one
        # character apart and both are real variables for two different
        # hosts. Setting the new platform's credentials under the legacy
        # names is the obvious mistake, it leaves this job silently idle,
        # and worse — if it overwrote the legacy values it breaks the
        # legacy leg too. A log line costs nothing and saves a day.
        import os

        missing = [k for k in ("TBX_EMAIL", "TBX_PASSWORD") if not os.getenv(k)]
        print(f"{_LOG} not configured — missing {', '.join(missing)}. "
              f"Nothing pulled.")
        print(f"{_LOG}   Set them in the Render dashboard (Environment) on the "
              f"pgam-intelligence-scheduler worker. This job then fills "
              f"pgam_direct.tbx_daily_* with no code change and no redeploy.")

        legacy_set = [k for k in ("TB_EMAIL", "TB_PASSWORD") if os.getenv(k)]
        if legacy_set and missing:
            print(f"{_LOG}   NOTE: {', '.join(legacy_set)} IS set. Those are the "
                  f"LEGACY host's credentials (ssp.pgammedia.com) and this job "
                  f"cannot use them — it needs the TBX_ prefix for "
                  f"api.pgammedia.com. If the new platform's login was entered "
                  f"under the TB_ names, the legacy ETL is now authenticating "
                  f"with the wrong credentials and will fail too. Check both.")
        return {"ok": True, "skipped": "not_configured", "missing": missing}

    try:
        ensure_tables()
    except Exception as exc:
        # Fatal: without the tables there is nowhere to put anything, and
        # continuing would report a per-grain UPSERT failure three times over
        # for one root cause.
        print(f"{_LOG} could not ensure destination tables — {exc}")
        return {"ok": False, "error": f"ensure_tables: {exc}"}

    end = date.today()
    start = end - timedelta(days=max(window_days - 1, 0))
    df, dt = start.isoformat(), end.isoformat()
    print(f"{_LOG} pulling {df}..{dt} ({window_days}d) from {tbx.TBX_BASE}")

    results: dict[str, object] = {"ok": True}
    total_dropped = 0
    failures: list[str] = []

    for attribute, table, id_col in GRAINS:
        name_col = id_col.replace("_id", "_name")
        try:
            rows, missing_days, off_window = _fetch_daily(attribute, start, end)
        except Exception as exc:
            # One grain failing must not cost the others. A partial load is
            # worth more than none, and the failure is named rather than
            # folded into a generic error at the end.
            print(f"{_LOG} {attribute}: FAILED — {exc}")
            failures.append(f"{attribute}: {exc}")
            results["ok"] = False
            continue

        if off_window:
            # The platform answered a single-day request with other dates.
            # Those rows are discarded rather than attributed to the day we
            # asked for, which would put one day's revenue on another.
            print(f"{_LOG} {attribute}: {off_window} row(s) came back outside "
                  f"the day requested and were discarded")
        if missing_days:
            print(f"{_LOG} {attribute}: no rows for {len(missing_days)} day(s): "
                  f"{', '.join(missing_days[:8])}"
                  f"{' …' if len(missing_days) > 8 else ''}")

        records, dropped = _aggregate(rows, attribute)
        total_dropped += dropped
        try:
            n = _write(table, id_col, name_col, records)
        except Exception as exc:
            print(f"{_LOG} {attribute}: Neon UPSERT failed — {exc}")
            failures.append(f"{attribute} upsert: {exc}")
            results["ok"] = False
            continue

        gross = sum(r["gross_revenue"] for r in records)
        note = f", {dropped} row(s) dropped (unresolvable id/date)" if dropped else ""
        print(f"{_LOG} {attribute}: {len(rows)} row(s) -> {n} upserted, "
              f"gross ${gross:,.2f}{note}")
        results[attribute] = n

    if total_dropped:
        # Loud, because a dropped row is missing revenue, and a total that is
        # quietly short is worse than one that is obviously broken.
        print(f"{_LOG} WARNING: {total_dropped} row(s) dropped across all grains. "
              f"Totals here are UNDERSTATED by whatever those rows carried — "
              f"check the report response shape before trusting them.")
        results["dropped_rows"] = total_dropped

    if failures:
        results["failures"] = failures
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Land TBX (api.pgammedia.com) daily revenue into Neon")
    parser.add_argument("--backfill", type=int, default=None,
                        help=f"pull this many trailing days (default {WINDOW_DAYS})")
    args = parser.parse_args()
    outcome = run(window_days=args.backfill or WINDOW_DAYS)
    sys.exit(0 if outcome.get("ok") else 1)
