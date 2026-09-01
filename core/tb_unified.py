"""
core/tb_unified.py

One answer to "what did Teqblaze earn on day D", across the platform
migration, for every consumer that asks.

Why this exists
---------------
The marketplace moved from `ssp.pgammedia.com` to `api.pgammedia.com` around
2026-08-20. That makes "TB revenue" a per-day question rather than a
per-platform one, and three surfaces were each answering it differently:

  * `/admin/pnl`      — repointed to TBX, then overwrote pre-cutover days
                        with TBX's migration trickle until a cutover guard
                        was added in pgam-recon.
  * the Slack alert   — still reading the retired host, so posting $0.00.
  * `/admin/finance`  — repointed to TBX with the same pre-cutover exposure.

Three implementations of one rule is how they drift, and a P&L and a Slack
alert disagreeing about revenue is worse than either being wrong on its own:
it costs someone an afternoon deciding which to believe.

The rule
--------
    day <  TB_SPLIT_START     -> legacy only
    day in [SPLIT, CUTOVER)   -> legacy + TBX, summed
    day >= TB_TBX_CUTOVER     -> TBX only

The split window is the one place the "never add the two platforms" rule in
CLAUDE.md does not apply. During a cutover each host reports only what
actually flowed through it, so the two are complementary rather than two
readings of one number. Outside that window they do report the same
marketplace and summing would double-count every impression. Defaults are
measured, not chosen: legacy runs full through 2026-08-20 and stops, TBX
carries trickle on 08-17..19 and runs full from 08-20.

Where the numbers come from
---------------------------
**Both legs from Neon**, not from the platforms:

  * legacy — `pgam_direct.tb_daily_publisher_revenue`. The host is retired;
    the rollup is the only surviving source and still holds the history.
  * TBX    — `pgam_direct.tbx_daily_supply_revenue`, filled hourly by
    `agents/etl/tbx_revenue_etl.py`.

Reading the warehouse rather than the live API is the deliberate choice. It
is what makes the Slack alert and the P&L agree *by construction* — same
table, same arithmetic — instead of by coincidence. The cost is freshness:
today's figure lags the hourly ETL by up to an hour. For an hourly snapshot
that is a fair trade, and a number that matches the P&L is worth more than a
number that is forty minutes newer and different.

Row shape
---------
Legacy-flavoured keys (`GROSS_REVENUE`, `PUB_PAYOUT`, …) so existing callers
need only swap the fetch function. `agents/alerts/tb_revenue.py` reads them
through `_extract`, which accepts either case.

One mapping to know: legacy counts `bids`, TBX counts `requests`. They are
the same denominator for win rate and are unified under `BIDS`.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta

_LOG = "[tb_unified]"

SPLIT_START_DEFAULT = "2026-08-20"
CUTOVER_DEFAULT = "2026-08-21"

LEGACY_TABLE = "pgam_direct.tb_daily_publisher_revenue"
LEGACY_NAME_COL = "publisher_name"
TBX_TABLE = "pgam_direct.tbx_daily_supply_revenue"
TBX_NAME_COL = "supply_name"

# The demand side of the same marketplace. Same rule, same shape, different
# tables — so it is a parameter here rather than a second implementation of
# legs_for() somewhere else. Supply and demand are two views of one set of
# impressions: each sums to the same gross, so they are alternatives to read,
# never things to add.
DEMAND_LEGACY_TABLE = "pgam_direct.tb_daily_demand_revenue"
DEMAND_LEGACY_NAME_COL = "demand_name"
DEMAND_TBX_TABLE = "pgam_direct.tbx_daily_demand_revenue"
DEMAND_TBX_NAME_COL = "demand_name"

SIDES = {
    "supply": (LEGACY_TABLE, LEGACY_NAME_COL, TBX_TABLE, TBX_NAME_COL),
    "demand": (DEMAND_LEGACY_TABLE, DEMAND_LEGACY_NAME_COL,
               DEMAND_TBX_TABLE, DEMAND_TBX_NAME_COL),
}


def _env_date(var: str, default: str) -> date:
    raw = (os.environ.get(var) or default).strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        print(f"{_LOG} {var}={raw!r} is not YYYY-MM-DD — using {default}",
              file=sys.stderr)
        return date.fromisoformat(default)


def split_start() -> date:
    return _env_date("TB_SPLIT_START", SPLIT_START_DEFAULT)


def cutover() -> date:
    return _env_date("TB_TBX_CUTOVER", CUTOVER_DEFAULT)


def legs_for(day: date) -> tuple[bool, bool]:
    """`(use_legacy, use_tbx)` for one day."""
    lo, hi = split_start(), cutover()
    if day >= hi:
        return False, True
    if day >= lo:
        return True, True          # split day — both, summed
    return True, False


def _rows(table: str, name_col: str, start: date, end: date,
          by_entity: bool) -> list[dict]:
    """
    Per-day (or per-day-per-entity) rows out of one rollup table.

    `bids` and `requests` are the same denominator under different names on
    the two platforms; COALESCE picks whichever the table has so one query
    shape serves both.
    """
    from core.neon import connect

    group = "report_date" + (f", {name_col}" if by_entity else "")
    select = ("report_date" + (f", {name_col} AS entity_name" if by_entity
                               else ", NULL::text AS entity_name"))
    denom = "requests" if table.startswith("pgam_direct.tbx_") else "bids"
    sql = f"""
        SELECT {select},
               sum(impressions)   AS impressions,
               sum({denom})       AS bids,
               sum(wins)          AS wins,
               sum(gross_revenue) AS gross_revenue,
               sum(pub_payout)    AS pub_payout
          FROM {table}
         WHERE report_date BETWEEN %s AND %s
         GROUP BY {group}
    """
    out: list[dict] = []
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(sql, (start, end))
            for row in cur.fetchall():
                out.append({
                    "report_date": row[0],
                    "entity_name": row[1],
                    "IMPRESSIONS": float(row[2] or 0),
                    "BIDS": float(row[3] or 0),
                    "WINS": float(row[4] or 0),
                    "GROSS_REVENUE": float(row[5] or 0),
                    "PUB_PAYOUT": float(row[6] or 0),
                })
    return out


def fetch(breakdown: str, metrics: list | tuple, start_date, end_date,
          side: str = "supply") -> list[dict]:
    """
    Drop-in replacement for `core.api.fetch_tb`, sourced per day.

    `metrics` is accepted and ignored — every column is cheap here and the
    signature exists so callers do not have to change shape.

    `side` selects supply-source or demand-source tables. The cutover rule is
    identical either way — that is the point of it living here — so a caller
    that wants both reads twice rather than reimplementing legs_for().
    """
    if side not in SIDES:
        raise ValueError(f"side must be one of {sorted(SIDES)}, got {side!r}")
    legacy_table, legacy_col, tbx_table, tbx_col = SIDES[side]

    start = date.fromisoformat(str(start_date)[:10])
    end = date.fromisoformat(str(end_date)[:10])
    by_entity = str(breakdown).upper() != "DATE"

    legacy = _rows(legacy_table, legacy_col, start, end, by_entity)
    tbx = _rows(tbx_table, tbx_col, start, end, by_entity)

    # Keep only the leg(s) that actually served each day, then fold. A split
    # day keeps both and they add; every other day keeps exactly one, so the
    # fold is a no-op there and cannot double-count.
    merged: dict[tuple, dict] = {}
    for rows, is_tbx in ((legacy, False), (tbx, True)):
        for row in rows:
            use_legacy, use_tbx = legs_for(row["report_date"])
            if (is_tbx and not use_tbx) or (not is_tbx and not use_legacy):
                continue
            key = (row["report_date"], row["entity_name"])
            slot = merged.setdefault(key, {
                "DATE": row["report_date"].isoformat(),
                "PUBLISHER": row["entity_name"],
                "IMPRESSIONS": 0.0, "BIDS": 0.0, "WINS": 0.0,
                "GROSS_REVENUE": 0.0, "PUB_PAYOUT": 0.0,
            })
            for k in ("IMPRESSIONS", "BIDS", "WINS", "GROSS_REVENUE", "PUB_PAYOUT"):
                slot[k] += row[k]

    return sorted(merged.values(), key=lambda r: (r["DATE"], r["PUBLISHER"] or ""))


def configured() -> bool:
    """True when the warehouse is reachable — the only dependency here."""
    return bool((os.environ.get("PGAM_DIRECT_DATABASE_URL")
                 or os.environ.get("DATABASE_URL") or "").strip())


def describe_window(start_date, end_date) -> str:
    """Which platform each day in the range is attributed to, for a log line."""
    start = date.fromisoformat(str(start_date)[:10])
    end = date.fromisoformat(str(end_date)[:10])
    parts, day = [], start
    while day <= end:
        use_l, use_t = legs_for(day)
        parts.append(f"{day}:{'legacy+tbx' if use_l and use_t else ('legacy' if use_l else 'tbx')}")
        day += timedelta(days=1)
    return " ".join(parts)
