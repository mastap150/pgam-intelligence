"""
agents/compliance/observed_monetization.py

Derive the rolling (publisher_key × ssp_key) activity table from
pgam_direct.ll_daily_partner_revenue. Truncate + repopulate each run so
absence == "no longer monetizing".

Input  : ll_daily_partner_revenue (publisher_id, demand_id, demand_name,
         report_date, gross_revenue, impressions) joined to
         compliance_publishers.ll_publisher_id to get publisher_key.

Output : compliance_observed_monetization (publisher_key, ssp_key,
         ssp_domain, lookback_days, revenue_usd, impressions,
         demand_count, demand_names, first/last_observed_at).

Classification: each LL demand_name is run through
ssp_registry.classify_demand_name(). Demands that don't classify to
any known SSP are tallied separately for visibility but don't drive
findings (we can't write an expected ads.txt line for an unknown SSP).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from core.neon import connect

from agents.compliance.ssp_registry import (
    PHASE_2_SSP_EXPECTATIONS,
    SspExpectation,
    classify_demand_name,
)

LOOKBACK_DAYS = 7


# ── Source query: join LL revenue to bridged publishers ──────────────────────


_PULL_SQL = """
SELECT
    cp.publisher_key,
    r.demand_id,
    r.demand_name,
    SUM(r.gross_revenue)::numeric AS revenue_usd,
    SUM(r.impressions)::bigint    AS impressions
FROM pgam_direct.ll_daily_partner_revenue r
JOIN pgam_direct.compliance_publishers cp
  ON cp.ll_publisher_id = r.publisher_id
WHERE r.report_date >= (current_date - %(lookback)s::int)
  AND cp.is_active = TRUE
GROUP BY cp.publisher_key, r.demand_id, r.demand_name
HAVING SUM(r.gross_revenue) > 0;
"""


# ── Truncate + insert ────────────────────────────────────────────────────────


_TRUNCATE_SQL = "TRUNCATE TABLE pgam_direct.compliance_observed_monetization;"

_INSERT_SQL = """
INSERT INTO pgam_direct.compliance_observed_monetization
    (publisher_key, ssp_key, ssp_domain, lookback_days,
     revenue_usd, impressions, demand_count, demand_names,
     first_observed_at, last_observed_at)
VALUES
    (%(publisher_key)s, %(ssp_key)s, %(ssp_domain)s, %(lookback_days)s,
     %(revenue_usd)s, %(impressions)s, %(demand_count)s, %(demand_names)s,
     now(), now());
"""


@dataclass(frozen=True)
class ObservedRow:
    publisher_key: str
    ssp_key: str
    ssp_domain: str
    revenue_usd: float
    impressions: int
    demand_count: int
    demand_names: tuple[str, ...]


def _classify_rows(
    raw_rows: list[dict],
) -> tuple[list[ObservedRow], list[dict]]:
    """Group by (publisher_key, ssp_key); separate unclassified demands."""
    grouped: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "ssp_domain":  "",
        "revenue_usd": 0.0,
        "impressions": 0,
        "demand_names": set(),
    })
    unclassified: list[dict] = []

    for row in raw_rows:
        exp: SspExpectation | None = classify_demand_name(row["demand_name"] or "")
        if exp is None:
            unclassified.append(row)
            continue
        key = (row["publisher_key"], exp.ssp_key)
        agg = grouped[key]
        agg["ssp_domain"] = exp.ads_txt_domain
        agg["revenue_usd"] += float(row["revenue_usd"] or 0)
        agg["impressions"] += int(row["impressions"] or 0)
        agg["demand_names"].add(row["demand_name"])

    observed = [
        ObservedRow(
            publisher_key=k[0],
            ssp_key=k[1],
            ssp_domain=v["ssp_domain"],
            revenue_usd=round(v["revenue_usd"], 4),
            impressions=v["impressions"],
            demand_count=len(v["demand_names"]),
            demand_names=tuple(sorted(v["demand_names"])),
        )
        for k, v in grouped.items()
    ]
    return observed, unclassified


def _replace(observed: list[ObservedRow], lookback_days: int) -> int:
    rows = [
        {
            "publisher_key": o.publisher_key,
            "ssp_key":       o.ssp_key,
            "ssp_domain":    o.ssp_domain,
            "lookback_days": lookback_days,
            "revenue_usd":   o.revenue_usd,
            "impressions":   o.impressions,
            "demand_count":  o.demand_count,
            "demand_names":  list(o.demand_names),
        }
        for o in observed
    ]
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_TRUNCATE_SQL)
            if rows:
                cur.executemany(_INSERT_SQL, rows)
        conn.commit()
    return len(rows)


@dataclass(frozen=True)
class ObservedStats:
    observed_rows: int
    unique_publishers: int
    unique_ssps: int
    unclassified_demands: int


def refresh_observed_monetization(lookback_days: int = LOOKBACK_DAYS) -> ObservedStats:
    """End-to-end: pull, classify, replace, return summary."""
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_PULL_SQL, {"lookback": lookback_days})
            cols = [c.name for c in cur.description]
            raw = [dict(zip(cols, r)) for r in cur.fetchall()]

    observed, unclassified = _classify_rows(raw)
    inserted = _replace(observed, lookback_days)

    return ObservedStats(
        observed_rows=inserted,
        unique_publishers=len({o.publisher_key for o in observed}),
        unique_ssps=len({o.ssp_key for o in observed}),
        unclassified_demands=len(unclassified),
    )


def load_observed_for_publishers(publisher_keys: list[str]) -> dict[str, list[ObservedRow]]:
    """Read back observed_monetization for the given publishers. Used by the validator."""
    if not publisher_keys:
        return {}
    sql = """
    SELECT publisher_key, ssp_key, ssp_domain, revenue_usd, impressions,
           demand_count, demand_names
    FROM pgam_direct.compliance_observed_monetization
    WHERE publisher_key = ANY(%(keys)s);
    """
    out: dict[str, list[ObservedRow]] = defaultdict(list)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"keys": publisher_keys})
            for r in cur.fetchall():
                out[r[0]].append(ObservedRow(
                    publisher_key=r[0],
                    ssp_key=r[1],
                    ssp_domain=r[2],
                    revenue_usd=float(r[3] or 0),
                    impressions=int(r[4] or 0),
                    demand_count=int(r[5] or 0),
                    demand_names=tuple(r[6] or []),
                ))
    return dict(out)


# Expose internals for tests
classify_rows_for_tests = _classify_rows
