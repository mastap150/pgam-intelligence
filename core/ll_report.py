"""
core/ll_report.py
~~~~~~~~~~~~~~~~~
Wrapper for the LL Extended Reporting API (POST http://stats.ortb.net/v1/report).

This module complements core/api.py (which wraps the basic GET stats endpoint)
by exposing the richer POST-based reporting API that supports arbitrary
dimension/metric combinations and server-side filtering.

.. warning:: **Date filter bug in the POST /v1/report endpoint**

    The extended POST API at ``http://stats.ortb.net/v1/report`` **ignores
    ``startDate`` / ``endDate`` filters** and returns all-time data regardless
    of the date range supplied.  The basic GET API at
    ``http://stats.ortb.net/v1/stats`` (wrapped by ``core.api.fetch``) correctly
    respects date ranges.

    Use ``fetch_publisher_demand()`` and ``fetch_publisher()`` (defined in this
    module) when date accuracy is required — they route through the GET stats API.
    Check ``DATE_FILTER_WORKS`` at import time to guard call sites::

        import core.ll_report as llr
        if not llr.DATE_FILTER_WORKS:
            rows = llr.fetch_publisher_demand(start, end)   # date-accurate path

.. warning:: **WINS event drop bug for Start.IO and certain other adapters**

    LL's reporting pipeline does not emit ``WINS`` events for ~49 demand
    adapter entries (~$10K/wk revenue, of which Start.IO is ~$7.6K/wk).
    Affected rows return ``WINS=0`` despite having millions of bids,
    hundreds of thousands of impressions, and real revenue. ``OPPORTUNITIES``
    is broken in the same way for these rows.

    Detection signature: ``WINS == 0`` AND ``IMPRESSIONS > 0``.

    Workaround applied in :func:`_sanitize_rows`: backfill
    ``WINS = IMPRESSIONS`` for matching rows. This is a *proxy*, not a
    true count — IMPRESSIONS and WINS are not a strict superset/subset
    in LL's data model (billing reconciliation, multi-imp creatives,
    etc. can make either larger). But IMPRESSIONS is the most reliable
    nonzero signal we have, and substituting it lets downstream win-rate
    consumers (floor optimizer, demand saturation, win_rate_maximizer,
    etc.) actually see Start.IO instead of treating it as dead inventory.

    Patched rows are tagged with ``_WINS_BACKFILLED = True`` so callers
    that care can detect the workaround. Filed upstream with LL; remove
    once they ship a fix. See ``scripts/diagnose_startio_wins.py`` and
    ``scripts/verify_wins_backfill.py`` to re-test current API behavior.

Typical usage
-------------
    from core.ll_report import report, report_pub_demand, FUNNEL_METRICS

    rows = report_pub_demand("2026-04-01", "2026-04-12")
    for r in sorted(rows, key=lambda x: -_sf(x.get("GROSS_REVENUE", 0)))[:5]:
        print(r["PUBLISHER_NAME"], r["GROSS_REVENUE"])
"""

import os
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Known API limitation — see module docstring for details
# ---------------------------------------------------------------------------
DATE_FILTER_WORKS: bool = False
"""
``False`` — the POST /v1/report endpoint ignores ``startDate``/``endDate``.
Use ``fetch_publisher_demand()`` / ``fetch_publisher()`` for date-accurate data.
"""

# ---------------------------------------------------------------------------
# Credentials — mirrors the pattern in core/api.py
# LL_CLIENT_KEY / LL_SECRET_KEY take priority; fall back to TB_* for compat.
# ---------------------------------------------------------------------------
_CLIENT_KEY: str = os.environ.get("LL_CLIENT_KEY", os.environ.get("TB_CLIENT_KEY", ""))
_SECRET_KEY: str = os.environ.get("LL_SECRET_KEY", os.environ.get("TB_SECRET_KEY", ""))

_REPORT_URL: str = "http://stats.ortb.net/v1/report"
_TIMEOUT: int = 30

# ---------------------------------------------------------------------------
# Available dimensions (for reference / validation)
# ---------------------------------------------------------------------------
DIMENSIONS = [
    "DATE", "HOUR",
    "PUBLISHER_ID", "PUBLISHER_NAME",
    "DEMAND_ID", "DEMAND_NAME",
    "CHANNEL_ID", "AD_UNIT_ID",
    "SIZE", "BUNDLE", "OS", "COUNTRY", "DOMAIN",
    "DEVICE_TYPE", "CREATIVE_ID", "BROWSER",
    "DEVICE_MAKE", "DEVICE_MODEL",
]

# ---------------------------------------------------------------------------
# Available metrics (for reference / validation)
# ---------------------------------------------------------------------------
METRICS = [
    "OPPORTUNITIES", "BID_REQUESTS", "BIDS", "WINS", "IMPRESSIONS",
    "PUB_PAYOUT", "DEMAND_PAYOUT", "GROSS_REVENUE",
    "BID_RESPONSE_TIMEOUTS", "BID_RESPONSE_ERRORS",
    "GROSS_ECPM", "OPPORTUNITY_ECPM",
    "OPPORTUNITY_FILL_RATE", "BID_REQUEST_FILL_RATE",
    "VAST_START", "VAST_FIRST_QUARTILE", "VAST_MIDPOINT",
    "VAST_THIRD_QUARTILE", "VAST_COMPLETE",
]

# ---------------------------------------------------------------------------
# Standard funnel metric set used by convenience helpers
# ---------------------------------------------------------------------------
FUNNEL_METRICS: list[str] = [
    "OPPORTUNITIES",
    "BID_REQUESTS",
    "BIDS",
    "WINS",
    "IMPRESSIONS",
    "GROSS_REVENUE",
    "PUB_PAYOUT",
    "OPPORTUNITY_FILL_RATE",
    "BID_REQUEST_FILL_RATE",
    "OPPORTUNITY_ECPM",
    "GROSS_ECPM",
]

# ---------------------------------------------------------------------------
# Filter operator constants (for reference)
# ---------------------------------------------------------------------------
FILTER_OPS = [
    "EQ", "NOT_EQ",
    "IN", "NOT_IN",
    "GREATER_THAN", "GREATER_THAN_OR_EQ",
    "LESS_THAN", "LESS_THAN_OR_EQ",
    "LIKE",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    """
    Safe float conversion.

    Handles None, empty string, and the literal string "NaN" that the LL API
    sometimes returns for metrics with no data — all map to 0.0.
    """
    if v is None:
        return 0.0
    if isinstance(v, str):
        stripped = v.strip()
        if stripped.lower() in ("nan", ""):
            return 0.0
        try:
            return float(stripped)
        except ValueError:
            return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _patch_zero_wins(row: dict) -> dict:
    """
    Workaround for the LL ``WINS`` event-drop bug (see module docstring).

    When ``WINS == 0`` AND ``IMPRESSIONS > 0`` (impossible for healthy data),
    backfill ``WINS = IMPRESSIONS`` as a conservative lower-bound estimate
    and tag the row with ``_WINS_BACKFILLED = True``.

    Mutates and returns the row in place.
    """
    if "WINS" not in row or "IMPRESSIONS" not in row:
        return row
    wins = _sf(row.get("WINS"))
    imps = _sf(row.get("IMPRESSIONS"))
    if wins == 0.0 and imps > 0.0:
        row["WINS"] = imps
        row["_WINS_BACKFILLED"] = True
    return row


def _sanitize_rows(rows: list[dict]) -> list[dict]:
    """
    Walk every row and replace "NaN" string values in numeric-looking fields
    with 0.0 so callers never have to deal with them.

    Also applies the WINS event-drop workaround via :func:`_patch_zero_wins`.
    """
    metric_set = set(METRICS)
    sanitized = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if k in metric_set:
                clean[k] = _sf(v)
            else:
                clean[k] = v
        _patch_zero_wins(clean)
        sanitized.append(clean)
    return sanitized


# ---------------------------------------------------------------------------
# Core public API
# ---------------------------------------------------------------------------

def report(
    dimensions: list[str],
    metrics: list[str],
    start_date: str,
    end_date: str,
    filters: list[dict] | None = None,
) -> list[dict]:
    """
    POST to the LL Extended Reporting API and return the result rows.

    Parameters
    ----------
    dimensions : list[str]
        One or more dimension names from ``DIMENSIONS``, e.g.
        ``["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_NAME"]``.
    metrics : list[str]
        One or more metric names from ``METRICS``, e.g.
        ``["IMPRESSIONS", "GROSS_REVENUE"]``.
    start_date : str
        Inclusive start date in "YYYY-MM-DD" format.
    end_date : str
        Inclusive end date in "YYYY-MM-DD" format.
    filters : list[dict] or None
        Optional server-side filters.  Each entry must be a dict with keys:

        * ``"dimension"``  — one of the ``DIMENSIONS`` constants
        * ``"type"``       — one of the ``FILTER_OPS`` constants
        * ``"value"``      — the value to filter on

        Example::

            [{"dimension": "PUBLISHER_NAME", "type": "EQ", "value": "Foo"}]

    Returns
    -------
    list[dict]
        The ``body`` array from the API response with "NaN" values in metric
        fields normalised to ``0.0``.

    Raises
    ------
    ValueError
        If API credentials are not configured.
    RuntimeError
        If the API returns a non-SUCCESS status.
    requests.HTTPError
        If the HTTP response indicates a server or client error.
    """
    if not _CLIENT_KEY:
        raise ValueError(
            "LL API credentials not configured. "
            "Set LL_CLIENT_KEY / LL_SECRET_KEY (or TB_CLIENT_KEY / TB_SECRET_KEY) "
            "in your .env file."
        )

    payload: dict = {
        "clientKey": _CLIENT_KEY,
        "secretKey": _SECRET_KEY,
        "dimensions": dimensions,
        "metrics": metrics,
        "filters": filters if filters is not None else [],
        "startDate": start_date,
        "endDate": end_date,
    }

    response = requests.post(_REPORT_URL, json=payload, timeout=_TIMEOUT)
    response.raise_for_status()

    data = response.json()

    status = data.get("status", "")
    if status != "SUCCESS":
        msg = data.get("message") or data.get("error") or repr(data)
        raise RuntimeError(f"LL report API returned status={status!r}: {msg}")

    rows = data.get("body", [])
    return _sanitize_rows(rows)


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def report_pub_demand(
    start_date: str,
    end_date: str,
    extra_dimensions: list[str] | None = None,
) -> list[dict]:
    """
    Publisher × demand breakdown with the full ``FUNNEL_METRICS`` set.

    Parameters
    ----------
    start_date : str
        Inclusive start date in "YYYY-MM-DD" format.
    end_date : str
        Inclusive end date in "YYYY-MM-DD" format.
    extra_dimensions : list[str] or None
        Additional dimensions to append after the default
        ``["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"]``
        grouping, e.g. ``["DATE"]``.

    Returns
    -------
    list[dict]
        Rows from the API with metric values sanitised.
    """
    dims = ["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME"]
    if extra_dimensions:
        dims = dims + [d for d in extra_dimensions if d not in dims]
    return report(dims, FUNNEL_METRICS, start_date, end_date)


def report_pub_country(start_date: str, end_date: str) -> list[dict]:
    """
    Publisher × country breakdown with the full ``FUNNEL_METRICS`` set.

    Parameters
    ----------
    start_date : str
        Inclusive start date in "YYYY-MM-DD" format.
    end_date : str
        Inclusive end date in "YYYY-MM-DD" format.

    Returns
    -------
    list[dict]
        Rows from the API with metric values sanitised.
    """
    dims = ["PUBLISHER_ID", "PUBLISHER_NAME", "COUNTRY"]
    return report(dims, FUNNEL_METRICS, start_date, end_date)


def report_pub_demand_country(start_date: str, end_date: str) -> list[dict]:
    """
    Publisher × demand × country breakdown with the full ``FUNNEL_METRICS`` set.

    Parameters
    ----------
    start_date : str
        Inclusive start date in "YYYY-MM-DD" format.
    end_date : str
        Inclusive end date in "YYYY-MM-DD" format.

    Returns
    -------
    list[dict]
        Rows from the API with metric values sanitised.
    """
    dims = ["PUBLISHER_ID", "PUBLISHER_NAME", "DEMAND_ID", "DEMAND_NAME", "COUNTRY"]
    return report(dims, FUNNEL_METRICS, start_date, end_date)


def fetch_publisher_demand(start_date: str, end_date: str) -> list[dict]:
    """
    Fetch publisher x demand breakdown using the basic GET stats API which
    correctly respects date ranges (unlike the POST /v1/report endpoint).

    Returns rows with: PUBLISHER_ID, PUBLISHER_NAME, DEMAND_PARTNER, DEMAND_PARTNER_NAME,
    GROSS_REVENUE, PUB_PAYOUT, IMPRESSIONS, WINS, BIDS, BID_REQUESTS, OPPORTUNITIES,
    OPPORTUNITY_FILL_RATE, BID_REQUEST_FILL_RATE, GROSS_ECPM
    """
    from core.api import fetch
    return fetch(
        "PUBLISHER,DEMAND_PARTNER",
        "GROSS_REVENUE,PUB_PAYOUT,IMPRESSIONS,WINS,BIDS,BID_REQUESTS,OPPORTUNITIES,OPPORTUNITY_FILL_RATE,BID_REQUEST_FILL_RATE,GROSS_ECPM,OPPORTUNITY_ECPM",
        start_date,
        end_date,
    )


def fetch_publisher(start_date: str, end_date: str) -> list[dict]:
    """Publisher-only breakdown using the correctly date-filtered GET API."""
    from core.api import fetch
    return fetch(
        "PUBLISHER",
        "GROSS_REVENUE,PUB_PAYOUT,IMPRESSIONS,WINS,BIDS,BID_REQUESTS,OPPORTUNITIES,OPPORTUNITY_FILL_RATE,BID_REQUEST_FILL_RATE,GROSS_ECPM,OPPORTUNITY_ECPM",
        start_date,
        end_date,
    )


def funnel_metrics() -> list[str]:
    """
    Return the standard funnel metric list.

    This is a functional accessor for ``FUNNEL_METRICS`` — useful when callers
    want to retrieve the list without importing the constant directly.

    Returns
    -------
    list[str]
        A copy of ``FUNNEL_METRICS``.
    """
    return list(FUNNEL_METRICS)


# ---------------------------------------------------------------------------
# Date helpers (mirrors core/api.py for standalone use)
# ---------------------------------------------------------------------------

def _yesterday() -> str:
    """Return yesterday's date as 'YYYY-MM-DD'."""
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Smoke test / quick CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    """
    Quick smoke test: pull yesterday's publisher × demand data and print the
    top 5 rows by gross revenue.

    Usage:
        python -m core.ll_report
    """
    yest = _yesterday()
    print(f"Fetching publisher × demand report for {yest} …")

    try:
        rows = report_pub_demand(yest, yest)
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)

    print(f"Total rows returned: {len(rows)}")

    top5 = sorted(rows, key=lambda r: -_sf(r.get("GROSS_REVENUE", 0)))[:5]

    print("\nTop 5 by Gross Revenue:")
    print(f"{'PUBLISHER_NAME':<35} {'DEMAND_NAME':<30} {'GROSS_REVENUE':>14} {'IMPRESSIONS':>12}")
    print("-" * 95)
    for r in top5:
        print(
            f"{str(r.get('PUBLISHER_NAME', '')):<35} "
            f"{str(r.get('DEMAND_NAME', '')):<30} "
            f"${_sf(r.get('GROSS_REVENUE', 0)):>13,.2f} "
            f"{int(_sf(r.get('IMPRESSIONS', 0))):>12,}"
        )
