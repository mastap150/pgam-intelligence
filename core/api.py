import os
import requests
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# LL (Limelight) — stats.ortb.net
# ---------------------------------------------------------------------------
LL_API_BASE_URL = os.environ.get("LL_API_BASE_URL", "http://stats.ortb.net/v1/stats")
LL_CLIENT_KEY   = os.environ.get("LL_CLIENT_KEY",   os.environ.get("TB_CLIENT_KEY", ""))
LL_SECRET_KEY   = os.environ.get("LL_SECRET_KEY",   os.environ.get("TB_SECRET_KEY", ""))

# ---------------------------------------------------------------------------
# TB (Teqblaze) — your white-label SSP stats endpoint
# Set TB_API_BASE_URL, TB_CLIENT_KEY, TB_SECRET_KEY in .env once you have
# the Teqblaze API credentials from ssp.pgammedia.com → Settings → API.
# ---------------------------------------------------------------------------
TB_API_BASE_URL = os.environ.get("TB_API_BASE_URL", "")
TB_CLIENT_KEY   = os.environ.get("TB_CLIENT_KEY",   "")
TB_SECRET_KEY   = os.environ.get("TB_SECRET_KEY",   "")


def _fetch(base_url: str, client_key: str, secret_key: str,
           breakdown, metrics, start_date: str, end_date: str) -> list:
    """Core fetch — shared by fetch() and fetch_tb()."""
    if not base_url or not client_key:
        raise ValueError(f"API not configured (base_url={base_url!r})")

    if isinstance(metrics, list):
        metrics = ",".join(metrics)

    params = {
        "clientKey": client_key,
        "secretKey": secret_key,
        "breakdown": breakdown,
        "metrics":   metrics,
        "startDate": start_date,
        "endDate":   end_date,
        "output":    "json",
    }

    response = requests.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, dict):
        return data.get("body", data.get("data", data.get("rows", [])))
    return data


def fetch(breakdown, metrics, start_date, end_date) -> list:
    """
    Fetch stats from the LL (Limelight / stats.ortb.net) platform.

    Args:
        breakdown  (str):       e.g. "DATE", "PUBLISHER", "DEMAND_PARTNER"
        metrics    (list|str):  metric names to retrieve
        start_date (str):       "YYYY-MM-DD"
        end_date   (str):       "YYYY-MM-DD"

    Returns:
        list[dict]: rows returned by the API, or [] on error.
    """
    return _fetch(LL_API_BASE_URL, LL_CLIENT_KEY, LL_SECRET_KEY,
                  breakdown, metrics, start_date, end_date)


def fetch_tb(breakdown, metrics, start_date, end_date) -> list:
    """
    Fetch stats from the TB (Teqblaze white-label SSP) platform.

    Uses token-based auth via core.tb_api. Requires TB_EMAIL and TB_PASSWORD
    in .env (your ssp.pgammedia.com login credentials).

    Returns LL-compatible rows (GROSS_REVENUE, PUB_PAYOUT, IMPRESSIONS, etc.)
    """
    from core.tb_api import fetch_tb as _fetch_tb
    return _fetch_tb(breakdown, metrics, start_date, end_date)


def tb_configured() -> bool:
    """Return True if TB credentials are present in the environment."""
    from core.tb_api import tb_configured as _tc
    return _tc()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def yesterday() -> str:
    """Return yesterday's date as 'YYYY-MM-DD'."""
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def today() -> str:
    """Return today's date as 'YYYY-MM-DD'."""
    return date.today().strftime("%Y-%m-%d")


def n_days_ago(n: int) -> str:
    """Return the date n days ago as 'YYYY-MM-DD'."""
    return (date.today() - timedelta(days=n)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def sf(v) -> float:
    """Safe float conversion — returns 0.0 on None / empty / non-numeric."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def pct(n, d) -> float:
    """Safe percentage: (n / d) * 100, returns 0.0 if d is zero."""
    n, d = sf(n), sf(d)
    return (n / d * 100) if d else 0.0


def fmt_usd(v) -> str:
    """Format a value as a USD dollar string, e.g. 1234.5 → '$1,234.50'."""
    return f"${sf(v):,.2f}"


def fmt_n(v) -> str:
    """Format a value as a comma-separated integer string, e.g. 1234567 → '1,234,567'."""
    return f"{int(sf(v)):,}"


def arrow(v) -> str:
    """Return '▲' for positive values and '▼' for zero or negative values."""
    return "▲" if sf(v) > 0 else "▼"
