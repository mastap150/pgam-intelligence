"""
core/tb_api.py

Teqblaze (TB) white-label SSP reporting API client.
Base URL: https://ssp.pgammedia.com

Authentication
--------------
Token-based. Steps:
  1. POST /api/create_token  { email, password, time=0 }  → { token, end }
  2. All subsequent calls:   GET /api/{token}/adx-report?...

Tokens with time=0 last 1 year so we request one on first call and cache it
in /tmp/pgam_tb_token.json. If the token is expired or invalid the client
automatically re-authenticates.

Metric mapping vs LL (stats.ortb.net)
--------------------------------------
  LL metric           TB metric / computation
  ─────────────────── ────────────────────────────────────────
  GROSS_REVENUE       ssp_revenue + profit  (total DSP spend)
  PUB_PAYOUT          ssp_revenue           (publisher net)
  PROFIT              profit                (Teqblaze margin)
  IMPRESSIONS         impressions
  WINS                ssp_wins
  BIDS                bid_requests
  AVG_FLOOR_PRICE     avg_ssp_bid_floor
  AVG_BID_PRICE       avg_ssp_bid_price
  GROSS_ECPM          ssp_ecpm

Breakdown mapping vs LL
------------------------
  LL breakdown        TB attribute / day_group
  ─────────────────── ────────────────────────
  DATE                day_group=day  (no attribute)
  PUBLISHER           attribute[]=ssp_name
  DEMAND_PARTNER      attribute[]=dsp_name
  COUNTRY_NAME        attribute[]=country
  DATE,PUBLISHER      day_group=day + attribute[]=ssp_name
  (etc. — combine freely)
"""

import json
import os
import time
import urllib.request
import urllib.parse
import urllib.error
from dotenv import load_dotenv

# Global lock to prevent concurrent TB API calls (TB allows 1 query at a time per user)
import threading
_TB_LOCK = threading.Lock()

load_dotenv(override=True)

TB_BASE         = "https://ssp.pgammedia.com/api"
TB_EMAIL        = os.environ.get("TB_EMAIL", "")
TB_PASSWORD     = os.environ.get("TB_PASSWORD", "")
TOKEN_CACHE     = "/tmp/pgam_tb_token.json"

# ---------------------------------------------------------------------------
# Metric / breakdown name maps
# ---------------------------------------------------------------------------

# Metrics that need to be fetched to reconstruct LL-style values
_CORE_METRICS = [
    "ssp_revenue",
    "profit",
    "impressions",
    "ssp_wins",
    "bid_requests",
    "avg_ssp_bid_floor",
    "avg_ssp_bid_price",
    "ssp_ecpm",
    "dsp_wins",
    "bid_responses",
]

# TB attribute value → LL-style breakdown label constant
_BREAKDOWN_MAP = {
    "DATE":           {"day_group": "day",  "attribute": []},
    "PUBLISHER":      {"day_group": "total","attribute": ["ssp_name"]},
    "DEMAND_PARTNER": {"day_group": "total","attribute": ["dsp_name"]},
    "COUNTRY_NAME":   {"day_group": "total","attribute": ["country"]},
    # Compound breakdowns
    "DATE,PUBLISHER":      {"day_group": "day",   "attribute": ["ssp_name"]},
    "DATE,DEMAND_PARTNER": {"day_group": "day",   "attribute": ["dsp_name"]},
    "PUBLISHER,DEMAND_PARTNER": {"day_group": "total","attribute": ["ssp_name", "dsp_name"]},
    "DATE,PUBLISHER,DEMAND_PARTNER": {"day_group": "day","attribute": ["ssp_name", "dsp_name"]},
    # Discovered 2026-04-30 — TB API actually supports more attribute
    # combinations than the historical map implied. Pub × country and
    # standalone OS work cleanly; pub × device_type returns 400.
    "PUBLISHER,COUNTRY_NAME":   {"day_group": "total","attribute": ["ssp_name", "country"]},
    "OS":                       {"day_group": "total","attribute": ["os"]},
}

# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def _load_cached_token() -> str:
    """Return a cached token if it exists and won't expire in the next 5 minutes."""
    if not os.path.exists(TOKEN_CACHE):
        return ""
    try:
        with open(TOKEN_CACHE) as f:
            data = json.load(f)
        token = data.get("token", "")
        end   = data.get("end", 0)
        if token and (end == 0 or end > time.time() + 300):
            return token
    except (json.JSONDecodeError, OSError):
        pass
    return ""


def _save_token(token: str, end: int):
    try:
        with open(TOKEN_CACHE, "w") as f:
            json.dump({"token": token, "end": end}, f)
    except OSError:
        pass


def _create_token() -> str:
    """POST /api/create_token — returns a 1-year token."""
    if not TB_EMAIL or not TB_PASSWORD:
        raise ValueError(
            "TB credentials not configured. "
            "Add TB_EMAIL and TB_PASSWORD to .env "
            "(your ssp.pgammedia.com login email and password)."
        )

    body = urllib.parse.urlencode({
        "email":    TB_EMAIL,
        "password": TB_PASSWORD,
        "time":     0,          # 0 = 1 year lifetime
    }).encode()

    req = urllib.request.Request(
        f"{TB_BASE}/create_token",
        data=body,
        method="POST",
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())

    token = data.get("token", "")
    end   = data.get("end", 0)
    if not token:
        raise RuntimeError(f"TB token creation failed: {data}")

    _save_token(token, end)
    return token


def get_token() -> str:
    """Return a valid TB token, creating one if needed."""
    token = _load_cached_token()
    if token:
        return token
    return _create_token()


def force_refresh_token() -> str:
    """Drop cache and create a fresh token. Use after a 401."""
    try:
        if os.path.exists(TOKEN_CACHE):
            os.remove(TOKEN_CACHE)
    except OSError:
        pass
    return _create_token()


# ---------------------------------------------------------------------------
# Normalise TB response rows into LL-compatible field names
# ---------------------------------------------------------------------------

def _normalise_rows(rows: list) -> list:
    """
    Convert TB response rows into LL-compatible dicts so all existing agents
    can consume TB data without changes.

    TB field                → LL field
    ssp_revenue + profit    → GROSS_REVENUE  (total DSP spend)
    ssp_revenue             → PUB_PAYOUT     (publisher net payout)
    profit                  → PROFIT         (Teqblaze platform margin)
    impressions             → IMPRESSIONS
    ssp_wins        → WINS
    bid_requests    → BIDS
    avg_ssp_bid_floor → AVG_FLOOR_PRICE
    avg_ssp_bid_price → AVG_BID_PRICE
    ssp_ecpm        → GROSS_ECPM
    ssp_name        → PUBLISHER / PUBLISHER_NAME
    dsp_name        → DEMAND_PARTNER / DEMAND_PARTNER_NAME
    country         → COUNTRY_NAME
    date (Y-m-d)    → DATE
    """
    out = []
    for row in rows:
        pub_payout = float(row.get("ssp_revenue", 0) or 0)   # ssp_revenue = what publishers receive
        profit     = float(row.get("profit", 0) or 0)        # profit      = Teqblaze platform margin
        gross      = pub_payout + profit                      # GROSS = total DSP spend (what advertisers pay)

        norm = {
            # Revenue — DSP spend is the gross figure we track against targets
            "GROSS_REVENUE":   gross,       # total DSP spend = ssp_revenue + profit
            "PUB_PAYOUT":      pub_payout,  # publisher net   = ssp_revenue
            "PROFIT":          profit,      # platform margin = profit
            # Volume
            "IMPRESSIONS":     float(row.get("impressions", 0) or 0),
            "WINS":            float(row.get("ssp_wins", 0) or 0),
            "BIDS":            float(row.get("bid_requests", 0) or 0),
            # Pricing
            "GROSS_ECPM":      float(row.get("ssp_ecpm", 0) or 0),
            "AVG_FLOOR_PRICE": float(row.get("avg_ssp_bid_floor", 0) or 0),
            "AVG_BID_PRICE":   float(row.get("avg_ssp_bid_price", 0) or 0),
            # Breakdowns — map to LL-style keys so existing parsers work
            "DATE":            row.get("date", ""),
            "PUBLISHER":       row.get("ssp_name", ""),
            "PUBLISHER_NAME":  row.get("ssp_name", ""),
            "DEMAND_PARTNER":  row.get("dsp_name", ""),
            "DEMAND_PARTNER_NAME": row.get("dsp_name", ""),
            "COUNTRY_NAME":    row.get("country", ""),
        }
        # Preserve any original fields not mapped above
        for k, v in row.items():
            if k not in norm:
                norm[k] = v
        out.append(norm)
    return out


# ---------------------------------------------------------------------------
# Core fetch function
# ---------------------------------------------------------------------------

def fetch_tb(breakdown: str, metrics, start_date: str, end_date: str) -> list:
    """
    Fetch stats from the TB (Teqblaze) platform and return LL-compatible rows.

    Args:
        breakdown  (str):       LL-style breakdown constant, e.g. "DATE",
                                "PUBLISHER", "DEMAND_PARTNER", "COUNTRY_NAME",
                                "PUBLISHER,DEMAND_PARTNER"
        metrics    (list|str):  Ignored — all core metrics are always fetched
                                and normalised to LL-style names.
        start_date (str):       "YYYY-MM-DD"
        end_date   (str):       "YYYY-MM-DD"

    Returns:
        list[dict]: LL-compatible rows (GROSS_REVENUE, PUB_PAYOUT, etc.)

    Raises:
        ValueError: if TB credentials are not configured in .env
    """
    if not TB_EMAIL:
        raise ValueError(
            "TB_EMAIL not set in .env — "
            "add your ssp.pgammedia.com login email."
        )

    token = get_token()
    bd    = breakdown.upper().replace(" ", "")
    cfg   = _BREAKDOWN_MAP.get(bd, {"day_group": "day", "attribute": []})

    # Build query string manually (attribute[] needs repeated keys)
    params = [
        ("from",       start_date),
        ("to",         end_date),
        ("day_group",  cfg["day_group"]),
        ("limit",      1000),
    ]
    for attr in cfg["attribute"]:
        params.append(("attribute[]", attr))
    for m in _CORE_METRICS:
        params.append(("metric[]", m))

    def _do_request(tok: str) -> dict:
        url = f"{TB_BASE}/{tok}/adx-report?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "PGAM-Intelligence/1.0")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())

    # TB only allows one concurrent query per user — use a process-wide lock.
    # Also retry up to 3 times on 400 "One user have one query for one time" errors.
    with _TB_LOCK:
        last_err = None
        for attempt in range(3):
            try:
                raw = _do_request(token)
                # Check for application-level error responses
                if isinstance(raw, dict) and raw.get("status") is False:
                    err_msg = raw.get("errors", str(raw))
                    if "one query" in err_msg.lower() or "one user" in err_msg.lower():
                        # Rate limit — TB only allows one active query per user at a time.
                        # Wait progressively longer to let the server release its lock.
                        wait = 20 * (attempt + 1)   # 20s, 40s, 60s
                        time.sleep(wait)
                        last_err = RuntimeError(f"TB rate limit: {err_msg}")
                        continue
                    raise RuntimeError(f"TB API error: {err_msg}")
                break  # success
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    # Token expired — clear cache and refresh
                    if os.path.exists(TOKEN_CACHE):
                        os.remove(TOKEN_CACHE)
                    token = get_token()
                    last_err = e
                    continue
                elif e.code == 400:
                    # Could be rate limit or bad request — wait and retry
                    body = e.read().decode(errors="replace")
                    if "one query" in body.lower() or "one user" in body.lower():
                        wait = 20 * (attempt + 1)   # 20s, 40s, 60s
                        time.sleep(wait)
                        last_err = RuntimeError(f"TB rate limit (HTTP 400): {body}")
                        continue
                    raise
                raise
        else:
            raise last_err or RuntimeError("TB API fetch failed after 3 attempts")

    # TB returns either a list or {"data": [...], "totals": ..., ...}
    rows = raw if isinstance(raw, list) else raw.get("data", raw.get("rows", []))
    return _normalise_rows(rows)


def tb_configured() -> bool:
    """Return True if TB credentials are present in the environment."""
    return bool(TB_EMAIL and TB_PASSWORD)
