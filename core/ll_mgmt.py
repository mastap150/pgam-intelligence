"""
core/ll_mgmt.py

Limelight management API client.
Base URL: https://ui.pgamrtb.com

Authentication
--------------
Token-based. Steps:
  1. POST /v1/login  { name: <email>, password: <password> }  → header + body contain token
  2. All subsequent calls:  token: <value>  header

Tokens expire in 24 hours. The token is cached in /tmp/pgam_ll_mgmt_token.json.
Auto-refreshes on 401 or when the cached token is within 5 minutes of expiry.

Dry-run mode
------------
All write operations accept dry_run=True to log the intended action without
calling the API.  If the env var LL_DRY_RUN=true is set, ALL writes are
dry-run regardless of the parameter value passed by the caller.

Audit logging
-------------
Every write operation (real or dry-run) emits a structured line to stdout:
  [ll_mgmt] ACTION  key=value ...
  [ll_mgmt] DRY_RUN action  key=value ...

Status codes (confirmed from API)
----------------------------------
  1 = active / enabled
  2 = paused / disabled
"""

import json
import os
import time
import threading
import argparse

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------------------------------------------------------------
# Module-level config
# ---------------------------------------------------------------------------

LL_MGMT_BASE   = "https://ui.pgamrtb.com"
LL_UI_EMAIL    = os.environ.get("LL_UI_EMAIL", "")
LL_UI_PASSWORD = os.environ.get("LL_UI_PASSWORD", "")
TOKEN_CACHE    = "/tmp/pgam_ll_mgmt_token.json"

# ---------------------------------------------------------------------------
# Contractual floor minimums
# ---------------------------------------------------------------------------
# Clamp applied at the bottom of set_demand_floor() so ANY caller (portfolio
# optimizer, per-tuple optimizer, dayparting, manual scripts) that tries to
# drop a protected floor below its contract minimum is silently clamped back
# up. Higher floors are allowed — this is a minimum, not a target.
#
# History: 2026-04-18 the portfolio optimizer autonomously dropped 9 Dots
# demands 692/693/955 from ~$1.80 to $0.00, contributing to a 16 % WoW
# Saturday revenue drop. Restored manually 2026-04-19.
PROTECTED_FLOOR_MINIMUMS: list[tuple[tuple[str, ...], float]] = [
    # Name tokens (any match, case-insensitive), min floor
    (("9 dots", "9dots"), 1.70),
]

# Global dry-run override: if LL_DRY_RUN=true, all writes become no-ops
_GLOBAL_DRY_RUN = os.environ.get("LL_DRY_RUN", "").lower() in ("1", "true", "yes")

# Thread-safety lock for token refresh
_TOKEN_LOCK = threading.Lock()


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
        token      = data.get("token", "")
        expires_at = data.get("expires_at", 0)
        if token and expires_at > time.time() + 300:
            return token
    except (json.JSONDecodeError, OSError):
        pass
    return ""


def _save_token(token: str, expires_at: float):
    try:
        with open(TOKEN_CACHE, "w") as f:
            json.dump({"token": token, "expires_at": expires_at}, f)
    except OSError:
        pass


def _create_token() -> str:
    """POST /v1/login — returns a fresh token valid for 24 hours."""
    if not LL_UI_EMAIL or not LL_UI_PASSWORD:
        raise ValueError(
            "Limelight management credentials not configured. "
            "Add LL_UI_EMAIL and LL_UI_PASSWORD to .env "
            "(your ui.pgamrtb.com login email and password)."
        )

    resp = requests.post(
        f"{LL_MGMT_BASE}/v1/login",
        json={"name": LL_UI_EMAIL, "password": LL_UI_PASSWORD},
        timeout=30,
    )

    if not resp.ok:
        raise RuntimeError(
            f"LL management login failed: HTTP {resp.status_code} — {resp.text}"
        )

    # Token may be in response body or in the response headers
    body  = resp.json() if resp.text else {}
    token = body.get("token") or resp.headers.get("token", "")

    if not token:
        raise RuntimeError(
            f"LL management login: no token in response. body={body!r}"
        )

    # 24-hour lifetime
    expires_at = time.time() + 86400
    _save_token(token, expires_at)
    return token


def get_token() -> str:
    """Return a valid LL management token, refreshing if expired or missing."""
    with _TOKEN_LOCK:
        token = _load_cached_token()
        if token:
            return token
        return _create_token()


# ---------------------------------------------------------------------------
# Internal HTTP helpers
# ---------------------------------------------------------------------------

def _headers(token: str) -> dict:
    return {"token": token, "Content-Type": "application/json"}


def _unwrap(raw: dict | list):
    """Unwrap the standard {status, body} envelope the API wraps all responses in."""
    if isinstance(raw, dict):
        if raw.get("status") == "SUCCESS":
            return raw.get("body", raw)
        if raw.get("status") == "FAILED":
            raise RuntimeError(f"LL mgmt API error: {raw.get('body', raw)}")
    return raw


def _get(path: str) -> dict | list:
    """
    Perform an authenticated GET request.
    Auto-refreshes the token once on 401.
    """
    token = get_token()

    for attempt in range(2):
        resp = requests.get(
            f"{LL_MGMT_BASE}{path}",
            headers=_headers(token),
            timeout=30,
        )

        if resp.status_code == 401 and attempt == 0:
            # Token was rejected — clear cache and retry with a fresh one
            with _TOKEN_LOCK:
                if os.path.exists(TOKEN_CACHE):
                    os.remove(TOKEN_CACHE)
                token = _create_token()
            continue

        if not resp.ok:
            raise RuntimeError(
                f"LL mgmt GET {path} failed: HTTP {resp.status_code} — {resp.text}"
            )

        return _unwrap(resp.json())

    raise RuntimeError(f"LL mgmt GET {path} failed after token refresh")


def _put(path: str, payload: dict) -> dict | list:
    """
    Perform an authenticated PUT request.
    Auto-refreshes the token once on 401.
    """
    token = get_token()

    for attempt in range(2):
        resp = requests.put(
            f"{LL_MGMT_BASE}{path}",
            headers=_headers(token),
            json=payload,
            timeout=30,
        )

        if resp.status_code == 401 and attempt == 0:
            with _TOKEN_LOCK:
                if os.path.exists(TOKEN_CACHE):
                    os.remove(TOKEN_CACHE)
                token = _create_token()
            continue

        if not resp.ok:
            raise RuntimeError(
                f"LL mgmt PUT {path} failed: HTTP {resp.status_code} — {resp.text}"
            )

        return _unwrap(resp.json())

    raise RuntimeError(f"LL mgmt PUT {path} failed after token refresh")


# ---------------------------------------------------------------------------
# Publishers
# ---------------------------------------------------------------------------

def get_publishers(include_archived: bool = False) -> list[dict]:
    """
    GET /v1/publishers — returns a list of publisher objects.

    Args:
        include_archived: If False (default), filters out publishers whose
                          status is not 1 (active) or 2 (paused).
    """
    data = _get("/v1/publishers")
    publishers = data if isinstance(data, list) else []

    if not include_archived:
        publishers = [p for p in publishers if p.get("status") in (1, 2)]

    return publishers


def get_publisher(publisher_id: int) -> dict:
    """GET /v1/publishers/{id} — returns a single publisher object."""
    data = _get(f"/v1/publishers/{publisher_id}")
    return data if isinstance(data, dict) else {}


def get_publisher_by_name(name: str) -> dict | None:
    """
    Case-insensitive partial match on publisher name.
    Returns the first matching publisher, or None.
    """
    needle = name.lower()
    for pub in get_publishers(include_archived=True):
        if needle in pub.get("name", "").lower():
            return pub
    return None


# ---------------------------------------------------------------------------
# Ad Units / Floors
# ---------------------------------------------------------------------------

def get_adunits(publisher_id: int) -> list[dict]:
    """GET /v1/adunits?publisher={id} — returns ad units for a publisher."""
    data = _get(f"/v1/adunits?publisher={publisher_id}")
    return data if isinstance(data, list) else []


def update_floor(adunit_id: int, new_floor: float, dry_run: bool = False) -> dict:
    """
    Update the bidFloor on an ad unit.

    Fetches the current ad unit, modifies only bidFloor, then PUTs the full
    object back (the API requires the complete object).

    Args:
        adunit_id: The ad unit ID.
        new_floor:  The new bid floor value.
        dry_run:    If True (or LL_DRY_RUN=true), logs the action but does NOT
                    call the API.

    Returns:
        The updated ad unit dict (or the current ad unit dict on dry-run).
    """
    effective_dry_run = dry_run or _GLOBAL_DRY_RUN

    # Always fetch the current state so we can log the old value
    adunit    = _get(f"/v1/adunits/{adunit_id}")
    old_floor = adunit.get("bidFloor")

    if effective_dry_run:
        print(
            f"[ll_mgmt] DRY_RUN update_floor  "
            f"adunit_id={adunit_id}  floor={old_floor}→{new_floor}"
        )
        return adunit

    print(
        f"[ll_mgmt] update_floor  "
        f"adunit_id={adunit_id}  floor={old_floor}→{new_floor}"
    )

    adunit["bidFloor"] = new_floor
    return _put(f"/v1/adunits/{adunit_id}", adunit)


# ---------------------------------------------------------------------------
# Demands
# ---------------------------------------------------------------------------

def set_demand_floor(
    demand_id: int,
    new_floor: float | None,
    *,
    verify: bool = True,
    dry_run: bool = False,
    allow_multi_pub: bool = False,
    _publishers_running_it: int | None = None,
) -> dict:
    """Canonical floor-write path for a demand partner.

    BACKGROUND (2026-04-18 verifier investigation)
    ----------------------------------------------
    PUT /v1/publishers/{id} with modified biddingpreferences[].value[].minBidFloor
    returns 200 OK but SILENTLY DISCARDS the nested change. Every phase1*/
    startio_*/high_wr_* script that wrote via that path for months was a no-op.
    The 221/221 "reverted" verdict in the April-18 verifier report was not a
    revert — the PUT never landed in the first place.

    The working endpoint is PUT /v1/demands/{demand_id}. That sticks, and a
    re-GET confirms the new minBidFloor. Note this is a **demand-global**
    floor: it applies to every publisher that has this demand wired in.

    MULTI-PUB SAFETY
    ----------------
    73% of demand_ids run on only one publisher, so demand-level = per-pub
    in practice. The remaining 27% (67 demands) span multiple pubs — setting
    a floor there changes it on all of them simultaneously. By default this
    function refuses to write those unless the caller passes
    ``allow_multi_pub=True`` AND has aggregated their per-pub recommendations
    into a single demand-level decision.

    VERIFICATION
    ------------
    If ``verify=True`` (default), re-GETs the demand after the write and
    raises RuntimeError if the live value doesn't match. No more silent
    failures.
    """
    if _publishers_running_it is not None and _publishers_running_it > 1 and not allow_multi_pub:
        raise ValueError(
            f"demand_id={demand_id} runs on {_publishers_running_it} publishers — "
            "demand-level floor write would change all of them. Pass "
            "allow_multi_pub=True after aggregating per-pub recommendations."
        )

    # Fetch demand (needed for the name-based contract clamp below, even in
    # dry-run — we want dry-run to reflect the final clamped value not the raw
    # requested one).
    demand = _get(f"/v1/demands/{demand_id}")
    old_floor = demand.get("minBidFloor")

    # Enforce contractual floor minimums (e.g. 9 Dots @ $1.70). Raises a
    # too-low request up to the minimum — never lowers. This is the last line
    # of defense: catches any caller (portfolio optimizer, dayparting, manual
    # scripts) that tries to drop a protected floor below its contract.
    name_lower = (demand.get("name") or "").lower()
    for tokens, min_floor in PROTECTED_FLOOR_MINIMUMS:
        if any(tok in name_lower for tok in tokens):
            if new_floor is None or float(new_floor) < min_floor:
                print(
                    f"[ll_mgmt] protected floor clamp: demand_id={demand_id} "
                    f"name={demand.get('name')!r} requested={new_floor} "
                    f"→ clamped to {min_floor} (contract minimum)"
                )
                new_floor = min_floor
            break

    if dry_run or _GLOBAL_DRY_RUN:
        print(f"[ll_mgmt] DRY_RUN set_demand_floor demand_id={demand_id} "
              f"floor={old_floor}→{new_floor}")
        return {"dry_run": True, "demand_id": demand_id, "new_floor": new_floor,
                "old_floor": old_floor}

    if old_floor == new_floor:
        return {"no_change": True, "demand_id": demand_id, "floor": new_floor}

    demand["minBidFloor"] = new_floor
    resp = _put(f"/v1/demands/{demand_id}", demand)

    result = {
        "demand_id": demand_id,
        "old_floor": old_floor,
        "new_floor": new_floor,
        "put_response_floor": resp.get("minBidFloor") if isinstance(resp, dict) else None,
    }

    if verify:
        import time as _time
        _time.sleep(1)
        live = _get(f"/v1/demands/{demand_id}").get("minBidFloor")
        result["live_floor_after"] = live
        if (live is None and new_floor is not None) or (
            live is not None and new_floor is not None
            and abs(float(live) - float(new_floor)) > 0.001
        ):
            raise RuntimeError(
                f"set_demand_floor verification FAILED: demand_id={demand_id} "
                f"expected={new_floor} live={live}"
            )
        result["verified"] = True

    return result


def get_demands(include_archived: bool = False) -> list[dict]:
    """
    GET /v1/demands — returns a list of demand objects.

    Args:
        include_archived: If False (default), filters out demands whose
                          status is not 1 (active) or 2 (paused).
    """
    data = _get("/v1/demands")
    demands = data if isinstance(data, list) else []

    if not include_archived:
        demands = [d for d in demands if d.get("status") in (1, 2)]

    return demands


def get_demand_by_name(name: str) -> dict | None:
    """
    Case-insensitive partial match on demand name.
    Returns the first matching demand, or None.
    """
    needle = name.lower()
    for demand in get_demands(include_archived=True):
        if needle in demand.get("name", "").lower():
            return demand
    return None


# ---------------------------------------------------------------------------
# Publisher demand assignments (biddingpreferences)
# ---------------------------------------------------------------------------

def get_publisher_demands(publisher_id: int) -> list[dict]:
    """
    Returns a flat list of demand objects from publisher.biddingpreferences[].value.

    Each biddingpreference has the shape:
      { "type": 3, "ruleType": 1, "value": [<demand objects>] }

    All demand objects across all biddingpreferences are merged into one list.
    """
    publisher = get_publisher(publisher_id)
    demands: list[dict] = []
    for pref in publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            demands.append(item)
    return demands


def _set_publisher_demand_status(
    publisher_id: int,
    demand_id: int,
    new_status: int,
    dry_run: bool = False,
) -> dict:
    """
    Internal helper: set status on a demand entry within publisher.biddingpreferences.

    Fetches the full publisher, walks biddingpreferences looking for the demand
    by id, updates its status in-memory, then PUTs the full publisher object back.
    """
    effective_dry_run = dry_run or _GLOBAL_DRY_RUN

    publisher = get_publisher(publisher_id)
    action_label = "enable_publisher_demand" if new_status == 1 else "disable_publisher_demand"
    status_label = "enabled (1)" if new_status == 1 else "disabled (2)"

    found = False
    for pref in publisher.get("biddingpreferences", []):
        for item in pref.get("value", []):
            if item.get("id") == demand_id:
                old_status = item.get("status")
                if effective_dry_run:
                    print(
                        f"[ll_mgmt] DRY_RUN {action_label}  "
                        f"publisher_id={publisher_id}  demand_id={demand_id}  "
                        f"status={old_status}→{new_status}"
                    )
                    return publisher
                print(
                    f"[ll_mgmt] {action_label}  "
                    f"publisher_id={publisher_id}  demand_id={demand_id}  "
                    f"status={old_status}→{new_status}"
                )
                item["status"] = new_status
                found = True

    if not found:
        raise ValueError(
            f"demand_id={demand_id} not found in biddingpreferences "
            f"for publisher_id={publisher_id}"
        )

    return _put(f"/v1/publishers/{publisher_id}", publisher)


def enable_publisher_demand(
    publisher_id: int, demand_id: int, dry_run: bool = False
) -> dict:
    """
    Set status=1 (active/enabled) on the demand within publisher biddingpreferences.

    Args:
        publisher_id: The publisher ID.
        demand_id:    The demand ID to enable.
        dry_run:      If True (or LL_DRY_RUN=true), logs but does NOT call the API.

    Returns:
        The updated publisher dict (or the current publisher dict on dry-run).
    """
    return _set_publisher_demand_status(publisher_id, demand_id, 1, dry_run=dry_run)


def disable_publisher_demand(
    publisher_id: int, demand_id: int, dry_run: bool = False
) -> dict:
    """
    Set status=2 (paused/disabled) on the demand within publisher biddingpreferences.

    Args:
        publisher_id: The publisher ID.
        demand_id:    The demand ID to disable.
        dry_run:      If True (or LL_DRY_RUN=true), logs but does NOT call the API.

    Returns:
        The updated publisher dict (or the current publisher dict on dry-run).
    """
    return _set_publisher_demand_status(publisher_id, demand_id, 2, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Bulk helpers
# ---------------------------------------------------------------------------

def get_all_publisher_adunits() -> dict[int, list]:
    """
    Returns {publisher_id: [adunit, ...]} for all active/paused publishers.

    Iterates over all publishers and fetches their ad units. Publishers with
    no ad units are included with an empty list.
    """
    result: dict[int, list] = {}
    for pub in get_publishers():
        pub_id           = pub["id"]
        result[pub_id]   = get_adunits(pub_id)
    return result


def build_publisher_name_map() -> dict[str, dict]:
    """
    Returns {publisher_name_lower: publisher_dict} for fast lookups.

    All publisher names are lowercased. If two publishers share a
    lowercased name the last one wins (names should be unique in practice).
    """
    return {
        pub.get("name", "").lower(): pub
        for pub in get_publishers(include_archived=True)
    }


def build_demand_name_map() -> dict[str, dict]:
    """
    Returns {demand_name_lower: demand_dict} for fast lookups.

    All demand names are lowercased. If two demands share a lowercased name
    the last one wins.
    """
    return {
        d.get("name", "").lower(): d
        for d in get_demands(include_archived=True)
    }


# ---------------------------------------------------------------------------
# Status check
# ---------------------------------------------------------------------------

def ll_mgmt_configured() -> bool:
    """Returns True if LL_UI_EMAIL and LL_UI_PASSWORD are set."""
    return bool(LL_UI_EMAIL and LL_UI_PASSWORD)


# ---------------------------------------------------------------------------
# __main__ smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LL management API smoke test"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Enable dry-run mode (no writes will be executed)",
    )
    args = parser.parse_args()

    if args.dry_run:
        os.environ["LL_DRY_RUN"] = "true"
        # Re-evaluate the global flag so it takes effect immediately
        import core.ll_mgmt as _self
        _self._GLOBAL_DRY_RUN = True
        print("[ll_mgmt] Dry-run mode ENABLED — no writes will be executed\n")

    if not ll_mgmt_configured():
        print(
            "ERROR: LL_UI_EMAIL and/or LL_UI_PASSWORD not set in environment/.env\n"
            "Add them to .env and re-run."
        )
        raise SystemExit(1)

    print("=== LL Management API smoke test ===\n")

    # 1. List publishers
    publishers = get_publishers()
    print(f"Publishers found: {len(publishers)}")

    # Top 5 by name (alphabetical)
    top5 = sorted(publishers, key=lambda p: p.get("name", "").lower())[:5]
    print("\nTop 5 publishers by name:")
    for pub in top5:
        print(
            f"  id={pub['id']:>6}  status={pub.get('status')}  "
            f"name={pub.get('name', '(no name)')}"
        )

    # 2. Count ad units across all publishers (fetch for all, show total)
    print("\nFetching ad units for all publishers …")
    pub_adunits = get_all_publisher_adunits()
    total_adunits = sum(len(v) for v in pub_adunits.values())
    print(f"Total ad units: {total_adunits} across {len(pub_adunits)} publishers")

    # 3. List demands
    demands = get_demands()
    print(f"\nDemands found: {len(demands)}")
    top5_demands = sorted(demands, key=lambda d: d.get("name", "").lower())[:5]
    print("Top 5 demands by name:")
    for d in top5_demands:
        print(
            f"  id={d['id']:>6}  status={d.get('status')}  "
            f"name={d.get('name', '(no name)')}"
        )

    print("\nSmoke test complete.")
