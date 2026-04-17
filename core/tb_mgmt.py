"""
core/tb_mgmt.py
~~~~~~~~~~~~~~~
Teqblaze (TB) Inventory Management API client.
Base URL: https://ssp.pgammedia.com/api/{token}/

Authentication
--------------
Uses the SAME token as the stats API (core/tb_api.py).
Token endpoint:  POST https://ssp.pgammedia.com/api/create_token
All requests:    https://ssp.pgammedia.com/api/{token}/{endpoint}
Body encoding:   application/x-www-form-urlencoded (not JSON)

This module delegates token management to core.tb_api.get_token() so the
two share the same cached token at /tmp/pgam_tb_token.json.

Capabilities
------------
- list_inventories()           → list all TB inventories (sites/apps)
- list_placements(inventory_id)→ all placements for an inventory
- get_placement(placement_id)  → single placement detail
- set_floor(placement_id, price, price_country, is_optimal_price)
    → update floor on video / banner / native placement
- disable_placement(placement_id)  → status=false
- enable_placement(placement_id)   → status=true
- report(...)                  → TB stats with placement/country breakdown
- test_connection()            → diagnostic health check

Floor geo-targeting
-------------------
price_country accepts a list of dicts:
    [{"code": "USA", "price": 2.50}, {"code": "GBR", "price": 1.80}]
Country codes are ISO 3166-1 alpha-3 (3-letter).

Usage
-----
    import core.tb_mgmt as tbm

    # See all inventories
    inventories = tbm.list_inventories()

    # See placements for an inventory
    placements = tbm.list_placements(inventory_id=1001)

    # Set a flat floor
    tbm.set_floor(placement_id=2001, price=0.50)

    # Set geo-specific floors
    tbm.set_floor(
        placement_id=2001,
        price=0.30,                          # base / ROW floor
        price_country=[
            {"code": "USA", "price": 1.00},
            {"code": "GBR", "price": 0.80},
            {"code": "CAN", "price": 0.75},
        ],
    )

    # Enable TB dynamic floor optimisation
    tbm.set_floor(placement_id=2001, is_optimal_price=True)
"""

import json
import os
import time
import urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

TB_BASE = "https://ssp.pgammedia.com/api"

# Default PGAM user id — TB requires userId for list_inventory / list_placement.
# Override via env var TB_USER_ID (e.g. 34 for Dexerto).
DEFAULT_USER_ID = os.getenv("TB_USER_ID", "45")

# ---------------------------------------------------------------------------
# Token  — delegate to tb_api so both modules share one cached token
# ---------------------------------------------------------------------------

def _get_token() -> str:
    """Return a valid TB token.  Shares cache with core.tb_api."""
    # Import lazily to avoid circular imports
    from core.tb_api import get_token
    return get_token()


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _url(endpoint: str) -> str:
    return f"{TB_BASE}/{_get_token()}/{endpoint}"


def _get(endpoint: str, params: dict | None = None) -> list | dict:
    """Authenticated GET — token lives in the URL path."""
    resp = requests.get(_url(endpoint), params=params or {}, timeout=30)
    if not resp.ok:
        raise RuntimeError(
            f"TB mgmt GET {endpoint} failed: HTTP {resp.status_code} — {resp.text[:300]}"
        )
    # TB returns 204 No Content when a required scoping param (e.g. userId) is missing.
    if resp.status_code == 204 or not resp.content:
        return []
    data = resp.json()
    # TB sometimes returns {"status": false, "errors": "..."}
    if isinstance(data, dict) and data.get("status") is False:
        raise RuntimeError(f"TB mgmt GET {endpoint} error: {data.get('errors', data)}")
    return data


def _post(endpoint: str, payload: dict) -> dict:
    """Authenticated form-encoded POST — token lives in the URL path."""
    # price_country must be serialised as a JSON string in form body
    form = {}
    for k, v in payload.items():
        if isinstance(v, (list, dict)):
            form[k] = json.dumps(v)
        elif isinstance(v, bool):
            form[k] = "true" if v else "false"
        else:
            form[k] = v

    resp = requests.post(
        _url(endpoint),
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(
            f"TB mgmt POST {endpoint} failed: HTTP {resp.status_code} — {resp.text[:300]}"
        )
    data = resp.json()
    if isinstance(data, dict) and data.get("status") is False:
        raise RuntimeError(f"TB mgmt POST {endpoint} error: {data.get('errors', data)}")
    return data


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def list_inventories(user_id: int | str | None = None) -> list[dict]:
    """
    Return all TB inventories registered on the platform.

    Each inventory represents a site or app.  Contains inventory_id, title,
    platform (web/android/ios/ctv), address (domain or bundle), status, etc.

    NOTE: The TB API uses userId as a URL path segment for list_inventory:
          GET /api/{token}/list_inventory/{userId}
    Common user IDs:
        45 = PGAM Media
        34 = Dexerto
    """
    uid = user_id if user_id is not None else DEFAULT_USER_ID
    # userId is a PATH segment for list_inventory (not a query param)
    endpoint = f"list_inventory/{uid}"
    result = _get(endpoint)
    return result if isinstance(result, list) else []


def get_inventory(inventory_id: int | str) -> dict:
    """Return details for a single inventory."""
    return _get("inventory", {"inventory_id": inventory_id})


# ---------------------------------------------------------------------------
# Placements
# ---------------------------------------------------------------------------

def list_placements(inventory_id: int | str | None = None,
                    user_id: str | None = None) -> list[dict]:
    """
    Return all placements (ad units) for an inventory, or all placements on
    the account if inventory_id is omitted.

    Each placement contains:
        placement_id, inventory_id, title, type (banner/video/native),
        status (bool), price (floor CPM), price_country (list),
        is_optimal_price (bool)
    """
    params = {"userId": user_id or DEFAULT_USER_ID}
    if inventory_id is not None:
        params["inventory_id"] = inventory_id
    result = _get("list_placement", params)
    return result if isinstance(result, list) else []


def get_placement(placement_id: int | str) -> dict:
    """Return full details for a single placement."""
    return _get("placement", {"placement_id": placement_id})


# ---------------------------------------------------------------------------
# Floor management
# ---------------------------------------------------------------------------

def set_floor(
    placement_id: int | str,
    price: float | None = None,
    price_country: list[dict] | None = None,
    is_optimal_price: bool | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Set bid floor on a TB placement.

    Automatically detects placement type (banner / video / native) and
    calls the correct edit endpoint.

    Parameters
    ----------
    placement_id     : int | str
        TB placement ID (from list_placements())
    price            : float, optional
        Global floor CPM in USD.  0 to 10.  Pass None to leave unchanged.
    price_country    : list[dict], optional
        Geo-specific floors.  E.g.:
            [{"code": "USA", "price": 2.50}, {"code": "GBR", "price": 1.80}]
        Replaces the entire existing price_country array.
        Pass [] to clear all geo-specific floors.
    is_optimal_price : bool, optional
        True → enable TB's dynamic floor optimisation for this placement.
        False → disable it.  None → leave unchanged.
    dry_run          : bool
        If True, print what would be changed without making any API call.

    Returns
    -------
    dict with placement data (or dry_run summary)
    """
    # Fetch current placement to know its type
    placement = get_placement(placement_id)
    ptype     = placement.get("type", "video").lower()   # banner / video / native

    current_price   = placement.get("price", 0)
    current_geo     = placement.get("price_country", [])
    current_optimal = placement.get("is_optimal_price", False)

    effective_price   = price           if price           is not None else current_price
    effective_geo     = price_country   if price_country   is not None else current_geo
    effective_optimal = is_optimal_price if is_optimal_price is not None else current_optimal

    if dry_run:
        print(
            f"[tb_mgmt] DRY_RUN  placement_id={placement_id}  type={ptype}"
            f"  floor ${current_price:.4f} → ${effective_price:.4f}"
            f"  geo_floors={len(effective_geo)}  optimal={effective_optimal}"
        )
        return {
            "placement_id": placement_id,
            "type": ptype,
            "old_price": current_price,
            "new_price": effective_price,
            "price_country": effective_geo,
            "is_optimal_price": effective_optimal,
            "applied": False,
            "dry_run": True,
        }

    endpoint_map = {
        "video":  "edit_placement_video",
        "banner": "edit_placement_banner",
        "native": "edit_placement_native",
    }
    endpoint = endpoint_map.get(ptype, "edit_placement_video")

    payload: dict = {"placement_id": placement_id}
    if price is not None:
        payload["price"] = price
    if price_country is not None:
        payload["price_country"] = price_country
    if is_optimal_price is not None:
        payload["is_optimal_price"] = is_optimal_price

    result = _post(endpoint, payload)
    new_price = result.get("price", effective_price)

    print(
        f"[tb_mgmt] set_floor  placement_id={placement_id}  type={ptype}"
        f"  ${current_price:.4f} → ${new_price:.4f}"
        f"  geo_floors={len(result.get('price_country', []))}"
        f"  optimal={result.get('is_optimal_price', effective_optimal)}  ✓"
    )
    return {
        "placement_id": placement_id,
        "type": ptype,
        "old_price": current_price,
        "new_price": new_price,
        "price_country": result.get("price_country", []),
        "is_optimal_price": result.get("is_optimal_price", effective_optimal),
        "applied": True,
        "result": result,
    }


def bulk_set_floors(
    floor_map: dict[int | str, float],
    price_country: list[dict] | None = None,
    dry_run: bool = True,
) -> list[dict]:
    """
    Set floors on multiple placements at once.

    Parameters
    ----------
    floor_map    : {placement_id: floor_price}
    price_country: optional geo-specific floors applied to ALL placements
    dry_run      : bool (default True for safety)

    Returns
    -------
    list of results from set_floor()
    """
    results = []
    for pid, floor in floor_map.items():
        try:
            r = set_floor(pid, price=floor, price_country=price_country, dry_run=dry_run)
            results.append(r)
        except Exception as e:
            print(f"[tb_mgmt] ✗  placement_id={pid}  error: {e}")
            results.append({"placement_id": pid, "applied": False, "error": str(e)})
    return results


# ---------------------------------------------------------------------------
# Status management
# ---------------------------------------------------------------------------

def set_placement_status(
    placement_id: int | str,
    active: bool,
    dry_run: bool = False,
) -> dict:
    """Enable (active=True) or disable (active=False) a placement."""
    placement = get_placement(placement_id)
    ptype     = placement.get("type", "video").lower()

    if dry_run:
        action = "ENABLE" if active else "DISABLE"
        print(f"[tb_mgmt] DRY_RUN {action}  placement_id={placement_id}  type={ptype}")
        return {"placement_id": placement_id, "active": active, "applied": False, "dry_run": True}

    endpoint_map = {
        "video":  "edit_placement_video",
        "banner": "edit_placement_banner",
        "native": "edit_placement_native",
    }
    endpoint = endpoint_map.get(ptype, "edit_placement_video")

    result = _post(endpoint, {"placement_id": placement_id, "status": active})
    action = "enabled" if active else "disabled"
    print(f"[tb_mgmt] {action}  placement_id={placement_id}  type={ptype}  ✓")
    return {"placement_id": placement_id, "active": active, "applied": True, "result": result}


def enable_placement(placement_id: int | str, dry_run: bool = False) -> dict:
    return set_placement_status(placement_id, active=True, dry_run=dry_run)


def disable_placement(placement_id: int | str, dry_run: bool = False) -> dict:
    return set_placement_status(placement_id, active=False, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def report(
    start_date: str,
    end_date: str,
    attributes: list[str] | None = None,
    day_group: str = "day",
    placement_ids: list[int] | None = None,
    country_codes: list[str] | None = None,
    limit: int = 1000,
) -> list[dict]:
    """
    Pull a stats report from TB management API.

    Parameters
    ----------
    start_date    : "YYYY-MM-DD"
    end_date      : "YYYY-MM-DD"
    attributes    : list of grouping attributes, e.g.
                    ["placement", "country", "ad_format"]
                    Full list: placement, country, traffic, ad_format,
                    inventory, domain, company_dsp, publisher,
                    placement_name, size, user
    day_group     : "day" | "hour" | "month" | "total"
    placement_ids : filter to specific placement IDs
    country_codes : filter to specific country codes (alpha-3)
    limit         : rows per page (max 1000)

    Returns
    -------
    list[dict] — raw response rows
    """
    params = [
        ("from",      start_date),
        ("to",        end_date),
        ("day_group", day_group),
        ("limit",     limit),
    ]
    for attr in (attributes or []):
        params.append(("attribute[]", attr))
    for pid in (placement_ids or []):
        params.append(("filter[placement][]", pid))
    for cc in (country_codes or []):
        params.append(("filter[country][]", cc))

    token = _get_token()
    url   = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
    resp  = requests.get(url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"TB mgmt report failed: HTTP {resp.status_code} — {resp.text[:300]}")
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


# ---------------------------------------------------------------------------
# Diagnostic
# ---------------------------------------------------------------------------

def test_connection(verbose: bool = True) -> bool:
    """
    Test whether the TB management API credentials and endpoints work.
    Returns True on success.
    """
    try:
        token = _get_token()
        if verbose:
            print(f"✅ Token obtained: {token[:16]}...")
    except Exception as e:
        print(f"❌ Token creation failed: {e}")
        return False

    try:
        inventories = list_inventories()
        if verbose:
            print(f"✅ list_inventory: {len(inventories)} inventories")
            for inv in inventories[:5]:
                print(f"   [{inv.get('inventory_id')}] {inv.get('title')}  "
                      f"platform={inv.get('platform')}  status={inv.get('status')}")
            if len(inventories) > 5:
                print(f"   ... and {len(inventories) - 5} more")
    except Exception as e:
        print(f"❌ list_inventory failed: {e}")
        return False

    try:
        all_placements = list_placements()
        if verbose:
            print(f"\n✅ list_placement (all): {len(all_placements)} placements")
            for p in all_placements[:10]:
                geo_count = len(p.get("price_country") or [])
                print(
                    f"   [{p.get('placement_id')}] {p.get('title'):<35}  "
                    f"type={p.get('type'):<7}  "
                    f"floor=${p.get('price', 0):.2f}  "
                    f"geo_floors={geo_count}  "
                    f"optimal={p.get('is_optimal_price')}  "
                    f"inv={p.get('inventory_id')}"
                )
            if len(all_placements) > 10:
                print(f"   ... and {len(all_placements) - 10} more")
    except Exception as e:
        print(f"❌ list_placement failed: {e}")
        return False

    try:
        partners = list_partners()
        if verbose:
            print(f"\n✅ reference_dsp_list: {len(partners)} ad-exchange partners")
    except Exception as e:
        print(f"❌ reference_dsp_list failed: {e}")
        return False

    try:
        if inventories:
            sample_inv = inventories[0]["inventory_id"]
            parts = get_inventory_partners(sample_inv)
            if verbose:
                print(
                    f"✅ inventory {sample_inv} partners: "
                    f"{len(parts['whitelist_ids'])} whitelisted, "
                    f"{len(parts['blacklist_ids'])} blacklisted"
                )
                for p in parts["whitelist"][:5]:
                    print(f"   [{p['id']}] {p['name']}")
    except Exception as e:
        print(f"❌ get_inventory_partners failed: {e}")
        return False

    return True


def dump_placements(output_file: str | None = None) -> list[dict]:
    """
    Fetch all inventories + placements and print a structured summary.
    Optionally write to a JSON file for offline analysis.

    Returns list of all placement dicts.
    """
    inventories  = list_inventories()
    all_placements = list_placements()

    # Index placements by inventory
    inv_idx: dict[int, list] = {}
    for p in all_placements:
        inv_id = p.get("inventory_id")
        inv_idx.setdefault(inv_id, []).append(p)

    print(f"\n{'='*70}")
    print(f"TB Inventory + Placement Map  ({len(inventories)} inventories, "
          f"{len(all_placements)} placements)")
    print(f"{'='*70}")
    for inv in inventories:
        inv_id = inv.get("inventory_id")
        placements = inv_idx.get(inv_id, [])
        print(f"\n[{inv_id}] {inv.get('title')}  "
              f"({inv.get('platform')})  addr={inv.get('address')}  "
              f"status={inv.get('status')}")
        if not placements:
            print("    (no placements)")
        for p in placements:
            geo   = p.get("price_country") or []
            geo_s = f"  geo=[{', '.join(g['code'] for g in geo[:3])}{'...' if len(geo)>3 else ''}]" if geo else ""
            print(
                f"    [{p.get('placement_id')}] {p.get('title'):<35}  "
                f"type={p.get('type'):<7}  "
                f"floor=${p.get('price', 0):.2f}  "
                f"optimal={p.get('is_optimal_price')}"
                f"{geo_s}"
            )

    if output_file:
        with open(output_file, "w") as f:
            json.dump({"inventories": inventories, "placements": all_placements}, f, indent=2)
        print(f"\n✅ Written to {output_file}")

    return all_placements


# ---------------------------------------------------------------------------
# Ad-exchange partners (DSP whitelist/blacklist per inventory)
# ---------------------------------------------------------------------------
#
# The TB UI's /ad-exchange/ page is backed by per-inventory DSP lists.
# Each inventory has `inventory_dsp[white]` (allowed partners) and
# `inventory_dsp[black]` (blocked partners).  The global catalog of partner
# IDs → names comes from `reference_dsp_list`.
#
# To change which ad-exchange partners bid on a given site/app, update the
# whitelist via edit_inventory.  To see partner-level revenue, use
# partner_report().

_partner_cache: dict | None = None


def list_partners(refresh: bool = False) -> dict[int, str]:
    """
    Return the full catalog of ad-exchange partners as {partner_id: name}.
    Cached in-process; pass refresh=True to force re-fetch.
    """
    global _partner_cache
    if _partner_cache is not None and not refresh:
        return _partner_cache
    rows = _get("reference_dsp_list")
    _partner_cache = {int(r["key"]): r["name"] for r in rows if "key" in r}
    return _partner_cache


def get_inventory_partners(inventory_id: int | str) -> dict:
    """
    Return the ad-exchange partner whitelist/blacklist for an inventory,
    with partner names resolved.

    Returns:
        {
          "inventory_id": 441,
          "whitelist": [{"id": 289, "name": "..."}, ...],
          "blacklist": [...],
          "whitelist_ids": [289, 327, ...],
          "blacklist_ids": [...],
        }
    """
    inv = get_inventory(inventory_id)
    catalog = list_partners()
    white_ids = inv.get("inventory_dsp[white]") or []
    black_ids = inv.get("inventory_dsp[black]") or []
    return {
        "inventory_id": inv.get("inventory_id", inventory_id),
        "title": inv.get("title"),
        "whitelist_ids": white_ids,
        "blacklist_ids": black_ids,
        "whitelist": [{"id": i, "name": catalog.get(i, f"<unknown {i}>")} for i in white_ids],
        "blacklist": [{"id": i, "name": catalog.get(i, f"<unknown {i}>")} for i in black_ids],
    }


def edit_inventory(
    inventory_id: int | str,
    whitelist: list[int] | None = None,
    blacklist: list[int] | None = None,
    title: str | None = None,
    ron_traffic: bool | None = None,
    extra: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Update an inventory.  Primary use: change ad-exchange partner allow/block.

    whitelist / blacklist fully replace existing arrays.  Pass [] to clear.
    Pass None to leave unchanged.
    """
    inv = get_inventory(inventory_id)
    current_white = inv.get("inventory_dsp[white]") or []
    current_black = inv.get("inventory_dsp[black]") or []
    new_white = whitelist if whitelist is not None else current_white
    new_black = blacklist if blacklist is not None else current_black

    if dry_run:
        added   = sorted(set(new_white) - set(current_white))
        removed = sorted(set(current_white) - set(new_white))
        print(
            f"[tb_mgmt] DRY_RUN edit_inventory {inventory_id}  "
            f"whitelist: {len(current_white)} → {len(new_white)} "
            f"(+{len(added)} -{len(removed)})  "
            f"blacklist: {len(current_black)} → {len(new_black)}"
        )
        return {
            "inventory_id": inventory_id,
            "whitelist_added": added,
            "whitelist_removed": removed,
            "applied": False,
            "dry_run": True,
        }

    # edit_inventory requires array-form encoding: inventory_dsp[white][]=289&...
    form: list[tuple[str, str]] = [("inventory_id", str(inventory_id))]
    for pid in new_white:
        form.append(("inventory_dsp[white][]", str(pid)))
    for pid in new_black:
        form.append(("inventory_dsp[black][]", str(pid)))
    if title is not None:
        form.append(("title", title))
    if ron_traffic is not None:
        form.append(("ron_traffic", "true" if ron_traffic else "false"))
    for k, v in (extra or {}).items():
        if isinstance(v, (list, tuple)):
            for item in v:
                form.append((f"{k}[]", str(item)))
        else:
            form.append((k, str(v)))

    resp = requests.post(
        _url("edit_inventory"),
        data=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    if not resp.ok:
        raise RuntimeError(
            f"TB edit_inventory failed: HTTP {resp.status_code} — {resp.text[:300]}"
        )
    data = resp.json()
    if isinstance(data, dict) and data.get("status") is False:
        raise RuntimeError(f"TB edit_inventory error: {data.get('errors', data)}")

    print(
        f"[tb_mgmt] edit_inventory {inventory_id}  "
        f"whitelist={len(new_white)} blacklist={len(new_black)}  ✓"
    )
    return {"inventory_id": inventory_id, "applied": True, "result": data}


def set_inventory_partners(
    inventory_id: int | str,
    whitelist: list[int] | None = None,
    blacklist: list[int] | None = None,
    add_whitelist: list[int] | None = None,
    remove_whitelist: list[int] | None = None,
    add_blacklist: list[int] | None = None,
    remove_blacklist: list[int] | None = None,
    dry_run: bool = True,
) -> dict:
    """
    High-level partner list editor.  Use `whitelist=` / `blacklist=` for a
    full replacement, or `add_*` / `remove_*` for incremental updates.

    Default is dry_run=True for safety.
    """
    current = get_inventory_partners(inventory_id)
    cur_w = list(current["whitelist_ids"])
    cur_b = list(current["blacklist_ids"])

    new_w = list(whitelist) if whitelist is not None else cur_w
    new_b = list(blacklist) if blacklist is not None else cur_b
    for pid in (add_whitelist or []):
        if pid not in new_w: new_w.append(pid)
    for pid in (remove_whitelist or []):
        if pid in new_w: new_w.remove(pid)
    for pid in (add_blacklist or []):
        if pid not in new_b: new_b.append(pid)
    for pid in (remove_blacklist or []):
        if pid in new_b: new_b.remove(pid)

    return edit_inventory(
        inventory_id,
        whitelist=new_w,
        blacklist=new_b,
        dry_run=dry_run,
    )


def partner_report(
    start_date: str,
    end_date: str,
    inventory_ids: list[int] | None = None,
    extra_attributes: list[str] | None = None,
    day_group: str = "total",
    limit: int = 1000,
) -> list[dict]:
    """
    Partner-level revenue/impression stats.  Groups by company_dsp (partner).
    Combine with extra_attributes like ['inventory','country'] to slice.
    """
    attrs = ["company_dsp"] + list(extra_attributes or [])
    # TB requires the filter field to also appear in the attribute list
    if inventory_ids and "inventory" not in attrs:
        attrs.append("inventory")
    params: list[tuple] = [
        ("from", start_date),
        ("to", end_date),
        ("day_group", day_group),
        ("limit", limit),
    ]
    for a in attrs:
        params.append(("attribute[]", a))
    for iid in (inventory_ids or []):
        params.append(("filter[inventory][]", iid))

    token = _get_token()
    url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
    resp = requests.get(url, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"TB partner_report failed: HTTP {resp.status_code} — {resp.text[:300]}")
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


# ---------------------------------------------------------------------------
# Entry point — connection test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== TB Management API — connection test ===\n")
    ok = test_connection(verbose=True)
    if ok:
        print("\n=== Full placement map ===")
        dump_placements()
