"""
core/tbx_mgmt.py
~~~~~~~~~~~~~~~~

Entity + **write** layer for the new Teqblaze platform
(`api.pgammedia.com`). Transport, auth and analytics reads live in
`core/tbx_api.py` — read that module's header first for how this platform
differs from the legacy `ssp.pgammedia.com` pair (`tb_api` / `tb_mgmt`).

Write-path discipline
---------------------
Everything here follows the same rules the LL and legacy-TB writers earned
the hard way (see the 2026-04-18 floor-zeroing incident and the 2026-04-24
floor-thrash incident recorded in `render.yaml`):

1. **`dry_run=True` by default.** A caller has to ask for a live write.
2. **A second, environment-level gate.** Even `dry_run=False` refuses
   unless `TBX_ALLOW_WRITES=1`. New platform, unproven payload shapes —
   two locks, not one.
3. **Read-modify-write, never partial.** `/supply-sources/{id}/update` and
   `/demand-sources/{id}/update` take the *entire* entity. Posting a
   partial body would blank every field left out. `_merged_update` GETs
   current state, deep-merges the change, and refuses to write if the
   merge would drop a key that was present before.
4. **Clamps before payload.** Floors are clamped up to contract minimums
   and by a maximum per-run delta. A clamp is always logged.
5. **Verify read-after-write.** A silent no-op write is the failure mode
   that cost months on the legacy platform; every write re-reads and
   reports `verify_ok`.
6. **Ledger every write** to `logs/tb_ledger.jsonl` via `core.tb_ledger`,
   tagged `platform="tbx"` so the two platforms stay separable.
7. **Partner freeze respected** via `core.partner_freeze`.

Nothing in this module is wired into `scheduler.py`. Wiring an autonomous
writer is a separate, deliberate decision.

Shape caveat — read before enabling writes
------------------------------------------
The spec's `*Resource` (read) and `*Request` (write) schemas are close but
not identical:

  * `SupplySourceResource` adds `id` and source-level `margin_type` /
    `margin_min` / `margin_max`, none of which `SupplySourceRequest`
    accepts.
  * `DemandSourceResource` adds `id`, `operation_systems` and `uuid`, none of
    which `DemandSourceRequest` accepts. `uuid` is the asymmetric one — the
    *supply* write schema does accept it — and it was missing from the strip
    list until 2026-08-21.

`_strip_read_only` drops exactly those, and `read_only_fields_from_spec()`
recomputes the set from the vendored spec so the two cannot drift silently
(`tests/test_tbx.py` asserts they agree). Whether the platform tolerates the
round-trip in practice is still unverified — `scripts/tbx_probe.py
--diff-shape` prints the GET → update-payload diff for one entity, and checks
its keys against the write schema, so it can be validated against a real
account before any live write.
"""

from __future__ import annotations

import copy
import json
import os
import sys
import time
from typing import Any

from core import tbx_api as tbx
from core.tbx_api import TbxError

_LOG = "[tbx_mgmt]"

# ---------------------------------------------------------------------------
# Write gates
# ---------------------------------------------------------------------------

def writes_enabled() -> bool:
    """True only when `TBX_ALLOW_WRITES=1`. Default: writes are refused."""
    return os.getenv("TBX_ALLOW_WRITES", "0") == "1"


def _default_dry_run() -> bool:
    """`TBX_DRY_RUN=false` opts out; anything else (or unset) stays dry."""
    return os.getenv("TBX_DRY_RUN", "true").strip().lower() not in ("0", "false", "no")


# ---------------------------------------------------------------------------
# Floor safety
# ---------------------------------------------------------------------------

# Mirror of `tb_mgmt.PROTECTED_FLOOR_MINIMUMS` and LL's
# PROTECTED_FLOOR_MINIMUMS. Keyed by this platform's own IDs.
#
# ID portability, per Teqblaze 2026-08-20: PLACEMENT ids are the SAME as on the
# legacy host (placements were transferred as-is, settings included), so a
# legacy placement id is a valid key here. INVENTORY ids are different. Supply-
# source ids were not covered — do not assume a legacy id resolves to one.
# LL ids remain unrelated to both; LL's map is name-token keyed, not id keyed.
#
#   {"placement": {placement_id: min}, "supply_source": {supply_id: min}}
#
# A placement-level minimum wins over its supply source's.
PROTECTED_FLOOR_MINIMUMS: dict[str, dict[int, float]] = {
    "placement": {},
    "supply_source": {},
}

# Absolute zero-out guard. Nothing may set a floor below this.
GLOBAL_MIN_FLOOR = 0.01

# Largest single-run floor move, as a fraction. 0.25 == ±25%. The legacy
# platform's every-2h tuner moved floors ±39% in one run and tanked revenue;
# this cap makes that arithmetically impossible.
MAX_FLOOR_DELTA = float(os.getenv("TBX_MAX_FLOOR_DELTA", "0.25"))


def _protected_minimum(placement_id: int | None, supply_source_id: int | None) -> float | None:
    if placement_id is not None and int(placement_id) in PROTECTED_FLOOR_MINIMUMS["placement"]:
        return float(PROTECTED_FLOOR_MINIMUMS["placement"][int(placement_id)])
    if supply_source_id is not None and int(supply_source_id) in PROTECTED_FLOOR_MINIMUMS["supply_source"]:
        return float(PROTECTED_FLOOR_MINIMUMS["supply_source"][int(supply_source_id)])
    return None


def clamp_floor(
    requested: float,
    current: float | None = None,
    placement_id: int | None = None,
    supply_source_id: int | None = None,
) -> tuple[float, list[str]]:
    """
    Apply every floor guard and return `(clamped_value, reasons)`.

    Order matters: the delta cap runs first (so a big requested move is
    trimmed toward current), then the contract minimum and the global floor
    raise it back up. Contract minimums always win.
    """
    reasons: list[str] = []
    value = float(requested)

    if current is not None and float(current) > 0 and MAX_FLOOR_DELTA > 0:
        cur = float(current)
        lo, hi = cur * (1 - MAX_FLOOR_DELTA), cur * (1 + MAX_FLOOR_DELTA)
        if value < lo:
            reasons.append(f"delta cap: ${value:.4f} → ${lo:.4f} (max -{MAX_FLOOR_DELTA:.0%} from ${cur:.4f})")
            value = lo
        elif value > hi:
            reasons.append(f"delta cap: ${value:.4f} → ${hi:.4f} (max +{MAX_FLOOR_DELTA:.0%} from ${cur:.4f})")
            value = hi

    contract_min = _protected_minimum(placement_id, supply_source_id)
    if contract_min is not None and value < contract_min:
        reasons.append(f"contract floor: ${value:.4f} → ${contract_min:.4f}")
        value = contract_min
    elif value < GLOBAL_MIN_FLOOR:
        reasons.append(f"global min: ${value:.4f} → ${GLOBAL_MIN_FLOOR:.4f}")
        value = GLOBAL_MIN_FLOOR

    for reason in reasons:
        print(f"{_LOG} ⚠️  CLAMP  {reason}")
    return round(value, 4), reasons


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

def _ledger(
    actor: str,
    action: str,
    entity_type: str,
    entity_id: int | str,
    reason: str,
    before: dict,
    after: dict,
    applied: bool,
    verify_ok: bool | None = None,
    dry_run: bool = False,
    extra: dict | None = None,
) -> None:
    """Append to `logs/tb_ledger.jsonl`, tagged for this platform."""
    try:
        from core import tb_ledger
        tb_ledger.record(
            actor=actor, action=action,
            entity_type=f"tbx_{entity_type}", entity_id=entity_id,
            reason=reason, before=before, after=after,
            applied=applied, verify_ok=verify_ok, dry_run=dry_run,
            extra={**(extra or {}), "platform": "tbx", "base": tbx.TBX_BASE},
        )
    except Exception as exc:
        print(f"{_LOG} ledger record failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Read — supply sources
# ---------------------------------------------------------------------------

def list_supply_sources(
    search: str | None = None,
    status: int | None = None,
    source_type: list[str] | None = None,
    company_id: list[int] | None = None,
    traffic_type: list[str] | None = None,
    ad_format: list[str] | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
) -> list[dict]:
    """
    `POST /supply-sources` — one row per supply source with live operational
    columns: incoming_qps, qps, bid_qps, win_rate, srpm, supply_fill_rate,
    revenue_today / revenue_yesterday, placement count, status.

    That column set makes this the cheapest health sweep on the platform —
    a single call ranks every supply source by yesterday's revenue and fill.
    """
    flt: dict[str, Any] = {}
    if search is not None:
        flt["search"] = search
    if status is not None:
        flt["status"] = status
    if source_type:
        flt["type"] = source_type
    if company_id:
        flt["company_id"] = company_id
    if traffic_type:
        flt["traffic_type"] = traffic_type
    if ad_format:
        flt["ad_format"] = ad_format
    payload: dict[str, Any] = {"filter": flt}
    if sort:
        payload["sort"] = sort
    return tbx.fetch_all("/supply-sources", payload, per_page=per_page)


def get_supply_source(supply_source_id: int) -> dict:
    """`GET /supply-sources/{id}` — full config including nested placements."""
    body = tbx.get(f"/supply-sources/{supply_source_id}")
    return body.get("data", body) if isinstance(body, dict) else body


def list_placements(
    supply_source_id: int,
    search: str | None = None,
    status: int | None = None,
    ad_format: list[str] | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
) -> list[dict]:
    """
    `POST /supply-sources/{id}/placements` — id, name, ad_format, type,
    floor_price, margin_status/type/min/max, size, status.

    This is the read side of the floor lever: `floor_price` here is the
    per-placement bid floor.
    """
    flt: dict[str, Any] = {}
    if search is not None:
        flt["search"] = search
    if status is not None:
        flt["status"] = status
    if ad_format:
        flt["ad_format"] = ad_format
    payload: dict[str, Any] = {"filter": flt}
    if sort:
        payload["sort"] = sort
    return tbx.fetch_all(f"/supply-sources/{supply_source_id}/placements",
                         payload, per_page=per_page)


def all_placements(per_page: int = 250) -> list[dict]:
    """
    Every placement on the platform, annotated with its owning supply source.

    One call per supply source — fine for a nightly sweep, too chatty for a
    tight loop.
    """
    out: list[dict] = []
    for src in list_supply_sources(per_page=per_page):
        sid = src.get("id")
        if sid is None:
            continue
        for pl in list_placements(int(sid), per_page=per_page):
            out.append({
                **pl,
                "supply_source_id": sid,
                "supply_source_name": src.get("name"),
                "company_id": src.get("company_id"),
                "company_name": src.get("company_name"),
            })
    return out


def get_supply_ads_txt(supply_source_id: int) -> Any:
    """`GET /supply-sources/{id}/ads-txt` — the ads.txt lines this source needs."""
    return tbx.get(f"/supply-sources/{supply_source_id}/ads-txt")


def supply_qps_capacity() -> Any:
    """`GET /supply-sources/qps-capacity` — headroom against the QPS ceiling."""
    return tbx.get("/supply-sources/qps-capacity")


# ---------------------------------------------------------------------------
# Read — demand sources
# ---------------------------------------------------------------------------

def list_demand_sources(
    search: str | None = None,
    status: int | None = None,
    source_type: list[str] | None = None,
    company_id: list[int] | None = None,
    traffic_type: list[str] | None = None,
    ad_format: list[str] | None = None,
    sort: list[dict] | None = None,
    per_page: int = 250,
) -> list[dict]:
    """
    `POST /demand-sources` — per DSP: qps_limit, qps, bid_qps, spend_limit,
    spend_today / spend_yesterday, srpm, demand_fill_rate, status.

    `spend_limit` beside `spend_today` is the pacing signal the legacy
    platform never exposed.
    """
    flt: dict[str, Any] = {}
    if search is not None:
        flt["search"] = search
    if status is not None:
        flt["status"] = status
    if source_type:
        flt["type"] = source_type
    if company_id:
        flt["company_id"] = company_id
    if traffic_type:
        flt["traffic_type"] = traffic_type
    if ad_format:
        flt["ad_format"] = ad_format
    payload: dict[str, Any] = {"filter": flt}
    if sort:
        payload["sort"] = sort
    return tbx.fetch_all("/demand-sources", payload, per_page=per_page)


def get_demand_source(demand_source_id: int) -> dict:
    """`GET /demand-sources/{id}` — full config: geo_settings, qps_limit,
    schain flags, margin, seats, filters, api_sync."""
    body = tbx.get(f"/demand-sources/{demand_source_id}")
    return body.get("data", body) if isinstance(body, dict) else body


def demand_qps_capacity() -> Any:
    """`GET /demand-sources/qps-capacity`."""
    return tbx.get("/demand-sources/qps-capacity")


def list_companies(search: str | None = None, per_page: int = 250) -> list[dict]:
    """`POST /companies`."""
    flt = {"search": search} if search is not None else {}
    return tbx.fetch_all("/companies", {"filter": flt}, per_page=per_page)


# ---------------------------------------------------------------------------
# Read-modify-write core
# ---------------------------------------------------------------------------

# Fields the read schema returns but the write schema rejects. See the module
# docstring's shape caveat.
#
# Derived from the vendored spec by set difference — `SupplySourceResource` -
# `SupplySourceRequest` and `DemandSourceResource` - `DemandSourceRequest`.
# `write_schema_fields()` below recomputes that from the spec on demand, which
# is how `uuid` was caught: it is returned on a demand source and rejected on
# the way back in, and only the supply schema accepts it. Keep this tuple and
# the spec in agreement — `python3 tests/test_tbx.py` fails if they diverge,
# allowing only the hand-named exceptions in `_UNDECLARED_RESPONSE_FIELDS`.
_READ_ONLY_FIELDS = {
    "supply_source": ("id", "margin_type", "margin_min", "margin_max",
                      "has_inactive_company"),
    "demand_source": ("id", "operation_systems", "uuid"),
}

# The subset of the above that the spec cannot account for: fields the *live*
# API returns which appear in neither the read nor the write schema, so the
# set difference above yields nothing for them and they have to be named by
# hand. Each one is a real finding from `--diff-shape` against the account,
# and each is also a sign the vendored spec has fallen behind the platform.
#
# `has_inactive_company` (supply, found 2026-08-28 on source 264): returned
# by GET, undeclared by SupplySourceRequest, so leaving it in the body sends
# the platform a key it never advertised accepting.
_UNDECLARED_RESPONSE_FIELDS = {
    "supply_source": ("has_inactive_company",),
    "demand_source": (),
}

# The OpenAPI request schema each entity's `/update` endpoint accepts. Used to
# check an outgoing payload's keys before it is sent.
_WRITE_SCHEMA = {
    "supply_source": "SupplySourceRequest",
    "demand_source": "DemandSourceRequest",
}
_READ_SCHEMA = {
    "supply_source": "SupplySourceResource",
    "demand_source": "DemandSourceResource",
}

_SPEC_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "docs", "api", "teqblaze-openapi.json",
)
_schema_cache: dict[str, frozenset[str]] = {}


def _spec_properties(schema_name: str) -> frozenset[str]:
    """Top-level property names of one schema in the vendored OpenAPI spec."""
    if schema_name in _schema_cache:
        return _schema_cache[schema_name]
    try:
        with open(_SPEC_PATH) as handle:
            spec = json.load(handle)
        props = (spec.get("components", {}).get("schemas", {})
                 .get(schema_name, {}).get("properties") or {})
        fields = frozenset(props.keys())
    except (OSError, ValueError, AttributeError) as exc:
        # A missing or unreadable spec must not break the write path; the
        # checks that use this degrade to "unknown", which callers treat as
        # "no opinion" rather than "approved".
        print(f"{_LOG} WARN: could not read {_SPEC_PATH} ({exc}); "
              f"payload key checks are unavailable", file=sys.stderr)
        fields = frozenset()
    _schema_cache[schema_name] = fields
    return fields


def write_schema_fields(kind: str) -> frozenset[str]:
    """Field names the platform's write schema accepts for `kind`."""
    return _spec_properties(_WRITE_SCHEMA.get(kind, ""))


def read_only_fields_from_spec(kind: str) -> frozenset[str]:
    """
    Fields the read schema returns that the write schema does not accept.

    The authority for `_READ_ONLY_FIELDS`. Returns an empty set when the spec
    cannot be read, so a caller can tell "nothing to strip" from "don't know"
    only by checking `write_schema_fields()` too.
    """
    return _spec_properties(_READ_SCHEMA.get(kind, "")) - write_schema_fields(kind)


def unknown_write_keys(payload: dict, kind: str) -> list[str]:
    """
    Keys in `payload` that the write schema for `kind` does not declare.

    A non-empty result means the update would send fields the platform did
    not advertise accepting — most likely a 422, which fails safe, but on a
    lenient server it could also be silently ignored, which does not. Returns
    `[]` when the spec is unreadable rather than guessing.
    """
    accepted = write_schema_fields(kind)
    if not accepted:
        return []
    return sorted(k for k in payload if k not in accepted)


def _strip_read_only(entity: dict, kind: str) -> dict:
    """Drop response-only fields so the body matches the write schema."""
    out = copy.deepcopy(entity)
    for field in _READ_ONLY_FIELDS.get(kind, ()):
        out.pop(field, None)
    return out


def _deep_merge(base: dict, changes: dict) -> dict:
    """
    Recursive merge of `changes` onto `base`.

    Dicts merge key-wise; every other type (lists included) replaces
    wholesale. Lists must replace: `geo_settings.bid_floor`,
    `demand_sources`, `placements` are all "the complete set", so a
    element-wise merge would produce a set nobody asked for.
    """
    out = copy.deepcopy(base)
    for key, value in changes.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def _assert_no_key_loss(before: dict, payload: dict, path: str = "") -> list[str]:
    """
    Return the keys present in `before` but missing from `payload`.

    A non-empty result means the update would blank real configuration —
    the writers treat it as fatal rather than shipping it.
    """
    lost: list[str] = []
    for key, value in before.items():
        here = f"{path}.{key}" if path else key
        if key not in payload:
            lost.append(here)
        elif isinstance(value, dict) and isinstance(payload.get(key), dict):
            lost.extend(_assert_no_key_loss(value, payload[key], here))
    return lost


def _apply_update(
    kind: str,
    entity_id: int,
    changes: dict,
    actor: str,
    reason: str,
    action: str,
    dry_run: bool | None = None,
    verify: bool = True,
    verify_paths: list[str] | None = None,
) -> dict:
    """
    Read-modify-write one supply or demand source.

    `changes` is a sparse dict shaped like the entity; it is deep-merged onto
    current state and the whole object is POSTed to `.../update`.

    Returns a result dict with `applied`, `verify_ok`, `before`, `after`,
    `clamps` and (on dry run) the payload that *would* have been sent.
    """
    if kind not in ("supply_source", "demand_source"):
        raise ValueError("kind must be 'supply_source' or 'demand_source'")
    dry_run = _default_dry_run() if dry_run is None else dry_run
    path_root = "supply-sources" if kind == "supply_source" else "demand-sources"

    current = (get_supply_source(entity_id) if kind == "supply_source"
               else get_demand_source(entity_id))
    if not isinstance(current, dict) or not current:
        raise TbxError(f"GET /{path_root}/{entity_id} returned nothing usable")

    body = _strip_read_only(current, kind)
    payload = _deep_merge(body, changes)

    lost = _assert_no_key_loss(body, payload)
    if lost:
        raise TbxError(
            f"refusing to update {kind} {entity_id}: the merged payload would "
            f"drop {lost}. This is the field-blanking failure mode — fix the "
            f"merge, don't bypass this check."
        )

    # The other direction: keys the write schema never declared. `_assert_no_
    # key_loss` only compares the payload against the *stripped* body, so it
    # cannot see a field the platform will reject — every key it checks is one
    # we just put there. Warned, not refused: an undeclared key most likely
    # 422s (which fails safe and changes nothing), whereas hard-refusing on a
    # vendored spec that has fallen behind the platform would block every
    # write for a reason that is ours, not theirs.
    unknown = unknown_write_keys(payload, kind)
    if unknown:
        print(f"{_LOG} WARN  {action}  {kind}={entity_id} — payload carries "
              f"{unknown}, which {_WRITE_SCHEMA[kind]} does not declare. Either "
              f"add them to _READ_ONLY_FIELDS or re-vendor the spec.",
              file=sys.stderr)

    before_slice = {k: current.get(k) for k in changes}
    after_slice = {k: payload.get(k) for k in changes}

    if dry_run:
        print(f"{_LOG} DRY_RUN  {action}  {kind}={entity_id}  "
              f"{json.dumps(before_slice, default=str)[:200]} → "
              f"{json.dumps(after_slice, default=str)[:200]}")
        _ledger(actor, action, kind, entity_id, reason,
                before_slice, after_slice, applied=False, dry_run=True)
        return {
            "entity_type": kind, "entity_id": entity_id,
            "before": before_slice, "after": after_slice,
            "applied": False, "dry_run": True, "payload": payload,
            "unknown_keys": unknown,
        }

    if not writes_enabled():
        print(f"{_LOG} REFUSED  {action}  {kind}={entity_id} — "
              f"TBX_ALLOW_WRITES is not 1. Live writes to the new platform "
              f"are gated at the environment level.", file=sys.stderr)
        _ledger(actor, action, kind, entity_id,
                f"{reason} [refused: TBX_ALLOW_WRITES unset]",
                before_slice, after_slice, applied=False, dry_run=False)
        return {
            "entity_type": kind, "entity_id": entity_id,
            "before": before_slice, "after": after_slice,
            "applied": False, "refused": "TBX_ALLOW_WRITES!=1",
        }

    tbx.post(f"/{path_root}/{entity_id}/update", payload)

    verify_ok: bool | None = None
    verify_diff: dict | None = None
    if verify:
        try:
            time.sleep(0.5)
            live = (get_supply_source(entity_id) if kind == "supply_source"
                    else get_demand_source(entity_id))
            verify_diff = {}
            for key, expected in after_slice.items():
                actual = live.get(key)
                if _roughly_equal(actual, expected):
                    continue
                verify_diff[key] = {"live": actual, "expected": expected}
            verify_ok = not verify_diff
            if verify_diff:
                print(f"{_LOG} ⚠️  VERIFY FAIL  {kind}={entity_id}  "
                      f"diff={json.dumps(verify_diff, default=str)[:300]}  ← silent no-op write?",
                      file=sys.stderr)
        except Exception as exc:
            verify_ok = None
            print(f"{_LOG} verify-read failed {kind}={entity_id}: {exc}")

    mark = "✓" if verify_ok else ("✗" if verify_ok is False else "?")
    print(f"{_LOG} {action}  {kind}={entity_id}  applied  verify={mark}  {reason}")

    _ledger(actor, action, kind, entity_id, reason,
            before_slice, after_slice, applied=True, verify_ok=verify_ok,
            dry_run=False, extra={"verify_diff": verify_diff} if verify_diff else None)

    return {
        "entity_type": kind, "entity_id": entity_id,
        "before": before_slice, "after": after_slice,
        "applied": True, "verify_ok": verify_ok, "verify_diff": verify_diff,
        "unknown_keys": unknown,
    }


def _roughly_equal(a: Any, b: Any) -> bool:
    """Numeric-tolerant equality, so 2.5 == 2.50 == "2.5" on verify."""
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    try:
        return abs(float(a) - float(b)) < 1e-4
    except (TypeError, ValueError):
        return a == b


# ---------------------------------------------------------------------------
# Write — supply side
# ---------------------------------------------------------------------------

def set_placement_floor(
    supply_source_id: int,
    placement_id: int,
    floor_price: float,
    price_type: str | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Set the bid floor on one placement.

    The platform has no per-placement update endpoint — a placement's floor
    lives inside its supply source's `source.placements[]` array, so the write
    is a full supply-source update with that one array element edited. Every
    other placement is passed through untouched.

    `price_type` switches between `bid_floor` (a floor) and
    `fixed_bid_price` (a fixed price). Leave it None to keep the current mode;
    flipping it changes monetisation behaviour, not just a number.
    """
    current = get_supply_source(supply_source_id)
    source = (current or {}).get("source") or {}
    placements = source.get("placements") or []
    if not placements:
        raise TbxError(
            f"supply source {supply_source_id} has no source.placements[] — "
            f"placement floors can only be written for direct-inventory "
            f"sources that expose them"
        )

    target = next((p for p in placements if int(p.get("id") or 0) == int(placement_id)), None)
    if target is None:
        ids = [p.get("id") for p in placements]
        raise TbxError(f"placement {placement_id} not found on supply source "
                       f"{supply_source_id}; it has {ids}")

    current_floor = float(target.get("floor_price") or 0)
    clamped, clamps = clamp_floor(
        floor_price, current=current_floor,
        placement_id=placement_id, supply_source_id=supply_source_id,
    )

    new_placements = copy.deepcopy(placements)
    for placement in new_placements:
        if int(placement.get("id") or 0) == int(placement_id):
            placement["floor_price"] = clamped
            if price_type is not None:
                if price_type not in ("bid_floor", "fixed_bid_price"):
                    raise ValueError("price_type must be 'bid_floor' or 'fixed_bid_price'")
                placement["price_type"] = price_type

    detail = (reason or "floor update") + (f" [clamped: {'; '.join(clamps)}]" if clamps else "")
    result = _apply_update(
        "supply_source", supply_source_id,
        {"source": {"placements": new_placements}},
        actor=actor,
        reason=f"placement {placement_id} floor ${current_floor:.4f} → ${clamped:.4f}: {detail}",
        action="set_placement_floor",
        dry_run=dry_run,
    )
    result.update({
        "placement_id": placement_id,
        "old_floor": current_floor,
        "new_floor": clamped,
        "requested_floor": float(floor_price),
        "clamps": clamps,
    })
    return result


def set_placement_status(
    supply_source_id: int,
    placement_id: int,
    active: bool,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Pause or resume one placement via
    `POST /supply-sources/{id}/placements/{placement_id}/status`.

    Unlike floors, status has its own endpoint — no full-object round trip.
    """
    dry_run = _default_dry_run() if dry_run is None else dry_run
    action = "enable_placement" if active else "disable_placement"

    if dry_run:
        print(f"{_LOG} DRY_RUN  {action}  supply={supply_source_id} placement={placement_id}")
        _ledger(actor, action, "placement", placement_id, reason,
                {}, {"status": active}, applied=False, dry_run=True)
        return {"placement_id": placement_id, "status": active,
                "applied": False, "dry_run": True}

    if not writes_enabled():
        print(f"{_LOG} REFUSED  {action}  placement={placement_id} — "
              f"TBX_ALLOW_WRITES is not 1", file=sys.stderr)
        _ledger(actor, action, "placement", placement_id,
                f"{reason} [refused: TBX_ALLOW_WRITES unset]",
                {}, {"status": active}, applied=False)
        return {"placement_id": placement_id, "status": active,
                "applied": False, "refused": "TBX_ALLOW_WRITES!=1"}

    tbx.post(f"/supply-sources/{supply_source_id}/placements/{placement_id}/status",
             {"status": bool(active)})
    print(f"{_LOG} {action}  supply={supply_source_id} placement={placement_id}  {reason}")
    _ledger(actor, action, "placement", placement_id, reason,
            {}, {"status": active}, applied=True)
    return {"placement_id": placement_id, "status": active, "applied": True}


def set_supply_source_status(
    supply_source_id: int,
    active: bool,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """`POST /supply-sources/{id}/status` — pause or resume a whole source."""
    return _simple_status("supply-sources", "supply_source", supply_source_id,
                          active, actor, reason, dry_run)


def set_demand_source_status(
    demand_source_id: int,
    active: bool,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
    demand_name: str | None = None,
) -> dict:
    """
    `POST /demand-sources/{id}/status`.

    Honours `core.partner_freeze`: a frozen partner is never toggled. Pass
    `demand_name` so the name-based freeze check can fire — this platform's
    IDs are not the LL demand IDs the freeze list is keyed on.
    """
    if demand_name:
        from core import partner_freeze
        if partner_freeze.check_and_skip(demand_name=demand_name, actor=actor):
            return {"entity_id": demand_source_id, "applied": False,
                    "refused": "partner_freeze"}
    return _simple_status("demand-sources", "demand_source", demand_source_id,
                          active, actor, reason, dry_run)


def _simple_status(path_root: str, kind: str, entity_id: int, active: bool,
                   actor: str, reason: str, dry_run: bool | None) -> dict:
    dry_run = _default_dry_run() if dry_run is None else dry_run
    action = f"{'enable' if active else 'disable'}_{kind}"

    if dry_run:
        print(f"{_LOG} DRY_RUN  {action}  {kind}={entity_id}")
        _ledger(actor, action, kind, entity_id, reason, {},
                {"status": active}, applied=False, dry_run=True)
        return {"entity_id": entity_id, "status": active, "applied": False, "dry_run": True}

    if not writes_enabled():
        print(f"{_LOG} REFUSED  {action}  {kind}={entity_id} — "
              f"TBX_ALLOW_WRITES is not 1", file=sys.stderr)
        _ledger(actor, action, kind, entity_id,
                f"{reason} [refused: TBX_ALLOW_WRITES unset]",
                {}, {"status": active}, applied=False)
        return {"entity_id": entity_id, "status": active,
                "applied": False, "refused": "TBX_ALLOW_WRITES!=1"}

    tbx.post(f"/{path_root}/{entity_id}/status", {"status": bool(active)})
    print(f"{_LOG} {action}  {kind}={entity_id}  {reason}")
    _ledger(actor, action, kind, entity_id, reason, {}, {"status": active}, applied=True)
    return {"entity_id": entity_id, "status": active, "applied": True}


def set_supply_source_fields(
    supply_source_id: int,
    *,
    floor_price: float | None = None,
    bid_floor_type: str | None = None,
    spend_limit: float | None = None,
    default_tmax: int | None = None,
    is_smart_floor: bool | None = None,
    is_dynamic_margin: bool | None = None,
    dynamic_margin: float | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Update source-level economics on an indirect-supplier source: its own
    `floor_price`, spend limit, tmax, Teqblaze's smart-floor optimiser, and
    dynamic margin.

    `is_smart_floor` hands floor control to the platform's own optimiser. It
    and an autonomous PGAM floor agent must not run on the same source at
    once — they will fight, which is exactly the thrash that took revenue
    down on the legacy platform in April.
    """
    source_changes: dict[str, Any] = {}
    clamps: list[str] = []

    if floor_price is not None:
        current = get_supply_source(supply_source_id)
        cur_floor = float(((current or {}).get("source") or {}).get("floor_price") or 0)
        clamped, clamps = clamp_floor(floor_price, current=cur_floor,
                                      supply_source_id=supply_source_id)
        source_changes["floor_price"] = clamped
    if bid_floor_type is not None:
        if bid_floor_type not in ("bid_floor", "fixed_bid_price"):
            raise ValueError("bid_floor_type must be 'bid_floor' or 'fixed_bid_price'")
        source_changes["bid_floor_type"] = bid_floor_type
    if spend_limit is not None:
        source_changes["spend_limit"] = float(spend_limit)
    if default_tmax is not None:
        source_changes["default_tmax"] = int(default_tmax)
    if is_smart_floor is not None:
        source_changes["is_smart_floor"] = bool(is_smart_floor)
    if is_dynamic_margin is not None:
        source_changes["is_dynamic_margin"] = bool(is_dynamic_margin)
    if dynamic_margin is not None:
        source_changes["dynamic_margin"] = float(dynamic_margin)

    if not source_changes:
        raise ValueError("set_supply_source_fields called with nothing to change")

    detail = (reason or "supply source update") + (f" [clamped: {'; '.join(clamps)}]" if clamps else "")
    result = _apply_update("supply_source", supply_source_id,
                           {"source": source_changes},
                           actor=actor, reason=detail,
                           action="set_supply_source_fields", dry_run=dry_run)
    result["clamps"] = clamps
    return result


def set_supply_allowed_demand(
    supply_source_id: int,
    demand_source_ids: list[int],
    companies: list[int] | None = None,
    is_allowed: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Rewrite which demand sources / companies may buy this supply.

    `is_allowed=True` treats the lists as an allowlist, `False` as a
    blocklist. **The lists replace the existing sets wholesale** — read
    current state and pass the full intended set, never a delta.
    """
    changes: dict[str, Any] = {
        "is_allowed_sources": bool(is_allowed),
        "demand_sources": [int(d) for d in demand_source_ids],
    }
    if companies is not None:
        changes["companies"] = [int(c) for c in companies]
    return _apply_update("supply_source", supply_source_id, changes,
                         actor=actor,
                         reason=reason or f"{'allow' if is_allowed else 'block'} "
                                          f"{len(demand_source_ids)} demand sources",
                         action="set_supply_allowed_demand", dry_run=dry_run)


# ---------------------------------------------------------------------------
# Write — demand side
# ---------------------------------------------------------------------------

def set_demand_geo_bid_floors(
    demand_source_id: int,
    floors_by_country_id: dict[int, float],
    replace: bool = False,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
    demand_name: str | None = None,
) -> dict:
    """
    Set per-country bid floors on a DSP (`geo_settings.bid_floor`).

    This lever does not exist on the legacy platform, and it is the cleanest
    way to price a DSP by geo without touching publisher-side floors.

    `floors_by_country_id` is keyed by the platform's numeric country IDs —
    resolve names with `tbx_api.country_ids(["United States", "GBR"])`.

    `replace=False` (default) merges into the existing per-country set;
    `replace=True` discards every country not named here.
    """
    if demand_name:
        from core import partner_freeze
        if partner_freeze.check_and_skip(demand_name=demand_name, actor=actor):
            return {"entity_id": demand_source_id, "applied": False,
                    "refused": "partner_freeze"}

    current = get_demand_source(demand_source_id)
    geo = (current or {}).get("geo_settings") or {}
    existing = list(geo.get("bid_floor") or [])
    existing_by_country = {
        int(row["country_id"]): float(row.get("value") or 0)
        for row in existing if row.get("country_id") is not None
    }

    merged: dict[int, float] = {} if replace else dict(existing_by_country)
    all_clamps: list[str] = []
    for country_id, value in floors_by_country_id.items():
        clamped, clamps = clamp_floor(value, current=existing_by_country.get(int(country_id)))
        merged[int(country_id)] = clamped
        all_clamps.extend(clamps)

    rows = [{"country_id": cid, "value": val} for cid, val in sorted(merged.items())]
    detail = (reason or f"{len(floors_by_country_id)} geo floors") + \
             (f" [clamped: {'; '.join(all_clamps)}]" if all_clamps else "")

    result = _apply_update("demand_source", demand_source_id,
                           {"geo_settings": {"bid_floor": rows}},
                           actor=actor, reason=detail,
                           action="set_demand_geo_bid_floors", dry_run=dry_run)
    result["clamps"] = all_clamps
    return result


def set_demand_geo_qps(
    demand_source_id: int,
    qps_by_country_id: dict[int, float],
    replace: bool = False,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """Per-country QPS caps (`geo_settings.qps`). Merge semantics match
    `set_demand_geo_bid_floors`."""
    current = get_demand_source(demand_source_id)
    geo = (current or {}).get("geo_settings") or {}
    existing = {
        int(r["country_id"]): float(r.get("value") or 0)
        for r in (geo.get("qps") or []) if r.get("country_id") is not None
    }
    merged = {} if replace else dict(existing)
    merged.update({int(k): float(v) for k, v in qps_by_country_id.items()})
    rows = [{"country_id": cid, "value": val} for cid, val in sorted(merged.items())]
    return _apply_update("demand_source", demand_source_id,
                         {"geo_settings": {"qps": rows}},
                         actor=actor, reason=reason or f"{len(qps_by_country_id)} geo QPS caps",
                         action="set_demand_geo_qps", dry_run=dry_run)


def set_demand_geo_blacklist(
    demand_source_id: int,
    country_ids: list[int],
    replace: bool = False,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Add countries to a DSP's blocked-country list (`geo_settings.blacklist`).

    The wire field replaces wholesale, so this reads the current blacklist and
    sends the union. That is not a nicety: `geo_settings.blacklist` is a
    standing trading rule someone set by hand, and an agent that appends one
    dead country by POSTing only its own list silently unblocks every country
    a human blocked earlier. Merge semantics therefore match the two
    neighbouring geo writers, `set_demand_geo_bid_floors` and
    `set_demand_geo_qps`, and for the same reason.

    `replace=True` sends exactly `country_ids` and drops anything already
    there — only for a caller that has read the current list and means it.

    `country_ids` are the platform's numeric ids, not ISO codes; resolve with
    `tbx_api.country_ids(["Brazil", "RU"])`.

    The result carries `added` and `removed` (country ids) so a ledger can
    record what actually changed rather than what was asked for.
    """
    current = get_demand_source(demand_source_id)
    geo = (current or {}).get("geo_settings") or {}
    existing = {
        int(r["country_id"]) for r in (geo.get("blacklist") or [])
        if r.get("country_id") is not None
    }
    wanted = {int(cid) for cid in country_ids}

    merged = wanted if replace else (existing | wanted)
    added = sorted(merged - existing)
    removed = sorted(existing - merged)

    rows = [{"country_id": cid, "value": 0} for cid in sorted(merged)]
    detail = reason or (
        f"blacklist +{len(added)} countries "
        f"({len(existing)} already blocked{', ' + str(len(removed)) + ' dropped' if removed else ''})"
    )
    result = _apply_update("demand_source", demand_source_id,
                           {"geo_settings": {"blacklist": rows}},
                           actor=actor, reason=detail,
                           action="set_demand_geo_blacklist", dry_run=dry_run)
    result["added"] = added
    result["removed"] = removed
    result["blacklist_before"] = sorted(existing)
    result["blacklist_after"] = sorted(merged)
    return result


def set_demand_qps_limit(
    demand_source_id: int,
    *,
    max_qps_limit: int | None = None,
    min_qps_limit: int | None = None,
    qps_recalculation: int | None = None,
    qps_optimization_by: str | None = None,
    is_prioritize_pub: bool | None = None,
    is_prioritize_direct_supply: bool | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Configure a DSP's QPS envelope, including the platform's own QPS
    auto-optimiser.

    `qps_optimization_by` ∈ {rcpm, spend, clicks} tells the platform which
    signal to throttle on; `qps_recalculation` ∈ {15, 30, 60} is the interval
    in minutes. Setting these delegates QPS tuning to Teqblaze — do that
    *or* run a PGAM QPS agent, not both.

    The BidMachine QPS cap in the TB playbook is a hard partner rule: it is
    not encoded here, so any caller that could touch BidMachine must check it.
    """
    qps: dict[str, Any] = {}
    if max_qps_limit is not None:
        qps["max_qps_limit"] = int(max_qps_limit)
    if min_qps_limit is not None:
        qps["min_qps_limit"] = int(min_qps_limit)
    if qps_recalculation is not None:
        if int(qps_recalculation) not in (15, 30, 60):
            raise ValueError("qps_recalculation must be 15, 30 or 60")
        qps["qps_recalculation"] = int(qps_recalculation)
    if qps_optimization_by is not None:
        if qps_optimization_by not in ("rcpm", "spend", "clicks"):
            raise ValueError("qps_optimization_by must be rcpm, spend or clicks")
        qps["qps_optimization_by"] = qps_optimization_by
    if is_prioritize_pub is not None:
        qps["is_prioritize_pub"] = bool(is_prioritize_pub)
    if is_prioritize_direct_supply is not None:
        qps["is_prioritize_direct_supply"] = bool(is_prioritize_direct_supply)
    if not qps:
        raise ValueError("set_demand_qps_limit called with nothing to change")

    return _apply_update("demand_source", demand_source_id, {"qps_limit": qps},
                         actor=actor, reason=reason or f"qps_limit {qps}",
                         action="set_demand_qps_limit", dry_run=dry_run)


def set_demand_economics(
    demand_source_id: int,
    *,
    spend_limit: float | None = None,
    margin_type: str | None = None,
    margin_min: float | None = None,
    margin_max: float | None = None,
    target_srcpm: str | None = None,
    target_srcpm_value: float | None = None,
    is_vcr_optimization: bool | None = None,
    vcr_optimization: float | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
    demand_name: str | None = None,
) -> dict:
    """
    Update a DSP's spend cap, margin model, sRCPM target and VCR optimiser.

    `margin_type` ∈ {fixed, adaptive, range}; `adaptive`/`range` use
    margin_min/margin_max. Margin changes move what PGAM books per
    impression — treat them as commercial, not tuning.
    """
    if demand_name:
        from core import partner_freeze
        if partner_freeze.check_and_skip(demand_name=demand_name, actor=actor):
            return {"entity_id": demand_source_id, "applied": False,
                    "refused": "partner_freeze"}

    changes: dict[str, Any] = {}
    if spend_limit is not None:
        changes["spend_limit"] = float(spend_limit)
    if margin_type is not None:
        if margin_type not in ("fixed", "adaptive", "range"):
            raise ValueError("margin_type must be fixed, adaptive or range")
        changes["margin_type"] = margin_type
    if margin_min is not None:
        changes["margin_min"] = float(margin_min)
    if margin_max is not None:
        changes["margin_max"] = float(margin_max)
    if target_srcpm is not None:
        if target_srcpm not in ("default", "target"):
            raise ValueError("target_srcpm must be 'default' or 'target'")
        changes["target_srcpm"] = target_srcpm
    if target_srcpm_value is not None:
        changes["target_srcpm_value"] = float(target_srcpm_value)
    if is_vcr_optimization is not None:
        changes["is_vcr_optimization"] = bool(is_vcr_optimization)
    if vcr_optimization is not None:
        changes["vcr_optimization"] = float(vcr_optimization)
    if not changes:
        raise ValueError("set_demand_economics called with nothing to change")

    return _apply_update("demand_source", demand_source_id, changes,
                         actor=actor, reason=reason or f"economics {sorted(changes)}",
                         action="set_demand_economics", dry_run=dry_run)


def set_demand_schain_policy(
    demand_source_id: int,
    *,
    is_schain: bool | None = None,
    is_complete_schain: bool | None = None,
    is_only_schain_complete: bool | None = None,
    is_only_verified_nodes: bool | None = None,
    is_remove_unverified_adstxt: bool | None = None,
    is_schain_node: bool | None = None,
    schain_node: int | None = None,
    is_sensitive: bool | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Set a DSP's supply-chain integrity policy.

    These are the enforcement counterparts to the read-only audits in
    `agents/compliance`: the audit finds an unverified or incomplete chain,
    and these flags stop PGAM sending it onward. `is_sensitive` marks a DSP
    that must not receive traffic the scanners flagged as invalid.

    `schain_node` (2–10) caps the node count and requires `is_schain_node`.
    """
    changes: dict[str, Any] = {}
    for name, value in (
        ("is_schain", is_schain),
        ("is_complete_schain", is_complete_schain),
        ("is_only_schain_complete", is_only_schain_complete),
        ("is_only_verified_nodes", is_only_verified_nodes),
        ("is_remove_unverified_adstxt", is_remove_unverified_adstxt),
        ("is_schain_node", is_schain_node),
        ("is_sensitive", is_sensitive),
    ):
        if value is not None:
            changes[name] = bool(value)
    if schain_node is not None:
        if int(schain_node) not in range(2, 11):
            raise ValueError("schain_node must be between 2 and 10")
        changes["schain_node"] = int(schain_node)
    if not changes:
        raise ValueError("set_demand_schain_policy called with nothing to change")

    return _apply_update("demand_source", demand_source_id, changes,
                         actor=actor, reason=reason or f"schain policy {sorted(changes)}",
                         action="set_demand_schain_policy", dry_run=dry_run)


def set_demand_allowed_supply(
    demand_source_id: int,
    supply_source_ids: list[int],
    companies: list[int] | None = None,
    is_allowed: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Rewrite which supply sources / companies a DSP may buy from. Lists
    replace wholesale — pass the full intended set.
    """
    changes: dict[str, Any] = {
        "is_allowed_sources": bool(is_allowed),
        "supply_sources": [int(s) for s in supply_source_ids],
    }
    if companies is not None:
        changes["companies"] = [int(c) for c in companies]
    return _apply_update("demand_source", demand_source_id, changes,
                         actor=actor,
                         reason=reason or f"{'allow' if is_allowed else 'block'} "
                                          f"{len(supply_source_ids)} supply sources",
                         action="set_demand_allowed_supply", dry_run=dry_run)


def set_api_sync_url(
    kind: str,
    entity_id: int,
    url: str,
    url_type: str = "csv",
    map_headers: dict | None = None,
    validate_first: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    Register a partner's reporting URL on a supply or demand source so the
    platform pulls their numbers and fills the Discrepancy Report.

    `map_headers` names the partner's columns, e.g.
    `{"date": "day", "imps": "impressions", "spend": "revenue"}`.

    With `validate_first=True` the URL is checked via
    `/discrepancy-report/validate-api-url` before being committed; a
    validation failure aborts rather than storing a URL that will never sync.

    Setting this up per partner is what turns revenue recon from a monthly
    spreadsheet exercise into a query.
    """
    if kind not in ("supply_source", "demand_source"):
        raise ValueError("kind must be 'supply_source' or 'demand_source'")
    if url_type not in ("csv", "json", "xml"):
        raise ValueError("url_type must be csv, json or xml")

    if validate_first:
        check = tbx.validate_api_sync_url(url, url_type, map_headers)
        print(f"{_LOG} api-sync validation: {json.dumps(check, default=str)[:300]}")
        if isinstance(check, dict) and check.get("status") is False:
            raise TbxError(f"partner API URL failed validation: {str(check)[:300]}")

    api_sync: dict[str, Any] = {"url": url, "type": url_type}
    if map_headers:
        api_sync["map_headers"] = map_headers

    changes = ({"source": {"api_sync": api_sync}} if kind == "supply_source"
               else {"api_sync": api_sync})
    return _apply_update(kind, entity_id, changes, actor=actor,
                         reason=reason or f"api_sync → {url_type} {url}",
                         action="set_api_sync_url", dry_run=dry_run)


def sync_discrepancy(date_from: str, date_to: str,
                     filters: dict | None = None) -> dict:
    """
    `POST /discrepancy-report/sync` — pull partner-side numbers now instead
    of waiting for the platform's own schedule. A read-triggering call, not
    a config change, so it is not gated.
    """
    flt: dict[str, Any] = {"date_from": date_from, "date_to": date_to}
    flt.update(filters or {})
    return tbx.post("/discrepancy-report/sync", {"filter": flt})


# ---------------------------------------------------------------------------
# Filter lists  — block / allow lists
# ---------------------------------------------------------------------------

FILTER_RECORD_TYPES = (
    "bundle", "publisher_id", "site_app_id", "crid", "adomain",
    "schain_node_domain",
)


def list_filter_lists(search: str | None = None, per_page: int = 250) -> list[dict]:
    """`POST /filter-lists`."""
    flt = {"search": search} if search is not None else {}
    return tbx.fetch_all("/filter-lists", {"filter": flt}, per_page=per_page)


def get_filter_list(filter_list_id: int) -> dict:
    """`GET /filter-lists/{id}`."""
    body = tbx.get(f"/filter-lists/{filter_list_id}")
    return body.get("data", body) if isinstance(body, dict) else body


def get_filter_list_values(filter_list_id: int, per_page: int = 500) -> list[dict]:
    """`POST /filter-lists/{id}/values` — the entries in the list."""
    return tbx.fetch_all(f"/filter-lists/{filter_list_id}/values", {}, per_page=per_page)


def create_filter_list(
    name: str,
    record_type: str,
    list_type: str = "black",
    filtering_node: str | None = None,
    supply: list[str] | None = None,
    demand: list[str] | None = None,
    status: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /filter-lists/store`.

    `record_type` picks what the list matches — `adomain` for advertiser
    domains, `crid` for creatives, `bundle` / `site_app_id` for inventory,
    `schain_node_domain` for supply-chain nodes. `filtering_node`
    (all/first/last) only applies to schain_node_domain.

    `list_type` is `black` or `white`; `supply` / `demand` scope the list to
    specific sources (omit for platform-wide).
    """
    if record_type not in FILTER_RECORD_TYPES:
        raise ValueError(f"record_type must be one of {FILTER_RECORD_TYPES}")
    if list_type not in ("black", "white"):
        raise ValueError("list_type must be 'black' or 'white'")

    payload: dict[str, Any] = {
        "name": name, "record_type": record_type,
        "type": list_type, "status": bool(status),
    }
    if filtering_node is not None:
        if filtering_node not in ("all", "first", "last"):
            raise ValueError("filtering_node must be all, first or last")
        payload["filtering_node"] = filtering_node
    if supply is not None or demand is not None:
        payload["sources"] = {"supply": supply or [], "demand": demand or []}

    return _simple_write("/filter-lists/store", payload, "filter_list", 0,
                         "create_filter_list", actor, reason or f"create {list_type} {record_type} list",
                         dry_run)


def add_filter_values(
    filter_list_id: int,
    values: list[str],
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
    chunk_size: int = 1000,
) -> dict:
    """
    `POST /filter-lists/{id}/add-value` — append values to an existing list.

    The endpoint takes an *array* (`value[]`) over `multipart/form-data`, so
    the whole batch goes in one call rather than one call per value. Batches
    over `chunk_size` are split, because a single request carrying tens of
    thousands of domains is the kind of thing a proxy truncates.

    For a full replace rather than an append, `clear_filter_values` first —
    and read the current values out before you do, since clearing is not
    reversible from our side.
    """
    values = [str(v).strip() for v in values if str(v).strip()]
    if not values:
        raise ValueError("add_filter_values: no non-empty values given")

    chunks = [values[i:i + chunk_size] for i in range(0, len(values), chunk_size)]
    results = []
    for index, chunk in enumerate(chunks, 1):
        note = reason or f"add {len(values)} values"
        if len(chunks) > 1:
            note = f"{note} (chunk {index}/{len(chunks)})"
        results.append(_simple_write(
            f"/filter-lists/{filter_list_id}/add-value", {"value": chunk},
            "filter_list", filter_list_id, "add_filter_values", actor,
            note, dry_run, multipart=True))
    return {"filter_list_id": filter_list_id, "count": len(values),
            "chunks": len(chunks), "results": results}


def remove_filter_values(
    filter_list_id: int,
    values: str | list[str],
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /filter-lists/{id}/remove-value`.

    Like `add-value`, this takes an array over `multipart/form-data`. Accepts
    a single value or a list.
    """
    if isinstance(values, str):
        values = [values]
    values = [str(v).strip() for v in values if str(v).strip()]
    if not values:
        raise ValueError("remove_filter_values: no non-empty values given")
    return _simple_write(f"/filter-lists/{filter_list_id}/remove-value",
                         {"value": values}, "filter_list", filter_list_id,
                         "remove_filter_values", actor,
                         reason or f"remove {len(values)} values", dry_run,
                         multipart=True)


# Kept because the singular form reads better at a call site removing one entry.
remove_filter_value = remove_filter_values


def clear_filter_values(filter_list_id: int, actor: str = "manual",
                        reason: str = "", dry_run: bool | None = None) -> dict:
    """
    `POST /filter-lists/{id}/clear-values` — empties the list.

    Destructive and not reversible from the API. Snapshot with
    `get_filter_list_values` first.
    """
    return _simple_write(f"/filter-lists/{filter_list_id}/clear-values", {},
                         "filter_list", filter_list_id, "clear_filter_values",
                         actor, reason or "clear all values", dry_run)


def import_filter_values(
    filter_list_id: int,
    values: list[str],
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /filter-lists/{id}/import-values` — bulk load from a file.

    The spec declares this as `multipart/form-data` with a single `import`
    file field, so the values are serialised to a one-column CSV and uploaded.
    `GET /filter-lists/export-values-example` returns the platform's own
    template; if a live import is rejected, compare against that before
    assuming the endpoint is at fault.

    `add_filter_values` is the better choice for anything that fits in a
    request — it needs no file and reports per-chunk. Use this for the
    tens-of-thousands case.
    """
    values = [str(v).strip() for v in values if str(v).strip()]
    if not values:
        raise ValueError("import_filter_values: no non-empty values given")

    blob = ("\n".join(values) + "\n").encode()
    return _simple_write(
        f"/filter-lists/{filter_list_id}/import-values",
        {"value_count": len(values)},   # shown in the log/ledger, not sent as data
        "filter_list", filter_list_id, "import_filter_values", actor,
        reason or f"import {len(values)} values from a {len(blob)}-byte CSV",
        dry_run,
        files={"import": (f"filter_list_{filter_list_id}.csv", blob, "text/csv")})


def set_filter_list_status(filter_list_id: int, active: bool, actor: str = "manual",
                           reason: str = "", dry_run: bool | None = None) -> dict:
    """`POST /filter-lists/{id}/status`."""
    return _simple_write(f"/filter-lists/{filter_list_id}/status",
                         {"status": bool(active)}, "filter_list", filter_list_id,
                         "set_filter_list_status", actor, reason, dry_run)


# ---------------------------------------------------------------------------
# Deals  (PMP)
# ---------------------------------------------------------------------------

def list_deals(search: str | None = None, per_page: int = 250) -> list[dict]:
    """`POST /deals`."""
    flt = {"search": search} if search is not None else {}
    return tbx.fetch_all("/deals", {"filter": flt}, per_page=per_page)


def get_deal(deal_id: int) -> dict:
    """`GET /deals/{id}`."""
    body = tbx.get(f"/deals/{deal_id}")
    return body.get("data", body) if isinstance(body, dict) else body


def create_deal(
    name: str,
    deal_hash: str,
    cpm: float,
    deal_type: str = "SSP",
    auction_type: int = 1,
    countries: list[int] | None = None,
    sizes: list[int] | None = None,
    seats: list[int] | None = None,
    supply: list[str] | None = None,
    demand: list[str] | None = None,
    ad_formats: list[str] | None = None,
    traffic_types: list[str] | None = None,
    status: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /deals/store` — create a PMP deal.

    `deal_type` is `SSP` (a deal PGAM offers to buyers) or `DSP` (a deal PGAM
    consumes). `auction_type` 1 = first price, 2 = second price.
    `ad_formats` / `traffic_types` are expanded into the flag fields the
    platform expects.
    """
    if deal_type not in ("SSP", "DSP"):
        raise ValueError("deal_type must be 'SSP' or 'DSP'")
    if int(auction_type) not in (1, 2):
        raise ValueError("auction_type must be 1 or 2")

    payload: dict[str, Any] = {
        "name": name, "deal_hash": deal_hash, "hash": deal_hash,
        "type": deal_type, "cpm": float(cpm),
        "auction_type": int(auction_type), "status": bool(status),
    }
    for fmt in (ad_formats or []):
        if fmt not in ("banner", "video", "audio", "native"):
            raise ValueError(f"unknown ad format {fmt!r}")
        payload[f"ad_format_{fmt}"] = True
    for tt in (traffic_types or []):
        key = {"ctv": "traffic_type_ctv", "web": "traffic_type_web",
               "mobile_app": "traffic_type_mobile_app",
               "mobile_web": "traffic_type_mobile_web"}.get(tt)
        if not key:
            raise ValueError(f"unknown traffic type {tt!r}; use ctv, web, "
                             f"mobile_app or mobile_web")
        payload[key] = True
    if countries:
        payload["countries"] = [int(c) for c in countries]
    if sizes:
        payload["sizes"] = [int(s) for s in sizes]
    if seats:
        payload["seats"] = [int(s) for s in seats]
    if supply is not None or demand is not None:
        payload["sources"] = {"supply": supply or [], "demand": demand or []}

    return _simple_write("/deals/store", payload, "deal", 0, "create_deal",
                         actor, reason or f"create {deal_type} deal @ ${cpm}", dry_run)


def set_deal_status(deal_id: int, active: bool, actor: str = "manual",
                    reason: str = "", dry_run: bool | None = None) -> dict:
    """`POST /deals/{id}/status`."""
    return _simple_write(f"/deals/{deal_id}/status", {"status": bool(active)},
                         "deal", deal_id, "set_deal_status", actor, reason, dry_run)


# ---------------------------------------------------------------------------
# Alerts + scheduled reports  — push the platform's own monitoring at us
# ---------------------------------------------------------------------------

ALERT_METRICS = ("ssp_requests", "requests", "responses", "imps", "ssp_price", "dsp_price")


def list_alerts(per_page: int = 250) -> list[dict]:
    """`POST /alerts`."""
    return tbx.fetch_all("/alerts", {}, per_page=per_page)


def create_alert(
    name: str,
    metrics: list[dict],
    period: int = 1,
    channel: str = "slack",
    recipients: str = "",
    operator: str = "AND",
    attributes: dict | None = None,
    status: bool = True,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /alerts/store` — a platform-side alert that fires without PGAM
    polling for it.

    `metrics` is a list of `{"name": <ALERT_METRICS>, "operator": "<|>|=|%",
    "value": <number>}`. `period` ∈ {1, 4, 12, 24} hours. `channel` is
    `email` or `slack`; `attributes` narrows the scope with
    `dsp_id` / `endpoint_id` / `dsp_company_id` / `ssp_company_id` lists.

    Worth using for the coarse "requests fell off a cliff" alarms so PGAM's
    own agents can spend their budget on judgement instead of heartbeat
    checks.
    """
    if channel not in ("email", "slack"):
        raise ValueError("channel must be 'email' or 'slack'")
    if int(period) not in (1, 4, 12, 24):
        raise ValueError("period must be 1, 4, 12 or 24")
    if operator not in ("AND", "OR"):
        raise ValueError("operator must be 'AND' or 'OR'")
    for metric in metrics:
        if metric.get("name") not in ALERT_METRICS:
            raise ValueError(f"alert metric must be one of {ALERT_METRICS}, "
                             f"got {metric.get('name')!r}")
        if metric.get("operator") not in ("<", ">", "=", "%"):
            raise ValueError("alert metric operator must be <, >, = or %")

    conditions: dict[str, Any] = {"metrics": metrics, "operator": operator}
    if attributes:
        conditions["attributes"] = attributes
    payload = {
        "name": name, "status": bool(status), "conditions": conditions,
        "period": int(period), "channel": channel, "recipients": recipients,
    }
    return _simple_write("/alerts/store", payload, "alert", 0, "create_alert",
                         actor, reason or f"alert '{name}' via {channel}", dry_run)


def list_scheduled_reports(per_page: int = 250) -> list[dict]:
    """`POST /scheduled-reports`."""
    return tbx.fetch_all("/scheduled-reports", {}, per_page=per_page)


def create_scheduled_report(
    name: str,
    preset_id: int,
    emails: list[str],
    interval: str = "daily",
    timezone: str | None = None,
    actor: str = "manual",
    reason: str = "",
    dry_run: bool | None = None,
) -> dict:
    """
    `POST /scheduled-reports/store` — the platform emails a saved preset on a
    schedule. `interval` ∈ {daily, weekly, monthly}; `preset_id` comes from
    the `presets` dictionary or `POST /presets`.
    """
    if interval not in ("daily", "weekly", "monthly"):
        raise ValueError("interval must be daily, weekly or monthly")
    payload = {
        "name": name, "preset_id": int(preset_id), "email": emails,
        "interval": interval, "timezone": timezone or tbx.DEFAULT_TZ,
    }
    return _simple_write("/scheduled-reports/store", payload, "scheduled_report",
                         0, "create_scheduled_report", actor,
                         reason or f"{interval} report '{name}'", dry_run)


# ---------------------------------------------------------------------------
# Shared write helper for endpoints that take a self-contained body
# ---------------------------------------------------------------------------

def _simple_write(path: str, payload: dict, entity_type: str, entity_id: int | str,
                  action: str, actor: str, reason: str,
                  dry_run: bool | None,
                  multipart: bool = False,
                  files: dict | None = None) -> dict:
    """
    POST a self-contained body under the same gates as `_apply_update`,
    for endpoints that need no read-modify-write round trip.

    `multipart=True` sends `multipart/form-data` instead of JSON, which the
    spec requires for several write endpoints. `files` carries real file parts
    (used by the bulk `import-values` endpoints); it implies multipart.
    """
    dry_run = _default_dry_run() if dry_run is None else dry_run
    multipart = multipart or files is not None

    if dry_run:
        print(f"{_LOG} DRY_RUN  {action}  POST {path}  "
              f"{json.dumps(payload, default=str)[:250]}")
        _ledger(actor, action, entity_type, entity_id, reason, {}, payload,
                applied=False, dry_run=True)
        return {"path": path, "payload": payload, "applied": False, "dry_run": True}

    if not writes_enabled():
        print(f"{_LOG} REFUSED  {action}  POST {path} — TBX_ALLOW_WRITES is not 1",
              file=sys.stderr)
        _ledger(actor, action, entity_type, entity_id,
                f"{reason} [refused: TBX_ALLOW_WRITES unset]", {}, payload,
                applied=False)
        return {"path": path, "payload": payload, "applied": False,
                "refused": "TBX_ALLOW_WRITES!=1"}

    if multipart:
        result = tbx.post_form(path, fields=payload, files=files)
    else:
        result = tbx.post(path, payload)
    print(f"{_LOG} {action}  POST {path}  applied  {reason}")
    _ledger(actor, action, entity_type, entity_id, reason, {}, payload,
            applied=True, extra={"response": result} if isinstance(result, dict) else None)
    return {"path": path, "payload": payload, "applied": True, "result": result}
