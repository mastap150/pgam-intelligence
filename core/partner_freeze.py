"""
core/partner_freeze.py

Single source of truth for demand partners that are FROZEN — no automation
in this codebase is allowed to write anything to their demands or wirings.

Why a shared module (not per-agent blocklists)
----------------------------------------------
Prior pattern was per-agent name-substring blocklists (see
config_health_scanner.QPS_DEMAND_NAME_BLOCKLIST). That worked for the QPS
auto-raise check but didn't cover schain auto-enable, floor writers, margin
adjusters, unpausers, or wiring writers — each one had to be edited
separately. When Priyesh said "freeze Unruly, don't touch anything" on
2026-07-13, coordinating 10+ writers to all block Unruly consistently was
error-prone. This module centralizes that.

Every writer that mutates an LL demand or a publisher biddingpreferences
entry MUST call `check_and_skip(demand_id_or_dp, actor)` at the top of the
write path. If the partner is frozen, it returns True and the writer must
early-return (no side effects).

How to freeze a partner
-----------------------
1. Add the partner_id to FROZEN_PARTNERS below (canonical, no substring hazard).
2. Document the reason inline — future readers need to know why.

How to unfreeze
---------------
1. Remove the partner_id from FROZEN_PARTNERS.
2. All writers automatically start touching that partner again.

Currently frozen
----------------
- Unruly (dp=5) — added 2026-07-13. Unruly compliance team keeps flagging PGAM;
  any automated change risks tripping another flag. Full freeze until
  compliance root-cause is fixed (see current Unruly investigation).
  Note: this does NOT pause the demands — they keep bidding as they are.
  It only stops US from modifying their config.
"""
from __future__ import annotations

from typing import Optional

from core import ll_mgmt

FROZEN_PARTNERS: set[int] = {
    5,   # Unruly — 2026-07-13, compliance flags
}

# Cache demand_id → demand_partner mapping. Built lazily on first lookup
# so this module doesn't hit the LL API at import time.
_did_to_dp_cache: Optional[dict[int, int]] = None


def _get_did_to_dp() -> dict[int, int]:
    """Return {demand_id: demand_partner_id}. Cached for the process."""
    global _did_to_dp_cache
    if _did_to_dp_cache is None:
        try:
            resp = ll_mgmt._get("/v1/demands")
            items = resp.get("items", resp) if isinstance(resp, dict) else resp
            _did_to_dp_cache = {int(d["id"]): d.get("demandPartner") for d in items if d.get("id")}
        except Exception:
            _did_to_dp_cache = {}
    return _did_to_dp_cache


def is_frozen(*, demand_id: Optional[int] = None,
              demand_partner: Optional[int] = None,
              demand_name: Optional[str] = None) -> bool:
    """True if the target demand belongs to a frozen partner.

    Accepts any of demand_id, demand_partner, or demand_name — whichever
    the caller has on hand. When demand_id is given, resolves via the LL
    cache. demand_name is a substring fallback (name-based match to the
    canonical partner name).
    """
    if demand_partner is not None:
        return demand_partner in FROZEN_PARTNERS
    if demand_id is not None:
        dp = _get_did_to_dp().get(int(demand_id))
        if dp is None:
            return False
        return dp in FROZEN_PARTNERS
    if demand_name:
        # Backstop: partner name substring match. Kept narrow to avoid
        # false positives from other DSPs whose demands transit these SSPs.
        nl = demand_name.lower()
        # Unruly's demand names all start with "Unruly " or "OTTA Unruly "
        if 5 in FROZEN_PARTNERS and ("unruly" in nl or "tremor" in nl):
            return True
    return False


def check_and_skip(*, demand_id: Optional[int] = None,
                   demand_partner: Optional[int] = None,
                   demand_name: Optional[str] = None,
                   actor: str = "unknown") -> bool:
    """Return True if the writer should early-return without side effects.

    Emits a print for observability so freeze-skips show up in scheduler logs.
    Callers must honor the return value — this module doesn't have a way
    to prevent a subsequent _put(); the caller has to short-circuit.
    """
    if is_frozen(demand_id=demand_id, demand_partner=demand_partner, demand_name=demand_name):
        target = f"d={demand_id}" if demand_id else f"dp={demand_partner}" if demand_partner else f"name={demand_name}"
        print(f"[partner_freeze] {actor} SKIPPED {target}: partner frozen")
        return True
    return False


def frozen_demand_ids() -> set[int]:
    """Return the set of demand IDs belonging to any frozen partner.

    Useful for writers that want to filter their candidate list up-front
    rather than checking one at a time.
    """
    dmap = _get_did_to_dp()
    return {did for did, dp in dmap.items() if dp in FROZEN_PARTNERS}
