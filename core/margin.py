"""
core/margin.py

Shared publisher-margin lookup used by optimizers + alerts.

Margin definition
-----------------
    margin = (GROSS_REVENUE - PUB_PAYOUT) / GROSS_REVENUE

GROSS_REVENUE = what demand (DSPs) paid us
PUB_PAYOUT    = what we pay the supply partner (publisher)
Margin        = what PGAM keeps

Threshold: 30% is the minimum acceptable margin. Below that the economics
of adding new demand to that publisher stop making sense — we're just
amplifying a bad rev share.

Public API
----------
    get_publisher_margins(lookback_days=30) → {pub_id: {rev, pay, margin_pct, wins}}
    is_healthy_margin(pub_id, cache=None, threshold=30.0) → bool
    MARGIN_HEALTHY_THRESHOLD (default 30%, env LL_MARGIN_MIN overrides)
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

from core.api import fetch
from core.ll_report import _sf

MARGIN_HEALTHY_THRESHOLD: float = float(
    os.environ.get("LL_MARGIN_MIN", "30.0")
)


def get_publisher_margins(lookback_days: int = 30,
                           min_revenue: float = 50.0) -> dict[int, dict]:
    """Return {pub_id: {name, rev, pay, margin_pct, wins}} for the window.

    Publishers with revenue < min_revenue are excluded — margin is too noisy
    to be meaningful on tiny revenue bases.
    """
    end = date.today()
    start = end - timedelta(days=lookback_days)
    try:
        rows = fetch(
            "PUBLISHER",
            "GROSS_REVENUE,PUB_PAYOUT,WINS",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        print(f"[margin] WARNING: fetch failed: {e}")
        return {}

    out: dict[int, dict] = {}
    for r in rows:
        try:
            pid = int(_sf(r.get("PUBLISHER_ID", 0)))
            rev = _sf(r.get("GROSS_REVENUE", 0))
            pay = _sf(r.get("PUB_PAYOUT", 0))
            wins = _sf(r.get("WINS", 0))
            if not pid or rev < min_revenue:
                continue
            margin = (rev - pay) / rev * 100.0 if rev > 0 else 0.0
            out[pid] = {
                "name": r.get("PUBLISHER_NAME", ""),
                "rev": rev,
                "pay": pay,
                "margin_pct": round(margin, 2),
                "wins": wins,
            }
        except Exception:
            continue
    return out


def is_healthy_margin(pub_id: int,
                       cache: Optional[dict] = None,
                       threshold: float = MARGIN_HEALTHY_THRESHOLD) -> bool:
    """Return True iff this publisher's 30-day margin ≥ threshold.

    Pass `cache` (the result of get_publisher_margins) for repeated lookups
    to avoid hitting the API multiple times per agent run.
    """
    if cache is None:
        cache = get_publisher_margins()
    entry = cache.get(int(pub_id))
    if not entry:
        # No revenue history — treat as healthy so new publishers aren't blocked
        return True
    return entry["margin_pct"] >= threshold
