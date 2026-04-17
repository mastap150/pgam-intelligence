"""
agents/alerts/margin_health.py

Daily publisher-margin health agent.

Runs every day at 08:15 ET. Pulls 30-day margin per active publisher,
compares to yesterday's snapshot, and posts a Slack alert if:

  * Any publisher is currently below the 30% healthy threshold
  * Any publisher's margin has dropped more than DROP_ALERT_PP percentage
    points since yesterday (early-warning signal for demand-mix shifts
    or rev-share changes that haven't been flagged yet)

State is kept in logs/margin_history.json — a rolling 30-day log of
per-publisher margins keyed by date. This lets us also produce weekly
trend reports later without re-querying.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, date

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

from core.margin import (
    get_publisher_margins,
    MARGIN_HEALTHY_THRESHOLD,
)

HISTORY_PATH = os.path.join(_REPO_ROOT, "logs", "margin_history.json")
DROP_ALERT_PP = 2.0   # alert on day-over-day drop ≥ this many percentage points
TREND_WINDOW = 30     # keep this many days of history per publisher


def _today() -> str:
    return date.today().strftime("%Y-%m-%d")


def load_history() -> dict:
    if not os.path.exists(HISTORY_PATH):
        return {}
    try:
        with open(HISTORY_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_history(h: dict) -> None:
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(HISTORY_PATH, "w") as f:
        json.dump(h, f, indent=2)


def update_history(history: dict, margins: dict) -> None:
    today = _today()
    for pid, m in margins.items():
        key = str(pid)
        entry = history.setdefault(key, {"name": m.get("name", ""), "history": {}})
        entry["name"] = m.get("name", entry.get("name", ""))
        entry["history"][today] = m["margin_pct"]
        # Trim to rolling window
        if len(entry["history"]) > TREND_WINDOW:
            keep = sorted(entry["history"].keys())[-TREND_WINDOW:]
            entry["history"] = {k: entry["history"][k] for k in keep}


def compute_drops(history: dict, margins: dict) -> list[dict]:
    """Return list of publishers whose margin dropped ≥ DROP_ALERT_PP since
    yesterday (or the most recent prior data point)."""
    today = _today()
    drops = []
    for pid, m in margins.items():
        key = str(pid)
        entry = history.get(key)
        if not entry or not entry.get("history"):
            continue
        prior_dates = sorted([d for d in entry["history"] if d != today])
        if not prior_dates:
            continue
        prev = entry["history"][prior_dates[-1]]
        curr = m["margin_pct"]
        delta = curr - prev
        if delta <= -DROP_ALERT_PP:
            drops.append({
                "pub_id": pid, "name": m.get("name", ""),
                "prev": prev, "curr": curr, "delta_pp": round(delta, 2),
                "prev_date": prior_dates[-1],
            })
    return drops


def run() -> None:
    print(f"\n{'='*70}")
    print(f"  Margin Health Check — {_today()}")
    print(f"{'='*70}\n")

    margins = get_publisher_margins(lookback_days=30, min_revenue=50.0)
    if not margins:
        print("[margin_health] no data — skipping")
        return

    history = load_history()
    drops = compute_drops(history, margins)
    update_history(history, margins)
    save_history(history)

    below_threshold = sorted(
        [(pid, m) for pid, m in margins.items() if m["margin_pct"] < MARGIN_HEALTHY_THRESHOLD],
        key=lambda x: -x[1]["rev"],
    )

    print(f"  Publishers tracked:       {len(margins)}")
    print(f"  Below {MARGIN_HEALTHY_THRESHOLD:.0f}%:              {len(below_threshold)}")
    print(f"  Day-over-day drops ≥{DROP_ALERT_PP}pp: {len(drops)}")

    if below_threshold:
        print(f"\n  Below-threshold publishers:")
        for pid, m in below_threshold:
            print(f"    {pid:>10}  {m['name'][:45]:<45}  margin {m['margin_pct']:>5.1f}%  rev ${m['rev']:,.0f}")

    if drops:
        print(f"\n  Drops since yesterday:")
        for d in drops:
            print(f"    {d['pub_id']:>10}  {d['name'][:40]:<40}  {d['prev']:.1f}% → {d['curr']:.1f}%  ({d['delta_pp']:+.1f}pp)")

    # Slack
    try:
        from core.slack import post_message
    except Exception:
        return

    if not below_threshold and not drops:
        # Healthy day — short green message
        total_rev = sum(m["rev"] for m in margins.values())
        total_pay = sum(m["pay"] for m in margins.values())
        overall = (total_rev - total_pay) / max(total_rev, 1) * 100
        try:
            post_message(
                f"💚 *Margin Health* — all {len(margins)} publishers ≥ "
                f"{MARGIN_HEALTHY_THRESHOLD:.0f}% · overall {overall:.1f}%"
            )
        except Exception:
            pass
        return

    lines = [f"💸 *Margin Health Report* — {_today()}"]
    if below_threshold:
        lines.append(f"*{len(below_threshold)} publishers below {MARGIN_HEALTHY_THRESHOLD:.0f}% threshold:*")
        for pid, m in below_threshold[:10]:
            lines.append(f"  • `{m['name'][:30]}` — {m['margin_pct']:.1f}% · ${m['rev']:,.0f}/30d")
    if drops:
        lines.append(f"\n*{len(drops)} publishers dropped ≥{DROP_ALERT_PP}pp since {drops[0]['prev_date']}:*")
        for d in drops[:8]:
            lines.append(f"  ⬇ `{d['name'][:30]}` — {d['prev']:.1f}% → {d['curr']:.1f}% ({d['delta_pp']:+.1f}pp)")
    try:
        post_message("\n".join(lines))
    except Exception:
        pass


if __name__ == "__main__":
    run()
