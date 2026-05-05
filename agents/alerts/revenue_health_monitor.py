"""
agents/alerts/revenue_health_monitor.py

Account-wide kill switch. Runs every 6h. If today's revenue is
materially below same-day-of-week 4-week median, halt all
autonomous-apply agents until a human confirms.

This is the meta-safety layer above guardian / floor_nudge / etc.
Each of those has per-change rollback. This catches the case where
AGGREGATE revenue tanks even though no single change crossed its
own threshold (death by a thousand cuts — exactly what May 1
brand_safety_sweep + April floor changes did to us).

Logic
-----
1. Pull today-so-far rev (UTC midnight to now) and prior 4 same-DOW
   day-totals.
2. Compute median of prior 4. Project today's per-hour rate forward
   to a daily-equivalent.
3. If today_projected < median × (1 - HARD_DROP_PCT):
   - Set TB_AUTO_APPLY_KILL_SWITCH=1 in a kill-flag file
   - Slack-page an emergency
   - All apply-mode agents check this flag at start and skip
4. Once a human reviews and revenue stabilises, manually delete
   the flag file or `python -m scripts.revenue_health_monitor --resume`

Apply-mode agents that should respect the flag
----------------------------------------------
- tb_floor_nudge (every 4h)
- revenue_guardian (every 4h)
- aggressive_floor_lift (manual)
- min_floor_sweep (manual)
- brand_safety_sweep (manual)
- blocked_domains_agent (daily)
- placement_status_agent (daily)
- revenue_recovery (manual)

Each calls is_kill_switch_active() at start of run().

Reporting agents (read-only) ignore the flag.
"""
from __future__ import annotations
import os, sys, json, urllib.parse, requests, statistics
from datetime import datetime, timezone, timedelta, date

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from dotenv import load_dotenv; load_dotenv(override=True)
import core.tb_mgmt as tbm

KILL_FILE = os.path.join(_REPO_ROOT, "logs", "tb_kill_switch.flag")
RECS = os.path.join(_REPO_ROOT, "logs", "revenue_health_recs.json")
os.makedirs(os.path.dirname(KILL_FILE), exist_ok=True)

HARD_DROP_PCT      = 0.25   # ≥25% below 4-week DOW median → kill switch
WARN_DROP_PCT      = 0.15   # ≥15% → Slack warning, no kill
MIN_PROJECTED_USD  = 100.0  # don't trigger on tiny days (testing/holidays)

TB = "https://ssp.pgammedia.com/api"


def is_kill_switch_active() -> bool:
    """Public API for other agents to check before applying."""
    return os.path.exists(KILL_FILE)


def activate_kill_switch(reason: str) -> None:
    with open(KILL_FILE, "w") as f:
        json.dump({
            "activated_at": datetime.now(timezone.utc).isoformat(),
            "reason":       reason,
        }, f, indent=2)


def deactivate_kill_switch() -> bool:
    if os.path.exists(KILL_FILE):
        os.remove(KILL_FILE)
        return True
    return False


def _day_revenue(d: date) -> float:
    """Pull total publisher_revenue for a single UTC day."""
    end = d + timedelta(days=1)
    url = f"{TB}/{tbm._get_token()}/report?" + urllib.parse.urlencode([
        ("from", d.isoformat()), ("to", end.isoformat()),
        ("day_group", "total"), ("limit", 5000)])
    r = requests.get(url, timeout=120)
    if not r.ok: return 0.0
    rows = r.json().get("data", r.json()) or []
    return sum(x.get("publisher_revenue", 0.0) or 0.0 for x in rows)


def run(resume: bool = False) -> dict:
    print(f"\n{'='*72}\n  Revenue Health Monitor   "
          f"{datetime.now(timezone.utc).isoformat()}\n{'='*72}")

    if resume:
        ok = deactivate_kill_switch()
        msg = "kill switch CLEARED" if ok else "no kill switch active"
        print(f"  {msg}")
        try:
            from core.slack import post_message
            post_message(f"🟢 *Revenue Health Monitor* — {msg}, autonomous apply re-enabled")
        except Exception: pass
        return {"resumed": ok}

    today = date.today()
    now_utc = datetime.now(timezone.utc)
    hours_into_today = now_utc.hour + now_utc.minute / 60
    if hours_into_today < 4:
        print("  too early in UTC day for projection — skipping")
        return {"skipped": "too_early"}

    today_dow = today.weekday()  # 0=Mon
    prior_days = []
    for w in range(1, 5):  # 4 weeks back
        d = today - timedelta(weeks=w)
        rev = _day_revenue(d)
        prior_days.append((d.isoformat(), rev))
        print(f"  {d.isoformat()}  (DOW {today_dow}, {w}w ago)  rev=${rev:,.2f}")

    median_prior = statistics.median(r for _, r in prior_days if r > 0)
    today_so_far = _day_revenue(today)
    projected = today_so_far / hours_into_today * 24
    drop_pct = (median_prior - projected) / median_prior if median_prior else 0

    print(f"\n  today so far ({hours_into_today:.1f}h):  ${today_so_far:,.2f}")
    print(f"  projected full day:        ${projected:,.2f}")
    print(f"  prior 4-week DOW median:   ${median_prior:,.2f}")
    print(f"  projected vs median:       {drop_pct*100:+.1f}%")

    state = "OK"
    if projected < MIN_PROJECTED_USD:
        state = "SKIPPED_LOW_VOLUME"
    elif drop_pct >= HARD_DROP_PCT:
        state = "KILL_SWITCH"
        reason = (f"projected ${projected:,.0f} vs DOW median ${median_prior:,.0f} "
                  f"({drop_pct*100:.0f}% drop)")
        activate_kill_switch(reason)
        print(f"\n  🚨 KILL SWITCH ACTIVATED  {reason}")
        try:
            from core.slack import post_message
            post_message(
                f"🚨 *KILL SWITCH ACTIVATED* — Revenue Health Monitor\n"
                f"Today projected ${projected:,.0f} vs DOW median "
                f"${median_prior:,.0f} ({drop_pct*100:.0f}% drop)\n"
                f"All autonomous-apply agents will skip until "
                f"`python -m agents.alerts.revenue_health_monitor --resume`")
        except Exception: pass
    elif drop_pct >= WARN_DROP_PCT:
        state = "WARN"
        print(f"\n  ⚠️ WARN  drop {drop_pct*100:.0f}% (threshold {WARN_DROP_PCT*100:.0f}%)")
        try:
            from core.slack import post_message
            post_message(f"⚠️ *Revenue Health Monitor* — {drop_pct*100:.0f}% below DOW median "
                        f"(${projected:,.0f} vs ${median_prior:,.0f})")
        except Exception: pass
    else:
        print(f"\n  ✓ healthy (drop {drop_pct*100:+.0f}% < warn threshold)")

    out = {
        "timestamp":         now_utc.isoformat(),
        "state":             state,
        "today_so_far":      today_so_far,
        "projected":         projected,
        "median_prior":      median_prior,
        "drop_pct":          drop_pct,
        "hours_into_day":    hours_into_today,
        "prior_days":        prior_days,
    }
    with open(RECS, "w") as f:
        json.dump(out, f, indent=2, default=str)
    return out


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--resume", action="store_true",
                    help="clear active kill switch (after human review)")
    args = ap.parse_args()
    run(resume=args.resume)
