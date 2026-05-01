"""
agents/alerts/monthly_forecast.py
──────────────────────────────────────────────────────────────────────────────
Month-end revenue forecast.  Runs on the 1st, 10th, and 20th of each month.

Projection methods
──────────────────
1. Simple run rate   — MTD daily average × days-in-month
2. Weighted          — last-7-day daily average × days remaining + MTD so far
3. Adjusted          — weighted run rate scaled by an end-of-month seasonal
                       index (programmatic budgets typically flush in the
                       final 8–10 days, running ~25% above the monthly mean)

Alert tone
──────────
  1st  → informational baseline ("here's where you start")
 10th  → informational check-in  (one-third through the month)
 20th  → informational if on/above target; CRITICAL ALERT if below 95% target

Delivery
────────
HTML email to ppatel@pgammedia.com only (not the general RECIPIENTS list).
Deduped via /tmp/pgam_forecast_state.json — fires once per scheduled date
(one entry per year-month-day combination).
"""

from __future__ import annotations

import calendar
import json
import math
from datetime import datetime, date, timedelta
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONTHLY_TARGET  = 1_000_000.0      # $1 M
FORECAST_DAYS   = {1, 10, 20}      # day-of-month gates
RECIPIENT       = "ppatel@pgammedia.com"
ALERT_KEY_FMT   = "monthly_forecast_{year}_{month:02d}_{day:02d}"
STATE_FILE      = Path("/tmp/pgam_forecast_state.json")
ET              = pytz.timezone("America/New_York")

BREAKDOWN       = "DATE"
METRICS         = "GROSS_REVENUE,PUB_PAYOUT"

# End-of-month seasonal indices per day-of-month position (1-indexed bucket).
# Programmatic budgets typically flush in the final week/10 days.
# Buckets: days 1-7, 8-14, 15-21, 22-end.
# Normalised so the weighted average over a 30-day month ≈ 1.0.
_SEASONAL_BUCKETS = {
    "early":   0.88,   # days 1-7
    "mid":     0.96,   # days 8-14
    "late":    1.08,   # days 15-21
    "flush":   1.30,   # days 22+  (budget flush)
}


# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

def _imports():
    from core.api    import fetch, sf
    from core.config import SENDGRID_KEY, SENDER_EMAIL
    from intelligence.claude_analyst import analyze_monthly_forecast
    return fetch, sf, SENDGRID_KEY, SENDER_EMAIL, analyze_monthly_forecast


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as exc:
        print(f"[monthly_forecast] State write failed: {exc}")


def _already_sent(year: int, month: int, day: int) -> bool:
    state = _load_state()
    key   = ALERT_KEY_FMT.format(year=year, month=month, day=day)
    return state.get(key, False)


def _mark_sent(year: int, month: int, day: int) -> None:
    state = _load_state()
    key   = ALERT_KEY_FMT.format(year=year, month=month, day=day)
    state[key] = True
    _save_state(state)


# ---------------------------------------------------------------------------
# Safe float helper
# ---------------------------------------------------------------------------

def _sf(v) -> float:
    if v is None:
        return 0.0
    try:
        f = float(v)
        return 0.0 if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Seasonal index helper
# ---------------------------------------------------------------------------

def _seasonal_index(day_of_month: int) -> float:
    """Return the seasonal multiplier for a given day-of-month."""
    if day_of_month <= 7:
        return _SEASONAL_BUCKETS["early"]
    if day_of_month <= 14:
        return _SEASONAL_BUCKETS["mid"]
    if day_of_month <= 21:
        return _SEASONAL_BUCKETS["late"]
    return _SEASONAL_BUCKETS["flush"]


def _remaining_seasonal_weight(start_day: int, days_in_month: int) -> float:
    """
    Average seasonal index for days [start_day .. days_in_month] (inclusive).
    Used to scale the remaining-day projection.
    """
    total = sum(_seasonal_index(d) for d in range(start_day, days_in_month + 1))
    n     = days_in_month - start_day + 1
    return total / n if n > 0 else 1.0


# ---------------------------------------------------------------------------
# Core calculations
# ---------------------------------------------------------------------------

def _compute_recap(daily_rows: list[dict], month_start: date, days_in_month: int) -> dict:
    """
    Summarize a fully-completed month (used on day 1 of the next month).
    No projections — just final numbers + best/worst day + vs-target gap.
    """
    daily:   dict[str, float] = {}
    payouts: dict[str, float] = {}
    for row in daily_rows:
        d   = str(row.get("DATE", ""))
        rev = _sf(row.get("GROSS_REVENUE"))
        pay = _sf(row.get("PUB_PAYOUT"))
        if d:
            daily[d]   = daily.get(d, 0.0) + rev
            payouts[d] = payouts.get(d, 0.0) + pay

    total_rev    = sum(daily.values())
    total_payout = sum(payouts.values())
    margin_pct   = (total_rev - total_payout) / total_rev * 100 if total_rev > 0 else 0.0
    daily_avg    = total_rev / days_in_month if days_in_month > 0 else 0.0
    gap          = total_rev - MONTHLY_TARGET
    gap_pct      = gap / MONTHLY_TARGET * 100 if MONTHLY_TARGET > 0 else 0.0
    sorted_days  = sorted(daily.items(), key=lambda x: x[1], reverse=True)

    return {
        "total_revenue":  round(total_rev, 2),
        "total_payout":   round(total_payout, 2),
        "margin_pct":     round(margin_pct, 1),
        "daily_avg":      round(daily_avg, 2),
        "days_in_month":  days_in_month,
        "days_with_data": len(daily),
        "monthly_target": MONTHLY_TARGET,
        "gap_vs_target":  round(gap, 2),
        "gap_pct":        round(gap_pct, 1),
        "hit_target":     total_rev >= MONTHLY_TARGET * 0.95,
        "best_day":       {"date": sorted_days[0][0],  "revenue": round(sorted_days[0][1], 2)}  if sorted_days else {},
        "worst_day":      {"date": sorted_days[-1][0], "revenue": round(sorted_days[-1][1], 2)} if sorted_days else {},
    }


def _compute_projections(
    daily_rows:    list[dict],
    month_start:   date,
    today_et:      date,
    days_in_month: int,
) -> dict:
    """
    Given DATE-breakdown rows for [month_start, today], compute all three
    projections plus supporting metrics.

    Returns a dict with keys the caller and Claude can use directly.
    """
    days_elapsed  = (today_et - month_start).days + 1   # inclusive
    days_remaining = days_in_month - days_elapsed

    # ── Build daily series ────────────────────────────────────────────────────
    daily: dict[str, float] = {}        # date_str → gross_revenue
    payouts: dict[str, float] = {}
    for row in daily_rows:
        d   = str(row.get("DATE", ""))
        rev = _sf(row.get("GROSS_REVENUE"))
        pay = _sf(row.get("PUB_PAYOUT"))
        if d:
            daily[d]   = daily.get(d, 0.0) + rev
            payouts[d] = payouts.get(d, 0.0) + pay

    mtd_revenue = sum(daily.values())
    mtd_payout  = sum(payouts.values())
    mtd_margin  = (mtd_revenue - mtd_payout) / mtd_revenue * 100 if mtd_revenue > 0 else 0.0

    # Daily run rate (simple: MTD average)
    simple_daily_rate = mtd_revenue / days_elapsed if days_elapsed > 0 else 0.0

    # Last-7-day rate (forward-looking weight)
    cutoff_7d  = today_et - timedelta(days=6)
    last7_days = [
        rev for d_str, rev in daily.items()
        if d_str >= cutoff_7d.strftime("%Y-%m-%d")
    ]
    n7 = len(last7_days)
    weighted_daily_rate = sum(last7_days) / n7 if n7 > 0 else simple_daily_rate

    # ── Projection 1: Simple run rate ─────────────────────────────────────────
    proj_simple = round(simple_daily_rate * days_in_month, 2)

    # ── Projection 2: Weighted (last-7-day rate × remaining + MTD) ────────────
    proj_weighted = round(mtd_revenue + weighted_daily_rate * days_remaining, 2)

    # ── Projection 3: Adjusted (seasonal factor on remaining days) ────────────
    # Scale the weighted daily rate by the average seasonal index for remaining days
    remaining_start    = today_et.day + 1   # tomorrow
    avg_seasonal_rest  = _remaining_seasonal_weight(remaining_start, days_in_month)
    adjusted_daily_rate = weighted_daily_rate * avg_seasonal_rest
    proj_adjusted = round(mtd_revenue + adjusted_daily_rate * days_remaining, 2)

    # ── vs Target ─────────────────────────────────────────────────────────────
    def _vs_target(proj: float) -> dict:
        gap     = proj - MONTHLY_TARGET
        gap_pct = gap / MONTHLY_TARGET * 100
        return {
            "projection":     proj,
            "gap_vs_target":  round(gap, 2),
            "gap_pct":        round(gap_pct, 1),
            "on_track":       proj >= MONTHLY_TARGET * 0.95,
        }

    # Revenue needed per day for the rest of the month to hit target
    rev_needed_rest = max(0.0, MONTHLY_TARGET - mtd_revenue)
    needed_per_day  = rev_needed_rest / days_remaining if days_remaining > 0 else 0.0

    # Best and worst days so far
    sorted_days = sorted(daily.items(), key=lambda x: x[1], reverse=True)

    return {
        "mtd_revenue":         round(mtd_revenue, 2),
        "mtd_payout":          round(mtd_payout, 2),
        "mtd_margin_pct":      round(mtd_margin, 1),
        "days_elapsed":        days_elapsed,
        "days_remaining":      days_remaining,
        "days_in_month":       days_in_month,
        "simple_daily_rate":   round(simple_daily_rate, 2),
        "weighted_daily_rate": round(weighted_daily_rate, 2),
        "last7_n_days":        n7,
        "monthly_target":      MONTHLY_TARGET,
        "needed_per_day":      round(needed_per_day, 2),
        "revenue_needed_rest": round(rev_needed_rest, 2),
        "proj_simple":         _vs_target(proj_simple),
        "proj_weighted":       _vs_target(proj_weighted),
        "proj_adjusted":       _vs_target(proj_adjusted),
        "best_day":            {"date": sorted_days[0][0],  "revenue": round(sorted_days[0][1], 2)}  if sorted_days else {},
        "worst_day":           {"date": sorted_days[-1][0], "revenue": round(sorted_days[-1][1], 2)} if sorted_days else {},
        "daily_series":        sorted(daily.items()),
    }


# ---------------------------------------------------------------------------
# Claude integration
# ---------------------------------------------------------------------------

def _claude_forecast_analysis(projections: dict, day_of_month: int) -> dict:
    """
    Ask Claude for: confidence level, biggest risk, required daily run rate.
    Returns a dict with keys: confidence, confidence_pct, biggest_risk,
    needed_daily_commentary, actions, summary.
    Falls back to a data-driven stub if Claude is unavailable.
    """
    try:
        from intelligence.claude_analyst import analyze_monthly_forecast
        return analyze_monthly_forecast(projections, day_of_month)
    except Exception as exc:
        print(f"[monthly_forecast] Claude failed: {exc}")

    # Fallback
    best_proj  = max(
        projections["proj_simple"]["projection"],
        projections["proj_weighted"]["projection"],
        projections["proj_adjusted"]["projection"],
    )
    on_track = best_proj >= MONTHLY_TARGET * 0.95
    gap_pct  = (best_proj - MONTHLY_TARGET) / MONTHLY_TARGET * 100
    needed   = projections["needed_per_day"]

    if on_track:
        conf_pct = min(90, int(50 + gap_pct * 3))
        confidence = "high" if conf_pct >= 75 else "medium"
        risk = "Run-rate slowdown in the final week of the month."
    else:
        conf_pct = max(15, int(50 + gap_pct * 3))
        confidence = "low"
        risk = f"Current run rate is insufficient — need ${needed:,.0f}/day to close the gap."

    return {
        "confidence":              confidence,
        "confidence_pct":          conf_pct,
        "biggest_risk":            risk,
        "needed_daily_commentary": f"${needed:,.0f}/day required for the remaining {projections['days_remaining']} days.",
        "actions":                 ["Review top publisher floors.", "Check demand partner configuration."],
        "summary":                 "Claude analysis unavailable — data-driven fallback shown.",
    }


# ---------------------------------------------------------------------------
# HTML builder
# ---------------------------------------------------------------------------

_BG     = "#0f1117"
_CARD   = "#1a1d27"
_BORDER = "#2a2d3a"
_TEXT   = "#e2e8f0"
_MUTED  = "#94a3b8"
_GREEN  = "#4ade80"
_RED    = "#f87171"
_YELLOW = "#fbbf24"
_BLUE   = "#60a5fa"
_ORANGE = "#fb923c"


def _css() -> str:
    return f"""
    body {{
        margin:0; padding:0; background:{_BG};
        font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
        color:{_TEXT}; font-size:14px; line-height:1.6;
    }}
    .wrapper {{ max-width:680px; margin:0 auto; padding:24px 16px; }}
    .header {{
        background:linear-gradient(135deg,#1e2235 0%,#252a3d 100%);
        border:1px solid {_BORDER}; border-radius:12px;
        padding:28px 32px; margin-bottom:20px;
    }}
    .header h1 {{ margin:0 0 4px; font-size:22px; font-weight:700; color:{_TEXT}; }}
    .header .sub {{ color:{_MUTED}; font-size:13px; margin:0; }}
    .alert-banner {{
        background:#300; border:1px solid #7f1d1d;
        border-radius:10px; padding:16px 20px; margin-bottom:16px;
        font-weight:600; color:{_RED}; font-size:15px;
    }}
    .card {{
        background:{_CARD}; border:1px solid {_BORDER};
        border-radius:10px; padding:20px 24px; margin-bottom:16px;
    }}
    .card h2 {{
        margin:0 0 16px; font-size:13px; font-weight:600;
        color:{_MUTED}; text-transform:uppercase; letter-spacing:0.06em;
    }}
    .metric-grid {{
        display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-bottom:12px;
    }}
    .metric {{ background:#0f1117; border-radius:8px; padding:14px 16px; }}
    .metric .label {{ font-size:11px; color:{_MUTED}; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px; }}
    .metric .value {{ font-size:20px; font-weight:700; color:{_TEXT}; }}
    .metric .sub {{ font-size:12px; margin-top:2px; color:{_MUTED}; }}
    .green  {{ color:{_GREEN};  }}
    .red    {{ color:{_RED};    }}
    .yellow {{ color:{_YELLOW}; }}
    .blue   {{ color:{_BLUE};   }}
    .orange {{ color:{_ORANGE}; }}
    .muted  {{ color:{_MUTED};  }}
    .proj-row {{
        display:flex; align-items:center; justify-content:space-between;
        padding:10px 0; border-bottom:1px solid {_BORDER};
    }}
    .proj-row:last-child {{ border-bottom:none; }}
    .proj-label {{ font-weight:600; font-size:14px; }}
    .proj-amount {{ font-size:18px; font-weight:700; }}
    .proj-badge {{
        font-size:11px; font-weight:700; text-transform:uppercase;
        padding:3px 8px; border-radius:4px; letter-spacing:0.04em;
    }}
    .badge-green  {{ background:#052e16; color:{_GREEN}; }}
    .badge-red    {{ background:#300;    color:{_RED};   }}
    .badge-yellow {{ background:#2d1d00; color:{_YELLOW}; }}
    .progress-bg {{ background:#0f1117; border-radius:4px; height:10px; overflow:hidden; margin:10px 0 4px; }}
    .progress-fill {{ height:100%; border-radius:4px; }}
    .confidence-row {{
        display:flex; align-items:center; gap:12px; margin-bottom:12px;
    }}
    .confidence-dot {{
        width:14px; height:14px; border-radius:50%; flex-shrink:0;
    }}
    ul {{ margin:8px 0; padding-left:20px; }}
    li {{ margin-bottom:6px; color:{_TEXT}; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; color:{_TEXT}; }}
    th {{
        text-align:left; color:{_MUTED}; font-weight:600;
        font-size:11px; text-transform:uppercase; letter-spacing:0.05em;
        padding:0 0 8px; border-bottom:1px solid {_BORDER};
    }}
    td {{ padding:10px 8px 10px 0; border-bottom:1px solid #1e2235; color:{_TEXT}; vertical-align:top; }}
    tr:last-child td {{ border-bottom:none; }}
    strong {{ color:{_TEXT}; }}
    .footer {{ color:{_MUTED}; font-size:11px; text-align:center; padding-top:12px; }}
    """


def _color_for_proj(gap_pct: float) -> tuple[str, str, str]:
    """Return (text_class, badge_class, badge_text) based on gap vs target."""
    if gap_pct >= 5:
        return "green", "badge-green", "On Track"
    if gap_pct >= 0:
        return "green", "badge-green", "On Track"
    if gap_pct >= -10:
        return "yellow", "badge-yellow", "At Risk"
    return "red", "badge-red", "Below Target"


def _progress_color(gap_pct: float) -> str:
    if gap_pct >= 0:
        return _GREEN
    if gap_pct >= -10:
        return _YELLOW
    return _RED


def _build_html(
    day_of_month:    int,
    month_label:     str,
    date_label:      str,
    projections:     dict,
    claude:          dict,
    is_critical:     bool,
    ll_projections:  dict | None = None,
    tb_projections:  dict | None = None,
) -> str:
    ll_projections = ll_projections or {}
    tb_projections = tb_projections or {}
    mtd_rev   = projections["mtd_revenue"]
    mtd_margin = projections["mtd_margin_pct"]
    simple_dr = projections["simple_daily_rate"]
    w7_dr     = projections["weighted_daily_rate"]
    days_el   = projections["days_elapsed"]
    days_rem  = projections["days_remaining"]
    days_tot  = projections["days_in_month"]
    needed_pd = projections["needed_per_day"]
    rev_need  = projections["revenue_needed_rest"]

    target   = MONTHLY_TARGET
    mtd_pct  = mtd_rev / target * 100 if target > 0 else 0.0
    progress_pct = min(mtd_pct, 100.0)
    progress_color = _progress_color(
        projections["proj_adjusted"]["gap_pct"]
    )

    # Checkpoint label
    checkpoint = {1: "Month Start", 10: "10-Day Check", 20: "20-Day Alert"}.get(day_of_month, "Forecast")

    # Critical alert banner
    alert_banner = ""
    if is_critical:
        worst_gap = min(
            projections["proj_simple"]["gap_pct"],
            projections["proj_weighted"]["gap_pct"],
            projections["proj_adjusted"]["gap_pct"],
        )
        alert_banner = f"""
        <div class="alert-banner">
          ⚠ CRITICAL — 20-Day Forecast Alert<br>
          <span style="font-weight:400;font-size:13px;">
            All three projections are tracking below the ${target:,.0f} target
            (worst case: {worst_gap:+.1f}%).  Immediate action required.
          </span>
        </div>"""

    # Per-platform MTD breakdown line
    ll_mtd = ll_projections.get("mtd_revenue", 0)
    tb_mtd = tb_projections.get("mtd_revenue", 0)
    platform_split = ""
    if ll_mtd > 0 or tb_mtd > 0:
        platform_split = (f'<div style="font-size:11px;color:{_MUTED};margin-top:4px;">'
                          f'LL ${ll_mtd:,.0f} · TB ${tb_mtd:,.0f}</div>')

    # MTD metrics (combined headline + per-platform sub-line)
    mtd_html = f"""
    <div class="card">
      <h2>Month-to-Date — {month_label}</h2>
      <div class="metric-grid">
        <div class="metric">
          <div class="label">MTD Revenue (Combined)</div>
          <div class="value">${mtd_rev:,.0f}</div>
          <div class="sub">{days_el}d of {days_tot}d elapsed</div>
          {platform_split}
        </div>
        <div class="metric">
          <div class="label">MTD Margin</div>
          <div class="value {'green' if mtd_margin >= 30 else 'yellow' if mtd_margin >= 20 else 'red'}">{mtd_margin:.1f}%</div>
        </div>
        <div class="metric">
          <div class="label">Daily Run Rate</div>
          <div class="value">${simple_dr:,.0f}</div>
          <div class="sub">Last-7d: ${w7_dr:,.0f}</div>
        </div>
      </div>
      <div class="progress-bg">
        <div class="progress-fill" style="width:{progress_pct:.1f}%;background:{progress_color};"></div>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:12px;color:{_MUTED};">
        <span>${mtd_rev:,.0f} MTD ({mtd_pct:.0f}% of target)</span>
        <span>${target:,.0f} target</span>
      </div>
    </div>"""

    # Three projections — combined headline + LL/TB sub-line
    def _proj_row(label: str, desc: str, proj_key: str) -> str:
        proj_data = projections[proj_key]
        proj  = proj_data["projection"]
        gap   = proj_data["gap_pct"]
        cls, badge_cls, badge_txt = _color_for_proj(gap)
        sign  = "+" if gap >= 0 else ""
        ll_p = (ll_projections.get(proj_key) or {}).get("projection", 0)
        tb_p = (tb_projections.get(proj_key) or {}).get("projection", 0)
        platform_line = ""
        if ll_p > 0 or tb_p > 0:
            platform_line = (f'<div style="font-size:11px;color:{_MUTED};margin-top:2px;">'
                             f'LL ${ll_p:,.0f} · TB ${tb_p:,.0f}</div>')
        return f"""
        <div class="proj-row">
          <div>
            <div class="proj-label">{label}</div>
            <div style="font-size:12px;color:{_MUTED};">{desc}</div>
          </div>
          <div style="text-align:right;">
            <div class="proj-amount {cls}">${proj:,.0f}</div>
            {platform_line}
            <div style="margin-top:4px;">
              <span class="proj-badge {badge_cls}">{badge_txt}</span>
              <span style="font-size:12px;color:{_MUTED};margin-left:6px;">{sign}{gap:.1f}% vs target</span>
            </div>
          </div>
        </div>"""

    proj_html = f"""
    <div class="card">
      <h2>Month-End Projections</h2>
      {_proj_row(
          "Simple Run Rate",
          f"MTD daily average (${simple_dr:,.0f}/day) × {days_tot} days",
          "proj_simple"
      )}
      {_proj_row(
          "Weighted (Last 7 Days)",
          f"Last-{projections['last7_n_days']}d rate (${w7_dr:,.0f}/day) × {days_rem}d remaining + MTD",
          "proj_weighted"
      )}
      {_proj_row(
          "Seasonally Adjusted",
          "Weighted rate scaled by end-of-month budget-flush index",
          "proj_adjusted"
      )}
    </div>"""

    # Claude assessment
    conf         = claude.get("confidence", "medium")
    conf_pct     = claude.get("confidence_pct", 50)
    risk         = claude.get("biggest_risk", "—")
    need_comment = claude.get("needed_daily_commentary", "")
    actions      = claude.get("actions", [])
    summary      = claude.get("summary", "")

    conf_color = {"high": _GREEN, "medium": _YELLOW, "low": _RED}.get(conf, _YELLOW)

    actions_html = "".join(f"<li>{a}</li>" for a in actions) if actions else ""

    claude_html = f"""
    <div class="card" style="border-color:#3730a3;background:linear-gradient(135deg,#1a1d27 0%,#1e1b2e 100%);">
      <h2 style="color:#a78bfa;">Claude's Forecast Assessment</h2>
      <div class="confidence-row">
        <div class="confidence-dot" style="background:{conf_color};"></div>
        <div>
          <span style="font-weight:700;font-size:16px;color:{conf_color};">
            {conf.upper()} CONFIDENCE
          </span>
          <span style="color:{_MUTED};font-size:13px;margin-left:8px;">({conf_pct}% probability of hitting target)</span>
        </div>
      </div>
      <div style="margin-bottom:12px;">
        <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px;">Biggest Risk</div>
        <div style="color:{_RED};">{risk}</div>
      </div>
      <div style="margin-bottom:12px;">
        <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px;">Required to Hit Target</div>
        <div>
          <span style="font-size:18px;font-weight:700;color:{_YELLOW};">${needed_pd:,.0f}/day</span>
          <span style="color:{_MUTED};font-size:13px;margin-left:8px;">for {days_rem} remaining days (${rev_need:,.0f} total)</span>
        </div>
        {f'<div style="font-size:13px;color:{_MUTED};margin-top:4px;">{need_comment}</div>' if need_comment else ''}
      </div>
      {f'<div><div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:4px;">Recommended Actions</div><ul style="margin:0;padding-left:18px;">{actions_html}</ul></div>' if actions_html else ''}
      {f'<div style="margin-top:14px;padding-top:12px;border-top:1px solid #2a2d3a;font-size:13px;color:{_MUTED};">{summary}</div>' if summary else ''}
    </div>"""

    # Best / worst day callout
    best  = projections.get("best_day",  {})
    worst = projections.get("worst_day", {})
    days_html = ""
    if best and worst:
        days_html = f"""
    <div class="card">
      <h2>Day Extremes (MTD)</h2>
      <div style="display:flex;gap:16px;">
        <div class="metric" style="flex:1;">
          <div class="label">Best Day</div>
          <div class="value green">${best['revenue']:,.0f}</div>
          <div class="sub">{best['date']}</div>
        </div>
        <div class="metric" style="flex:1;">
          <div class="label">Worst Day</div>
          <div class="value red">${worst['revenue']:,.0f}</div>
          <div class="sub">{worst['date']}</div>
        </div>
        <div class="metric" style="flex:1;">
          <div class="label">Day Range</div>
          <div class="value">${best['revenue'] - worst['revenue']:,.0f}</div>
          <div class="sub">high-low spread</div>
        </div>
      </div>
    </div>"""

    footer = f"""
    <div class="footer">
      PGAM Intelligence &nbsp;·&nbsp; Monthly Forecast &nbsp;·&nbsp;
      {checkpoint} &nbsp;·&nbsp; {date_label}
    </div>"""

    header = f"""
    <div class="header">
      <h1>Monthly Revenue Forecast</h1>
      <p class="sub">{month_label} &nbsp;·&nbsp; {checkpoint} &nbsp;·&nbsp; {date_label}</p>
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>PGAM Monthly Forecast — {month_label}</title>
  <style>{_css()}</style>
</head>
<body>
  <div class="wrapper">
    {header}
    {alert_banner}
    {mtd_html}
    {proj_html}
    {claude_html}
    {days_html}
    {footer}
  </div>
</body>
</html>"""


def _build_recap_html(
    prev_month_label: str,
    ll_recap:         dict,
    tb_recap:         dict,
    combined_recap:   dict,
    cur_month_label:  str,
    cur_day:          int,
    cur_days_total:   int,
    cur_today_rev:    float,
    date_label:       str,
) -> str:
    """
    Day-1-of-month email: recap of the month that just closed (LL + TB +
    Combined) + a placeholder for the new month. No projections, no Claude
    narrative.
    """
    # Combined headline drives status colour + progress bar
    rev      = combined_recap.get("total_revenue", 0)
    target   = combined_recap.get("monthly_target", MONTHLY_TARGET)
    gap_pct  = combined_recap.get("gap_pct", 0)
    hit      = combined_recap.get("hit_target", False)
    pct_of_target = (rev / target * 100) if target > 0 else 0

    if hit:
        status_color, status_label = _GREEN, "Hit Target"
    elif pct_of_target >= 80:
        status_color, status_label = _YELLOW, "Near Target"
    else:
        status_color, status_label = _RED, "Below Target"

    bar_pct   = min(pct_of_target, 100)
    bar_color = status_color

    # ── Per-platform table ───────────────────────────────────────────────────
    def _row(label: str, recap: dict, sub: str = "") -> str:
        if not recap or recap.get("total_revenue", 0) <= 0:
            return f"""
        <tr>
          <td><strong>{label}</strong>{f'<div class="muted" style="font-size:11px;">{sub}</div>' if sub else ''}</td>
          <td colspan="3" class="muted">No data</td>
        </tr>"""
        r_rev    = recap.get("total_revenue", 0)
        r_margin = recap.get("margin_pct", 0)
        r_avg    = recap.get("daily_avg", 0)
        r_gap    = recap.get("gap_pct", 0)
        r_sign   = "+" if r_gap >= 0 else ""
        gap_color = _GREEN if r_gap >= -5 else (_YELLOW if r_gap >= -20 else _RED)
        return f"""
        <tr>
          <td><strong>{label}</strong>{f'<div class="muted" style="font-size:11px;">{sub}</div>' if sub else ''}</td>
          <td>${r_rev:,.0f}</td>
          <td>${r_avg:,.0f}<div class="muted" style="font-size:11px;">/day</div></td>
          <td>{r_margin:.1f}%</td>
          <td style="color:{gap_color};">{r_sign}{r_gap:.1f}%</td>
        </tr>"""

    platform_rows = (
        _row("LL (Limelight)", ll_recap) +
        _row("TB (Teqblaze)",  tb_recap,
             sub=f"{tb_recap.get('days_with_data', 0)}/{tb_recap.get('days_in_month', 0)} days") +
        _row("Combined",       combined_recap)
    )

    # Best / worst day per platform (where available)
    extremes_html = ""
    extreme_blocks = []
    for label, plat_recap in (("LL", ll_recap), ("TB", tb_recap)):
        best  = plat_recap.get("best_day", {})
        worst = plat_recap.get("worst_day", {})
        if not (best and worst):
            continue
        spread = best["revenue"] - worst["revenue"]
        extreme_blocks.append(f"""
        <div style="margin-bottom:14px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">
            {label} day extremes
          </div>
          <div style="display:flex;gap:12px;">
            <div class="metric" style="flex:1;">
              <div class="label">Best</div>
              <div class="value" style="color:{_GREEN};">${best['revenue']:,.0f}</div>
              <div class="sub">{best['date']}</div>
            </div>
            <div class="metric" style="flex:1;">
              <div class="label">Worst</div>
              <div class="value" style="color:{_RED};">${worst['revenue']:,.0f}</div>
              <div class="sub">{worst['date']}</div>
            </div>
            <div class="metric" style="flex:1;">
              <div class="label">Range</div>
              <div class="value">${spread:,.0f}</div>
              <div class="sub">high-low</div>
            </div>
          </div>
        </div>""")
    if extreme_blocks:
        extremes_html = f"""
    <div class="card">
      <h2>Day Extremes — {prev_month_label}</h2>
      {''.join(extreme_blocks)}
    </div>"""

    recap_html = f"""
    <div class="card" style="border-color:{status_color};border-width:1px 1px 1px 4px;">
      <h2>{prev_month_label} — Final</h2>
      <table>
        <thead><tr>
          <th>Platform</th><th>Revenue</th><th>Daily Avg</th><th>Margin</th><th>vs Target</th>
        </tr></thead>
        <tbody>{platform_rows}</tbody>
      </table>
      <div style="margin-top:16px;">
        <div class="progress-bar-bg">
          <div class="progress-bar-fill" style="width:{bar_pct:.1f}%;background:{bar_color};"></div>
        </div>
        <div style="font-size:12px;color:{_MUTED};margin-top:4px;">
          Combined: ${rev:,.0f} of ${target:,.0f} ({pct_of_target:.1f}% of monthly target) — {status_label}
        </div>
      </div>
    </div>"""

    new_month_html = f"""
    <div class="card">
      <h2>{cur_month_label} — Now Underway</h2>
      <div style="display:flex;gap:16px;align-items:flex-start;">
        <div class="metric" style="flex:1;">
          <div class="label">Day</div>
          <div class="value">{cur_day} of {cur_days_total}</div>
          <div class="sub">month elapsed</div>
        </div>
        <div class="metric" style="flex:1;">
          <div class="label">Today (so far)</div>
          <div class="value">${cur_today_rev:,.0f}</div>
          <div class="sub">preliminary</div>
        </div>
      </div>
      <div style="margin-top:14px;font-size:13px;color:{_MUTED};line-height:1.6;">
        Forecasting resumes at the <strong style="color:{_TEXT};">10-day checkpoint</strong>.
        Day-1 data is too thin to project meaningfully — a single day extrapolated
        to a full month would mislead more than inform.
      </div>
    </div>"""

    header = f"""
    <div class="header">
      <h1>Monthly Recap</h1>
      <p class="sub">{prev_month_label} closed &nbsp;·&nbsp; {date_label}</p>
    </div>"""

    footer = f"""
    <div class="footer">
      PGAM Intelligence &nbsp;·&nbsp; Monthly Recap &nbsp;·&nbsp; {date_label}
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>PGAM Monthly Recap — {prev_month_label}</title>
  <style>{_css()}</style>
</head>
<body>
  <div class="wrapper">
    {header}
    {recap_html}
    {new_month_html}
    {extremes_html}
    {footer}
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Email delivery
# ---------------------------------------------------------------------------

def _send_email(
    html_body:    str,
    subject:      str,
    sendgrid_key: str,
    sender:       str,
) -> bool:
    """Send HTML email to RECIPIENT via SendGrid REST API."""
    if not sendgrid_key or not sender:
        print("[monthly_forecast] SendGrid key or sender not configured.")
        return False

    import urllib.request

    payload = {
        "personalizations": [{"to": [{"email": RECIPIENT}]}],
        "from":    {"email": sender},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_body}],
    }
    data = json.dumps(payload).encode("utf-8")
    req  = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=data,
        headers={
            "Authorization": f"Bearer {sendgrid_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.getcode() in (200, 202):
                print(f"[monthly_forecast] Email sent to {RECIPIENT}.")
                return True
            print(f"[monthly_forecast] Unexpected status: {resp.getcode()}")
            return False
    except urllib.request.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"[monthly_forecast] SendGrid error {exc.code}: {body}")
        return False
    except Exception as exc:
        print(f"[monthly_forecast] Email delivery failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _combine_projections(ll: dict, tb: dict, today_day: int, days_in_month: int) -> dict:
    """
    Sum two platform projection dicts into a combined view, recomputing all
    derived rates and the three projection methods. Same return shape as
    _compute_projections() so the existing _build_html consumer just works.
    """
    if not ll and not tb:
        return {}
    ll = ll or {}
    tb = tb or {}

    mtd_rev     = ll.get("mtd_revenue", 0)     + tb.get("mtd_revenue", 0)
    mtd_payout  = ll.get("mtd_payout", 0)      + tb.get("mtd_payout", 0)
    mtd_margin  = (mtd_rev - mtd_payout) / mtd_rev * 100 if mtd_rev > 0 else 0
    days_el     = ll.get("days_elapsed")   or tb.get("days_elapsed")   or today_day
    days_rem    = ll.get("days_remaining") or tb.get("days_remaining") or (days_in_month - today_day)

    simple_dr   = mtd_rev / days_el if days_el > 0 else 0
    weighted_dr = ll.get("weighted_daily_rate", 0) + tb.get("weighted_daily_rate", 0)

    # Adjusted = weighted × avg seasonal index for remaining days
    avg_seasonal = _remaining_seasonal_weight(today_day + 1, days_in_month)
    adjusted_dr  = weighted_dr * avg_seasonal

    proj_simple   = round(simple_dr   * days_in_month, 2)
    proj_weighted = round(mtd_rev     + weighted_dr * days_rem, 2)
    proj_adjusted = round(mtd_rev     + adjusted_dr * days_rem, 2)

    def _vs_target(p):
        gap = p - MONTHLY_TARGET
        return {
            "projection":    p,
            "gap_vs_target": round(gap, 2),
            "gap_pct":       round(gap / MONTHLY_TARGET * 100, 1) if MONTHLY_TARGET > 0 else 0,
            "on_track":      p >= MONTHLY_TARGET * 0.95,
        }

    rev_needed_rest = max(0.0, MONTHLY_TARGET - mtd_rev)
    needed_pd       = rev_needed_rest / days_rem if days_rem > 0 else 0

    return {
        "mtd_revenue":         round(mtd_rev, 2),
        "mtd_payout":          round(mtd_payout, 2),
        "mtd_margin_pct":      round(mtd_margin, 1),
        "days_elapsed":        days_el,
        "days_remaining":      days_rem,
        "days_in_month":       days_in_month,
        "simple_daily_rate":   round(simple_dr, 2),
        "weighted_daily_rate": round(weighted_dr, 2),
        "last7_n_days":        max(ll.get("last7_n_days", 0), tb.get("last7_n_days", 0)),
        "monthly_target":      MONTHLY_TARGET,
        "needed_per_day":      round(needed_pd, 2),
        "revenue_needed_rest": round(rev_needed_rest, 2),
        "proj_simple":         _vs_target(proj_simple),
        "proj_weighted":       _vs_target(proj_weighted),
        "proj_adjusted":       _vs_target(proj_adjusted),
        "best_day":            ll.get("best_day", {}),   # LL extremes only — TB extremes shown separately if needed
        "worst_day":           ll.get("worst_day", {}),
    }


def _combine_recaps(ll: dict, tb: dict) -> dict:
    """Sum two platform recaps into a combined view. Re-derives ratios."""
    if not ll and not tb:
        return {}
    ll = ll or {}
    tb = tb or {}
    rev = ll.get("total_revenue", 0)  + tb.get("total_revenue", 0)
    pay = ll.get("total_payout", 0)   + tb.get("total_payout", 0)
    days_in_month = ll.get("days_in_month") or tb.get("days_in_month") or 0
    margin = ((rev - pay) / rev * 100) if rev > 0 else 0.0
    daily_avg = (rev / days_in_month) if days_in_month > 0 else 0.0
    gap = rev - MONTHLY_TARGET
    gap_pct = (gap / MONTHLY_TARGET * 100) if MONTHLY_TARGET > 0 else 0.0
    return {
        "total_revenue":  round(rev, 2),
        "total_payout":   round(pay, 2),
        "margin_pct":     round(margin, 1),
        "daily_avg":      round(daily_avg, 2),
        "days_in_month":  days_in_month,
        "monthly_target": MONTHLY_TARGET,
        "gap_vs_target":  round(gap, 2),
        "gap_pct":        round(gap_pct, 1),
        "hit_target":     rev >= MONTHLY_TARGET * 0.95,
    }


def _run_month_recap(now_et, fetch, sendgrid_key, sender, date_label: str) -> bool:
    """
    Day-1-of-month flow: recap of the month that just closed.
    Skips projections + Claude entirely — single-day extrapolation is noise.
    Surfaces LL + TB + Combined for honest cross-platform reporting.
    """
    from core import tb_data
    today_et = now_et.date()

    # Previous month boundaries
    if now_et.month == 1:
        prev_year, prev_month = now_et.year - 1, 12
    else:
        prev_year, prev_month = now_et.year, now_et.month - 1
    prev_month_start = date(prev_year, prev_month, 1)
    prev_days_in_month = calendar.monthrange(prev_year, prev_month)[1]
    prev_month_end = date(prev_year, prev_month, prev_days_in_month)
    prev_month_label = prev_month_start.strftime("%B %Y")
    cur_month_label  = now_et.strftime("%B %Y")
    cur_days_total   = calendar.monthrange(now_et.year, now_et.month)[1]
    pms_str = prev_month_start.strftime("%Y-%m-%d")
    pme_str = prev_month_end.strftime("%Y-%m-%d")

    print(f"[monthly_forecast] Day-1 recap path — fetching {prev_month_label} "
          f"({prev_month_start} → {prev_month_end})")

    # ── LL ──────────────────────────────────────────────────────────────────
    try:
        ll_rows = fetch(BREAKDOWN, METRICS, pms_str, pme_str)
    except Exception as exc:
        print(f"[monthly_forecast] LL prev-month fetch failed: {exc}")
        ll_rows = []
    ll_recap = _compute_recap(ll_rows or [], prev_month_start, prev_days_in_month)
    print(f"[monthly_forecast] LL: ${ll_recap['total_revenue']:,.0f}  "
          f"margin {ll_recap['margin_pct']:.1f}%  best {ll_recap.get('best_day', {}).get('date','—')} "
          f"${ll_recap.get('best_day', {}).get('revenue', 0):,.0f}")

    # ── TB ──────────────────────────────────────────────────────────────────
    # TB API times out on long DATE ranges; fetch_daily_rows issues 30 single-
    # day calls with sleeps. Slow (~2-3 min) but reliable for once-a-month run.
    print(f"[monthly_forecast] TB: fetching {prev_days_in_month} daily rows…")
    try:
        tb_rows = tb_data.fetch_daily_rows(pms_str, pme_str)
    except Exception as exc:
        print(f"[monthly_forecast] TB prev-month fetch failed: {exc}")
        tb_rows = []
    tb_recap = _compute_recap(tb_rows or [], prev_month_start, prev_days_in_month)
    print(f"[monthly_forecast] TB: ${tb_recap['total_revenue']:,.0f}  "
          f"margin {tb_recap['margin_pct']:.1f}%  ({tb_recap.get('days_with_data', 0)}/{prev_days_in_month} days)")

    # ── Combined ────────────────────────────────────────────────────────────
    combined_recap = _combine_recaps(ll_recap, tb_recap)
    print(f"[monthly_forecast] Combined: ${combined_recap.get('total_revenue', 0):,.0f}  "
          f"vs target {combined_recap.get('gap_pct', 0):+.1f}%")

    if (ll_recap["total_revenue"] + tb_recap["total_revenue"]) <= 0:
        print("[monthly_forecast] No data on either platform. Exiting recap.")
        return False

    # ── Today-so-far (LL + TB best-effort) ──────────────────────────────────
    today_str = today_et.strftime("%Y-%m-%d")
    cur_today_rev = 0.0
    try:
        cur_ll = fetch(BREAKDOWN, METRICS, today_str, today_str)
        cur_today_rev += sum(_sf(r.get("GROSS_REVENUE")) for r in (cur_ll or []))
    except Exception as exc:
        print(f"[monthly_forecast] LL today-so-far fetch failed (non-fatal): {exc}")
    try:
        cur_tb = tb_data.fetch_summary(today_str, today_str)
        cur_today_rev += cur_tb.get("revenue", 0) if cur_tb else 0
    except Exception as exc:
        print(f"[monthly_forecast] TB today-so-far fetch failed (non-fatal): {exc}")

    html = _build_recap_html(
        prev_month_label = prev_month_label,
        ll_recap         = ll_recap,
        tb_recap         = tb_recap,
        combined_recap   = combined_recap,
        cur_month_label  = cur_month_label,
        cur_day          = now_et.day,
        cur_days_total   = cur_days_total,
        cur_today_rev    = cur_today_rev,
        date_label       = date_label,
    )

    rev = combined_recap.get("total_revenue", 0)
    pct_of_target = (rev / MONTHLY_TARGET * 100) if MONTHLY_TARGET > 0 else 0
    subject = (f"PGAM Monthly Recap — {prev_month_label} | "
               f"${rev:,.0f} final ({pct_of_target:.0f}% of target)")

    return _send_email(html, subject, sendgrid_key, sender)


def run():
    now_et       = datetime.now(ET)
    day_of_month = now_et.day
    month        = now_et.month
    year         = now_et.year

    # ── Gate: only run on scheduled days ────────────────────────────────────
    if day_of_month not in FORECAST_DAYS:
        print(f"[monthly_forecast] Day {day_of_month} is not a forecast day. Exiting.")
        return

    # ── Gate: deduplication ──────────────────────────────────────────────────
    if _already_sent(year, month, day_of_month):
        print(f"[monthly_forecast] Already sent for {year}-{month:02d}-{day_of_month:02d}. Exiting.")
        return

    fetch, sf, sendgrid_key, sender, _claude_fn = _imports()

    today_et    = now_et.date()
    date_label  = now_et.strftime("%A, %B %-d, %Y")

    # ── Day 1: recap-only path (last month closed, this month just started) ──
    if day_of_month == 1:
        ok = _run_month_recap(now_et, fetch, sendgrid_key, sender, date_label)
        if ok:
            _mark_sent(year, month, day_of_month)
        return

    # ── Date ranges ──────────────────────────────────────────────────────────
    month_start   = date(year, month, 1)
    days_in_month = calendar.monthrange(year, month)[1]

    start_str = month_start.strftime("%Y-%m-%d")
    end_str   = today_et.strftime("%Y-%m-%d")
    month_label = now_et.strftime("%B %Y")

    print(f"[monthly_forecast] Fetching {BREAKDOWN} {start_str} → {end_str}…")

    # ── Fetch LL ────────────────────────────────────────────────────────────
    try:
        ll_rows = fetch(BREAKDOWN, METRICS, start_str, end_str)
    except Exception as exc:
        print(f"[monthly_forecast] LL fetch failed: {exc}")
        ll_rows = []
    ll_projections = _compute_projections(ll_rows or [], month_start, today_et, days_in_month)

    # ── Fetch TB (per-day, slow but reliable) ───────────────────────────────
    from core import tb_data
    print(f"[monthly_forecast] TB: fetching {(today_et - month_start).days + 1} daily rows "
          f"(per-day calls, ~{(today_et - month_start).days * 5}s)…")
    try:
        tb_rows = tb_data.fetch_daily_rows(start_str, end_str)
    except Exception as exc:
        print(f"[monthly_forecast] TB fetch failed: {exc}")
        tb_rows = []
    tb_projections = _compute_projections(tb_rows or [], month_start, today_et, days_in_month)

    # ── Combine ─────────────────────────────────────────────────────────────
    projections = _combine_projections(ll_projections, tb_projections, day_of_month, days_in_month)

    if projections.get("mtd_revenue", 0) <= 0:
        print("[monthly_forecast] No data on either platform. Exiting.")
        return

    print(
        f"[monthly_forecast] MTD: LL ${ll_projections['mtd_revenue']:,.0f} + "
        f"TB ${tb_projections['mtd_revenue']:,.0f} = "
        f"${projections['mtd_revenue']:,.0f}  |  "
        f"Adjusted projection: ${projections['proj_adjusted']['projection']:,.0f}"
    )

    # ── Determine alert mode ─────────────────────────────────────────────────
    # Critical on 20th only if ALL three projections are below 95% of target
    all_below = all(
        not projections[k]["on_track"]
        for k in ("proj_simple", "proj_weighted", "proj_adjusted")
    )
    is_critical = (day_of_month == 20) and all_below

    # ── Claude assessment (on combined view) ─────────────────────────────────
    claude_result = _claude_forecast_analysis(projections, day_of_month)

    # ── Build email ──────────────────────────────────────────────────────────
    html = _build_html(
        day_of_month   = day_of_month,
        month_label    = month_label,
        date_label     = date_label,
        projections    = projections,
        ll_projections = ll_projections,
        tb_projections = tb_projections,
        claude         = claude_result,
        is_critical    = is_critical,
    )

    checkpoint_label = {1: "Month Start", 10: "10-Day Check", 20: "20-Day Forecast"}.get(day_of_month, "Forecast")
    alert_prefix     = "⚠ CRITICAL ALERT — " if is_critical else ""
    subject = (
        f"{alert_prefix}PGAM Monthly Forecast — "
        f"{month_label} {checkpoint_label} | "
        f"${projections['proj_adjusted']['projection']:,.0f} projected"
    )

    # ── Send ─────────────────────────────────────────────────────────────────
    ok = _send_email(html, subject, sendgrid_key, sender)
    if ok:
        _mark_sent(year, month, day_of_month)
    else:
        print("[monthly_forecast] Email failed — not marking as sent (will retry next run).")


if __name__ == "__main__":
    run()
