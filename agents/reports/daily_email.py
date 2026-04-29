"""
agents/reports/daily_email.py
──────────────────────────────────────────────────────────────────────────────
Daily HTML email report for PGAM Intelligence.

Sends once per day at ~7 AM ET via SendGrid.  Aggregates data from:
  • core API (revenue pacing, floor gaps, opp/fill)
  • agents/reports/floor_elasticity  → get_optimization_data()
  • agents/alerts/ctv_optimizer      → export_ctv_section()
  • intelligence/claude_analyst      → synthesize_daily_brief()

Deduped via /tmp/pgam_email_state.json (date-keyed, same pattern as slack.py).
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

import pytz

# ---------------------------------------------------------------------------
# Lazy imports – keep startup fast and failures isolated
# ---------------------------------------------------------------------------

def _core():
    from core.api import fetch, yesterday, today, n_days_ago, sf, pct, fmt_usd, fmt_n
    from core.config import (
        SENDGRID_KEY, SENDER_EMAIL, RECIPIENTS,
        THRESHOLDS,
    )
    return fetch, yesterday, today, n_days_ago, sf, pct, fmt_usd, fmt_n, \
           SENDGRID_KEY, SENDER_EMAIL, RECIPIENTS, THRESHOLDS


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATE_FILE   = Path("/tmp/pgam_email_state.json")
ET           = pytz.timezone("America/New_York")
SEND_HOUR_ET = 7          # Send at or after 7 AM ET

# API breakdown / metric strings
BD_PUBLISHER   = "PUBLISHER"
BD_DATE        = "DATE"
BD_DATE_PUB    = "DATE,PUBLISHER"
BD_BUNDLE      = "BUNDLE"
METRICS_REV    = "GROSS_REVENUE,BIDS,WINS,IMPRESSIONS,OPPORTUNITIES"
METRICS_FLOOR  = "GROSS_REVENUE,BIDS,WINS,OPPORTUNITIES,AVG_FLOOR_PRICE,AVG_BID_PRICE"


# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------

def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _already_sent(date_str: str) -> bool:
    if not STATE_FILE.exists():
        return False
    try:
        data = json.loads(STATE_FILE.read_text())
        return data.get("sent_date") == date_str
    except Exception:
        return False


def _mark_sent(date_str: str) -> None:
    try:
        STATE_FILE.write_text(json.dumps({"sent_date": date_str}))
    except Exception as exc:
        print(f"[daily_email] State write failed: {exc}")


# ---------------------------------------------------------------------------
# Data collection helpers
# ---------------------------------------------------------------------------

def _collect_topline(yesterday_fn, n_days_ago_fn) -> dict:
    """
    Cross-platform supply rollup for YESTERDAY (full day) — LL + TB + Combined.

    Compares yesterday vs:
      - Day-before-yesterday  (DoD)
      - 7-day daily average ending day-before-yesterday (clean WoW baseline)

    Also returns top movers (per-publisher revenue delta vs the 7d-avg
    baseline) tagged with their platform.
    """
    from core import ll_data, tb_data

    yest    = yesterday_fn()
    d2_ago  = n_days_ago_fn(2)
    d8_ago  = n_days_ago_fn(8)   # baseline range start (8 days ago through 2 days ago = 7 days)

    # ── LL ────────────────────────────────────────────────────────────────────
    ll_yest    = ll_data.fetch_summary(yest, yest)
    ll_d2      = ll_data.fetch_summary(d2_ago, d2_ago)
    ll_7d_sum  = ll_data.fetch_summary(d8_ago, d2_ago)
    ll_7d_avg  = ll_data.avg_per_day(ll_7d_sum, 7)

    ll_pubs_yest = ll_data.fetch_top_publishers(yest, yest, n=30)
    ll_pubs_7d   = ll_data.fetch_top_publishers(d8_ago, d2_ago, n=80)

    # ── TB ────────────────────────────────────────────────────────────────────
    # TB API times out on long ranges + has shorter history than LL, so we use
    # per-day summaries for the 7d window and skip the 7d publisher breakdown.
    # Movers fall back to DoD baseline (day-before-yesterday) when 7d is empty.
    tb_yest = tb_data.fetch_summary(yest, yest)
    tb_data.sleep_between()
    tb_d2   = tb_data.fetch_summary(d2_ago, d2_ago)
    tb_data.sleep_between()
    tb_7d_sum = tb_data.fetch_summary_by_day(d8_ago, d2_ago)
    tb_data.sleep_between()
    tb_7d_avg = tb_data.avg_per_day(tb_7d_sum, 7)

    tb_pubs_yest = tb_data.fetch_top_publishers(yest, yest, n=30)
    tb_data.sleep_between()
    tb_pubs_d2   = tb_data.fetch_top_publishers(d2_ago, d2_ago, n=80)

    # ── Combined totals ───────────────────────────────────────────────────────
    combined_yest = _combine(ll_yest, tb_yest)
    combined_d2   = _combine(ll_d2,   tb_d2)
    combined_7d   = _combine(ll_7d_avg, tb_7d_avg)

    # ── Movers ────────────────────────────────────────────────────────────────
    # LL uses 7d-avg baseline; TB uses DoD (day-before) baseline since 7d
    # publisher fetches time out and TB has limited history. If the TB baseline
    # fetch comes back empty we skip TB movers entirely rather than fabricate
    # a "NEW" tag for every TB publisher.
    ll_movers = _compute_movers(ll_pubs_yest, ll_pubs_7d, "LL",
                                baseline_label="7d avg", baseline_divisor=7)
    tb_movers = (_compute_movers(tb_pubs_yest, tb_pubs_d2, "TB",
                                  baseline_label="day-before", baseline_divisor=1)
                 if tb_pubs_d2 else [])
    movers = ll_movers + tb_movers
    movers.sort(key=lambda m: abs(m["delta"]), reverse=True)

    return {
        "date":     yest,
        "ll":       {"yest": ll_yest, "d2": ll_d2, "d7avg": ll_7d_avg},
        "tb":       {"yest": tb_yest, "d2": tb_d2, "d7avg": tb_7d_avg},
        "combined": {"yest": combined_yest, "d2": combined_d2, "d7avg": combined_7d},
        "movers":   movers[:6],
    }


def _combine(a: dict, b: dict) -> dict:
    """Sum two summary dicts and recompute derived ratios."""
    if not a and not b:
        return {}
    a = a or {}
    b = b or {}
    rev  = a.get("revenue", 0)     + b.get("revenue", 0)
    pay  = a.get("payout", 0)      + b.get("payout", 0)
    imp  = a.get("impressions", 0) + b.get("impressions", 0)
    wins = a.get("wins", 0)        + b.get("wins", 0)
    bids = a.get("bids", 0)        + b.get("bids", 0)
    return {
        "revenue": rev, "payout": pay, "impressions": imp, "wins": wins, "bids": bids,
        "margin":   ((rev - pay) / rev * 100) if rev > 0 else 0.0,
        "ecpm":     (rev / imp * 1000) if imp > 0 else 0.0,
        "win_rate": (wins / bids * 100) if bids > 0 else 0.0,
    }


def _compute_movers(yest_pubs: list, baseline_pubs: list, platform: str,
                    baseline_label: str = "7d avg",
                    baseline_divisor: int = 7,
                    min_abs_delta: float = 50.0) -> list[dict]:
    """
    For each pub in yest, compute revenue delta vs a baseline.

    baseline_divisor: how many days the baseline_pubs revenue sums cover
                      (7 for a 7-day range, 1 for a single-day DoD baseline).
    Pubs with no baseline history are returned with delta_pct=None and tagged
    as "new" rather than fabricating a meaningless +0% / +inf%.
    """
    base_by_name = {p["name"]: p["revenue"] / baseline_divisor for p in baseline_pubs}
    movers = []
    for p in yest_pubs:
        baseline = base_by_name.get(p["name"], 0.0)
        delta    = p["revenue"] - baseline
        if abs(delta) < min_abs_delta:
            continue
        is_new   = baseline <= 0
        delta_pct = None if is_new else ((p["revenue"] - baseline) / baseline * 100)
        movers.append({
            "platform":       platform,
            "publisher":      p["name"],
            "yest_rev":       p["revenue"],
            "baseline":       baseline,
            "baseline_label": baseline_label,
            "delta":          delta,
            "delta_pct":      delta_pct,
            "is_new":         is_new,
        })
    return movers


def _delta_pct(now: float, base: float) -> float | None:
    if base is None or base == 0:
        return None
    return (now - base) / base * 100.0


def _collect_revenue_summary(fetch, yesterday_fn, today_fn, n_days_ago_fn,
                              sf, pct, fmt_usd, fmt_n) -> dict:
    """Fetch today + yesterday publisher-level data and build a summary dict."""
    yest  = yesterday_fn()
    tod   = today_fn()
    w7ago = n_days_ago_fn(7)

    try:
        rows_today  = fetch(BD_PUBLISHER, METRICS_REV, tod,  tod)
        rows_yest   = fetch(BD_PUBLISHER, METRICS_REV, yest, yest)
        rows_7d     = fetch(BD_PUBLISHER, METRICS_REV, w7ago, yest)
    except Exception as exc:
        print(f"[daily_email] Revenue fetch failed: {exc}")
        return {}

    def _sum(rows: list, field: str) -> float:
        return sum(sf(r.get(field, 0)) for r in rows)

    rev_today  = _sum(rows_today, "GROSS_REVENUE")
    rev_yest   = _sum(rows_yest,  "GROSS_REVENUE")
    imps_today = _sum(rows_today, "IMPRESSIONS")
    imps_yest  = _sum(rows_yest,  "IMPRESSIONS")
    bids_today = _sum(rows_today, "BIDS")
    wins_today = _sum(rows_today, "WINS")
    rev_7d     = _sum(rows_7d,    "GROSS_REVENUE")

    now_et  = datetime.now(ET)
    hour_et = now_et.hour + now_et.minute / 60.0
    exp_rev = rev_yest * (max(hour_et, 1) / 24.0) if rev_yest > 0 else 0.0
    pacing  = (rev_today / exp_rev * 100.0) if exp_rev > 0 else None

    return {
        "date":           tod,
        "revenue_today":  round(rev_today, 2),
        "revenue_yest":   round(rev_yest, 2),
        "expected_rev":   round(exp_rev, 2),
        "pacing_pct":     round(pacing, 1) if pacing is not None else None,
        "impressions_today": int(imps_today),
        "impressions_yest":  int(imps_yest),
        "win_rate_pct":   round(pct(wins_today, bids_today), 1),
        "revenue_7d_avg": round(rev_7d / 7.0, 2) if rev_7d else 0.0,
        "publisher_count": len({r.get("PUBLISHER_NAME", r.get("publisher","")) for r in rows_today if r.get("PUBLISHER_NAME") or r.get("publisher")}),
    }


def _collect_floor_gaps(fetch, yesterday_fn, sf) -> dict:
    """Collect top raise / lower floor gap candidates for the report."""
    yest = yesterday_fn()
    try:
        rows = fetch("PUBLISHER", METRICS_FLOOR, yest, yest)
    except Exception as exc:
        print(f"[daily_email] Floor gap fetch failed: {exc}")
        return {"raise": [], "lower": []}

    raise_cands = []
    lower_cands = []
    for r in rows:
        bids      = sf(r.get("BIDS",            0))
        wins      = sf(r.get("WINS",            0))
        revenue   = sf(r.get("GROSS_REVENUE",   0))
        avg_floor = sf(r.get("AVG_FLOOR_PRICE", 0))
        avg_bid   = sf(r.get("AVG_BID_PRICE",   0))
        pub       = r.get("PUBLISHER_NAME") or r.get("publisher", "Unknown")

        if bids < 5_000 or avg_floor <= 0 or avg_bid <= 0:
            continue

        ratio = avg_bid / avg_floor
        if ratio >= 2.0:
            raise_cands.append({
                "publisher":    pub,
                "avg_floor":    round(avg_floor, 3),
                "avg_bid":      round(avg_bid, 3),
                "recommended":  round(avg_bid, 3),
                "revenue":      round(revenue, 2),
                "ratio":        round(ratio, 2),
            })
        elif ratio <= 0.5:
            lower_cands.append({
                "publisher":    pub,
                "avg_floor":    round(avg_floor, 3),
                "avg_bid":      round(avg_bid, 3),
                "recommended":  round(avg_bid * 1.1, 3),
                "revenue":      round(revenue, 2),
                "ratio":        round(ratio, 2),
            })

    raise_cands.sort(key=lambda x: x["revenue"], reverse=True)
    lower_cands.sort(key=lambda x: x["revenue"], reverse=True)
    return {"raise": raise_cands[:5], "lower": lower_cands[:5]}


def _collect_opp_fill(fetch, today_fn, n_days_ago_fn, sf, pct) -> dict:
    """Fetch MTD opportunity / fill rate metrics."""
    tod        = today_fn()
    month_start = tod[:8] + "01"

    try:
        rows = fetch(BD_DATE, METRICS_REV, month_start, tod)
    except Exception as exc:
        print(f"[daily_email] Opp/fill fetch failed: {exc}")
        return {}

    def _sum(field: str) -> float:
        return sum(sf(r.get(field, 0)) for r in rows)

    opps = _sum("OPPORTUNITIES")
    imps = _sum("IMPRESSIONS")
    rev  = _sum("GROSS_REVENUE")

    fill_rate = imps / opps if opps > 0 else 0.0
    threshold = 0.0005

    return {
        "mtd_opportunities":  int(opps),
        "mtd_impressions":    int(imps),
        "mtd_revenue":        round(rev, 2),
        "fill_rate":          round(fill_rate, 6),
        "fill_rate_pct":      round(fill_rate * 100, 4),
        "threshold_pct":      threshold * 100,
        "above_threshold":    fill_rate >= threshold,
        "imps_needed":        max(0, int(opps * threshold - imps)),
    }


# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------

# Colour palette
_BG      = "#0f1117"
_CARD    = "#1a1d27"
_BORDER  = "#2a2d3a"
_TEXT    = "#e2e8f0"
_MUTED   = "#94a3b8"
_GREEN   = "#4ade80"
_RED     = "#f87171"
_YELLOW  = "#fbbf24"
_BLUE    = "#60a5fa"
_PURPLE  = "#a78bfa"


def _css() -> str:
    return f"""
    body {{
        margin: 0; padding: 0; background: {_BG};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
        color: {_TEXT}; font-size: 14px; line-height: 1.6;
    }}
    .wrapper {{ max-width: 700px; margin: 0 auto; padding: 24px 16px; }}
    .header {{
        background: linear-gradient(135deg, #1e2235 0%, #252a3d 100%);
        border: 1px solid {_BORDER}; border-radius: 12px;
        padding: 28px 32px; margin-bottom: 20px;
    }}
    .header h1 {{ margin: 0 0 4px; font-size: 22px; font-weight: 700; color: {_TEXT}; }}
    .header .sub {{ color: {_MUTED}; font-size: 13px; margin: 0; }}
    .card {{
        background: {_CARD}; border: 1px solid {_BORDER};
        border-radius: 10px; padding: 20px 24px; margin-bottom: 16px;
    }}
    .card h2 {{
        margin: 0 0 16px; font-size: 15px; font-weight: 600;
        color: {_MUTED}; text-transform: uppercase; letter-spacing: 0.06em;
    }}
    .metric-grid {{
        display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px;
        margin-bottom: 12px;
    }}
    .metric {{ background: #0f1117; border-radius: 8px; padding: 14px 16px; }}
    .metric .label {{ font-size: 11px; color: {_MUTED}; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 4px; }}
    .metric .value {{ font-size: 20px; font-weight: 700; color: {_TEXT}; }}
    .metric .change {{ font-size: 12px; margin-top: 2px; }}
    .green {{ color: {_GREEN}; }}
    .red   {{ color: {_RED}; }}
    .yellow {{ color: {_YELLOW}; }}
    .blue  {{ color: {_BLUE}; }}
    .purple {{ color: {_PURPLE}; }}
    .muted {{ color: {_MUTED}; }}
    .progress-bar-bg {{
        background: #0f1117; border-radius: 4px; height: 8px;
        overflow: hidden; margin: 8px 0 4px;
    }}
    .progress-bar-fill {{ height: 100%; border-radius: 4px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th {{
        text-align: left; color: {_MUTED}; font-weight: 600;
        font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em;
        padding: 0 0 8px; border-bottom: 1px solid {_BORDER};
    }}
    td {{ padding: 8px 0; border-bottom: 1px solid #1e2235; }}
    tr:last-child td {{ border-bottom: none; }}
    .badge {{
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 600; text-transform: uppercase;
    }}
    .badge-green  {{ background: #052e16; color: {_GREEN}; }}
    .badge-red    {{ background: #300; color: {_RED}; }}
    .badge-yellow {{ background: #2d1d00; color: {_YELLOW}; }}
    .badge-blue   {{ background: #0c1a2e; color: {_BLUE}; }}
    .brief-para {{ color: {_TEXT}; margin: 0 0 14px; line-height: 1.7; }}
    .brief-para:last-child {{ margin: 0; }}
    .footer {{ color: {_MUTED}; font-size: 11px; text-align: center; padding-top: 12px; }}
    """


def _pacing_color(pct: float | None) -> str:
    if pct is None:
        return _MUTED
    if pct >= 90:
        return _GREEN
    if pct >= 70:
        return _YELLOW
    return _RED


def _pacing_badge(pct: float | None) -> str:
    if pct is None:
        return '<span class="badge badge-yellow">N/A</span>'
    if pct >= 90:
        return '<span class="badge badge-green">On Track</span>'
    if pct >= 70:
        return '<span class="badge badge-yellow">Caution</span>'
    return '<span class="badge badge-red">Behind</span>'


def _html_header(date_str: str, now_et: datetime) -> str:
    ts = now_et.strftime("%I:%M %p ET")
    return f"""
    <div class="header">
      <h1>PGAM Intelligence — Daily Report</h1>
      <p class="sub">{date_str} &nbsp;·&nbsp; Generated {ts}</p>
    </div>
    """


def _html_topline_section(top: dict, fmt_usd, fmt_n) -> str:
    """Cross-platform LL+TB+Combined supply rollup for yesterday."""
    if not top:
        return ""

    yest_date = top.get("date", "")
    ll  = top.get("ll", {})
    tb  = top.get("tb", {})
    cb  = top.get("combined", {})
    movers = top.get("movers", [])

    def _delta_html(now: float, base: float, kind: str = "pct") -> str:
        d = _delta_pct(now, base)
        if d is None:
            return f'<span class="muted">—</span>'
        cls  = "green" if d >= 0 else "red"
        arr  = "▲" if d >= 0 else "▼"
        sign = "+" if d >= 0 else ""
        return f'<span class="{cls}">{arr} {sign}{d:.1f}%</span>'

    def _row(label: str, plat: dict) -> str:
        y    = plat.get("yest", {}) or {}
        d2   = plat.get("d2", {})   or {}
        d7   = plat.get("d7avg", {}) or {}
        rev  = y.get("revenue", 0)
        imp  = y.get("impressions", 0)
        ecpm = y.get("ecpm", 0)
        marg = y.get("margin", 0)
        return f"""
        <tr>
          <td><strong>{label}</strong></td>
          <td>{fmt_usd(rev)}<div style="font-size:11px;">{_delta_html(rev, d2.get('revenue'))} <span class="muted">DoD</span> · {_delta_html(rev, d7.get('revenue'))} <span class="muted">vs 7d</span></div></td>
          <td>{fmt_n(imp)}<div style="font-size:11px;">{_delta_html(imp, d7.get('impressions'))} <span class="muted">vs 7d</span></div></td>
          <td>{fmt_usd(ecpm)}<div style="font-size:11px;">{_delta_html(ecpm, d7.get('ecpm'))} <span class="muted">vs 7d</span></div></td>
          <td>{marg:.1f}%<div style="font-size:11px;" class="muted">7d: {d7.get('margin', 0):.1f}%</div></td>
        </tr>"""

    has_ll = bool(ll.get("yest"))
    has_tb = bool(tb.get("yest"))
    rows_html = ""
    if has_ll:
        rows_html += _row("LL (Limelight)", ll)
    if has_tb:
        rows_html += _row("TB (Teqblaze)", tb)
    if has_ll and has_tb:
        rows_html += _row("Combined", cb)

    if not rows_html:
        return '<div class="card"><h2>Yesterday — Supply Rollup</h2><p class="muted">No platform data available.</p></div>'

    # Movers block
    movers_html = ""
    if movers:
        m_rows = ""
        for m in movers:
            plat_badge = ('<span class="badge badge-blue">LL</span>'
                          if m["platform"] == "LL" else
                          '<span class="badge badge-yellow">TB</span>')
            delta = m["delta"]
            cls   = "green" if delta >= 0 else "red"
            arr   = "▲" if delta >= 0 else "▼"
            sign  = "+" if delta >= 0 else ""
            if m.get("is_new"):
                baseline_cell = '<span class="badge badge-green">new</span>'
                delta_cell    = f'<span class="{cls}">{arr} {sign}{fmt_usd(delta)}</span>'
            else:
                baseline_cell = f'<span class="muted">{fmt_usd(m["baseline"])}<br><span style="font-size:11px;">{m["baseline_label"]}</span></span>'
                pct_str       = f'({sign}{m["delta_pct"]:.0f}%)' if m["delta_pct"] is not None else ''
                delta_cell    = f'<span class="{cls}">{arr} {sign}{fmt_usd(delta)} <span class="muted" style="font-size:11px;">{pct_str}</span></span>'
            m_rows += f"""
            <tr>
              <td>{plat_badge}</td>
              <td>{m['publisher']}</td>
              <td>{fmt_usd(m['yest_rev'])}</td>
              <td>{baseline_cell}</td>
              <td>{delta_cell}</td>
            </tr>"""
        movers_html = f"""
        <div style="margin-top:18px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;">
            Top Movers vs Baseline
          </div>
          <table>
            <thead><tr>
              <th></th><th>Publisher</th><th>Yesterday</th><th>Baseline</th><th>Delta</th>
            </tr></thead>
            <tbody>{m_rows}</tbody>
          </table>
        </div>"""

    return f"""
    <div class="card" style="border-color:#1e3a5f;">
      <h2>Yesterday — Supply Rollup ({yest_date})</h2>
      <table>
        <thead><tr>
          <th>Platform</th><th>Revenue</th><th>Impressions</th><th>eCPM</th><th>Margin</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
      {movers_html}
    </div>
    """


def _html_revenue_section(rev: dict, fmt_usd, fmt_n) -> str:
    if not rev:
        return '<div class="card"><h2>Revenue Overview</h2><p class="muted">Data unavailable</p></div>'

    pacing        = rev.get("pacing_pct")
    rev_today     = rev.get("revenue_today", 0)
    rev_yest      = rev.get("revenue_yest", 0)
    exp_rev       = rev.get("expected_rev", 0)
    imps          = rev.get("impressions_today", 0)
    win_rate      = rev.get("win_rate_pct", 0)
    avg_7d        = rev.get("revenue_7d_avg", 0)
    pub_count     = rev.get("publisher_count", 0)

    dod_pct = ((rev_today - rev_yest) / rev_yest * 100) if rev_yest > 0 else None
    bar_pct = min(pacing or 0, 100)
    bar_color = _pacing_color(pacing)

    dod_html = ""
    if dod_pct is not None:
        cls  = "green" if dod_pct >= 0 else "red"
        sign = "+" if dod_pct >= 0 else ""
        dod_html = f'<span class="{cls}">{sign}{dod_pct:.1f}% DoD</span>'

    return f"""
    <div class="card">
      <h2>Revenue Overview</h2>
      <div class="metric-grid">
        <div class="metric">
          <div class="label">Today (so far)</div>
          <div class="value">{fmt_usd(rev_today)}</div>
          <div class="change">{dod_html}</div>
        </div>
        <div class="metric">
          <div class="label">Expected by now</div>
          <div class="value">{fmt_usd(exp_rev)}</div>
          <div class="change muted">Based on yesterday</div>
        </div>
        <div class="metric">
          <div class="label">7-Day Avg</div>
          <div class="value">{fmt_usd(avg_7d)}</div>
          <div class="change muted">Daily average</div>
        </div>
        <div class="metric">
          <div class="label">Impressions</div>
          <div class="value">{fmt_n(imps)}</div>
          <div class="change muted">Today</div>
        </div>
        <div class="metric">
          <div class="label">Win Rate</div>
          <div class="value">{win_rate:.1f}%</div>
          <div class="change muted">Bids → wins</div>
        </div>
        <div class="metric">
          <div class="label">Publishers</div>
          <div class="value">{pub_count}</div>
          <div class="change muted">Active today</div>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;margin-top:4px;">
        <div style="flex:1;">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{bar_pct:.1f}%;background:{bar_color};"></div>
          </div>
          <div style="font-size:12px;color:{_MUTED};">Pacing: {f"{pacing:.1f}" if pacing is not None else "N/A"}% of expected</div>
        </div>
        <div>{_pacing_badge(pacing)}</div>
      </div>
    </div>
    """


def _html_floor_section(floors: dict, fmt_usd) -> str:
    raise_list = floors.get("raise", [])
    lower_list = floors.get("lower", [])

    if not raise_list and not lower_list:
        return '<div class="card"><h2>Floor Price Actions</h2><p class="muted">No floor gap actions needed today.</p></div>'

    def _table(items: list, action: str, color: str) -> str:
        if not items:
            return ""
        action_badge = f'<span class="badge badge-{color}">{action}</span>'
        rows_html = ""
        for r in items:
            rows_html += f"""
            <tr>
              <td>{r['publisher']}</td>
              <td class="muted">{fmt_usd(r['avg_floor'])}</td>
              <td style="color:{_BLUE};">{fmt_usd(r['avg_bid'])}</td>
              <td style="color:{_GREEN if action == 'Raise' else _YELLOW};">{fmt_usd(r['recommended'])}</td>
              <td style="color:{_MUTED};">${r['revenue']:,.2f}</td>
            </tr>"""
        return f"""
        <div style="margin-bottom:16px;">
          <div style="margin-bottom:8px;">{action_badge}</div>
          <table>
            <thead><tr>
              <th>Publisher</th><th>Current Floor</th><th>Avg Bid</th>
              <th>Recommended</th><th>Revenue</th>
            </tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

    body = _table(raise_list, "Raise", "green") + _table(lower_list, "Lower", "yellow")
    return f'<div class="card"><h2>Floor Price Actions</h2>{body}</div>'


def _html_opp_fill_section(opp: dict, fmt_n) -> str:
    if not opp:
        return '<div class="card"><h2>MTD Opportunity Fill Rate</h2><p class="muted">Data unavailable</p></div>'

    fill_pct   = opp.get("fill_rate_pct", 0)
    threshold  = opp.get("threshold_pct", 0.05)
    above      = opp.get("above_threshold", False)
    imps_needed = opp.get("imps_needed", 0)
    mtd_rev    = opp.get("mtd_revenue", 0)
    mtd_opps   = opp.get("mtd_opportunities", 0)
    mtd_imps   = opp.get("mtd_impressions", 0)

    status_badge = ('<span class="badge badge-green">Above Threshold</span>'
                    if above else
                    '<span class="badge badge-red">Below Threshold</span>')
    bar_pct   = min(fill_pct / threshold * 100, 100) if threshold > 0 else 0
    bar_color = _GREEN if above else _RED

    imps_row = ""
    if not above and imps_needed > 0:
        imps_row = f'<p style="font-size:13px;color:{_YELLOW};margin:8px 0 0;">Need {fmt_n(imps_needed)} more impressions to reach threshold.</p>'

    return f"""
    <div class="card">
      <h2>MTD Opportunity Fill Rate</h2>
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;">
        <div style="flex:1;">
          <div class="progress-bar-bg">
            <div class="progress-bar-fill" style="width:{bar_pct:.1f}%;background:{bar_color};"></div>
          </div>
          <div style="font-size:12px;color:{_MUTED};">Fill Rate: {fill_pct:.4f}% (threshold: {threshold:.2f}%)</div>
        </div>
        <div>{status_badge}</div>
      </div>
      <div class="metric-grid">
        <div class="metric"><div class="label">MTD Revenue</div><div class="value">${mtd_rev:,.0f}</div></div>
        <div class="metric"><div class="label">Opportunities</div><div class="value">{fmt_n(mtd_opps)}</div></div>
        <div class="metric"><div class="label">Impressions</div><div class="value">{fmt_n(mtd_imps)}</div></div>
      </div>
      {imps_row}
    </div>
    """


def _html_floor_elasticity_section(opps: list, fmt_usd) -> str:
    if not opps:
        return ""

    rows_html = ""
    for o in opps[:8]:
        pub      = o.get("publisher", "")
        direction = o.get("direction", "")
        cur_floor = o.get("current_floor", 0)
        opt_floor = o.get("optimal_floor", 0)
        uplift    = o.get("daily_rev_uplift", 0)
        conf      = o.get("confidence", 0)
        priority  = o.get("priority", "medium")

        badge_cls = {"high": "badge-red", "medium": "badge-yellow", "low": "badge-blue"}.get(priority, "badge-blue")
        dir_arrow = "↑" if direction == "raise" else "↓"
        uplift_color = _GREEN if uplift >= 0 else _RED

        rows_html += f"""
        <tr>
          <td>{pub}</td>
          <td><span class="badge {badge_cls}">{priority}</span></td>
          <td class="muted">{dir_arrow} {fmt_usd(cur_floor)} → {fmt_usd(opt_floor)}</td>
          <td style="color:{uplift_color};">${abs(uplift):,.2f}/day</td>
          <td class="muted">{conf:.0%}</td>
        </tr>"""

    return f"""
    <div class="card">
      <h2>Floor Elasticity Opportunities</h2>
      <table>
        <thead><tr>
          <th>Publisher</th><th>Priority</th><th>Floor Change</th>
          <th>Est. Daily Uplift</th><th>Confidence</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def _html_ctv_section(ctv: dict, fmt_usd, fmt_n) -> str:
    if not ctv:
        return ""

    summary  = ctv.get("summary", {})
    pubs     = ctv.get("top_publishers", [])
    proj     = ctv.get("projections", {})
    n_pubs   = ctv.get("n_publishers", 0)

    avg_ecpm      = summary.get("avg_ecpm", 0)
    fill_rate     = summary.get("fill_rate", 0)
    avg_daily_rev = summary.get("avg_daily_revenue", 0)
    total_rev     = summary.get("total_revenue", 0)

    pub_rows = ""
    for p in pubs[:5]:
        name       = p.get("publisher", "")
        ecpm       = p.get("ecpm", 0)
        fill       = p.get("fill_rate", 0)
        opp_score  = p.get("opportunity_score", 0)
        pub_rows += f"""
        <tr>
          <td>{name}</td>
          <td style="color:{_PURPLE};">{fmt_usd(ecpm)}</td>
          <td class="muted">{fill:.2%}</td>
          <td style="color:{_GREEN};">{fmt_usd(opp_score)}/day</td>
        </tr>"""

    proj_html = ""
    for tier in ("10pct", "25pct", "50pct"):
        p = proj.get(tier, {})
        if p:
            label     = tier.replace("pct", "%")
            daily     = p.get("daily_revenue", 0)
            annual    = p.get("annual_revenue", 0)
            proj_html += f"""
            <tr>
              <td>+{label} volume</td>
              <td style="color:{_BLUE};">{fmt_usd(daily)}/day</td>
              <td style="color:{_BLUE};">{fmt_usd(annual)}/yr</td>
            </tr>"""

    proj_section = ""
    if proj_html:
        proj_section = f"""
        <div style="margin-top:16px;">
          <div style="font-size:12px;color:{_MUTED};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;">
            Revenue Projections
          </div>
          <table>
            <thead><tr><th>Scenario</th><th>Daily</th><th>Annual</th></tr></thead>
            <tbody>{proj_html}</tbody>
          </table>
        </div>"""

    return f"""
    <div class="card">
      <h2>CTV / OTT Opportunities</h2>
      <div class="metric-grid" style="margin-bottom:14px;">
        <div class="metric">
          <div class="label">Avg eCPM</div>
          <div class="value purple">{fmt_usd(avg_ecpm)}</div>
        </div>
        <div class="metric">
          <div class="label">Fill Rate</div>
          <div class="value">{fill_rate:.2%}</div>
        </div>
        <div class="metric">
          <div class="label">Avg Daily Rev</div>
          <div class="value">{fmt_usd(avg_daily_rev)}</div>
        </div>
      </div>
      {'<table><thead><tr><th>Publisher</th><th>eCPM</th><th>Fill Rate</th><th>Scale Opp</th></tr></thead><tbody>' + pub_rows + '</tbody></table>' if pub_rows else ''}
      {proj_section}
    </div>
    """


def _html_brief_section(brief: str) -> str:
    if not brief:
        return ""

    paragraphs = [p.strip() for p in brief.strip().split("\n\n") if p.strip()]
    paras_html = "".join(f'<p class="brief-para">{p}</p>' for p in paragraphs)

    return f"""
    <div class="card" style="border-color:#3730a3;background:linear-gradient(135deg,#1a1d27 0%,#1e1b2e 100%);">
      <h2 style="color:{_PURPLE};">Executive Intelligence Brief</h2>
      {paras_html}
    </div>
    """


def _html_win_rate_section(wr: dict, fmt_usd) -> str:
    if not wr or not wr.get("top_combinations"):
        return ""

    combos       = wr["top_combinations"]
    total_daily  = wr.get("total_daily_recovery", 0)
    total_weekly = wr.get("total_weekly_recovery", 0)
    n_found      = wr.get("total_combos_found", 0)

    rows_html = ""
    for c in combos[:8]:
        wr_pct     = c.get("win_rate_pct", 0)
        cur_floor  = c.get("current_floor", 0)
        new_floor  = c.get("new_floor", 0)
        add_rev    = c.get("add_rev_per_day", 0)
        adj_pct    = c.get("floor_adj_pct", 0)
        rows_html += f"""
        <tr>
          <td>{c['publisher']}</td>
          <td style="color:{_MUTED};">{c['demand_partner']}</td>
          <td style="color:{_RED};">{wr_pct:.2f}%</td>
          <td class="muted">{fmt_usd(cur_floor)} → <span style="color:{_GREEN};">{fmt_usd(new_floor)}</span> <span style="color:{_MUTED};font-size:11px;">({adj_pct:+.1f}%)</span></td>
          <td style="color:{_GREEN};">+{fmt_usd(add_rev)}/day</td>
        </tr>"""

    return f"""
    <div class="card">
      <h2>Win Rate Opportunities</h2>
      <div style="display:flex;gap:12px;margin-bottom:14px;flex-wrap:wrap;">
        <div class="metric" style="min-width:160px;">
          <div class="label">Daily Recovery</div>
          <div class="value green">+{fmt_usd(total_daily)}</div>
          <div class="change muted">{n_found} combinations</div>
        </div>
        <div class="metric" style="min-width:160px;">
          <div class="label">Weekly Recovery</div>
          <div class="value green">+{fmt_usd(total_weekly)}</div>
          <div class="change muted">Win rate target 10%</div>
        </div>
      </div>
      <table>
        <thead><tr>
          <th>Publisher</th><th>Demand Partner</th><th>Win Rate</th>
          <th>Floor Adjustment</th><th>Est. Recovery</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>
    """


def _html_footer(date_str: str) -> str:
    return f"""
    <div class="footer">
      PGAM Intelligence &nbsp;·&nbsp; {date_str} &nbsp;·&nbsp;
      Automated daily report &nbsp;·&nbsp; Unsubscribe not applicable (internal ops)
    </div>
    """


def _build_html(
    date_str:    str,
    now_et:      datetime,
    topline:     dict,
    rev_summary: dict,
    floors:      dict,
    opp_fill:    dict,
    floor_opps:  list,
    ctv:         dict,
    win_rate:    dict,
    brief:       str,
    fmt_usd,
    fmt_n,
) -> str:
    body_parts = [
        _html_header(date_str, now_et),
        _html_brief_section(brief),
        _html_topline_section(topline, fmt_usd, fmt_n),
        _html_revenue_section(rev_summary, fmt_usd, fmt_n),
        _html_opp_fill_section(opp_fill, fmt_n),
        _html_floor_section(floors, fmt_usd),
        _html_floor_elasticity_section(floor_opps, fmt_usd),
        _html_win_rate_section(win_rate, fmt_usd),
        _html_ctv_section(ctv, fmt_usd, fmt_n),
        _html_footer(date_str),
    ]

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>PGAM Intelligence — {date_str}</title>
  <style>{_css()}</style>
</head>
<body>
  <div class="wrapper">
    {''.join(body_parts)}
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# SendGrid delivery
# ---------------------------------------------------------------------------

def _send_email(
    html_body: str,
    date_str:  str,
    sendgrid_key: str,
    sender:    str,
    recipients: list[str],
) -> bool:
    """Send HTML email via SendGrid REST API. Returns True on success."""
    try:
        import urllib.request
    except ImportError:
        print("[daily_email] urllib not available")
        return False

    subject = f"PGAM Intelligence — Daily Report {date_str}"
    payload = {
        "personalizations": [{"to": [{"email": r} for r in recipients]}],
        "from": {"email": sender},
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
            status = resp.getcode()
            if status in (200, 202):
                print(f"[daily_email] Email delivered to {len(recipients)} recipient(s). Status {status}.")
                return True
            print(f"[daily_email] Unexpected status {status}")
            return False
    except Exception as exc:
        print(f"[daily_email] Delivery failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run():
    now_et    = datetime.now(ET)
    hour_et   = now_et.hour
    date_str  = now_et.strftime("%Y-%m-%d")

    # Hour gate — only send at or after SEND_HOUR_ET
    if hour_et < SEND_HOUR_ET:
        print(f"[daily_email] Too early ({hour_et:02d}:xx ET). Will send at {SEND_HOUR_ET:02d}:00 ET.")
        return

    # Deduplication — once per day
    if _already_sent(date_str):
        print(f"[daily_email] Already sent for {date_str}. Exiting.")
        return

    # ------------------------------------------------------------------
    # Load core dependencies
    # ------------------------------------------------------------------
    try:
        (fetch, yesterday_fn, today_fn, n_days_ago_fn,
         sf, pct, fmt_usd, fmt_n,
         sendgrid_key, sender, recipients, thresholds) = _core()
    except Exception as exc:
        print(f"[daily_email] Core import failed: {exc}")
        traceback.print_exc()
        return

    if not sendgrid_key:
        print("[daily_email] SENDGRID_KEY not set. Exiting.")
        return
    if not recipients:
        print("[daily_email] No EMAIL_TO recipients configured. Exiting.")
        return

    print(f"[daily_email] Building report for {date_str}…")

    # ------------------------------------------------------------------
    # Collect data from all sources (failures are non-fatal)
    # ------------------------------------------------------------------
    topline: dict = {}
    try:
        topline = _collect_topline(yesterday_fn, n_days_ago_fn)
        ll_rev = topline.get("ll", {}).get("yest", {}).get("revenue", 0)
        tb_rev = topline.get("tb", {}).get("yest", {}).get("revenue", 0)
        print(f"[daily_email] Topline: LL ${ll_rev:,.0f}  |  TB ${tb_rev:,.0f}  |  "
              f"{len(topline.get('movers', []))} movers")
    except Exception as exc:
        print(f"[daily_email] Topline collection failed: {exc}")
        traceback.print_exc()

    rev_summary = _collect_revenue_summary(
        fetch, yesterday_fn, today_fn, n_days_ago_fn, sf, pct, fmt_usd, fmt_n
    )

    floors = _collect_floor_gaps(fetch, yesterday_fn, sf)

    opp_fill = _collect_opp_fill(fetch, today_fn, n_days_ago_fn, sf, pct)

    # Floor elasticity (weekly report module)
    floor_opps: list = []
    try:
        from agents.reports.floor_elasticity import get_optimization_data
        floor_opps = get_optimization_data(top_n=8)
        print(f"[daily_email] Floor elasticity: {len(floor_opps)} opportunities")
    except Exception as exc:
        print(f"[daily_email] Floor elasticity import failed: {exc}")

    # CTV section
    ctv: dict = {}
    try:
        from agents.alerts.ctv_optimizer import export_ctv_section
        ctv = export_ctv_section(top_n=5)
        print(f"[daily_email] CTV section: {'ok' if ctv else 'empty'}")
    except Exception as exc:
        print(f"[daily_email] CTV import failed: {exc}")

    # Win rate maximizer section
    win_rate: dict = {}
    try:
        from agents.reports.win_rate_maximizer import export_win_rate_section
        win_rate = export_win_rate_section(top_n=8)
        print(f"[daily_email] Win rate: {win_rate.get('total_combos_found', 0)} combos, "
              f"${win_rate.get('total_daily_recovery', 0):,.0f}/day recoverable")
    except Exception as exc:
        print(f"[daily_email] Win rate import failed: {exc}")

    # Claude executive brief
    brief = ""
    try:
        from intelligence.claude_analyst import synthesize_daily_brief

        anomalies: list = []
        # Populate anomalies from floor gaps if any meaningful gaps exist
        if floors.get("raise"):
            anomalies.append({
                "type": "floor_underpriced",
                "count": len(floors["raise"]),
                "top_publisher": floors["raise"][0]["publisher"] if floors["raise"] else None,
            })
        if floors.get("lower"):
            anomalies.append({
                "type": "floor_overpriced",
                "count": len(floors["lower"]),
                "top_publisher": floors["lower"][0]["publisher"] if floors["lower"] else None,
            })
        if opp_fill and not opp_fill.get("above_threshold", True):
            anomalies.append({
                "type": "fill_rate_below_threshold",
                "fill_rate_pct": opp_fill.get("fill_rate_pct"),
                "imps_needed": opp_fill.get("imps_needed"),
            })

        fix_summary = {
            "raise_count":  len(floors.get("raise", [])),
            "lower_count":  len(floors.get("lower", [])),
            "top_raise":    floors.get("raise", [{}])[0] if floors.get("raise") else {},
            "top_elasticity_opps": floor_opps[:3] if floor_opps else [],
        }

        brief = synthesize_daily_brief(
            summary   = rev_summary,
            fix       = fix_summary,
            anomalies = anomalies,
            opp_fill  = opp_fill,
            date_str  = date_str,
        )
        print("[daily_email] Claude brief: generated")
    except Exception as exc:
        print(f"[daily_email] Claude brief failed: {exc}")

    # ------------------------------------------------------------------
    # Build and send HTML
    # ------------------------------------------------------------------
    html = _build_html(
        date_str    = date_str,
        now_et      = now_et,
        topline     = topline,
        rev_summary = rev_summary,
        floors      = floors,
        opp_fill    = opp_fill,
        floor_opps  = floor_opps,
        ctv         = ctv,
        win_rate    = win_rate,
        brief       = brief,
        fmt_usd     = fmt_usd,
        fmt_n       = fmt_n,
    )

    success = _send_email(html, date_str, sendgrid_key, sender, recipients)

    if success:
        _mark_sent(date_str)
    else:
        print("[daily_email] Email not delivered — state NOT marked as sent.")


if __name__ == "__main__":
    run()
