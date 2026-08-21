#!/usr/bin/env python3
"""Daily marketplace digest — findings and recommendations, never changes.

This agent has **no write path**. Not a gated one, not a flag: it reads Neon,
works out what it would recommend, and posts that to Slack for a person to
decide on. Deliberate — the marketplace has already been damaged once by
automation acting faster than anyone could check it (April 2026), and the point
of this tier is to earn the trust that a write tier would need.

Every recommendation carries four things, because a suggestion without them is
just noise:

    WHAT      the observation, in numbers
    EVIDENCE  where it came from, so it can be checked
    SUGGESTED the specific change, naming the platform field
    OWNER     commercial or engineering — they are different problems

That last one came out of the 2026-08-11 decline. Two demand sources lost 96%
and 88% of their revenue with their eCPM *unchanged* — the buyers still paid
$6.50–7.55, they just stopped taking volume. No floor or margin lever recovers
that; it is a conversation, not a config change. An alert that cannot tell a
volume problem from a price problem sends the wrong person to look.

Usage
-----
    python3 -m agents.alerts.marketplace_digest              # print only
    python3 -m agents.alerts.marketplace_digest --post       # also post to Slack
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

ACTOR = "marketplace_digest"
_LOG = "[marketplace_digest]"

# Thresholds. Env-overridable; defaults argued for in docs/optimization-cadence.md.
REGRESSION_PCT   = float(os.getenv("PGAM_DIGEST_REGRESSION_PCT", "15"))
ESCALATE_PCT     = float(os.getenv("PGAM_DIGEST_ESCALATE_PCT", "30"))
FLAT_PRICE_PCT   = float(os.getenv("PGAM_DIGEST_FLAT_PRICE_PCT", "10"))
SOURCE_DROP_PCT  = float(os.getenv("PGAM_DIGEST_SOURCE_DROP_PCT", "50"))
RENDER_FLOOR_PCT = float(os.getenv("PGAM_DIGEST_RENDER_PCT", "80"))
MARGIN_DRIFT_PTS = float(os.getenv("PGAM_DIGEST_MARGIN_DRIFT", "3"))


class Finding:
    """One recommendation. Kept as a class so the Slack and stdout renderers
    agree on the shape rather than each inventing one."""

    def __init__(self, severity: str, title: str, what: str, evidence: str,
                 suggested: str, owner: str):
        self.severity = severity        # critical | warning | info
        self.title = title
        self.what = what
        self.evidence = evidence
        self.suggested = suggested
        self.owner = owner              # commercial | engineering | either

    def as_text(self) -> str:
        icon = {"critical": "🔴", "warning": "🟠", "info": "🔵"}.get(self.severity, "•")
        return (f"{icon} {self.title}\n"
                f"    what      {self.what}\n"
                f"    evidence  {self.evidence}\n"
                f"    suggested {self.suggested}\n"
                f"    owner     {self.owner}")

    def as_block(self) -> dict:
        icon = {"critical": "🔴", "warning": "🟠", "info": "🔵"}.get(self.severity, "•")
        return {"type": "section", "text": {"type": "mrkdwn", "text": (
            f"{icon} *{self.title}*\n"
            f"{self.what}\n"
            f"_evidence_ {self.evidence}\n"
            f"*suggested* {self.suggested}\n"
            f"*owner* `{self.owner}`")}}


# ── Checks ──────────────────────────────────────────────────────────────────

def check_regression(conn, findings: list[Finding]) -> None:
    """7 days vs the 7 before, with the volume-or-price verdict."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH d AS (
                SELECT report_date,
                       sum(impressions)::bigint AS imps,
                       sum(gross_revenue)::numeric AS gross,
                       sum(gross_revenue) - sum(pub_payout) AS profit
                FROM pgam_direct.tb_daily_publisher_revenue
                WHERE report_date > current_date - 15
                GROUP BY report_date
            )
            SELECT CASE WHEN report_date > current_date - 8 THEN 'recent' ELSE 'prior' END,
                   sum(imps)::bigint, sum(gross)::numeric, sum(profit)::numeric
            FROM d GROUP BY 1
        """)
        rows = {r[0]: r for r in cur.fetchall()}

    if "recent" not in rows or "prior" not in rows:
        findings.append(Finding(
            "warning", "Not enough history to judge a regression",
            "Fewer than two full weeks of rows in tb_daily_publisher_revenue.",
            "pgam_direct.tb_daily_publisher_revenue",
            "Check the TB revenue ETL is still running before trusting any figure below.",
            "engineering"))
        return

    _, r_imps, r_gross, r_profit = rows["recent"]
    _, p_imps, p_gross, p_profit = rows["prior"]
    r_gross, p_gross = float(r_gross or 0), float(p_gross or 0)
    r_imps, p_imps = int(r_imps or 0), int(p_imps or 0)
    if not p_gross:
        return

    gross_chg = 100 * (r_gross - p_gross) / p_gross
    imps_chg = 100 * (r_imps - p_imps) / p_imps if p_imps else 0
    r_ecpm = 1000 * r_gross / r_imps if r_imps else 0
    p_ecpm = 1000 * p_gross / p_imps if p_imps else 0
    ecpm_chg = 100 * (r_ecpm - p_ecpm) / p_ecpm if p_ecpm else 0

    if gross_chg > -REGRESSION_PCT:
        findings.append(Finding(
            "info", f"Revenue steady ({gross_chg:+.1f}% week on week)",
            f"7d gross ${r_gross:,.0f} against ${p_gross:,.0f}; "
            f"eCPM ${r_ecpm:.3f} against ${p_ecpm:.3f}.",
            "tb_daily_publisher_revenue, trailing 14 days",
            "Nothing. Recorded so a later move has a baseline.",
            "either"))
        return

    # The split that decides who gets called.
    if abs(ecpm_chg) < FLAT_PRICE_PCT:
        verdict = ("VOLUME, not price — eCPM is flat, so buyers pay the same and "
                   "are taking less. No floor or margin lever recovers this.")
        owner, sev = "commercial", "critical"
    elif imps_chg > -FLAT_PRICE_PCT:
        verdict = ("PRICE, not volume — impressions held and eCPM fell, which "
                   "points at floors, competition, or a demand mix shift.")
        owner, sev = "engineering", "critical"
    else:
        verdict = "BOTH volume and price moved — treat as supply loss until ruled out."
        owner, sev = "commercial", "critical"

    if gross_chg > -ESCALATE_PCT:
        sev = "warning"

    findings.append(Finding(
        sev, f"Revenue down {abs(gross_chg):.1f}% week on week",
        f"7d gross ${r_gross:,.0f} vs ${p_gross:,.0f}. "
        f"Impressions {imps_chg:+.1f}%, eCPM {ecpm_chg:+.1f}%. "
        f"Profit ${float(r_profit or 0):,.0f} vs ${float(p_profit or 0):,.0f}. {verdict}",
        "tb_daily_publisher_revenue, 7d vs prior 7d",
        "Run `scripts/tb_whatchanged.py --pivot <date>` to name the rows behind it "
        "before changing anything.",
        owner))


def check_flat_price_collapses(conn, findings: list[Finding]) -> None:
    """Individual sources that lost volume while holding price — the Advetisi shape."""
    with conn.cursor() as cur:
        cur.execute("""
            WITH w AS (
                SELECT demand_name,
                       CASE WHEN report_date > current_date - 8 THEN 'r' ELSE 'p' END AS half,
                       sum(impressions)::bigint AS imps,
                       sum(gross_revenue)::numeric AS gross
                FROM pgam_direct.tb_daily_demand_revenue
                WHERE report_date > current_date - 15
                GROUP BY demand_name, 2
            )
            SELECT r.demand_name, p.gross, r.gross, p.imps, r.imps,
                   CASE WHEN p.imps > 0 THEN 1000.0 * p.gross / p.imps END,
                   CASE WHEN r.imps > 0 THEN 1000.0 * r.gross / r.imps END
            FROM w r JOIN w p ON p.demand_name = r.demand_name
            WHERE r.half = 'r' AND p.half = 'p'
              AND p.gross > 500
              AND r.gross < p.gross * %(drop)s
            ORDER BY (p.gross - r.gross) DESC
            LIMIT 8
        """, {"drop": 1 - SOURCE_DROP_PCT / 100})
        rows = cur.fetchall()

    for name, pg, rg, pi, ri, pe, re_ in rows:
        pg, rg = float(pg or 0), float(rg or 0)
        pe, re_ = float(pe or 0), float(re_ or 0)
        ecpm_move = 100 * (re_ - pe) / pe if pe else 0
        flat = abs(ecpm_move) < FLAT_PRICE_PCT
        findings.append(Finding(
            "critical" if flat else "warning",
            f"{name} lost {100 * (pg - rg) / pg:.0f}% of revenue",
            (f"${pg:,.0f} → ${rg:,.0f}. Impressions {int(pi or 0):,} → {int(ri or 0):,}. "
             f"eCPM ${pe:.3f} → ${re_:.3f} ({ecpm_move:+.1f}%)."
             + (" Price held, so this is the buyer stopping, not repricing."
                if flat else " Price moved too, so a pricing cause is in play.")),
            "tb_daily_demand_revenue, 7d vs prior 7d",
            ("Ask the partner whether budget, campaign or seat changed — then check "
             "the integration. Do not adjust floors for this."
             if flat else
             "Check floors and demand mix for this source before contacting the partner."),
            "commercial" if flat else "engineering"))


def check_render(conn, findings: list[Finding]) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT demand_name, sum(wins)::bigint, sum(impressions)::bigint,
                   sum(gross_revenue)::numeric
            FROM pgam_direct.tb_daily_demand_revenue
            WHERE report_date > current_date - 8
            GROUP BY demand_name
            HAVING sum(wins) > 500000
               AND sum(impressions) < %(pct)s * sum(wins)
            ORDER BY (sum(wins) - sum(impressions)) DESC
            LIMIT 6
        """, {"pct": RENDER_FLOOR_PCT / 100})
        rows = cur.fetchall()

    for name, wins, imps, gross in rows:
        wins, imps = int(wins or 0), int(imps or 0)
        pct = 100 * imps / wins if wins else 0
        # Revenue per impression is gross/imps. An earlier version also divided
        # by 1000, conflating it with eCPM, and printed every upside as $0.
        upside = (0.9 * wins - imps) * (float(gross or 0) / imps) if imps else 0
        findings.append(Finding(
            "critical" if pct < 60 else "warning",
            f"{name} renders {pct:.1f}% of wins",
            f"{wins:,} wins produced {imps:,} impressions over 7 days. "
            f"Healthy sources here run 93–99%. At this source's own eCPM, reaching "
            f"90% would be worth roughly ${upside:,.0f} per 7 days — an upper bound, "
            f"since eCPM falls as volume rises and some loss is legitimate.",
            "tb_daily_demand_revenue, imps ÷ wins over 7 days",
            "Read `render_rate` and `timeout_rate` on the new platform to confirm, "
            "then check `integration.default_tmax`, `billing_type` and "
            "`video_filter[]` sizes. Diagnose before changing anything.",
            "engineering"))


def check_margin_drift(conn, findings: list[Finding]) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            WITH d AS (
                SELECT CASE WHEN report_date > current_date - 8 THEN 'r' ELSE 'p' END AS half,
                       sum(gross_revenue)::numeric AS gross,
                       sum(gross_revenue) - sum(pub_payout) AS profit
                FROM pgam_direct.tb_daily_publisher_revenue
                WHERE report_date > current_date - 15
                GROUP BY 1
            )
            SELECT half, CASE WHEN gross > 0 THEN 100.0 * profit / gross END FROM d
        """)
        m = {r[0]: float(r[1] or 0) for r in cur.fetchall()}
    if "r" in m and "p" in m and abs(m["r"] - m["p"]) >= MARGIN_DRIFT_PTS:
        findings.append(Finding(
            "warning", f"Blended margin moved {m['r'] - m['p']:+.1f} points",
            f"{m['p']:.2f}% → {m['r']:.2f}% week on week.",
            "tb_daily_publisher_revenue, derived as (gross − payout) / gross",
            "Check whether a margin setting changed or the source mix shifted. "
            "Note this margin is derived; the new platform computes its own.",
            "engineering"))


def check_qps_waste(conn, findings: list[Finding]) -> None:
    """Reuses the sentry's rule so the digest and the sweep never disagree."""
    from agents.optimization import qps_waste_sentry as q

    end = date.today() - timedelta(days=1)
    df = (end - timedelta(days=q.OBSERVE_DAYS - 1)).isoformat()
    rows = q.measure(conn, "tb_daily_demand_revenue", "demand_name", df, end.isoformat())
    if not rows:
        return
    total_req = sum(r["requests"] for r in rows)
    total_gross = sum(r["gross"] for r in rows)
    blended = total_gross / (total_req / 1_000_000) if total_req else 0
    rows = q.classify(rows, blended, end)
    actions = q.enforce_blast_radius(rows, total_req)
    if not actions:
        return

    freed = sum(r["requests"] for r in actions)
    risked = sum(r["gross"] for r in actions)
    names = ", ".join(f"{r['name']} (${r['gpm']:.4f}/M)" for r in actions[:5])
    findings.append(Finding(
        "warning",
        f"{len(actions)} demand source(s) recommended for a QPS cut",
        f"They consume {freed / 1e9:,.2f}B bid requests "
        f"({100 * freed / total_req:,.1f}% of QPS) and returned ${risked:,.2f} over "
        f"{q.OBSERVE_DAYS} days. Blended baseline is ${blended:.4f} per million. {names}.",
        f"agents/optimization/qps_waste_sentry.py, {q.OBSERVE_DAYS}d window",
        "Review, then cut manually on the platform. Teqblaze cannot shape traffic, "
        "so there is no partial option — confirm each is broken rather than "
        "seasonal first. The rule requires two consecutive sweeps before it "
        "recommends the same source twice.",
        "engineering"))


# ── Render ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--post", action="store_true", help="post to Slack as well as print")
    a = ap.parse_args()

    if not (os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")):
        print("No Neon DSN set.", file=sys.stderr)
        return 2

    from core.neon import connect

    findings: list[Finding] = []
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        for check in (check_regression, check_flat_price_collapses, check_render,
                      check_margin_drift, check_qps_waste):
            try:
                check(conn, findings)
            except Exception as exc:
                conn.rollback()
                print(f"{_LOG} WARN: {check.__name__} failed — {exc}", file=sys.stderr)
                findings.append(Finding(
                    "warning", f"Check `{check.__name__}` could not run",
                    str(exc)[:200], "digest internals",
                    "Fix the check. A silent skip would read as 'nothing wrong'.",
                    "engineering"))
    finally:
        conn.close()

    order = {"critical": 0, "warning": 1, "info": 2}
    findings.sort(key=lambda f: order.get(f.severity, 9))

    crit = sum(1 for f in findings if f.severity == "critical")
    header = (f"PGAM marketplace digest · {date.today().isoformat()} · "
              f"{len(findings)} finding(s), {crit} critical")
    print("=" * 78); print(header); print("=" * 78)
    print("RECOMMENDATIONS ONLY — this agent has no write path.\n")
    for f in findings:
        print(f.as_text()); print()

    if a.post:
        from core import slack
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": "Marketplace digest"}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": (
                f"{date.today().isoformat()} · {len(findings)} finding(s), "
                f"{crit} critical · *recommendations only, nothing was changed*")}]},
            {"type": "divider"},
        ]
        for f in findings[:12]:
            blocks.append(f.as_block())
        if len(findings) > 12:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn",
                          "text": f"_…{len(findings) - 12} more in the job log_"}]})
        try:
            slack.send_blocks(blocks, text=header)
            print(f"{_LOG} posted to Slack")
        except Exception as exc:
            print(f"{_LOG} Slack post failed: {exc}", file=sys.stderr)
            return 1
    else:
        print(f"{_LOG} not posted (pass --post to send to Slack)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
