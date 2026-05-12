"""
agents/recon/revenue_recheck.py

Billing-cycle reconciliation loop. Detects retrospective changes to
TB and LL reported numbers so finance can sign off on invoices and
payouts with confidence the dashboard reflects the latest values.

Model
-----
For each (target_date, source, publisher_name) cell:

  - "Current value" = whatever the LL/TB daily tables hold right now
    (these UPSERT from each ETL run, so they reflect the latest
     upstream-reported numbers).

  - "Prior value"   = the most recent row in pgam_direct.revenue_snapshots
                      for the cell. Captured by previous agent runs.

  - "Variance"      = current - prior. Anything beyond a $1 noise
                      floor and 0.1% relative threshold becomes a
                      row in pgam_direct.revenue_variances with a
                      severity tier (notice / warning / critical).

Each invocation writes a row to pgam_direct.recheck_runs and stamps
every snapshot + variance with the run_id, so operators can answer
"what changed in last night's run?" with one query.

Schedule (in pgam-intelligence/scheduler.py): daily 06:00 ET.

Scope
-----
By default we scan the current calendar month and the prior calendar
month (most retrospective revisions land within a few days). Months
in 'paid' or 'closed' state stay scanned but any new variances get
flagged is_post_lock=true + is_carry_forward=true so they surface as
adjustments in the next cycle rather than mutating already-paid
invoices.

Slack
-----
Critical-tier variances or aggregate variance > $500 in a single run
trigger a Slack message via core.slack.send_blocks. Dedupe key is
per-(run_id) so we don't spam.

What this DOESN'T do (Phase 2 scope)
-------------------------------------
  - Demand-side variance (DSP payouts to us). Just publisher-side
    for now since that's what billing/payouts care about.
  - Per-placement / per-domain variance. Just publisher-level.
  - Auto-approve / auto-resolve workflows. Operators drive
    resolution via the web UI for now.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from core.neon import connect
from core.slack import send_blocks

# ---------------------------------------------------------------------------
# Variance thresholds — tuned to keep the open-variances list actionable.
# ---------------------------------------------------------------------------

NOISE_FLOOR_DOLLARS = Decimal("1.00")       # ignore changes < $1
NOISE_FLOOR_PCT     = Decimal("0.001")      # AND ignore changes < 0.1%

# Severity: (abs_threshold_$, pct_threshold)
CRITICAL_ABS = Decimal("1000")
CRITICAL_PCT = Decimal("0.20")
WARNING_ABS  = Decimal("100")
WARNING_PCT  = Decimal("0.05")

# Slack alert threshold — aggregate abs(delta) above which we ping.
SLACK_AGG_THRESHOLD = Decimal("500")


@dataclass
class CellKey:
    target_date: date
    source: str          # 'LL' or 'TB'
    publisher_name: str


@dataclass
class CellSnapshot:
    gross_revenue: Decimal
    pub_payout: Decimal
    impressions: int


def _classify_severity(delta_abs: Decimal, delta_pct: Decimal | None) -> str | None:
    """Return severity tier, or None if below noise floor."""
    if delta_abs < NOISE_FLOOR_DOLLARS:
        return None
    # If we have a pct, both abs and pct must clear the noise floor.
    if delta_pct is not None and delta_pct < NOISE_FLOOR_PCT:
        return None
    if delta_abs >= CRITICAL_ABS or (delta_pct is not None and delta_pct >= CRITICAL_PCT):
        return "critical"
    if delta_abs >= WARNING_ABS or (delta_pct is not None and delta_pct >= WARNING_PCT):
        return "warning"
    return "notice"


def _months_to_scan() -> list[str]:
    """Current month + prior month, as YYYY-MM strings."""
    today = date.today()
    cur = today.strftime("%Y-%m")
    first_of_month = today.replace(day=1)
    prev = (first_of_month - timedelta(days=1)).strftime("%Y-%m")
    return [cur, prev]


def _date_range_for_month(ym: str) -> tuple[date, date]:
    y, m = ym.split("-")
    start = date(int(y), int(m), 1)
    if int(m) == 12:
        end_excl = date(int(y) + 1, 1, 1)
    else:
        end_excl = date(int(y), int(m) + 1, 1)
    end = end_excl - timedelta(days=1)
    return start, end


def _fetch_current_cells(conn, start: date, end: date) -> dict[tuple, CellSnapshot]:
    """Read the current values from ll_ + tb_ daily tables. Keyed by
    (target_date, source, publisher_name)."""
    cells: dict[tuple, CellSnapshot] = {}
    with conn.cursor() as cur:
        # LL — sum across multiple demand rows per (date, publisher).
        cur.execute(
            """
            SELECT report_date, publisher_name,
                   SUM(gross_revenue)::numeric AS gross,
                   SUM(pub_payout)::numeric    AS payout,
                   SUM(impressions)::bigint    AS imps
              FROM pgam_direct.ll_daily_partner_revenue
             WHERE report_date BETWEEN %s AND %s
             GROUP BY report_date, publisher_name
            """,
            (start, end),
        )
        for d, name, gross, payout, imps in cur.fetchall():
            cells[(d, "LL", name)] = CellSnapshot(
                Decimal(str(gross or 0)),
                Decimal(str(payout or 0)),
                int(imps or 0),
            )

        # TB — same shape from the publisher-daily table.
        cur.execute(
            """
            SELECT report_date, publisher_name,
                   gross_revenue::numeric AS gross,
                   pub_payout::numeric    AS payout,
                   impressions::bigint    AS imps
              FROM pgam_direct.tb_daily_publisher_revenue
             WHERE report_date BETWEEN %s AND %s
            """,
            (start, end),
        )
        for d, name, gross, payout, imps in cur.fetchall():
            cells[(d, "TB", name)] = CellSnapshot(
                Decimal(str(gross or 0)),
                Decimal(str(payout or 0)),
                int(imps or 0),
            )
    return cells


def _fetch_latest_snapshots(conn, start: date, end: date) -> dict[tuple, CellSnapshot]:
    """For each (target_date, source, publisher_name) cell in the
    window, return the MOST RECENT snapshot we've stored. Cells with
    no prior snapshot won't appear in the dict — caller treats them
    as first-time seen (no variance check)."""
    out: dict[tuple, CellSnapshot] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (target_date, source, publisher_name)
                   target_date, source, publisher_name,
                   gross_revenue, pub_payout, impressions
              FROM pgam_direct.revenue_snapshots
             WHERE target_date BETWEEN %s AND %s
             ORDER BY target_date, source, publisher_name, captured_at DESC
            """,
            (start, end),
        )
        for d, src, name, gross, payout, imps in cur.fetchall():
            out[(d, src, name)] = CellSnapshot(
                Decimal(str(gross)),
                Decimal(str(payout)),
                int(imps),
            )
    return out


def _month_locked(conn, ym: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM pgam_direct.recheck_month_status WHERE year_month = %s AND scope = 'all'",
            (ym,),
        )
        row = cur.fetchone()
    if not row:
        return False
    return row[0] in ("locked", "paid", "closed")


def _upsert_month_status(conn, ym: str, status: str, run_id: int,
                          variance_count: int, variance_abs: Decimal):
    """Upsert the workflow row for a month. We preserve `locked` /
    `paid` / `closed` states — they only advance via operator action
    in the web UI, never auto-flipped by the recheck agent."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT status FROM pgam_direct.recheck_month_status
             WHERE year_month = %s AND scope = 'all'
            """,
            (ym,),
        )
        row = cur.fetchone()
        cur_status = row[0] if row else None
        # Never demote locked/paid/closed. If the month is locked
        # and new variances are detected, the variance rows carry
        # is_post_lock=true and the workflow stays locked.
        if cur_status in ("locked", "paid", "closed"):
            cur.execute(
                """
                UPDATE pgam_direct.recheck_month_status
                   SET variance_count = %s,
                       variance_abs_total = %s,
                       last_recheck_at = now(),
                       last_recheck_run = %s,
                       updated_at = now()
                 WHERE year_month = %s AND scope = 'all'
                """,
                (variance_count, variance_abs, run_id, ym),
            )
            return
        cur.execute(
            """
            INSERT INTO pgam_direct.recheck_month_status
                (year_month, scope, status, variance_count, variance_abs_total,
                 last_recheck_at, last_recheck_run, updated_at)
            VALUES (%s, 'all', %s, %s, %s, now(), %s, now())
            ON CONFLICT (year_month, scope) DO UPDATE
               SET status = EXCLUDED.status,
                   variance_count = EXCLUDED.variance_count,
                   variance_abs_total = EXCLUDED.variance_abs_total,
                   last_recheck_at = now(),
                   last_recheck_run = EXCLUDED.last_recheck_run,
                   updated_at = now()
            """,
            (ym, status, variance_count, variance_abs, run_id),
        )


def _audit(conn, ym: str, action: str, actor: str = "recheck-agent", detail: dict | None = None):
    import json
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pgam_direct.recheck_audit_log
                (year_month, scope, action, actor, detail)
            VALUES (%s, 'all', %s, %s, %s::jsonb)
            """,
            (ym, action, actor, json.dumps(detail or {})),
        )


def _post_slack(variances_by_month: dict[str, list[dict]], run_id: int):
    """Slack ping when this run found enough variance to warrant attention.
    Quiet otherwise — operators check the web UI for the long tail."""
    interesting: list[tuple[str, list[dict]]] = []
    for ym, vs in variances_by_month.items():
        critical = [v for v in vs if v["severity"] == "critical"]
        agg = sum((abs(v["delta_gross"]) for v in vs), Decimal("0"))
        if critical or agg >= SLACK_AGG_THRESHOLD:
            interesting.append((ym, critical or vs))
    if not interesting:
        return
    lines: list[str] = []
    for ym, sample in interesting:
        sample = sorted(sample, key=lambda v: abs(v["delta_gross"]), reverse=True)[:6]
        for v in sample:
            sign = "+" if v["delta_gross"] >= 0 else "-"
            emoji = ":red_circle:" if v["severity"] == "critical" else ":large_yellow_circle:"
            pct_s = f" ({v['delta_pct']*100:+.1f}%)" if v.get("delta_pct") is not None else ""
            lock_tag = " · _post-lock carry-forward_" if v.get("is_post_lock") else ""
            lines.append(
                f"{emoji} *{v['publisher_name']}* ({v['source']}) "
                f"{v['target_date']}: ${v['prior_gross']:.2f} → ${v['current_gross']:.2f} "
                f"({sign}${abs(v['delta_gross']):.2f}{pct_s}){lock_tag}"
            )
    text = (
        "*Revenue recheck — variances detected*\n"
        + "\n".join(lines[:15])
        + (f"\n_+{sum(len(vs) for _, vs in interesting) - len(lines)} more in the workflow page_" if sum(len(vs) for _, vs in interesting) > len(lines) else "")
    )
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": ":balance_scale: Revenue recheck", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"Run #{run_id} · "
                    f"<https://app.pgammedia.com/admin/finance/recheck|Open workflow> · "
                    f"approve or carry-forward each variance there"
                ),
            }],
        },
    ]
    send_blocks(blocks=blocks, text="Revenue recheck — variances detected. Review in dashboard.")


def run() -> dict:
    """Run the recheck loop. Returns a summary dict for scheduler logging."""
    months = _months_to_scan()
    started = datetime.utcnow()
    print(f"[revenue_recheck] starting; months={months}", flush=True)

    with connect() as conn:
        # 1. Open a run row.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pgam_direct.recheck_runs (months_scanned, status)
                VALUES (%s, 'running')
                RETURNING id
                """,
                (months,),
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        cells_compared = 0
        variances_found = 0
        variances_by_month: dict[str, list[dict]] = {}

        try:
            for ym in months:
                start, end = _date_range_for_month(ym)
                locked = _month_locked(conn, ym)
                _audit(conn, ym, "recheck_started",
                       detail={"run_id": run_id, "locked": locked})

                current = _fetch_current_cells(conn, start, end)
                prior   = _fetch_latest_snapshots(conn, start, end)

                month_variances: list[dict] = []
                snapshot_rows: list[tuple] = []
                variance_rows: list[tuple] = []

                for key, cur_val in current.items():
                    target_date, source, name = key
                    cells_compared += 1
                    prior_val = prior.get(key)

                    # Always record a snapshot of the current value so
                    # future runs have a baseline.
                    snapshot_rows.append((
                        target_date, source, name,
                        cur_val.gross_revenue, cur_val.pub_payout,
                        cur_val.impressions, run_id,
                    ))

                    if prior_val is None:
                        # First time we've seen this cell — no variance.
                        continue

                    delta_gross = cur_val.gross_revenue - prior_val.gross_revenue
                    delta_payout = cur_val.pub_payout - prior_val.pub_payout
                    abs_delta = abs(delta_gross)
                    if prior_val.gross_revenue and prior_val.gross_revenue != 0:
                        delta_pct = abs_delta / abs(prior_val.gross_revenue)
                    else:
                        delta_pct = None
                    severity = _classify_severity(abs_delta, delta_pct)
                    if severity is None:
                        continue  # within noise floor

                    is_post_lock = locked
                    is_carry_forward = locked   # post-lock variances default to carry-forward
                    signed_pct = delta_pct if delta_pct is None else (
                        delta_gross / prior_val.gross_revenue
                    ) if prior_val.gross_revenue != 0 else None
                    variance_rows.append((
                        target_date, source, name,
                        prior_val.gross_revenue, cur_val.gross_revenue,
                        prior_val.pub_payout,    cur_val.pub_payout,
                        signed_pct, severity,
                        is_post_lock, is_carry_forward, run_id,
                    ))
                    month_variances.append({
                        "target_date": target_date.isoformat(),
                        "source": source,
                        "publisher_name": name,
                        "prior_gross": prior_val.gross_revenue,
                        "current_gross": cur_val.gross_revenue,
                        "delta_gross": delta_gross,
                        "delta_pct": signed_pct,
                        "severity": severity,
                        "is_post_lock": is_post_lock,
                    })
                    variances_found += 1

                # Batch-insert snapshots + variances for the month.
                if snapshot_rows:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO pgam_direct.revenue_snapshots
                                (target_date, source, publisher_name,
                                 gross_revenue, pub_payout, impressions, captured_in_run)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            snapshot_rows,
                        )
                if variance_rows:
                    with conn.cursor() as cur:
                        cur.executemany(
                            """
                            INSERT INTO pgam_direct.revenue_variances
                                (target_date, source, publisher_name,
                                 prior_gross, current_gross,
                                 prior_payout, current_payout,
                                 delta_pct, severity,
                                 is_post_lock, is_carry_forward, detected_in_run)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            variance_rows,
                        )

                # Update month_status.
                abs_total = sum((abs(v["delta_gross"]) for v in month_variances), Decimal("0"))
                if month_variances and not locked:
                    new_status = "variance_detected"
                elif not month_variances and not locked:
                    new_status = "open"
                else:
                    new_status = None  # locked path is handled inside _upsert_month_status
                _upsert_month_status(
                    conn, ym, new_status or "open",
                    run_id, len(month_variances), abs_total,
                )
                if month_variances:
                    variances_by_month[ym] = month_variances
                    _audit(conn, ym, "variance_detected",
                           detail={
                               "run_id": run_id, "count": len(month_variances),
                               "abs_total_usd": float(abs_total),
                               "post_lock": locked,
                           })
                conn.commit()

            # Mark run complete.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pgam_direct.recheck_runs
                       SET finished_at = now(),
                           cells_compared = %s,
                           variances_found = %s,
                           status = 'ok'
                     WHERE id = %s
                    """,
                    (cells_compared, variances_found, run_id),
                )
            conn.commit()
        except Exception as exc:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pgam_direct.recheck_runs
                       SET finished_at = now(),
                           status = 'failed',
                           error_message = %s
                     WHERE id = %s
                    """,
                    (str(exc)[:500], run_id),
                )
            conn.commit()
            raise

    # Slack ping (outside the DB tx).
    try:
        _post_slack(variances_by_month, run_id)
    except Exception as exc:
        print(f"[revenue_recheck] slack post failed (non-fatal): {exc}", flush=True)

    elapsed = (datetime.utcnow() - started).total_seconds()
    summary = {
        "ok": True,
        "run_id": run_id,
        "months": months,
        "cells_compared": cells_compared,
        "variances_found": variances_found,
        "elapsed_s": round(elapsed, 1),
    }
    print(f"[revenue_recheck] done: {summary}", flush=True)
    return summary


if __name__ == "__main__":
    res = run()
    sys.exit(0 if res.get("ok") else 1)
