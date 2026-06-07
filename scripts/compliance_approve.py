"""
scripts/compliance_approve.py

One-command approval/rejection of compliance block-list entries.
Designed for ops to action items posted in the daily digest's
"Action queue" section.

Workflow:
  1. Daily digest posts a list of pending-review (entity × partner)
     paths with revenue-at-risk and the exact compliance gap.
  2. Ops reviews each, then runs (per item):
       python -m scripts.compliance_approve --id <id> [--reject|--snooze 7 "in outreach"]
  3. Approved rows flip compliance_path_block_list.status='active'.
     The enforcer agent (next cron tick) picks them up and calls
     LL mgmt to actually pause the (LL_publisher × demand_id) pair.

Safety guards:
  • By default REQUIRES explicit --confirm-high-value for paths with
    revenue_7d >= $500/7d. Without that flag the script refuses to
    flip status='active' on a high-rev path. Prevents accidentally
    paping a 5-figure path with a typo.
  • Logs every action to compliance_enforcement_log so we have an
    audit trail of who approved/rejected what and when.
  • --dry-run prints what WOULD happen without writing.

Examples:
  # List all pending-review items
  python -m scripts.compliance_approve --list

  # Approve item ID 142
  python -m scripts.compliance_approve --id 142

  # Approve a high-rev path (>= $500/7d) — requires explicit flag
  python -m scripts.compliance_approve --id 142 --confirm-high-value

  # Reject (won't ever block; whitelisted)
  python -m scripts.compliance_approve --id 142 --reject "publisher confirmed line in next ads.txt push"

  # Snooze 7 days (don't escalate but don't whitelist either)
  python -m scripts.compliance_approve --id 142 --snooze 7 "in outreach"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from core.neon import connect


HIGH_VALUE_THRESHOLD_USD = 500.0


def _actor() -> str:
    """Best-effort 'who is running this' for audit trail."""
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
    return f"cli:{user}"


def list_pending() -> int:
    """Print pending-review items ordered by revenue desc."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT ROW_NUMBER() OVER (ORDER BY revenue_7d DESC) AS id,
                   entity_key, supply_partner_key, entity_value,
                   revenue_7d, reason, last_flagged_at, flagged_count
            FROM pgam_direct.compliance_path_block_list
            WHERE status = 'pending_review'
            ORDER BY revenue_7d DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
    if not rows:
        print("No pending-review items.")
        return 0
    print(f"{'ID':>3} {'$/7d':>10} {'flag×':>5} {'Entity':<35} {'Partner':<20} {'Reason'}")
    for r in rows:
        rid, ek, pk, ev, rev, reason, flagged, count = r
        high = " ⚠️" if float(rev) >= HIGH_VALUE_THRESHOLD_USD else ""
        print(f"{int(rid):>3} ${float(rev):>9,.0f} {int(count):>5}"
              f" {(ev or ek)[:34]:<35} {(pk or '?')[:19]:<20} {reason}{high}")
    return 0


def _find_path_by_id(cur, ord_id: int):
    """Resolve --id (1-based ordinal from --list) to the underlying
    (entity_key, supply_partner_key) PK."""
    cur.execute("""
        SELECT entity_key, supply_partner_key, entity_value, revenue_7d,
               status, reason, ll_publisher_id
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY revenue_7d DESC) AS ord,
                   entity_key, supply_partner_key, entity_value,
                   revenue_7d, status, reason, ll_publisher_id
            FROM pgam_direct.compliance_path_block_list
            WHERE status = 'pending_review'
        ) sub
        WHERE sub.ord = %s
    """, (ord_id,))
    return cur.fetchone()


def approve(ord_id: int, confirm_high_value: bool, dry_run: bool, notes: str | None) -> int:
    with connect() as c, c.cursor() as cur:
        row = _find_path_by_id(cur, ord_id)
        if not row:
            print(f"No pending-review item with id={ord_id}. Use --list to see current queue.")
            return 1
        ek, pk, ev, rev, status, reason, ll_pub = row
        rev_f = float(rev)
        if rev_f >= HIGH_VALUE_THRESHOLD_USD and not confirm_high_value:
            print(f"❌ Refused: path is HIGH-VALUE (${rev_f:,.0f}/7d ≥ "
                  f"${HIGH_VALUE_THRESHOLD_USD:.0f} threshold).\n"
                  f"   Re-run with --confirm-high-value to approve.")
            return 2
        if dry_run:
            print(f"🔍 [DRY-RUN] Would APPROVE entity={ek!r} partner={pk!r} (${rev_f:,.0f}/7d)")
            return 0
        actor = _actor()
        cur.execute("""
            UPDATE pgam_direct.compliance_path_block_list
            SET status='active', status_updated_at=now(),
                status_updated_by=%s, review_notes=%s
            WHERE entity_key=%s AND supply_partner_key=%s
        """, (actor, notes, ek, pk))
        cur.execute("""
            INSERT INTO pgam_direct.compliance_enforcement_log
              (entity_key, supply_partner_key, ll_publisher_id, entity_value,
               revenue_7d_at_action, action, triggered_by, reason, dry_run)
            VALUES (%s, %s, %s, %s, %s, 'manual_override', %s, %s, FALSE)
        """, (ek, pk, ll_pub, ev, rev_f, actor,
              f"approved-for-enforcement: {notes or reason}"))
        c.commit()
        print(f"✅ APPROVED entity={ek!r} partner={pk!r} (${rev_f:,.0f}/7d). "
              f"Enforcer will pick this up on next tick.")
        return 0


def reject(ord_id: int, notes: str, dry_run: bool) -> int:
    with connect() as c, c.cursor() as cur:
        row = _find_path_by_id(cur, ord_id)
        if not row:
            print(f"No pending-review item with id={ord_id}.")
            return 1
        ek, pk, ev, rev, *_ = row
        if dry_run:
            print(f"🔍 [DRY-RUN] Would WHITELIST entity={ek!r} partner={pk!r}")
            return 0
        actor = _actor()
        cur.execute("""
            UPDATE pgam_direct.compliance_path_block_list
            SET status='whitelisted', status_updated_at=now(),
                status_updated_by=%s, review_notes=%s
            WHERE entity_key=%s AND supply_partner_key=%s
        """, (actor, notes, ek, pk))
        cur.execute("""
            INSERT INTO pgam_direct.compliance_enforcement_log
              (entity_key, supply_partner_key, entity_value, revenue_7d_at_action,
               action, triggered_by, reason, dry_run)
            VALUES (%s, %s, %s, %s, 'whitelisted', %s, %s, FALSE)
        """, (ek, pk, ev, float(rev), actor, notes))
        c.commit()
        print(f"✅ WHITELISTED entity={ek!r} partner={pk!r}. Won't auto-block.")
        return 0


def snooze(ord_id: int, days: int, notes: str, dry_run: bool) -> int:
    with connect() as c, c.cursor() as cur:
        row = _find_path_by_id(cur, ord_id)
        if not row:
            print(f"No pending-review item with id={ord_id}.")
            return 1
        ek, pk, ev, rev, *_ = row
        until = datetime.now(timezone.utc) + timedelta(days=days)
        if dry_run:
            print(f"🔍 [DRY-RUN] Would SNOOZE entity={ek!r} partner={pk!r} "
                  f"until {until.isoformat()}")
            return 0
        actor = _actor()
        cur.execute("""
            INSERT INTO pgam_direct.compliance_block_snooze
              (entity_key, supply_partner_key, snoozed_until, reason, snoozed_by)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (entity_key, supply_partner_key) DO UPDATE SET
              snoozed_until=EXCLUDED.snoozed_until, reason=EXCLUDED.reason,
              snoozed_by=EXCLUDED.snoozed_by, created_at=now()
        """, (ek, pk, until, notes, actor))
        cur.execute("""
            INSERT INTO pgam_direct.compliance_enforcement_log
              (entity_key, supply_partner_key, entity_value, revenue_7d_at_action,
               action, triggered_by, reason, dry_run)
            VALUES (%s, %s, %s, %s, 'snooze_applied', %s, %s, FALSE)
        """, (ek, pk, ev, float(rev), actor, f"snooze {days}d: {notes}"))
        c.commit()
        print(f"💤 SNOOZED entity={ek!r} partner={pk!r} until {until.date()}")
        return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Approve/reject/snooze compliance block-list items.")
    p.add_argument("--list", action="store_true", help="List pending items.")
    p.add_argument("--id", type=int, help="Ordinal ID from --list to act on.")
    p.add_argument("--reject", action="store_true",
                   help="Whitelist (never auto-block).")
    p.add_argument("--snooze", type=int, metavar="DAYS",
                   help="Snooze N days (won't escalate, stays pending).")
    p.add_argument("--notes", "-m", default=None,
                   help="Reason / notes (recorded in audit log).")
    p.add_argument("--confirm-high-value", action="store_true",
                   help=f"Required to approve paths ≥ ${HIGH_VALUE_THRESHOLD_USD:.0f}/7d.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would happen, don't write.")
    args = p.parse_args()

    if args.list:
        return list_pending()
    if not args.id:
        p.print_help()
        return 1
    if args.reject:
        if not args.notes:
            print("--reject requires --notes/-m explaining why.")
            return 1
        return reject(args.id, args.notes, args.dry_run)
    if args.snooze is not None:
        if not args.notes:
            print("--snooze requires --notes/-m explaining why.")
            return 1
        return snooze(args.id, args.snooze, args.notes, args.dry_run)
    return approve(args.id, args.confirm_high_value, args.dry_run, args.notes)


if __name__ == "__main__":
    raise SystemExit(main())
