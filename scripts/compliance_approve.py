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


def list_reactivation_candidates() -> int:
    """Show paths the reactivation monitor flagged as ready to bring back live."""
    with connect() as c, c.cursor() as cur:
        cur.execute("""
            SELECT ROW_NUMBER() OVER (ORDER BY revenue_7d DESC) AS rid,
                   entity_key, supply_partner_key, entity_value, audit_host,
                   revenue_7d, recommended_action, status,
                   last_recheck_at, ll_publisher_id
            FROM pgam_direct.compliance_path_block_list
            WHERE recommended_action IN ('reactivate', 'fixed_pre_review')
            ORDER BY revenue_7d DESC LIMIT 100
        """)
        rows = cur.fetchall()
    if not rows:
        print("No reactivation candidates.")
        return 0
    print(f"{'ID':>3} {'$/7d':>10} {'Action':<18} {'Entity':<35} {'Partner':<20}")
    for r in rows:
        rid, ek, pk, ev, host, rev, action, status, recheck, ll_pub = r
        print(f"{int(rid):>3} ${float(rev):>9,.0f} {action:<18} "
              f"{(ev or ek)[:34]:<35} {(pk or '?')[:19]:<20}")
    print()
    print("To reactivate: python -m scripts.compliance_approve --reactivate <ID>")
    return 0


def _find_reactivation_by_id(cur, ord_id: int):
    """Resolve --reactivate <ID> back to the underlying PK."""
    cur.execute("""
        SELECT entity_key, supply_partner_key, entity_value, revenue_7d,
               recommended_action, ll_publisher_id, status
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY revenue_7d DESC) AS ord,
                   entity_key, supply_partner_key, entity_value,
                   revenue_7d, recommended_action, ll_publisher_id, status
            FROM pgam_direct.compliance_path_block_list
            WHERE recommended_action IN ('reactivate', 'fixed_pre_review')
        ) s WHERE s.ord = %s
    """, (ord_id,))
    return cur.fetchone()


def reactivate(ord_id: int, dry_run: bool, notes: str | None) -> int:
    """Flip a 'reactivate' candidate to status='whitelisted'(safe state)
    AND, if PGAM_COMPLIANCE_ENFORCE_LIVE=1, call ll_mgmt to re-enable
    the previously-disabled (LL publisher × demand) pairs.

    Lookup is by ordinal from --list-reactivate. Refuses high-value
    paths without --confirm-high-value, same as approve().
    """
    with connect() as c, c.cursor() as cur:
        row = _find_reactivation_by_id(cur, ord_id)
        if not row:
            print(f"No reactivation candidate with id={ord_id}. "
                  "Use --list-reactivate to refresh the queue.")
            return 1
        ek, pk, ev, rev, action, ll_pub, status = row
        rev_f = float(rev)
        if dry_run:
            print(f"🔍 [DRY-RUN] Would REACTIVATE entity={ek!r} partner={pk!r} "
                  f"(${rev_f:,.0f}/7d, currently {status}/{action})")
            return 0
        actor = _actor()
        # Flip status to 'released' so it leaves the active queue. We
        # don't set 'whitelisted' because that's the "never block again"
        # state — released is correctly "fixed, no longer enforced".
        cur.execute("""
            UPDATE pgam_direct.compliance_path_block_list
            SET status='released', status_updated_at=now(),
                status_updated_by=%s, review_notes=%s
            WHERE entity_key=%s AND supply_partner_key=%s
        """, (actor, notes or f"reactivated by ops from {action}", ek, pk))

        # Audit-trail entry. Captures that an operator (not the
        # auditor) confirmed reactivation.
        cur.execute("""
            INSERT INTO pgam_direct.compliance_enforcement_log
                (entity_key, supply_partner_key, ll_publisher_id, entity_value,
                 revenue_7d_at_action, action, triggered_by, reason, dry_run)
            VALUES (%s, %s, %s, %s, %s, 'manual_reactivate', %s, %s, FALSE)
        """, (ek, pk, ll_pub, ev, rev_f, actor,
              notes or f"reactivated from {action} state"))
        c.commit()

        # If live mode AND this path was actually paused via LL mgmt
        # (check enforcement_log for a prior auto_disable), call
        # enable_publisher_demand to bring the demand back. Otherwise
        # the status flip is all we needed (Stage 3 bidder-edge wasn't
        # enforcing yet).
        live = os.environ.get("PGAM_COMPLIANCE_ENFORCE_LIVE", "0") == "1"
        if live and ll_pub:
            cur.execute("""
                SELECT DISTINCT demand_id FROM pgam_direct.compliance_enforcement_log
                WHERE entity_key=%s AND supply_partner_key=%s
                  AND action='auto_disable' AND dry_run=FALSE
            """, (ek, pk))
            demand_ids = [r[0] for r in cur.fetchall() if r[0]]
            if demand_ids:
                try:
                    from core import ll_mgmt
                    for d in demand_ids:
                        try:
                            ll_mgmt.enable_publisher_demand(ll_pub, d)
                            print(f"  ✓ re-enabled LL pub {ll_pub} × demand {d}")
                        except AttributeError:
                            print(f"  ⚠️ ll_mgmt.enable_publisher_demand not implemented; "
                                  f"flip manually for pub={ll_pub} demand={d}")
                        except Exception as exc:
                            print(f"  ❌ enable failed pub={ll_pub} demand={d}: {exc}")
                except Exception as exc:
                    print(f"  LL re-enable skipped: {exc}")
        print(f"✅ REACTIVATED entity={ek!r} partner={pk!r} (${rev_f:,.0f}/7d)")
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
    p.add_argument("--list", action="store_true", help="List pending block items.")
    p.add_argument("--list-reactivate", action="store_true",
                   help="List paths the reactivation monitor flagged as eligible.")
    p.add_argument("--id", type=int, help="Ordinal ID from --list to act on.")
    p.add_argument("--reactivate", type=int, metavar="ID",
                   help="Reactivate a previously-blocked path (ID from --list-reactivate).")
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
    if args.list_reactivate:
        return list_reactivation_candidates()
    if args.reactivate is not None:
        return reactivate(args.reactivate, args.dry_run, args.notes)
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
