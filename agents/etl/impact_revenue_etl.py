"""
agents/etl/impact_revenue_etl.py

Lands impact.com affiliate actions into Neon, so PGAM's affiliate revenue sits
in the warehouse next to its programmatic revenue instead of only in a vendor
dashboard.

Why this and not the impact.com MCP server
------------------------------------------
impact.com ships a remote MCP server (integrations.impact.com/ai-solutions),
and it is a good thing — for a person asking a question in a session. It puts
no row in Neon. Nothing scheduled can read it, no dashboard can chart it, and
next month it cannot tell you what last month did unless someone asks it
again. This is the durable leg. The MCP server and this ETL are complements;
neither substitutes for the other. See docs/impact-affiliate-etl.md.

Destination (created by migrations/2026_08_26_impact_affiliate.sql):

  pgam_direct.impact_actions                    ledger, PK action_id
  pgam_direct.impact_daily_campaign_revenue     view — per program per day
  pgam_direct.impact_daily_property_revenue     view — per PGAM site per day

The one way affiliate data is not like ad data
----------------------------------------------
Affiliate revenue goes backwards. A conversion recorded in March can be
REVERSED in June, when the shopper returns the item. Every other ETL in this
repo writes pre-aggregated daily rows and refreshes a trailing window, which
is correct for impressions — an impression is final when it is counted.

Applied here, that shape would ratchet revenue permanently upward: the
reversal lands long after the window has moved past the action's date, so the
day silently keeps money it lost. Understating revenue gets noticed;
overstating it does not.

So this writes one row per ACTION keyed on the vendor's action id, and the
daily numbers are views over that ledger. Two consequences worth knowing:

  1. A reversal of any age corrects every rollup the instant it lands, because
     there is only one copy of each number.
  2. Catching a reversal still requires *seeing* it, which a window over event
     dates cannot do. Hence the second pass below, keyed on modification date.

Passes per run
--------------
  1. Event-date window (default 45 trailing days) — new and recent actions.
  2. Modification sweep (default 7 trailing days) — anything whose state or
     payout changed, at any event date, including years back. This is the
     pass that catches reversals, and it degrades to a warning rather than an
     error if the account's API does not accept the parameter.

Safe to schedule before credentials exist: with IMPACT_ACCOUNT_SID /
IMPACT_AUTH_TOKEN absent this no-ops with an actionable log line rather than
raising every hour. Wiring it now means the day the credentials land in
Render, data starts flowing with no redeploy.

READ-only against impact.com. It writes to our own warehouse and calls no
vendor write endpoint.

Schedule: hourly (scheduler.py), plus a daily deep pass at 05:20 ET that
widens the event-date window to a year — see DEEP_WINDOW_DAYS.

    python -m agents.etl.impact_revenue_etl --dry-run
    python -m agents.etl.impact_revenue_etl --backfill 365
    python -m agents.etl.impact_revenue_etl --from 2026-01-01 --to 2026-03-31
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

_LOG = "[impact_revenue_etl]"

# Trailing event-date days re-read on every run. Wider than the programmatic
# ETLs' 14 because an affiliate action's *payout* is not settled at 14 days —
# many programs hold actions PENDING for a 30-day return window and only then
# lock them. A window that ends before the hold does would land every action
# in its provisional state and never see the final one.
WINDOW_DAYS = 45

# The daily deep pass. A year is not paranoia: locking periods of 60-90 days
# are ordinary in travel and finance affiliate programs, which are exactly the
# verticals in 08_monetization_strategy.md.
DEEP_WINDOW_DAYS = 365

# Trailing days of *modification* history swept for state changes. Only needs
# to exceed the gap since the last successful run; 7 days gives a week of
# scheduler outage before a reversal could slip past both passes.
REVERSAL_SWEEP_DAYS = 7

# Event-date span per request. The Actions endpoint pages properly, so this is
# not about truncation — it bounds how many pages a single call can owe us, so
# one slow month cannot stall the whole window behind a 400-page walk.
CHUNK_DAYS = 7

_MIGRATION = (Path(__file__).resolve().parents[2]
              / "migrations" / "2026_08_26_impact_affiliate.sql")

# first_seen_at is deliberately NOT in the UPDATE list: it records when we
# first saw the action, which never changes. Everything else is the vendor's
# current truth and is overwritten wholesale — including payout, which is the
# column a reversal rewrites.
_UPSERT_SQL = """
INSERT INTO pgam_direct.impact_actions (
    action_id, campaign_id, campaign_name, tracker_id, tracker_name,
    event_date, creation_date, locking_date, modification_date, referring_date,
    state, payout, sale_amount, currency, payout_currency,
    sub_id1, sub_id2, sub_id3, promo_code, customer_country, referring_domain,
    raw, first_seen_at, updated_at
) VALUES (
    %(action_id)s, %(campaign_id)s, %(campaign_name)s, %(tracker_id)s,
    %(tracker_name)s,
    %(event_date)s::date, %(creation_date)s::timestamptz,
    %(locking_date)s::timestamptz, %(modification_date)s::timestamptz,
    %(referring_date)s::timestamptz,
    %(state)s, %(payout)s, %(sale_amount)s, %(currency)s, %(payout_currency)s,
    %(sub_id1)s, %(sub_id2)s, %(sub_id3)s, %(promo_code)s,
    %(customer_country)s, %(referring_domain)s,
    %(raw)s::jsonb, now(), now()
)
ON CONFLICT (action_id) DO UPDATE SET
    campaign_id       = EXCLUDED.campaign_id,
    campaign_name     = EXCLUDED.campaign_name,
    tracker_id        = EXCLUDED.tracker_id,
    tracker_name      = EXCLUDED.tracker_name,
    event_date        = EXCLUDED.event_date,
    creation_date     = EXCLUDED.creation_date,
    locking_date      = EXCLUDED.locking_date,
    modification_date = EXCLUDED.modification_date,
    referring_date    = EXCLUDED.referring_date,
    state             = EXCLUDED.state,
    payout            = EXCLUDED.payout,
    sale_amount       = EXCLUDED.sale_amount,
    currency          = EXCLUDED.currency,
    payout_currency   = EXCLUDED.payout_currency,
    sub_id1           = EXCLUDED.sub_id1,
    sub_id2           = EXCLUDED.sub_id2,
    sub_id3           = EXCLUDED.sub_id3,
    promo_code        = EXCLUDED.promo_code,
    customer_country  = EXCLUDED.customer_country,
    referring_domain  = EXCLUDED.referring_domain,
    raw               = EXCLUDED.raw,
    updated_at        = now();
"""


def _f(value) -> float:
    """Vendor numerics arrive as strings, sometimes with a currency symbol."""
    if value in (None, "", "-"):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _i(value):
    """Optional integer id. Returns None rather than 0 — a campaign id of 0 is
    a real-looking value that would group unrelated rows together."""
    if value in (None, "", "-"):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _s(value):
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _day(value):
    """
    The date portion of a vendor timestamp, as the vendor expressed it.

    Deliberately NOT converted to UTC or to ET. impact.com reports in the
    account's own timezone, and its UI totals are drawn on those day
    boundaries. Shifting the boundary here would make our per-day numbers
    disagree with the vendor's own dashboard by a few hours' worth of
    conversions every day — a discrepancy that looks like data loss and is
    impossible to reconcile against an invoice.
    """
    text = _s(value)
    if not text:
        return None
    head = text[:10]
    try:
        return date.fromisoformat(head)
    except ValueError:
        return None


def _ts(value):
    """Pass a timestamp through as text for Postgres to cast, or None."""
    return _s(value)


def normalize(row: dict) -> dict | None:
    """
    One vendor action row -> one ledger record, or None if unusable.

    Unusable means no action id (nothing to key on — an UPSERT with a null key
    would collapse every such row onto one) or no event date (nowhere to put
    it in time). Both are counted and reported by the caller rather than
    dropped quietly.
    """
    from core import impact_api as imp

    action_id = _s(imp.action_field(row, "action_id"))
    event_date = _day(imp.action_field(row, "event_date"))
    if not action_id or event_date is None:
        return None

    state = _s(imp.action_field(row, "state"))
    return {
        "action_id": action_id,
        "campaign_id": _i(imp.action_field(row, "campaign_id")),
        "campaign_name": _s(imp.action_field(row, "campaign_name")),
        "tracker_id": _i(imp.action_field(row, "tracker_id")),
        "tracker_name": _s(imp.action_field(row, "tracker_name")),
        "event_date": event_date,
        "creation_date": _ts(imp.action_field(row, "creation_date")),
        "locking_date": _ts(imp.action_field(row, "locking_date")),
        "modification_date": _ts(imp.action_field(row, "modification_date")),
        "referring_date": _ts(imp.action_field(row, "referring_date")),
        # Uppercased so the views' state filters cannot miss a row because an
        # account returns "Reversed" instead of "REVERSED".
        "state": state.upper() if state else None,
        "payout": round(_f(imp.action_field(row, "payout")), 4),
        "sale_amount": round(_f(imp.action_field(row, "sale_amount")), 4),
        "currency": _s(imp.action_field(row, "currency")),
        "payout_currency": _s(imp.action_field(row, "payout_currency")),
        "sub_id1": _s(imp.action_field(row, "sub_id1")),
        "sub_id2": _s(imp.action_field(row, "sub_id2")),
        "sub_id3": _s(imp.action_field(row, "sub_id3")),
        "promo_code": _s(imp.action_field(row, "promo_code")),
        "customer_country": _s(imp.action_field(row, "customer_country")),
        "referring_domain": _s(imp.action_field(row, "referring_domain")),
        "raw": json.dumps(row, default=str, sort_keys=True),
    }


def dedupe(records: list[dict]) -> list[dict]:
    """
    Collapse to one record per action_id, keeping the most recently modified.

    The two passes overlap by design — a recently modified action is usually
    also inside the event-date window — so the same action arrives twice per
    run. `executemany` would apply both, and if they disagreed the last one
    written would win by accident of ordering rather than by recency. Resolve
    it here, where the rule can be stated: newest modification wins, and an
    action with no modification stamp never displaces one that has it.
    """
    best: dict[str, dict] = {}
    for rec in records:
        key = rec["action_id"]
        prior = best.get(key)
        if prior is None:
            best[key] = rec
            continue
        new_mod, old_mod = rec.get("modification_date"), prior.get("modification_date")
        if new_mod and (not old_mod or str(new_mod) >= str(old_mod)):
            best[key] = rec
    return list(best.values())


def summarize(records: list[dict]) -> dict:
    """Per-state payout totals and currency spread, for the run log."""
    by_state: dict[str, float] = defaultdict(float)
    by_currency: dict[str, float] = defaultdict(float)
    days: set[date] = set()
    for rec in records:
        by_state[rec["state"] or "UNKNOWN"] += rec["payout"]
        by_currency[rec["payout_currency"] or rec["currency"] or "UNKNOWN"] += rec["payout"]
        days.add(rec["event_date"])
    return {
        "actions": len(records),
        "by_state": dict(by_state),
        "by_currency": dict(by_currency),
        "days_covered": len(days),
        "earliest": min(days).isoformat() if days else None,
        "latest": max(days).isoformat() if days else None,
    }


def ensure_tables() -> None:
    """
    Apply the migration. Idempotent; safe to run every pass.

    This repo has no migration runner — `migrations/*.sql` is a record, not
    something applied on deploy (see agents/etl/tbx_revenue_etl.py, which
    ensures its own schema for the same reason). Without this the first
    authenticated run dies on `relation "pgam_direct.impact_actions" does not
    exist`, which reads as a broken ETL rather than an unapplied migration.

    The views are DROP + CREATE inside this one transaction, so readers see
    the old definition or the new one, never a missing view.
    """
    from core.neon import connect

    ddl = _MIGRATION.read_text(encoding="utf-8")
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def write(records: list[dict]) -> int:
    if not records:
        return 0
    # Imported here, not at module scope: normalize/dedupe/summarize above are
    # pure and worth testing without a Postgres driver installed.
    from core.neon import connect

    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL, records)
        conn.commit()
    finally:
        conn.close()
    return len(records)


def _fetch_window(start: date, end: date) -> tuple[list[dict], list[str]]:
    """Pull actions by event date, in CHUNK_DAYS slices. Returns (rows, errors)."""
    from core import impact_api as imp

    rows: list[dict] = []
    errors: list[str] = []
    day = start
    while day <= end:
        chunk_end = min(day + timedelta(days=CHUNK_DAYS - 1), end)
        try:
            got = imp.actions(date_start=day.isoformat(),
                              date_end=chunk_end.isoformat())
        except Exception as exc:
            # One chunk failing must not cost the rest of the window. A partial
            # load is worth more than none, and the gap is named.
            print(f"{_LOG} {day}..{chunk_end}: FAILED — {exc}")
            errors.append(f"{day}..{chunk_end}: {exc}")
        else:
            print(f"{_LOG} {day}..{chunk_end}: {len(got)} action(s)")
            rows.extend(got)
        day = chunk_end + timedelta(days=1)
    return rows, errors


def _fetch_modified(since: date) -> tuple[list[dict], str | None]:
    """
    The reversal pass. Returns (rows, unsupported_reason).

    A failure here is reported, not raised. The modification-date parameter is
    not part of every account's accepted set, and an account without it is
    still worth running the event-date pass for — it just needs the daily deep
    window to catch reversals instead, which is why that pass exists.
    """
    from core import impact_api as imp

    try:
        rows = imp.actions_modified_since(since.isoformat())
    except Exception as exc:
        return [], str(exc)
    print(f"{_LOG} modified since {since}: {len(rows)} action(s)")
    return rows, None


def run(window_days: int = WINDOW_DAYS,
        date_from: str | None = None,
        date_to: str | None = None,
        reversal_days: int = REVERSAL_SWEEP_DAYS,
        deep: bool = False,
        dry_run: bool = False) -> dict:
    """
    Pull impact.com actions and UPSERT them into the Neon ledger.

    `date_from`/`date_to` (YYYY-MM-DD, inclusive) name an explicit window and
    override `window_days`. A repair is always for particular dates, and
    expressing that as a trailing-day count means recomputing the count every
    day it goes unfixed. The ledger UPSERTs on action_id, so naming the same
    window twice is free.

    `deep=True` widens the event-date window to DEEP_WINDOW_DAYS. That is the
    daily pass whose job is to see settlements and reversals on actions older
    than the hourly window.

    `dry_run=True` pulls from impact.com and reports exactly what it would
    land, without touching Neon.
    """
    from core import impact_api as imp

    if not imp.configured():
        # Not an error. Scheduled ahead of the credentials on purpose so that
        # adding them in Render is the only remaining step.
        missing = imp.missing_env()
        print(f"{_LOG} not configured — missing {', '.join(missing)}. "
              f"Nothing pulled.")
        print(f"{_LOG}   Both values come from the impact.com UI: Settings → "
              f"API Access (Account SID + Auth Token). Set them on the "
              f"pgam-intelligence-scheduler worker in the Render dashboard "
              f"(Environment). This job then fills pgam_direct.impact_actions "
              f"with no code change and no redeploy.")
        print(f"{_LOG}   Confirm the credentials first with: "
              f"python3 scripts/impact_probe.py")
        return {"ok": True, "skipped": "not_configured", "missing": missing}

    if date_from or date_to:
        try:
            start = date.fromisoformat(date_from) if date_from else date.today()
            end = date.fromisoformat(date_to) if date_to else date.today()
        except ValueError as exc:
            print(f"{_LOG} bad date bound — {exc}")
            return {"ok": False, "error": f"bad date: {exc}"}
        if start > end:
            print(f"{_LOG} --from {start} is after --to {end}")
            return {"ok": False, "error": "from after to"}
        span = (end - start).days + 1
        print(f"{_LOG} event-date window {start}..{end} ({span}d, explicit)")
    else:
        span = DEEP_WINDOW_DAYS if deep else window_days
        end = date.today()
        start = end - timedelta(days=max(span - 1, 0))
        label = "deep" if deep else "trailing"
        print(f"{_LOG} event-date window {start}..{end} ({span}d {label})")

    if not dry_run:
        try:
            ensure_tables()
        except Exception as exc:
            print(f"{_LOG} could not ensure destination tables — {exc}")
            return {"ok": False, "error": f"ensure_tables: {exc}"}

    result: dict = {"ok": True, "window": [start.isoformat(), end.isoformat()]}
    raw_rows, errors = _fetch_window(start, end)

    # Pass 2: state changes at any event date. Skipped when an explicit window
    # was named — that is a repair of specific dates, and sweeping unrelated
    # recent modifications into it would write rows the operator did not ask
    # for and cannot see in the output.
    swept = 0
    if reversal_days > 0 and not (date_from or date_to):
        since = date.today() - timedelta(days=max(reversal_days - 1, 0))
        mod_rows, unsupported = _fetch_modified(since)
        if unsupported:
            print(f"{_LOG} WARNING: modification sweep unavailable — "
                  f"{unsupported}")
            print(f"{_LOG}   Reversals of actions OLDER than the "
                  f"{span}-day event window will not be seen by this pass. "
                  f"The daily deep run (--deep, {DEEP_WINDOW_DAYS}d) is what "
                  f"covers that; do not shorten it while this is failing.")
            result["reversal_sweep"] = f"unavailable: {unsupported}"
        else:
            raw_rows.extend(mod_rows)
            swept = len(mod_rows)
            result["reversal_sweep"] = swept

    if errors:
        result["ok"] = False
        result["failures"] = errors

    records: list[dict] = []
    dropped = 0
    for row in raw_rows:
        rec = normalize(row)
        if rec is None:
            dropped += 1
            continue
        records.append(rec)

    before = len(records)
    records = dedupe(records)
    overlap = before - len(records)

    if dropped:
        # Loud: a dropped action is missing revenue, and a total that is
        # quietly short is worse than one that is obviously broken.
        print(f"{_LOG} WARNING: {dropped} row(s) dropped — no usable action id "
              f"or event date. Totals here are UNDERSTATED by whatever those "
              f"carried. Check the field mapping with: "
              f"python3 scripts/impact_probe.py --actions")
        result["dropped_rows"] = dropped

    stats = summarize(records)
    result.update(stats)

    if len(stats["by_currency"]) > 1:
        # Payouts in different currencies must not be added together, and this
        # repo has no FX source. The views keep currency in the grain so a
        # query cannot mix them by accident; this line is so a human reading
        # the run log knows the account is mixed at all.
        print(f"{_LOG} NOTE: payouts span {len(stats['by_currency'])} "
              f"currencies {sorted(stats['by_currency'])} — these are stored "
              f"unconverted and the views group by currency. Do not sum "
              f"across them.")

    if dry_run:
        print(f"{_LOG} DRY RUN — would upsert {len(records)} action(s) across "
              f"{stats['days_covered']} day(s) "
              f"({stats['earliest']}..{stats['latest']})")
        for state in sorted(stats["by_state"]):
            print(f"{_LOG}   {state:<10} ${stats['by_state'][state]:,.2f}")
        if raw_rows:
            sample = raw_rows[0]
            # The vendor's actual keys, once. Guessing the row shape from docs
            # is how a field mapping goes wrong silently; print it instead.
            print(f"{_LOG} sample row keys = {sorted(sample.keys())}")
        result["written"] = 0
        return result

    try:
        written = write(records)
    except Exception as exc:
        print(f"{_LOG} Neon UPSERT failed — {exc}")
        result["ok"] = False
        result.setdefault("failures", []).append(f"upsert: {exc}")
        return result

    note = f", {overlap} overlap collapsed" if overlap else ""
    print(f"{_LOG} {len(raw_rows)} row(s) in ({swept} from the modification "
          f"sweep) -> {written} upserted{note}")
    for state in sorted(stats["by_state"]):
        print(f"{_LOG}   {state:<10} ${stats['by_state'][state]:,.2f}")
    result["written"] = written
    return result


def run_deep() -> dict:
    """Entry point for the daily deep pass (scheduler.py)."""
    return run(deep=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Land impact.com affiliate actions into Neon")
    parser.add_argument("--backfill", type=int, default=None,
                        help=f"trailing event-date days (default {WINDOW_DAYS})")
    parser.add_argument("--deep", action="store_true",
                        help=f"widen the event-date window to "
                             f"{DEEP_WINDOW_DAYS}d to pick up late "
                             f"settlements and reversals")
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                        help="explicit start date (inclusive); overrides "
                             "--backfill and skips the modification sweep")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                        help="explicit end date (inclusive); defaults to today")
    parser.add_argument("--reversal-days", type=int, default=REVERSAL_SWEEP_DAYS,
                        help=f"trailing days of modification history to sweep "
                             f"for state changes (default "
                             f"{REVERSAL_SWEEP_DAYS}; 0 disables)")
    parser.add_argument("--dry-run", action="store_true",
                        help="pull and report without writing to Neon")
    args = parser.parse_args()
    outcome = run(window_days=args.backfill or WINDOW_DAYS,
                  date_from=args.date_from, date_to=args.date_to,
                  reversal_days=args.reversal_days,
                  deep=args.deep,
                  dry_run=args.dry_run)
    sys.exit(0 if outcome.get("ok") else 1)

