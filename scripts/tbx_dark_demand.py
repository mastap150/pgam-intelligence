#!/usr/bin/env python3
"""
Demand sources that have answered nothing for N days running.

A DSP that receives bid requests and returns **zero bid responses** is not a
buyer declining to bid — it is an endpoint that is not answering. That is a
broken integration, and every request sent to it is outbound cost with no
possible upside.

Why this is not tbx_cut's NO-WIN bucket
---------------------------------------
NO-WIN is `wins == 0` aggregated over a window. This is stricter and
differently shaped:

    NO-WIN        responds, never wins        a pricing/quality problem
    DARK          does not respond at all     a plumbing problem

and, crucially, **this one is about recency**. A DSP dark for the last three
days but healthy the four before it has `wins > 0` over a 7-day window and
NO-WIN will never flag it. That endpoint went down on Tuesday and nobody
noticed. Catching it is the whole point.

The measurement that makes this safe
------------------------------------
Every day is checked **individually**, and a day must have *answered* to count
against a source. That is not pedantry — it is the one property that keeps
this automation from being dangerous:

    a day whose query fails returns no rows,
    which is indistinguishable from a day with no responses.

Summing `responses_sum` over a window would read a platform outage as every
DSP on the book going dark simultaneously, and an unattended job would then
switch off the entire demand side. So a run that cannot measure all N days
refuses to write, and says so. (§5.9 — days do time out here, routinely.)

The rails
---------
1. **Every one of the N days must have answered.** No partial windows, ever.
2. **Each day must clear `--min-requests-day` on its own.** A source given
   nine requests yesterday has not been given a chance to answer.
3. **A source must be present for all N days.** One that first appears
   mid-window is new, not dark, and gets a pass.
4. **`--max-disable` caps a single run** (default 25). An automation that can
   switch off the whole book before anyone is awake is a hazard whatever its
   logic says. Overflow is reported, never silently dropped.
5. **`--apply` plus `TBX_ALLOW_WRITES=1`**, and `core.tbx_mgmt` enforces the
   second independently.
6. Every applied run writes a ledger; `--revert` undoes exactly it.
7. **A source already switched off is left alone.** The window is history,
   and history does not change when a source is disabled — so yesterday's cut
   is still dark today. Re-flagging it would spend the cap on no-ops and
   re-announce the same names every morning.
8. **An unattended run announces itself.** The scheduled job auto-applies
   (PGAM, 2026-09-01), so every applied run posts the sources it switched off
   to Slack. A silent automatic write is one nobody can audit until they think
   to look. A failed post never fails the run — the cut already happened, and
   losing it over a webhook would be the worse outcome.

Usage
-----
    # what is dark right now — read-only
    python3 scripts/tbx_dark_demand.py

    # the automation's own setting: three days running
    python3 scripts/tbx_dark_demand.py --days 3 --apply

    # undo
    python3 scripts/tbx_dark_demand.py --revert dark-ledger-<stamp>.json --apply

Exit codes:
    0  nothing dark, or the run succeeded
    1  at least one write was refused or failed
    2  credentials absent, or the window could not be measured
    3  the platform was unreachable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

try:                                     # optional: absent locally is fine
    from core import slack               # noqa: E402
except Exception:                        # noqa: BLE001
    slack = None

_HDR = "=" * 78

# requests_sum is what we send; responses_sum is what comes back. The whole
# question is the second being zero while the first is not.
METRICS = ["requests_sum", "responses_sum"]

# Buyers a scheduled job must never switch off on its own, with the reason.
# Empty today: the Illumin RON cluster sat here until 2026-09-01, when PGAM
# confirmed the conversation with the partner had happened.
NEVER_AUTO_DISABLE: dict[int, str] = {}


def ledger_path() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"dark-ledger-{stamp}.json"


def pull_days(start: date, days: int) -> tuple[dict[int, dict], list[str]]:
    """One request per day. Returns {id: {name, per_day: {day: (req, resp)}}}.

    A day that raises is recorded as unanswered rather than as a day of
    silence, because those mean opposite things.
    """
    seen: dict[int, dict] = {}
    answered: list[str] = []
    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        try:
            rows, _ = tbx.report(day, day,
                                 attributes=["date", "demand_source"],
                                 metrics=METRICS)
        except tbx.TbxError as exc:
            print(f"    {day}: FAILED — {exc}", file=sys.stderr, flush=True)
            continue
        kept = 0
        for row in rows:
            if str(row.get("date") or "")[:10] != day:
                continue
            name, did = trim.split_name_id(row.get("demand_source") or "")
            if did is None:
                continue
            entry = seen.setdefault(did, {"name": name, "per_day": {}})
            requests = trim.num(row, "requests_sum")
            responses = trim.num(row, "responses_sum")
            prev = entry["per_day"].get(day, (0.0, 0.0))
            entry["per_day"][day] = (prev[0] + requests, prev[1] + responses)
            kept += 1
        print(f"    {day}: {kept} demand rows", flush=True)
        if kept:
            answered.append(day)
    return seen, answered


def live_status() -> dict[int, bool]:
    """{demand id: currently enabled} — one paginated call, not one per source.

    The report grain this script measures is *history*, and history does not
    change when a source is switched off. So a source disabled yesterday is
    still dark across the window today, and without this the job would re-flag
    every cut it has ever made: spending its --max-disable budget on no-ops,
    re-announcing them to Slack each morning, and burying anything new under
    them. The first live run (2026-09-01) surfaced 68, of which 19 were
    already off from the night before.
    """
    rows = tbm.list_demand_sources()
    out: dict[int, bool] = {}
    for row in rows:
        did = row.get("id")
        if did is None:
            continue
        out[int(did)] = bool(row.get("status"))
    return out


def select(seen: dict[int, dict], answered: list[str],
           status: dict[int, bool], args
           ) -> tuple[list[dict], list[tuple[dict, str]], list[dict]]:
    """Sources dark on EVERY answered day.

    Returns (targets, skipped, already_off).
    """
    targets, skipped, already_off = [], [], []
    for did, entry in seen.items():
        per_day = entry["per_day"]
        row = {"id": did, "name": entry["name"],
               "requests_day": sum(r for r, _ in per_day.values()) / len(answered),
               "responses": sum(s for _, s in per_day.values()),
               "days_present": len(per_day)}

        # Rail 3: present for the whole window, or it is new rather than dark.
        missing = [d for d in answered if d not in per_day]
        if missing:
            continue                      # no opinion; not an error

        # Rail 2: each day on its own must have been a fair chance to answer.
        quiet = [d for d in answered if per_day[d][0] < args.min_requests_day]
        if quiet:
            continue

        responded = [d for d in answered if per_day[d][1] > 0]
        if responded:
            continue                      # it answered on at least one day

        if did in args.exclude:
            skipped.append((row, args.exclude[did]))
            continue
        if args.include and did not in args.include:
            continue

        # Dark, but already switched off — by an earlier run of this job, by
        # tbx_cut, or by hand. Nothing to do, and it must not eat the cap.
        # A source the dictionary does not know is treated the same way:
        # writing to an entity whose current state we could not read is
        # exactly the guess this script exists to avoid.
        if not status.get(did, False):
            already_off.append(row)
            continue

        targets.append(row)

    targets.sort(key=lambda r: -r["requests_day"])
    already_off.sort(key=lambda r: -r["requests_day"])
    return targets, skipped, already_off


def render(targets: list[dict], skipped: list[tuple[dict, str]],
           already_off: list[dict], answered: list[str], args) -> list[dict]:
    print(f"\n{_HDR}\nDemand sources dark across {len(answered)} day(s)\n{_HDR}")
    print(f"  days measured: {', '.join(answered)}\n")
    if not targets:
        print("  nothing is dark and still switched on")
    for row in targets[:args.max_disable]:
        print(f"  {row['requests_day']:>15,.0f} req/day   0 responses   "
              f"{row['name']} #{row['id']}")

    overflow = targets[args.max_disable:]
    if overflow:
        print(f"\n  ⚠ {len(overflow)} more are dark but over the "
              f"--max-disable {args.max_disable} cap and will NOT be touched "
              f"this run:")
        for row in overflow[:10]:
            print(f"      {row['name']} #{row['id']} "
                  f"({row['requests_day']:,.0f} req/day)")
        print(f"  Raise --max-disable deliberately, or run again tomorrow. "
              f"A cap that\n  silently truncated would be worse than one that "
              f"says what it held back.")

    if targets:
        total = sum(r["requests_day"] for r in targets[:args.max_disable])
        print(f"\n  {min(len(targets), args.max_disable)} to disable: "
              f"{total:,.0f} requests/day, 0 responses across the window")
    if skipped:
        print(f"\n  {len(skipped)} skipped:")
        for row, why in skipped:
            print(f"    · {row['name']} #{row['id']} — {why}")
    if already_off:
        total = sum(r["requests_day"] for r in already_off)
        print(f"\n  {len(already_off)} dark but already disabled "
              f"({total:,.0f} req/day, no longer being sent) — left alone:")
        for row in already_off[:10]:
            print(f"    · {row['name']} #{row['id']} "
                  f"({row['requests_day']:,.0f} req/day)")
        if len(already_off) > 10:
            print(f"    · …and {len(already_off) - 10} more")
        print("  These are history: the window still contains the requests "
              "they were\n  sent before the switch-off, which is why they "
              "keep appearing.")
    return targets[:args.max_disable]


def apply_cuts(targets: list[dict], args) -> tuple[list[dict], int]:
    entries, failures = [], 0
    for row in targets:
        reason = (f"tbx_dark_demand: 0 bid responses on all {args.days} "
                  f"measured days, {row['requests_day']:,.0f} req/day")
        try:
            result = tbm.set_demand_source_status(
                row["id"], False, actor=args.actor, reason=reason,
                dry_run=not args.apply, demand_name=row["name"])
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {row['id']} ({row['name']}): {exc}",
                  file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {row['id']} ({row['name']}) refused: "
                  f"{result.get('refused', 'unknown')}", file=sys.stderr)
            failures += 1
            continue
        entries.append({
            "kind": "demand_source", "id": row["id"], "name": row["name"],
            "requests_day": row["requests_day"],
            "days": args.days,
            "applied": bool(result.get("applied")),
        })
    return entries, failures


def revert(path: str, args) -> int:
    with open(path) as handle:
        ledger = json.load(handle)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0
    print(f"Re-enabling {len(entries)} demand source(s) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for entry in entries:
        try:
            result = tbm.set_demand_source_status(
                entry["id"], True, actor=args.actor,
                reason=f"revert of {os.path.basename(path)}",
                dry_run=not args.apply, demand_name=entry.get("name"))
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {entry['id']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {entry['id']} refused: "
                  f"{result.get('refused', 'unknown')}", file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ demand {entry['id']}  {entry.get('name', '?')}")
    return 1 if failures else 0


def announce(entries: list[dict], args, ledger: str) -> None:
    """Tell Slack what an unattended run switched off.

    A scheduled job that disables partners in silence is one nobody can audit
    until someone thinks to look at an Actions log. The ledger is the record;
    this is the notification that a record exists. Failure to post must never
    fail the run — the write already happened, and losing the cut over a
    webhook would be the worse outcome.
    """
    if slack is None or not entries:
        return
    total = sum(e["requests_day"] for e in entries)
    lines = [f"• {e['name']} #{e['id']} — {e['requests_day']:,.0f} req/day"
             for e in entries[:15]]
    if len(entries) > 15:
        lines.append(f"• …and {len(entries) - 15} more")
    text = (
        f"*TBX dark demand — {len(entries)} source(s) disabled automatically*\n"
        f"Zero bid responses on all {args.days} settled days. "
        f"{total:,.0f} requests/day removed.\n"
        + "\n".join(lines)
        + f"\n\nLedger `{ledger}` (artifact on the run). Undo:\n"
        f"`tbx-dark-demand.yml` → revert_ledger = `{ledger}`, apply = yes"
    )
    try:
        slack.send_text(text)
    except Exception as exc:              # noqa: BLE001
        print(f"[slack] notification failed, cut still stands: {exc}",
              file=sys.stderr)


def parse_ids(raw: str | None) -> set[int]:
    if not raw:
        return set()
    return {int(part) for part in raw.replace(",", " ").split()}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Disable demand sources returning zero bid responses.")
    p.add_argument("--days", type=int, default=3,
                   help="consecutive settled days that must ALL be dark "
                        "(default 3)")
    p.add_argument("--min-requests-day", type=float, default=10_000,
                   help="each day must exceed this for the day to count. A "
                        "source given nine requests has not been given a "
                        "chance to answer.")
    p.add_argument("--max-disable", type=int, default=25,
                   help="most sources one run may disable (default 25). "
                        "Overflow is reported, never silently dropped.")
    p.add_argument("--include", default="",
                   help="only these demand ids, filtered against the fresh "
                        "measurement")
    p.add_argument("--also-exclude", default="",
                   help="demand ids to skip on top of the built-in list")
    p.add_argument("--apply", action="store_true",
                   help="actually disable. Also needs TBX_ALLOW_WRITES=1.")
    p.add_argument("--revert", metavar="LEDGER",
                   help="re-enable exactly what one ledger disabled")
    p.add_argument("--actor", default="tbx_dark_demand")
    p.add_argument("--ledger", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.exclude = dict(NEVER_AUTO_DISABLE)
    args.exclude.update({eid: "excluded on the command line"
                         for eid in parse_ids(args.also_exclude)})
    args.include = parse_ids(args.include)

    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to do.",
              file=sys.stderr)
        return 2
    if args.revert:
        return revert(args.revert, args)

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Measuring {start} → {end} ({args.days} settled days)\n")
    seen, answered = pull_days(start, args.days)

    # Rail 1. A failed day looks exactly like a silent one, so a run that
    # cannot see the whole window must not conclude anything from it.
    if len(answered) < args.days:
        missing = args.days - len(answered)
        print(f"\n::error::only {len(answered)}/{args.days} day(s) answered "
              f"({missing} failed). A day that did not answer is "
              f"indistinguishable from a day of silence, so this run cannot "
              f"tell a dark endpoint from an unmeasured one. Refusing to "
              f"report or write. Re-run.", file=sys.stderr)
        return 2

    print(f"\n  {len(seen)} demand source(s) seen across {len(answered)} days")

    # Which of them are still switched on. Without this the job re-flags its
    # own past cuts forever (see live_status).
    status = live_status()
    if not status:
        print("\n::error::the demand-source list came back empty, so this run "
              "cannot tell a source that is already off from one that is "
              "still running. Refusing to write. Re-run.", file=sys.stderr)
        return 2
    print(f"  {sum(1 for on in status.values() if on)} of {len(status)} "
          f"currently enabled")

    targets, skipped, already_off = select(seen, answered, status, args)
    to_cut = render(targets, skipped, already_off, answered, args)

    if not to_cut:
        return 0
    if not args.apply:
        print(f"\n{_HDR}\nDRY RUN — nothing was written. Re-run with --apply "
              f"(and TBX_ALLOW_WRITES=1).\n{_HDR}")

    print()
    entries, failures = apply_cuts(to_cut, args)
    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as handle:
            json.dump({"created": datetime.now(timezone.utc).isoformat(),
                       "actor": args.actor, "days": args.days,
                       "measured": answered, "entries": entries},
                      handle, indent=2)
        print(f"\nLedger: {path}")
        print(f"Undo with: python3 scripts/tbx_dark_demand.py "
              f"--revert {path} --apply")
        announce([e for e in entries if e.get("applied")], args, path)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
