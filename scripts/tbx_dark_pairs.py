#!/usr/bin/env python3
"""
Pause a supply → demand connection that has returned zero bid responses on
every one of the last N settled days.

Why this is a different job from tbx_dark_demand
------------------------------------------------
`tbx_dark_demand` switches off a DEMAND SOURCE that answers nothing on any
supply. This is the pair-grain version of the same idea, for the DSP that
answers some publishers and ignores others: "Magnite - Aditude Display #566"
takes 266k requests a day from one supply source and returns nothing, while
buying happily elsewhere. Disabling the DSP would be wrong; sending it that
supply is pure outbound cost. The lever is the supply source's
demand allow/block list — the connection is paused, both parties stay live.

Where the write lands
---------------------
`SupplySourceRequest.is_allowed_sources` (bool) + `demand_sources[]` (ints).
`false` = the list is a BLOCKLIST → pause = add the demand id.
`true`  = the list is an ALLOWLIST → pause = remove the demand id.
The list replaces wholesale on the wire (§6.2 shape), so the current list is
read first and the full intended set is written; the ledger records the
exact prior list and `--revert` puts it back. Two things are refused rather
than guessed: emptying an allowlist (its meaning is undocumented), and an
allowlist that does not contain a demand which is nevertheless receiving
requests (the list is not governing what we think it is).

Rails — the dark_demand rules, at pair grain
--------------------------------------------
1. Every one of the N days must have answered; a failed day is unmeasured,
   not silent. No partial windows, ever.
2. The pair must be present on every day, and clear `--min-requests-day` on
   each day by itself (default 10,000 — the report's own filter).
3. Zero responses on every day. One response on one day and it is a
   pricing problem, not a plumbing one.
4. The DEMAND source must have answered somewhere else in the window. If it
   answered nowhere it is dark, and that is `tbx_dark_demand`'s job — one
   automation per failure mode, so nothing is switched off twice.
5. The demand source must be live; a pair already paused (already in the
   blocklist / out of the allowlist) is left alone and does not eat the cap.
6. `core.partner_freeze` — a frozen partner's demand is never touched.
7. `--max-pause` caps a run (default 10). `--apply` plus `TBX_ALLOW_WRITES=1`,
   enforced independently by core.tbx_mgmt. Ledger; `--revert`.
8. An unattended run announces every pause to Slack.

Exit codes: 0 ok / nothing to do · 1 a write failed · 2 creds absent or
window unmeasured · 3 platform unreachable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx                          # noqa: E402
from core import tbx_mgmt as tbm                         # noqa: E402
from core import partner_freeze                          # noqa: E402
from scripts import tbx_trim as trim                     # noqa: E402
from scripts import tbx_dark_demand as dd                # noqa: E402

_HDR = "=" * 78
METRICS = ["requests_sum", "responses_sum"]


def ledger_path() -> str:
    return f"pairs-ledger-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"


# ------------------------------------------------------------------ measure

def pull_pairs(start: date, days: int) -> tuple[dict[tuple, dict], list[str]]:
    """{(sid, did): {sname, dname, per_day: {day: (req, resp)}}}, answered."""
    seen: dict[tuple, dict] = {}
    answered: list[str] = []
    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        try:
            rows, _ = tbx.report(day, day,
                                 attributes=["date", "supply_source", "demand_source"],
                                 metrics=METRICS)
        except tbx.TbxError as exc:
            print(f"    {day}: FAILED — {exc}", file=sys.stderr, flush=True)
            continue
        kept = 0
        for row in rows:
            if str(row.get("date") or "")[:10] != day:
                continue
            sname, sid = trim.split_name_id(row.get("supply_source") or "")
            dname, did = trim.split_name_id(row.get("demand_source") or "")
            if sid is None or did is None:
                continue
            e = seen.setdefault((sid, did), {"sname": sname, "dname": dname, "per_day": {}})
            prev = e["per_day"].get(day, (0.0, 0.0))
            e["per_day"][day] = (prev[0] + trim.num(row, "requests_sum"),
                                 prev[1] + trim.num(row, "responses_sum"))
            kept += 1
        print(f"    {day}: {kept} pair rows", flush=True)
        if kept:
            answered.append(day)
    return seen, answered


def demand_responses(seen: dict[tuple, dict]) -> dict[int, float]:
    """Total responses per demand source across every supply in the window."""
    out: dict[int, float] = {}
    for (_, did), e in seen.items():
        out[did] = out.get(did, 0.0) + sum(r for _, r in e["per_day"].values())
    return out


def supply_lists(sids: set[int]) -> dict[int, dict]:
    out = {}
    for sid in sorted(sids):
        try:
            cfg = tbm.get_supply_source(sid) or {}
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! supply {sid}: {exc}", file=sys.stderr)
            continue
        raw = cfg.get("demand_sources") or []
        ids = []
        for x in raw:
            try:
                ids.append(int(x["id"] if isinstance(x, dict) else x))
            except (TypeError, ValueError, KeyError):
                pass
        out[sid] = {"is_allowed": bool(cfg.get("is_allowed_sources")),
                    "demand_sources": ids, "name": cfg.get("name")}
    return out


# ------------------------------------------------------------------- select

def select(seen, answered, dresp, status, lists, args
           ) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Pairs dark on every answered day, with the write each one needs."""
    targets, held = [], []
    n = len(answered)
    for (sid, did), e in seen.items():
        per_day = e["per_day"]
        row = {"sid": sid, "sname": e["sname"], "did": did, "dname": e["dname"],
               "requests_day": sum(r for r, _ in per_day.values()) / n}
        if any(d not in per_day for d in answered):
            continue                                   # new, not dark
        if any(per_day[d][0] < args.min_requests_day for d in answered):
            continue                                   # not a fair chance
        if any(per_day[d][1] > 0 for d in answered):
            continue                                   # it answered
        tag = f"{e['sname']} #{sid} → {e['dname']} #{did}"
        if args.include and (did not in args.include and sid not in args.include):
            continue
        if did in args.exclude or sid in args.exclude:
            held.append((row, "excluded"))
            continue
        if dresp.get(did, 0.0) <= 0:
            held.append((row, "demand answered nowhere — tbx_dark_demand's case"))
            continue
        if not status.get(did, False):
            held.append((row, "demand source already off"))
            continue
        if partner_freeze.is_frozen(demand_id=did, demand_name=e["dname"]):
            held.append((row, "frozen partner"))
            continue
        lst = lists.get(sid)
        if lst is None:
            held.append((row, "supply allow/block list unreadable"))
            continue
        ids = lst["demand_sources"]
        if not lst["is_allowed"]:                      # blocklist
            if did in ids:
                held.append((row, "already in the supply's blocklist"))
                continue
            row["mode"] = "block"
        else:                                          # allowlist
            if did not in ids:
                held.append((row, "allowlist does not contain this demand yet it "
                                  "receives requests — list not governing; refusing"))
                continue
            if len(ids) <= 1:
                held.append((row, "would empty the allowlist; its meaning is "
                                  "undocumented — refusing"))
                continue
            row["mode"] = "allow"
        targets.append(row)
    targets.sort(key=lambda r: -r["requests_day"])
    return targets, held


def plan_writes(targets: list[dict], lists: dict[int, dict]) -> list[dict]:
    """One write per supply source carrying every paused pair on it."""
    by_sid: dict[int, list[dict]] = {}
    for t in targets:
        by_sid.setdefault(t["sid"], []).append(t)
    writes = []
    for sid, ts in by_sid.items():
        lst = lists[sid]
        before = list(lst["demand_sources"])
        dids = [t["did"] for t in ts]
        if lst["is_allowed"]:
            after = [d for d in before if d not in dids]
        else:
            after = before + [d for d in dids if d not in before]
        writes.append({"sid": sid, "sname": ts[0]["sname"],
                       "is_allowed": lst["is_allowed"],
                       "before": before, "after": after,
                       "pairs": [{"did": t["did"], "dname": t["dname"],
                                  "requests_day": t["requests_day"]} for t in ts]})
    return writes


# -------------------------------------------------------------------- write

def apply(writes: list[dict], args) -> tuple[list[dict], int]:
    entries, failures = [], 0
    for w in writes:
        names = ", ".join(f"{p['dname']} #{p['did']}" for p in w["pairs"])
        reason = (f"tbx_dark_pairs: 0 bid responses on all {args.days} settled days "
                  f"from {w['sname']} → {names}")
        try:
            result = tbm.set_supply_allowed_demand(
                w["sid"], w["after"], is_allowed=w["is_allowed"],
                actor=args.actor, reason=reason, dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ supply {w['sid']} ({w['sname']}): {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ supply {w['sid']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
            continue
        entries.append({**w, "days": args.days, "applied": bool(result.get("applied")),
                        "verify_ok": result.get("verify_ok")})
    return entries, failures


def revert(path: str, args) -> int:
    with open(path) as fh:
        ledger = json.load(fh)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0
    print(f"Restoring demand lists on {len(entries)} supply source(s) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for e in entries:
        try:
            result = tbm.set_supply_allowed_demand(
                e["sid"], e["before"], is_allowed=e["is_allowed"],
                actor=args.actor, reason=f"revert of {os.path.basename(path)}",
                dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ supply {e['sid']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ supply {e['sid']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ supply {e['sid']} {e['sname']} → "
                  f"{'allow' if e['is_allowed'] else 'block'}list restored "
                  f"({len(e['before'])} ids)")
    return 1 if failures else 0


# ------------------------------------------------------------------- report

def render(targets, held, writes, answered, args) -> None:
    print(f"\n{_HDR}\nConnections with 0 bid responses on all {len(answered)} settled "
          f"day(s) ({answered[0]} → {answered[-1]}), ≥ {args.min_requests_day:,.0f} "
          f"req/day each day\n{_HDR}")
    if not targets:
        print("  none")
    for t in targets[:args.max_pause]:
        print(f"  {t['requests_day']:>12,.0f} req/day   {t['sname']} #{t['sid']} → "
              f"{t['dname']} #{t['did']}   [{t['mode']}list]")
    over = targets[args.max_pause:]
    if over:
        print(f"\n  ⚠ {len(over)} more exceed --max-pause {args.max_pause} and will NOT "
              f"be touched this run:")
        for t in over[:10]:
            print(f"    {t['requests_day']:>12,.0f}   {t['sname']} #{t['sid']} → "
                  f"{t['dname']} #{t['did']}")
    if held:
        print(f"\n  held ({len(held)}):")
        for row, why in held[:15]:
            print(f"    {row['requests_day']:>12,.0f}   {row['sname']} #{row['sid']} → "
                  f"{row['dname']} #{row['did']}: {why}")
        if len(held) > 15:
            print(f"    … {len(held) - 15} more")
    if writes:
        print(f"\n  writes: {len(writes)} supply list(s) — "
              + "; ".join(f"#{w['sid']} {'allow' if w['is_allowed'] else 'block'}list "
                          f"{len(w['before'])} → {len(w['after'])} ids" for w in writes))


def announce(entries: list[dict], args, ledger: str) -> None:
    pairs = [(e["sname"], e["sid"], p) for e in entries for p in e["pairs"]]
    if not pairs:
        return
    lines = [f"TBX dark pairs — paused {len(pairs)} connection(s): 0 bid responses on all "
             f"{args.days} settled days, ≥ {args.min_requests_day:,.0f} req/day."]
    for sname, sid, p in pairs:
        lines.append(f"  • {sname} #{sid} → {p['dname']} #{p['did']}  "
                     f"({p['requests_day']:,.0f} req/day)")
    lines.append(f"Ledger {ledger} (run artifact). Undo: tbx_dark_pairs.py --revert.")
    try:
        from core import slack
        slack.send_text("\n".join(lines))
    except Exception as exc:                           # noqa: BLE001
        print(f"  ! slack post failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------- cli

def parse_ids(raw: str | None) -> set[int]:
    return {int(x) for x in raw.replace(",", " ").split()} if raw else set()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pause supply→demand connections with "
                                            "zero bid responses on every recent day.")
    p.add_argument("--days", type=int, default=3,
                   help="settled days that must ALL be dark (default 3 = 'more than 2 days')")
    p.add_argument("--min-requests-day", type=float, default=10_000,
                   help="each day must carry at least this many requests (default 10000)")
    p.add_argument("--max-pause", type=int, default=10)
    p.add_argument("--include", default="", help="only these supply or demand ids")
    p.add_argument("--exclude", default="", help="never these supply or demand ids")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--slack", action="store_true")
    p.add_argument("--revert", metavar="LEDGER")
    p.add_argument("--actor", default="tbx_dark_pairs")
    p.add_argument("--ledger", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.include = parse_ids(args.include)
    args.exclude = parse_ids(args.exclude)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.", file=sys.stderr)
        return 2
    if args.revert:
        return revert(args.revert, args)
    if args.days < 1:
        print("::error::--days must be ≥ 1", file=sys.stderr)
        return 1

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Measuring {start} → {end} ({args.days} settled days), pair grain\n")
    seen, answered = pull_pairs(start, args.days)
    if len(answered) < args.days:
        print(f"\n::error::only {len(answered)}/{args.days} day(s) answered — a failed "
              f"day is unmeasured, not silent. Refusing to pause anything.",
              file=sys.stderr)
        return 2

    status = dd.live_status()
    if not status:
        print("::error::could not read demand source status — refusing to write "
              "against unknown state.", file=sys.stderr)
        return 2
    dresp = demand_responses(seen)

    # Only read config for supplies that have a candidate pair on them.
    n = len(answered)
    cand_sids = {sid for (sid, did), e in seen.items()
                 if all(d in e["per_day"] for d in answered)
                 and all(e["per_day"][d][0] >= args.min_requests_day for d in answered)
                 and all(e["per_day"][d][1] == 0 for d in answered)}
    lists = supply_lists(cand_sids)

    targets, held = select(seen, answered, dresp, status, lists, args)
    writes = plan_writes(targets[:args.max_pause], lists)
    render(targets, held, writes, answered, args)
    if not writes:
        return 0
    if not args.apply:
        print(f"\n{_HDR}\nDRY RUN — nothing was written.\n{_HDR}")
    print()
    entries, failures = apply(writes, args)
    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as fh:
            json.dump({"created": datetime.now(timezone.utc).isoformat(),
                       "actor": args.actor, "days": args.days,
                       "min_requests_day": args.min_requests_day,
                       "measured": answered, "entries": entries}, fh, indent=2)
        print(f"\nLedger: {path}\nUndo: python3 scripts/tbx_dark_pairs.py --revert {path} --apply")
        if args.slack:
            announce(entries, args, path)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
