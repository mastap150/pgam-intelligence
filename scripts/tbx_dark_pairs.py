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
supply is pure outbound cost. The lever is an allow/block list — the
connection is paused, both parties stay live.

Where the write lands — and why it has to pick a side
-----------------------------------------------------
Both entities carry the same shape: `is_allowed_sources` (bool) plus a list
of the other side's ids (`demand_sources[]` on supply, `supply_sources[]` on
demand) plus `companies[]`. `false` = BLOCKLIST, `true` = ALLOWLIST, and the
spec's own wording is "Companies and Supply Sources is allowed" — a pair can
be let through by the *company* even when the id is absent from the list.
The first dry run showed exactly that: Erie News Now #1503 receives requests
from thirteen demand ids that are not in its allowlist.

So removing an id from an allowlist is not a pause when a company-level
allow covers the pair, and the tool never assumes it is. Per pair, in order:

    1. supply is a BLOCKLIST         → add the demand id there
    2. demand is a BLOCKLIST         → add the supply id there
    3. supply ALLOWLIST contains the demand id, and the demand's company is
       NOT in the supply's companies, and the list would not empty
                                     → remove the demand id
    4. same on the demand side       → remove the supply id
    5. otherwise                     → refused, with the reason, as an alert

Lists replace wholesale on the wire (§6.2 shape): the current list is read,
the full intended set is written, the exact prior list is ledgered, and
`--revert` puts it back.

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
5. The demand source must be live. A pair that is *already* blocked on
   either side yet still carries requests is an ALERT ("pause not
   effective"), never a second write.
6. `core.partner_freeze` — a frozen partner's demand is never touched.
7. `--max-pause` caps a run (default 10). `--apply` plus `TBX_ALLOW_WRITES=1`,
   enforced independently by core.tbx_mgmt. Ledger; `--revert`.
8. An unattended run announces every pause and every alert to Slack.

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
ALERT = "ALERT: "


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


def _ints(raw) -> list[int]:
    out = []
    for x in raw or []:
        try:
            out.append(int(x["id"] if isinstance(x, dict) else x))
        except (TypeError, ValueError, KeyError):
            pass
    return out


def _lists(cfg: dict, key: str) -> dict:
    try:
        company = int(cfg.get("company_id")) if cfg.get("company_id") is not None else None
    except (TypeError, ValueError):
        company = None
    return {"is_allowed": bool(cfg.get("is_allowed_sources")),
            "ids": _ints(cfg.get(key)), "companies": _ints(cfg.get("companies")),
            "company_id": company, "name": cfg.get("name")}


def supply_lists(sids: set[int]) -> dict[int, dict]:
    out = {}
    for sid in sorted(sids):
        try:
            out[sid] = _lists(tbm.get_supply_source(sid) or {}, "demand_sources")
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! supply {sid}: {exc}", file=sys.stderr)
    return out


def demand_lists(dids: set[int]) -> dict[int, dict]:
    out = {}
    for did in sorted(dids):
        try:
            out[did] = _lists(tbm.get_demand_source(did) or {}, "supply_sources")
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! demand {did}: {exc}", file=sys.stderr)
    return out


# ------------------------------------------------------------------- select

def choose_side(sid: int, did: int, S: dict, D: dict) -> tuple[str | None, str]:
    """Which list to edit, or (None, why not). Blocklists first."""
    if not S["is_allowed"] and did in S["ids"]:
        return None, (ALERT + "already in the supply's blocklist yet still receiving "
                      "requests — pause not effective")
    if not D["is_allowed"] and sid in D["ids"]:
        return None, (ALERT + "already in the demand's blocklist yet still receiving "
                      "requests — pause not effective")
    if not S["is_allowed"]:
        return "supply_block", ""
    if not D["is_allowed"]:
        return "demand_block", ""
    why = []
    if did in S["ids"]:
        if D["company_id"] is not None and D["company_id"] in S["companies"]:
            why.append(f"supply allowlist also allows the demand's company "
                       f"#{D['company_id']}")
        elif len(S["ids"]) <= 1:
            why.append("removing it would empty the supply allowlist")
        else:
            return "supply_allow", ""
    else:
        why.append("demand id not in the supply allowlist yet traffic flows"
                   + (f" (company #{D['company_id']} allowed)"
                      if D["company_id"] in S["companies"] else ""))
    if sid in D["ids"]:
        if S["company_id"] is not None and S["company_id"] in D["companies"]:
            why.append(f"demand allowlist also allows the supply's company "
                       f"#{S['company_id']}")
        elif len(D["ids"]) <= 1:
            why.append("removing it would empty the demand allowlist")
        else:
            return "demand_allow", ""
    else:
        why.append("supply id not in the demand allowlist yet traffic flows"
                   + (f" (company #{S['company_id']} allowed)"
                      if S["company_id"] in D["companies"] else ""))
    return None, ALERT + "no unambiguous pause: " + "; ".join(why) + " — needs a human"


def select(seen, answered, dresp, status, slists, dlists, args
           ) -> tuple[list[dict], list[tuple[dict, str]]]:
    """Pairs dark on every answered day, each with the write it needs."""
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
        S, D = slists.get(sid), dlists.get(did)
        if S is None or D is None:
            held.append((row, "allow/block list unreadable on one side"))
            continue
        mode, why = choose_side(sid, did, S, D)
        if mode is None:
            held.append((row, why))
            continue
        row["mode"] = mode
        targets.append(row)
    targets.sort(key=lambda r: -r["requests_day"])
    return targets, held


def plan_writes(targets: list[dict], slists: dict, dlists: dict) -> list[dict]:
    """One write per (side, entity) carrying every paused pair on it."""
    groups: dict[tuple, list[dict]] = {}
    for t in targets:
        side = "supply" if t["mode"].startswith("supply") else "demand"
        key = (side, t["sid"] if side == "supply" else t["did"])
        groups.setdefault(key, []).append(t)
    writes = []
    for (side, eid), ts in groups.items():
        lst = slists[eid] if side == "supply" else dlists[eid]
        before = list(lst["ids"])
        others = [t["did"] if side == "supply" else t["sid"] for t in ts]
        if lst["is_allowed"]:
            after = [x for x in before if x not in others]
        else:
            after = before + [x for x in others if x not in before]
        writes.append({"side": side, "id": eid,
                       "name": ts[0]["sname"] if side == "supply" else ts[0]["dname"],
                       "is_allowed": lst["is_allowed"], "before": before, "after": after,
                       "pairs": [{"sid": t["sid"], "sname": t["sname"], "did": t["did"],
                                  "dname": t["dname"], "requests_day": t["requests_day"]}
                                 for t in ts]})
    return writes


# -------------------------------------------------------------------- write

def _write(side: str, eid: int, ids: list[int], is_allowed: bool, actor, reason, dry_run):
    if side == "supply":
        return tbm.set_supply_allowed_demand(eid, ids, is_allowed=is_allowed,
                                             actor=actor, reason=reason, dry_run=dry_run)
    return tbm.set_demand_allowed_supply(eid, ids, is_allowed=is_allowed,
                                         actor=actor, reason=reason, dry_run=dry_run)


def apply(writes: list[dict], args) -> tuple[list[dict], int]:
    entries, failures = [], 0
    for w in writes:
        pairs = ", ".join(f"{p['sname']} #{p['sid']} → {p['dname']} #{p['did']}"
                          for p in w["pairs"])
        reason = (f"tbx_dark_pairs: 0 bid responses on all {args.days} settled days: {pairs}")
        try:
            result = _write(w["side"], w["id"], w["after"], w["is_allowed"],
                            args.actor, reason, not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {w['side']} {w['id']} ({w['name']}): {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ {w['side']} {w['id']} refused: {result.get('refused', '?')}",
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
    print(f"Restoring lists on {len(entries)} entit(y/ies) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for e in entries:
        try:
            result = _write(e["side"], e["id"], e["before"], e["is_allowed"], args.actor,
                            f"revert of {os.path.basename(path)}", not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {e['side']} {e['id']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ {e['side']} {e['id']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ {e['side']} {e['id']} {e['name']} → "
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
              f"{t['dname']} #{t['did']}   [{t['mode']}]")
    over = targets[args.max_pause:]
    if over:
        print(f"\n  ⚠ {len(over)} more exceed --max-pause {args.max_pause} and will NOT "
              f"be touched this run:")
        for t in over[:10]:
            print(f"    {t['requests_day']:>12,.0f}   {t['sname']} #{t['sid']} → "
                  f"{t['dname']} #{t['did']}")
    alerts = [(r, w[len(ALERT):]) for r, w in held if w.startswith(ALERT)]
    quiet = [(r, w) for r, w in held if not w.startswith(ALERT)]
    if alerts:
        print(f"\n  alerts ({len(alerts)}) — pairs still carrying requests that this "
              f"tool cannot pause unambiguously:")
        for row, why in alerts[:15]:
            print(f"    {row['requests_day']:>12,.0f}   {row['sname']} #{row['sid']} → "
                  f"{row['dname']} #{row['did']}: {why}")
        if len(alerts) > 15:
            print(f"    … {len(alerts) - 15} more")
    if quiet:
        print(f"\n  held ({len(quiet)}):")
        for row, why in quiet[:10]:
            print(f"    {row['requests_day']:>12,.0f}   {row['sname']} #{row['sid']} → "
                  f"{row['dname']} #{row['did']}: {why}")
        if len(quiet) > 10:
            print(f"    … {len(quiet) - 10} more")
    if writes:
        print(f"\n  writes: {len(writes)} list(s) — "
              + "; ".join(f"{w['side']} #{w['id']} {'allow' if w['is_allowed'] else 'block'}list "
                          f"{len(w['before'])} → {len(w['after'])} ids" for w in writes))


def announce(entries: list[dict], held, args, ledger: str | None) -> None:
    pairs = [p for e in entries for p in e["pairs"]]
    alerts = [(r, w[len(ALERT):]) for r, w in held if w.startswith(ALERT)]
    if not pairs and not alerts:
        return
    lines = []
    if pairs:
        lines.append(f"TBX dark pairs — paused {len(pairs)} connection(s): 0 bid responses "
                     f"on all {args.days} settled days, ≥ {args.min_requests_day:,.0f} req/day.")
        for p in pairs:
            lines.append(f"  • {p['sname']} #{p['sid']} → {p['dname']} #{p['did']}  "
                         f"({p['requests_day']:,.0f} req/day)")
        if ledger:
            lines.append(f"Ledger {ledger} (run artifact). Undo: tbx_dark_pairs.py --revert.")
    if alerts:
        lines.append(f"⚠ {len(alerts)} dark connection(s) this tool cannot pause unambiguously:")
        for r, why in alerts[:8]:
            lines.append(f"  • {r['sname']} #{r['sid']} → {r['dname']} #{r['did']} "
                         f"({r['requests_day']:,.0f} req/day): {why}")
        if len(alerts) > 8:
            lines.append(f"  … {len(alerts) - 8} more in the run log")
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

    # Config is read only for entities with a candidate pair on them.
    cand = [(sid, did) for (sid, did), e in seen.items()
            if all(d in e["per_day"] for d in answered)
            and all(e["per_day"][d][0] >= args.min_requests_day for d in answered)
            and all(e["per_day"][d][1] == 0 for d in answered)
            and dresp.get(did, 0.0) > 0 and status.get(did, False)]
    print(f"  {len(cand)} candidate pair(s); reading lists for "
          f"{len({s for s, _ in cand})} supply + {len({d for _, d in cand})} demand ...")
    slists = supply_lists({s for s, _ in cand})
    dlists = demand_lists({d for _, d in cand})

    targets, held = select(seen, answered, dresp, status, slists, dlists, args)
    writes = plan_writes(targets[:args.max_pause], slists, dlists)
    render(targets, held, writes, answered, args)
    entries, failures, path = [], 0, None
    if writes:
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
    if args.slack and args.apply:
        announce(entries, held, args, path)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
