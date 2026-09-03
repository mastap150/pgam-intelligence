#!/usr/bin/env python3
"""
The one supply-side margin lever this API will accept — read it, and test it
on one source.

Why this exists
---------------
`margin_type` / `margin_min` / `margin_max` on a SUPPLY source are read-only
over this API (§6.1): present on the Resource, absent from the Request. That
left every fan-out connection — a demand endpoint that buys across several
supply sources — with no per-connection lever at all, because a demand floor
lands on all of its supply legs at once. Illumin - Video Unruly #65, the
largest source in the book at ~12% take, is the standing example.

But `SupplySourceRequest.source` is `oneOf [DirectInventoryResource,
IndirectSuppliersResource]`, and the indirect-supplier shape carries

    is_dynamic_margin   boolean
    dynamic_margin      number, "%", example 30

Both are in the WRITE schema. `core.tbx_mgmt.set_supply_source_fields` has
exposed them since it was written. What nobody has established is what they
DO: §6.1 records `is_dynamic_margin: false` on every source read so far and
notes that switching it on "swaps which mechanism governs the source" rather
than adjusting the current one. Its interaction with a live `margin_type` is
undocumented (§6.1, and question 2 of the Teqblaze list).

So this is an instrument, not a rollout. It reads the shape of a source and,
behind every gate, sets dynamic margin on ONE source so the next settled day
can say what it did. If the realised take on that source moves to the value
set, the supply side is writable after all and #65 is reachable. If it does
not, that is worth knowing before anyone asks Teqblaze for a field that may
already exist.

Rails
-----
1. Refuses a source whose `source.type` is not indirect-supplier: the field
   does not exist on direct inventory and the write would be a guess.
2. `--apply` requires `--include`, and `--max-apply` defaults to 1. This is
   a test, and a test is one source.
3. `is_smart_floor` is reported. A source the platform optimiser already
   owns is flagged, because two controllers on one source is the April
   thrash — though margin and floor are different levers, and this tool does
   not touch the floor.
4. `dry_run=True` per call plus `TBX_ALLOW_WRITES=1`, enforced independently
   by core.tbx_mgmt. Ledger; `--revert` restores the exact prior pair.

Direct mode (added 2026-09-03)
------------------------------
The first live dynamic-margin write (source 196, run 33814887759) never got
to test the field: the update endpoint answered `422 margin_type /
margin_min / margin_max REQUIRED`. The spec omits those three from
`SupplySourceRequest`, which is the whole reason §6.1 called them read-only
— nobody had tried. Required is not the same as honoured, but it is a
strong hint, so this tool now has a second mode that sets the top-level
band directly via `core.tbx_mgmt.set_supply_margin`:

    --margin-min 20 [--margin-max 40]     write the band, not dynamic_margin

Same rails: one source, `--include`, dry-run default, ledger, `--revert`
restores the exact prior band. `_apply_update` re-reads after the write, so
a `verify ✗` on `margin_min` is the platform ignoring the field — the
answer to question 2 for Teqblaze, either way.

Exit codes: 0 ok · 1 a write refused/failed · 2 nothing readable or creds
absent · 3 platform unreachable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402

_HDR = "=" * 78
INDIRECT = ("indirect", "indirect_suppliers", "indirect-suppliers", "ssp", "rtb")


def ledger_path() -> str:
    return f"dynmargin-ledger-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"


def shape(entity: dict) -> dict:
    src = (entity or {}).get("source") or {}
    return {
        "id": entity.get("id"), "name": entity.get("name"),
        "type": (src.get("type") or entity.get("type") or "").lower(),
        "margin_type": entity.get("margin_type"),
        "margin_min": entity.get("margin_min"), "margin_max": entity.get("margin_max"),
        "is_dynamic_margin": bool(src.get("is_dynamic_margin")),
        "dynamic_margin": float(src.get("dynamic_margin") or 0),
        "is_smart_floor": bool(src.get("is_smart_floor")),
        "has_dynamic_fields": "is_dynamic_margin" in src,
    }


def is_indirect(s: dict) -> bool:
    """The field is only defined on the indirect-supplier shape. The type
    string is not pinned by the spec, so presence of the field is the
    stronger test and the type name the fallback."""
    return s["has_dynamic_fields"] or any(t in s["type"] for t in INDIRECT)


def read(ids: list[int]) -> list[dict]:
    out = []
    for sid in ids:
        try:
            out.append(shape(tbm.get_supply_source(sid) or {}))
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! supply {sid}: {exc}", file=sys.stderr)
    return out


def render(rows: list[dict]) -> None:
    print(f"\n{_HDR}\nSupply-side margin shape\n{_HDR}")
    print(f"  {'id':>5}  {'type':<20} {'band':<16} {'dyn?':<5} {'dyn%':>5} "
          f"{'smartfloor':<10} name")
    for s in rows:
        band = f"{s['margin_type'] or '?'} {s['margin_min']}–{s['margin_max']}"
        print(f"  {s['id']:>5}  {s['type'][:20]:<20} {band:<16} "
              f"{'yes' if s['is_dynamic_margin'] else 'no':<5} "
              f"{s['dynamic_margin']:>5.1f} "
              f"{'ON' if s['is_smart_floor'] else 'off':<10} {s['name']}")
    print("\n  'band' is margin_type/min/max — omitted from the write schema but\n"
          "  REQUIRED by the live update endpoint (§6.1a); --margin-min writes it.\n"
          "  'dyn' is is_dynamic_margin/dynamic_margin — in the write schema on\n"
          "  indirect-supplier sources. What either governs is what a test answers.")


def direct_mode(args) -> bool:
    return args.margin_min is not None or args.margin_max is not None


def _fnum(v) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def plan(rows: list[dict], args) -> tuple[list[dict], list[str]]:
    todo, refused = [], []
    for s in rows:
        if s["id"] not in args.include:
            continue
        if direct_mode(args):
            cur_min, cur_max = _fnum(s["margin_min"]), _fnum(s["margin_max"])
            new_min = args.margin_min if args.margin_min is not None else cur_min
            new_max = args.margin_max if args.margin_max is not None else cur_max
            if s["margin_type"] != "fixed" and new_min is not None \
                    and new_max is not None and new_max <= new_min:
                refused.append(f"supply {s['id']}: {s['margin_type']} band needs "
                               f"max > min, got {new_min:g}–{new_max:g} "
                               f"(pass --margin-max too)")
                continue
            if new_min == cur_min and new_max == cur_max:
                refused.append(f"supply {s['id']}: band already "
                               f"{s['margin_type']} {cur_min}–{cur_max}")
                continue
            todo.append(s)
            continue
        if not is_indirect(s):
            refused.append(f"supply {s['id']}: type '{s['type']}' is not "
                           f"indirect-supplier — the field does not exist on it")
            continue
        if s["is_dynamic_margin"] and abs(s["dynamic_margin"] - args.set) < 1e-9:
            refused.append(f"supply {s['id']}: already dynamic at {args.set:g}%")
            continue
        todo.append(s)
    return todo[:args.max_apply], refused


def apply_direct(todo: list[dict], args) -> tuple[list[dict], int]:
    """Direct mode: write the top-level band via set_supply_margin."""
    entries, failures = [], 0
    for s in todo:
        kw = {}
        if args.margin_min is not None:
            kw["margin_min"] = args.margin_min
        if args.margin_max is not None:
            kw["margin_max"] = args.margin_max
        reason = (f"tbx_dynamic_margin DIRECT: {s['name']} band "
                  f"{s['margin_type']} {s['margin_min']}–{s['margin_max']} → "
                  + ", ".join(f"{k}={v:g}" for k, v in kw.items()))
        try:
            result = tbm.set_supply_margin(
                s["id"], actor=args.actor, reason=reason,
                dry_run=not args.apply, **kw)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ supply {s['id']} ({s['name']}): {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ supply {s['id']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
            continue
        entries.append({
            "mode": "direct",
            "sid": s["id"], "name": s["name"],
            "before": {"margin_type": s["margin_type"],
                       "margin_min": _fnum(s["margin_min"]),
                       "margin_max": _fnum(s["margin_max"])},
            "after": kw,
            "applied": bool(result.get("applied")),
            "verify_ok": result.get("verify_ok"),
        })
    return entries, failures


def apply(todo: list[dict], args) -> tuple[list[dict], int]:
    if direct_mode(args):
        return apply_direct(todo, args)
    entries, failures = [], 0
    for s in todo:
        reason = (f"tbx_dynamic_margin TEST: {s['name']} band "
                  f"{s['margin_type']} {s['margin_min']}–{s['margin_max']}, "
                  f"dynamic {s['is_dynamic_margin']}/{s['dynamic_margin']:g} "
                  f"→ on/{args.set:g}%")
        try:
            result = tbm.set_supply_source_fields(
                s["id"], is_dynamic_margin=True, dynamic_margin=args.set,
                actor=args.actor, reason=reason, dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ supply {s['id']} ({s['name']}): {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ supply {s['id']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
            continue
        entries.append({
            "sid": s["id"], "name": s["name"],
            "before": {"is_dynamic_margin": s["is_dynamic_margin"],
                       "dynamic_margin": s["dynamic_margin"]},
            "after": {"is_dynamic_margin": True, "dynamic_margin": args.set},
            "band_at_write": {"type": s["margin_type"], "min": s["margin_min"],
                              "max": s["margin_max"]},
            "applied": bool(result.get("applied")),
            "verify_ok": result.get("verify_ok"),
        })
    return entries, failures


def revert(path: str, args) -> int:
    with open(path) as fh:
        ledger = json.load(fh)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0
    print(f"Restoring dynamic-margin state on {len(entries)} source(s) from "
          f"{path}{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for e in entries:
        b = e["before"]
        try:
            if e.get("mode") == "direct":
                result = tbm.set_supply_margin(
                    e["sid"], margin_type=b.get("margin_type"),
                    margin_min=b.get("margin_min"), margin_max=b.get("margin_max"),
                    actor=args.actor,
                    reason=f"revert of {os.path.basename(path)}",
                    dry_run=not args.apply)
            else:
                result = tbm.set_supply_source_fields(
                    e["sid"], is_dynamic_margin=b["is_dynamic_margin"],
                    dynamic_margin=b["dynamic_margin"], actor=args.actor,
                    reason=f"revert of {os.path.basename(path)}",
                    dry_run=not args.apply)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ supply {e['sid']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ supply {e['sid']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
        elif e.get("mode") == "direct":
            print(f"  ✓ supply {e['sid']} {e['name']} → band "
                  f"{b.get('margin_type')} {b.get('margin_min')}–{b.get('margin_max')}")
        else:
            print(f"  ✓ supply {e['sid']} {e['name']} → "
                  f"dynamic {b['is_dynamic_margin']}/{b['dynamic_margin']:g}")
    return 1 if failures else 0


def parse_ids(raw: str | None) -> set[int]:
    return {int(x) for x in raw.replace(",", " ").split()} if raw else set()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--ids", default="",
                   help="supply ids to READ the shape of (comma-separated)")
    p.add_argument("--include", default="",
                   help="supply ids eligible for --apply (required with it)")
    p.add_argument("--set", type=float, default=30.0,
                   help="dynamic_margin %% to set on --include (default 30)")
    p.add_argument("--margin-min", type=float, default=None,
                   help="DIRECT mode: set the top-level margin_min on --include "
                        "instead of dynamic_margin")
    p.add_argument("--margin-max", type=float, default=None,
                   help="DIRECT mode: set the top-level margin_max (range bands "
                        "need max > min)")
    p.add_argument("--max-apply", type=int, default=1,
                   help="this is a test; default 1")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--revert", metavar="LEDGER")
    p.add_argument("--actor", default="tbx_dynamic_margin")
    p.add_argument("--ledger", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.include = parse_ids(args.include)
    ids = sorted(parse_ids(args.ids) | args.include)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.", file=sys.stderr)
        return 2
    if args.revert:
        return revert(args.revert, args)
    if args.apply and not args.include:
        print("::error::--apply requires --include. One source.", file=sys.stderr)
        return 1
    if not ids:
        print("nothing to read: pass --ids and/or --include", file=sys.stderr)
        return 2

    rows = read(ids)
    if not rows:
        print("::error::no source was readable.", file=sys.stderr)
        return 2
    render(rows)
    if not args.include:
        return 0

    todo, refused = plan(rows, args)
    what = "Supply margin-band" if direct_mode(args) else "Dynamic-margin"
    print(f"\n{_HDR}\n{what} write this run would make\n{_HDR}")
    for why in refused:
        print(f"  · refused — {why}")
    for s in todo:
        flag = "  ⚠ is_smart_floor is ON — platform optimiser owns this source's floor" \
               if s["is_smart_floor"] else ""
        if direct_mode(args):
            tgt_min = args.margin_min if args.margin_min is not None else s["margin_min"]
            tgt_max = args.margin_max if args.margin_max is not None else s["margin_max"]
            print(f"  supply {s['id']} {s['name']}: band {s['margin_type']} "
                  f"{s['margin_min']}–{s['margin_max']} → {tgt_min}–{tgt_max}{flag}")
            continue
        print(f"  supply {s['id']} {s['name']}: is_dynamic_margin "
              f"{s['is_dynamic_margin']} → True, dynamic_margin "
              f"{s['dynamic_margin']:g} → {args.set:g}{flag}")
    if not todo:
        return 0
    if not args.apply:
        print(f"\n{_HDR}\nDRY RUN — nothing was written.\n{_HDR}")
    print()
    entries, failures = apply(todo, args)
    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as fh:
            json.dump({"created": datetime.now(timezone.utc).isoformat(),
                       "actor": args.actor,
                       "mode": "direct" if direct_mode(args) else "dynamic",
                       "set": args.set, "margin_min": args.margin_min,
                       "margin_max": args.margin_max, "entries": entries},
                      fh, indent=2)
        print(f"\nLedger: {path}\nUndo: python3 scripts/tbx_dynamic_margin.py "
              f"--revert {path} --apply")
        if direct_mode(args):
            bad = [e for e in entries if e.get("verify_ok") is False]
            if bad:
                print("\n⚠ verify ✗ — the platform accepted the POST but the band did\n"
                      "  not change. Supply margin is NOT honoured over this API;\n"
                      "  that is the answer for Teqblaze.")
            else:
                print("\nverify ✓ — the band changed on the platform. Measure the next\n"
                      "SETTLED day (tbx-connection-margin) to see it in the take rate.")
        else:
            print("\nMeasure the next SETTLED day for this source (tbx-net-margin or\n"
                  "tbx-connection-margin). If its take rate moved to the value set,\n"
                  "the supply side is writable. If not, revert and tell Teqblaze.")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
