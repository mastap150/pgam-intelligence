#!/usr/bin/env python3
"""
Margin per CONNECTION — one supply source sold to one demand source — with
both sides' configured bands, the realised total, and the demand setting that
would land the total on a target.

Why connections, not entities
-----------------------------
The two margin tools here look at one side at a time (`tbx_margin_sentry`)
or at the blended total (`tbx_net_margin`). Neither can answer "what does
this buyer take on this publisher", and that is the unit a 30% target is
stated in. The report answers it directly at pair grain:

    attributes = [date, supply_source, demand_source]

What the 2026-09-02 sentry runs established, and this tool builds on:

  * Realised take exceeds the DEMAND ceiling on 9 connections and the
    SUPPLY ceiling on 2. A side capped at 20% cannot realise 34.7% alone, so
    **the two sides stack**. Setting demand to the target where supply
    already takes a cut overshoots and underpays the publisher.
  * The thin connections realise ~2 points above a 5–7% configured floor,
    on both sides. The adaptive algorithm is sitting at the bottom of its
    band, not misbehaving — which means the floor is the lever.
  * Supply margin is read-only over this API (§6.1). Demand margin is
    writable. So the demand floor is the ONLY lever this tool can pull, and
    it can only hit a per-connection target if connections are 1:1. The
    pair grain checks that per demand source, and the apply path refuses a
    demand source that fans out across several supply sources with material
    gross — one setting cannot land on all of them.

How the proposal is computed, and what it assumes
-------------------------------------------------
Realised total T, supply band S, demand band D. The exact composition
(additive vs multiplicative) is undocumented; at these magnitudes the two
differ by ~3 points, so both are shown and the additive one is used for the
write because it is the conservative one (it proposes the smaller raise).

    supply's effective contribution   S_eff = T - D_eff
    demand floor to hit the target    D_new = target - S_eff
                                            = D_eff + (target - T)

i.e. raise the demand floor by exactly the gap. `D_eff` is taken as the
configured `margin_min`, on the evidence above that adaptive sits at its
floor on the connections that matter. Where the demand band is `fixed`
that is exact; where it is range/adaptive it is an estimate and is marked.

The write raises `margin_min` (and lifts `margin_max` if it would otherwise
sit below the new floor). It never changes `margin_type`: flipping range to
fixed swaps mechanisms, and that is a different decision.

Rails
-----
1. Every day in the window must have answered — a failed day is unmeasured,
   not a day of zero revenue.
2. `--apply` requires `--include`: this tool never writes book-wide. One
   connection is the right first write, measured the next settled day.
3. `--max-apply` caps a run (default 3); `--max-raise-pp` caps how far one
   step can move a floor (default 15 points). A margin change moves what a
   publisher is paid; it is commercial, not tuning.
4. A demand source that is not 1:1 is reported and refused.
5. `dry_run=True` per call plus `TBX_ALLOW_WRITES=1`, enforced independently
   by core.tbx_mgmt. Every applied run writes a ledger; `--revert` restores
   margin_min/margin_max exactly.
6. `core.partner_freeze` is honoured by set_demand_economics.

Exit codes:
    0  ok / nothing to do
    1  a write was refused or failed
    2  credentials absent, or the window could not be measured
    3  the platform was unreachable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx          # noqa: E402
from core import tbx_mgmt as tbm         # noqa: E402
from scripts import tbx_trim as trim     # noqa: E402

_HDR = "=" * 78
METRICS = ["imps_sum", "dsp_price_sum", "ssp_price_sum"]
# When a raise pushes the floor past the ceiling, the ceiling moves to
# floor + this. Never floor == ceiling: the platform 422s on equality.
MAX_HEADROOM_PP = 5.0


def ledger_path() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"margin-ledger-{stamp}.json"


# ------------------------------------------------------------------ measure

def pull_pairs(start, days: int) -> tuple[dict[tuple, dict], list[str]]:
    """{(sid, did): {snames, dname, gross, payout, imps}} over the window."""
    pairs: dict[tuple, dict] = {}
    answered: list[str] = []
    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        try:
            rows, _ = tbx.report(day, day,
                                 attributes=["date", "supply_source",
                                             "demand_source"],
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
            entry = pairs.setdefault((sid, did), {
                "sname": sname, "dname": dname,
                "gross": 0.0, "payout": 0.0, "imps": 0.0})
            entry["gross"] += trim.num(row, "dsp_price_sum")
            entry["payout"] += trim.num(row, "ssp_price_sum")
            entry["imps"] += trim.num(row, "imps_sum")
            kept += 1
        print(f"    {day}: {kept} pair rows", flush=True)
        if kept:
            answered.append(day)
    return pairs, answered


def fanout(pairs: dict[tuple, dict], min_gross_day: float, n_days: int
           ) -> dict[int, list[int]]:
    """demand id -> supply ids it buys with material gross."""
    out: dict[int, set[int]] = {}
    for (sid, did), e in pairs.items():
        if e["gross"] / n_days >= min_gross_day:
            out.setdefault(did, set()).add(sid)
    return {d: sorted(s) for d, s in out.items()}


def band(cfg: dict) -> dict:
    kind = (cfg.get("margin_type") or "").lower() or None
    try:
        lo = float(cfg.get("margin_min") or 0)
    except (TypeError, ValueError):
        lo = 0.0
    try:
        hi = float(cfg.get("margin_max") or 0)
    except (TypeError, ValueError):
        hi = 0.0
    if kind == "fixed":
        hi = lo
    return {"type": kind, "min": lo, "max": hi}


def read_config(sids: set[int], dids: set[int]) -> tuple[dict, dict]:
    supply, demand = {}, {}
    for sid in sorted(sids):
        try:
            supply[sid] = band(tbm.get_supply_source(sid) or {})
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! supply {sid}: {exc}", file=sys.stderr)
    for did in sorted(dids):
        try:
            demand[did] = band(tbm.get_demand_source(did) or {})
        except Exception as exc:                       # noqa: BLE001
            print(f"  ! demand {did}: {exc}", file=sys.stderr)
    return supply, demand


# ------------------------------------------------------------------ assess

def propose(take: float, d_band: dict, target: float) -> dict:
    """Demand floor that lands the connection on `target`, both models."""
    d_eff = d_band["min"]
    s_eff_add = take - d_eff
    additive = target - s_eff_add                       # == d_eff + gap
    # compound: T = 1 - (1-S)(1-D) with S from the additive estimate
    s = max(min(s_eff_add / 100.0, 0.99), -0.99)
    compound = (1.0 - (1.0 - target / 100.0) / (1.0 - s)) * 100.0
    return {
        "d_eff": d_eff, "s_eff": s_eff_add,
        "additive": additive, "compound": compound,
        "exact": d_band["type"] == "fixed",
    }


def assess(pairs, answered, supply_cfg, demand_cfg, fan, args) -> list[dict]:
    n = len(answered)
    out = []
    for (sid, did), e in pairs.items():
        gross_day = e["gross"] / n
        if gross_day < args.min_gross_day or e["gross"] <= 0:
            continue
        take = (e["gross"] - e["payout"]) / e["gross"] * 100.0
        s_band = supply_cfg.get(sid)
        d_band = demand_cfg.get(did)
        row = {
            "sid": sid, "sname": e["sname"], "did": did, "dname": e["dname"],
            "gross_day": gross_day, "take": take,
            "net": take - args.fee_pct,
            "gap": args.target - take,
            "s_band": s_band, "d_band": d_band,
            "one_to_one": len(fan.get(did, [])) <= 1,
            "peers": fan.get(did, []),
            "proposal": propose(take, d_band, args.target) if d_band else None,
        }
        out.append(row)
    out.sort(key=lambda r: -r["gross_day"])
    return out


def fmt_band(b: dict | None) -> str:
    if not b or not b["type"]:
        return "      ?      "
    if b["type"] == "fixed":
        return f"fixed {b['min']:>4.0f}%   "
    return f"{b['type'][:5]:<5} {b['min']:>3.0f}–{b['max']:<3.0f}%"


def render(rows: list[dict], answered: list[str], args) -> None:
    print(f"\n{_HDR}\nConnections (supply → demand), per day over "
          f"{len(answered)} settled day(s) — target {args.target:g}% total, "
          f"{args.fee_pct:g}% fee\n{_HDR}")
    print(f"  {'gross/d':>8} {'take':>6} {'net':>6}  {'supply band':<14} "
          f"{'demand band':<14} {'→ demand floor for target':<26} connection")
    print(f"  {'-'*8} {'-'*6} {'-'*6}  {'-'*14} {'-'*14} {'-'*26} {'-'*40}")
    for r in rows:
        p = r["proposal"]
        if p is None:
            prop = "        (no demand config)"
        elif r["take"] >= args.target - 0.5:
            prop = "        at target"
        else:
            est = "" if p["exact"] else "≈"
            prop = (f"{est}{p['additive']:>5.1f}%  (compound {p['compound']:.1f}%)")
        flag = "" if r["one_to_one"] else f"   ⚠ not 1:1 — also sells to supply {r['peers']}"
        print(f"  {r['gross_day']:>8,.2f} {r['take']:>5.1f}% {r['net']:>5.1f}%  "
              f"{fmt_band(r['s_band']):<14} {fmt_band(r['d_band']):<14} "
              f"{prop:<26} {r['sname'][:22]} #{r['sid']} → "
              f"{r['dname'][:24]} #{r['did']}{flag}")

    below = [r for r in rows if r["take"] < args.target - 0.5]
    lost = sum(r["gross_day"] * r["gap"] / 100.0 for r in below)
    print(f"\n  {len(below)} of {len(rows)} connections below {args.target:g}%. "
          f"Closing every gap is {trim.money(lost)}/day of margin.")
    print("  '≈' marks a demand band that is range/adaptive: the proposal "
          "assumes it sits at its floor,\n  which is what the thin "
          "connections show. A fixed band is exact.")


# ------------------------------------------------------------------- write

def plan_writes(rows: list[dict], args) -> tuple[list[dict], list[str]]:
    """The demand-floor raises this run would make, with refusals named."""
    todo, refused = [], []
    for r in rows:
        if r["did"] not in args.include:
            continue
        p = r["proposal"]
        if p is None:
            refused.append(f"demand {r['did']}: no readable margin config")
            continue
        if not r["one_to_one"]:
            refused.append(f"demand {r['did']}: not 1:1 (supply {r['peers']}) — "
                           f"one floor cannot land every connection on target")
            continue
        if r["take"] >= args.target - 0.5:
            refused.append(f"demand {r['did']}: already at target ({r['take']:.1f}%)")
            continue
        kind = r["d_band"]["type"]
        # 2026-09-03, demand 1986: POST returned 200 and margin_min stayed at
        # 5. The update endpoint does not honour the floor on an `adaptive`
        # band — the same shape as the supply-side fields in §6.1. Converting
        # the type is a mechanism change this tool does not make on its own.
        if kind == "adaptive":
            refused.append(f"demand {r['did']}: band is adaptive — the update "
                           f"endpoint ignores margin_min on that type (verified "
                           f"no-op on #1986). Convert to range first.")
            continue
        new_min = round(p["additive"], 1)
        raise_pp = new_min - r["d_band"]["min"]
        if raise_pp > args.max_raise_pp:
            new_min = round(r["d_band"]["min"] + args.max_raise_pp, 1)
            note = f" (capped: full raise was {raise_pp:.1f}pp)"
        else:
            note = ""
        if new_min <= 0:
            refused.append(f"demand {r['did']}: proposal {new_min}% is not a margin")
            continue
        # A fixed band has one number; the platform reports margin_max as 0
        # for it and a verify on that field is noise (demand 35, 2026-09-03).
        if kind == "fixed":
            new_max = None
        else:
            # The platform requires max STRICTLY above min — equality is a 422
            # ("Max Margin Value must be greater than 25.7", demand 2408).
            cur_max = r["d_band"]["max"]
            new_max = cur_max if cur_max > new_min else round(new_min + MAX_HEADROOM_PP, 1)
        todo.append({
            "did": r["did"], "dname": r["dname"], "sid": r["sid"],
            "sname": r["sname"], "take": r["take"], "gross_day": r["gross_day"],
            "before": dict(r["d_band"]),
            "margin_min": new_min, "margin_max": new_max, "note": note,
        })
    return todo[:args.max_apply], refused


def apply_writes(todo: list[dict], args) -> tuple[list[dict], int]:
    entries, failures = [], 0
    for w in todo:
        reason = (f"tbx_connection_margin: {w['sname']} → {w['dname']} realised "
                  f"{w['take']:.1f}% vs target {args.target:g}%; demand floor "
                  f"{w['before']['min']:g}% → {w['margin_min']:g}%{w['note']}")
        try:
            kwargs = {"margin_min": w["margin_min"]}
            if w.get("margin_max") is not None:
                kwargs["margin_max"] = w["margin_max"]
            result = tbm.set_demand_economics(
                w["did"], actor=args.actor, reason=reason,
                dry_run=not args.apply, demand_name=w["dname"], **kwargs)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {w['did']} ({w['dname']}): {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {w['did']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
            continue
        entries.append({**w, "applied": bool(result.get("applied"))})
    return entries, failures


def revert(path: str, args) -> int:
    with open(path) as handle:
        ledger = json.load(handle)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0
    print(f"Restoring {len(entries)} demand margin band(s) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for e in entries:
        b = e["before"]
        try:
            kwargs = {"margin_min": b["min"]}
            if b.get("type") != "fixed":
                kwargs["margin_max"] = b["max"]
            result = tbm.set_demand_economics(
                e["did"], actor=args.actor,
                reason=f"revert of {os.path.basename(path)}",
                dry_run=not args.apply, demand_name=e.get("dname"), **kwargs)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ demand {e['did']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ demand {e['did']} refused: {result.get('refused', '?')}",
                  file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ demand {e['did']} {e.get('dname', '')}  "
                  f"→ {b['min']:g}–{b['max']:g}%")
    return 1 if failures else 0


# --------------------------------------------------------------------- cli

def parse_ids(raw: str | None) -> set[int]:
    return {int(x) for x in raw.replace(",", " ").split()} if raw else set()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Margin per supply→demand connection, and the demand "
                    "floor that lands each on a target.")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--target", type=float, default=30.0,
                   help="total take %% per connection (default 30)")
    p.add_argument("--fee-pct", type=float, default=8.0)
    p.add_argument("--top", type=int, default=40,
                   help="connections by gross to read config for "
                        "(two GETs each, default 40)")
    p.add_argument("--min-gross-day", type=float, default=5.0)
    p.add_argument("--include", default="",
                   help="demand ids eligible for --apply (required with it)")
    p.add_argument("--max-apply", type=int, default=3)
    p.add_argument("--max-raise-pp", type=float, default=15.0,
                   help="most a floor may move in one step (default 15)")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--revert", metavar="LEDGER")
    p.add_argument("--actor", default="tbx_connection_margin")
    p.add_argument("--ledger", default=None)
    p.add_argument("--json", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.include = parse_ids(args.include)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.",
              file=sys.stderr)
        return 2
    if args.revert:
        return revert(args.revert, args)
    if args.apply and not args.include:
        print("::error::--apply requires --include: this tool never writes "
              "margins book-wide. Name the demand id(s).", file=sys.stderr)
        return 1

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=args.days - 1)
    print(f"Measuring {start} → {end} ({args.days} settled days), pair grain\n")
    pairs, answered = pull_pairs(start, args.days)

    if len(answered) < args.days:
        print(f"\n::error::only {len(answered)}/{args.days} day(s) answered. "
              f"Refusing to conclude anything about margins from a partial "
              f"window. Re-run.", file=sys.stderr)
        return 2

    n = len(answered)
    fan = fanout(pairs, args.min_gross_day, n)
    ranked = sorted(pairs.items(), key=lambda kv: -kv[1]["gross"])
    top = [k for k, e in ranked if e["gross"] / n >= args.min_gross_day][:args.top]
    sids = {s for s, _ in top} | {s for s, d in pairs if d in args.include}
    dids = {d for _, d in top} | set(args.include)
    print(f"\n  {len(pairs)} connection(s) seen; reading config for "
          f"{len(sids)} supply + {len(dids)} demand ...")
    supply_cfg, demand_cfg = read_config(sids, dids)

    rows = assess({k: pairs[k] for k in pairs if k in set(top)
                   or k[1] in args.include},
                  answered, supply_cfg, demand_cfg, fan, args)
    render(rows, answered, args)

    if args.json:
        print("\n" + json.dumps(rows, indent=2, default=str))
    if not args.include:
        return 0

    todo, refused = plan_writes(rows, args)
    print(f"\n{_HDR}\nDemand floor raises this run would make\n{_HDR}")
    for why in refused:
        print(f"  · refused — {why}")
    for w in todo:
        mx = ("(fixed — single value)" if w["margin_max"] is None
              else f"(max {w['before']['max']:g}% → {w['margin_max']:g}%)")
        print(f"  {w['dname']} #{w['did']} on {w['sname']} #{w['sid']}: "
              f"floor {w['before']['min']:g}% → {w['margin_min']:g}% {mx}{w['note']}")
    if not todo:
        return 0
    if not args.apply:
        print(f"\n{_HDR}\nDRY RUN — nothing was written. Re-run with --apply "
              f"(and TBX_ALLOW_WRITES=1).\n{_HDR}")

    print()
    entries, failures = apply_writes(todo, args)
    if args.apply and entries:
        path = args.ledger or ledger_path()
        with open(path, "w") as handle:
            json.dump({"created": datetime.now(timezone.utc).isoformat(),
                       "actor": args.actor, "target": args.target,
                       "measured": answered, "entries": entries},
                      handle, indent=2)
        print(f"\nLedger: {path}\nUndo with: python3 scripts/"
              f"tbx_connection_margin.py --revert {path} --apply")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
