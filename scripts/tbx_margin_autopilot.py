#!/usr/bin/env python3
"""
Margin autopilot: when a connection's realised take falls below the trigger,
move the one lever that lands it on target — automatically, inside rails.

Why this exists
---------------
The 2026-09-02/03 rollout put the book on ~30% by hand: demand floors on 1:1
connections, supply bands on fan-outs (`docs/teqblaze-new-platform.md`
§6.1a). Margins drift — a DSP re-bids, a publisher's mix shifts, a band is
edited in the UI — and nobody wants to re-run that by hand every morning.
This is the standing rule: **realised take < trigger (25%) → raise the
right floor so the compound lands on target (30%)**, one settled day at a
time, ledgered, revertible, and posted to Slack.

What it decides (per connection below the trigger, biggest gross first)
-----------------------------------------------------------------------
Stacking is compound and a band rests at its floor (§6.1a, measured):
T = 1 − (1−S)(1−D). So the floor that lands a connection on target is
`x = 1 − (1−target)/(1−other)`.

* **1:1 connection** (the demand endpoint buys from one supply source):
  raise the DEMAND floor to `x` given the supply floor.
* **Fan-out** (the demand endpoint buys across several supply sources): a
  demand floor would hit every leg, so raise the SUPPLY band on that
  source instead — but only if the source's gross-weighted take across
  all of its legs is itself below the trigger. If the supply is fine on
  average and one leg is low, there is no one-sided lever; it is reported.
* **Floors not honoured**: if realised take sits below what the configured
  floors already imply (by more than `not_honoured_gap_pp`), raising the
  floor cannot help — the platform is not applying it. Alert, do not write.
  This is also the runaway guard: a floor that does not take can never be
  raised again the next day.

Rails
-----
1. `config/tbx_margin_autopilot.json` is the rule. `enabled: false` is the
   kill switch; `exclude_supply` / `exclude_demand` pin sources; every
   threshold is there, not here. Edit it by PR.
2. Window must be fully settled (every day answered) or nothing happens.
3. `max_writes` per run (default 4), `max_raise_pp` per step (10), absolute
   caps on any floor (demand 40, supply 30), and an overshoot cap: a supply
   raise is trimmed so no leg's compound projection passes
   `overshoot_cap_pct` (45).
4. Adaptive bands are converted to `range` on write — adaptive ignores the
   floor (verified no-op, §6.1a); the ledger keeps the prior type.
5. `core.partner_freeze`: demand writes go through `set_demand_economics`,
   which honours it; a supply raise is refused when any leg's demand is a
   frozen partner, because it would change that partner's economics.
6. `dry_run=True` per call plus `TBX_ALLOW_WRITES=1`, enforced by
   core.tbx_mgmt. Every applied run writes `autopilot-ledger-*.json`;
   `--revert` restores type/min/max on both kinds.
7. Slack gets one message per run with every write and every alert.

Exit codes: 0 ok / nothing to do · 1 a write failed · 2 creds absent or
window not settled · 3 platform unreachable
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

sys.path.insert(0, __file__.rsplit("/scripts/", 1)[0])

from core import tbx_api as tbx                          # noqa: E402
from core import tbx_mgmt as tbm                         # noqa: E402
from core import partner_freeze                          # noqa: E402
from scripts import tbx_trim as trim                     # noqa: E402
from scripts import tbx_connection_margin as cm          # noqa: E402

_HDR = "=" * 78
REPO = __file__.rsplit("/scripts/", 1)[0]
DEFAULT_CONFIG = os.path.join(REPO, "config", "tbx_margin_autopilot.json")

DEFAULTS = {
    "enabled": True,
    "trigger_pct": 25.0,
    "target_pct": 30.0,
    "days": 1,
    "min_gross_day": 10.0,
    "top": 80,
    "max_writes": 4,
    "max_raise_pp": 10.0,
    "demand_floor_cap_pct": 40.0,
    "supply_floor_cap_pct": 30.0,
    "overshoot_cap_pct": 45.0,
    "not_honoured_gap_pp": 3.0,
    "fee_pct": 8.0,
    "exclude_supply": [],
    "exclude_demand": [],
}


def ledger_path() -> str:
    return f"autopilot-ledger-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"


# ------------------------------------------------------------------ config

def load_config(path: str) -> dict:
    cfg = dict(DEFAULTS)
    if path and os.path.exists(path):
        with open(path) as fh:
            raw = json.load(fh)
        cfg.update({k: v for k, v in raw.items() if not k.startswith("_")})
    cfg["exclude_supply"] = {int(x) for x in cfg.get("exclude_supply") or []}
    cfg["exclude_demand"] = {int(x) for x in cfg.get("exclude_demand") or []}
    if not (0 < cfg["trigger_pct"] < cfg["target_pct"] < 100):
        raise ValueError("config: need 0 < trigger_pct < target_pct < 100")
    if cfg["max_writes"] < 0 or cfg["max_raise_pp"] <= 0:
        raise ValueError("config: max_writes >= 0 and max_raise_pp > 0")
    return cfg


# ------------------------------------------------------------------ maths

def effective_floor(b: dict | None) -> float:
    """The floor the platform actually applies. Adaptive ignores its min."""
    if not b or not b.get("type"):
        return 0.0
    if b["type"] == "adaptive":
        return 0.0
    return float(b["min"] or 0.0)


def compound(s_pct: float, d_pct: float) -> float:
    return (1.0 - (1.0 - s_pct / 100.0) * (1.0 - d_pct / 100.0)) * 100.0


def floor_for_target(other_pct: float, target_pct: float) -> float:
    other = min(max(other_pct, 0.0), 99.0)
    return (1.0 - (1.0 - target_pct / 100.0) / (1.0 - other / 100.0)) * 100.0


def band_step(b: dict, want: float, cap: float, max_raise: float) -> dict:
    """One capped step of a band toward `want`. Returns a plan or a refusal."""
    cur = float(b["min"] or 0.0)
    new_min = round(min(want, cap, cur + max_raise), 1)
    if new_min <= cur + 0.05:
        return {"refuse": f"floor already {cur:g}%, proposal {want:.1f}% "
                          f"(cap {cap:g}) is not a raise"}
    if new_min <= 0:
        return {"refuse": f"proposal {new_min:g}% is not a margin"}
    note = ""
    if new_min < round(min(want, cap), 1):
        note = f" (stepped: full raise to {min(want, cap):.1f}% exceeds {max_raise:g}pp)"
    if want > cap:
        note += f" (capped at {cap:g}%)"
    convert = "range" if b["type"] == "adaptive" else None
    if b["type"] == "fixed":
        new_max = None
    else:
        cur_max = float(b["max"] or 0.0)
        new_max = cur_max if cur_max > new_min else round(new_min + cm.MAX_HEADROOM_PP, 1)
    return {"margin_min": new_min, "margin_max": new_max,
            "margin_type": convert, "note": note}


# ----------------------------------------------------------------- partial

def partial_today(last_settled, min_gross: float = 1.0) -> dict[tuple, float]:
    """
    {(sid, did): take%} for the days AFTER the last settled one, up to now —
    normally just today, unsettled. Never used to size a raise (partial days
    inflate proposals); used only to tell "band changed since yesterday"
    from "floor not honoured". Returns {} when nothing answered.
    """
    today = datetime.now(timezone.utc).date()
    start = last_settled + timedelta(days=1)
    ndays = (today - start).days + 1
    if ndays < 1:
        return {}
    print(f"\n  reading the unsettled day(s) {start} → {today} for the in-flight check")
    pairs, answered = cm.pull_pairs(start, ndays)
    if not answered:
        print("  (no partial-day data answered — in-flight check unavailable)")
        return {}
    out = {}
    for key, e in pairs.items():
        if e["gross"] >= min_gross:
            out[key] = (e["gross"] - e["payout"]) / e["gross"] * 100.0
    return out


# ------------------------------------------------------------------ decide

def decide(rows: list[dict], cfg: dict, partial: dict | None = None
           ) -> tuple[list[dict], list[str], list[str]]:
    """
    rows: output of tbx_connection_margin.assess (sorted by gross desc).
    partial: {(sid, did): take%} for the CURRENT, unsettled day — the one
    signal that says whether a band changed after the measured day. The
    bands are read now; the take was realised yesterday. If yesterday sits
    below what today's bands imply, either the platform ignores the floor
    (alert) or the floor was raised since (hold and wait). Today's partial
    take tells the two apart: still below the implied floor → not honoured;
    at or above it → in flight.
    Returns (writes, alerts, notes). Alerts are things a human should see;
    notes are refusals that need no action.
    """
    trig, target = cfg["trigger_pct"], cfg["target_pct"]
    partial = partial or {}
    todo, alerts, notes = [], [], []
    by_sid: dict[int, list[dict]] = {}
    for r in rows:
        by_sid.setdefault(r["sid"], []).append(r)
    seen_sids: set[int] = set()

    for r in rows:
        if r["take"] >= trig:
            continue
        tag = f"{r['sname']} #{r['sid']} → {r['dname']} #{r['did']} ({r['take']:.1f}%)"
        if r["sid"] in cfg["exclude_supply"] or r["did"] in cfg["exclude_demand"]:
            notes.append(f"{tag}: excluded by config")
            continue
        s_b, d_b = r.get("s_band"), r.get("d_band")
        if not s_b or not d_b or not s_b.get("type") or not d_b.get("type"):
            alerts.append(f"{tag}: margin config unreadable — nothing written")
            continue
        today = partial.get((r["sid"], r["did"]))
        if today is not None and today >= trig:
            notes.append(f"{tag}: today's partial take is {today:.1f}% ≥ {trig:g}% — a "
                         f"change is already in flight; waiting for it to settle")
            continue
        implied = compound(effective_floor(s_b), effective_floor(d_b))
        if r["take"] < implied - cfg["not_honoured_gap_pp"]:
            if today is None:
                notes.append(f"{tag}: below what the current floors imply "
                             f"({implied:.1f}%) and no partial-day read to say whether "
                             f"the band changed since — holding one day")
                continue
            if today >= implied - cfg["not_honoured_gap_pp"]:
                notes.append(f"{tag}: band changed after the measured day — today's "
                             f"partial take {today:.1f}% is consistent with the current "
                             f"floors ({implied:.1f}%); waiting for a settled day")
                continue
            alerts.append(f"{tag}: realised is {implied - r['take']:.1f}pp BELOW what "
                          f"the configured floors imply ({implied:.1f}%), and today's "
                          f"partial ({today:.1f}%) is too — the platform is not applying "
                          f"a floor here; raising it would not help. Not written.")
            continue

        if r["one_to_one"]:
            want = floor_for_target(effective_floor(s_b), target)
            step = band_step(d_b, want, cfg["demand_floor_cap_pct"], cfg["max_raise_pp"])
            if "refuse" in step:
                notes.append(f"{tag}: demand {step['refuse']}")
                continue
            todo.append({
                "kind": "demand", "did": r["did"], "dname": r["dname"],
                "sid": r["sid"], "sname": r["sname"], "take": r["take"],
                "gross_day": r["gross_day"], "before": dict(d_b),
                "projected": compound(effective_floor(s_b), step["margin_min"]),
                **step,
            })
            continue

        # fan-out: the lever is the supply band, judged on the whole source
        if r["sid"] in seen_sids:
            continue
        seen_sids.add(r["sid"])
        legs = by_sid[r["sid"]]
        gross = sum(l["gross_day"] for l in legs) or 1e-9
        wtake = sum(l["gross_day"] * l["take"] for l in legs) / gross
        today_legs = [(l, partial.get((l["sid"], l["did"]))) for l in legs]
        if any(t is not None for _, t in today_legs):
            g2 = sum(l["gross_day"] for l, t in today_legs if t is not None) or 1e-9
            wtoday = sum(l["gross_day"] * t for l, t in today_legs if t is not None) / g2
            if wtoday >= trig:
                notes.append(f"{tag}: fan-out; today's partial weighted take on supply "
                             f"#{r['sid']} is {wtoday:.1f}% ≥ {trig:g}% — a change is "
                             f"already in flight; waiting for it to settle")
                continue
        if wtake >= trig:
            notes.append(f"{tag}: fan-out; supply-weighted take across "
                         f"{len(legs)} leg(s) is {wtake:.1f}% ≥ {trig:g}% — the low "
                         f"leg is demand-specific and a supply raise would "
                         f"overshoot the others")
            continue
        frozen = [l["dname"] for l in legs
                  if partner_freeze.is_frozen(demand_id=l["did"], demand_name=l["dname"])]
        if frozen:
            notes.append(f"{tag}: supply raise refused — leg(s) on frozen partner: "
                         f"{', '.join(frozen[:3])}")
            continue
        d_w = sum(l["gross_day"] * effective_floor(l.get("d_band")) for l in legs) / gross
        want = floor_for_target(d_w, target)
        # trim so no leg's projection passes the overshoot cap
        for l in legs:
            d_l = effective_floor(l.get("d_band"))
            lim = floor_for_target(d_l, cfg["overshoot_cap_pct"])
            want = min(want, lim)
        step = band_step(s_b, want, cfg["supply_floor_cap_pct"], cfg["max_raise_pp"])
        if "refuse" in step:
            notes.append(f"{tag}: supply {step['refuse']}")
            continue
        todo.append({
            "kind": "supply", "sid": r["sid"], "sname": r["sname"],
            "legs": [l["did"] for l in legs], "take": wtake,
            "gross_day": gross, "before": dict(s_b),
            "projected": compound(step["margin_min"], d_w),
            **step,
        })

    return todo[:cfg["max_writes"]], alerts, notes


# ------------------------------------------------------------------- write

def _fmt(w: dict) -> str:
    b = w["before"]
    mx = "" if w["margin_max"] is None else f"–{w['margin_max']:g}"
    typ = f" [{b['type']} → {w['margin_type']}]" if w.get("margin_type") else ""
    if w["kind"] == "demand":
        who = f"demand {w['dname']} #{w['did']} on {w['sname']} #{w['sid']}"
    else:
        who = f"supply {w['sname']} #{w['sid']} ({len(w['legs'])} legs)"
    return (f"{who}: {w['take']:.1f}% → floor {b['min']:g}% → {w['margin_min']:g}%{mx}"
            f"{typ}, projected {w['projected']:.1f}%{w['note']}")


def apply(todo: list[dict], args) -> tuple[list[dict], int]:
    entries, failures = [], 0
    for w in todo:
        reason = f"tbx_margin_autopilot: {_fmt(w)}"
        kw = {"margin_min": w["margin_min"]}
        if w["margin_max"] is not None:
            kw["margin_max"] = w["margin_max"]
        if w.get("margin_type"):
            kw["margin_type"] = w["margin_type"]
        try:
            if w["kind"] == "demand":
                result = tbm.set_demand_economics(
                    w["did"], actor=args.actor, reason=reason,
                    dry_run=not args.apply, demand_name=w["dname"], **kw)
            else:
                result = tbm.set_supply_margin(
                    w["sid"], actor=args.actor, reason=reason,
                    dry_run=not args.apply, **kw)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {w['kind']} {w.get('did') or w['sid']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ {w['kind']} {w.get('did') or w['sid']} refused: "
                  f"{result.get('refused', '?')}", file=sys.stderr)
            failures += 1
            continue
        entries.append({**w, "applied": bool(result.get("applied")),
                        "verify_ok": result.get("verify_ok")})
    return entries, failures


def revert(path: str, args) -> int:
    with open(path) as fh:
        ledger = json.load(fh)
    entries = [e for e in ledger.get("entries", []) if e.get("applied")]
    if not entries:
        print(f"{path} records no applied writes — nothing to revert.")
        return 0
    print(f"Restoring {len(entries)} band(s) from {path}"
          f"{'' if args.apply else '  (DRY RUN)'}\n")
    failures = 0
    for e in entries:
        b = e["before"]
        kw = {"margin_type": b["type"], "margin_min": b["min"]}
        if b["type"] != "fixed":
            kw["margin_max"] = b["max"]
        reason = f"revert of {os.path.basename(path)}"
        try:
            if e["kind"] == "demand":
                result = tbm.set_demand_economics(
                    e["did"], actor=args.actor, reason=reason,
                    dry_run=not args.apply, demand_name=e.get("dname"), **kw)
            else:
                result = tbm.set_supply_margin(
                    e["sid"], actor=args.actor, reason=reason,
                    dry_run=not args.apply, **kw)
        except Exception as exc:                       # noqa: BLE001
            print(f"  ✗ {e['kind']} {e.get('did') or e['sid']}: {exc}", file=sys.stderr)
            failures += 1
            continue
        if args.apply and not result.get("applied"):
            print(f"  ✗ {e['kind']} {e.get('did') or e['sid']} refused: "
                  f"{result.get('refused', '?')}", file=sys.stderr)
            failures += 1
        else:
            print(f"  ✓ {e['kind']} {e.get('did') or e['sid']} → "
                  f"{b['type']} {b['min']:g}–{b['max']:g}")
    return 1 if failures else 0


# ------------------------------------------------------------------- report

def summary(answered, rows, todo, entries, alerts, notes, cfg, args) -> str:
    below = [r for r in rows if r["take"] < cfg["trigger_pct"]]
    mode = "APPLIED" if args.apply else "DRY RUN"
    lines = [f"TBX margin autopilot — {', '.join(answered)} — {mode}",
             f"{len(below)} of {len(rows)} connections below {cfg['trigger_pct']:g}% "
             f"(target {cfg['target_pct']:g}%)"]
    if entries:
        lines.append("Writes:")
        for e in entries:
            mark = "✓" if e.get("verify_ok") else ("✗" if e.get("verify_ok") is False else "·")
            lines.append(f"  {mark} {_fmt(e)}")
    elif todo:
        lines.append("Would write:")
        lines += [f"  · {_fmt(w)}" for w in todo]
    else:
        lines.append("No write needed.")
    if alerts:
        lines.append("Alerts (need a human):")
        lines += [f"  ⚠ {a}" for a in alerts]
    if notes:
        lines.append(f"Refused/held: {len(notes)}")
        lines += [f"  · {n}" for n in notes[:8]]
        if len(notes) > 8:
            lines.append(f"  · … {len(notes) - 8} more in the run log")
    return "\n".join(lines)


def post_slack(text: str) -> None:
    try:
        from core import slack
        slack.send_text(text)
    except Exception as exc:                           # noqa: BLE001
        print(f"  ! slack post failed: {exc}", file=sys.stderr)


# --------------------------------------------------------------------- cli

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--config", default=DEFAULT_CONFIG)
    p.add_argument("--apply", action="store_true",
                   help="write (also needs TBX_ALLOW_WRITES=1); default is dry run")
    p.add_argument("--slack", action="store_true", help="post the summary to SLACK_WEBHOOK")
    p.add_argument("--revert", metavar="LEDGER")
    p.add_argument("--actor", default="tbx_margin_autopilot")
    p.add_argument("--ledger", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config(args.config)
    if not tbx.configured():
        print("TBX_EMAIL / TBX_PASSWORD are not set — nothing to read.", file=sys.stderr)
        return 2
    if args.revert:
        return revert(args.revert, args)
    if not cfg["enabled"]:
        print("autopilot is disabled in config (enabled=false). Nothing done.")
        return 0

    end = trim.latest_settled(datetime.now(timezone.utc))
    start = end - timedelta(days=cfg["days"] - 1)
    print(f"Measuring {start} → {end} ({cfg['days']} settled day(s)); trigger "
          f"{cfg['trigger_pct']:g}%, target {cfg['target_pct']:g}%\n")
    pairs, answered = cm.pull_pairs(start, cfg["days"])
    if len(answered) < cfg["days"]:
        print(f"\n::error::only {len(answered)}/{cfg['days']} day(s) answered — "
              f"refusing to act on a partial window.", file=sys.stderr)
        return 2

    n = len(answered)
    fan = cm.fanout(pairs, cfg["min_gross_day"], n)
    ranked = sorted(pairs.items(), key=lambda kv: -kv[1]["gross"])
    top = [k for k, e in ranked if e["gross"] / n >= cfg["min_gross_day"]][:cfg["top"]]
    sids = {s for s, _ in top}
    dids = {d for _, d in top}
    print(f"  {len(pairs)} connection(s); reading config for {len(sids)} supply + "
          f"{len(dids)} demand ...")
    supply_cfg, demand_cfg = cm.read_config(sids, dids)
    cm_args = SimpleNamespace(min_gross_day=cfg["min_gross_day"],
                              fee_pct=cfg["fee_pct"], target=cfg["target_pct"])
    rows = cm.assess({k: pairs[k] for k in top}, answered, supply_cfg, demand_cfg,
                     fan, cm_args)
    cm.render(rows, answered, cm_args)

    partial = partial_today(end)
    todo, alerts, notes = decide(rows, cfg, partial)
    print(f"\n{_HDR}\nAutopilot decision (trigger {cfg['trigger_pct']:g}%)\n{_HDR}")
    for a in alerts:
        print(f"  ⚠ {a}")
    for nline in notes:
        print(f"  · {nline}")
    for w in todo:
        print(f"  → {_fmt(w)}")
    if not todo:
        print("  nothing to write")

    entries, failures = ([], 0)
    if todo:
        if not args.apply:
            print(f"\n{_HDR}\nDRY RUN — nothing was written.\n{_HDR}")
        print()
        entries, failures = apply(todo, args)
        if args.apply and entries:
            path = args.ledger or ledger_path()
            with open(path, "w") as fh:
                json.dump({"created": datetime.now(timezone.utc).isoformat(),
                           "actor": args.actor, "config": {k: (sorted(v) if isinstance(v, set) else v)
                                                           for k, v in cfg.items()},
                           "measured": answered, "entries": entries}, fh, indent=2)
            print(f"\nLedger: {path}\nUndo: python3 scripts/tbx_margin_autopilot.py "
                  f"--revert {path} --apply")

    text = summary(answered, rows, todo, entries, alerts, notes, cfg, args)
    print(f"\n{text}")
    if args.slack and (entries or alerts or todo):
        post_slack(text)
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except tbx.TbxError as exc:
        print(f"\nplatform unreachable: {exc}", file=sys.stderr)
        sys.exit(3)
