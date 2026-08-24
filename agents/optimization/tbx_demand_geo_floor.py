"""
agents/optimization/tbx_demand_geo_floor.py

Per-DSP × country bid-floor optimizer for the **new** Teqblaze platform
(`api.pgammedia.com`), writing `geo_settings.bid_floor` on demand sources.

Why this lever, and why this one first
--------------------------------------
The LL side runs a fleet of dynamic optimizers — floor tuning, wiring gaps,
dead-demand pruning, auto-revert. TBX has none, because until now it had no
verified read path to base decisions on. This is the first of them, and the
choice of lever is not arbitrary. `docs/teqblaze-new-platform.md` §7 tranche 3
names it the safest first writer for three reasons that still hold:

1. **Demand-side floors carry no publisher contract exposure.** A supply-side
   floor written wrong breaches a rate card — the 9 Dots $1.70 minimum is the
   live example — and `tbm.PROTECTED_FLOOR_MINIMUMS` is still **empty** on this
   platform, so nothing in code would stop it. A demand-side floor written
   wrong costs fill on one DSP and is reversible in one call.
2. **The lever does not exist on the legacy host**, so there is no risk of
   two platforms' agents fighting over one setting during the migration.
3. It is a *merge*, not a replace: `set_demand_geo_bid_floors(replace=False)`
   leaves every country this run did not name exactly as it was.

What it does
------------
1. Pulls `demand_source × country` over a trailing window from TBX.
2. Computes each pair's observed demand eCPM = dsp_price_sum / imps × 1000.
3. Proposes `floor = FLOOR_PCT × observed_ecpm` where the pair clears the
   quality bars, and only where that is a meaningful *increase* on what is
   set today.
4. By default **proposes**: prints, writes a recs file, posts to Slack, and
   changes nothing.

Three gates stand between this file and a live write, and all three must be
open:

    --apply                     on the command line (dry_run defaults True)
    TBX_ALLOW_WRITES=1          the platform-wide write gate in tbx_mgmt
    PGAM_OPTIMIZER_AUTO_APPLY=1 the fleet-wide autonomy gate

The third is what makes the scheduled run safe: `render.yaml` ships it at 0,
so the hourly-scheduled job proposes into Slack and a human decides. That
mirrors how the LL optimizers were introduced, and it is the setting the
April floor incidents argued for.

What it refuses to touch
------------------------
* **A DSP whose `is_smart_floor` is on.** That is Teqblaze's own floor
  optimizer. Two optimizers on one floor is the April thrash again, this time
  with a vendor on the other side. One owner per lever; if we want this DSP,
  turn theirs off first, deliberately.
* **A frozen partner** — `core.partner_freeze`, enforced inside
  `set_demand_geo_bid_floors` via the `demand_name` passthrough.
* **A DSP whose report name does not resolve to exactly one demand-source
  id.** Writing a floor to the wrong DSP is the failure worth being paranoid
  about, and an ambiguous name is not worth guessing at. Skipped and counted.

Clamps come from `core.tbx_mgmt.clamp_floor` and are applied twice on purpose:
once here so the proposal shows the number that would really land, and once
inside the writer, which is the one that actually binds. `TBX_MAX_FLOOR_DELTA`
(±25% by default) and the $0.01 zero-out guard both apply.

Usage
-----
    python -m agents.optimization.tbx_demand_geo_floor              # propose
    python -m agents.optimization.tbx_demand_geo_floor --days 7
    python -m agents.optimization.tbx_demand_geo_floor --apply      # write

Safe to schedule before credentials exist: with TBX_EMAIL / TBX_PASSWORD
absent it no-ops with a log line, matching `agents/etl/tbx_revenue_etl.py`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_LOG = "[tbx_demand_geo_floor]"

# ─── Tunables ────────────────────────────────────────────────────────────────

SCORING_WINDOW_DAYS = int(os.getenv("TBX_GEO_FLOOR_WINDOW_DAYS", "14"))

# Premium geos only, to start. A floor on long-tail traffic prices out the
# only bidder more often than it lifts the clear price, and the whole point of
# a first writer is that its blast radius is legible.
GEO_ALLOWLIST = [
    c.strip() for c in
    os.getenv("TBX_GEO_FLOOR_COUNTRIES", "USA,GBR,CAN,AUS,DEU").split(",")
    if c.strip()
]

# floor = FLOOR_PCT × observed eCPM. Below 1.0 by construction: a floor at or
# above what a DSP already clears removes them from the auction instead of
# repricing them.
FLOOR_PCT = float(os.getenv("TBX_GEO_FLOOR_PCT", "0.85"))

# Quality bars. A pair below either of these has not told us enough about
# itself for a floor to be anything but a guess with money on it.
MIN_IMPS_PER_PAIR = int(os.getenv("TBX_GEO_FLOOR_MIN_IMPS", "1000"))
MIN_ECPM = float(os.getenv("TBX_GEO_FLOOR_MIN_ECPM", "0.30"))

# Don't propose a rounding error. Below this the write costs more attention
# than the uplift is worth.
MIN_UPLIFT_PCT = float(os.getenv("TBX_GEO_FLOOR_MIN_UPLIFT", "0.05"))

# Per-run blast radius.
MAX_COUNTRIES_PER_SOURCE = int(os.getenv("TBX_GEO_FLOOR_MAX_COUNTRIES", "8"))
MAX_SOURCES_PER_RUN = int(os.getenv("TBX_GEO_FLOOR_MAX_SOURCES", "15"))

LOG_DIR = _REPO_ROOT / "logs"
RECS_FILE = LOG_DIR / "tbx_demand_geo_floor_recs.json"
ACTIONS_LOG = LOG_DIR / "tbx_demand_geo_floor_actions.json"

METRICS = ("imps_sum", "dsp_price_sum", "ssp_price_sum", "requests_sum")


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _f(value) -> float:
    """Platform numerics arrive as strings. Same helper as the ETL's."""
    if value in (None, "", "-"):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return 0.0


def _window(days: int) -> tuple[str, str]:
    """Trailing window ending yesterday — today is never settled."""
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return start.isoformat(), end.isoformat()


def auto_apply_enabled() -> bool:
    """The fleet-wide autonomy gate, shared with intelligence/proposer.py."""
    return os.environ.get("PGAM_OPTIMIZER_AUTO_APPLY", "0") == "1"


# ─── Data ────────────────────────────────────────────────────────────────────


def demand_country_rows(start: str, end: str) -> list[dict]:
    """`demand_source × country` over the window, one row per pair."""
    from core import tbx_api as tbx

    rows, _totals = tbx.report(
        start, end,
        attributes=["demand_source", "country"],
        metrics=list(METRICS),
    )
    return rows


def demand_source_index() -> tuple[dict[str, list[int]], dict[int, dict]]:
    """
    `(name -> [ids], id -> source)` over every demand source on the account.

    The report identifies a DSP by name; the write endpoint needs its id. The
    map is built name → *list* of ids rather than name → id so that a
    duplicated name is visible as ambiguity instead of resolving to whichever
    one happened to be last. An ambiguous name is skipped, never guessed.
    """
    from core import tbx_mgmt as tbm

    by_name: dict[str, list[int]] = defaultdict(list)
    by_id: dict[int, dict] = {}
    for src in tbm.list_demand_sources():
        sid = src.get("id")
        if sid is None:
            continue
        sid = int(sid)
        by_id[sid] = src
        name = (src.get("name") or src.get("title") or "").strip()
        if name:
            by_name[name.lower()].append(sid)
    return by_name, by_id


def _resolve_demand_id(row: dict, by_name: dict[str, list[int]]) -> tuple[int | None, str]:
    """
    `(demand_source_id, reason_if_unresolved)` for one report row.

    Tries the row's own id fields first — `agents/etl/tbx_revenue_etl._entity`
    already handles the three shapes this dimension comes back in — and falls
    back to matching the display name against the account's demand sources.
    """
    from agents.etl.tbx_revenue_etl import _entity

    entity_id, name = _entity(row, "demand_source")
    if entity_id is not None:
        return entity_id, ""
    if not name:
        return None, "row carries neither a demand_source id nor a name"

    matches = by_name.get(name.strip().lower(), [])
    if len(matches) == 1:
        return matches[0], ""
    if not matches:
        return None, f"'{name}' matches no demand source on the account"
    return None, f"'{name}' matches {len(matches)} demand sources — ambiguous"


# ─── Proposals ───────────────────────────────────────────────────────────────


def build_proposals(
    rows: list[dict],
    by_name: dict[str, list[int]],
    by_id: dict[int, dict],
    country_id_by_code: dict[str, int],
    fetch_source=None,
) -> tuple[list[dict], list[str]]:
    """
    Group report rows into one proposal per demand source.

    Two passes, and the reason is the shape of the API rather than taste:
    `POST /demand-sources` (the list) returns pacing and status but **not**
    `geo_settings`, so it cannot say what a DSP's floors are today. Only
    `GET /demand-sources/{id}` can. Reading the detail for every source on the
    account would be dozens of calls to price a handful, so pass one ranks
    candidates on report data alone and pass two fetches the detail for the
    survivors — bounded by `MAX_SOURCES_PER_RUN`.

    Skipping the detail fetch and treating a missing `geo_settings` as "no
    floor set" would be the quiet version of this bug: every proposal would
    read `$0.00 → $x`, the delta cap would never engage in the preview, and
    the number in Slack would not be the number that landed.

    Returns `(proposals, skips)`. `skips` is returned rather than logged in
    place because a silently skipped DSP looks identical to a DSP with nothing
    to do, and those are very different states to be in.

    `fetch_source` defaults to `tbx_mgmt.get_demand_source`; it is a parameter
    so the offline tests can drive this without a platform.
    """
    from core import tbx_mgmt as tbm

    if fetch_source is None:
        fetch_source = tbm.get_demand_source

    allow = {c.upper() for c in GEO_ALLOWLIST}
    skips: list[str] = []
    by_source: dict[int, list[dict]] = defaultdict(list)
    unresolved: dict[str, int] = defaultdict(int)

    # ── Pass 1: resolve ids, keep the pairs that clear the volume bars ──
    for row in rows:
        code = str(row.get("country") or "").strip()
        if not code or code.upper() not in allow:
            continue
        imps = int(_f(row.get("imps_sum")))
        if imps < MIN_IMPS_PER_PAIR:
            continue
        spend = _f(row.get("dsp_price_sum"))
        ecpm = (spend * 1000.0 / imps) if imps else 0.0
        if ecpm < MIN_ECPM:
            continue

        sid, why = _resolve_demand_id(row, by_name)
        if sid is None:
            unresolved[why] += 1
            continue
        by_source[sid].append({"country": code, "imps": imps,
                               "spend": spend, "ecpm": ecpm})

    for why, count in sorted(unresolved.items(), key=lambda kv: -kv[1]):
        skips.append(f"{count} qualifying row(s) dropped: {why}")

    # Rank on observed spend before spending a detail call on anyone. Spend
    # rather than eCPM: a $2.00 eCPM on 1,200 impressions is noise next to
    # $0.80 on 400,000.
    ranked = sorted(
        by_source.items(),
        key=lambda kv: -sum(p["spend"] for p in kv[1]),
    )
    if len(ranked) > MAX_SOURCES_PER_RUN:
        skips.append(
            f"{len(ranked) - MAX_SOURCES_PER_RUN} source(s) over the per-run "
            f"cap of {MAX_SOURCES_PER_RUN} — not considered this run"
        )
        ranked = ranked[:MAX_SOURCES_PER_RUN]

    # ── Pass 2: read each candidate's real config, then price it ──
    proposals: list[dict] = []
    for sid, pairs in ranked:
        listed = by_id.get(sid) or {}
        name = (listed.get("name") or listed.get("title") or f"#{sid}").strip()

        try:
            source = fetch_source(sid) or {}
        except Exception as exc:                      # noqa: BLE001
            skips.append(f"[{sid}] {name}: GET failed ({type(exc).__name__}: {exc})")
            continue
        name = (source.get("name") or source.get("title") or name).strip()

        if source.get("is_smart_floor"):
            # Teqblaze's own floor optimizer owns this DSP's floors. Writing
            # here would put two optimizers on one lever.
            skips.append(f"[{sid}] {name}: is_smart_floor is on — vendor owns this lever")
            continue

        geo = source.get("geo_settings") or {}
        current_by_country_id = {
            int(r["country_id"]): _f(r.get("value"))
            for r in (geo.get("bid_floor") or [])
            if r.get("country_id") is not None
        }

        picks: list[dict] = []
        for pair in pairs:
            code = pair["country"]
            country_id = country_id_by_code.get(code.strip().lower())
            if country_id is None:
                skips.append(f"[{sid}] {name}: country '{code}' has no platform id")
                continue

            current = current_by_country_id.get(country_id)
            proposed, clamps = tbm.clamp_floor(pair["ecpm"] * FLOOR_PCT,
                                               current=current)

            # Only ever propose an increase, and only a material one. A floor
            # cut has no upside this agent can measure — it gives away price
            # on traffic that was already clearing.
            baseline = current if current and current > 0 else 0.0
            if baseline > 0 and proposed <= baseline * (1 + MIN_UPLIFT_PCT):
                continue
            if baseline == 0 and proposed <= tbm.GLOBAL_MIN_FLOOR:
                continue

            picks.append({
                "country": code,
                "country_id": country_id,
                "current": round(baseline, 4),
                "proposed": proposed,
                "observed_ecpm": round(pair["ecpm"], 4),
                "imps": pair["imps"],
                "spend": round(pair["spend"], 2),
                "clamps": clamps,
            })

        if not picks:
            continue

        picks.sort(key=lambda p: -p["spend"])
        picks = picks[:MAX_COUNTRIES_PER_SOURCE]

        proposals.append({
            "demand_source_id": sid,
            "demand_name": name,
            "floors_by_country_id": {p["country_id"]: p["proposed"] for p in picks},
            "picks": picks,
            "spend_in_scope": round(sum(p["spend"] for p in picks), 2),
        })

    proposals.sort(key=lambda p: -p["spend_in_scope"])
    return proposals, skips


# ─── Apply ───────────────────────────────────────────────────────────────────


def apply_proposals(proposals: list[dict], dry_run: bool) -> list[dict]:
    from core import tbx_mgmt as tbm

    actions: list[dict] = []
    ok = failed = refused = 0
    for prop in proposals:
        sid = prop["demand_source_id"]
        try:
            result = tbm.set_demand_geo_bid_floors(
                demand_source_id=sid,
                floors_by_country_id=prop["floors_by_country_id"],
                replace=False,          # never discard a country we did not price
                actor="tbx_demand_geo_floor",
                reason=(f"{len(prop['picks'])} geo floors at "
                        f"{FLOOR_PCT:.0%} of {SCORING_WINDOW_DAYS}d observed eCPM"),
                dry_run=dry_run,
                demand_name=prop["demand_name"],
            )
        except Exception as exc:                      # noqa: BLE001
            failed += 1
            actions.append({"demand_source_id": sid, "applied": False,
                            "error": f"{type(exc).__name__}: {exc}",
                            "timestamp": datetime.now(timezone.utc).isoformat()})
            continue

        if result.get("refused"):
            refused += 1
        elif result.get("applied"):
            ok += 1
        actions.append({
            "demand_source_id": sid,
            "demand_name": prop["demand_name"],
            "picks": prop["picks"],
            "dry_run": dry_run,
            "applied": bool(result.get("applied")),
            "refused": result.get("refused"),
            "clamps": result.get("clamps") or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    print(f"{_LOG} {'dry-run' if dry_run else 'applied'}: {ok} written, "
          f"{refused} refused, {failed} failed")
    return actions


# ─── Output ──────────────────────────────────────────────────────────────────


def slack_summary(proposals: list[dict], skips: list[str], applied: bool) -> str:
    tag = "🟢 APPLIED" if applied else "🔍 PROPOSED"
    if not proposals:
        head = f"🌍 *TBX demand geo floors* — nothing actionable this run."
        return head + (f"\n  _{len(skips)} skip(s); see the run log._" if skips else "")

    lines = [f"🌍 *TBX demand geo floors* {tag} — {len(proposals)} DSP(s)"]
    for prop in proposals[:8]:
        bumps = ", ".join(
            f"{p['country']} ${p['current']:.2f}→${p['proposed']:.2f}"
            for p in prop["picks"][:4]
        )
        lines.append(f"  • [{prop['demand_source_id']}] {prop['demand_name'][:32]}  {bumps}")
    if len(proposals) > 8:
        lines.append(f"  … +{len(proposals) - 8} more")
    if skips:
        lines.append(f"  _{len(skips)} skip(s) — is_smart_floor, freezes, "
                     f"or unresolved names; see the run log._")
    if not applied:
        lines.append("  _Propose-only. `--apply` with TBX_ALLOW_WRITES=1 and "
                     "PGAM_OPTIMIZER_AUTO_APPLY=1 to write._")
    return "\n".join(lines)


def _append_actions(actions: list[dict]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    prior: list = []
    if ACTIONS_LOG.exists():
        try:
            prior = json.loads(ACTIONS_LOG.read_text())
        except (OSError, json.JSONDecodeError):
            prior = []
    prior.extend(actions)
    ACTIONS_LOG.write_text(json.dumps(prior, indent=2))


# ─── Entry point ─────────────────────────────────────────────────────────────


def run(dry_run: bool = True, days: int = SCORING_WINDOW_DAYS) -> dict:
    from core import tbx_api as tbx
    from core import tbx_mgmt as tbm

    if not tbx.configured():
        missing = [k for k in ("TBX_EMAIL", "TBX_PASSWORD") if not os.getenv(k)]
        print(f"{_LOG} not configured — missing {', '.join(missing)}. Nothing proposed.")
        print(f"{_LOG}   Set them in the Render dashboard (Environment) on the "
              f"pgam-intelligence-scheduler worker. These are the "
              f"ssp-new.pgammedia.com login, not the legacy TB_* one.")
        return {"ok": True, "skipped": "not configured"}

    # A write run needs all three gates. Checked before the report rather than
    # after, so a misconfigured "--apply" costs nothing and says why.
    if not dry_run:
        if not tbm.writes_enabled():
            print(f"{_LOG} --apply given but TBX_ALLOW_WRITES is not 1. "
                  f"Falling back to propose-only.")
            dry_run = True
        elif not auto_apply_enabled():
            print(f"{_LOG} --apply given but PGAM_OPTIMIZER_AUTO_APPLY is not 1. "
                  f"Falling back to propose-only — that gate is the fleet's, "
                  f"and it is off deliberately.")
            dry_run = True

    start, end = _window(days)
    print(f"{_LOG} demand_source × country  {start} → {end}  "
          f"({'propose' if dry_run else 'LIVE'})")

    rows = demand_country_rows(start, end)
    by_name, by_id = demand_source_index()
    print(f"{_LOG} {len(rows)} report row(s) | {len(by_id)} demand source(s)")

    country_id_by_code: dict[str, int] = {}
    for row in tbx.dictionary("countries"):
        cid = row.get("id")
        if cid is None:
            continue
        for field in ("name", "code", "alpha2", "alpha3", "iso", "iso2", "iso3"):
            val = row.get(field)
            if isinstance(val, str) and val:
                country_id_by_code[val.strip().lower()] = int(cid)

    proposals, skips = build_proposals(rows, by_name, by_id, country_id_by_code)
    print(f"{_LOG} {len(proposals)} DSP(s) with actionable geo floors, "
          f"{len(skips)} skip(s)")

    for prop in proposals[:10]:
        print(f"\n  [{prop['demand_source_id']}] {prop['demand_name'][:45]}  "
              f"(${prop['spend_in_scope']:,.2f} in scope)")
        for p in prop["picks"]:
            clamp = f"  [{'; '.join(p['clamps'])}]" if p["clamps"] else ""
            print(f"      {p['country']:<4} eCPM=${p['observed_ecpm']:>6.2f}  "
                  f"imps={p['imps']:>9,}  "
                  f"floor ${p['current']:>5.2f} → ${p['proposed']:>5.2f}{clamp}")
    for skip in skips[:15]:
        print(f"{_LOG} skip: {skip}")

    actions = apply_proposals(proposals, dry_run=dry_run)
    if actions:
        _append_actions(actions)

    recs = {
        "window": {"start": start, "end": end},
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "floor_pct": FLOOR_PCT,
        "proposals": proposals,
        "skips": skips,
        "actions": actions,
    }
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RECS_FILE.write_text(json.dumps(recs, indent=2, default=str))
    print(f"{_LOG} recs → {RECS_FILE}")

    try:
        from core.slack import send_text
        send_text(slack_summary(proposals, skips, applied=not dry_run))
    except Exception as exc:                          # noqa: BLE001
        print(f"{_LOG} Slack post skipped: {type(exc).__name__}: {exc}")

    recs["ok"] = True
    return recs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Propose (or write) per-DSP × country bid floors on TBX")
    parser.add_argument("--apply", action="store_true",
                        help="write the floors; also needs TBX_ALLOW_WRITES=1 "
                             "and PGAM_OPTIMIZER_AUTO_APPLY=1")
    parser.add_argument("--days", type=int, default=SCORING_WINDOW_DAYS,
                        help=f"scoring window (default {SCORING_WINDOW_DAYS})")
    args = parser.parse_args()
    outcome = run(dry_run=not args.apply, days=args.days)
    sys.exit(0 if outcome.get("ok") else 1)
