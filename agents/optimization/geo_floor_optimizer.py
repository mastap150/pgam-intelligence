"""
agents/optimization/geo_floor_optimizer.py

Per-placement × country floor optimizer.

Idea
----
Flat floors leave revenue on the table. A placement that averages $0.40
eCPM overall might do $3.00 in the US and $0.05 in SEA. A flat $0.30
floor lets in the low-value traffic AND lets US bidders steal below
their willingness-to-pay. The fix is geo-specific floors:
  - US/UK/CA/AU/DE floor = FLOOR_PCT × observed_country_ecpm
  - Everywhere else = existing global floor (unchanged)

What it does
------------
1. Pulls placement × country eCPM from the TB report endpoint.
2. For each placement, computes per-country floor = observed eCPM × FLOOR_PCT.
3. Only writes floors for countries meeting quality bars:
      imps ≥ MIN_IMP_PER_COUNTRY
      ecpm ≥ MIN_COUNTRY_ECPM
      AND proposed_floor > current flat floor
4. Respects:
      FLOOR_MIN / FLOOR_MAX hard caps
      MAX_COUNTRIES_PER_PLACEMENT (keep the list tight)
      MAX_FLOOR_DELTA_PCT (never >X% change in one step)

Targets premium geos list (GEO_ALLOWLIST) first. Defaults to top 5 EN-
speaking markets.

Safety
------
- Dry-run default. --apply executes.
- Per-run caps: MAX_PLACEMENTS_PER_RUN, MAX_COUNTRIES_PER_PLACEMENT.
- Preserves the global 'price' field — only edits price_country.
- Audit log + Slack summary.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.parse
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(override=True)

import core.tb_mgmt as tbm

# ─── Tunables ────────────────────────────────────────────────────────────────

SCORING_WINDOW_DAYS       = 14
GEO_ALLOWLIST             = ["USA", "GBR", "CAN", "AUS", "DEU", "FRA", "JPN"]
FLOOR_PCT                 = 0.70     # floor = eCPM * 0.70
FLOOR_MIN                 = 0.10     # never floor below $0.10
FLOOR_MAX                 = 10.00    # TB hard cap
MIN_IMP_PER_COUNTRY       = 500
MIN_COUNTRY_ECPM          = 0.30     # don't set a floor if avg eCPM is trash
MAX_FLOOR_DELTA_PCT       = 0.50     # no single-step jump > +50% of prior
MAX_COUNTRIES_PER_PLCMNT  = 10
MAX_PLACEMENTS_PER_RUN    = 50

TB_BASE = "https://ssp.pgammedia.com/api"

LOG_DIR       = os.path.join(_REPO_ROOT, "logs")
ACTIONS_LOG   = os.path.join(LOG_DIR, "geo_floor_actions.json")
RECS_FILE     = os.path.join(LOG_DIR, "geo_floor_recs.json")
os.makedirs(LOG_DIR, exist_ok=True)


# ─── Data pull ───────────────────────────────────────────────────────────────

def _window() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=SCORING_WINDOW_DAYS)
    return start.isoformat(), end.isoformat()


def placement_country_report(start: str, end: str) -> list[dict]:
    """Pull placement × country stats."""
    token = tbm._get_token()
    params = [
        ("from", start), ("to", end),
        ("day_group", "total"),
        ("limit", 1000),
        ("attribute[]", "placement"),
        ("attribute[]", "country"),
    ]
    url = f"{TB_BASE}/{token}/report?" + urllib.parse.urlencode(params)
    resp = requests.get(url, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"report failed: {resp.status_code} {resp.text[:200]}")
    data = resp.json()
    return data.get("data", data) if isinstance(data, dict) else data


# ─── Proposal builder ────────────────────────────────────────────────────────

def build_proposals(
    rows: list[dict],
    placements: list[dict],
) -> list[dict]:
    """
    For each placement, build a list of country floor proposals.
    Returns [{placement_id, type, current_floor, current_geo, proposals:[{code,price,reason}]}].
    """
    # Index placements by id
    pmap = {p["placement_id"]: p for p in placements}

    # Group rows by placement_id
    by_pid: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        pid = r.get("placement_id")
        if pid is None:
            raw = r.get("placement", "")
            if "#" in raw:
                try: pid = int(raw.rsplit("#", 1)[1].strip())
                except ValueError: pass
        if pid is None:
            continue
        by_pid[int(pid)].append(r)

    proposals = []
    for pid, country_rows in by_pid.items():
        p = pmap.get(pid)
        if not p:
            continue
        current_floor = float(p.get("price", 0.0) or 0.0)
        current_geo   = p.get("price_country") or []
        current_geo_map = {g["code"]: float(g["price"]) for g in current_geo if "code" in g}

        picks = []
        for cr in country_rows:
            code = cr.get("country")
            if not code or code not in GEO_ALLOWLIST:
                continue
            imps = cr.get("impressions", 0) or 0
            if imps < MIN_IMP_PER_COUNTRY:
                continue
            spend = cr.get("dsp_spend", 0.0) or 0.0
            ecpm  = (spend * 1000.0 / imps) if imps else 0.0
            if ecpm < MIN_COUNTRY_ECPM:
                continue
            proposed = ecpm * FLOOR_PCT
            proposed = max(FLOOR_MIN, min(FLOOR_MAX, proposed))

            # Clamp by MAX_FLOOR_DELTA_PCT over prior setting
            prior = current_geo_map.get(code, current_floor)
            if prior > 0:
                max_allowed = prior * (1.0 + MAX_FLOOR_DELTA_PCT)
                proposed = min(proposed, max_allowed)

            # Only propose if meaningfully above prior (≥5% bump)
            if proposed <= prior * 1.05:
                continue

            picks.append({
                "code":      code,
                "price":     round(proposed, 2),
                "prior":     round(prior, 2),
                "ecpm":      round(ecpm, 2),
                "imps":      imps,
            })

        # Keep top N countries by eCPM
        picks.sort(key=lambda x: -x["ecpm"])
        picks = picks[:MAX_COUNTRIES_PER_PLCMNT]

        if picks:
            # Merge: keep existing non-allowlist country floors, replace allowlist ones
            merged_map = dict(current_geo_map)
            for pick in picks:
                merged_map[pick["code"]] = pick["price"]
            merged_geo = [{"code": c, "price": v} for c, v in merged_map.items()]

            proposals.append({
                "placement_id":   pid,
                "title":          p.get("title"),
                "type":           p.get("type"),
                "inventory_id":   p.get("inventory_id"),
                "current_floor":  current_floor,
                "current_geo":    current_geo,
                "new_geo":        merged_geo,
                "country_bumps":  picks,
            })

    # Rank placements by total expected uplift (sum of imps × new_floor)
    proposals.sort(
        key=lambda x: -sum(b["imps"] * b["price"] for b in x["country_bumps"])
    )
    return proposals[:MAX_PLACEMENTS_PER_RUN]


# ─── Apply ───────────────────────────────────────────────────────────────────

def apply_proposals(props: list[dict], dry_run: bool) -> list[dict]:
    actions = []
    ok = fail = 0
    for prop in props:
        pid = prop["placement_id"]
        try:
            tbm.set_floor(
                placement_id=pid,
                price_country=prop["new_geo"],
                dry_run=dry_run,
            )
            ok += 1
            actions.append({
                "type":          "geo_floor_update",
                "placement_id":  pid,
                "title":         prop["title"],
                "country_bumps": prop["country_bumps"],
                "dry_run":       dry_run,
                "applied":       not dry_run,
                "timestamp":     datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            fail += 1
            actions.append({
                "type":         "geo_floor_update",
                "placement_id": pid,
                "applied":      False,
                "error":        str(e),
                "timestamp":    datetime.now(timezone.utc).isoformat(),
            })
    print(f"  {'dry-run' if dry_run else 'applied'}: {ok} placements "
          f"({fail} failed)")
    return actions


# ─── Slack ───────────────────────────────────────────────────────────────────

def post_slack(props: list[dict], applied: bool) -> None:
    try:
        from core.slack import post_message
    except Exception:
        return
    if not props:
        post_message("🌍 *Geo Floor Optimizer* — no actionable country bumps today.")
        return
    tag = "🟢 APPLIED" if applied else "🔍 DRY-RUN"
    lines = [f"🌍 *Geo Floor Optimizer* {tag} — {len(props)} placements"]
    for prop in props[:8]:
        bumps = ", ".join(
            f"{b['code']} ${b['prior']:.2f}→${b['price']:.2f}"
            for b in prop["country_bumps"][:4]
        )
        lines.append(f"  • [{prop['placement_id']}] {prop['title'][:35]}  {bumps}")
    if len(props) > 8:
        lines.append(f"  … +{len(props) - 8} more")
    try:
        post_message("\n".join(lines))
    except Exception:
        pass


def append_log(actions: list[dict]) -> None:
    prior = []
    if os.path.exists(ACTIONS_LOG):
        with open(ACTIONS_LOG) as f:
            try: prior = json.load(f)
            except Exception: prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)


# ─── Entry point ─────────────────────────────────────────────────────────────

def run(dry_run: bool = True) -> dict:
    print(f"\n{'='*72}")
    print(f"  Geo Floor Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"{'='*72}")

    start, end = _window()
    print(f"\n→ placement × country report  {start} → {end}")
    rows = placement_country_report(start, end)
    placements = tbm.list_placements()
    print(f"  {len(rows)} rows | {len(placements)} placements")

    proposals = build_proposals(rows, placements)
    print(f"\n  {len(proposals)} placements have actionable country bumps")

    for prop in proposals[:10]:
        print(f"\n  [{prop['placement_id']}] {prop['title'][:45]}  "
              f"(current ${prop['current_floor']:.2f})")
        for b in prop["country_bumps"]:
            print(
                f"      {b['code']}  eCPM=${b['ecpm']:>5.2f}  "
                f"imps={b['imps']:>7,}  "
                f"floor ${b['prior']:>4.2f} → ${b['price']:>4.2f}"
            )

    actions = apply_proposals(proposals, dry_run=dry_run)
    if actions:
        append_log(actions)

    recs = {
        "window":     {"start": start, "end": end},
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "dry_run":    dry_run,
        "proposals":  proposals,
        "actions":    actions,
    }
    with open(RECS_FILE, "w") as f:
        json.dump(recs, f, indent=2)
    print(f"\n  Recs → {RECS_FILE}")

    post_slack(proposals, applied=not dry_run)
    return recs


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    run(dry_run=not args.apply)
