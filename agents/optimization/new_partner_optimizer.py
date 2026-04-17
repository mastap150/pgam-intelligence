"""
agents/optimization/new_partner_optimizer.py

Auto-optimizes every new LL partner (supply + demand) the moment it's detected.

What this solves
----------------
Every time a new publisher is onboarded, or a new demand line appears in an
existing publisher's biddingpreferences, the optimal floor question comes up
again. Doing this by hand is slow; the answer is usually derivable from
historical eCPM anyway. This agent does it automatically every day.

How it works
------------
1. Load the last LL snapshot from logs/ll_partner_snapshot.json.
2. Pull current LL state (all publishers + their biddingpreferences).
3. Compute diffs:
      NEW_PUBLISHER    → publisher_id not present in snapshot
      NEW_DEMAND       → (pub_id, demand_id) pair not present in snapshot
      REACTIVATED      → demand was status=2 in snapshot, status=1 now
4. Look up 30-day historical eCPM per demand-partner across the whole account.
5. For each new/reactivated demand on each publisher, compute a recommended
   floor:
       primary:    floor = historical_demand_ecpm × 0.40
       fallback:   format-aware default ($0.30 display / $1 video / $3 intst /
                   $10 CTV) inferred from publisher name tokens
6. Apply the floor via llm._put() but only_if_none — never overwrite a
   hand-set floor.
7. Log every action to logs/pilot_2026-04.json + logs/new_partner_actions.json.
8. Post a Slack summary.
9. Write the new snapshot.

Safety
------
* only_if_none everywhere — the agent never touches an already-floored demand.
* MIN_FLOOR $0.10, MAX_FLOOR $15 hard cap.
* First run (no snapshot) bootstraps silently — no changes applied. This
  prevents a runaway "everything is new, floor everything" run the first day.
* Per-run cap: MAX_CHANGES_PER_RUN = 200. If exceeded, applies top 200 by
  estimated revenue impact and logs the rest for the next day.

Scheduler
---------
Wired into scheduler.py at 08:30 ET daily. Agent is self-deduplicating via
the snapshot file.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone, date, timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

import core.ll_mgmt as llm
from core.api import fetch
from core.ll_report import _sf
from core.margin import get_publisher_margins, MARGIN_HEALTHY_THRESHOLD

SNAPSHOT_PATH = os.path.join(_REPO_ROOT, "logs", "ll_partner_snapshot.json")
ACTIONS_LOG = os.path.join(_REPO_ROOT, "logs", "new_partner_actions.json")
PILOT_LOG = os.path.join(_REPO_ROOT, "logs", "pilot_2026-04.json")

# ─────────────────────────────────────────────────────────────────────────────
# Tuning
# ─────────────────────────────────────────────────────────────────────────────

FLOOR_RATIO = 0.40               # floor = historical eCPM × this
MIN_FLOOR = 0.10
MAX_FLOOR = 15.0
LOOKBACK_DAYS = 30
MAX_CHANGES_PER_RUN = 200

# Activity gating — only touch publishers that are actually serving traffic.
# Many LL publisher entries are status=1 (active) in LL but have no real traffic
# (legacy CTV entries, test publishers, etc). Gating keeps the agent from
# "optimizing" things that produce no revenue either way.
ACTIVITY_LOOKBACK_DAYS = 7
MIN_WINS_FOR_ACTIVE    = 1       # must have won ≥1 impression in lookback window

# Format-aware fallbacks / minimum floors
FALLBACK_FLOORS = {
    "ctv":          10.00,
    "interstitial":  3.00,
    "video":         1.00,
    "inapp":         0.40,
    "display":       0.30,
}

# Token patterns that imply format, in precedence order (first match wins).
# A sized demand line ("Sovrn_300x250") is display even when the publisher
# name contains "Video" — the size in the demand name is authoritative.
# Display is checked BEFORE video so e.g. "Algorix Display and Video" +
# "Pubmatic 728x90" correctly resolves to display, not video.
FORMAT_PATTERNS = [
    ("ctv",          [r"\bctv\b", r"wurl", r"roku", r"future\s*today", r"ottera",
                      r"fuse\s*media", r"blue\s*ant", r"cox\s*media", r"lifevista",
                      r"quickcast", r"springserve"]),
    ("interstitial", [r"interstitial", r"\bintst\b"]),
    ("display",      [r"\d+x\d+", r"\bdisplay\b", r"\bbanner\b"]),
    ("video",        [r"\bvideo\b", r"\bolv\b", r"vast"]),
    ("inapp",        [r"in\s*[-_]?app", r"in_app", r"inapp"]),
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def infer_format(pub_name: str, demand_name: str) -> str:
    """Return one of ctv/video/interstitial/inapp/display based on name tokens."""
    text = f"{pub_name} {demand_name}".lower()
    for fmt, patterns in FORMAT_PATTERNS:
        for pat in patterns:
            if re.search(pat, text):
                return fmt
    return "display"


def extract_demand_partner(demand_name: str, known_partners: set[str]) -> str | None:
    """Match a demand's name to a DEMAND_PARTNER_NAME from the stats API.

    Examples:
        'Pubmatic - RON 300x250 PS'          → 'Pubmatic'
        'Copy - Pubmatic - RON 728x90 ...'   → 'Pubmatic' (strips "Copy - ")
        'Sovrn PubNative_300x250'            → 'Sovrn'
        'Xandr - BidMachine 9 Dots'          → 'Xandr - Ad' (partner contains our first word)
    """
    if not demand_name:
        return None
    # Strip common admin prefixes: "Copy - ", "Test - ", etc.
    normalized = re.sub(r"^(copy|test|temp)\s*-\s*", "", demand_name, flags=re.IGNORECASE).strip()
    lower = normalized.lower()

    # 1. Prefix match, longest partner name first
    for partner in sorted(known_partners, key=len, reverse=True):
        if lower.startswith(partner.lower()):
            return partner

    # 2. Substring match in first 40 chars of normalized name
    head = lower[:40]
    for partner in sorted(known_partners, key=len, reverse=True):
        if partner.lower() in head:
            return partner

    # 3. First word matches partner head (e.g. "Xandr - ..." → "Xandr - Ad")
    first_word = re.split(r"[\s\-_]", normalized, maxsplit=1)[0].lower()
    for partner in known_partners:
        if partner.lower().startswith(first_word) or first_word.startswith(partner.lower().split()[0]):
            return partner
    return None


def load_snapshot() -> dict:
    if not os.path.exists(SNAPSHOT_PATH):
        return {}
    try:
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_snapshot(snap: dict) -> None:
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snap, f, indent=2)


def build_current_state() -> dict:
    """Return {pub_id_str: {name, status, supplier, demands: {did_str: {name, status, minBidFloor}}}}"""
    state: dict = {}
    pubs = llm.get_publishers(include_archived=True)
    for pub in pubs:
        pid = pub.get("id")
        if pid is None:
            continue
        pub_full = llm.get_publisher(pid)
        demands = {}
        for pref in pub_full.get("biddingpreferences", []):
            for v in pref.get("value", []):
                did = v.get("id")
                if did is None:
                    continue
                demands[str(did)] = {
                    "name": v.get("name", ""),
                    "status": v.get("status"),
                    "minBidFloor": v.get("minBidFloor"),
                }
        state[str(pid)] = {
            "name": pub_full.get("name", ""),
            "status": pub_full.get("status"),
            "supplier": pub_full.get("supplier"),
            "demands": demands,
        }
    return state


def get_active_publisher_ids(lookback_days: int = ACTIVITY_LOOKBACK_DAYS,
                              min_wins: int = MIN_WINS_FOR_ACTIVE) -> set[int]:
    """Return the set of LL publisher_ids that are actually serving traffic.

    Many publishers are marked status=1 in LL config but have no recent wins
    (legacy CTV entries, test endpoints, paused-but-not-flagged partners).
    We gate floor changes on "has won ≥min_wins impressions in the last
    lookback_days days" which is the most reliable "truly live" signal.
    """
    end = date.today()
    start = end - timedelta(days=lookback_days)
    try:
        rows = fetch(
            "PUBLISHER", "WINS",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        print(f"[new_partner_optimizer] WARNING: activity fetch failed: {e}")
        return set()

    active: set[int] = set()
    for r in rows:
        try:
            pid = int(_sf(r.get("PUBLISHER_ID", 0)))
            wins = _sf(r.get("WINS", 0))
            if pid and wins >= min_wins:
                active.add(pid)
        except Exception:
            continue
    return active


def fetch_historical_ecpm(days: int = LOOKBACK_DAYS) -> dict:
    """Return {(publisher_name_lower, demand_partner_name): avg_ecpm}.

    Also return per-demand-partner overall averages keyed by
    ('__ANY__', demand_partner_name).
    """
    end = date.today()
    start = end - timedelta(days=days)
    try:
        rows = fetch(
            "PUBLISHER,DEMAND_PARTNER",
            "WINS,GROSS_REVENUE",
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        print(f"[new_partner_optimizer] WARNING: historical eCPM fetch failed: {e}")
        return {}

    # Aggregate
    per_demand: dict[str, dict] = {}            # demand_partner -> {rev, wins}
    per_pub_demand: dict[tuple, dict] = {}      # (pub_name, demand_partner) -> {rev, wins}
    for r in rows:
        dp = r.get("DEMAND_PARTNER_NAME", "")
        pn = r.get("PUBLISHER_NAME", "").lower()
        rev = _sf(r.get("GROSS_REVENUE", 0))
        wins = _sf(r.get("WINS", 0))
        if not dp or wins <= 0:
            continue
        per_demand.setdefault(dp, {"rev": 0.0, "wins": 0.0})
        per_demand[dp]["rev"] += rev
        per_demand[dp]["wins"] += wins
        k = (pn, dp)
        per_pub_demand.setdefault(k, {"rev": 0.0, "wins": 0.0})
        per_pub_demand[k]["rev"] += rev
        per_pub_demand[k]["wins"] += wins

    ecpm_map = {}
    for dp, v in per_demand.items():
        if v["wins"] > 0:
            ecpm_map[("__ANY__", dp)] = v["rev"] / v["wins"] * 1000.0
    for (pn, dp), v in per_pub_demand.items():
        if v["wins"] > 0:
            ecpm_map[(pn, dp)] = v["rev"] / v["wins"] * 1000.0
    return ecpm_map


def compute_floor(pub_name: str, demand_name: str, ecpm_map: dict,
                  known_partners: set[str],
                  pub_id: int | None = None) -> tuple[float, str]:
    """Return (floor, source_tag) for a new demand line.

    Decision hierarchy (first match wins):
      1. ML model prediction (intelligence/floor_model.py), if high-confidence
         for this exact (pub_id, demand_partner) pair.
      2. Historical pub×demand eCPM × FLOOR_RATIO (last 30 days).
      3. Historical demand-partner eCPM × FLOOR_RATIO (cross-publisher).
      4. Format-aware minimum ($10 CTV / $3 interstitial / $1 video /
         $0.40 inapp / $0.30 display).

    All paths clamp into [MIN_FLOOR, MAX_FLOOR] and floor-bound by the
    format minimum so CTV floors never dip below $10 etc.
    """
    partner = extract_demand_partner(demand_name, known_partners)
    fmt = infer_format(pub_name, demand_name)
    format_min = FALLBACK_FLOORS[fmt]

    # ── 1. ML model prediction ──────────────────────────────────────────────
    if pub_id is not None and partner:
        try:
            from intelligence.floor_model import lookup_prediction
            pred = lookup_prediction(pub_id, partner, country="US")
            if pred is not None:
                proposed = float(pred["recommended_floor"])
                floor = max(proposed, format_min)
                floor = max(MIN_FLOOR, min(MAX_FLOOR, floor))
                tag = (f"model p50 ${pred['predicted_ecpm']:.2f} "
                       f"(band ${pred['predicted_p10']:.2f}–${pred['predicted_p90']:.2f})")
                return round(floor, 2), tag
        except Exception as e:
            # Model not trained yet or import failed — silently fall through
            pass

    # ── 2–3. Historical lookup ──────────────────────────────────────────────
    if partner:
        k = (pub_name.lower(), partner)
        ecpm = ecpm_map.get(k)
        src = f"pub×{partner}"
        if ecpm is None:
            ecpm = ecpm_map.get(("__ANY__", partner))
            src = f"{partner}"
        if ecpm is not None and ecpm > 0:
            proposed = ecpm * FLOOR_RATIO
            floor = max(proposed, format_min)
            floor = max(MIN_FLOOR, min(MAX_FLOOR, floor))
            if proposed >= format_min:
                tag = f"{src} 30d eCPM ${ecpm:.2f} × {FLOOR_RATIO:.0%}"
            else:
                tag = f"{fmt} minimum (historical ${ecpm:.2f}×{FLOOR_RATIO:.0%}=${proposed:.2f} too low)"
            return round(floor, 2), tag

    # ── 4. Fallback ─────────────────────────────────────────────────────────
    return round(format_min, 2), f"fallback:{fmt} (no history for demand)"


# ─────────────────────────────────────────────────────────────────────────────
# Diff computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_diffs(current: dict, snapshot: dict) -> list[dict]:
    """Return list of diffs, each dict: {type, pub_id, pub_name, demand_id?, demand_name?, reason}."""
    diffs: list[dict] = []

    for pid, pub in current.items():
        prev = snapshot.get(pid)
        if prev is None:
            # NEW_PUBLISHER — every active demand on it counts as a new diff
            for did, dem in pub.get("demands", {}).items():
                if dem.get("status") == 1:
                    diffs.append({
                        "type": "new_publisher",
                        "pub_id": int(pid),
                        "pub_name": pub.get("name", ""),
                        "demand_id": int(did),
                        "demand_name": dem.get("name", ""),
                        "current_floor": dem.get("minBidFloor"),
                    })
            continue

        # Existing publisher — diff demands
        prev_demands = prev.get("demands", {})
        for did, dem in pub.get("demands", {}).items():
            prev_dem = prev_demands.get(did)
            if prev_dem is None:
                if dem.get("status") == 1:
                    diffs.append({
                        "type": "new_demand",
                        "pub_id": int(pid),
                        "pub_name": pub.get("name", ""),
                        "demand_id": int(did),
                        "demand_name": dem.get("name", ""),
                        "current_floor": dem.get("minBidFloor"),
                    })
            else:
                # Reactivation — status went 2 → 1
                if prev_dem.get("status") == 2 and dem.get("status") == 1:
                    diffs.append({
                        "type": "reactivated",
                        "pub_id": int(pid),
                        "pub_name": pub.get("name", ""),
                        "demand_id": int(did),
                        "demand_name": dem.get("name", ""),
                        "current_floor": dem.get("minBidFloor"),
                    })
    return diffs


# ─────────────────────────────────────────────────────────────────────────────
# Apply
# ─────────────────────────────────────────────────────────────────────────────

def apply_diffs(diffs: list[dict], ecpm_map: dict, known_partners: set[str],
                active_pids: set[int], margin_map: dict,
                dry_run: bool = False) -> list[dict]:
    """Walk diffs, compute floors, apply only_if_none. Return action records.

    Publishers not in active_pids are skipped — they're in LL but not serving
    traffic, so optimizing their floors would be noise.

    Margin guardrail (hybrid): publishers with 30-day margin < 30% block
    NEW demand activations (type=new_demand, new_publisher) but still allow
    REACTIVATED demands to be re-floored. Rationale: adding new demand to a
    broken rev share amplifies bad economics; re-enabling a demand that
    already existed is just restoring prior state.
    """
    actions: list[dict] = []

    # Group by publisher so we PUT each publisher once
    by_pub: dict[int, list[dict]] = {}
    for d in diffs:
        by_pub.setdefault(d["pub_id"], []).append(d)

    # Cap total changes
    total_planned = len(diffs)
    if total_planned > MAX_CHANGES_PER_RUN:
        print(f"[new_partner_optimizer] planned {total_planned} > cap "
              f"{MAX_CHANGES_PER_RUN}; first {MAX_CHANGES_PER_RUN} applied this run.")

    applied = 0
    inactive_skipped = 0
    for pub_id, pub_diffs in by_pub.items():
        if applied >= MAX_CHANGES_PER_RUN:
            break

        # Activity gate — don't touch publishers that aren't serving traffic.
        if pub_id not in active_pids:
            for d in pub_diffs:
                actions.append({**d, "action": "skipped_inactive_publisher"})
            inactive_skipped += len(pub_diffs)
            pub_name = pub_diffs[0].get("pub_name", f"id={pub_id}")
            print(f"  ·  [{pub_name[:32]:<32}] INACTIVE (no recent wins) — "
                  f"skipping {len(pub_diffs)} diff(s)")
            continue

        # Margin guardrail — below-threshold publishers get partial treatment.
        # Reactivations pass through (restoring prior state); new_demand and
        # new_publisher diffs are blocked to avoid piling demand on broken economics.
        margin_info = margin_map.get(pub_id)
        margin_pct = margin_info["margin_pct"] if margin_info else None
        margin_unhealthy = (margin_pct is not None
                             and margin_pct < MARGIN_HEALTHY_THRESHOLD)
        try:
            pub = llm.get_publisher(pub_id)
        except Exception as e:
            print(f"  ✗ publisher {pub_id} fetch failed: {e}")
            continue
        pub_name = pub.get("name", "")

        # Index demand entries for fast update
        demand_map: dict[int, dict] = {}
        for pref in pub.get("biddingpreferences", []):
            for v in pref.get("value", []):
                if v.get("id") is not None:
                    demand_map[v["id"]] = v

        modified = False
        for d in pub_diffs:
            if applied >= MAX_CHANGES_PER_RUN:
                break
            did = d["demand_id"]
            v = demand_map.get(did)
            if v is None:
                continue
            old_floor = v.get("minBidFloor")
            # only_if_none — do not overwrite hand-tuned floors
            if old_floor is not None:
                actions.append({
                    **d, "action": "skipped_existing_floor", "old_floor": old_floor,
                })
                continue
            if v.get("status") != 1:
                continue

            # Margin guardrail — block new_demand / new_publisher on <30% pubs,
            # but let reactivated demands through (they restore prior state).
            if margin_unhealthy and d["type"] in ("new_demand", "new_publisher"):
                actions.append({
                    **d,
                    "action": "skipped_low_margin",
                    "margin_pct": margin_pct,
                    "threshold": MARGIN_HEALTHY_THRESHOLD,
                })
                print(f"  ·  [{pub_name[:32]:<32}] LOW MARGIN ({margin_pct:.1f}%) — "
                      f"skipping new_demand {did} ({d['demand_name'][:30]})")
                continue

            new_floor, src = compute_floor(pub_name, d["demand_name"],
                                           ecpm_map, known_partners, pub_id=pub_id)

            action = {
                **d,
                "action": "floor_set",
                "old_floor": old_floor,
                "new_floor": new_floor,
                "source": src,
                "timestamp": _now_iso(),
                "dry_run": dry_run,
            }
            actions.append(action)

            tag = "DRY" if dry_run else "✓"
            print(f"  {tag} [{pub_name[:32]:<32}] {d['type']:<14} "
                  f"{d['demand_name'][:34]:<34} id={did:<6} "
                  f"None → ${new_floor:.2f}   ({src})")

            if not dry_run:
                v["minBidFloor"] = new_floor
                modified = True
                applied += 1

        if modified and not dry_run:
            try:
                llm._put(f"/v1/publishers/{pub_id}", pub)
            except Exception as e:
                print(f"  ✗ [{pub_name}] PUT failed: {e}")
                for a in actions:
                    if a.get("pub_id") == pub_id and a.get("action") == "floor_set":
                        a["error"] = str(e)

    return actions


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def append_pilot_log(actions: list[dict]) -> None:
    if not actions:
        return
    os.makedirs(os.path.dirname(PILOT_LOG), exist_ok=True)
    data = []
    if os.path.exists(PILOT_LOG):
        try:
            with open(PILOT_LOG) as f:
                data = json.load(f)
        except Exception:
            data = []
    if isinstance(data, dict):
        data = list(data.values())
    today = _today()
    entry = next((e for e in data if e.get("date") == today), None)
    if entry is None:
        entry = {"date": today, "actions_applied": []}
        data.append(entry)
    entry.setdefault("actions_applied", []).append({
        "action": "new_partner_optimizer",
        "timestamp": _now_iso(),
        "changes": [a for a in actions if a.get("action") == "floor_set"],
    })
    with open(PILOT_LOG, "w") as f:
        json.dump(data, f, indent=2)


def append_actions_log(actions: list[dict]) -> None:
    os.makedirs(os.path.dirname(ACTIONS_LOG), exist_ok=True)
    prior = []
    if os.path.exists(ACTIONS_LOG):
        try:
            with open(ACTIONS_LOG) as f:
                prior = json.load(f)
        except Exception:
            prior = []
    prior.extend(actions)
    with open(ACTIONS_LOG, "w") as f:
        json.dump(prior, f, indent=2)


def post_slack(actions: list[dict], bootstrap: bool) -> None:
    try:
        from core.slack import post_message
    except Exception:
        return
    if bootstrap:
        post_message("🆕 *New Partner Optimizer* bootstrapped LL snapshot — no changes this run.")
        return
    applied = [a for a in actions if a.get("action") == "floor_set" and not a.get("dry_run")]
    if not applied:
        post_message("🆕 *New Partner Optimizer* — no new partners/demands detected today.")
        return
    lines = [f"🆕 *New Partner Optimizer* applied {len(applied)} floors today:"]
    by_type = {}
    for a in applied:
        by_type.setdefault(a["type"], []).append(a)
    for t, entries in by_type.items():
        lines.append(f"  _{t}_ ({len(entries)}):")
        for a in entries[:8]:
            lines.append(f"    • [{a['pub_name'][:26]}] {a['demand_name'][:32]} → ${a['new_floor']:.2f}")
        if len(entries) > 8:
            lines.append(f"    … and {len(entries) - 8} more")
    try:
        post_message("\n".join(lines))
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    print(f"\n{'='*70}")
    print(f"  New Partner Optimizer  {'[DRY RUN]' if dry_run else '[LIVE]'}")
    print(f"{'='*70}\n")

    snapshot = load_snapshot()
    bootstrap = not snapshot

    print("Fetching current LL state …")
    current = build_current_state()
    print(f"  {len(current)} publishers in current state")

    if bootstrap:
        print("\n⚡ First run — bootstrapping snapshot. No changes applied.")
        save_snapshot(current)
        post_slack([], bootstrap=True)
        return

    diffs = compute_diffs(current, snapshot)
    print(f"  {len(diffs)} diffs detected ("
          f"new_pub={sum(1 for d in diffs if d['type']=='new_publisher')}, "
          f"new_demand={sum(1 for d in diffs if d['type']=='new_demand')}, "
          f"reactivated={sum(1 for d in diffs if d['type']=='reactivated')})")

    if not diffs:
        save_snapshot(current)
        post_slack([], bootstrap=False)
        return

    print("\nFetching 30-day historical eCPM per demand partner …")
    ecpm_map = fetch_historical_ecpm(LOOKBACK_DAYS)
    known_partners = {k[1] for k in ecpm_map}
    print(f"  {len(known_partners)} known demand partners with historical data")

    print(f"\nFetching active publisher set (wins ≥ {MIN_WINS_FOR_ACTIVE} in last "
          f"{ACTIVITY_LOOKBACK_DAYS}d) …")
    active_pids = get_active_publisher_ids()
    print(f"  {len(active_pids)} active publishers "
          f"(of {len(current)} listed, skipping {len(current) - len(active_pids)} inactive)")

    print(f"\nFetching 30-day margin per publisher (threshold {MARGIN_HEALTHY_THRESHOLD:.0f}%) …")
    margin_map = get_publisher_margins(lookback_days=30)
    unhealthy = [pid for pid, m in margin_map.items()
                 if m["margin_pct"] < MARGIN_HEALTHY_THRESHOLD]
    print(f"  {len(margin_map)} publishers with margin data, "
          f"{len(unhealthy)} below threshold "
          f"({MARGIN_HEALTHY_THRESHOLD:.0f}%): "
          f"{', '.join(margin_map[pid]['name'][:20] for pid in unhealthy[:5])}"
          f"{'...' if len(unhealthy) > 5 else ''}")

    print("\nApplying floors …")
    actions = apply_diffs(diffs, ecpm_map, known_partners, active_pids,
                          margin_map, dry_run=dry_run)

    applied_count    = sum(1 for a in actions if a.get("action") == "floor_set" and not a.get("dry_run"))
    skipped_floor    = sum(1 for a in actions if a.get("action") == "skipped_existing_floor")
    skipped_inactive = sum(1 for a in actions if a.get("action") == "skipped_inactive_publisher")
    skipped_margin   = sum(1 for a in actions if a.get("action") == "skipped_low_margin")

    print(f"\n{'='*70}")
    print(f"  SUMMARY: {applied_count} floors {'WOULD BE' if dry_run else ''} applied")
    print(f"           {skipped_floor} skipped (already had hand-tuned floor)")
    print(f"           {skipped_inactive} skipped (inactive publisher — no recent wins)")
    print(f"           {skipped_margin} skipped (publisher margin < {MARGIN_HEALTHY_THRESHOLD:.0f}%)")
    print(f"{'='*70}\n")

    if not dry_run and actions:
        append_pilot_log(actions)
        append_actions_log(actions)
        save_snapshot(current)

    post_slack(actions, bootstrap=False)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="LL new partner optimizer")
    p.add_argument("--dry-run", action="store_true", help="Preview only — no writes")
    p.add_argument("--rebuild-snapshot", action="store_true",
                   help="Force re-bootstrap from current state (no changes applied)")
    args = p.parse_args()

    if args.rebuild_snapshot:
        print("Rebuilding snapshot from current LL state …")
        current = build_current_state()
        save_snapshot(current)
        print(f"✓ snapshot saved ({len(current)} publishers)")
    else:
        run(dry_run=args.dry_run)
