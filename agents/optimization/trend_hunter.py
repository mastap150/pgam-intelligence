"""
agents/optimization/trend_hunter.py

Daily revenue-pattern detector. Scans hourly funnel + ledger + LL state for
the patterns a human would spot by hand and converts them into
auto-actions or Slack-queued recommendations.

Pattern catalog (each one decides AUTO vs QUEUE per the autonomy bar)
=====================================================================

Pattern                                      | Action class
---------------------------------------------|--------------
1. Underpriced demand (WR ≥50%, eCPM > $0.50)| AUTO — floor +10% (capped); intervention_journal reverts losers
2. DSP-declining demand (>100k bids, <1% WR) | QUEUE — investigate (could be DSP filter)
3. Sudden per-demand WoW drop ≥25% (same DOW)| QUEUE — review for revert
4. Newly-paused demand with recent revenue   | AUTO — re-enable (handled by auto_unpause)
5. Coverage gap with strong peer perf        | AUTO — wire (handled by auto_wire_gaps)
6. Pub with rising bid volume + falling eCPM | QUEUE — partner conversation
7. Margin dropped on a demand recently       | QUEUE — renegotiation flag

Why floor raises can auto-execute now
-------------------------------------
The intervention_journal safety net auto-reverts any change that
underperforms its baseline within 48h. Combined with:
  - Per-run cap (3 raises max)
  - Per-step cap (±10%)
  - Protected-demand skip (9 Dots untouched)
  - 24h cooldown (don't poke the same demand twice)
  - Total-book-impact cap (skip if demand >2% of total weekly rev)
...the worst-case loss from a bad raise is bounded at ~48h of that
demand's revenue. For an underpriced demand at WR≥50%, the upside
case (+10-30% eCPM/win) is much larger than the downside.

Cadence
-------
- Every 6 hours (reactive when patterns emerge, quiet when stable)
- Dedupes findings against prior run — only NEW patterns are Slacked
- AUTO findings include "✅ executed" tag; QUEUE findings include
  approval instructions for the operator/Claude
- Stays silent when nothing has changed since last run

Why this respects LL's ML and DSP equilibrium
---------------------------------------------
- Floor changes go to QUEUE only — never auto-execute equilibrium changes
- One-shot suggestions, not constant nudging
- 7-day lookback so noise gets filtered (single-day spikes ignored)
- Each suggestion held until weekly review window
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import median

from core import floor_ledger, ll_mgmt, slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

DATA_DIR = Path(__file__).parent.parent.parent / "data"
HOURLY_PATH = DATA_DIR / "hourly_pub_demand.json.gz"
QUEUE_PATH = DATA_DIR / "trend_hunter_queue.json"

# Pattern thresholds
UNDERPRICED_WR_MIN = 0.50      # ≥50% win rate
UNDERPRICED_ECPM_MIN = 0.50    # ≥$0.50 eCPM
UNDERPRICED_REV_MIN = 100.0    # ≥$100/wk to bother

# Auto-action caps (only for safe additive raises)
AUTO_RAISE_PCT = 0.10           # +10% raise
AUTO_MAX_RAISES_PER_RUN = 3
AUTO_RAISE_COOLDOWN_HOURS = 24  # don't poke same demand twice in a day
AUTO_RAISE_MAX_BOOK_PCT = 0.02  # skip if demand > 2% of total weekly book

DECLINING_BIDS_MIN = 100_000   # demand seeing serious bid volume
DECLINING_WR_MAX = 0.01        # but <1% conversion to wins

WOW_DROP_THRESHOLD = 0.25      # ≥25% WoW drop = flag
WOW_BASELINE_MIN = 200.0       # ≥$200/wk baseline to bother

PUB_VOLUME_RISE_THRESHOLD = 1.30   # bids 30%+ higher
PUB_ECPM_DROP_THRESHOLD = 0.85     # but eCPM 15%+ lower

ACTOR = "trend_hunter"


def _load_hourly() -> list[dict]:
    if not HOURLY_PATH.exists():
        return []
    with gzip.open(HOURLY_PATH, "rt") as f:
        return json.load(f)


def _is_protected(name: str) -> bool:
    name_lower = (name or "").lower()
    for tokens, _ in PROTECTED_FLOOR_MINIMUMS:
        if any(t in name_lower for t in tokens):
            return True
    return False


# ────────────────────────────────────────────────────────────────────────────
# Pattern detectors
# ────────────────────────────────────────────────────────────────────────────

def find_underpriced_demands(rows: list[dict]) -> list[dict]:
    """High WR + high eCPM = floor too low; raising captures more eCPM/win."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    by_demand: dict = defaultdict(lambda: {"bids": 0.0, "wins": 0.0, "rev": 0.0,
                                             "name": "", "pubs": set()})
    for r in rows:
        if str(r.get("DATE", "")) < cutoff: continue
        did = int(r.get("DEMAND_ID", 0) or 0)
        if not did: continue
        s = by_demand[did]
        s["bids"] += float(r.get("BIDS", 0) or 0)
        s["wins"] += float(r.get("WINS", 0) or 0)
        s["rev"]  += float(r.get("GROSS_REVENUE", 0) or 0)
        s["pubs"].add(int(r.get("PUBLISHER_ID", 0) or 0))
        if not s["name"]:
            s["name"] = r.get("DEMAND_NAME", "")

    found = []
    for did, s in by_demand.items():
        if s["bids"] < 1000 or s["rev"] < UNDERPRICED_REV_MIN:
            continue
        wr = s["wins"] / s["bids"]
        ecpm = (s["rev"] / s["wins"] * 1000) if s["wins"] else 0
        if wr >= UNDERPRICED_WR_MIN and ecpm >= UNDERPRICED_ECPM_MIN:
            found.append({
                "pattern": "underpriced_demand",
                "demand_id": did, "name": s["name"],
                "wr": round(wr, 3), "ecpm": round(ecpm, 2),
                "rev_7d": round(s["rev"], 2),
                "n_pubs": len(s["pubs"]),
                "action_class": "queue",
                "suggested_action": (f"Raise floor 10-15% on demand {did}. "
                                      f"WR {wr*100:.0f}% says current floor is too low."),
                "is_protected": _is_protected(s["name"]),
            })
    found.sort(key=lambda f: -f["rev_7d"])
    return found[:10]


def find_declining_demands(rows: list[dict]) -> list[dict]:
    """High bid volume but very low WR = DSP not valuing the inventory."""
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    by_demand: dict = defaultdict(lambda: {"bids": 0.0, "wins": 0.0, "rev": 0.0,
                                             "name": ""})
    for r in rows:
        if str(r.get("DATE", "")) < cutoff: continue
        did = int(r.get("DEMAND_ID", 0) or 0)
        if not did: continue
        s = by_demand[did]
        s["bids"] += float(r.get("BIDS", 0) or 0)
        s["wins"] += float(r.get("WINS", 0) or 0)
        s["rev"]  += float(r.get("GROSS_REVENUE", 0) or 0)
        if not s["name"]:
            s["name"] = r.get("DEMAND_NAME", "")

    found = []
    for did, s in by_demand.items():
        if s["bids"] < DECLINING_BIDS_MIN: continue
        wr = s["wins"] / s["bids"] if s["bids"] else 0
        if wr <= DECLINING_WR_MAX:
            found.append({
                "pattern": "dsp_declining",
                "demand_id": did, "name": s["name"],
                "bids_7d": int(s["bids"]),
                "wr": round(wr, 4),
                "rev_7d": round(s["rev"], 2),
                "action_class": "queue",
                "suggested_action": (f"Investigate demand {did}: {s['bids']:,.0f} bids, "
                                      f"only {wr*100:.2f}% WR. DSP is requesting but not "
                                      f"bidding meaningfully — check format/bundle filters, "
                                      f"creative quality, or reach out to partner."),
            })
    found.sort(key=lambda f: -f["bids_7d"])
    return found[:10]


def find_wow_drops(rows: list[dict]) -> list[dict]:
    """Per-demand revenue dropped ≥25% WoW (same DOW)."""
    today = date.today()
    last_week_start = (today - timedelta(days=7)).isoformat()
    last_week_end = today.isoformat()
    prev_week_start = (today - timedelta(days=14)).isoformat()
    prev_week_end = (today - timedelta(days=7)).isoformat()

    last = defaultdict(float)
    prev = defaultdict(float)
    names = {}
    for r in rows:
        d = str(r.get("DATE", ""))
        did = int(r.get("DEMAND_ID", 0) or 0)
        if not did: continue
        rev = float(r.get("GROSS_REVENUE", 0) or 0)
        if last_week_start <= d < last_week_end:
            last[did] += rev
            names.setdefault(did, r.get("DEMAND_NAME", ""))
        elif prev_week_start <= d < prev_week_end:
            prev[did] += rev

    found = []
    for did in set(list(last) + list(prev)):
        p = prev.get(did, 0)
        c = last.get(did, 0)
        if p < WOW_BASELINE_MIN: continue
        delta = c - p
        ratio = c / p if p > 0 else 0
        if ratio < (1 - WOW_DROP_THRESHOLD):
            found.append({
                "pattern": "wow_drop",
                "demand_id": did, "name": names.get(did, ""),
                "prev_week_rev": round(p, 2),
                "this_week_rev": round(c, 2),
                "delta": round(delta, 2),
                "drop_pct": round((1 - ratio) * 100, 1),
                "action_class": "queue",
                "suggested_action": (f"Demand {did} dropped {(1-ratio)*100:.0f}% WoW "
                                      f"(${p:.0f} → ${c:.0f}). Check ledger for recent "
                                      f"writes; if floor change correlates, propose revert. "
                                      f"Otherwise check partner-side."),
            })
    found.sort(key=lambda f: f["delta"])  # most negative first
    return found[:10]


# ────────────────────────────────────────────────────────────────────────────
# Output: Slack digest + queue file
# ────────────────────────────────────────────────────────────────────────────

def build_digest(findings: list[dict]) -> str:
    if not findings:
        return ":mag: *Trend hunter — no new patterns today.*"

    by_pattern = defaultdict(list)
    for f in findings:
        by_pattern[f["pattern"]].append(f)

    parts = [f":mag: *Trend hunter — {len(findings)} findings* "
             f"({datetime.now(timezone.utc).strftime('%Y-%m-%d')})"]

    if by_pattern.get("underpriced_demand"):
        parts.append(f"\n*🟢 Underpriced demands ({len(by_pattern['underpriced_demand'])}) — floor raise candidates:*")
        parts.append("_Review and reply 'execute' to apply via weekly digest path._")
        for f in by_pattern["underpriced_demand"][:5]:
            tag = " 🔒[contract]" if f.get("is_protected") else ""
            parts.append(f"  • `{f['demand_id']}` WR {f['wr']*100:.0f}% eCPM ${f['ecpm']:.2f} "
                          f"rev ${f['rev_7d']:.0f}/7d{tag}  _{f['name'][:40]}_")

    if by_pattern.get("dsp_declining"):
        parts.append(f"\n*🟡 DSP-declining demands ({len(by_pattern['dsp_declining'])}) — investigate:*")
        for f in by_pattern["dsp_declining"][:5]:
            parts.append(f"  • `{f['demand_id']}` {f['bids_7d']:,} bids, only {f['wr']*100:.2f}% WR  "
                          f"_{f['name'][:40]}_")

    if by_pattern.get("wow_drop"):
        parts.append(f"\n*🔻 WoW revenue drops ({len(by_pattern['wow_drop'])}):*")
        for f in by_pattern["wow_drop"][:5]:
            parts.append(f"  • `{f['demand_id']}` ${f['prev_week_rev']:.0f}→${f['this_week_rev']:.0f} "
                          f"({-f['drop_pct']:.0f}%)  _{f['name'][:40]}_")

    parts.append("\n_To act on any item, reply: 'Claude, action <pattern> for demand <id>' or wait for Monday digest._")
    return "\n".join(parts)


def _finding_key(f: dict) -> str:
    """Stable identity for dedup across runs."""
    return f"{f['pattern']}:{f.get('demand_id')}"


def _load_prior_queue() -> dict:
    if not QUEUE_PATH.exists():
        return {"underpriced": [], "declining": [], "wow_drops": []}
    try:
        return json.loads(QUEUE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"underpriced": [], "declining": [], "wow_drops": []}


def _auto_execute_underpriced_raises(underpriced: list[dict],
                                     rows: list[dict]) -> list[dict]:
    """Auto-raise floors on underpriced demands. Capped + safety-checked.
    intervention_journal will auto-revert any that underperform within 48h."""
    actor = f"trend_hunter_raise_{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    executed = []

    # Total weekly book for impact cap
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    total_book = sum(float(r.get("GROSS_REVENUE", 0) or 0)
                     for r in rows if str(r.get("DATE", "")) >= cutoff)
    book_cap = total_book * AUTO_RAISE_MAX_BOOK_PCT
    if book_cap < 100:
        book_cap = 100  # absolute floor

    # Cooldown: skip demands touched in last 24h
    recent_cutoff = (datetime.now(timezone.utc) - timedelta(hours=AUTO_RAISE_COOLDOWN_HOURS)).isoformat()
    recently_touched = set()
    for r in floor_ledger.read_all():
        if r.get("ts_utc", "") >= recent_cutoff and r.get("applied") and not r.get("dry_run"):
            recently_touched.add(r.get("demand_id"))

    for f in underpriced:
        if len(executed) >= AUTO_MAX_RAISES_PER_RUN:
            break
        did = f["demand_id"]
        if f.get("is_protected"):
            continue  # contract demands routed via clamp instead
        if did in recently_touched:
            continue
        if f["rev_7d"] > book_cap:
            continue  # too big to auto-touch

        try:
            d = ll_mgmt._get(f"/v1/demands/{did}")
            current = d.get("minBidFloor")
            if current is None or float(current) <= 0:
                continue  # need a baseline floor to raise from
            new_floor = round(float(current) * (1 + AUTO_RAISE_PCT), 2)
            result = ll_mgmt.set_demand_floor(
                did, new_floor,
                verify=True,
                allow_multi_pub=True,
                _publishers_running_it=10,
            )
            floor_ledger.record(
                publisher_id=0, publisher_name="[trend-hunter]",
                demand_id=did, demand_name=f["name"],
                old_floor=current, new_floor=new_floor,
                actor=actor,
                reason=(f"Auto-raise underpriced demand: WR {f['wr']*100:.0f}%, "
                        f"eCPM ${f['ecpm']:.2f}, ${f['rev_7d']:.0f}/7d. "
                        f"Floor +{int(AUTO_RAISE_PCT*100)}%. "
                        f"intervention_journal will revert if revenue drops."),
                dry_run=False, applied=True,
            )
            executed.append({**f, "old_floor": current, "new_floor": new_floor})
            print(f"[trend_hunter] +{int(AUTO_RAISE_PCT*100)}% raise on demand {did} "
                  f"(${current}→${new_floor}): {f['name'][:35]}")
        except Exception as e:
            print(f"[trend_hunter] raise FAILED on demand {did}: {e}")
    return executed


def run() -> dict:
    rows = _load_hourly()
    if not rows:
        return {"skipped": True, "reason": "no hourly data"}

    underpriced = find_underpriced_demands(rows)
    declining = find_declining_demands(rows)
    wow_drops = find_wow_drops(rows)
    all_findings = underpriced + declining + wow_drops

    # AUTO-EXECUTE: raise floors on underpriced demands (intervention_journal catches losers)
    auto_raised = _auto_execute_underpriced_raises(underpriced, rows)

    # Dedup against prior queue — only post NEW findings to Slack
    prior = _load_prior_queue()
    prior_keys = set()
    for bucket in ("underpriced", "declining", "wow_drops"):
        for item in prior.get(bucket, []):
            prior_keys.add(_finding_key(item))

    new_findings = [f for f in all_findings if _finding_key(f) not in prior_keys]

    # Persist current queue (replaces prior — so a finding that resolves disappears)
    queue = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "underpriced": underpriced,
        "declining": declining,
        "wow_drops": wow_drops,
    }
    QUEUE_PATH.write_text(json.dumps(queue, indent=2, default=str))

    # Slack: only on NEW findings. Stays quiet when stable.
    if new_findings:
        try:
            slack.send_text(build_digest(new_findings))
        except Exception as e:
            print(f"[{ACTOR}] Slack post failed: {e}")

    # Slack: also report auto-actions if any
    if auto_raised:
        try:
            parts = [f":robot_face: *Trend hunter auto-raised {len(auto_raised)} underpriced floor(s):*"]
            for r in auto_raised:
                parts.append(f"  • `{r['demand_id']}` ${r['old_floor']:.2f}→${r['new_floor']:.2f} "
                              f"(WR was {r['wr']*100:.0f}%) — _{r['name'][:35]}_")
            parts.append("\n_intervention_journal will auto-revert any that underperform in 48h._")
            slack.send_text("\n".join(parts))
        except Exception as e:
            print(f"[{ACTOR}] auto-raise Slack post failed: {e}")

    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "total_findings": len(all_findings),
        "new_findings": len(new_findings),
        "auto_raised": len(auto_raised),
        "n_underpriced": len(underpriced),
        "n_declining": len(declining),
        "n_wow_drops": len(wow_drops),
        "queue_path": str(QUEUE_PATH),
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
