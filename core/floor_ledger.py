"""
Floor change ledger — append-only log of every (publisher_id, demand_id) floor
change the system has made, with before/after, actor, reason, and a unique id
for later pre/post measurement and rollback.

All automated optimizers (phase1*, startio_*, tb_floor_*, future controller)
should write here in addition to their own logs. This is the single source of
truth for "what changes did we make, and when".

Schema (one JSON object per line, gzip):
    {
      "id": "lx12a…",                     # ulid-style
      "ts_utc": "2026-04-17T14:00:00Z",
      "publisher_id": 290115332,
      "publisher_name": "BidMachine - In App Interstitial (WL)",
      "demand_id": 604,
      "demand_name": "Magnite BidMachine In App",
      "old_floor": null,
      "new_floor": 3.0,
      "actor": "phase1b_executor_apr17",  # which script / human
      "reason": "Activate premium floors — $21.25 eCPM latent value",
      "dry_run": false,
      "applied": true,
      "source_log": "logs/phase1b_results_apr17.json"
    }
"""
from __future__ import annotations

import gzip
import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LEDGER_PATH = Path(__file__).parent.parent / "data" / "floor_ledger.jsonl.gz"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_id() -> str:
    return f"{int(time.time() * 1000):x}{uuid.uuid4().hex[:6]}"


def _ensure_dir() -> None:
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)


def record(
    *,
    publisher_id: int,
    demand_id: int,
    old_floor: float | None,
    new_floor: float | None,
    actor: str,
    reason: str = "",
    publisher_name: str = "",
    demand_name: str = "",
    dry_run: bool = False,
    applied: bool = True,
    source_log: str = "",
    ts_utc: str | None = None,
) -> dict[str, Any]:
    """Append one entry. Returns the written row."""
    _ensure_dir()
    row = {
        "id": _new_id(),
        "ts_utc": ts_utc or _now_iso(),
        "publisher_id": int(publisher_id),
        "publisher_name": publisher_name,
        "demand_id": int(demand_id),
        "demand_name": demand_name,
        "old_floor": old_floor,
        "new_floor": new_floor,
        "actor": actor,
        "reason": reason,
        "dry_run": bool(dry_run),
        "applied": bool(applied),
        "source_log": source_log,
    }
    # append-only gzip JSONL
    mode = "at" if LEDGER_PATH.exists() else "wt"
    with gzip.open(LEDGER_PATH, mode) as f:
        f.write(json.dumps(row) + "\n")
    return row


def read_all() -> list[dict]:
    if not LEDGER_PATH.exists():
        return []
    out = []
    with gzip.open(LEDGER_PATH, "rt") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def latest_floor(publisher_id: int, demand_id: int) -> float | None | str:
    """Return most-recent applied new_floor for a tuple, or 'unknown' if none."""
    matches = [
        r for r in read_all()
        if r["publisher_id"] == publisher_id
        and r["demand_id"] == demand_id
        and r["applied"]
        and not r["dry_run"]
    ]
    if not matches:
        return "unknown"
    return sorted(matches, key=lambda r: r["ts_utc"])[-1]["new_floor"]


def rewrite(rows: list[dict]) -> None:
    """Atomic rewrite (used by backfill and dedupe)."""
    _ensure_dir()
    fd, tmp = tempfile.mkstemp(dir=str(LEDGER_PATH.parent), suffix=".tmp.gz")
    os.close(fd)
    with gzip.open(tmp, "wt") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    os.replace(tmp, LEDGER_PATH)


# ────────────────────────────────────────────────────────────────────────────
# Backfill from existing phase1*/rollback/optimizer result logs
# ────────────────────────────────────────────────────────────────────────────

_SOURCE_LOGS = [
    "logs/phase1_results_apr17.json",
    "logs/phase1b_results_apr17.json",
    "logs/high_wr_raise_apr16_results.json",
    "logs/startio_floor_results_apr17.json",
    "logs/lurl_fleet_enable_apr17.json",
    "logs/tb_floor_nudge_actions.json",
    "logs/phase1b_rollback_apr17.json",
]


def backfill_from_logs(repo_root: Path | None = None) -> int:
    """Rebuild the ledger from known result logs. Idempotent — replaces file."""
    root = repo_root or Path(__file__).parent.parent
    existing_ids = {r["id"] for r in read_all()}
    entries: list[dict] = [r for r in read_all()]

    for rel in _SOURCE_LOGS:
        path = root / rel
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        if not isinstance(data, list):
            data = [data]
        for action in data:
            if not isinstance(action, dict):
                continue
            actor = action.get("action") or action.get("actor") or rel
            pub_id = action.get("publisher_id")
            pub_name = action.get("publisher_label") or action.get("publisher_name") or ""
            ts = action.get("timestamp") or action.get("ts_utc") or _now_iso()
            reason = action.get("strategy") or action.get("reason") or ""
            dry_run = bool(action.get("dry_run", False))
            applied = bool(action.get("applied", True))
            for ch in action.get("changes", []) or []:
                did = ch.get("demand_id")
                if pub_id is None or did is None:
                    continue
                entry = {
                    "id": _new_id(),
                    "ts_utc": ts,
                    "publisher_id": int(pub_id),
                    "publisher_name": pub_name,
                    "demand_id": int(did),
                    "demand_name": ch.get("demand_name", ""),
                    "old_floor": ch.get("old_floor"),
                    "new_floor": ch.get("new_floor"),
                    "actor": actor,
                    "reason": reason,
                    "dry_run": dry_run,
                    "applied": applied,
                    "source_log": rel,
                }
                # dedupe: same (ts,pub,demand,old,new,actor) = skip
                dedupe_key = (entry["ts_utc"], entry["publisher_id"], entry["demand_id"],
                              entry["old_floor"], entry["new_floor"], entry["actor"])
                if not any(
                    (e["ts_utc"], e["publisher_id"], e["demand_id"], e["old_floor"], e["new_floor"], e["actor"]) == dedupe_key
                    for e in entries
                ):
                    entries.append(entry)

    entries.sort(key=lambda r: r["ts_utc"])
    rewrite(entries)
    return len(entries)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    if args.backfill:
        n = backfill_from_logs()
        print(f"ledger now has {n} entries → {LEDGER_PATH}")
    elif args.show:
        rows = read_all()
        print(f"{len(rows)} entries")
        for r in rows[-15:]:
            print(f"  {r['ts_utc'][:19]}  pub={r['publisher_id']}  demand={r['demand_id']:<5}  "
                  f"{r['old_floor']} → {r['new_floor']}   [{r['actor']}]")
    else:
        ap.print_help()
