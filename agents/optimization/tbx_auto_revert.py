"""
agents/optimization/tbx_auto_revert.py

The safety net under `tbx_demand_geo_floor`. Every run, it re-reads the geo
floor writes that agent made on `api.pgammedia.com`, measures what happened
to the DSP afterwards, and puts the floors back if the change did harm.

This is the TBX analogue of `agents/optimization/auto_revert_harmful.py`,
which does the same job for LiftLeap. It is a separate file rather than a
branch inside that one because almost nothing carries over: a different
ledger, a different write path, a different revert primitive, and — the part
that shapes the whole design — a different measurement grain.

Why this exists
---------------
On 2026-04-18 the LL portfolio optimizer dropped 9 Dots floors to $0 and
nobody noticed for over 24 hours. Every floor writer since is expected to
ship with the thing that notices. `tbx_demand_geo_floor` is the first writer
on the new platform, so this is the first net.

The measurement grain is the whole design constraint
----------------------------------------------------
The LL agent compares *hourly* revenue and can act six hours after a bad
write. The TBX report has no `hour` attribute — `date` is the finest grain
the platform offers (`docs/api/teqblaze-openapi.json`; the 25 attributes are
listed in the reference under A3). And today is never settled, so the newest
usable day is yesterday.

That puts a floor under how fast this can possibly react:

    write lands on day D
    day D is partial            -> unusable
    day D+1 settles overnight   -> first usable post-day
    run on day D+2              -> earliest possible revert

So the earliest revert is ~2 days after the write, against ~6 hours on LL.
Three consequences, all deliberate:

* `MIN_POST_DAYS` is 2, not 1. One day against a 7-day average is mostly
  day-of-week noise, and a false revert is itself a harmful write.
* This net is therefore *weaker* than LL's, and the forward agent's caps are
  what actually bound the damage — `MAX_FLOOR_DELTA` (25%), the per-run
  source cap, and `FLOOR_PCT` below 1.0. Do not loosen those on the theory
  that auto-revert will catch it. Over a two-day detection window it will
  not, it will only end it.
* If Teqblaze ever exposes an hour attribute, `PRE_DAYS`/`MIN_POST_DAYS`
  become hours and this agent gets much sharper. §8.1 of
  `docs/teqblaze-new-platform.md` is the place to ask.

What counts as harm
-------------------
Two independent triggers, either one is enough:

1. **Profit collapse** — daily `profit` rate falls `DROP_THRESHOLD_PCT`
   below the pre-change rate. Profit rather than gross because profit is
   what the floor is for: `dsp_price_sum - ssp_price_sum`, and a floor that
   raises margin while destroying volume still shows up here.
2. **Fill collapse** — daily impressions fall `IMPS_COLLAPSE_PCT` below the
   pre-change rate. A floor raise that zeroes out a DSP is unambiguous harm
   even in the rare case where the surviving impressions are profitable
   enough to hold the profit line.

Both are rates per settled day, so an uneven pre/post window length does not
skew the comparison.

What it refuses to do
---------------------
* **Revert a write it did not make.** Only actors in `REVERTABLE_ACTORS` are
  candidates. A human's manual floor change is theirs.
* **Revert its own reverts**, or revert the same write twice — each revert
  records `reverted_from` with the original entry's ledger id.
* **Clobber a third party.** If anything else wrote to the same demand source
  between our change and now, restoring our snapshot would silently undo
  their work too. That escalates to Slack instead.
* **Override a partner freeze.** `set_demand_geo_bid_floors` refuses frozen
  partners, and this agent does not go around it — but a freeze blocking a
  revert is reported loudly, because the harm is still live and now needs
  hands.

Gates
-----
    --apply              on the command line (dry_run defaults True)
    TBX_ALLOW_WRITES=1   the platform-wide write gate in tbx_mgmt

Deliberately **not** gated on `PGAM_OPTIMIZER_AUTO_APPLY`, which every
forward optimizer requires. That gate authorises taking *new* positions; a
revert only restores one the platform was already in and a human already
lived with. Gating the net behind the accelerator means that closing the
accelerator mid-incident — the exact reflex someone has on seeing a bad
write — also disables the thing that undoes it. `TBX_ALLOW_WRITES` remains
the master switch: with it off, nothing here writes either.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

_LOG = "[tbx_auto_revert]"

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
ACTIONS_LOG = LOG_DIR / "tbx_auto_revert_actions.json"

# Writes this agent is allowed to undo. Anything not listed here — a human, a
# different agent, an earlier revert — is out of scope by construction.
REVERTABLE_ACTORS = ("tbx_demand_geo_floor",)
ACTOR_PREFIX = "tbx_auto_revert"

# The one action we know how to reverse. Restoring a geo floor snapshot is a
# single idempotent write; other TBX actions are not, so they are not listed.
REVERTABLE_ACTION = "set_demand_geo_bid_floors"
ENTITY_TYPE = "tbx_demand_source"

REVERT_WINDOW_DAYS = 7      # how far back to reconsider a write
PRE_DAYS = 7                # baseline sample, long enough to average out DOW
MIN_POST_DAYS = 2           # see the grain note above — 1 is mostly noise
DROP_THRESHOLD_PCT = 0.20   # profit rate below this fraction of pre -> revert
IMPS_COLLAPSE_PCT = 0.50    # impressions rate below this fraction -> revert
MIN_PRE_PROFIT = 50.0       # total pre-window profit worth defending, USD
MAX_REVERTS_PER_RUN = 3

METRICS = ("imps_sum", "dsp_price_sum", "ssp_price_sum", "profit")


# ─── Small helpers ───────────────────────────────────────────────────────────


def _f(value) -> float:
    """Platform numerics arrive as strings. Same helper as the ETL's."""
    if value in (None, "", "-"):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return 0.0


def _yesterday() -> "datetime.date":
    """Newest settled day. Today is always partial, so it is never used."""
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _parse_ts(entry: dict):
    raw = str(entry.get("ts") or "")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _floor_rows(geo_settings) -> list[dict]:
    """The `bid_floor` rows out of a `geo_settings` blob, defensively."""
    if not isinstance(geo_settings, dict):
        return []
    rows = geo_settings.get("bid_floor")
    return list(rows) if isinstance(rows, list) else []


def _floor_map(geo_settings) -> dict[int, float]:
    """`{country_id: value}` from a `geo_settings` blob."""
    out: dict[int, float] = {}
    for row in _floor_rows(geo_settings):
        if not isinstance(row, dict) or row.get("country_id") is None:
            continue
        try:
            out[int(row["country_id"])] = _f(row.get("value"))
        except (TypeError, ValueError):
            continue
    return out


# ─── Candidate selection ─────────────────────────────────────────────────────


def find_candidates(entries: list[dict], now: datetime | None = None) -> tuple[list[dict], list[str]]:
    """
    `(revertable writes, skip reasons)` out of a ledger slice.

    Pure over its input so the selection rules can be tested without a ledger
    file, a platform, or a clock.
    """
    from core import tb_ledger

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=REVERT_WINDOW_DAYS)

    # Every entry id we have already reverted, so a second run does not undo
    # the same write twice. Read from the ledger itself rather than a side
    # file — one source of truth, and it survives a lost container.
    already: set[str] = set()
    for entry in entries:
        if str(entry.get("actor", "")).startswith(ACTOR_PREFIX):
            target = (entry.get("extra") or {}).get("reverted_from")
            if target:
                already.add(str(target))

    # Latest write per entity by anyone, so we can spot a third party having
    # moved the floors since ours landed.
    writes_by_entity: dict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        if entry.get("entity_type") != ENTITY_TYPE:
            continue
        if not entry.get("applied") or entry.get("dry_run"):
            continue
        writes_by_entity[str(entry.get("entity_id"))].append(entry)

    candidates: list[dict] = []
    skips: list[str] = []
    for entry in entries:
        if entry.get("entity_type") != ENTITY_TYPE:
            continue
        if entry.get("action") != REVERTABLE_ACTION:
            continue
        if not entry.get("applied") or entry.get("dry_run"):
            continue

        actor = str(entry.get("actor") or "")
        if actor.startswith(ACTOR_PREFIX):
            continue            # never revert a revert
        if not any(actor.startswith(a) for a in REVERTABLE_ACTORS):
            continue            # someone else's write is not ours to undo

        ts = _parse_ts(entry)
        if ts is None or ts < cutoff:
            continue

        key = tb_ledger.entry_key(entry)
        if key in already:
            continue

        before = _floor_map((entry.get("before") or {}).get("geo_settings"))
        after = _floor_map((entry.get("after") or {}).get("geo_settings"))
        if before == after:
            continue            # the write changed no floor; nothing to undo

        # A third-party write to the same entity after ours means restoring
        # our snapshot would also roll back whatever they did.
        entity_id = str(entry.get("entity_id"))
        intruders = [
            other for other in writes_by_entity.get(entity_id, [])
            if (_parse_ts(other) or ts) > ts
            and not str(other.get("actor") or "").startswith(ACTOR_PREFIX)
            and tb_ledger.entry_key(other) != key
        ]
        if intruders:
            who = ", ".join(sorted({str(i.get("actor")) for i in intruders}))
            skips.append(
                f"[{entity_id}] {entry.get('reason','')[:40]} — written again by "
                f"{who} since; restoring our snapshot would undo theirs too. "
                f"Needs a human.")
            continue

        candidates.append({
            "ledger_id": key,
            "ts": ts,
            "actor": actor,
            "demand_source_id": int(entry.get("entity_id")),
            "before_floors": before,
            "after_floors": after,
            "reason": entry.get("reason") or "",
        })

    candidates.sort(key=lambda c: c["ts"])
    return candidates, skips


# ─── Measurement ─────────────────────────────────────────────────────────────


def daily_rows(start: str, end: str) -> list[dict]:
    """`date × demand_source` over the window, one row per pair."""
    from core import tbx_api as tbx

    rows, _totals = tbx.report(
        start, end,
        attributes=["date", "demand_source"],
        metrics=list(METRICS),
    )
    return rows


def _index_by_source_day(rows: list[dict]) -> dict[tuple[int, str], dict]:
    """Fold report rows to `{(demand_source_id, YYYY-MM-DD): metrics}`."""
    from agents.etl.tbx_revenue_etl import _entity

    out: dict[tuple[int, str], dict] = defaultdict(
        lambda: {"imps": 0.0, "gross": 0.0, "payout": 0.0, "profit": 0.0})
    for row in rows:
        entity_id, _name = _entity(row, "demand_source")
        if entity_id is None:
            continue
        day = str(row.get("date") or row.get("report_date") or "")[:10]
        if not day:
            continue
        bucket = out[(int(entity_id), day)]
        bucket["imps"] += _f(row.get("imps_sum"))
        bucket["gross"] += _f(row.get("dsp_price_sum"))
        bucket["payout"] += _f(row.get("ssp_price_sum"))
        bucket["profit"] += _f(row.get("profit"))
    return dict(out)


def _window_rate(index: dict, source_id: int, days: list[str]) -> tuple[dict, int]:
    """
    `(per-day rates, days_with_data)` for one source across named days.

    A day with no row is a real zero for this source, not missing data — the
    platform drops all-zero rows (reference A5.2) — so it counts toward the
    denominator. That matters: a floor that zeroes a DSP produces *no rows*,
    and treating those as missing would make the collapse invisible.
    """
    totals = {"imps": 0.0, "gross": 0.0, "payout": 0.0, "profit": 0.0}
    with_data = 0
    for day in days:
        bucket = index.get((source_id, day))
        if bucket:
            with_data += 1
            for key in totals:
                totals[key] += bucket[key]
    span = max(len(days), 1)
    return {k: v / span for k, v in totals.items()}, with_data


def assess(candidate: dict, index: dict, today: "datetime.date | None" = None) -> dict:
    """
    Decide whether one candidate write did harm.

    Returns a verdict dict with `revert` (bool), the pre/post rates, and a
    human-readable `why`. Pure over `index` so the rule is testable.
    """
    newest = _yesterday() if today is None else today
    change_day = candidate["ts"].date()

    post_start = change_day + timedelta(days=1)
    if post_start > newest:
        return {"revert": False, "why": "no settled day since the write yet"}
    post_days = [(post_start + timedelta(days=i)).isoformat()
                 for i in range((newest - post_start).days + 1)]
    if len(post_days) < MIN_POST_DAYS:
        return {"revert": False,
                "why": f"only {len(post_days)} settled day(s) since the write, "
                       f"need {MIN_POST_DAYS}"}

    pre_end = change_day - timedelta(days=1)
    pre_days = [(pre_end - timedelta(days=i)).isoformat() for i in range(PRE_DAYS)]

    sid = candidate["demand_source_id"]
    pre, pre_seen = _window_rate(index, sid, pre_days)
    post, _post_seen = _window_rate(index, sid, post_days)

    verdict = {
        "revert": False,
        "pre": {k: round(v, 2) for k, v in pre.items()},
        "post": {k: round(v, 2) for k, v in post.items()},
        "pre_days": len(pre_days),
        "post_days": len(post_days),
        "pre_days_with_data": pre_seen,
    }

    if pre_seen == 0:
        verdict["why"] = "no pre-change data for this source — nothing to compare"
        return verdict

    pre_profit_total = pre["profit"] * len(pre_days)
    if pre_profit_total < MIN_PRE_PROFIT:
        verdict["why"] = (f"pre-window profit ${pre_profit_total:.2f} is below the "
                          f"${MIN_PRE_PROFIT:.0f} floor — too small to act on")
        return verdict

    triggers: list[str] = []
    if pre["profit"] > 0:
        ratio = post["profit"] / pre["profit"]
        verdict["profit_ratio"] = round(ratio, 3)
        if ratio < (1 - DROP_THRESHOLD_PCT):
            triggers.append(
                f"profit ${pre['profit']:.2f}/day → ${post['profit']:.2f}/day "
                f"({ratio:.0%} of pre, threshold {1 - DROP_THRESHOLD_PCT:.0%})")

    if pre["imps"] > 0:
        imps_ratio = post["imps"] / pre["imps"]
        verdict["imps_ratio"] = round(imps_ratio, 3)
        if imps_ratio < (1 - IMPS_COLLAPSE_PCT):
            triggers.append(
                f"impressions {pre['imps']:,.0f}/day → {post['imps']:,.0f}/day "
                f"({imps_ratio:.0%} of pre)")

    verdict["revert"] = bool(triggers)
    verdict["why"] = "; ".join(triggers) if triggers else "within tolerance"
    return verdict


# ─── The revert itself ───────────────────────────────────────────────────────


def revert_one(candidate: dict, verdict: dict, dry_run: bool) -> dict:
    """
    Put one demand source's geo floors back to the pre-change snapshot.

    `replace=True` is correct here and wrong almost everywhere else: the
    snapshot *is* the complete prior state, so replacing rebuilds it exactly,
    including dropping any country the forward run added. Merging would leave
    those additions in place, which is not a revert. The third-party check in
    `find_candidates` is what makes replacing safe — without it this would
    quietly roll back someone else's countries too.
    """
    from core import tbx_mgmt as tbm

    sid = candidate["demand_source_id"]
    target = candidate["before_floors"]

    result = tbm.set_demand_geo_bid_floors(
        demand_source_id=sid,
        floors_by_country_id=target,
        replace=True,
        actor=f"{ACTOR_PREFIX}_{datetime.now(timezone.utc):%Y%m%d}",
        reason=(f"auto-revert of {candidate['actor']} write {candidate['ledger_id']}: "
                f"{verdict.get('why', '')}"),
        dry_run=dry_run,
        demand_name=candidate.get("demand_name"),
    )

    action = {
        "ledger_id": candidate["ledger_id"],
        "demand_source_id": sid,
        "reverted_to": target,
        "reverted_from_floors": candidate["after_floors"],
        "verdict": verdict,
        "dry_run": dry_run,
        "applied": bool(result.get("applied")),
        "refused": result.get("refused"),
        "clamps": result.get("clamps") or [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # A clamp during a *revert* means the floors did not land where the
    # snapshot says they were, so the entity is now in a third state that is
    # neither before nor after. Worth saying out loud rather than reporting a
    # clean revert. In practice the delta cap permits it — undoing a raise of
    # at most +MAX_FLOOR_DELTA needs a cut of at most that same fraction of
    # the raised value, which is always inside the cap — so a clamp here
    # usually means GLOBAL_MIN_FLOOR raising a prior 0.00 to 0.01.
    if action["clamps"]:
        action["inexact"] = True

    # Link the revert to the write it undid, in the ledger, so the next run
    # can see it. `set_demand_geo_bid_floors` already wrote its own entry; this
    # is the cross-reference, and it has to be its own record because the
    # ledger is append-only and the earlier line cannot be edited.
    if not dry_run:
        from core import tb_ledger
        tb_ledger.record(
            actor=f"{ACTOR_PREFIX}_{datetime.now(timezone.utc):%Y%m%d}",
            action="auto_revert_link",
            entity_type=ENTITY_TYPE,
            entity_id=sid,
            reason=verdict.get("why", ""),
            before={"geo_settings": {"bid_floor": [
                {"country_id": c, "value": v}
                for c, v in sorted(candidate["after_floors"].items())]}},
            after={"geo_settings": {"bid_floor": [
                {"country_id": c, "value": v}
                for c, v in sorted(target.items())]}},
            applied=bool(result.get("applied")),
            dry_run=False,
            extra={"reverted_from": candidate["ledger_id"],
                   "platform": "tbx",
                   "refused": result.get("refused")},
        )
    return action


# ─── Output ──────────────────────────────────────────────────────────────────


def slack_summary(actions: list[dict], skips: list[str], examined: int,
                  applied: bool) -> str:
    reverted = [a for a in actions if a.get("applied")]
    blocked = [a for a in actions if a.get("refused")]

    if not actions and not skips:
        return (f"↩️ *TBX auto-revert* — {examined} geo-floor write(s) checked, "
                f"none did harm.")

    tag = "🟢 REVERTED" if applied else "🔍 WOULD REVERT"
    lines = [f"↩️ *TBX auto-revert* {tag} — {len(actions)} of {examined} "
             f"write(s) flagged"]
    for action in actions[:6]:
        sid = action["demand_source_id"]
        why = action.get("verdict", {}).get("why", "")
        lines.append(f"  • [{sid}] {why[:110]}")
        if action.get("refused"):
            lines.append(f"      ⚠️ *blocked: {action['refused']}* — the harmful "
                         f"floors are still live and need hands")
        elif action.get("inexact"):
            lines.append(f"      ⚠️ clamped on the way back: "
                         f"{'; '.join(action['clamps'])[:120]}")
    if len(actions) > 6:
        lines.append(f"  … +{len(actions) - 6} more")
    for skip in skips[:4]:
        lines.append(f"  ⏭️ {skip[:140]}")
    if not applied:
        lines.append("  _Propose-only. `--apply` with TBX_ALLOW_WRITES=1 to write._")
    if blocked:
        lines.append(f"  _{len(blocked)} revert(s) refused — see the run log._")
    elif reverted:
        lines.append(f"  _{len(reverted)} floor set(s) restored to their "
                     f"pre-change snapshot._")
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


def run(dry_run: bool = True) -> dict:
    from core import tb_ledger
    from core import tbx_api as tbx
    from core import tbx_mgmt as tbm

    if not tbx.configured():
        missing = [k for k in ("TBX_EMAIL", "TBX_PASSWORD") if not os.getenv(k)]
        print(f"{_LOG} not configured — missing {', '.join(missing)}. Nothing to do.")
        return {"ok": True, "skipped": "not configured"}

    # One gate, not the optimizer's three — see the module docstring on why
    # PGAM_OPTIMIZER_AUTO_APPLY deliberately does not apply to a revert.
    if not dry_run and not tbm.writes_enabled():
        print(f"{_LOG} --apply given but TBX_ALLOW_WRITES is not 1. "
              f"Falling back to propose-only.")
        dry_run = True

    now = datetime.now(timezone.utc)
    entries = list(tb_ledger.iter_entries(since=now - timedelta(days=REVERT_WINDOW_DAYS + 1)))
    candidates, skips = find_candidates(entries, now=now)
    print(f"{_LOG} {len(candidates)} revertable geo-floor write(s) in the last "
          f"{REVERT_WINDOW_DAYS}d, {len(skips)} skipped")

    if not candidates:
        if skips:
            from core.slack import send_text
            send_text(slack_summary([], skips, 0, applied=False))
        return {"ok": True, "examined": 0, "reverts": 0, "skips": skips}

    # One report call covers every candidate: widest window any of them needs.
    oldest = min(c["ts"].date() for c in candidates)
    start = (oldest - timedelta(days=PRE_DAYS + 1)).isoformat()
    end = _yesterday().isoformat()
    print(f"{_LOG} measuring {start} → {end} (date × demand_source)")
    index = _index_by_source_day(daily_rows(start, end))

    # Names, for the freeze check and for anything a human reads.
    names: dict[int, str] = {}
    try:
        for src in tbm.list_demand_sources():
            if src.get("id") is not None:
                names[int(src["id"])] = (src.get("name") or src.get("title") or "").strip()
    except Exception as exc:                              # noqa: BLE001
        print(f"{_LOG} could not list demand sources ({exc}); "
              f"proceeding without names — partner_freeze cannot match on a "
              f"name it does not have, so a frozen partner may not be caught.")

    actions: list[dict] = []
    for candidate in candidates:
        if len(actions) >= MAX_REVERTS_PER_RUN:
            skips.append(f"per-run cap of {MAX_REVERTS_PER_RUN} reached; "
                         f"remaining candidates wait for the next run")
            break
        candidate["demand_name"] = names.get(candidate["demand_source_id"])
        verdict = assess(candidate, index)
        label = candidate["demand_name"] or candidate["demand_source_id"]
        if not verdict["revert"]:
            print(f"{_LOG}   keep [{label}] — {verdict['why']}")
            continue
        print(f"{_LOG}   REVERT [{label}] — {verdict['why']}")
        try:
            actions.append(revert_one(candidate, verdict, dry_run))
        except Exception as exc:                          # noqa: BLE001
            print(f"{_LOG}   revert FAILED [{label}]: {type(exc).__name__}: {exc}")
            actions.append({
                "ledger_id": candidate["ledger_id"],
                "demand_source_id": candidate["demand_source_id"],
                "verdict": verdict,
                "applied": False,
                "error": f"{type(exc).__name__}: {exc}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    if actions:
        _append_actions(actions)
    try:
        from core.slack import send_text
        send_text(slack_summary(actions, skips, len(candidates), applied=not dry_run))
    except Exception as exc:                              # noqa: BLE001
        print(f"{_LOG} slack post failed: {exc}")

    return {
        "ok": True,
        "ran_at": now.isoformat(),
        "examined": len(candidates),
        "reverts": sum(1 for a in actions if a.get("applied")),
        "blocked": sum(1 for a in actions if a.get("refused")),
        "dry_run": dry_run,
        "skips": skips,
        "actions": actions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Revert TBX geo-floor writes that correlate with harm")
    parser.add_argument("--apply", action="store_true",
                        help="write the reverts (needs TBX_ALLOW_WRITES=1)")
    args = parser.parse_args()
    outcome = run(dry_run=not args.apply)
    print(json.dumps(outcome, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
