"""
agents/reports/weekly_review_digest.py

Monday 09:00 ET weekly digest of proposals needing human judgment.

Posts a ranked, annotated Slack digest of the accumulated proposals since
last Monday: what the optimizer thinks we should do, annotated with
rule-based quality flags, so the operator can make a batch decision by
replying with approval.

This replaces the daily Slack spam from ml_proposer with a single weekly
review that groups proposals by confidence + expected lift + risk flags.

Output format (Slack):
  Section 1: top-10 HIGH-CONFIDENCE proposals (auto-execute candidates)
  Section 2: MEDIUM-CONFIDENCE with explanatory context
  Section 3: flagged PROTECTED-CONTRACT proposals (these should never be here)
  Section 4: proposals that would DROP floors (extra scrutiny)
  Footer: approval instructions — ping Claude with proposal IDs

Does NOT execute anything — that's the whole point. Human-in-the-loop.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from core import slack
from core.ll_mgmt import PROTECTED_FLOOR_MINIMUMS

DATA_DIR = Path(__file__).parent.parent.parent / "data"
PROPOSALS_PATH = DATA_DIR / "proposals.json"


def _is_protected(name: str) -> bool:
    name_lower = (name or "").lower()
    for tokens, _ in PROTECTED_FLOOR_MINIMUMS:
        if any(t in name_lower for t in tokens):
            return True
    return False


def _is_monday_9am_et() -> bool:
    """Gate: only fire on Monday at 9:xx ET."""
    import pytz
    et = datetime.now(pytz.timezone("US/Eastern"))
    return et.weekday() == 0 and et.hour == 9


def _fmt_floor(f) -> str:
    if f is None:
        return "None"
    return f"${float(f):.2f}"


def build_digest() -> dict:
    """Read proposals, categorize, build Slack digest."""
    if not PROPOSALS_PATH.exists():
        return {"skipped": True, "reason": "proposals.json missing"}

    data = json.loads(PROPOSALS_PATH.read_text())
    props = data.get("proposals", [])
    if not props:
        slack.send_text(":robot_face: *Weekly review digest* — no open proposals this week.")
        return {"posted": True, "count": 0}

    # Categorize
    high_conf = []
    medium_conf = []
    low_conf = []
    protected_flags = []
    drop_flags = []

    for p in props:
        name = p.get("demand_name", "")
        if _is_protected(name):
            protected_flags.append(p)
        cur = p.get("current_floor")
        new = p.get("proposed_floor")
        if cur is not None and new is not None and float(new) < float(cur):
            drop_flags.append(p)

        conf = p.get("confidence", "low")
        lift = float(p.get("expected_weekly_net_lift", 0) or 0)
        if conf == "high" and lift >= 25:
            high_conf.append(p)
        elif conf == "medium" or (conf == "high" and lift < 25):
            medium_conf.append(p)
        else:
            low_conf.append(p)

    high_conf.sort(key=lambda p: -float(p.get("expected_weekly_net_lift", 0) or 0))
    medium_conf.sort(key=lambda p: -float(p.get("expected_weekly_net_lift", 0) or 0))

    # Build message
    total_lift_high = sum(float(p.get("expected_weekly_net_lift", 0) or 0) for p in high_conf)
    header = (
        f":robot_face: *Weekly proposal review — {datetime.now(timezone.utc).strftime('%Y-%m-%d')}*\n"
        f"Total open proposals: {len(props)}  |  "
        f"High-conf lift: +${total_lift_high:,.0f}/wk"
    )

    sections = [header]

    if high_conf:
        lines = [f"\n*🟢 HIGH-CONFIDENCE (top {min(10, len(high_conf))} — recommend approve):*"]
        for p in high_conf[:10]:
            lines.append(
                f"  `{p['id']}`  {_fmt_floor(p['current_floor'])} → {_fmt_floor(p['proposed_floor'])}  "
                f"+${p['expected_weekly_net_lift']:.0f}/wk  CI[{p.get('ci_low_net',0):+.0f}…{p.get('ci_high_net',0):+.0f}]  "
                f"_{(p.get('demand_name','') or '')[:35]}_"
            )
        sections.append("\n".join(lines))

    if medium_conf:
        lines = [f"\n*🟡 MEDIUM-CONFIDENCE ({len(medium_conf)} total, showing top 5 — review case-by-case):*"]
        for p in medium_conf[:5]:
            lines.append(
                f"  `{p['id']}`  {_fmt_floor(p['current_floor'])} → {_fmt_floor(p['proposed_floor'])}  "
                f"+${p['expected_weekly_net_lift']:.0f}/wk  _{(p.get('demand_name','') or '')[:35]}_"
            )
        sections.append("\n".join(lines))

    if protected_flags:
        lines = [f"\n*⚠️ PROTECTED-CONTRACT DEMANDS IN PROPOSALS ({len(protected_flags)}):*",
                 "_These shouldn't be here — the pre-filter should have excluded them. Investigate._"]
        for p in protected_flags[:5]:
            lines.append(
                f"  `{p['id']}`  {_fmt_floor(p['current_floor'])} → {_fmt_floor(p['proposed_floor'])}  "
                f"_{(p.get('demand_name','') or '')[:35]}_"
            )
        sections.append("\n".join(lines))

    if drop_flags:
        lines = [f"\n*🔻 FLOOR-DROP PROPOSALS ({len(drop_flags)}) — extra scrutiny:*",
                 "_Drops carry more risk than raises (9 Dots incident). Review each._"]
        for p in drop_flags[:5]:
            lines.append(
                f"  `{p['id']}`  {_fmt_floor(p['current_floor'])} → {_fmt_floor(p['proposed_floor'])}  "
                f"_{(p.get('demand_name','') or '')[:35]}_"
            )
        sections.append("\n".join(lines))

    footer = (
        "\n---\n"
        ":memo: *To approve*: ping Claude with proposal IDs, e.g.\n"
        "  _\"Claude, execute these proposals: prop_abc prop_xyz prop_qwe\"_\n\n"
        ":white_check_mark: *Auto-executing this week* (no approval needed):\n"
        "  • Demand-gap wirings clearing thresholds (daily)\n"
        "  • Silent-pause re-activations (daily)\n"
        "  • Harmful-write reverts (every 4h)\n"
        "  • Contract floor enforcement (daily + write-path clamp)"
    )
    sections.append(footer)

    body = "\n".join(sections)
    slack.send_blocks(
        [{"type": "section", "text": {"type": "mrkdwn", "text": body}}],
        text=f"Weekly proposal review: {len(props)} proposals",
    )
    return {
        "posted": True,
        "high_conf": len(high_conf),
        "medium_conf": len(medium_conf),
        "protected_flags": len(protected_flags),
        "drop_flags": len(drop_flags),
        "total_lift_high": round(total_lift_high, 2),
    }


def run() -> dict:
    """Scheduler entry. Only fires on Monday at 9:xx ET."""
    if not _is_monday_9am_et():
        return {"skipped": True, "reason": "not Monday 9am ET"}
    return build_digest()


if __name__ == "__main__":
    # Allow manual force-run via CLI
    import sys
    if "--force" in sys.argv:
        print(json.dumps(build_digest(), indent=2, default=str))
    else:
        print(json.dumps(run(), indent=2, default=str))
