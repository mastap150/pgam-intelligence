"""
agents/alerts/adstxt_monitor.py

Daily ads.txt monitor for PGAM-owned O&O sites.

For every site in REQUIRED_ADSTXT below, fetches the live ads.txt over HTTPS
and verifies that each "required" seller entry (PGAM's own seats —
pgammedia.com / limelight.com / teqblaze.com) is present AND set to DIRECT.

Why
---
ads.txt is a publisher-side allowlist; if a PGAM seat is removed or flipped
from DIRECT to RESELLER, demand stops bidding into that seat and revenue
silently drops. The source-of-truth lives in the pgam-wrapper repo at
configs/ads.txt.<site> and is published via the static site build — but
nothing automatically verifies the published file matches intent.

This agent is the closing-the-loop check: if a deploy / hotfix / human
edit drops a critical line, we get a Slack page within ~24h instead of
finding out from a partner asking why bids dried up.

What it alerts on
-----------------
P1 — required entry missing OR present but RESELLER (= revenue stopped)
P2 — file unreachable / non-200 / non-text
P3 — DIRECT entry present in live but unfamiliar to us (= someone edited
     ads.txt without updating the source-of-truth in pgam-wrapper).
     Catches well-intentioned changes that bypass review.

State
-----
logs/adstxt_snapshots.json — last seen sha + entry count per site.
Used for change-detection so a steady-state day stays quiet.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone

import requests

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(dotenv_path=os.path.join(_REPO_ROOT, ".env"), override=True)

from core import slack  # noqa: E402

ACTOR = "adstxt_monitor"
SNAPSHOT_PATH = os.path.join(_REPO_ROOT, "logs", "adstxt_snapshots.json")
HTTP_TIMEOUT_SEC = 15


# ── Per-site contract: which entries MUST be present + DIRECT ────────────────
# Source-of-truth lives in pgam-wrapper/configs/ads.txt.<site>; the entries
# below are the SUBSET we treat as contract-critical (PGAM-owned seats).
# Adding new SSP partners to ads.txt does NOT require a code change here —
# we only enforce PGAM's own seats.
REQUIRED_ADSTXT: dict[str, list[tuple[str, str, str]]] = {
    "destination.com": [
        ("pgammedia.com", "pgam-dest-001", "DIRECT"),
        ("limelight.com", "ll-pgam-dest-001", "DIRECT"),
        ("teqblaze.com", "tb-pgam-dest-001", "DIRECT"),
    ],
    "boxingnews.com": [
        ("pgammedia.com", "pgam-bn-001", "DIRECT"),
        ("limelight.com", "ll-pgam-bn-001", "DIRECT"),
        ("teqblaze.com", "tb-pgam-bn-001", "DIRECT"),
    ],
}


# ── Parsing ──────────────────────────────────────────────────────────────────


def parse_adstxt(body: str) -> list[tuple[str, str, str]]:
    """Return [(domain, account_id, relationship), ...] from a raw ads.txt body.

    Strips comments, blank lines, and the trailing optional cert-id field.
    Lower-cases the domain and uppercases the relationship for case-insensitive
    comparison; account_id is preserved as-is (case can matter on some SSPs).
    """
    out: list[tuple[str, str, str]] = []
    for raw in body.splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            continue
        domain, account_id, relationship = parts[0], parts[1], parts[2]
        out.append((domain.lower(), account_id, relationship.upper()))
    return out


def fetch_adstxt(site: str) -> tuple[int, str]:
    """Fetch https://<site>/ads.txt — returns (status_code, body)."""
    url = f"https://{site}/ads.txt"
    headers = {"User-Agent": f"pgam-intelligence/{ACTOR}"}
    res = requests.get(url, timeout=HTTP_TIMEOUT_SEC, headers=headers)
    return res.status_code, res.text


# ── Snapshot state ───────────────────────────────────────────────────────────


def _load_snapshots() -> dict:
    if not os.path.exists(SNAPSHOT_PATH):
        return {}
    try:
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_snapshots(snapshots: dict) -> None:
    os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshots, f, indent=2)


# ── Diff logic ───────────────────────────────────────────────────────────────


def _check_site(site: str, required: list[tuple[str, str, str]]) -> dict:
    """Returns {missing, wrong_relationship, unexpected_direct, status, sha, entries}."""
    try:
        status, body = fetch_adstxt(site)
    except requests.RequestException as e:
        return {
            "site": site,
            "fetch_error": str(e),
            "status": None,
            "missing": [],
            "wrong_relationship": [],
            "unexpected_direct": [],
        }

    if status != 200:
        return {
            "site": site,
            "status": status,
            "fetch_error": f"HTTP {status}",
            "missing": [],
            "wrong_relationship": [],
            "unexpected_direct": [],
        }

    entries = parse_adstxt(body)
    sha = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]

    # Required-line checks: by (domain, account_id) — relationship is what we verify.
    by_key: dict[tuple[str, str], str] = {(d, aid): rel for (d, aid, rel) in entries}
    missing: list[tuple[str, str, str]] = []
    wrong_relationship: list[tuple[str, str, str, str]] = []  # +observed
    for dom, aid, expected_rel in required:
        observed = by_key.get((dom.lower(), aid))
        if observed is None:
            missing.append((dom, aid, expected_rel))
        elif observed.upper() != expected_rel.upper():
            wrong_relationship.append((dom, aid, expected_rel, observed))

    # Unexpected-DIRECT scan: any DIRECT line for a PGAM-owned domain that we
    # don't recognise. PGAM-owned domains = pgammedia.com / limelight.com /
    # teqblaze.com. Catches accidental additions on our own seats only — we
    # don't gate third-party SSP additions.
    pgam_domains = {"pgammedia.com", "limelight.com", "teqblaze.com"}
    known_pgam_keys = {(d.lower(), aid) for (d, aid, _) in required}
    unexpected_direct: list[tuple[str, str, str]] = []
    for dom, aid, rel in entries:
        if dom in pgam_domains and rel == "DIRECT":
            if (dom, aid) not in known_pgam_keys:
                unexpected_direct.append((dom, aid, rel))

    return {
        "site": site,
        "status": status,
        "sha": sha,
        "entry_count": len(entries),
        "missing": missing,
        "wrong_relationship": wrong_relationship,
        "unexpected_direct": unexpected_direct,
    }


# ── Slack formatting ─────────────────────────────────────────────────────────


def _format_p1(report: dict) -> str | None:
    site = report["site"]
    msgs: list[str] = []
    for dom, aid, expected_rel in report["missing"]:
        msgs.append(f"• MISSING `{dom}, {aid}, {expected_rel}`")
    for dom, aid, expected_rel, observed in report["wrong_relationship"]:
        msgs.append(
            f"• WRONG-RELATIONSHIP `{dom}, {aid}` — expected *{expected_rel}*, found *{observed}*"
        )
    if not msgs:
        return None
    return (
        f":rotating_light: *ads.txt contract breach on {site}* "
        f"(file sha `{report.get('sha', '?')}`):\n" + "\n".join(msgs) +
        "\n*Action:* fix the published ads.txt and reconcile with "
        f"pgam-wrapper/configs/ads.txt.{site}."
    )


def _format_p2(report: dict) -> str | None:
    if not report.get("fetch_error"):
        return None
    return (
        f":warning: *ads.txt unreachable on {report['site']}* — "
        f"`{report['fetch_error']}`. Site may be down or ads.txt removed."
    )


def _format_p3(report: dict) -> str | None:
    extras = report.get("unexpected_direct") or []
    if not extras:
        return None
    lines = "\n".join(f"• `{d}, {a}, {r}`" for (d, a, r) in extras)
    return (
        f":information_source: *Unexpected PGAM-domain DIRECT entry in {report['site']}/ads.txt* — "
        f"someone added a seat without updating pgam-wrapper source-of-truth:\n{lines}"
    )


# ── Entry points ─────────────────────────────────────────────────────────────


def scan() -> dict:
    """Scan every site in REQUIRED_ADSTXT — return per-site reports."""
    snapshots = _load_snapshots()
    out_reports: list[dict] = []
    for site, required in REQUIRED_ADSTXT.items():
        report = _check_site(site, required)

        p1 = _format_p1(report)
        p2 = _format_p2(report)
        p3 = _format_p3(report)

        # P1 + P2 always alert (no dedup — always-pageable).
        # P3 dedups per-day-per-site so a stable unexpected entry doesn't
        # spam every run; if it's still there tomorrow we re-page.
        try:
            if p1:
                slack.send_text(p1)
            if p2:
                slack.send_text(p2)
            if p3:
                key = f"adstxt_monitor:{site}:p3"
                if not slack.already_sent_today(key):
                    slack.send_text(p3)
                    slack.mark_sent(key)
        except Exception as e:
            print(f"[{ACTOR}] Slack post failed for {report['site']}: {e}")

        # Update snapshot if the fetch succeeded.
        if report.get("sha"):
            prev = snapshots.get(report["site"], {})
            snapshots[report["site"]] = {
                "sha": report["sha"],
                "entry_count": report["entry_count"],
                "prev_sha": prev.get("sha") if prev.get("sha") != report["sha"] else prev.get("prev_sha"),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            }

        print(
            f"[{ACTOR}] {report['site']}: status={report.get('status')} "
            f"missing={len(report.get('missing', []))} "
            f"wrong_rel={len(report.get('wrong_relationship', []))} "
            f"unexpected_direct={len(report.get('unexpected_direct', []))}"
        )
        out_reports.append(report)

    _save_snapshots(snapshots)
    return {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "sites_scanned": len(REQUIRED_ADSTXT),
        "reports": out_reports,
    }


def run() -> dict:
    """Scheduler entry."""
    return scan()


if __name__ == "__main__":
    result = run()
    print(json.dumps(result, indent=2, default=str))
