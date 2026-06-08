"""
agents/outbound/sdr_agent.py
────────────────────────────
Daily SDR lead-loader for PGAM's outbound motion.

WHY THIS EXISTS
───────────────
Priyesh is the entire commercial org. Founder-led outbound caps the DSP
pipeline at whatever Priyesh can hand-do in a week, which is roughly
zero once any other work hits. This agent decouples top-of-funnel
volume from founder time: Apollo finds the leads, HubSpot keeps the
pipeline, Instantly sends from a non-Priyesh persona ("Jordan Reilly"),
and Priyesh is only in the loop on replies.

ARCHITECTURE
────────────
    ┌──────────┐  search   ┌──────────────┐  dedupe   ┌──────────────┐
    │  Apollo  │ ────────▶ │  this agent  │ ────────▶ │   HubSpot    │
    └──────────┘           └──────────────┘           │ pipeline     │
                                  │                   │  899621236   │
                                  │ push net-new      └──────────────┘
                                  ▼
                           ┌──────────────┐
                           │   Instantly  │  ──▶ Jordan Reilly inboxes
                           │   campaign   │       (Instantly handles
                           └──────────────┘        sending + cadence)

Replies are NOT handled in this file. Instantly is configured to
forward classified replies (interested / OOO / unsubscribe) into a
shared inbox Priyesh monitors. A separate reply-classifier agent is
planned for v2 (see README.md).

SAFETY POSTURE
──────────────
Defaults to SDR_DRY_RUN=true. Nothing is created in HubSpot or
Instantly until the env var is flipped to "false". The dry-run mode
still calls Apollo (read-only) and prints what it would have done.

A daily cap per segment (SDR_DAILY_CAP_PER_SEGMENT, default 25) keeps
Instantly inbox warmth healthy — we add new leads slowly enough that
sends-per-inbox stays under ~30/day in aggregate across active
sequences.

If any single API call fails, the agent logs and continues. Failures
are bounded per-lead, never global, so a single bad row never kills
the run.

SCHEDULING
──────────
Registered in scheduler.py for daily 09:00 ET.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

import pytz

from agents.outbound.icp import ICP_SEGMENTS
from core.slack import send_text


ET = pytz.timezone("US/Eastern")


# ─────────────────────────────────────────────────────────────────────
# Env
# ─────────────────────────────────────────────────────────────────────
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "").strip()
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_ACCESS_TOKEN", "").strip()
HUBSPOT_PIPELINE_ID = os.environ.get("HUBSPOT_PIPELINE_ID", "899621236").strip()
# First stage in the pipeline. Override per HubSpot setup.
HUBSPOT_DEAL_STAGE = os.environ.get("HUBSPOT_DEAL_STAGE_NEW", "").strip()
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY", "").strip()

DRY_RUN = os.environ.get("SDR_DRY_RUN", "true").lower() != "false"
DAILY_CAP_PER_SEGMENT = int(
    os.environ.get("SDR_DAILY_CAP_PER_SEGMENT", "25")
)

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/search"
HUBSPOT_BASE = "https://api.hubapi.com"
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"


# ─────────────────────────────────────────────────────────────────────
# Tiny HTTP helper
# ─────────────────────────────────────────────────────────────────────
def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout: int = 30,
) -> tuple[int, dict[str, Any] | None]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return e.code, {"_raw": raw}
    except urllib.error.URLError as e:
        return 0, {"_error": str(e)}


# ─────────────────────────────────────────────────────────────────────
# Apollo
# ─────────────────────────────────────────────────────────────────────
def _apollo_search(filt: dict[str, Any]) -> list[dict[str, Any]]:
    if not APOLLO_API_KEY:
        print("[sdr_agent] APOLLO_API_KEY missing — skipping Apollo search")
        return []
    body = dict(filt)
    body.setdefault("page", 1)
    status, payload = _request(
        "POST",
        APOLLO_SEARCH_URL,
        headers={"X-Api-Key": APOLLO_API_KEY},
        body=body,
    )
    if status != 200 or not payload:
        print(f"[sdr_agent] apollo search failed status={status} payload={payload}")
        return []
    return payload.get("people", []) or []


# ─────────────────────────────────────────────────────────────────────
# HubSpot
# ─────────────────────────────────────────────────────────────────────
def _hubspot_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}


def _hubspot_find_contact_by_email(email: str) -> str | None:
    """Return contactId if a contact with this email exists, else None."""
    if not email or not HUBSPOT_TOKEN:
        return None
    status, payload = _request(
        "POST",
        f"{HUBSPOT_BASE}/crm/v3/objects/contacts/search",
        headers=_hubspot_headers(),
        body={
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "email",
                            "operator": "EQ",
                            "value": email.lower(),
                        }
                    ]
                }
            ],
            "limit": 1,
            "properties": ["email"],
        },
    )
    if status != 200 or not payload:
        return None
    results = payload.get("results") or []
    return results[0]["id"] if results else None


def _hubspot_create_contact(lead: dict[str, Any]) -> str | None:
    if DRY_RUN or not HUBSPOT_TOKEN:
        return None
    org = lead.get("organization") or {}
    props = {
        "email": (lead.get("email") or "").lower(),
        "firstname": lead.get("first_name") or "",
        "lastname": lead.get("last_name") or "",
        "jobtitle": lead.get("title") or "",
        "company": org.get("name") or "",
        "website": org.get("website_url") or "",
        "pgam_outbound_source": "apollo",
        "pgam_outbound_persona": "jordan_reilly",
    }
    status, payload = _request(
        "POST",
        f"{HUBSPOT_BASE}/crm/v3/objects/contacts",
        headers=_hubspot_headers(),
        body={"properties": props},
    )
    if status not in (200, 201) or not payload:
        print(f"[sdr_agent] hubspot create contact failed: {status} {payload}")
        return None
    return payload.get("id")


def _hubspot_create_deal(
    contact_id: str, lead: dict[str, Any], segment: dict[str, Any]
) -> str | None:
    if DRY_RUN or not HUBSPOT_TOKEN or not contact_id:
        return None
    org = lead.get("organization") or {}
    deal_name = (
        f"{segment['deal_label_prefix']} "
        f"{org.get('name') or lead.get('email') or 'unknown'}"
    )
    props: dict[str, Any] = {
        "dealname": deal_name,
        "pipeline": HUBSPOT_PIPELINE_ID,
        "pgam_outbound_segment": segment["label"],
        "pgam_outbound_persona": "jordan_reilly",
    }
    if HUBSPOT_DEAL_STAGE:
        props["dealstage"] = HUBSPOT_DEAL_STAGE
    status, payload = _request(
        "POST",
        f"{HUBSPOT_BASE}/crm/v3/objects/deals",
        headers=_hubspot_headers(),
        body={
            "properties": props,
            "associations": [
                {
                    "to": {"id": contact_id},
                    "types": [
                        {
                            # contact_to_deal = 3
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 3,
                        }
                    ],
                }
            ],
        },
    )
    if status not in (200, 201) or not payload:
        print(f"[sdr_agent] hubspot create deal failed: {status} {payload}")
        return None
    return payload.get("id")


# ─────────────────────────────────────────────────────────────────────
# Instantly
# ─────────────────────────────────────────────────────────────────────
def _instantly_add_lead(
    campaign_id: str, lead: dict[str, Any], segment: dict[str, Any]
) -> bool:
    if DRY_RUN or not INSTANTLY_API_KEY or not campaign_id:
        return False
    org = lead.get("organization") or {}
    body = {
        "campaign": campaign_id,
        "email": (lead.get("email") or "").lower(),
        "first_name": lead.get("first_name") or "",
        "last_name": lead.get("last_name") or "",
        "company_name": org.get("name") or "",
        "personalization": "",
        "custom_variables": {
            "title": lead.get("title") or "",
            "vertical": (org.get("industry") or "").strip(),
            "segment": segment["label"],
        },
    }
    status, payload = _request(
        "POST",
        f"{INSTANTLY_BASE}/leads",
        headers={"Authorization": f"Bearer {INSTANTLY_API_KEY}"},
        body=body,
    )
    if status not in (200, 201):
        print(f"[sdr_agent] instantly add lead failed: {status} {payload}")
        return False
    return True


# ─────────────────────────────────────────────────────────────────────
# Per-segment runner
# ─────────────────────────────────────────────────────────────────────
def _process_segment(segment: dict[str, Any]) -> dict[str, int]:
    metrics = {
        "candidates_seen": 0,
        "skipped_no_email": 0,
        "skipped_existing_hubspot": 0,
        "hubspot_contacts_created": 0,
        "hubspot_deals_created": 0,
        "instantly_pushed": 0,
        "errors": 0,
    }

    campaign_id = os.environ.get(segment["instantly_campaign_env"], "").strip()
    if not campaign_id and not DRY_RUN:
        print(
            f"[sdr_agent] {segment['label']} — "
            f"{segment['instantly_campaign_env']} not set; skipping live push"
        )

    candidates = _apollo_search(segment["apollo_filter"])
    metrics["candidates_seen"] = len(candidates)

    pushed = 0
    for lead in candidates:
        if pushed >= DAILY_CAP_PER_SEGMENT:
            break

        email = (lead.get("email") or "").strip().lower()
        if not email or "@" not in email:
            metrics["skipped_no_email"] += 1
            continue

        try:
            existing = _hubspot_find_contact_by_email(email)
            if existing:
                metrics["skipped_existing_hubspot"] += 1
                continue

            contact_id = _hubspot_create_contact(lead)
            if contact_id:
                metrics["hubspot_contacts_created"] += 1
                deal_id = _hubspot_create_deal(contact_id, lead, segment)
                if deal_id:
                    metrics["hubspot_deals_created"] += 1

            if _instantly_add_lead(campaign_id, lead, segment):
                metrics["instantly_pushed"] += 1
                pushed += 1
            elif DRY_RUN:
                # In dry-run, count "would have pushed" toward pushed so the
                # cap behaves the same as live mode for reporting purposes.
                pushed += 1

            # Be polite to APIs — small spacing keeps us well under rate limits.
            time.sleep(0.25)
        except Exception as e:  # noqa: BLE001 — agent must never crash the scheduler
            metrics["errors"] += 1
            print(f"[sdr_agent] lead error email={email}: {e}")

    return metrics


# ─────────────────────────────────────────────────────────────────────
# Public entrypoint
# ─────────────────────────────────────────────────────────────────────
def run() -> None:
    now = datetime.now(ET).strftime("%Y-%m-%d %H:%M ET")
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    print(f"[sdr_agent] ▶ {now} mode={mode}")

    if not APOLLO_API_KEY:
        send_text(
            "*SDR agent skipped* — APOLLO_API_KEY not set."
        )
        return

    lines = [f"*SDR agent* — daily run ({mode})"]
    totals = {
        "candidates_seen": 0,
        "skipped_no_email": 0,
        "skipped_existing_hubspot": 0,
        "hubspot_contacts_created": 0,
        "hubspot_deals_created": 0,
        "instantly_pushed": 0,
        "errors": 0,
    }
    for segment in ICP_SEGMENTS:
        m = _process_segment(segment)
        for k, v in m.items():
            totals[k] += v
        lines.append(
            f"• `{segment['label']}` — seen={m['candidates_seen']}  "
            f"pushed={m['instantly_pushed']}  "
            f"new_hs_contacts={m['hubspot_contacts_created']}  "
            f"dedup_existing={m['skipped_existing_hubspot']}  "
            f"errors={m['errors']}"
        )

    lines.append(
        f"\n*Totals* — pushed `{totals['instantly_pushed']}`  "
        f"new HubSpot contacts `{totals['hubspot_contacts_created']}`  "
        f"deals `{totals['hubspot_deals_created']}`  "
        f"errors `{totals['errors']}`"
    )

    send_text("\n".join(lines))
    print(f"[sdr_agent] ✔ done — {totals}")
