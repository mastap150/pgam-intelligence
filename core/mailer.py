"""Outbound email, over SendGrid or Resend, behind one interface.

Why this exists
---------------
Three modules each hand-rolled their own SendGrid POST against
https://api.sendgrid.com/v3/mail/send:

    agents/reports/daily_email.py              (the 7am ET report)
    agents/reports/partner_scheduled_reports.py (ZIP attachment)
    agents/recon/payment_matcher.py

Three copies of the same auth, payload shape, and error handling, and no
single place to change provider, add a dry-run, or fix a bug. This is that
place. It also makes SendGrid -> Resend a one-variable change rather than a
three-file edit.

Nothing is migrated onto this yet — the call sites still work as they did.
Adopt it one at a time, starting with the two small ones; daily_email.py is
the 7am report and deserves its own change.

Configuration
-------------
    EMAIL_PROVIDER      "sendgrid" (default) or "resend"
    SENDGRID_KEY        required when provider is sendgrid
    RESEND_API_KEY      required when provider is resend
    EMAIL_FROM          default sender
    EMAIL_TO            default recipients, comma-separated
    PGAM_EMAIL_DRY_RUN  "1" to log the send and deliver nothing

The default is sendgrid precisely so that importing this module changes no
behavior. Point EMAIL_PROVIDER at resend only once a Resend domain is
verified — an unverified sender is accepted by the API and then silently
never delivered.
"""

from __future__ import annotations

import base64
import os
from typing import Iterable, List, Sequence

import requests

SENDGRID_URL = "https://api.sendgrid.com/v3/mail/send"
RESEND_URL = "https://api.resend.com/emails"

DEFAULT_TIMEOUT = 30


class MailerError(RuntimeError):
    """Configuration or delivery failure. Never carries the API key."""


def _recipients(to: Sequence[str] | str | None) -> List[str]:
    if to is None:
        to = os.environ.get("EMAIL_TO", "")
    if isinstance(to, str):
        to = to.split(",")
    out = [addr.strip() for addr in to if addr and addr.strip()]
    if not out:
        raise MailerError("no recipients (pass to=... or set EMAIL_TO)")
    return out


def _encode_attachments(attachments: Iterable[dict] | None) -> List[dict]:
    """Normalise to [{filename, content(bytes|str), type}].

    Accepts raw bytes and base64-encodes them, so callers do not each repeat
    that step (partner_scheduled_reports.py currently does its own).
    """
    out = []
    for att in attachments or []:
        content = att.get("content", b"")
        if isinstance(content, bytes):
            content = base64.b64encode(content).decode("ascii")
        out.append({
            "filename": att.get("filename", "attachment"),
            "content": content,
            "type": att.get("type", "application/octet-stream"),
        })
    return out


def _send_sendgrid(key, sender, to, subject, html, attachments) -> bool:
    payload = {
        "personalizations": [{"to": [{"email": r} for r in to]}],
        "from": {"email": sender},
        "subject": subject,
        "content": [{"type": "text/html", "value": html}],
    }
    if attachments:
        payload["attachments"] = [{
            "content": a["content"],
            "filename": a["filename"],
            "type": a["type"],
            "disposition": "attachment",
        } for a in attachments]

    resp = requests.post(
        SENDGRID_URL,
        json=payload,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        timeout=DEFAULT_TIMEOUT,
    )
    if resp.status_code in (200, 202):
        return True
    raise MailerError(f"SendGrid returned {resp.status_code}: {resp.text[:300]}")


def _send_resend(key, sender, to, subject, html, attachments) -> bool:
    payload = {"from": sender, "to": to, "subject": subject, "html": html}
    if attachments:
        # Resend takes base64 in `content` too, but names the MIME field
        # differently and infers it from the filename when omitted.
        payload["attachments"] = [{
            "filename": a["filename"],
            "content": a["content"],
        } for a in attachments]

    resp = requests.post(
        RESEND_URL,
        json=payload,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"},
        timeout=DEFAULT_TIMEOUT,
    )
    if resp.status_code in (200, 201, 202):
        return True
    raise MailerError(f"Resend returned {resp.status_code}: {resp.text[:300]}")


def send(
    subject: str,
    html: str,
    to: Sequence[str] | str | None = None,
    sender: str | None = None,
    attachments: Iterable[dict] | None = None,
    provider: str | None = None,
    dry_run: bool | None = None,
) -> bool:
    """Send one HTML email. Returns True on success, raises MailerError otherwise.

    Callers that must not fail a job on a delivery problem should catch
    MailerError — the previous inline implementations swallowed everything and
    returned False, which is how a broken send can go unnoticed for weeks.
    """
    provider = (provider or os.environ.get("EMAIL_PROVIDER") or "sendgrid").lower()
    sender = sender or os.environ.get("EMAIL_FROM", "")
    if not sender:
        raise MailerError("no sender (pass sender=... or set EMAIL_FROM)")

    rcpts = _recipients(to)
    atts = _encode_attachments(attachments)

    if dry_run is None:
        dry_run = os.environ.get("PGAM_EMAIL_DRY_RUN") == "1"
    if dry_run:
        print(f"[mailer] DRY RUN via {provider}: {subject!r} -> "
              f"{len(rcpts)} recipient(s), {len(atts)} attachment(s)")
        return True

    if provider == "sendgrid":
        key = os.environ.get("SENDGRID_KEY", "")
        if not key:
            raise MailerError("SENDGRID_KEY is not set")
        return _send_sendgrid(key, sender, rcpts, subject, html, atts)

    if provider == "resend":
        key = os.environ.get("RESEND_API_KEY", "")
        if not key:
            raise MailerError("RESEND_API_KEY is not set")
        return _send_resend(key, sender, rcpts, subject, html, atts)

    raise MailerError(f"unknown EMAIL_PROVIDER {provider!r}; "
                      f"expected 'sendgrid' or 'resend'")
