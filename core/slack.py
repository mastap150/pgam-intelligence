import json
import os
import requests
from datetime import date

from core.config import SLACK_WEBHOOK

STATE_FILE = "/tmp/pgam_alert_state.json"


# ---------------------------------------------------------------------------
# Core delivery
# ---------------------------------------------------------------------------

def send(payload):
    """
    POST a raw JSON payload dict to the Slack webhook.

    Returns the requests.Response object, or None if no webhook is configured.
    """
    if not SLACK_WEBHOOK:
        print("[slack] No SLACK_WEBHOOK configured — skipping.")
        return None

    response = requests.post(
        SLACK_WEBHOOK,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    return response


def send_text(message):
    """Send a plain-text message to Slack."""
    return send({"text": message})


def send_blocks(blocks, text=""):
    """
    Send a Block Kit message to Slack.

    Args:
        blocks (list): Slack Block Kit block objects.
        text   (str):  Fallback plain-text for notifications / accessibility.
    """
    return send({"text": text, "blocks": blocks})


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _load_state():
    """Load the alert state file, returning an empty dict if missing or corrupt."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(state):
    """Persist the alert state dict to disk."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def already_sent_today(key):
    """
    Return True if an alert with this key has already been sent today.

    State is stored as:
        { "YYYY-MM-DD": ["key1", "key2", ...] }
    """
    today = date.today().isoformat()
    state = _load_state()
    return key in state.get(today, [])


def mark_sent(key):
    """
    Record that an alert with this key was sent today.

    Clears entries from previous days to keep the file small.
    """
    today = date.today().isoformat()
    state = _load_state()

    # Drop stale dates
    state = {k: v for k, v in state.items() if k == today}

    sent_today = state.setdefault(today, [])
    if key not in sent_today:
        sent_today.append(key)

    _save_state(state)
