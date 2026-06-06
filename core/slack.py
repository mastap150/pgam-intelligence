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


# ---------------------------------------------------------------------------
# Shared (Neon-backed) deduplication
# ---------------------------------------------------------------------------
#
# The /tmp/pgam_alert_state.json mechanism above is PROCESS-LOCAL — fine for
# any single agent firing only from one host. But for agents that may fire
# from MULTIPLE places on the same day (e.g. a manual ad-hoc invocation from
# a laptop AND a scheduled run on Render), the per-host state file means
# each host thinks the alert hasn't gone out yet → duplicate Slack posts.
#
# Observed 2026-06-06: manual fallback fire at 10:14 ET from a dev laptop +
# scheduled fallback at 10:30 ET on Render → two identical messages in
# #compliance because they read different /tmp files.
#
# These two functions store dedup state in Neon (`compliance_alert_state`
# table, created lazily on first call) so all processes share visibility.
# Existing callers stay on the file-based functions; only opt-in callers
# that need cross-host dedup use the *_shared variants.

_DEDUP_TABLE_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS pgam_direct.compliance_alert_state (
    dedup_key   TEXT NOT NULL,
    as_of       DATE NOT NULL,
    marked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dedup_key, as_of)
);
"""


def _shared_ensure_table():
    """Idempotent — create the dedup table if missing. Safe to call repeatedly."""
    try:
        from core.neon import connect
        with connect() as conn, conn.cursor() as cur:
            cur.execute(_DEDUP_TABLE_CREATE_SQL)
            conn.commit()
    except Exception as exc:
        print(f"[slack] dedup table ensure failed (non-fatal): {exc}")


def already_sent_today_shared(key):
    """Cross-host variant of already_sent_today. Reads from Neon."""
    try:
        from core.neon import connect
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pgam_direct.compliance_alert_state "
                "WHERE dedup_key = %s AND as_of = current_date LIMIT 1",
                (key,))
            return cur.fetchone() is not None
    except Exception as exc:
        # Table missing on first call → ensure + treat as not-sent.
        _shared_ensure_table()
        return False


def mark_sent_shared(key):
    """Cross-host variant of mark_sent. Writes to Neon, idempotent via PK."""
    _shared_ensure_table()
    try:
        from core.neon import connect
        with connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO pgam_direct.compliance_alert_state (dedup_key, as_of) "
                "VALUES (%s, current_date) ON CONFLICT DO NOTHING",
                (key,))
            conn.commit()
    except Exception as exc:
        print(f"[slack] mark_sent_shared failed (non-fatal): {exc}")
