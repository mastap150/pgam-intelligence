"""
core/dsp_neon.py — Postgres connection helper for the pgam-dsp-dashboard
Neon DB. This is the DSP demand-side DB (ss_campaigns,
ss_campaign_margin_events, buyer_agent_actions, etc.).

Distinct from `core/neon.py` which targets the SSP (pgam_direct) DB.

Connection string lives in DSP_DATABASE_URL. Both Neon DBs live in the
same Neon project (round-frog-99233431) but different schemas.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)


def _resolve_dsn() -> str:
    dsn = os.environ.get("DSP_DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DSP Neon DSN not configured: set DSP_DATABASE_URL in .env to "
            "the pgam-dsp-dashboard Neon connection string"
        )
    return dsn


def connect() -> psycopg.Connection:
    """Open a new psycopg connection to DSP Neon. Caller closes."""
    return psycopg.connect(_resolve_dsn(), autocommit=False)
