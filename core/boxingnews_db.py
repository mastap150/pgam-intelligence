"""
core/boxingnews_db.py — Postgres connection helper for the boxingnews
production Neon DB (separate project from pgam_direct).

The boxingnews app writes articles, msn_title, msn_title_variants, tags,
sanity_id, etc. into its own dedicated Neon project. The weekly review
agent needs to JOIN that data against pgam_direct.msn_article_peak (MSN
reads) — so this helper exposes a read-only connection.

Env var: BOXINGNEWS_DATABASE_URL  (kept distinct from PGAM_DIRECT_DATABASE_URL
to avoid an accidental write to boxingnews from a pgam-intelligence agent).

Read-only: the agent never writes back to boxingnews. If/when that
changes, add explicit WRITE-permission docs here and a separate
connect_write() helper.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)


def _resolve_dsn() -> str:
    dsn = os.environ.get("BOXINGNEWS_DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "Boxingnews DSN not configured: set BOXINGNEWS_DATABASE_URL "
            "in .env to the boxingnews Neon connection string "
            "(see boxingnews/.env.local DATABASE_URL)."
        )
    return dsn


def connect() -> psycopg.Connection:
    """Open a new psycopg connection to the boxingnews Neon DB.
    Caller is responsible for closing. Read-only by convention."""
    return psycopg.connect(_resolve_dsn(), autocommit=True)
