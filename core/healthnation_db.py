"""
core/healthnation_db.py — Postgres connection helper for healthnation.com.

The healthnation-web Next.js app (repo: mastap150/healthnation-web) writes
articles, buyer_guides, products, product_reviews and affiliate_clicks into
the `healthnation` schema. Per that repo's migrations/0001 header the schema
lives inside the shared Neon project so DSP (public) and SSP (pgam_direct)
tables stay isolated from it.

Env var: HEALTHNATION_DATABASE_URL — kept distinct from
PGAM_DIRECT_DATABASE_URL so a pgam-intelligence agent cannot accidentally
write to healthnation. Copy the value from healthnation-web's .env.local
DATABASE_URL (or the Vercel project env).

Read-only: nothing in this repo writes to healthnation. If that ever
changes, add a separate connect_write() helper and document why.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)


def _resolve_dsn() -> str:
    dsn = os.environ.get("HEALTHNATION_DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "HealthNation DSN not configured: set HEALTHNATION_DATABASE_URL "
            "in .env to the healthnation Neon connection string "
            "(see healthnation-web/.env.local DATABASE_URL, or the "
            "healthnation-web project env in Vercel)."
        )
    return dsn


def connect() -> psycopg.Connection:
    """Open a new psycopg connection to the healthnation Neon DB.
    Caller is responsible for closing. Read-only by convention."""
    return psycopg.connect(_resolve_dsn(), autocommit=True)
