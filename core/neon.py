"""
core/neon.py — Postgres connection helper for the pgam_direct Neon DB.

The Partner Revenue Dashboard lives in pgam-direct/web (admin.pgammedia.com)
and reads from `pgam_direct.financial_events` (TB) plus a sister table
`pgam_direct.ll_daily_partner_revenue` that this repo's
agents/etl/partner_revenue_etl.py writes hourly. This helper is the single
point of Postgres access for that ETL.

Connection string lives in PGAM_DIRECT_DATABASE_URL (preferred). Falls back
to DATABASE_URL for local convenience. Both should point at the same Neon
DB used by pgam-direct/web.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)


def _resolve_dsn() -> str:
    dsn = (
        os.environ.get("PGAM_DIRECT_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "Neon DSN not configured: set PGAM_DIRECT_DATABASE_URL "
            "(or DATABASE_URL) in .env to the pgam-direct Neon connection string"
        )
    return dsn


def connect() -> psycopg.Connection:
    """Open a new psycopg connection. Caller is responsible for closing."""
    return psycopg.connect(_resolve_dsn(), autocommit=False)
