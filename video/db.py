"""
video/db.py — Postgres connection for the dve schema.

Mirrors core/neon.py. DVE_DATABASE_URL preferred so the video engine can move
to its own database without code changes; falls back to the shared
PGAM_DIRECT_DATABASE_URL / DATABASE_URL DSN (same Neon project, dve schema).

Note for cloud sessions: Neon:5432 egress is blocked there (see CLAUDE.md) —
use the default JSON store instead. This module is for the Render worker and
local runs.
"""

import os

import psycopg
from dotenv import load_dotenv

load_dotenv(override=True)


def _resolve_dsn() -> str:
    dsn = (
        os.environ.get("DVE_DATABASE_URL")
        or os.environ.get("PGAM_DIRECT_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError(
            "DVE Postgres DSN not configured: set DVE_DATABASE_URL "
            "(or PGAM_DIRECT_DATABASE_URL) — or leave DVE_STORE=json"
        )
    return dsn


def connect() -> psycopg.Connection:
    """Open a new psycopg connection. Caller is responsible for closing."""
    return psycopg.connect(_resolve_dsn(), autocommit=False)
