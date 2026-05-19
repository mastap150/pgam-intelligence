#!/usr/bin/env python3
"""
scripts/session_backup_restore.py

Backup + restore the authenticated MSN Partner Hub Playwright session
into Neon (pgam_direct.puller_session_chunk).

Chunking: 18MB total as a single BYTEA exceeded Neon's per-statement
size on our network path (timed out at ~60s). Splitting into 100KB
chunks (~180 chunks for an 18MB session) lets each INSERT complete
in well under a second.

Usage
-----
    # Back up local session to Neon (run after first auth + as needed):
    python3 scripts/session_backup_restore.py backup

    # Restore Neon session to ~/.pgam/msn-session/ (run on GH Actions
    # before invoking the puller):
    python3 scripts/session_backup_restore.py restore

Env
---
    PGAM_DIRECT_DATABASE_URL    Neon DSN (required)
    MSN_SESSION_DIR             override (default ~/.pgam/msn-session)
    PULLER_SESSION_ID           Neon row id (default 'msn-partner-hub-boxingnews')
"""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DEFAULT_SESSION_ID = "msn-partner-hub-boxingnews"
CHUNK_SIZE = 100 * 1024  # 100KB per row — well within any pooler limit


def _session_dir() -> Path:
    override = os.environ.get("MSN_SESSION_DIR")
    return Path(override).expanduser().resolve() if override else (
        Path.home() / ".pgam" / "msn-session"
    ).resolve()


def _connect():
    try:
        import psycopg
    except ImportError:
        print("psycopg not installed. pip install psycopg[binary]", file=sys.stderr)
        sys.exit(2)
    dsn = os.environ.get("PGAM_DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        print("PGAM_DIRECT_DATABASE_URL (or DATABASE_URL) must be set", file=sys.stderr)
        sys.exit(2)
    # Direct host (skip pooler) + TCP keepalive for the longer-than-typical
    # connection lifetime that chunked uploads hold.
    direct_dsn = dsn.replace("-pooler.", ".")
    return psycopg.connect(
        direct_dsn,
        autocommit=False,
        connect_timeout=20,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )


def _session_id(args) -> str:
    return args.id or os.environ.get("PULLER_SESSION_ID") or DEFAULT_SESSION_ID


def _machine_label() -> str:
    user = "unknown"
    try:
        user = os.getlogin()
    except (OSError, AttributeError):
        user = os.environ.get("USER", "unknown")
    return f"{socket.gethostname()}:{user}"


def cmd_backup(args) -> int:
    src = _session_dir()
    if not src.exists():
        print(f"session dir not found at {src}", file=sys.stderr)
        return 2

    sid = _session_id(args)
    print(f"[backup] tarring {src} ...")
    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tar_path = tmp.name
    try:
        subprocess.run(
            ["tar", "-czf", tar_path, "-C", str(src.parent), src.name],
            check=True,
        )
        size = os.path.getsize(tar_path)
        chunks = [(i, _read_chunk(tar_path, i))
                  for i in range(0, (size + CHUNK_SIZE - 1) // CHUNK_SIZE)]
        total_chunks = len(chunks)
        print(f"[backup] tarball: {size / 1024 / 1024:.1f} MB → {total_chunks} chunks of ≤{CHUNK_SIZE // 1024}KB")

        conn = _connect()
        try:
            with conn:
                with conn.cursor() as cur:
                    # Clear any stale chunks for this id, then bulk-insert.
                    cur.execute(
                        "DELETE FROM pgam_direct.puller_session_chunk WHERE id = %s",
                        (sid,),
                    )
                    label = _machine_label()
                    for ord_, data in chunks:
                        cur.execute(
                            """
                            INSERT INTO pgam_direct.puller_session_chunk
                              (id, ord, chunk, total_chunks)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (sid, ord_, data, total_chunks),
                        )
                        if ord_ % 20 == 0 or ord_ == total_chunks - 1:
                            print(f"[backup]   chunk {ord_+1}/{total_chunks}")
                    # Also update the legacy single-blob row so older
                    # restore paths don't lose track. Soft-fail if it's
                    # too big — we have the chunks.
                    try:
                        with open(tar_path, "rb") as f:
                            blob = f.read()
                        cur.execute(
                            """
                            INSERT INTO pgam_direct.puller_session
                              (id, session_blob, updated_at, updated_by)
                            VALUES (%s, %s, NOW(), %s)
                            ON CONFLICT (id) DO UPDATE SET
                              session_blob = EXCLUDED.session_blob,
                              updated_at = NOW(),
                              updated_by = EXCLUDED.updated_by
                            """,
                            (sid, blob, label),
                        )
                    except Exception as exc:
                        # That's OK — chunks are the source of truth.
                        print(f"[backup] legacy single-blob update skipped: {exc}")
            print(f"[backup] uploaded {size/1024/1024:.1f} MB to Neon as id={sid!r} by={label!r}")
            return 0
        finally:
            conn.close()
    finally:
        try:
            os.unlink(tar_path)
        except Exception:
            pass


def _read_chunk(path: str, ord_: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(ord_ * CHUNK_SIZE)
        return f.read(CHUNK_SIZE)


def cmd_restore(args) -> int:
    sid = _session_id(args)
    dest = _session_dir()
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(f"[restore] fetching id={sid!r} from Neon (chunked) ...")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ord, chunk, total_chunks
                  FROM pgam_direct.puller_session_chunk
                 WHERE id = %s
                 ORDER BY ord ASC
                """,
                (sid,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        print(f"[restore] no chunks in Neon for id={sid!r}", file=sys.stderr)
        return 3

    total = rows[0][2]
    if len(rows) != total:
        print(
            f"[restore] chunk count mismatch — got {len(rows)} of expected {total} — refusing partial restore",
            file=sys.stderr,
        )
        return 5

    blob_bytes = b"".join(r[1] for r in rows)
    print(f"[restore] got {len(blob_bytes)/1024/1024:.1f} MB across {total} chunks")

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tar_path = tmp.name
        tmp.write(blob_bytes)
    staging = Path(tempfile.mkdtemp(prefix="msn-session-restore-"))
    try:
        subprocess.run(["tar", "-xzf", tar_path, "-C", str(staging)], check=True)
        extracted = staging / "msn-session"
        if not extracted.exists():
            print(f"[restore] tarball didn't contain msn-session/ — got {list(staging.iterdir())}", file=sys.stderr)
            return 4
        backup_aside = None
        if dest.exists():
            backup_aside = dest.with_suffix(".old-" + str(int(time.time())))
            os.rename(str(dest), str(backup_aside))
        try:
            shutil.move(str(extracted), str(dest))
        except Exception:
            if backup_aside is not None and backup_aside.exists():
                os.rename(str(backup_aside), str(dest))
            raise
        if backup_aside is not None:
            shutil.rmtree(backup_aside, ignore_errors=True)
        print(f"[restore] session restored to {dest}")
        return 0
    finally:
        try:
            os.unlink(tar_path)
        except Exception:
            pass
        shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    p = argparse.ArgumentParser(description="MSN Playwright session backup/restore to Neon (chunked)")
    p.add_argument("--id", help=f"row id (default {DEFAULT_SESSION_ID})")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("backup", help="tar local session + chunk + upload to Neon")
    sub.add_parser("restore", help="download chunks from Neon + reassemble + untar")
    args = p.parse_args()
    if args.cmd == "backup":
        return cmd_backup(args)
    if args.cmd == "restore":
        return cmd_restore(args)
    return 1


if __name__ == "__main__":
    sys.exit(main())
