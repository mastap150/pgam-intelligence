"""
video/store.py — persistence for the video engine.

One contract, two backends (see the architecture doc for why):

  JsonFileStore  — default. One JSON file per record under
                   DVE_DATA_DIR/store/<table>/<id>.json. Zero-infra, works in
                   cloud sessions and CI where Neon:5432 is unreachable.
  PostgresStore  — the dve schema (migrations/2026_09_05_...). Scalar columns
                   for everything queried, full record in payload JSONB.

Application code uses get/put/find only; nothing above this file knows which
backend is active.
"""

import json
import os
from pathlib import Path
from typing import Callable, Iterable

from video import settings
from video.models import now_iso

# Table name -> scalar columns promoted out of the payload in Postgres.
# Must stay in sync with the migration.
TABLES: dict[str, list[str]] = {
    "content_sources": ["source_type", "source_url", "destination", "country",
                        "theme", "publish_date", "freshness_score"],
    "opportunities":   ["source_id", "score", "manual_boost", "destination", "franchise"],
    "concepts":        ["source_id", "franchise", "destination", "working_title",
                        "confidence_score", "editorial_score", "originality_score",
                        "predicted_performance_score"],
    "hooks":           ["concept_id", "category", "text"],
    "scripts":         ["concept_id", "hook_id"],
    "assets":          ["asset_type", "destination", "country", "orientation",
                        "source_tier", "license_type", "license_end",
                        "rights_verified", "ai_generated", "usage_count", "quality_score"],
    "fare_deals":      ["origin", "destination", "fare", "currency", "cabin",
                        "discount_percentage", "interest_score", "expiration"],
    "videos":          ["concept_id", "script_id", "hook_id", "franchise",
                        "destination", "format", "duration_seconds", "voice_id",
                        "qa_result", "predicted_score", "video_score"],
    "render_jobs":     ["video_id"],
    "qa_results":      ["video_id", "verdict"],
    "approval_events": ["video_id", "action", "actor"],
    "publishing_jobs": ["video_id", "platform", "external_id", "publish_time"],
    "performance_snapshots": ["video_id", "platform", "snapshot_label"],
    "comments":        ["video_id", "classification"],
    "recommendations": ["kind"],
    "experiments":     ["variable"],
    "voices":          ["name", "provider"],
    "attribution_events": ["video_id", "event_type"],
    "settings_audit":  ["actor"],
}

Predicate = Callable[[dict], bool]


class JsonFileStore:
    def __init__(self, root: Path | None = None):
        self.root = Path(root) if root else settings.DATA_DIR / "store"

    def _path(self, table: str, rec_id: str) -> Path:
        if table not in TABLES:
            raise KeyError(f"unknown table {table!r}")
        safe = rec_id.replace("/", "_")
        return self.root / table / f"{safe}.json"

    def put(self, table: str, record: dict) -> dict:
        record = dict(record)
        record["updated_at"] = now_iso()
        record.setdefault("created_at", record["updated_at"])
        path = self._path(table, record["id"])
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=1, default=str))
        os.replace(tmp, path)
        return record

    def get(self, table: str, rec_id: str) -> dict | None:
        path = self._path(table, rec_id)
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def find(self, table: str, where: dict | None = None,
             predicate: Predicate | None = None) -> list[dict]:
        """All records matching the equality filters in `where` and the
        optional predicate. Small-N scan; fine for MVP volumes."""
        table_dir = self._path(table, "x").parent
        out = []
        if not table_dir.exists():
            return out
        for f in sorted(table_dir.glob("*.json")):
            rec = json.loads(f.read_text())
            if where and any(rec.get(k) != v for k, v in where.items()):
                continue
            if predicate and not predicate(rec):
                continue
            out.append(rec)
        return out

    def delete(self, table: str, rec_id: str) -> None:
        path = self._path(table, rec_id)
        if path.exists():
            path.unlink()


class PostgresStore:
    """Same contract against the dve schema. Import-lazy so the default
    JSON path never touches psycopg."""

    def __init__(self):
        from video import db
        self._db = db

    def put(self, table: str, record: dict) -> dict:
        if table not in TABLES:
            raise KeyError(f"unknown table {table!r}")
        record = dict(record)
        record["updated_at"] = now_iso()
        scalars = [c for c in TABLES[table] if c in record]
        cols = ["id", "status", "created_by", "payload"] + scalars
        vals = [record["id"], record.get("status", ""),
                record.get("created_by", "dve"), json.dumps(record, default=str)]
        vals += [record[c] for c in scalars]
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "id")
        sql = (f"INSERT INTO dve.{table} ({', '.join(cols)}) VALUES ({placeholders}) "
               f"ON CONFLICT (id) DO UPDATE SET {updates}")
        with self._db.connect() as conn:
            conn.execute(sql, vals)
            conn.commit()
        return record

    def get(self, table: str, rec_id: str) -> dict | None:
        if table not in TABLES:
            raise KeyError(f"unknown table {table!r}")
        with self._db.connect() as conn:
            row = conn.execute(
                f"SELECT payload FROM dve.{table} WHERE id = %s", (rec_id,)
            ).fetchone()
        return row[0] if row else None

    def find(self, table: str, where: dict | None = None,
             predicate: Predicate | None = None) -> list[dict]:
        if table not in TABLES:
            raise KeyError(f"unknown table {table!r}")
        allowed = set(TABLES[table]) | {"id", "status", "created_by"}
        clauses, vals = [], []
        pushdown = {k: v for k, v in (where or {}).items() if k in allowed}
        for k, v in pushdown.items():
            clauses.append(f"{k} = %s")
            vals.append(v)
        sql = f"SELECT payload FROM dve.{table}"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at"
        with self._db.connect() as conn:
            rows = conn.execute(sql, vals).fetchall()
        out = []
        residual = {k: v for k, v in (where or {}).items() if k not in allowed}
        for (rec,) in rows:
            if residual and any(rec.get(k) != v for k, v in residual.items()):
                continue
            if predicate and not predicate(rec):
                continue
            out.append(rec)
        return out

    def delete(self, table: str, rec_id: str) -> None:
        with self._db.connect() as conn:
            conn.execute(f"DELETE FROM dve.{table} WHERE id = %s", (rec_id,))
            conn.commit()


_store = None


def store():
    """The process-wide store, selected by DVE_STORE."""
    global _store
    if _store is None:
        if settings.STORE_BACKEND == "postgres":
            _store = PostgresStore()
        else:
            _store = JsonFileStore()
    return _store


def reset_store_for_tests(root: Path | None = None):
    global _store
    _store = JsonFileStore(root) if root else None
    return _store
