-- 2026_05_19_puller_session.sql
--
-- Stores the authenticated Playwright user-data-dir (cookies, local
-- storage, etc) as a BYTEA blob so it can be shared between the user's
-- Mac (where the interactive login happens) and GitHub Actions
-- runners (where the puller runs every 15 min). Without this, the
-- puller would only work when the user's Mac is awake.
--
-- One row per puller — currently just 'msn-partner-hub-boxingnews'.
-- The blob is the contents of `tar -czf - msn-session/` so a single
-- column holds the entire user-data-dir.
--
-- Typical row size: 15-25 MB compressed. Postgres BYTEA limit is
-- 1 GB so we have huge headroom.
--
-- The backup/restore helper at scripts/session_backup_restore.py
-- reads + writes this table.

CREATE TABLE IF NOT EXISTS pgam_direct.puller_session (
  id            TEXT        PRIMARY KEY,
  session_blob  BYTEA       NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by    TEXT,                 -- who last wrote it (host:user OR 'gh-actions:run_id')
  blob_bytes    INTEGER     GENERATED ALWAYS AS (octet_length(session_blob)) STORED
);

CREATE INDEX IF NOT EXISTS puller_session_updated_idx
  ON pgam_direct.puller_session (updated_at DESC);

COMMENT ON TABLE pgam_direct.puller_session IS
  'Shared Playwright user-data-dir blob for the MSN puller. Restored on each GH Actions run, backed up after each successful pull.';
COMMENT ON COLUMN pgam_direct.puller_session.session_blob IS
  'tar -czf - msn-session/ — restore with tar -xzf - -C ~/.pgam/';
