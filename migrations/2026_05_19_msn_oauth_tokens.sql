-- 2026-05-19-msn-oauth-tokens.sql
--
-- Stores the rotating MSN/Microsoft OAuth tokens used by the
-- refresh-token-chain puller (scripts/msn_refresh_puller.py).
--
-- Why a separate table from puller_session_chunk:
--   - puller_session_chunk holds a full Playwright user-data-dir
--     (44MB compressed) and is on its way out — Chromium profiles
--     don't survive ~16-24h MSN session timeouts.
--   - This table holds a 2-4KB refresh_token + metadata, the only
--     state needed for token-chain auth. The refresh_token rotates
--     on each use; we always store the most recent one.
--
-- Refresh-token chain semantics (Microsoft consumer accounts):
--   - Each refresh_token has a 24h lifetime (refresh_token_expires_in
--     = 86400 in the OAuth response).
--   - Using a refresh_token returns a NEW refresh_token with a fresh
--     24h window. As long as we refresh before the current one
--     expires, the chain is indefinite.
--   - If the chain breaks (no refresh within 24h), the user must
--     re-authenticate interactively (one MFA tap).
--
-- The `id` column lets us hold tokens for multiple partner accounts
-- in the future (e.g., a backup service account in case the primary
-- chain breaks). Default id = 'msn-partner-hub-boxingnews-primary'.

CREATE TABLE IF NOT EXISTS pgam_direct.msn_oauth_token (
  id              TEXT        PRIMARY KEY,
  client_id       TEXT        NOT NULL,
  tenant          TEXT        NOT NULL,
  scope           TEXT        NOT NULL,
  refresh_token   TEXT        NOT NULL,
  access_token    TEXT,
  access_expires_at  TIMESTAMPTZ,
  refresh_expires_at TIMESTAMPTZ NOT NULL,
  -- The OAuth `redirect_uri` we used for the initial code exchange.
  -- Microsoft requires the refresh-token call to use the same
  -- redirect_uri as the original code exchange or it 400s.
  redirect_uri    TEXT        NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_by      TEXT,
  refresh_count   INTEGER     NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS msn_oauth_token_refresh_expires_idx
  ON pgam_direct.msn_oauth_token (refresh_expires_at);

COMMENT ON TABLE pgam_direct.msn_oauth_token IS
  'Rotating Microsoft OAuth tokens for the MSN Partner Hub puller. '
  'refresh_token has 24h lifetime; each refresh rotates it. '
  'Use scripts/msn_oauth_capture.py to bootstrap (one-time, requires '
  'interactive MFA), scripts/msn_refresh_puller.py to refresh + run.';
