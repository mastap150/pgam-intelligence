-- migrations/2026_06_01_compliance_runs_ok_nullable.sql
--
-- PR #48 introduced a "tombstone" row inserted at the START of each
-- compliance run with ok=NULL, so that an OOM-killed run still leaves
-- evidence in compliance_runs for the scheduler's catch-up cooldown
-- to read. The original 2026_05_17_compliance.sql migration declared
-- `ok BOOLEAN NOT NULL`, which rejects the tombstone INSERT and leaves
-- the cooldown blind — letting the restart-loop fire repeatedly.
--
-- Drop the NOT NULL so ok=NULL means "run started, completion
-- unknown". Existing rows are untouched (they all have ok=TRUE).

ALTER TABLE pgam_direct.compliance_runs
    ALTER COLUMN ok DROP NOT NULL;
