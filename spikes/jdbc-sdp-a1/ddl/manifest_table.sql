-- Purpose: Delta DDL for the spike execution manifest table.
-- This table tracks per-table load state for a single run of the
-- JDBC SDP A1 spike. It is scoped to devtest_edp_metadata.jdbc_spike
-- and must NOT be confused with any production metadata schema.
--
-- Run once before deploying the spike bundle. Re-running is safe because
-- of the IF NOT EXISTS guard. Schema changes require manual ALTER TABLE.
--
-- Related upstream spec: docs/ideas/jdbc-execution-spike.md
-- Watermark state machine: insert_new / mark_landed / mark_failed
--   live in tasks/prepare_manifest.py and tasks/reconcile.py respectively.

CREATE TABLE IF NOT EXISTS devtest_edp_metadata.jdbc_spike.manifest (
    run_id                  STRING   COMMENT 'Spike run identifier (job run context or widget)',
    load_group              STRING   COMMENT 'Logical grouping; spike uses single group spike_a1',
    source_catalog          STRING   COMMENT 'Foreign catalog, e.g. freesql',
    source_schema           STRING   COMMENT 'Source schema name in the foreign catalog',
    source_table            STRING   COMMENT 'Source table name in the foreign catalog',
    target_table            STRING   COMMENT 'Bronze target under devtest_edp_bronze.jdbc_spike',
    watermark_column        STRING   COMMENT 'Timestamp column used as high-water mark',
    watermark_value_at_start STRING  COMMENT 'HWM captured by prepare_manifest before pipeline runs',
    execution_status        STRING   COMMENT 'pending|completed|failed|queued',
    started_at              TIMESTAMP COMMENT 'Pipeline flow start timestamp (UTC)',
    completed_at            TIMESTAMP COMMENT 'Pipeline flow completion timestamp (UTC)',
    error_class             STRING   COMMENT 'Exception class name on failure',
    error_message           STRING   COMMENT 'Truncated error message (max 4096 chars)',
    retry_count             INT      COMMENT 'Number of retries; 0 for fresh runs',
    parent_run_id           STRING   COMMENT 'Set on failed_only reruns to link back to originating run',
    rows_written            BIGINT   COMMENT 'Row count written to bronze target table'
)
USING DELTA
COMMENT 'JDBC SDP A1 Spike — per-run execution manifest. Isolated from production.';
