-- Purpose: Isolated spike watermark registry DDL.
-- Mirrors the production watermark registry contract used by
-- insert_new / mark_landed / mark_failed / get_recoverable_landed_run
-- (see src/lhp/templates/load/jdbc_watermark_job.py.j2 for the reference
-- implementation and L2 §5.3 contract details).
--
-- ISOLATION RATIONALE: This table lives at devtest_edp_orchestration.jdbc_spike
-- and has no connection to the production watermark registry
-- (devtest_edp_orchestration.jdbc_watermark_registry or any qa/prod equivalent).
-- The spike reproduces state machine logic inline in plain spark.sql calls
-- so there is zero dependency on the lhp_watermark package. If the spike
-- succeeds and is integrated into LHP, this DDL will be superseded by the
-- production DDL managed by LHP templates.
--
-- Safe to re-run (IF NOT EXISTS guard). Schema changes need ALTER TABLE.

CREATE TABLE IF NOT EXISTS devtest_edp_orchestration.jdbc_spike.watermark_registry (
    run_id                   STRING   NOT NULL COMMENT 'Run identifier; matches manifest.run_id',
    source_system_id         STRING   NOT NULL COMMENT 'Foreign catalog name, e.g. freesql_catalog',
    schema_name              STRING   NOT NULL COMMENT 'Source schema name',
    table_name               STRING   NOT NULL COMMENT 'Source table name',
    watermark_column_name    STRING   COMMENT 'Timestamp column used as high-water mark',
    watermark_value          STRING   COMMENT 'HWM as string for cross-type portability (ISO-8601 for timestamps)',
    previous_watermark_value STRING   COMMENT 'HWM from previous completed run; NULL on first full load',
    row_count                BIGINT   COMMENT 'Row count extracted in this run',
    extraction_type          STRING   COMMENT 'full|incremental',
    status                   STRING   COMMENT 'pending|landed|failed|completed',
    error_class              STRING   COMMENT 'Exception class name on failure',
    error_message            STRING   COMMENT 'Truncated error message (max 4096 chars)',
    created_at               TIMESTAMP COMMENT 'Row insert timestamp (UTC)',
    updated_at               TIMESTAMP COMMENT 'Last status update timestamp (UTC)'
)
USING DELTA
COMMENT 'JDBC SDP A1 Spike — isolated watermark registry. Mirrors production contract. Not production.';
