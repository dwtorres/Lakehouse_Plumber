-- Purpose: Create the bronze schema for Spike B targets.
-- Run once before deploying the spike bundle. Safe to re-run (IF NOT EXISTS).
--
-- Spike B targets are isolated under jdbc_spike_b to avoid colliding with
-- Spike A1's jdbc_spike schema in devtest_edp_bronze.

CREATE SCHEMA IF NOT EXISTS devtest_edp_bronze.jdbc_spike_b
  COMMENT 'JDBC SDP Spike B — for_each_task approach bronze landing zone';
