---
quick_id: 260423-tkf
phase: quick/260423-tkf
plan: 1
subsystem: spikes/jdbc-sdp-a1
tags: [spike, jdbc, sdp, federation, watermark, scaffold]
dependency_graph:
  requires: []
  provides: [spikes/jdbc-sdp-a1 scaffold — 9 files, syntax-valid, DAB-deployable]
  affects: []
tech_stack:
  added: []
  patterns:
    - DLT-META factory-closure pattern for dynamic SDP flows
    - Bookend task watermark state machine (no writes inside flow bodies)
    - Unity Catalog Lakehouse Federation (freesql foreign catalog)
    - spark.sql(sql, args={}) parameterised binding (T-tkf-01/02 mitigations)
key_files:
  created:
    - spikes/jdbc-sdp-a1/ddl/manifest_table.sql
    - spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql
    - spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py
    - spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
    - spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py
    - spikes/jdbc-sdp-a1/tasks/reconcile.py
    - spikes/jdbc-sdp-a1/tasks/validate.py
    - spikes/jdbc-sdp-a1/resources/spike_workflow.yml
    - spikes/jdbc-sdp-a1/README.md
  modified: []
decisions:
  - SDP flow bodies must not perform Delta writes; watermark state machine
    lives in bookend tasks (prepare_manifest/reconcile) not the pipeline
  - Factory-closure pattern (make_flow + dp.table as function) chosen over
    decorator-in-loop to avoid Python loop variable closure trap
  - spike.run_id and spike.inject_failure passed via SDP pipeline configuration
    block; reconcile reads pipeline_id from widget
  - file: library form chosen over notebook: for SDP pipeline source; alternative
    documented in spike_workflow.yml header for older DAB versions
  - Isolated devtest_edp_metadata.jdbc_spike.* schema; no lhp_watermark
    package dependency; state machine reproduced inline via spark.sql
metrics:
  duration: ~25min
  completed: 2026-04-24
  tasks_completed: 3
  files_created: 9
---

# Phase quick/260423-tkf Plan 1: JDBC SDP A1 Session 2 Scaffold Summary

**One-liner:** Nine-file scaffold for dynamic SDP federation spike — factory-closure
flows reading freesql foreign catalog, bookend watermark state machine, and DAB
resource wiring for devtest deploy.

## Tasks Completed

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Data layer — DDL + discovery + prepare_manifest | c99a9554 | ddl/manifest_table.sql, ddl/watermark_registry_spike.sql, tasks/fixtures_discovery.py, tasks/prepare_manifest.py |
| 2 | Compute layer — SDP pipeline + reconcile + validate | a1f1a0be | pipeline/sdp_pipeline.py, tasks/reconcile.py, tasks/validate.py |
| 3 | Wiring — DAB workflow resource + README | a52715af | resources/spike_workflow.yml, README.md |

## Verification Results

All plan verification checks passed:

```
Task 1:
  fixtures_discovery.py:      py_compile OK
  prepare_manifest.py:        py_compile OK
  manifest_table.sql:         CREATE TABLE found (devtest_edp_metadata.jdbc_spike.manifest)
  watermark_registry_spike:   CREATE TABLE found (devtest_edp_metadata.jdbc_spike.watermark_registry)

Task 2:
  sdp_pipeline.py:            py_compile OK
  reconcile.py:               py_compile OK
  validate.py:                py_compile OK
  SDP import check:           'from pyspark import pipelines as dp' present
  dp.table() factory check:   dp.table( call present
  no decorator-in-loop check: '@dp.table' string absent

Task 3:
  spike_workflow.yml:         YAML parses; task order [fixtures_discovery,
                              prepare_manifest, run_sdp_pipeline, reconcile, validate]
  README.md:                  non-empty; Purpose section; links to jdbc-execution-spike.md
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Comment strings triggered plan verification string checks**
- **Found during:** Task 2 verify step
- **Issue:** The plan's verification script uses literal string matching
  (`'@dp.table' not in src`, `'df.write' not in src`). Explanatory comments
  in the pipeline file that mentioned these patterns caused false positives.
- **Fix:** Reworded comments to avoid the exact literal strings while preserving
  full explanatory intent. No logic changed.
- **Files modified:** `spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py`
- **Commit:** a1f1a0be (same task commit)

None — plan executed exactly as written for all other aspects.

## Known Stubs

None. All 9 scaffold files are complete per their spec. The `validate.py` notebook
is intentionally minimal (a placeholder acceptance gate, not a data-quality suite)
— this is by design per the plan spec, not a stub.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes at
trust boundaries beyond those documented in the plan's threat model (T-tkf-01
through T-tkf-06). All mitigations implemented as specified:

| Mitigation | File | Implementation |
|------------|------|----------------|
| T-tkf-01 | prepare_manifest.py | Regex validation of run_id/parent_run_id + spark.sql args= binding |
| T-tkf-02 | reconcile.py | spark.sql args= binding; error_message truncated to 4096 chars |
| T-tkf-05 | All SQL | Hardcoded devtest_edp_metadata.jdbc_spike.* — no env interpolation |

## Self-Check: PASSED

All 9 spike files exist on disk. All 3 task commits (c99a9554, a1f1a0be, a52715af) confirmed in git log.
