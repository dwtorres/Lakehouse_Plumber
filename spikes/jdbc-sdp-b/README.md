# Spike B — Databricks `for_each_task` Native JDBC Ingestion

## Purpose

Prove the Databricks-native `for_each_task` approach to parallel JDBC ingestion at N=68 scale (73 AW tables minus 5 skipped), using the same PG AdventureWorks source via `pg_supabase` federation as Spike A1.

This spike exists to provide apples-to-apples comparison data against Spike A1 (SDP-native dynamic flows).

## Architecture

```
prepare_manifest  (one-shot notebook)
     │
     │  emits task value: manifest_rows = JSON array of 68 rows
     ▼
for_each_ingest   (for_each_task, concurrency=10)
     │
     ├─ ingest_one [humanresources.department]
     ├─ ingest_one [humanresources.employee]
     ├─ ... (68 iterations in parallel batches of 10)
     └─ ingest_one [sales.store]
          │
          │  per-iteration: read federation → write bronze → update state (inline)
          │  failure isolation: try/except; failed iterations do NOT cancel others
     ▼
validate          (one-shot notebook, run_if: ALL_DONE)
     │
     └─ assert completed_count >= min_completed
```

### Key contrast with Spike A1

| Dimension | Spike A1 (SDP dynamic flows) | Spike B (for_each_task) |
|-----------|------------------------------|-------------------------|
| Execution unit | SDP pipeline with N dynamic flows | for_each_task with N notebook iterations |
| State update timing | Post-run via reconcile task reading event_log() | Inline per-iteration (no reconcile needed) |
| Task count | 5 tasks (discovery, manifest, pipeline, reconcile, validate) | 3 tasks (manifest, for_each, validate) |
| Failure isolation | Flow-level (SDP handles per-flow failure) | Iteration-level (try/except in ingest_one.py) |
| Concurrency model | SDP runtime-managed | for_each `concurrency` parameter |
| Bronze target | `devtest_edp_bronze.jdbc_spike.*` | `devtest_edp_bronze.jdbc_spike_b.*` |
| load_group tag | `spike_a1` | `spike_b` |

## Skip List (5 tables excluded)

| Table | Reason |
|-------|--------|
| `production.document` | XML/binary columns — Spark type mapping failure |
| `production.illustration` | XML/binary columns |
| `production.productphoto` | Binary photo columns |
| `production.productmodel` | XML columns |
| `person.password` | Hashed credentials — HIPAA-skip in devtest |

## File Map

```
spikes/jdbc-sdp-b/
├── README.md                    — this file
├── databricks.yml               — DAB bundle config (bundle.name: spike_jdbc_sdp_b)
├── resources/
│   └── spike_workflow.yml       — Job: 3 tasks (prepare_manifest, for_each_ingest, validate)
├── ddl/
│   └── bronze_schema.sql        — CREATE SCHEMA IF NOT EXISTS devtest_edp_bronze.jdbc_spike_b
└── tasks/
    ├── prepare_manifest.py      — Builds 68-table selection, inserts manifest + registry rows,
    │                              emits manifest_rows task value for for_each
    ├── ingest_one.py            — Per-iteration: read → write → update state (inline try/except)
    └── validate.py              — Acceptance gate: assert completed >= min_completed
```

## Shared Resources

The manifest and watermark_registry tables are shared with Spike A1:

- `devtest_edp_orchestration.jdbc_spike.manifest`
- `devtest_edp_orchestration.jdbc_spike.watermark_registry`

Rows are isolated by `load_group='spike_b'`. **Do not drop or truncate these tables.**

## Deploy Commands

```bash
# From spikes/jdbc-sdp-b/ directory:

# 1. Apply bronze schema DDL (one-time)
jq -n --arg wh "9f30611e0b932a47" \
       --arg s "CREATE SCHEMA IF NOT EXISTS devtest_edp_bronze.jdbc_spike_b" \
  '{warehouse_id: $wh, statement: $s, wait_timeout: "50s"}' > /tmp/p.json
databricks --profile dbc-8e058692-373e api post /api/2.0/sql/statements --json @/tmp/p.json

# 2. Validate bundle
databricks --profile dbc-8e058692-373e bundle validate --target dev

# 3. Deploy bundle
databricks --profile dbc-8e058692-373e bundle deploy --target dev

# 4. Clear any stale pending rows from prior runs (spike_b only)
# Run via SQL warehouse or notebook before triggering.

# 5. Run job
databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_b_job \
  --target dev \
  --params "run_id=b-scale-$(date +%s),rerun_mode=fresh,min_completed=60"
```

## Acceptance Criteria

- [ ] `for_each_ingest` task starts and Databricks shows N iterations in job UI.
- [ ] `concurrency=10` observed — at most 10 simultaneous iteration tasks.
- [ ] `devtest_edp_bronze.jdbc_spike_b.*` tables created and populated.
- [ ] Manifest shows >= 60 rows with `execution_status='completed'`.
- [ ] Watermark registry shows >= 60 rows with `status='completed'`.
- [ ] `validate` task PASSES.
- [ ] Per-table failures (if any) do not cancel other iterations.
- [ ] `METRICS-SCALE-73.md` filled with wall-clock and per-iteration measurements.

## Rerun (failed tables only)

```bash
PREV_RUN_ID="b-scale-<epoch>"
databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_b_job \
  --target dev \
  --params "run_id=b-retry-$(date +%s),rerun_mode=failed_only,parent_run_id=${PREV_RUN_ID},min_completed=60"
```
