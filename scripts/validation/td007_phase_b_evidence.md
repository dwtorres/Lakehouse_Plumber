# TD-007 Phase B Databricks Validation — Evidence

- **Timestamp (UTC)**: 2026-04-18T14:59:04+00:00
- **Profile**: `dbc-8e058692-373e`
- **Catalog / schema**: `main.orchestration`
- **Landing root (Volume)**: `/Volumes/main/bronze/landing/td007_validation`
- **Source system id**: `td007_validation`
- **Compute**: Serverless Jobs task (`environment_version: "2"`)
- **LHP runtime path**: `/Workspace/Users/verbena1@gmail.com/Lakehouse_Plumber/src` (git folder on `watermark` branch, commit `8cb6c8474a6d`)
- **Job run**: `281471321334282` / task `873933535937682`

## Overall verdict: **PASS**

| ID | Invariant | Verdict | Duration (s) | Summary |
|---|---|---|---|---|
| V5 | Multiple runs land to distinct `_lhp_runs/{run_id}/` subdirs | **PASS** | 28.64 | 3 runs, 3 distinct subdirs, row counts 100/250/50 all match. |
| V6 | `insert_new` → write → `mark_landed` → `mark_complete` commits expected state | **PASS** | 34.53 | `status='completed'`, row_count=500, watermark_value `2023-11-16 02:08:19` — observed = expected. |
| V7 | Recovery path: `mark_landed` only, next call finalizes via `get_recoverable_landed_run` without JDBC reopen | **PASS** | 20.36 | Recovered run_id matches, row_count=300, final `status='completed'`. |
| V8 | Nested-glob read `{root}/_lhp_runs/*` covers all run subdirs | **PASS** | 8.64 | Expected 200, observed 200. Per-run counts 120/80 match. |

## Key observations

1. **One JDBC read per extraction is functionally sufficient.** V6 + V7 commit the correct watermark row without any pre-write aggregation; stats derive solely from Parquet footer metadata on the landed files. TD-007's core claim is empirically validated on Databricks serverless compute.
2. **Run-scoped subdirs satisfy Constitution P6 in practice.** V5 demonstrates that three runs under the same logical source key produce three disjoint `_lhp_runs/{run_id}/` directories. `mode("overwrite")` scoped to a per-run subdir did not touch sibling runs.
3. **Recovery protocol reconciles partial failure without re-fetching from JDBC.** V7 simulated a crash between `mark_landed` and `mark_complete`. The next logical call to `get_recoverable_landed_run` returned the landed row exactly; `mark_complete` then finalized it end-to-end. This is the operational property Constitution P6 demands and TD-007 proposal originally missed.
4. **`_lhp_runs/*` glob is the correct downstream read pattern.** Bare `recursiveFileLookup=true` at the landing root encountered an `UNABLE_TO_INFER_SCHEMA` failure on Unity Catalog Volumes (first run before fix); switching to the explicit `/_lhp_runs/*` glob resolved it deterministically. **Implication for cloudFiles-side (DLT pipeline)**: the generated `cloudfiles.py.j2` must read from a path that includes or traverses the `_lhp_runs/` tier. Worth double-checking the current `CloudFilesLoadGenerator` output on the same fixture.
5. **`run_id` validator (`SQLInputValidator.uuid_or_job_run_id`) accepts only three shapes**: UUID-v4, `local-<uuid>`, or `job-N-task-N-attempt-N`. The Phase B harness originally used an ad-hoc `td007-{tag}-{hex12}` shape; the validator rejected it with `LHP-WM-003`. Tests now use `local-<uuid>`. Observation, not a defect — the validator is enforcing FR-L-09 correctly.

## What was written + cleaned up

- **Watermark Delta table**: `main.orchestration.watermarks` — auto-created by `WatermarkManager.__init__` on first access. Test rows for V6/V7 run_ids deleted post-test. Table itself left in place.
- **Volume subtree**: `/Volumes/main/bronze/landing/td007_validation/_lhp_runs/*` — each test invariant removed its own run subdirs after verifying behavior. `cleanup_on_success=true` was set.

## Reproducibility

Rerun via:

```bash
databricks --profile dbc-8e058692-373e workspace import \
  --language PYTHON --format SOURCE --overwrite \
  --file scripts/validation/validate_td007_databricks.py \
  /Users/verbena1@gmail.com/td007/validate_td007_databricks

databricks --profile dbc-8e058692-373e jobs submit --json @/tmp/td007-submit.json
```

Submit payload shape:

```json
{
  "run_name": "td007-phase-b-validation",
  "tasks": [{
    "task_key": "validate",
    "notebook_task": {
      "notebook_path": "/Users/verbena1@gmail.com/td007/validate_td007_databricks",
      "base_parameters": {
        "catalog": "main",
        "schema": "orchestration",
        "landing_root": "/Volumes/main/bronze/landing/td007_validation",
        "source_system_id": "td007_validation",
        "schema_name": "td007",
        "table_name": "probe",
        "lhp_workspace_path": "/Workspace/Users/verbena1@gmail.com/Lakehouse_Plumber/src",
        "cleanup_on_success": "true"
      }
    },
    "environment_key": "default"
  }],
  "environments": [{
    "environment_key": "default",
    "spec": { "environment_version": "2", "dependencies": [] }
  }]
}
```

## Runtime-availability caveat (scoped to TD-008)

This Phase B run used a **per-user Databricks Git folder** (`/Workspace/Users/verbena1@gmail.com/Lakehouse_Plumber`) pointing at the `watermark` branch. The Phase B notebook prepends that path to `sys.path` via the `lhp_workspace_path` widget before importing `lhp.extensions.watermark_manager`. This validates that the V2 runtime behaves correctly when the LHP runtime is on PYTHONPATH from a workspace Git folder — consistent with Constitution P2's "workspace files or production Git folder" language. **It does not establish a production runtime-availability pattern.** That decision is deferred to TD-008 / ADR-002. For now, the current LHP workflow template still emits `environments.dependencies: [${var.lhp_whl_path}]`, which is a separate Constitution P2 drift.
