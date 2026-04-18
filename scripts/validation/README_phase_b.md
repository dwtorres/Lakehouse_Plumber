# TD-007 Phase B — Databricks Validation Runbook

Run the `validate_td007_databricks.py` notebook against a Databricks dev workspace
to confirm the V2 run-scoped + post-write-stats + recovery pattern behaves
correctly end-to-end against real Unity Catalog + Volume + `WatermarkManager`
state. Captures V5–V8 invariants (V1–V4 already covered by the laptop harness).

## Prerequisites

| Item | Value / Check |
|---|---|
| Databricks CLI profile | `dbc-8e058692-373e` (already configured; `databricks auth profiles` to confirm) |
| Unity Catalog write access | `lhp_dev.orchestration_validation` (or equivalent dev catalog/schema) |
| Volume path | `/Volumes/lhp_dev/orchestration_validation/td007_landing` (or equivalent writable Volume) |
| `lhp` package on cluster | Workspace files path **OR** installed wheel. If workspace files, capture the path into the `lhp_workspace_path` widget. |
| Cluster spec | Any all-purpose cluster or job cluster with DBR 14+ / DBR 17.3 LTS. Serverless is fine — the notebook is single-node compatible. |

## Option A — Run interactively from the Databricks UI

1. Import the notebook:
   ```bash
   databricks --profile dbc-8e058692-373e workspace import \
     scripts/validation/validate_td007_databricks.py \
     /Workspace/Users/<you>/td007/validate_td007_databricks \
     --language PYTHON --format SOURCE --overwrite
   ```
2. Open the notebook in the workspace UI. Review the `Widget defaults` cell.
3. Override widgets as appropriate:
   - `catalog` — your dev UC catalog (default `lhp_dev`)
   - `schema` — dev schema for the watermark table (default `orchestration_validation`)
   - `landing_root` — writable Volume path the test owns
   - `lhp_workspace_path` — only needed if `lhp` is not pre-installed; point at the
     workspace path containing `lhp/` (e.g. `/Workspace/Repos/<you>/Lakehouse_Plumber/src`)
4. Attach to a cluster, Run all. Final cell prints a JSON block and calls
   `dbutils.notebook.exit(json)` so Jobs capture records the summary.

## Option B — One-shot CLI submission (reproducible)

```bash
NOTEBOOK_SRC=scripts/validation/validate_td007_databricks.py
WORKSPACE_PATH=/Workspace/Users/<you>/td007/validate_td007_databricks
PROFILE=dbc-8e058692-373e

databricks --profile "$PROFILE" workspace import \
  "$NOTEBOOK_SRC" "$WORKSPACE_PATH" \
  --language PYTHON --format SOURCE --overwrite

cat > /tmp/td007-submit.json <<'JSON'
{
  "run_name": "td007-phase-b",
  "tasks": [
    {
      "task_key": "validate",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/<you>/td007/validate_td007_databricks",
        "base_parameters": {
          "catalog": "lhp_dev",
          "schema": "orchestration_validation",
          "landing_root": "/Volumes/lhp_dev/orchestration_validation/td007_landing",
          "source_system_id": "td007_validation",
          "schema_name": "td007",
          "table_name": "probe",
          "lhp_workspace_path": "/Workspace/Repos/<you>/Lakehouse_Plumber/src",
          "cleanup_on_success": "true"
        }
      },
      "existing_cluster_id": "<cluster-id>"
    }
  ]
}
JSON

RUN_JSON=$(databricks --profile "$PROFILE" jobs submit --json @/tmp/td007-submit.json)
RUN_ID=$(echo "$RUN_JSON" | jq -r '.run_id')

# Poll until terminal (the CLI doesn't expose a blocking wait; this idiom works
# across 0.2xx CLI versions).
while :; do
  STATE=$(databricks --profile "$PROFILE" jobs get-run "$RUN_ID" \
    | jq -r '.status.state // .state.life_cycle_state')
  case "$STATE" in
    TERMINATED|INTERNAL_ERROR|SKIPPED|SUCCESS|FAILED) break ;;
  esac
  sleep 10
done

databricks --profile "$PROFILE" jobs get-run-output "$RUN_ID" \
  | jq -r '.notebook_output.result' \
  | tee scripts/validation/td007_phase_b_evidence.json
```

Replace `<you>` and `<cluster-id>` as appropriate. For serverless, swap
`existing_cluster_id` for:

```json
"environment_key": "default"
```
and add a top-level `environments` entry per Databricks Jobs docs.

## Expected output

A PASS run emits JSON like:

```json
{
  "meta": {
    "timestamp": "2026-04-18T14:30:00+00:00",
    "catalog": "lhp_dev",
    "schema": "orchestration_validation",
    "landing_root": "/Volumes/lhp_dev/orchestration_validation/td007_landing",
    "source_system_id": "td007_validation"
  },
  "invariants": {
    "V5": { "verdict": "PASS", "duration_s": ..., "details": { ... }, "error": null },
    "V6": { "verdict": "PASS", "duration_s": ..., "details": { ... }, "error": null },
    "V7": { "verdict": "PASS", "duration_s": ..., "details": { ... }, "error": null },
    "V8": { "verdict": "PASS", "duration_s": ..., "details": { ... }, "error": null }
  },
  "overall_verdict": "PASS",
  "failed_invariants": []
}
```

Any non-`PASS` verdict records the error class, message, and a truncated
stack trace in the `error` field for triage.

## What gets written / cleaned up

During a PASS run with `cleanup_on_success=true` (default):

- Parquet files under `{landing_root}/_lhp_runs/{run_id}/` for each test run —
  removed on completion.
- Watermark rows in `{catalog}.{schema}.watermarks` for each test `run_id` —
  removed on completion.
- The watermark Delta table itself is **not** dropped; it is auto-created by
  `WatermarkManager.__init__` if absent and left in place for inspection.

If any invariant fails or errors, `cleanup_on_success` still runs per-invariant
inside each test function (they self-clean via `finally` blocks), so a failed
run does not leak state beyond the failing test's own artifacts. Review those
artifacts before dropping them for diagnostic purposes.

## Success criteria feeding ADR-001

ADR-001 records Phase A (local) + Phase B (Databricks) evidence. All eight
invariants (V1–V8) must report PASS before the ADR is marked Accepted and
TD-007 is closed as resolved. The Phase A run JSON lives at
`scripts/validation/td007_phase_a_evidence.json`; the Phase B run JSON is
written by the CLI snippet above to
`scripts/validation/td007_phase_b_evidence.json`.
