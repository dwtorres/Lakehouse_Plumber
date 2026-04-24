# JDBC SDP A1 Spike — Session 2 Scaffold

## Purpose

This spike validates Direction A1 from the JDBC execution design exploration
(see [docs/ideas/jdbc-execution-spike.md](../../docs/ideas/jdbc-execution-spike.md)).
The core question: can a single Databricks SDP (Lakeflow Spark Declarative
Pipelines) pipeline dynamically generate one flow per JDBC source table from a
manifest, read via Unity Catalog Lakehouse Federation (`freesql` foreign catalog),
and handle per-flow failure isolation so that one failing table does not block
the rest?

The revised A1 design uses bookend tasks to preserve L2 §5.3 watermark
correctness. This is necessary because SDP flow bodies forbid imperative Delta
writes — `df.write`, `spark.sql("INSERT INTO ...")`, and similar side-effect
operations are not permitted inside pipeline flow functions. Watermark state
transitions (`insert_new` / `mark_landed` / `mark_failed`) therefore live in
`prepare_manifest.py` (pre-bookend) and `reconcile.py` (post-bookend), with
the SDP pipeline itself responsible only for reading source data and returning
DataFrames.

Session 2 scaffold adds: failure injection, `failed_only` rerun semantics,
event-log reconciliation, and the full DAB resource wiring needed to deploy
with `databricks bundle deploy`. Static acceptance (file existence + parse
validity) is the gate for this session; runtime acceptance follows operator
deployment to devtest.

## Architecture

```
fixtures_discovery
        |
        v
prepare_manifest  ────────────────────────────────────────────────────────────
        |                                                                     |
        v                                                                     v
[SDP pipeline: N dynamic flows]                   devtest_edp_metadata.jdbc_spike.manifest
        |                                         devtest_edp_metadata.jdbc_spike.watermark_registry
        |  freesql.<schema>.<table>
        |      (filtered by watermark)
        v
devtest_edp_bronze.jdbc_spike.<target_table>  (one table per flow)
        |
        v
   reconcile  ──── reads event_log(pipeline(<id>))
        |               flow_progress COMPLETED / FAILED
        |          ──── UPDATE manifest execution_status
        |          ──── UPDATE watermark_registry (mark_landed / mark_failed)
        v
   validate   ──── assert count_completed >= min_completed
```

## File Map

| File | Purpose |
|------|---------|
| `ddl/manifest_table.sql` | Delta DDL for `devtest_edp_metadata.jdbc_spike.manifest` — per-run execution tracking |
| `ddl/watermark_registry_spike.sql` | Isolated spike watermark registry DDL mirroring L2 §5.3 contract |
| `tasks/fixtures_discovery.py` | Discovers 5 AdventureWorks tables via `freesql.information_schema`; persists to `selected_sources` |
| `tasks/prepare_manifest.py` | Pre-run bookend; inserts pending manifest + registry rows; supports `fresh` and `failed_only` modes |
| `pipeline/sdp_pipeline.py` | SDP source file; reads manifest at plan time; factory-closure dynamic flow generation |
| `tasks/reconcile.py` | Post-run bookend; queries event log; updates manifest + registry for COMPLETED / FAILED flows |
| `tasks/validate.py` | Acceptance gate; asserts `count_completed >= min_completed` |
| `resources/spike_workflow.yml` | DAB resource wiring 5-task job + SDP pipeline |
| `README.md` | This file |

## Prerequisites

- Databricks CLI authenticated with `dbc-8e058692-373e` profile (devtest workspace).
- `freesql` foreign catalog exists in devtest with PG AdventureWorks schemas/tables
  accessible to the running identity.
- `devtest_edp_metadata` catalog exists; user has `CREATE SCHEMA` and `CREATE TABLE`
  on `devtest_edp_metadata.jdbc_spike`.
- `devtest_edp_bronze` catalog exists; SDP pipeline service principal has
  `CREATE TABLE` on `devtest_edp_bronze.jdbc_spike`.
- Python 3.11+ locally for static validation (`python -m py_compile`).
- Databricks Asset Bundles CLI (`databricks bundle`) supporting SDP pipeline resources.

## Deploy & Run

```bash
# 1. Apply DDL manually (one-time, outside the bundle).
#    NOTE: the `databricks sql query` command requires a SQL warehouse endpoint.
#    If unavailable, run the DDL directly in a Databricks SQL editor notebook.
databricks --profile dbc-8e058692-373e sql query \
    --file spikes/jdbc-sdp-a1/ddl/manifest_table.sql
databricks --profile dbc-8e058692-373e sql query \
    --file spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql

# 2. Deploy the bundle (from the repo root, where databricks.yml lives).
databricks --profile dbc-8e058692-373e bundle deploy --target devtest

# 3. Fresh run (expect 5 completed flows):
databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
    --params run_id=spike-$(date +%s),rerun_mode=fresh,inject_failure=false,min_completed=5

# 4. Failure-injection run (expect 4 completed, 1 failed):
databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
    --params run_id=spike-fail-$(date +%s),rerun_mode=fresh,inject_failure=true,min_completed=4

# 5. Failed-only rerun (replace <prior-failing-run-id> with the run_id from step 4):
databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
    --params run_id=spike-retry-$(date +%s),rerun_mode=failed_only,parent_run_id=<prior-failing-run-id>,inject_failure=false,min_completed=1
```

## Acceptance Criteria (Runtime)

These are verified by the operator after deploying to devtest — not by static checks.

- **Fresh run:** 5 AdventureWorks tables complete end-to-end through a single
  SDP pipeline run; manifest shows `execution_status='completed'` for all 5.
- **Failure-injection run:** 4 flows complete, 1 fails; SDP continue-on-failure
  isolates the error; manifest reflects mixed statuses.
- **Failed-only rerun:** `rerun_mode=failed_only` processes exactly the 1 failed
  table from the prior run; other 4 are not re-processed.
- **Stretch — 100 flows:** change the `LIMIT 5` in `fixtures_discovery.py` to
  `LIMIT 100` and source 100 tables; scaffold supports this without code changes.

## Scaffold Acceptance (Static)

Run these checks locally before deployment to verify the scaffold is complete:

```bash
# All Python files compile:
python -m py_compile spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py
python -m py_compile spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
python -m py_compile spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py
python -m py_compile spikes/jdbc-sdp-a1/tasks/reconcile.py
python -m py_compile spikes/jdbc-sdp-a1/tasks/validate.py

# YAML parses:
python -c "import yaml; yaml.safe_load(open('spikes/jdbc-sdp-a1/resources/spike_workflow.yml'))"

# SQL contains valid CREATE TABLE statements:
grep -q 'CREATE TABLE' spikes/jdbc-sdp-a1/ddl/manifest_table.sql
grep -q 'CREATE TABLE' spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql

# No modifications to src/lhp/:
git diff --name-only HEAD~1 | grep '^src/lhp/' && echo 'FAIL: src/lhp modified' || echo 'OK: src/lhp clean'
```

## Scope Boundaries

- **Throwaway code** — this spike is NOT integrated into LHP core or the Wumbo
  metadata executor. All files live under `spikes/jdbc-sdp-a1/` and are
  explicitly excluded from the LHP package.
- **Isolated schema** — all tables use `devtest_edp_metadata.jdbc_spike.*`.
  No reads or writes to the production watermark registry
  (`devtest_edp_metadata.jdbc_watermark_registry` or any qa/prod equivalent).
- **No `lhp_watermark` dependency** — the spike reproduces `insert_new`,
  `mark_landed`, and `mark_failed` as inline `spark.sql` statements so the
  spike is fully self-contained.
- **If spike succeeds**, integrating this pattern into LHP templates is future
  work tracked as a separate GSD phase. The SDP dynamic-flow pattern would
  replace or complement the existing `jdbc_watermark_job.py.j2` template.

## Links

- Upstream spec: [docs/ideas/jdbc-execution-spike.md](../../docs/ideas/jdbc-execution-spike.md)
- Watermark contract reference: [src/lhp/templates/load/jdbc_watermark_job.py.j2](../../src/lhp/templates/load/jdbc_watermark_job.py.j2)
- Devtest validation norms: `.claude/projects/.../memory/project_devtest_validation.md`
