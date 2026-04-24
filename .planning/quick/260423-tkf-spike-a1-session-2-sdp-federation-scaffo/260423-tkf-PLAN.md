---
quick_id: 260423-tkf
phase: quick/260423-tkf
plan: 1
branch: spike/jdbc-sdp-a1
type: execute
goal: Scaffold 9 artifacts under spikes/jdbc-sdp-a1/ implementing revised A1 pattern (SDP dynamic flows + federation + bookend state machine).
wave: 1
depends_on: []
autonomous: true
files_modified:
  - spikes/jdbc-sdp-a1/README.md
  - spikes/jdbc-sdp-a1/ddl/manifest_table.sql
  - spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql
  - spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py
  - spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
  - spikes/jdbc-sdp-a1/tasks/reconcile.py
  - spikes/jdbc-sdp-a1/tasks/validate.py
  - spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py
  - spikes/jdbc-sdp-a1/resources/spike_workflow.yml
requirements:
  - SPIKE-A1-S2-01
must_haves:
  truths:
    - "All 9 scaffold files exist under spikes/jdbc-sdp-a1/"
    - "Python files parse without SyntaxError (py_compile clean)"
    - "YAML resource file parses as valid YAML"
    - "SQL DDL files contain valid CREATE TABLE statements for target schemas"
    - "SDP pipeline uses dp.table(fn, name=...) factory pattern (not decorator-in-loop) to avoid closure trap"
    - "Flow bodies return DataFrame only — no INSERT/UPDATE/MERGE/write inside @dp.table functions"
    - "Watermark state machine (insert_new / mark_landed / mark_failed) lives in prepare_manifest.py and reconcile.py, NOT inside the SDP pipeline"
    - "Spike registry and manifest tables live at devtest_edp_metadata.jdbc_spike.* — isolated from production watermark registry"
    - "No modifications to src/lhp/ — scaffold is throwaway spike code only"
  artifacts:
    - path: "spikes/jdbc-sdp-a1/README.md"
      provides: "Spike purpose, run instructions, acceptance criteria, links to upstream spec"
      min_lines: 40
    - path: "spikes/jdbc-sdp-a1/ddl/manifest_table.sql"
      provides: "Delta DDL for devtest_edp_metadata.jdbc_spike.manifest"
      contains: "CREATE TABLE"
    - path: "spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql"
      provides: "Isolated spike watermark registry DDL"
      contains: "CREATE TABLE"
    - path: "spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py"
      provides: "Runtime discovery of 5 PG AdventureWorks tables with timestamp watermark columns via freesql foreign catalog"
    - path: "spikes/jdbc-sdp-a1/tasks/prepare_manifest.py"
      provides: "Pre-run manifest insert + insert_new watermark calls; supports fresh and failed_only rerun modes"
    - path: "spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py"
      provides: "Single SDP pipeline reading manifest and dynamically generating one flow per pending table via factory closure"
      contains: "from pyspark import pipelines as dp"
    - path: "spikes/jdbc-sdp-a1/tasks/reconcile.py"
      provides: "Post-run event-log reconciliation; updates manifest + mark_landed/mark_failed in spike registry"
    - path: "spikes/jdbc-sdp-a1/tasks/validate.py"
      provides: "Placeholder validation notebook asserting minimum completed-row count"
    - path: "spikes/jdbc-sdp-a1/resources/spike_workflow.yml"
      provides: "DAB resource wiring 4 tasks + SDP pipeline in order: discovery → prepare → pipeline → reconcile → validate"
      contains: "resources:"
  key_links:
    - from: "spikes/jdbc-sdp-a1/tasks/prepare_manifest.py"
      to: "devtest_edp_metadata.jdbc_spike.manifest"
      via: "spark.sql INSERT with run_id, load_group, execution_status = 'pending'"
      pattern: "INSERT.*jdbc_spike\\.manifest"
    - from: "spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py"
      to: "devtest_edp_metadata.jdbc_spike.manifest"
      via: "spark.read of pending rows at pipeline plan time"
      pattern: "jdbc_spike\\.manifest"
    - from: "spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py"
      to: "freesql.<schema>.<table>"
      via: "spark.table(f'freesql.{schema}.{table}') filtered by watermark column"
      pattern: "freesql\\."
    - from: "spikes/jdbc-sdp-a1/tasks/reconcile.py"
      to: "event_log(pipeline(...))"
      via: "query flow_progress events with details:status IN ('COMPLETED','FAILED')"
      pattern: "flow_progress"
    - from: "spikes/jdbc-sdp-a1/resources/spike_workflow.yml"
      to: "spikes/jdbc-sdp-a1/tasks/*.py and pipeline/sdp_pipeline.py"
      via: "notebook_task / pipeline_task entries referencing workspace.file_path"
      pattern: "workspace\\.file_path"
---

<objective>
Scaffold a revised Direction A1 JDBC-agnostic SDP federation spike under
`spikes/jdbc-sdp-a1/`. Nine files total. No runtime execution in this plan —
acceptance is file existence + syntax/parse validity.

Purpose: Replace the prior per-table Databricks-tasks approach with a single
SDP pipeline that dynamically generates one flow per table from a manifest,
reads source data via Unity Catalog Lakehouse Federation (`freesql` foreign
catalog), and preserves L2 §5.3 watermark correctness via bookend tasks
(SDP flow bodies forbid imperative Delta writes).

Output: 9 scaffold files ready for user to deploy via `databricks bundle deploy`
and run manually against devtest workspace + `devtest_edp_metadata.jdbc_spike.*`
isolated schema.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@docs/ideas/jdbc-execution-spike.md
@src/lhp/templates/load/jdbc_watermark_job.py.j2
@src/lhp/generators/bundle/workflow_resource.py

<interfaces>
<!-- Key facts the executor needs. Extracted from upstream spec + existing watermark template. -->
<!-- Executor should use these directly — no codebase exploration needed. -->

## SDP imports (modern, required)
```python
from pyspark import pipelines as dp
# NOT: import dlt  (legacy)
```

## Dynamic flow factory pattern (DLT-META-proven, avoids loop closure trap)
```python
def make_flow(spec):
    def _flow():
        df = spark.table(f"freesql.{spec['source_schema']}.{spec['source_table']}")
        return df.filter(F.col(spec["watermark_column"]) > F.lit(spec["watermark_value_at_start"]))
    dp.table(_flow, name=spec["target_table"])

for spec in pending_specs:
    make_flow(spec)  # each call captures its own spec
```

## FORBIDDEN inside SDP flow bodies
- `spark.sql("INSERT INTO ...")`
- `df.write.saveAsTable(...)`
- `.write.mode(...).save(...)`
- ANY Delta table write / merge / update
- Quoted from Databricks docs: "Never use methods that save or write to files or tables as part of your pipeline dataset code."

## Watermark state machine (L2 §5.3) — lives OUTSIDE the pipeline
- `insert_new`    → called in prepare_manifest.py (pre-bookend task)
- `mark_landed`   → called in reconcile.py on COMPLETED flows (post-bookend task)
- `mark_failed`   → called in reconcile.py on FAILED flows (post-bookend task)
- Transitions: pending → landed (success) | failed (error)

## Event log query (for reconciliation)
```sql
SELECT origin:flow_name AS flow_name,
       details:status AS status,
       details:metrics:num_output_rows AS rows_written,
       message AS error_message,
       timestamp
FROM event_log(pipeline('<pipeline_id>'))
WHERE event_type = 'flow_progress'
  AND details:status IN ('COMPLETED', 'FAILED')
```

## Federation catalog assumption
- Foreign catalog name: `freesql`
- Source DB: Postgres AdventureWorks (schemas/tables discovered at runtime)
- Federation pushes down Filter, Projection, Limit (confirmed)

## Isolated spike schema (MUST use — do not touch production)
- Catalog: `devtest_edp_metadata`
- Schema:  `jdbc_spike`
- Tables:  `manifest`, `watermark_registry`, `selected_sources`
- Bronze target: `devtest_edp_bronze.jdbc_spike.<target_table>`

## Manifest columns (from upstream spec + open-question §3 resolution)
`run_id`, `load_group`, `source_catalog`, `source_schema`, `source_table`,
`target_table`, `watermark_column`, `watermark_value_at_start`,
`execution_status`, `started_at`, `completed_at`, `error_class`,
`error_message`, `retry_count`, `parent_run_id`, `rows_written`

## Watermark registry contract (from jdbc_watermark_job.py.j2)
Sufficient fields for: `insert_new`, `mark_landed`, `mark_failed`,
`get_recoverable_landed_run`. Columns inferred:
`run_id`, `source_system_id`, `schema_name`, `table_name`,
`watermark_column_name`, `watermark_value`, `previous_watermark_value`,
`row_count`, `extraction_type`, `status` (pending|landed|failed|completed),
`error_class`, `error_message`, `created_at`, `updated_at`
</interfaces>

## Constraints reminder
- Scaffold only — no runtime execution expected. Acceptance = files exist and parse.
- Do NOT modify `src/lhp/` — spike is throwaway.
- Python language for pipeline (dynamic flow gen requires Python, not SQL).
- Black line length 88. Keep notebook cells small and readable.
- Notebooks use `# Databricks notebook source` header + `# COMMAND ----------` cell separators where appropriate, matching existing pattern in `src/lhp/templates/load/jdbc_watermark_job.py.j2`.
- Pipeline source file `pipeline/sdp_pipeline.py` is a plain Python source file (not a notebook) — SDP pipelines consume `.py` source files directly.
</context>

<tasks>

<task type="auto">
  <name>Task 1: Data layer — DDL + discovery + prepare_manifest</name>
  <files>
    spikes/jdbc-sdp-a1/ddl/manifest_table.sql,
    spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql,
    spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py,
    spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
  </files>
  <action>
Create the data-layer scaffold: two DDL files and two pre-execution Databricks notebooks.

**1. `ddl/manifest_table.sql`** — Delta DDL for `devtest_edp_metadata.jdbc_spike.manifest`.
Columns in this order with inline `COMMENT` on each:
  - `run_id STRING COMMENT 'Spike run identifier (job run context or widget)'`
  - `load_group STRING COMMENT 'Logical grouping; spike uses single group'`
  - `source_catalog STRING COMMENT 'Foreign catalog, e.g. freesql'`
  - `source_schema STRING`
  - `source_table STRING`
  - `target_table STRING COMMENT 'Bronze target under devtest_edp_bronze.jdbc_spike'`
  - `watermark_column STRING`
  - `watermark_value_at_start STRING COMMENT 'HWM captured by prepare_manifest'`
  - `execution_status STRING COMMENT 'pending|completed|failed|queued'`
  - `started_at TIMESTAMP`
  - `completed_at TIMESTAMP`
  - `error_class STRING`
  - `error_message STRING`
  - `retry_count INT`
  - `parent_run_id STRING COMMENT 'Set on failed_only reruns to link back to originating run'`
  - `rows_written BIGINT`
Use `CREATE TABLE IF NOT EXISTS devtest_edp_metadata.jdbc_spike.manifest (...) USING DELTA;`.
Include a brief `-- Purpose:` header comment block at top of file.

**2. `ddl/watermark_registry_spike.sql`** — Delta DDL mirroring the production
watermark registry contract (see `src/lhp/templates/load/jdbc_watermark_job.py.j2`
for the fields used in `insert_new`, `mark_landed`, `mark_failed`,
`get_recoverable_landed_run`). Target:
`devtest_edp_metadata.jdbc_spike.watermark_registry`.
Columns with `COMMENT` on each:
  - `run_id STRING NOT NULL`
  - `source_system_id STRING NOT NULL`
  - `schema_name STRING NOT NULL`
  - `table_name STRING NOT NULL`
  - `watermark_column_name STRING`
  - `watermark_value STRING COMMENT 'HWM as string for cross-type portability'`
  - `previous_watermark_value STRING`
  - `row_count BIGINT`
  - `extraction_type STRING COMMENT 'full|incremental'`
  - `status STRING COMMENT 'pending|landed|failed|completed'`
  - `error_class STRING`
  - `error_message STRING`
  - `created_at TIMESTAMP`
  - `updated_at TIMESTAMP`
`USING DELTA`. Include top `-- Purpose:` header explaining the isolation
rationale (spike must not pollute production registry).

**3. `tasks/fixtures_discovery.py`** — Databricks notebook (Python).
Header: `# Databricks notebook source` as line 1.
Markdown cell (via `# MAGIC %md`): document intent — "Discover 5 PG
AdventureWorks tables accessible via `freesql` foreign catalog that carry a
timestamp/ModifiedDate-style watermark column. Persist selection to
`devtest_edp_metadata.jdbc_spike.selected_sources` for downstream tasks."
Then cells:
  - Cell: query `freesql.information_schema.tables` listing schemas + tables.
  - Cell: for each candidate table, query `freesql.information_schema.columns`
    looking for column names matching `ilike` patterns `%modified%`, `%updated%`,
    `%date%` where `data_type` is timestamp-like (`timestamp%` or `date`). First
    match wins per table.
  - Cell: select first 5 distinct (schema, table, watermark_column) rows.
  - Cell: build target_table name as `{source_schema}_{source_table}` lowercased,
    non-alphanumerics replaced with `_`.
  - Cell: `spark.createDataFrame(...)` + `.write.mode("overwrite").saveAsTable("devtest_edp_metadata.jdbc_spike.selected_sources")`.
    Schema: `source_schema STRING, source_table STRING, watermark_column STRING, target_table STRING`.
  - Cell: `display()` the final selection for visual verification.
Use `# COMMAND ----------` between cells. Include a concluding `print()` summarising the 5 selected tables.

**4. `tasks/prepare_manifest.py`** — Databricks notebook (Python).
Header: `# Databricks notebook source`. Widgets (one cell, block of `dbutils.widgets.text(...)`):
  - `run_id` (default empty — validate non-empty before proceeding)
  - `load_group` (default `spike_a1`)
  - `rerun_mode` (default `fresh`; must be one of `fresh`, `failed_only`)
  - `parent_run_id` (default empty; required when `rerun_mode=failed_only`)
Read widget values via `dbutils.widgets.get(...)`.

Behaviour:
  - If `rerun_mode = fresh`:
    - Read `devtest_edp_metadata.jdbc_spike.selected_sources` into a DataFrame.
    - For each row, INSERT a new row into `...jdbc_spike.manifest` with
      `execution_status='pending'`, `started_at=NULL`, `completed_at=NULL`,
      `retry_count=0`, `parent_run_id=NULL`, `rows_written=NULL`.
      `watermark_value_at_start` = current HWM from spike watermark_registry
      via subquery `(SELECT MAX(watermark_value) FROM ...watermark_registry
      WHERE schema_name=... AND table_name=... AND status='completed')`,
      fallback to `'1900-01-01T00:00:00'` if none (epoch-zero proxy for timestamp watermarks).
    - For each row, also INSERT a pending row into the spike watermark_registry
      with `status='pending'`, `extraction_type='incremental' if previous HWM exists else 'full'`,
      `source_system_id='freesql'`, matching `schema_name`/`table_name`/
      `watermark_column_name`/`watermark_value`/`previous_watermark_value`/`run_id`,
      `created_at = current_timestamp()`.
  - If `rerun_mode = failed_only`:
    - Assert `parent_run_id` is non-empty; raise `ValueError` otherwise.
    - Query manifest WHERE `run_id = :parent_run_id AND execution_status = 'failed'`.
    - For each failed row, INSERT a new manifest row with the NEW `run_id`,
      `parent_run_id` set, `retry_count = previous.retry_count + 1`,
      `execution_status = 'pending'`, clearing `error_class`/`error_message`/
      `completed_at`/`started_at`/`rows_written`. Reuse the original
      `watermark_value_at_start` (do NOT advance HWM on retry).
    - Also INSERT a fresh pending row in the spike watermark_registry for each.

Emit a summary `print(json.dumps(...))` at the end with counts of rows inserted.
All SQL uses parameterised spark.sql via named parameters (`spark.sql(sql, args=...)`)
where supported, else f-strings with explicit string-escaping for the run_id only
(run_ids are caller-controlled, not tainted). Imports at top: `json`, `from pyspark.sql import functions as F`.

Do not depend on the production `lhp_watermark` package — spike reproduces
`insert_new` logic inline as plain `spark.sql` INSERTs so the spike is
self-contained.
  </action>
  <verify>
    <automated>python -m py_compile spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py spikes/jdbc-sdp-a1/tasks/prepare_manifest.py && python -c "import sqlparse; [sqlparse.parse(open(f).read()) for f in ['spikes/jdbc-sdp-a1/ddl/manifest_table.sql','spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql']]; print('sql parsed')" 2>/dev/null || python -c "open('spikes/jdbc-sdp-a1/ddl/manifest_table.sql').read().upper().index('CREATE TABLE'); open('spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql').read().upper().index('CREATE TABLE'); print('sql contains CREATE TABLE')"</automated>
  </verify>
  <done>
All 4 files exist. Both `.py` notebooks compile without SyntaxError.
Both SQL files contain `CREATE TABLE` targeting the correct
`devtest_edp_metadata.jdbc_spike.*` names. `prepare_manifest.py` honours
both `fresh` and `failed_only` rerun modes per the action spec. No
imports of `lhp_watermark` anywhere (spike is self-contained).
  </done>
</task>

<task type="auto">
  <name>Task 2: Compute layer — SDP pipeline + reconcile + validate</name>
  <files>
    spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py,
    spikes/jdbc-sdp-a1/tasks/reconcile.py,
    spikes/jdbc-sdp-a1/tasks/validate.py
  </files>
  <action>
Create the compute layer: the SDP pipeline source file and two post-execution notebooks.

**1. `pipeline/sdp_pipeline.py`** — SDP pipeline source file (plain Python, NOT a notebook).
Do NOT add `# Databricks notebook source` header. SDP consumes `.py` source files directly.

File structure:
  - Module docstring describing the spike purpose + link to
    `docs/ideas/jdbc-execution-spike.md`.
  - Imports:
    ```python
    from pyspark import pipelines as dp
    from pyspark.sql import functions as F
    from pyspark.sql import SparkSession
    ```
  - Get SparkSession via `spark = SparkSession.getActiveSession()` or assume
    injected `spark` (SDP style — comment that SDP injects `spark`).
  - Read config via `spark.conf.get("spike.run_id")` and
    `spark.conf.get("spike.inject_failure", "false")`.
  - Read pending manifest rows at pipeline plan time:
    ```python
    pending_df = spark.read.table("devtest_edp_metadata.jdbc_spike.manifest").filter(
        (F.col("run_id") == run_id) & (F.col("execution_status") == "pending")
    )
    pending_specs = [row.asDict() for row in pending_df.collect()]
    ```
  - Factory function `def make_flow(spec: dict) -> None:` that:
    - Defines inner `_flow()` closure capturing `spec` by default argument
      (`def _flow(spec=spec):` pattern as extra defensive measure even though
      the factory scope already isolates `spec`).
    - Inner `_flow` body:
      ```python
      source_fqn = f"freesql.{spec['source_schema']}.{spec['source_table']}"
      df = spark.read.table(source_fqn)
      watermark_col = spec["watermark_column"]
      hwm = spec["watermark_value_at_start"]
      if hwm:
          df = df.filter(F.col(watermark_col) > F.lit(hwm))
      return df
      ```
    - Injects failure when `inject_failure == "true"` AND this is the first spec:
      replace source_fqn with `freesql.does_not_exist.nope_nope` so the flow
      fails at table resolution time but others continue.
    - Calls `dp.table(_flow, name=spec["target_table"], comment=f"Spike flow for {source_fqn}")`.
    - Do NOT pass `@dp.table` as a decorator — call it as a function per the
      DLT-META pattern to avoid the Python loop closure trap. Add an inline
      comment explaining WHY (loop closure trap, DLT-META pattern).
  - Main loop at module level:
    ```python
    for spec in pending_specs:
        make_flow(spec)
    ```
  - End-of-file comment: "NO imperative writes below — SDP forbids side effects in flow bodies. Watermark state transitions happen in tasks/reconcile.py."

**2. `tasks/reconcile.py`** — Databricks notebook (Python).
Header: `# Databricks notebook source`. Widgets: `run_id`, `pipeline_id`.

Cells:
  - Query the pipeline event log for this run. Use
    `event_log(pipeline('<pipeline_id>'))` table-valued function. Pull
    `event_type='flow_progress'` rows where `details:status IN ('COMPLETED', 'FAILED')`.
    Extract: `origin.flow_name`, `details:status`, `details:metrics:num_output_rows`,
    `message` (error), `timestamp`. Deduplicate by `flow_name` keeping the latest
    event per flow (in case a flow retried within the pipeline).
  - Left-join event-log outcomes with manifest rows WHERE `manifest.run_id = :run_id`
    on `flow_name = manifest.target_table`.
  - For each joined row:
    - If `details:status = 'COMPLETED'`:
      - UPDATE manifest SET `execution_status='completed'`, `completed_at=current_timestamp()`, `rows_written=<num_output_rows>` WHERE `run_id=:run_id AND target_table=<flow>`.
      - Compute new HWM: read the landed bronze table
        `devtest_edp_bronze.jdbc_spike.<target_table>`, run
        `SELECT MAX(<watermark_column>) FROM ...` to get the new HWM. If the
        bronze table is empty (0 rows written), reuse `watermark_value_at_start`.
      - UPDATE spike watermark_registry SET `status='landed'`, `watermark_value=<new_hwm>`, `row_count=<num_output_rows>`, `updated_at=current_timestamp()` WHERE `run_id=:run_id AND schema_name=... AND table_name=...`.
      - Then UPDATE status to `'completed'` (mirrors production
        `mark_landed` → `mark_complete` two-step; done in a single cell but
        two sequential UPDATEs so intent is visible).
    - If `details:status = 'FAILED'`:
      - UPDATE manifest SET `execution_status='failed'`, `completed_at=current_timestamp()`, `error_class='FlowFailure'`, `error_message=<message truncated to 4096>`.
      - UPDATE spike watermark_registry SET `status='failed'`, `error_class='FlowFailure'`, `error_message=<truncated>`, `updated_at=current_timestamp()`.
    - If `details:status IS NULL` (flow never emitted a terminal event — QUEUED/SKIPPED):
      - Leave manifest row as `pending` so `failed_only` rerun will NOT pick it
        up but a future `fresh` run will retry naturally. Log a warning line.
  - Emit `print(json.dumps(...))` summary: counts of completed, failed, untouched.

All SQL uses parameterised `spark.sql(sql, args={...})`. Imports: `json`, `from pyspark.sql import functions as F`.

**3. `tasks/validate.py`** — Databricks notebook (Python), placeholder.
Header: `# Databricks notebook source`. Widgets: `run_id`, `min_completed` (default `4`).

Cells:
  - Query manifest WHERE `run_id = :run_id AND execution_status = 'completed'`.
  - `count_completed = result.count()` (or single-row agg).
  - `assert count_completed >= int(min_completed), f"Only {count_completed} completed, expected >= {min_completed}"`.
  - `print` a summary table: for the run, show each target_table + execution_status + rows_written.
  - Include a `display(...)` call for the manifest snapshot so the Databricks UI shows the result.

Keep validate.py deliberately small — it is a placeholder acceptance gate, not
a comprehensive data-quality suite.
  </action>
  <verify>
    <automated>python -m py_compile spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py spikes/jdbc-sdp-a1/tasks/reconcile.py spikes/jdbc-sdp-a1/tasks/validate.py && python -c "import ast, sys; tree=ast.parse(open('spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py').read()); assert any(isinstance(n, ast.ImportFrom) and n.module=='pyspark' and any(a.name=='pipelines' for a in n.names) for n in ast.walk(tree)), 'missing pipelines import'; src=open('spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py').read(); assert 'dp.table(' in src, 'missing dp.table() factory call'; assert '@dp.table' not in src, 'decorator-in-loop detected (closure trap risk)'; print('sdp pipeline ok')"</automated>
  </verify>
  <done>
All 3 files exist and compile. `sdp_pipeline.py` imports `from pyspark import
pipelines as dp`, calls `dp.table(...)` as a function (not a decorator) inside
a factory closure, and contains no `df.write.*` or `spark.sql("INSERT...")`
calls inside any flow body. `reconcile.py` queries the event log and performs
manifest + registry UPDATEs for both COMPLETED and FAILED flow outcomes.
`validate.py` asserts `count_completed >= min_completed` and exits non-zero
on failure (via `assert`).
  </done>
</task>

<task type="auto">
  <name>Task 3: Wiring — DAB workflow resource + README</name>
  <files>
    spikes/jdbc-sdp-a1/resources/spike_workflow.yml,
    spikes/jdbc-sdp-a1/README.md
  </files>
  <action>
Create the two wiring artifacts: the DAB resource file and the spike README.

**1. `resources/spike_workflow.yml`** — Databricks Asset Bundle resource YAML.
Top-level: `resources:` with two sub-keys, `jobs:` and `pipelines:`.

**Pipeline resource** (`resources.pipelines.spike_jdbc_sdp_a1_pipeline`):
  - `name: spike_jdbc_sdp_a1_pipeline`
  - `catalog: devtest_edp_bronze`
  - `schema: jdbc_spike` (root schema for flow targets)
  - `serverless: true`
  - `channel: PREVIEW`  (SDP dynamic flow API is on preview/current)
  - `libraries:`
    - `- notebook:` with `path: ../pipeline/sdp_pipeline.py` (relative to this
      resource file's directory) — SDP accepts `.py` source files via
      `file:` entry OR notebook entry depending on DAB version; use
      `- file: path: ../pipeline/sdp_pipeline.py` which is the modern form.
  - `configuration:`
    - `spike.run_id: "${var.run_id}"`
    - `spike.inject_failure: "${var.inject_failure}"`
  - `continuous: false`

**Job resource** (`resources.jobs.spike_jdbc_sdp_a1_job`):
  - `name: spike_jdbc_sdp_a1`
  - `parameters:` job-level params:
    - `- name: run_id` default: `${workflow.run_id}` (Databricks macro) — but
      since bundles don't resolve that at deploy time, use plain `${bundle.name}-${var.stamp}`
      placeholder; operator overrides at trigger time.
    - `- name: rerun_mode` default `fresh`
    - `- name: inject_failure` default `false`
    - `- name: parent_run_id` default `""`
    - `- name: min_completed` default `4`
  - `tasks:` in this order with `depends_on` chain:
    1. `task_key: fixtures_discovery`
       - `notebook_task:`
         - `notebook_path: ../tasks/fixtures_discovery.py`
       - no `depends_on`
       - `job_cluster_key` omitted → serverless notebook task.
    2. `task_key: prepare_manifest`
       - `notebook_task:`
         - `notebook_path: ../tasks/prepare_manifest.py`
         - `base_parameters:` `{ run_id: "{{job.parameters.run_id}}", rerun_mode: "{{job.parameters.rerun_mode}}", parent_run_id: "{{job.parameters.parent_run_id}}", load_group: "spike_a1" }`
       - `depends_on: [{ task_key: fixtures_discovery }]`
    3. `task_key: run_sdp_pipeline`
       - `pipeline_task:`
         - `pipeline_id: ${resources.pipelines.spike_jdbc_sdp_a1_pipeline.id}`
         - `full_refresh: false`
       - `depends_on: [{ task_key: prepare_manifest }]`
    4. `task_key: reconcile`
       - `notebook_task:`
         - `notebook_path: ../tasks/reconcile.py`
         - `base_parameters:` `{ run_id: "{{job.parameters.run_id}}", pipeline_id: "${resources.pipelines.spike_jdbc_sdp_a1_pipeline.id}" }`
       - `depends_on: [{ task_key: run_sdp_pipeline }]`
    5. `task_key: validate`
       - `notebook_task:`
         - `notebook_path: ../tasks/validate.py`
         - `base_parameters:` `{ run_id: "{{job.parameters.run_id}}", min_completed: "{{job.parameters.min_completed}}" }`
       - `depends_on: [{ task_key: reconcile }]`

Top of file: `# Databricks Asset Bundle resource — JDBC SDP A1 Spike` comment
block briefly explaining the job topology (5 tasks, 1 SDP pipeline).

**2. `README.md`** — Spike README. Sections (use `##` headings):

  - `# JDBC SDP A1 Spike — Session 2 Scaffold`
  - `## Purpose` — 2-3 paragraphs explaining: revised Direction A1, why
    dynamic SDP flows + federation + bookend state machine (SDP forbids
    imperative writes in flow bodies), what this scaffold proves. Link to
    `docs/ideas/jdbc-execution-spike.md`.
  - `## Architecture` — textual diagram (ASCII boxes / mermaid fence):
    ```
    fixtures_discovery → prepare_manifest → [SDP pipeline: N dynamic flows] → reconcile → validate
                              │                         │                           │
                              │                         ▼                           │
                              │        freesql.<schema>.<table> → devtest_edp_bronze.jdbc_spike.<target>
                              │                                                     │
                              ▼                                                     ▼
                 devtest_edp_metadata.jdbc_spike.manifest ←──── UPDATE on event-log reconciliation
                 devtest_edp_metadata.jdbc_spike.watermark_registry (insert_new / mark_landed / mark_failed)
    ```
  - `## File Map` — table listing each of the 9 files + 1-line purpose each.
  - `## Prerequisites` — bullets:
    - Databricks CLI authenticated with `dbc-8e058692-373e` profile (devtest).
    - `freesql` foreign catalog exists with PG AdventureWorks schemas/tables.
    - `devtest_edp_metadata` and `devtest_edp_bronze` catalogs exist and user has CREATE privileges.
    - Python 3.11+ locally for static validation.
  - `## Deploy & Run` — commands:
    ```bash
    # 1. Apply DDL manually (one-time, outside the bundle):
    databricks --profile dbc-8e058692-373e sql query --file spikes/jdbc-sdp-a1/ddl/manifest_table.sql
    databricks --profile dbc-8e058692-373e sql query --file spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql

    # 2. Deploy the bundle:
    databricks --profile dbc-8e058692-373e bundle deploy --target devtest

    # 3. Fresh run (expect 5 completed flows):
    databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
        --params run_id=spike-$(date +%s),rerun_mode=fresh,inject_failure=false,min_completed=5

    # 4. Failure-injection run (expect 4 completed, 1 failed):
    databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
        --params run_id=spike-fail-$(date +%s),rerun_mode=fresh,inject_failure=true,min_completed=4

    # 5. Failed-only rerun:
    databricks --profile dbc-8e058692-373e bundle run spike_jdbc_sdp_a1 \
        --params run_id=spike-retry-$(date +%s),rerun_mode=failed_only,parent_run_id=<prior-failing-run-id>,inject_failure=false,min_completed=1
    ```
    Note: The exact DDL-apply command may need adjustment for the SQL warehouse setup; operator may run DDL manually via a SQL editor. The README should call this out explicitly.
  - `## Acceptance Criteria (Runtime)` — mirror from upstream spec:
    - 5 tables through end-to-end in a single SDP pipeline run.
    - Failure-injection run: 4 complete, 1 fails, pipeline continues.
    - Failed-only rerun: processes only the failed table.
    - Stretch: scale to 100 flows (scaffold supports this — just change `fixtures_discovery.py` limit).
  - `## Scaffold Acceptance (Static)` — list:
    - All 9 files present.
    - Python files compile (`python -m py_compile`).
    - YAML parses.
    - SQL contains valid `CREATE TABLE` statements.
    - No modifications to `src/lhp/`.
  - `## Scope Boundaries` — explicitly call out:
    - Throwaway code — NOT integrated into LHP core or Wumbo.
    - Isolated schema — does NOT touch production watermark registry.
    - No `lhp_watermark` package dependency — spike reproduces the state machine inline.
    - If spike succeeds, integration into LHP templates is future work (separate GSD phase).
  - `## Links` — bullets to `docs/ideas/jdbc-execution-spike.md`,
    `src/lhp/templates/load/jdbc_watermark_job.py.j2` (reference contract),
    and (if present) memory files describing devtest validation norms.

Target README length: 80–150 lines. Concise, code-block-heavy. No fluff.
  </action>
  <verify>
    <automated>python -c "import yaml; doc=yaml.safe_load(open('spikes/jdbc-sdp-a1/resources/spike_workflow.yml')); assert 'resources' in doc and 'jobs' in doc['resources'] and 'pipelines' in doc['resources'], 'missing resources/jobs/pipelines'; job=list(doc['resources']['jobs'].values())[0]; task_keys=[t['task_key'] for t in job['tasks']]; expected=['fixtures_discovery','prepare_manifest','run_sdp_pipeline','reconcile','validate']; assert task_keys==expected, f'task order wrong: {task_keys}'; print('yaml ok:', task_keys)" && test -s spikes/jdbc-sdp-a1/README.md && grep -q 'Purpose' spikes/jdbc-sdp-a1/README.md && grep -q 'jdbc-execution-spike.md' spikes/jdbc-sdp-a1/README.md && echo 'readme ok'</automated>
  </verify>
  <done>
Both files exist. `spike_workflow.yml` parses as YAML, contains
`resources.jobs.*` and `resources.pipelines.*`, has all 5 tasks in the correct
dependency chain (fixtures_discovery → prepare_manifest → run_sdp_pipeline →
reconcile → validate). `README.md` is non-empty, includes Purpose,
Architecture, File Map, Prerequisites, Deploy & Run, Acceptance Criteria,
and Links sections, and references the upstream spec
`docs/ideas/jdbc-execution-spike.md`.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| operator → Databricks job params | `run_id`, `rerun_mode`, `parent_run_id` flow from trigger command into notebook widgets and into spark.sql. Untrusted at the boundary. |
| freesql foreign catalog → SDP pipeline | External PG source; data crosses into bronze. |
| SDP event log → reconcile.py | Event log rows drive manifest UPDATEs; event log is workspace-managed (trusted). |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-tkf-01 | Tampering | prepare_manifest.py SQL composition with `run_id`/`parent_run_id` from widgets | mitigate | Use `spark.sql(..., args={...})` parameterised binding for all widget-sourced values; never f-string them into SQL strings. Add a regex validation on `run_id` (alphanumeric + dash + underscore only) at the top of the notebook; reject and exit if malformed. |
| T-tkf-02 | Tampering | reconcile.py event-log → manifest UPDATE | mitigate | Parameterise `run_id` and `pipeline_id` via `spark.sql` args binding. Truncate `error_message` to 4096 chars (matches production template) to prevent log-injection blow-up. |
| T-tkf-03 | Information Disclosure | Federation read of PG AdventureWorks via `freesql` | accept | Spike catalog is devtest + AdventureWorks is demo data (no real PII). Bronze target is in `devtest_edp_bronze.jdbc_spike.*` which is isolated dev scope. |
| T-tkf-04 | Denial of Service | Dynamic flow count unbounded (SDP plan time) | accept | Session 2 caps at 5 tables; stretch goal of 100 is a deliberate scaling test. If discovery ever returns >100, fixtures_discovery.py hard-caps the SELECT to LIMIT 100. |
| T-tkf-05 | Elevation of Privilege | Spike accidentally writes to production watermark_registry | mitigate | Hardcoded catalog.schema `devtest_edp_metadata.jdbc_spike.*` — no env interpolation, no LHP-core reuse. Readme explicitly documents this isolation. SQL `CREATE TABLE IF NOT EXISTS` prevents clobbering existing tables in the isolated schema. |
| T-tkf-06 | Repudiation | No audit trail for manifest state transitions | accept | Manifest itself records `started_at`/`completed_at`/`error_class`/`error_message` + spike watermark_registry records state transitions with timestamps. Delta time-travel on both tables provides forensic replay if needed. Full audit logging is post-spike work. |
</threat_model>

<verification>
All three tasks' automated checks must pass:

```bash
# Task 1
python -m py_compile spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
grep -q 'CREATE TABLE' spikes/jdbc-sdp-a1/ddl/manifest_table.sql
grep -q 'CREATE TABLE' spikes/jdbc-sdp-a1/ddl/watermark_registry_spike.sql

# Task 2
python -m py_compile spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py spikes/jdbc-sdp-a1/tasks/reconcile.py spikes/jdbc-sdp-a1/tasks/validate.py
grep -q 'from pyspark import pipelines as dp' spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py
grep -q 'dp.table(' spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py
! grep -q '^@dp.table' spikes/jdbc-sdp-a1/pipeline/sdp_pipeline.py

# Task 3
python -c "import yaml; yaml.safe_load(open('spikes/jdbc-sdp-a1/resources/spike_workflow.yml'))"
test -s spikes/jdbc-sdp-a1/README.md
```

Also verify no changes outside `spikes/jdbc-sdp-a1/`:
```bash
git diff --name-only | grep -v '^spikes/jdbc-sdp-a1/' && echo 'UNEXPECTED FILES' || echo 'scope clean'
```
</verification>

<success_criteria>
1. All 9 files exist under `spikes/jdbc-sdp-a1/` at the exact paths listed in `files_modified`.
2. All `.py` files compile (`python -m py_compile` exits 0).
3. `spike_workflow.yml` parses as valid YAML with expected structure.
4. Both `.sql` files contain `CREATE TABLE` for `devtest_edp_metadata.jdbc_spike.*`.
5. `sdp_pipeline.py` uses the dynamic-flow factory pattern (`dp.table(fn, name=...)` as a function call), not decorator-in-loop.
6. No file writes or SQL INSERT/UPDATE/MERGE calls inside any SDP flow body.
7. Zero modifications to `src/lhp/`.
8. Git diff is scoped entirely to `spikes/jdbc-sdp-a1/`.
</success_criteria>

<output>
After completion, create `.planning/quick/260423-tkf-spike-a1-session-2-sdp-federation-scaffo/260423-tkf-SUMMARY.md`
</output>
