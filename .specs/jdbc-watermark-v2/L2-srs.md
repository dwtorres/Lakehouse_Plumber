# L2 - Software Requirements Specification: JDBC Watermark v2

**Feature**: JDBC Watermark Incremental Ingestion via Autoloader Landing Zone
**Version**: 2.0
**Date**: 2026-04-14
**Status**: Draft
**Traces to**: [Idea Doc](../../../Wumbo/docs/ideas/jdbc-watermark-autoloader-landing.md)
**Supersedes**: N/A (v1 SRS covers a different architecture — self-watermark inside DLT)

---

## 1. Purpose

Extend LHP to support incremental JDBC ingestion by generating three coordinated artifacts from a single `source.type: jdbc_watermark` YAML config:

1. A **Databricks Job notebook** (extraction) that reads from JDBC and lands Parquet files
2. A **DLT pipeline** (ingestion) that reads the landing zone via Autoloader
3. A **DAB Workflow resource** (orchestration) that chains extraction → DLT

This architecture resolves the hard platform incompatibility between JDBC (batch) and DLT (streaming-only).

---

## 2. Scope

### 2.1 In Scope

- New `LoadSourceType.JDBC_WATERMARK_V2` enum value (distinct from existing `JDBC_WATERMARK`)
- New `landing_path` field on the `Action` model (optional, required only for `jdbc_watermark_v2`)
- New `watermark.source_system_id` sub-field on `WatermarkConfig`
- New `JDBCWatermarkJobGenerator` class — generates the extraction Job notebook
- New `load/jdbc_watermark_job.py.j2` Jinja2 template — extraction notebook body
- New `bundle/workflow_resource.yml.j2` Jinja2 template — DAB Workflow YAML
- New `WorkflowResourceGenerator` class — generates the DAB Workflow YAML
- Reuse of existing `CloudFilesLoadGenerator` + `cloudfiles.py.j2` for DLT ingestion (no changes)
- Reuse of existing `StreamingTableWriteGenerator` + `streaming_table.py.j2` for Bronze (no changes)
- Validation rules in `LoadActionValidator` for `jdbc_watermark_v2`
- Unit tests: generator, template rendering, validation
- Integration test: full YAML config → three-artifact output

### 2.2 Out of Scope

- Changes to `LoadSourceType.JDBC_WATERMARK` (v1 self-watermark pattern) — it remains untouched
- `WatermarkManager.mark_bronze_complete()`, `mark_silver_complete()`, `mark_failed()` — not wired in v2
- Partitioned JDBC reads (`num_partitions`, `partition_column`) — deferred to v3
- Landing path auto-truncation after DLT processing — deferred to v3
- Multiple JDBC source tables within a single flowgroup — one source per flowgroup
- Error retry logic inside the extraction notebook — Workflow task-level retries handle this
- Automatic `landing_path` default derivation — user must specify it explicitly in v2

---

## 3. Functional Requirements

### FR-01: New Source Type

The system **must** support a new load source type `jdbc_watermark_v2` via `LoadSourceType.JDBC_WATERMARK_V2`. When a flowgroup YAML specifies `source.type: jdbc_watermark_v2`, the system **must** route load code generation to `JDBCWatermarkJobGenerator` rather than `JDBCWatermarkLoadGenerator`.

### FR-02: Extraction Notebook Generation

`lhp generate` **must** produce an extraction notebook file at `generated/{env}/{pipeline}/extract_{action_name}.py` for each action with `source.type: jdbc_watermark_v2`. This file is a standard Python notebook (not a DLT file). It **must not** contain `@dp.temporary_view()` or any DLT decorators.

### FR-03: WatermarkManager HWM Lookup

The generated extraction notebook **must** instantiate `WatermarkManager` with the configured `catalog`, `schema`, and call `get_latest_watermark(source_system_id, schema_name, table_name)`. When the method returns `None` (no prior run), the notebook **must** treat HWM as absent and perform a full (unfiltered) JDBC load. When the method returns a dict, the notebook **must** use `result["watermark_value"]` as the filter bound.

### FR-04: JDBC Filtered Read

The extraction notebook **must** construct and execute a JDBC read using `spark.read.format("jdbc")`. The dbtable pushdown query **must** be:

- Full load (HWM is None): `(SELECT * FROM {jdbc_table}) AS t`
- Incremental load (HWM present, type `timestamp`): `(SELECT * FROM {jdbc_table} WHERE {watermark_column} >= '{hwm_value}') AS t`
- Incremental load (HWM present, type `numeric`): `(SELECT * FROM {jdbc_table} WHERE {watermark_column} >= {hwm_value}) AS t`

The comparison operator **must** default to `>=`. The YAML config **may** override it to `>` via `watermark.operator`.

### FR-05: Parquet Landing Write (amended by ADR-001, 2026-04-18)

The extraction notebook **must** write the JDBC result DataFrame as Parquet files to a **run-scoped subdirectory** under the configured `landing_path`:

```python
run_landing_path = f"{landing_path.rstrip('/')}/_lhp_runs/{run_id}"
df.write.mode("overwrite").format("parquet").save(run_landing_path)
```

The notebook **must** use `mode("overwrite")` scoped to the per-run subdirectory. It **must not** write to `landing_path` directly; landing path is a logical root, never a destination Spark writes to. Distinct `run_id` values (per FR-L-09) produce distinct subdirectories, so sibling runs never collide and `overwrite` is bounded to the owning run.

**Rationale**: Satisfies Constitution P6 (retry safety by construction). Also enables post-write stats derivation per FR-06.

**Supersedes**: prior `df.write.mode("append").format("parquet").save(landing_path)` into a shared path. The prior contract violated P6 and was discharged by Slice A waiver W-A-01, now discharged by this amendment.

### FR-06: WatermarkManager Watermark Lifecycle (amended by ADR-001, 2026-04-18)

The generated extraction notebook **must** commit the run lifecycle in the order `insert_new` → (JDBC read + landing write) → `mark_landed` → `mark_complete`. Slice A hardening (FR-L-01 through FR-L-09) adds the lifecycle control-flow contract (`insert_new` outside the try block, `mark_complete` outside the try block, `mark_failed` inside the except branch). This amendment pins the stats-derivation step:

1. Call `WatermarkManager.insert_new()` **before** the extraction try block with `run_count=0` and `watermark_value=<previous HWM or None>`. `run_id` is supplied by `derive_run_id(dbutils)` (FR-L-09), not built from pipeline/action/timestamp strings.
2. Inside the try block, read from JDBC, write the DataFrame to `run_landing_path` per FR-05.
3. Read the **landed Parquet** back and derive stats from it:
   ```python
   landed = spark.read.format("parquet").load(run_landing_path)
   stats = landed.agg(F.count("*").alias("row_count"), F.max(F.col(watermark_column)).alias("max_hwm")).first()
   row_count = int(stats["row_count"])
   new_hwm = stats["max_hwm"]
   ```
4. Call `WatermarkManager.mark_landed(run_id, watermark_value=str(new_hwm_or_previous), row_count=row_count)` to record the landed-not-committed intermediate state. This happens inside the post-write success branch.
5. Call `WatermarkManager.mark_complete(run_id, watermark_value=str(new_hwm_or_previous), row_count=row_count)` **outside** the try block to finalize (per FR-L-01).

On a subsequent run, the notebook **must** first call `WatermarkManager.get_recoverable_landed_run(source_system_id, schema_name, table_name)`. If it returns a non-None row, the notebook **must** finalize that prior run via `mark_complete` and exit via `dbutils.notebook.exit(...)` **without** reopening JDBC. This is the LANDED_NOT_COMMITTED recovery path required by Constitution P6.

**Supersedes**: prior requirement that `watermark_value` and `row_count` be computed via `df.agg({watermark_column: "max"}).collect()[0][0]` and `df.count()` **before** the write. Those pre-write aggregations issued independent JDBC queries and produced values that did not describe the landed bytes. See ADR-001 for the full analysis.

### FR-07: DLT Pipeline Generation (Autoloader)

For each `jdbc_watermark_v2` load action, `lhp generate` **must** also produce a DLT pipeline file using the existing `CloudFilesLoadGenerator`. The generated load action **must** read from `landing_path` using `spark.readStream.format("cloudFiles")`. The `cloudFiles.format` option **must** be `"parquet"`. This generation **must** reuse `cloudfiles.py.j2` without modification.

### FR-08: Bronze Streaming Table

The write action in the same flowgroup **must** produce a `@dp.table()` (streaming table) with `delta.enableChangeDataFeed = "true"` via the existing `StreamingTableWriteGenerator`. No changes to that generator are required.

### FR-09: DAB Workflow Resource Generation

`lhp generate` **must** produce a DAB Workflow YAML at `resources/lhp/{pipeline}_workflow.yml` when any action in the flowgroup has `source.type: jdbc_watermark_v2`. The Workflow **must** define exactly two tasks in dependency order:

1. **Task 1** (`extract_{action_name}`): Runs the extraction notebook. Type: `notebook_task`. `notebook_path` **must** point to the generated extraction notebook path.
2. **Task 2** (`dlt_{pipeline_name}`): Triggers the DLT pipeline. Type: `pipeline_task`. `pipeline_id` **must** reference the LHP-generated DLT pipeline by name using a DAB bundle reference.

Task 2 **must** declare `depends_on: [extract_{action_name}]`. The Workflow **must** inherit `job_clusters` or `existing_cluster_id` from the `bundle.yml` substitution parameters.

### FR-10: WatermarkManager Availability in Extraction Notebook

The generated extraction notebook **must** import `WatermarkManager` from `lhp.extensions.watermark_manager`. The DAB bundle configuration **must** install the `lhp` package as a library on the Job cluster. The `WorkflowResourceGenerator` **must** emit a `libraries` entry of type `whl` referencing the LHP wheel artifact in the DAB bundle.

### FR-11: YAML Configuration Fields

The following fields **must** be accepted in the YAML config for `source.type: jdbc_watermark_v2`:

| Field | Location | Required | Description |
|---|---|---|---|
| `source.url` | action | Yes | JDBC connection URL |
| `source.user` | action | Yes | Username or `${secret:scope/key}` |
| `source.password` | action | Yes | Password or `${secret:scope/key}` |
| `source.driver` | action | Yes | JDBC driver class |
| `source.table` | action | Yes | Fully-qualified source table (e.g. `"schema"."table"`) |
| `source.schema_name` | action | Yes | Unqualified schema name for WatermarkManager key |
| `source.table_name` | action | Yes | Unqualified table name for WatermarkManager key |
| `watermark.column` | action | Yes | Column used as high-water mark |
| `watermark.type` | action | Yes | `timestamp` or `numeric` |
| `watermark.operator` | action | No | `>=` (default) or `>` |
| `watermark.source_system_id` | action | Yes | Logical source system identifier for WatermarkManager |
| `watermark.catalog` | action | No | Catalog for WatermarkManager table; defaults to `"metadata"` |
| `watermark.schema` | action | No | Schema for WatermarkManager table; defaults to `"orchestration"` |
| `landing_path` | action | Yes | Absolute Volume path for Parquet landing files |
| `target` | action | Yes | Name of the DLT temporary view (Autoloader side) |

### FR-12: Validation

`LoadActionValidator` **must** enforce the following for `source.type: jdbc_watermark_v2`:

- `landing_path` is present and non-empty
- `watermark.column` is present and non-empty
- `watermark.type` is one of `timestamp`, `numeric`
- `watermark.source_system_id` is present and non-empty
- `source.schema_name` is present and non-empty
- `source.table_name` is present and non-empty
- `source.url`, `source.driver`, `source.table`, `source.user`, `source.password` are all present
- The same flowgroup **must** contain exactly one write action with `write_target.type: streaming_table`

Validation failures **must** be reported as `LHPValidationError` with an `LHP-VAL-*` error code and actionable suggestion text.

### FR-13: Source Type Discrimination

`lhp generate` **must** not invoke `JDBCWatermarkJobGenerator` for actions with `source.type: jdbc_watermark` (v1). The two source types **must** route to separate generators. Existing `jdbc_watermark` flowgroups **must** continue to generate as before.

---

## 4. Non-Functional Requirements

### NFR-01: Performance — JDBC Read Timeout

The generated extraction notebook **must** set `.option("queryTimeout", 3600)` on the JDBC reader. This caps a single JDBC read at one hour. Workflows exceeding this limit require manual partitioned-read configuration, which is out of scope for v2.

### NFR-02: Performance — Landing File Size (amended by ADR-001, 2026-04-18)

Under the run-scoped post-write-stats contract (FR-05, FR-06), the notebook does not have a pre-write `row_count` to branch on — stats derive from the landed Parquet after the write completes. The extraction notebook **should** default to a single repartition choice appropriate to the typical JDBC read shape (single connection → single partition → few output files). Dynamic repartition based on row-count thresholds is deferred to v3 and requires either a sampling step or a declarative hint from YAML.

**Supersedes**: prior rule that branched `df.repartition(1)` vs `df.repartition(4)` on pre-write `row_count < 1,000,000`. That branch relied on a pre-write count that FR-06 no longer computes.

### NFR-03: Reliability — Idempotency

`WatermarkManager.insert_new()` uses MERGE with `run_id` as the deduplication key. The generated `run_id` includes a timestamp component. If the extraction notebook is retried (e.g., by Workflow retry logic), it **will** generate a new `run_id` and a new landing write. Autoloader's checkpoint mechanism **must** prevent duplicate ingestion from re-read Parquet files. Duplicate landing writes from retries are acceptable in v2; cleanup is out of scope.

### NFR-04: Reliability — Extraction Atomicity

The extraction notebook **must** write Parquet files before calling `WatermarkManager.insert_new()`. If the Parquet write fails, `insert_new()` **must not** be called, leaving no dangling watermark record for a failed extraction.

### NFR-05: Reliability — WatermarkManager Table Bootstrap

`WatermarkManager.__init__()` calls `_ensure_table_exists()`, which creates the watermarks Delta table if absent. The generated extraction notebook **must** instantiate `WatermarkManager` before any JDBC read, so the watermarks table is guaranteed to exist before any insert attempt.

### NFR-06: Backward Compatibility

This feature **must not** change the behavior of any existing LHP source type, generator, template, or validator. All additions are strictly additive:

- `LoadSourceType.JDBC_WATERMARK` (v1) continues to route to `JDBCWatermarkLoadGenerator` unchanged
- The `Action` model's new `landing_path` field **must** be `Optional[str]` with `None` default
- The new `watermark.source_system_id`, `watermark.catalog`, `watermark.schema` fields **must** be `Optional` with documented defaults

### NFR-07: Code Generation Quality

All generated Python (extraction notebook) **must** be Black-compliant at line-length=88. All generated YAML (Workflow resource) **must** be valid YAML. LHP's existing `black.format_str()` post-processing step applies to the extraction notebook.

---

## 5. External Interfaces

### 5.1 JDBC Sources

The generated extraction notebook **must** support any JDBC-compatible database via driver class configuration. Tested drivers in scope for v2:

| Database | Driver Class |
|---|---|
| PostgreSQL | `org.postgresql.Driver` |
| SQL Server | `com.microsoft.sqlserver.jdbc.SQLServerDriver` |
| Oracle | `oracle.jdbc.OracleDriver` |
| MySQL | `com.mysql.cj.jdbc.Driver` |

The JDBC driver JAR **must** be installed on the Job cluster via the DAB bundle `libraries` configuration. Driver JAR installation is the user's responsibility; LHP emits a comment in the generated Workflow YAML directing the user to add driver JARs.

### 5.2 WatermarkManager Delta Table

`WatermarkManager` reads and writes `{catalog}.{schema}.watermarks` (default: `metadata.orchestration.watermarks`). The extraction notebook requires SELECT, INSERT, and UPDATE privileges on this table. The table is created automatically by `WatermarkManager._ensure_table_exists()` on first run.

**API contract (v2 scope — two methods only):**

```python
WatermarkManager.get_latest_watermark(
    source_system_id: str,
    schema_name: str,
    table_name: str,
) -> Optional[Dict[str, Any]]
# Returns dict with "watermark_value" key, or None if no prior run.

WatermarkManager.insert_new(
    run_id: str,
    source_system_id: str,
    schema_name: str,
    table_name: str,
    watermark_column_name: Optional[str],
    watermark_value: Optional[str],
    row_count: int,
    extraction_type: str,
    previous_watermark_value: Optional[str] = None,
) -> None
```

All other `WatermarkManager` methods (`mark_bronze_complete`, `mark_silver_complete`, `mark_failed`, `mark_complete`, etc.) are **not** called by v2 generated code.

### 5.3 Volume / Cloud Storage Landing Path

The `landing_path` **must** be an absolute path to a Databricks Unity Catalog Volume (e.g., `/Volumes/{catalog}/{schema}/landing/{table}`). The Job cluster's service principal **must** have WRITE permission on the Volume. The DLT pipeline's service principal **must** have READ permission. LHP does not create or validate the Volume — it only references the path in generated code.

### 5.4 Databricks Asset Bundles (DAB)

The Workflow resource YAML **must** be valid DAB YAML compatible with Databricks CLI `bundle deploy`. The generated file **must** use DAB reference syntax for pipeline IDs (e.g., `${resources.pipelines.{pipeline_name}.id}`). The template **must** emit a `run_as` block using a service principal reference from bundle substitution parameters.

---

## 6. Constraints

- Generated extraction notebooks **must** use `spark.read` (batch), never `spark.readStream`.
- Generated DLT pipeline files **must** use `spark.readStream.format("cloudFiles")` (streaming). The existing `CloudFilesLoadGenerator` already enforces this.
- The extraction notebook **must** import `WatermarkManager` from `lhp.extensions.watermark_manager`. It **must not** inline the `WatermarkManager` source.
- Secret references (`${secret:scope/key}`) in `source.user` and `source.password` **must** be rendered as `dbutils.secrets.get(scope="scope", key="key")` calls in the generated notebook. Existing `SecretCodeGenerator` handles this substitution.
- Each flowgroup with `source.type: jdbc_watermark_v2` **must** produce exactly one extraction notebook, one DLT file, and one Workflow YAML. LHP **must not** silently skip any artifact.
- The Workflow YAML **must** be written to `resources/lhp/`, not to `generated/`. This matches the existing DAB bundle directory convention.
- LHP **must** emit a warning (not an error) if the `landing_path` does not begin with `/Volumes/`. Alternative paths (ABFSS, S3, GCS) are technically valid but untested in v2.

---

## 7. Generated Artifact Summary

| Artifact | Template | Output Path | Generator |
|---|---|---|---|
| Extraction notebook | `load/jdbc_watermark_job.py.j2` (new) | `generated/{env}/{pipeline}/extract_{action_name}.py` | `JDBCWatermarkJobGenerator` (new) |
| DLT pipeline (Autoloader) | `load/cloudfiles.py.j2` (existing, unchanged) | `generated/{env}/{pipeline}/{target}.py` | `CloudFilesLoadGenerator` (existing) |
| DAB Workflow resource | `bundle/workflow_resource.yml.j2` (new) | `resources/lhp/{pipeline}_workflow.yml` | `WorkflowResourceGenerator` (new) |

---

## 8. Assumptions

| ID | Assumption | Risk if Wrong | Validation |
|---|---|---|---|
| A1 | Autoloader checkpoint prevents re-ingestion of Parquet files already processed | Duplicate Bronze rows on Job retry | Test with Autoloader checkpoint + file re-land |
| A2 | Databricks Volume paths are accessible from both Job cluster and DLT pipeline cluster under the same service principal | Permission errors at runtime | Verify IAM / Volume grants in devtest |
| A3 | `WatermarkManager._ensure_table_exists()` is idempotent and safe to call on every Job run | Table creation errors on concurrent first runs | Review `CREATE TABLE IF NOT EXISTS` DDL |
| A4 | DAB `${resources.pipelines.X.id}` reference resolves correctly when Workflow and Pipeline are in the same bundle | Broken pipeline_task link at deploy time | `bundle validate` in integration test |
| A5 | ~~`df.count()` before write does not trigger a second full JDBC fetch (Spark caches the DataFrame after first action)~~ — **SUPERSEDED 2026-04-18 by ADR-001**. The assumption was materially wrong: uncached Spark DataFrames re-evaluate on every action, so any pre-write `df.count()` or `df.agg(...)` issued an independent JDBC query that could diverge from the eventual `df.write`. ADR-001 removes the assumption's load-bearing role by moving stats derivation to a post-write Parquet read. Retained here only for traceability. | N/A (assumption retired) | N/A (assumption retired) |
| A6 | `WatermarkManager` identifier validation (alphanumeric + underscore + dot + hyphen) permits typical JDBC schema and table names | `insert_new()` raises `ValueError` for names with spaces or special chars | Validate against real source table names in devtest |
