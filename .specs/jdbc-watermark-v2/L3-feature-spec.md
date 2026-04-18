# L3 - Feature Specification: JDBC Watermark v2 (Autoloader Landing)

**Feature**: JDBC watermark incremental ingestion via WatermarkManager + Autoloader landing zone
**Version**: 2.0
**Date**: 2026-04-14
**Status**: Draft
**Branch**: watermark
**Supersedes**: `.specs/jdbc-watermark/L3-feature-spec.md` (v1 — self-watermark pattern, abandoned)

---

## 1. Feature Overview

Engineers configure a load action with `source.type: jdbc_watermark` in their flowgroup YAML. LHP generates three artifacts from that single config: (1) a Databricks Job notebook that reads the latest high-water mark from WatermarkManager, issues a filtered JDBC query against the source database, and writes Parquet files to a Unity Catalog Volume landing path; (2) a DLT pipeline that reads those files via CloudFiles Autoloader into a Bronze streaming table with CDC enabled; and (3) a DAB Workflow YAML that chains the extraction Job as Task 1 and the DLT pipeline as Task 2. On first run the Job performs a full load. On subsequent runs it loads only rows at or after the previous watermark value. The DLT pipeline is architecturally identical to any other CloudFiles pipeline — no JDBC-specific logic lives inside DLT.

---

## 2. Behavioral Scenarios

### 2.1 First Run — No Watermark Exists

```
Given a flowgroup YAML with source.type: jdbc_watermark
  And the WatermarkManager table exists (or will be auto-created)
  And no prior watermark row exists for this source_system_id / schema_name / table_name
When the extraction Job runs
Then WatermarkManager.get_latest_watermark() returns None
  And the Job issues a JDBC query with no WHERE clause (full table read)
  And all rows are written as Parquet files to landing_path
  And WatermarkManager.insert_new() is called with:
    - extraction_type = "full"
    - watermark_value = MAX(watermark.column) from the loaded data
    - row_count = total rows written
  And the DLT pipeline (Task 2) ingests those files into the Bronze streaming table
```

### 2.2 Subsequent Run — Watermark Exists

```
Given a prior watermark row exists with watermark_value = "2025-06-01 00:00:00"
  And watermark.operator = ">=" (default)
When the extraction Job runs
Then WatermarkManager.get_latest_watermark() returns {"watermark_value": "2025-06-01 00:00:00", ...}
  And the Job issues a JDBC query:
    SELECT * FROM {source.table} WHERE {watermark.column} >= '2025-06-01 00:00:00'
  And only rows at or after that value are written to landing_path
  And WatermarkManager.insert_new() is called with:
    - extraction_type = "incremental"
    - watermark_value = MAX(watermark.column) from the landed batch
    - previous_watermark_value = "2025-06-01 00:00:00"
    - row_count = rows written in this batch
  And the DLT pipeline appends those rows to the Bronze streaming table
```

### 2.3 No New Data — Zero Rows Returned

```
Given the JDBC query returns 0 rows (no data modified since last watermark)
When the extraction Job runs
Then no Parquet files are written to landing_path
  And WatermarkManager.insert_new() is called with:
    - watermark_value = the same value as the previous watermark (no new MAX possible)
    - row_count = 0
    - extraction_type = "incremental"
  And the DLT pipeline runs but ingests 0 new rows (Autoloader sees no new files)
  And the pipeline completes successfully with no error
```

### 2.4 JDBC Connection Failure

```
Given the JDBC source database is unreachable or returns a connection error
When the extraction Job attempts the JDBC read
Then the Spark JDBC call raises an exception
  And the Job notebook propagates the exception (does not swallow it)
  And WatermarkManager.insert_new() is NOT called
  And no Parquet files are written to landing_path
  And Databricks Workflow marks Task 1 as failed
  And Task 2 (DLT pipeline) does not execute
  And the watermark state is unchanged — the next run will retry from the same HWM
```

### 2.5 Large Table — Partitioned JDBC Reads (Out of Scope for v2)

```
This scenario is deferred to a future version.

Rationale: Partitioned JDBC reads require source.partition_column, source.lower_bound,
source.upper_bound, and source.num_partitions. These interact with the watermark
WHERE clause in non-trivial ways (the partitionColumn must also be the watermark column
or the bounds must be recalculated each run). The complexity is out of proportion with
v2 MVP goals.

The v2 Job performs a single-threaded JDBC read. Engineers with very large tables should
use JDBC native pagination or a dedicated ingestion tool until partitioned support ships.

If a user provides source.num_partitions in the YAML, lhp validate must emit a warning
(not an error) that partitioned reads are unsupported for jdbc_watermark in v2.
```

### 2.6 WatermarkManager Table Does Not Exist

```
Given the WatermarkManager catalog.schema.watermarks table has never been created
When the extraction Job runs and instantiates WatermarkManager(spark, catalog, schema)
Then _ensure_table_exists() runs automatically during __init__
  And the watermarks Delta table is created with liquid clustering on
    (source_system_id, schema_name, table_name)
  And CDF is enabled on the new table
  And execution continues — no manual DDL is required from the engineer
```

### 2.7 Landing Path Does Not Exist

```
Given the Volume path specified in landing_path does not exist
  (e.g., /Volumes/catalog/schema/landing/product/ has no prior files)
When the extraction Job writes Parquet files to landing_path
Then Spark's DataFrameWriter creates the path automatically
  And files are written successfully
When the DLT pipeline runs CloudFiles against that path
  And it is the first pipeline run (no checkpoint)
Then Autoloader processes all files in the path from the beginning
  And all rows are ingested into the Bronze streaming table
```

### 2.8 Multiple Source Tables in One Flowgroup

```
Given a flowgroup YAML with two load actions, both using source.type: jdbc_watermark
  - load_product_jdbc targeting landing_path = ".../landing/product"
  - load_order_jdbc targeting landing_path = ".../landing/order"
When lhp generate runs
Then two extraction Job notebooks are generated:
    generated/{env}/{pipeline}/extract_product.py
    generated/{env}/{pipeline}/extract_order.py
  And one DLT pipeline file is generated per action:
    generated/{env}/{pipeline}/load_product_jdbc.py
    generated/{env}/{pipeline}/load_order_jdbc.py
  And one DAB Workflow YAML is generated for the pipeline:
    resources/lhp/{pipeline}_workflow.yml
  And the Workflow has two Job tasks (one per source table) followed by one DLT pipeline task
  And each Job task has its own WatermarkManager entry keyed by its own table identity
  And the DLT pipeline task depends on both Job tasks completing successfully
```

### 2.9 `lhp generate` Produces All Three Artifacts

```
Given a valid flowgroup YAML with source.type: jdbc_watermark
When the engineer runs: lhp generate
Then lhp generates:
    1. Extraction Job notebook:
       generated/{env}/{pipeline}/extract_{source.table_slug}.py
       - Uses JDBCWatermarkJobGenerator
       - Template: load/jdbc_watermark_job.py.j2
    2. DLT CloudFiles pipeline file:
       generated/{env}/{pipeline}/{action.name}.py
       - Uses existing CloudFilesLoadGenerator (no changes)
       - Template: load/cloudfiles.py.j2
       - source path = landing_path
    3. DAB Workflow resource:
       resources/lhp/{pipeline}_workflow.yml
       - Template: bundle/workflow_resource.yml.j2
       - Task 1: Databricks Job (extraction notebook)
       - Task 2: DLT pipeline (depends_on: Task 1)
  And lhp generate exits 0
  And all generated files pass Black formatting
```

### 2.10 `lhp validate` Checks landing_path for jdbc_watermark Sources

```
Given a flowgroup YAML with source.type: jdbc_watermark
  And the landing_path field is missing from the action
When the engineer runs: lhp validate
Then validation fails with error code LHP-VAL-xxx
  And the error message contains:
    "landing_path is required when source.type is jdbc_watermark"
  And the error references the action name
  And lhp validate exits non-zero

Given a flowgroup YAML with source.type: jdbc_watermark
  And landing_path is present and non-empty
When the engineer runs: lhp validate
Then validation passes for this field
```

---

## 3. Acceptance Criteria

| ID | Criterion | Pass Condition |
|----|-----------|---------------|
| AC-01 | `LoadSourceType.JDBC_WATERMARK` enum value exists | `LoadSourceType("jdbc_watermark")` does not raise |
| AC-02 | `Action.source.landing_path` field exists and is `Optional[str]` | Pydantic model instantiation with and without `landing_path` |
| AC-03 | Validator rejects `jdbc_watermark` action missing `landing_path` | `lhp validate` exits non-zero; error text contains "landing_path is required" |
| AC-04 | Validator rejects `jdbc_watermark` action missing `watermark.column` | Error text contains "watermark.column is required" |
| AC-05 | Validator rejects `jdbc_watermark` action missing `watermark.type` | Error text contains "watermark.type is required" |
| AC-06 | Validator rejects invalid `watermark.type` values | Error text lists valid values: "timestamp", "numeric" |
| AC-07 | `lhp validate` emits a warning (not error) when `source.num_partitions` is set on `jdbc_watermark` | Exit code 0; warning printed to stderr |
| AC-08 | `JDBCWatermarkJobGenerator` is registered in `ActionRegistry` for source type `jdbc_watermark` | Registry lookup returns correct generator class |
| AC-09 | Existing `jdbc` source type routing is unchanged | All existing JDBC generator unit tests pass without modification |
| AC-10 | Generated extraction notebook calls `WatermarkManager.get_latest_watermark()` | String assertion on template output |
| AC-11 | When `get_latest_watermark()` returns `None`, generated Job issues JDBC query with no WHERE clause | Template rendering test: None HWM path |
| AC-12 | When `get_latest_watermark()` returns a value, generated Job issues JDBC query with `WHERE {column} >= '{value}'` for timestamp type | Template rendering test: HWM present path |
| AC-13 | Numeric watermark type uses unquoted comparison: `WHERE {column} >= {value}` | Template rendering test: numeric branch |
| AC-14 | Custom `watermark.operator: ">"` renders strict greater-than in WHERE clause | Template rendering test: operator override |
| AC-15 | Generated extraction notebook calls `WatermarkManager.insert_new()` with correct parameters after successful write | String assertion on template output |
| AC-16 | `extraction_type` is `"full"` when HWM is None, `"incremental"` otherwise | Template rendering test: both branches |
| AC-17 | Generated DLT pipeline file uses CloudFiles (Autoloader) format with `landing_path` as source | String assertion: `format("cloudFiles")` and landing path present |
| AC-18 | Generated DAB Workflow YAML contains Task 1 (Job) → Task 2 (DLT pipeline) with `depends_on` | YAML parse of generated resource; assert dependency chain |
| AC-19 | `lhp generate` exits 0 for a valid `jdbc_watermark` flowgroup | CLI invocation test |
| AC-20 | All generated Python files pass Black formatting | `black.format_str()` on each generated file returns unchanged content |
| AC-21 | `lhp generate` produces all three artifacts (Job notebook, DLT file, Workflow YAML) | File existence assertions after generation |
| AC-22 | Multiple `jdbc_watermark` actions in one flowgroup produce one Job notebook per action | File count assertion |
| AC-23 | Secret references (`${scope/key}`) in `source.user` / `source.password` are preserved in generated Job notebook | String assertion: `dbutils.secrets.get` calls present |
| AC-24 | WatermarkManager `__init__` with non-existent table creates the table without error | Unit test against mock Spark (verify DDL SQL emitted) |

---

## 4. API / Config Contract

### 4.1 User-Facing YAML Structure

```yaml
# flowgroup.yaml — complete example with all fields

substitutions:
  catalog: my_catalog
  bronze_schema: bronze
  silver_schema: silver
  wm_catalog: metadata          # catalog where WatermarkManager stores its table
  wm_schema: orchestration      # schema where WatermarkManager stores its table

actions:
  # ── Load action (jdbc_watermark) ──────────────────────────────────────
  - name: load_product_jdbc             # required; drives generated file name
    type: load                          # required; fixed value
    source:
      type: jdbc_watermark              # required; triggers v2 generation path

      # JDBC connection (all required)
      url: "jdbc:postgresql://host:5432/mydb"
      user: "${secret:scope/jdbc_user}"
      password: "${secret:scope/jdbc_password}"
      driver: "org.postgresql.Driver"
      table: '"Production"."Product"'   # quoted for case-sensitive schemas

    watermark:
      column: "ModifiedDate"            # required; column used for HWM comparison
      type: "timestamp"                 # required; "timestamp" | "numeric"
      operator: ">="                    # optional; default ">="

    # Landing zone (required for jdbc_watermark)
    landing_path: "/Volumes/${catalog}/${bronze_schema}/landing/product"

    # WatermarkManager location (optional; defaults shown)
    watermark_manager:
      catalog: "${wm_catalog}"          # optional; default "metadata"
      schema: "${wm_schema}"            # optional; default "orchestration"

    # Source system identity for WatermarkManager key (optional; defaults shown)
    source_system_id: "postgresql_prod" # optional; default: derived from url host
    schema_name: "Production"           # optional; default: derived from table
    table_name: "Product"               # optional; default: derived from table

  # ── Write action (Bronze streaming table) ────────────────────────────
  - name: write_product_bronze
    type: write
    source: load_product_jdbc           # references the load action name
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: "product"
      table_properties:
        delta.enableChangeDataFeed: "true"
```

**Field reference:**

| Field | Required | Default | Notes |
|-------|----------|---------|-------|
| `source.type` | yes | — | Must be `jdbc_watermark` |
| `source.url` | yes | — | Full JDBC URL |
| `source.user` | yes | — | Supports `${secret:scope/key}` |
| `source.password` | yes | — | Supports `${secret:scope/key}` |
| `source.driver` | yes | — | Fully-qualified driver class name |
| `source.table` | yes | — | Source table; use double quotes for case-sensitive identifiers |
| `watermark.column` | yes | — | Column name for HWM comparison |
| `watermark.type` | yes | — | `"timestamp"` or `"numeric"` |
| `watermark.operator` | no | `">="` | Comparison operator in WHERE clause |
| `landing_path` | yes | — | Unity Catalog Volume path for Parquet landing files |
| `watermark_manager.catalog` | no | `"metadata"` | UC catalog for watermarks table |
| `watermark_manager.schema` | no | `"orchestration"` | UC schema for watermarks table |
| `source_system_id` | no | derived from URL host | WatermarkManager key component |
| `schema_name` | no | derived from `source.table` | WatermarkManager key component |
| `table_name` | no | derived from `source.table` | WatermarkManager key component |
| `source.num_partitions` | unsupported | — | Triggers warning; ignored in v2 |

### 4.2 WatermarkManager API Used by Generated Job

The generated extraction notebook uses exactly two WatermarkManager methods:

```python
from lhp.extensions.watermark_manager import WatermarkManager

wm = WatermarkManager(
    spark,
    catalog="{{ watermark_manager.catalog }}",
    schema="{{ watermark_manager.schema }}",
)

# Step 1: Read the latest HWM before JDBC extraction
prior = wm.get_latest_watermark(
    source_system_id="{{ source_system_id }}",
    schema_name="{{ schema_name }}",
    table_name="{{ table_name }}",
)
# Returns: Optional[Dict] with keys:
#   run_id, watermark_value, watermark_time, row_count, status,
#   bronze_stage_complete, silver_stage_complete
# Returns None if no prior watermark exists.

hwm_value = prior["watermark_value"] if prior is not None else None

# ... JDBC read with conditional WHERE clause ...
# ... Parquet write to landing_path ...

# Step 2: Record the new HWM after successful write
wm.insert_new(
    run_id=f"{{ pipeline_name }}_{{ '{{' }} run_id {{ '}}' }}",  # unique per execution
    source_system_id="{{ source_system_id }}",
    schema_name="{{ schema_name }}",
    table_name="{{ table_name }}",
    watermark_column_name="{{ watermark.column }}",
    watermark_value=str(new_hwm),           # MAX(watermark.column) from landed batch
    row_count=row_count,
    extraction_type="full" if hwm_value is None else "incremental",
    previous_watermark_value=hwm_value,
)
# insert_new() raises ValueError if run_id already exists (idempotency guard).
# insert_new() sets status='running'; v2 does not call mark_complete().
```

**Methods NOT used in v2 generated code** (available for future wiring):

| Method | Purpose | Deferred to |
|--------|---------|-------------|
| `mark_failed(run_id, error_message)` | Record Job failure in watermarks table | v3 |
| `mark_bronze_complete(run_id)` | Signal bronze stage done | v3 |
| `mark_silver_complete(run_id)` | Signal silver stage done | v3 |
| `mark_complete(run_id, watermark_value, row_count)` | Force-complete a run | v3 |
| `cleanup_stale_runs(stale_timeout_hours)` | Mark timed-out runs | v3 |
| `get_run_history(...)` | Observability queries | v3 |
| `get_failed_runs(...)` | Failure analysis | v3 |

---

## 5. Edge Cases

### 5.1 Landing Path Contains Files from Prior Runs

Autoloader maintains a checkpoint per DLT pipeline stream. Files already processed are recorded in the checkpoint; they are not re-ingested on subsequent pipeline runs. Old Parquet files in the landing path accumulate indefinitely unless the engineer purges them. This is acceptable behavior in v2. File accumulation is bounded by the size of incremental batches. A Volume lifecycle policy or scheduled cleanup notebook can purge old files — LHP does not manage this.

### 5.2 Extraction Job Fails Mid-Write (Partial Parquet Files)

If the extraction Job fails after writing some Parquet files but before completing the full write or calling `WatermarkManager.insert_new()`, the watermarks table is not updated. The next Job run re-reads from the same prior HWM and overwrites the partial files with a complete write. Autoloader's checkpoint records file-level progress; partially-written Parquet files that are not yet seen by Autoloader cause no issue. If Autoloader has already ingested the partial files, the CDC Bronze table will receive duplicate or incomplete rows — the Silver `create_auto_cdc_flow` deduplicates via the CDC key. Engineers must define a CDC key on the Silver table for this guarantee to hold.

### 5.3 Watermark Column Has NULL Values in Source

`WatermarkManager.get_latest_watermark()` returns the `watermark_value` from the watermarks table, not from the source database. The new HWM written by `insert_new()` is `MAX(watermark.column)` computed from the landed DataFrame. `MAX()` in Spark ignores NULL values. If all rows in the current batch have NULL in the watermark column, `MAX()` returns `null`. The Job must handle this case: if `new_hwm` is null, write `previous_watermark_value` unchanged to `insert_new()` so the next run does not regress. Rows with NULL watermark columns are loaded in full-load runs and in any incremental run where the WHERE clause includes them — the `WHERE col >= value` predicate evaluates to UNKNOWN for NULLs in SQL, so NULL-watermarked rows are excluded from incremental loads. This is a known limitation: rows that permanently have NULL in the watermark column are only ingested once (on first full load) and never re-ingested incrementally.

### 5.4 `>=` Operator Causes Duplicate Reads

The default `watermark.operator: ">="` re-reads the boundary row(s) from the source on each run (any row where `watermark_column = HWM_value`). Those rows are written again to the landing path as Parquet files. Autoloader processes them again, writing duplicate rows to Bronze. The Silver `create_auto_cdc_flow` deduplicates via Delta MERGE on the CDC key — this is the intended deduplication point. Engineers must define a CDC key on the Silver write target. Using `">"` instead avoids duplicate reads but risks missing rows added within the same timestamp millisecond as the prior HWM.

---

## 6. Non-Goals

This spec explicitly does not cover:

- **Partitioned JDBC reads** — `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound`. Deferred to v3.
- **Composite watermarks** — multi-column HWM or compound WHERE clauses.
- **Auto-detection of the watermark column** — engineers must specify `watermark.column` explicitly.
- **Failure recording in WatermarkManager** — `mark_failed()` is not called by v2 generated code. Databricks Workflow handles retries.
- **Stage tracking** — `mark_bronze_complete()`, `mark_silver_complete()`, `mark_complete()` are not called in v2.
- **Automatic file cleanup** — LHP does not manage Volume lifecycle or purge old Parquet files from the landing path.
- **Schema evolution** — handled transparently by CloudFiles Autoloader and Delta Lake; no LHP-specific handling needed.
- **Custom HWM query** — the HWM is always `MAX(watermark.column)` from the landed batch; no override.
- **Support for `source.query` field** — `jdbc_watermark` only supports `source.table`; raw SQL queries cannot be watermarked safely.
- **WatermarkManager as a shared external dependency** — in v2 the WatermarkManager module ships as part of LHP (`lhp.extensions.watermark_manager`) and must be available on the extraction Job cluster (LHP wheel installed).
- **HIPAA hashing in the extraction Job** — hashing is applied in the DLT transform layer, not in the Job notebook.
- **Backfill or rewind operations** — re-loading historical data requires manual watermark table manipulation; LHP provides no CLI command for this.
- **Cross-environment watermark sharing** — each environment (`devtest`, `qa`, `prod`) has its own `wm_catalog` / `wm_schema` and independent watermark state.
