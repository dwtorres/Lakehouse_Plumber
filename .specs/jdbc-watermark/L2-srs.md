# L2 - Software Requirements Specification: JDBC Watermark Incremental Load

**Feature**: Self-watermark JDBC incremental ingestion
**Version**: 1.0
**Date**: 2026-04-13
**Status**: Draft
**Traces to**: [Idea Doc](../../docs/ideas/jdbc-watermark-self-watermark-architecture.md)

---

## 1. Purpose

Add watermark-driven incremental JDBC ingestion to LHP. The load template reads its high-water mark (HWM) from the Bronze target table itself (self-watermark pattern), then issues a filtered JDBC query to the source database. This eliminates the Phase 3 timing bug in the current Wumbo-generated code where `mark_complete()` runs before Bronze has the data.

## 2. Scope

### 2.1 In Scope

- New `LoadSourceType.JDBC_WATERMARK` enum value
- New `WatermarkConfig` and `WatermarkType` Pydantic models
- New `watermark` field on `Action` model
- New `JDBCWatermarkLoadGenerator` generator class
- New `load/jdbc_watermark.py.j2` Jinja2 template
- Registration in `ActionRegistry`
- Validation rules in `LoadActionValidator`
- Unit tests for generator, template rendering, and validation
- Integration test with sample YAML configs

### 2.2 Out of Scope

- `jdbc_watermarks` audit table and post-pipeline audit action (Phase 2)
- Auto-chunking for first-time large table loads (Phase 2)
- Preflight profiling notebook (Phase 2)
- Lookback window configuration (Phase 2)
- Composite watermarks / multi-column (Phase 2)
- UUID watermark type (Phase 2)
- NULL watermark column handling beyond first-run detection (Phase 2)

## 3. Functional Requirements

### FR-1: JDBC Watermark Load Source Type

The system must support a new load source type `jdbc_watermark` that extends the existing `LoadSourceType` enum. When a flowgroup YAML specifies `source.type: jdbc_watermark`, the system must route to `JDBCWatermarkLoadGenerator`.

### FR-2: Watermark Configuration Model

The system must accept watermark configuration on load actions via a `watermark` field containing:
- `column` (required, string): The column name used as the high-water mark
- `type` (required, enum): Either `timestamp` or `numeric`

### FR-3: Self-Watermark HWM Resolution

The generated load code must read the current high-water mark by executing `SELECT MAX({watermark_column}) AS _hwm FROM {bronze_target}` against the Bronze target table. The Bronze target table name must be derived from substitution parameters `${catalog}.${bronze_schema}.{target_table}` where `target_table` comes from the write action's table name in the same flowgroup.

### FR-4: First-Run Detection

When the Bronze target table does not exist (first pipeline run), the generated code must catch the exception and set HWM to `None`, resulting in a full (unfiltered) JDBC load.

### FR-5: Filtered JDBC Query

The generated code must construct a JDBC pushdown query:
- When HWM is `None`: `SELECT * FROM {jdbc_table}` (full load)
- When HWM is not `None` and type is `timestamp`: `SELECT * FROM {jdbc_table} WHERE {watermark_column} >= '{hwm}'`
- When HWM is not `None` and type is `numeric`: `SELECT * FROM {jdbc_table} WHERE {watermark_column} >= {hwm}`

The default comparison operator must be `>=` (inclusive). The YAML config may override this to `>` via an optional `operator` field.

### FR-6: JDBC Connection Parameters

The generated code must support the same JDBC connection parameters as the existing `jdbc` load type:
- `url` (required): JDBC connection URL
- `user` (required): Username or secret reference
- `password` (required): Password or secret reference
- `driver` (required): JDBC driver class name
- `table` (required): Source table name with schema qualification

**Note**: The idea doc's Wumbo template example includes a `schema_name` field on the source config. This is a Wumbo template variable used for source-side schema qualification — it is not an LHP config field and is out of scope for this feature. LHP passes `table` (which already includes schema qualification, e.g., `"sales"."orders"`) directly to the JDBC reader.

### FR-7: JDBC Partitioning

The generated code must support optional JDBC read partitioning with parameters:
- `num_partitions`: Number of parallel JDBC readers
- `partition_column`: Column to partition on
- `lower_bound`: Lower bound for partitioning
- `upper_bound`: Upper bound for partitioning

These parameters are passed through to Spark's JDBC reader `.option()` calls.

### FR-8: Operational Metadata

The generated code must support the existing operational metadata pattern — adding metadata columns (e.g., `_lhp_loaded_at`, `_lhp_source_table`) to the DataFrame before returning, consistent with the base `_get_operational_metadata()` behavior.

### FR-9: DLT Temporary View Output

The generated code must produce a `@dp.temporary_view()` decorated function that returns a DataFrame, consistent with all existing LHP load generators.

### FR-10: Validation Rules

The `LoadActionValidator` must enforce:
- `watermark.column` is required when source type is `jdbc_watermark`
- `watermark.type` is required and must be `timestamp` or `numeric`
- Standard JDBC fields (`url`, `driver`, `table`) are required
- `user` and `password` are required (may be secret references)

## 4. Non-Functional Requirements

### NFR-1: Performance — HWM Query

The self-watermark `SELECT MAX()` query must be a metadata-only operation on liquid-clustered Delta tables with column statistics. It must not trigger a full table scan. This is a Delta Lake platform guarantee, not an LHP implementation concern, but the spec documents it as an assumption.

### NFR-2: Correctness — ACID Consistency

The HWM read must be consistent with actual written data. Delta Lake's ACID transactions guarantee this — the HWM reflects only committed data, not in-flight writes.

### NFR-3: Backward Compatibility

The addition must not break any existing LHP functionality:
- No changes to existing `LoadSourceType.JDBC` behavior
- No changes to existing templates or generators
- No changes to the `Action` model's existing fields (additive only)
- The `watermark` field on `Action` must be `Optional` with `None` default

### NFR-4: Code Generation Quality

Generated Python code must pass Black formatting (line-length=88) and be syntactically valid. The template must produce readable, self-documenting code.

## 5. External Interfaces

### 5.1 YAML Configuration Interface

```yaml
actions:
  - name: load_{table}_jdbc
    type: load
    readMode: batch
    source:
      type: jdbc_watermark
      url: "jdbc:postgresql://host:5432/db"
      user: "${scope/jdbc_user}"
      password: "${scope/jdbc_password}"
      driver: "org.postgresql.Driver"
      table: '"schema"."table"'
    watermark:
      column: "modified_date"
      type: timestamp
      # operator: ">="  # optional, default >=
    target: v_{table}_raw
```

### 5.2 Generated Code Interface

The template produces a Python function decorated with `@dp.temporary_view()` that:
1. Reads `MAX(watermark_column)` from the Bronze target table
2. Constructs a filtered JDBC pushdown query
3. Reads from the JDBC source using `spark.read.format("jdbc")`
4. Optionally adds operational metadata columns
5. Returns the DataFrame

### 5.3 Integration Points

| Component | Interface | Direction |
|-----------|-----------|-----------|
| `ActionRegistry` | `LoadSourceType.JDBC_WATERMARK → JDBCWatermarkLoadGenerator` | Registration |
| `LoadActionValidator` | `_validate_jdbc_watermark_source(action)` | Validation |
| `SubstitutionManager` | `${var}` resolution in source config | Input processing |
| `SecretCodeGenerator` | Secret placeholder replacement in generated code | Post-processing |
| Bronze target table | `SELECT MAX(col) FROM table` | Runtime HWM read |
| JDBC source database | Filtered `SELECT * FROM table WHERE col >= hwm` | Runtime data read |

## 6. Assumptions

| ID | Assumption | Risk if Wrong | Validation |
|----|-----------|---------------|------------|
| A1 | Delta `MAX()` on liquid-clustered column is metadata-only | Full scan on every pipeline trigger, slow | Benchmark with >100K rows |
| A2 | Table-not-exists exception is catchable in DLT runtime | First run fails instead of doing full load | Test in triggered DLT mode |
| A3 | DLT re-evaluates `@dp.temporary_view()` each trigger | Stale HWM, missed data | Test triggered mode with multiple runs |
| A4 | Bronze target name derivable from `${catalog}.${bronze_schema}.{table}` | Wrong table name in HWM query | Verify substitution params |
| A5 | `>=` operator with Silver CDC dedup handles ties safely | Duplicate rows if Silver doesn't dedup | Verify `create_auto_cdc_flow` with `sequence_by` |

## 7. Constraints

- Must generate code compatible with Databricks Lakeflow Spark Declarative Pipelines runtime
- Must use `pyspark.pipelines` import (`from pyspark import pipelines as dp`)
- Must not import WatermarkManager or any external dependency beyond PySpark
- Secret references (`${scope/key}`) must be handled by existing `SecretCodeGenerator`
- Generated code must be a single Python file per flowgroup (existing LHP pattern)
