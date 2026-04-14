# L3 - Feature Specification: JDBC Watermark Incremental Load

**Feature**: Self-watermark JDBC incremental ingestion
**Version**: 1.0
**Date**: 2026-04-13
**Status**: Draft
**Traces to**: [L2 SRS](L2-srs.md) FR-1 through FR-10

---

## 1. Feature Overview

Engineers configure a load action with `source.type: jdbc_watermark` in their flowgroup YAML. LHP generates a DLT temporary view function that reads the high-water mark from the Bronze target table, then issues a filtered JDBC query to the source database. On first run, it performs a full load. On subsequent runs, it loads only rows at or after the previous high-water mark.

## 2. Behavioral Specification

### 2.1 YAML Parsing and Model Validation

#### SC-1: Valid JDBC watermark configuration

```
Given a flowgroup YAML with a load action where source.type is "jdbc_watermark"
  And watermark.column is "modified_date"
  And watermark.type is "timestamp"
  And source contains url, user, password, driver, and table
When the configuration is parsed and validated
Then validation passes with zero errors
  And the Action model has watermark.column = "modified_date"
  And the Action model has watermark.type = WatermarkType.TIMESTAMP
```

#### SC-2: Missing watermark column

```
Given a flowgroup YAML with source.type "jdbc_watermark"
  And watermark.column is not specified
When the configuration is validated
Then validation fails with error message containing "watermark.column is required"
  And the error references the action name
```

#### SC-3: Missing watermark type

```
Given a flowgroup YAML with source.type "jdbc_watermark"
  And watermark.type is not specified
When the configuration is validated
Then validation fails with error message containing "watermark.type is required"
```

#### SC-4: Invalid watermark type

```
Given a flowgroup YAML with source.type "jdbc_watermark"
  And watermark.type is "uuid"
When the configuration is validated
Then validation fails with error message containing valid types "timestamp" or "numeric"
```

#### SC-5: Missing JDBC connection fields

```
Given a flowgroup YAML with source.type "jdbc_watermark"
  And source.url is not specified
When the configuration is validated
Then validation fails with error referencing the missing required field
```

#### SC-6: Watermark field on non-watermark source type

```
Given a flowgroup YAML with source.type "jdbc" (not jdbc_watermark)
  And a watermark section is present
When the configuration is validated
Then the watermark section is ignored (no error, no effect)
  And the standard JDBC generator handles the action
```

### 2.2 Code Generation

#### SC-7: Timestamp watermark — incremental load

```
Given a valid jdbc_watermark action with:
  - watermark.column = "modified_date"
  - watermark.type = "timestamp"
  - source.table = '"sales"."orders"'
  - target = "v_orders_raw"
  - Bronze target resolves to "${catalog}.${bronze_schema}.orders"
When the generator produces code
Then the generated code contains:
  - A function named "v_orders_raw" decorated with @dp.temporary_view()
  - A spark.sql() call: SELECT MAX(modified_date) AS _hwm FROM {bronze_target}
  - A try/except block around the HWM query with _hwm = None on exception
  - A conditional: if _hwm is not None, query includes WHERE modified_date >= '{hwm}'
  - A conditional: if _hwm is None, query selects all rows
  - spark.read.format("jdbc") with url, user, password, driver options
  - .option("dbtable", _query) for the filtered query
  - return df
```

#### SC-8: Numeric watermark — incremental load

```
Given a valid jdbc_watermark action with:
  - watermark.column = "product_id"
  - watermark.type = "numeric"
When the generator produces code
Then the WHERE clause uses numeric comparison: WHERE product_id >= {hwm}
  And the HWM value is NOT quoted (no single quotes around numeric values)
```

#### SC-9: First run — full load

```
Given the Bronze target table does not exist (first pipeline run)
When the generated code executes at runtime
Then the try/except catches the exception from MAX() query
  And _hwm is set to None
  And the JDBC query selects all rows (no WHERE clause)
  And the full source table is loaded into the temporary view
```

#### SC-10: Failed run — retry semantics

```
Given a previous pipeline run failed after HWM read but before Bronze write committed
When the next pipeline run executes
Then _hwm reflects the last successfully committed Bronze data
  And the JDBC query re-reads from the correct watermark position
  And no data is lost
```

#### SC-11: Manual rewind — re-load semantics

```
Given an operator deletes recent rows from the Bronze target table
When the next pipeline run executes
Then MAX(watermark_column) returns a lower value than before the delete
  And the JDBC query re-loads rows from the new (lower) watermark
  And deleted data is re-ingested
```

#### SC-12: JDBC partitioning

```
Given a jdbc_watermark action with:
  - source.num_partitions = "4"
  - source.partition_column = "order_id"
  - source.lower_bound = "1"
  - source.upper_bound = "1000000"
When the generator produces code
Then the generated code includes:
  - .option("numPartitions", 4)
  - .option("partitionColumn", "order_id")
  - .option("lowerBound", "1")
  - .option("upperBound", "1000000")
```

#### SC-13: JDBC partitioning absent

```
Given a jdbc_watermark action without partitioning parameters
When the generator produces code
Then the generated code does NOT include numPartitions, partitionColumn, lowerBound, or upperBound options
```

#### SC-14: Operational metadata

```
Given a jdbc_watermark action with operational_metadata enabled
  And the project defines metadata columns (_lhp_loaded_at, _lhp_source_table)
When the generator produces code
Then the generated code includes withColumn() calls for each metadata column
  And metadata columns appear after the JDBC read, before return df
```

#### SC-15: Operational metadata disabled

```
Given a jdbc_watermark action with operational_metadata: false
When the generator produces code
Then the generated code does NOT include any withColumn() calls for metadata
```

#### SC-16: Secret references in JDBC connection

```
Given a jdbc_watermark action with:
  - source.user = "${scope/jdbc_user}"
  - source.password = "${scope/jdbc_password}"
When the generator produces code
Then the secret placeholders are preserved in the generated code
  And the SecretCodeGenerator post-processor replaces them with dbutils.secrets.get() calls
```

#### SC-17: Substitution variables in Bronze target

```
Given a jdbc_watermark action where:
  - The flowgroup substitution defines catalog = "dev_catalog"
  - The flowgroup substitution defines bronze_schema = "bronze"
  - The write action table = "orders"
When the generator resolves the Bronze target
Then bronze_target = "dev_catalog.bronze.orders"
  And this string is embedded in the HWM query
```

#### SC-18: Custom comparison operator

```
Given a jdbc_watermark action with:
  - watermark.operator = ">"
When the generator produces code
Then the WHERE clause uses strict greater-than: WHERE modified_date > '{hwm}'
```

### 2.3 Registry and Routing

#### SC-19: Action registry routing

```
Given an action with type "load" and source.type "jdbc_watermark"
When the ActionRegistry resolves the generator
Then it returns an instance of JDBCWatermarkLoadGenerator
  And does NOT return JDBCLoadGenerator
```

#### SC-20: Existing JDBC routing unchanged

```
Given an action with type "load" and source.type "jdbc"
When the ActionRegistry resolves the generator
Then it returns JDBCLoadGenerator (unchanged behavior)
```

## 3. Edge Cases

| Case | Expected Behavior |
|------|-------------------|
| Bronze table exists but is empty | `MAX()` returns `NULL` → `_hwm = None` → full load |
| All source rows have same watermark value | `>=` re-reads them each run; Silver deduplicates via CDC |
| Watermark column has NULL values in source | `MAX()` ignores NULLs; rows with NULL watermark are never loaded incrementally |
| Very large first load (millions of rows) | Works but may be slow; Phase 2 adds auto-chunking |
| JDBC source is unreachable | Spark throws connection exception; DLT marks pipeline as failed |
| Bronze target is in a different catalog | Substitution params must match; `${catalog}` controls this |

## 4. Non-Goals (Explicit)

- No composite watermarks (multi-column HWM)
- No audit table writes from the load action
- No auto-detection of watermark column
- No schema evolution handling (existing DLT handles this)
- No custom HWM query override
- No support for `query` field (only `table` for watermark loads)

## 5. Acceptance Criteria

| ID | Criterion | Verification Method |
|----|-----------|-------------------|
| AC-1 | `LoadSourceType.JDBC_WATERMARK` exists in config.py | Unit test: enum membership |
| AC-2 | `WatermarkConfig` model validates column + type | Unit test: Pydantic validation |
| AC-3 | `Action.watermark` field is Optional[WatermarkConfig] | Unit test: model instantiation |
| AC-4 | `JDBCWatermarkLoadGenerator.generate()` produces valid Python | Unit test: syntax check on output |
| AC-5 | Generated code contains self-watermark MAX() query | Unit test: string assertion on output |
| AC-6 | Generated code handles first-run (table not exists) | Unit test: template rendering with None HWM |
| AC-7 | Timestamp watermark uses quoted HWM in WHERE clause | Unit test: string assertion |
| AC-8 | Numeric watermark uses unquoted HWM in WHERE clause | Unit test: string assertion |
| AC-9 | JDBC partitioning options render when configured | Unit test: template rendering |
| AC-10 | JDBC partitioning options absent when not configured | Unit test: template rendering |
| AC-11 | Operational metadata columns render when enabled | Unit test: template rendering |
| AC-12 | Validation rejects missing watermark.column | Unit test: validator |
| AC-13 | Validation rejects invalid watermark.type | Unit test: validator |
| AC-14 | ActionRegistry routes jdbc_watermark to correct generator | Unit test: registry lookup |
| AC-15 | Existing jdbc load type is unaffected | Regression test: existing JDBC tests pass |
| AC-16 | Generated code passes Black formatting | Unit test: black.format_str() check |
| AC-17 | Integration test renders complete flowgroup with watermark | Integration test: YAML → generated code |
