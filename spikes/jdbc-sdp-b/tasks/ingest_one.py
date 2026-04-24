# Databricks notebook source

# MAGIC %md
# MAGIC ## ingest_one — JDBC SDP Spike B (per-iteration notebook)
# MAGIC
# MAGIC Executed once per manifest row by the `for_each_ingest` task.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `manifest_row` — JSON string with all manifest fields (from task value).
# MAGIC - `run_id`       — Job run ID (for context; also embedded in manifest_row).
# MAGIC
# MAGIC **Execution flow per iteration:**
# MAGIC 1. Parse manifest_row JSON.
# MAGIC 2. Read from `pg_supabase.<source_schema>.<source_table>` with watermark
# MAGIC    predicate `ModifiedDate > '<hwm_at_start>'`. Full load if HWM is EPOCH_ZERO.
# MAGIC 3. Drop `decimal(38,10)` columns defensively (no-op for PG decimal(19,4),
# MAGIC    but preserves parity with the Oracle path in A1).
# MAGIC 4. Append to `devtest_edp_bronze.jdbc_spike_b.<target_table>`.
# MAGIC    mode("append") for watermark-driven incremental loads.
# MAGIC 5. UPDATE manifest row: execution_status, rows_written, completed_at.
# MAGIC 6. UPDATE watermark_registry row: status='completed', row_count, watermark_value.
# MAGIC
# MAGIC **Failure isolation:**
# MAGIC The entire ingestion + state update block is wrapped in try/except.
# MAGIC Exceptions are caught and recorded as execution_status='failed' in the
# MAGIC manifest. The exception is NOT re-raised — this prevents a single table
# MAGIC failure from cancelling other for_each iterations.
# MAGIC
# MAGIC **Contrast with Spike A1:**
# MAGIC A1 uses a reconcile task that reads SDP event_log() after the pipeline
# MAGIC completes to infer per-flow success/failure. Spike B eliminates that
# MAGIC post-processing step by doing inline state updates here.

# COMMAND ----------

import json
import traceback
from decimal import Decimal

from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

dbutils.widgets.text("manifest_row", "{}", "Manifest row JSON (from task value)")
dbutils.widgets.text("run_id", "", "Job run ID (for context)")

# COMMAND ----------

manifest_row_raw = dbutils.widgets.get("manifest_row")
run_id_widget = dbutils.widgets.get("run_id")

try:
    row = json.loads(manifest_row_raw)
except json.JSONDecodeError as exc:
    raise ValueError(f"manifest_row is not valid JSON: {exc}\nRaw: {manifest_row_raw!r}")

# Extract fields from the manifest row.
run_id = row["run_id"]
load_group = row.get("load_group", "spike_b")
source_catalog = row["source_catalog"]
source_schema = row["source_schema"]
source_table = row["source_table"]
target_catalog = row.get("target_catalog", "devtest_edp_bronze")
target_schema = row.get("target_schema", "jdbc_spike_b")
target_table = row["target_table"]
watermark_column = row["watermark_column"]
hwm_at_start = row["watermark_value_at_start"]
extraction_type = row.get("extraction_type", "incremental")

EPOCH_ZERO = "1900-01-01T00:00:00"

source_fqn = f"`{source_catalog}`.`{source_schema}`.`{source_table}`"
target_fqn = f"`{target_catalog}`.`{target_schema}`.`{target_table}`"

print(json.dumps({
    "source": source_fqn,
    "target": target_fqn,
    "hwm_at_start": hwm_at_start,
    "extraction_type": extraction_type,
}))

# COMMAND ----------

rows_written = 0
error_class = None
error_message = None
iteration_success = False

try:
    # Mark started_at in manifest.
    spark.sql(
        """
        UPDATE devtest_edp_orchestration.jdbc_spike.manifest
        SET started_at = current_timestamp()
        WHERE run_id         = :run_id
          AND load_group     = :load_group
          AND source_schema  = :source_schema
          AND source_table   = :source_table
        """,
        args={
            "run_id": run_id,
            "load_group": load_group,
            "source_schema": source_schema,
            "source_table": source_table,
        },
    )

    # --- Read from federation ---
    # Use full-load (no filter) when HWM is EPOCH_ZERO; otherwise apply
    # the watermark predicate for incremental extraction.
    if hwm_at_start == EPOCH_ZERO:
        df = spark.read.table(source_fqn)
    else:
        df = spark.read.table(source_fqn).filter(
            F.col(watermark_column) > hwm_at_start
        )

    # --- Drop decimal(38,10) columns defensively ---
    # For PG decimal(19,4) this is a no-op. Included for A1 parity so the
    # same ingest_one.py can be adapted to Oracle sources without change.
    decimal_38_10_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, T.DecimalType)
        and field.dataType.precision == 38
        and field.dataType.scale == 10
    ]
    if decimal_38_10_cols:
        print(f"Dropping decimal(38,10) columns: {decimal_38_10_cols}")
        df = df.drop(*decimal_38_10_cols)

    # --- Write to bronze ---
    # mode("append") is correct for watermark-driven incremental.
    # First-run behavior: table does not exist yet → append creates it.
    rows_written = df.count()
    df.write.format("delta").mode("append").saveAsTable(target_fqn)

    # --- Advance watermark: compute MAX(watermark_column) from what we read ---
    max_hwm_row = df.agg(F.max(watermark_column).alias("max_hwm")).first()
    new_hwm = str(max_hwm_row["max_hwm"]) if max_hwm_row and max_hwm_row["max_hwm"] else hwm_at_start

    # --- Update manifest: mark completed ---
    spark.sql(
        """
        UPDATE devtest_edp_orchestration.jdbc_spike.manifest
        SET execution_status = 'completed',
            completed_at     = current_timestamp(),
            rows_written     = :rows_written
        WHERE run_id         = :run_id
          AND load_group     = :load_group
          AND source_schema  = :source_schema
          AND source_table   = :source_table
        """,
        args={
            "run_id": run_id,
            "load_group": load_group,
            "source_schema": source_schema,
            "source_table": source_table,
            "rows_written": rows_written,
        },
    )

    # --- Update watermark_registry: mark completed ---
    spark.sql(
        """
        UPDATE devtest_edp_orchestration.jdbc_spike.watermark_registry
        SET status     = 'completed',
            row_count  = :row_count,
            watermark_value = :new_hwm,
            updated_at = current_timestamp()
        WHERE run_id      = :run_id
          AND schema_name = :schema_name
          AND table_name  = :table_name
        """,
        args={
            "run_id": run_id,
            "schema_name": source_schema,
            "table_name": source_table,
            "row_count": rows_written,
            "new_hwm": new_hwm,
        },
    )

    iteration_success = True
    print(json.dumps({
        "status": "completed",
        "source": f"{source_schema}.{source_table}",
        "target": target_fqn,
        "rows_written": rows_written,
        "new_hwm": new_hwm,
    }))

except Exception as exc:
    # Capture failure details without re-raising.
    # This is the key isolation guarantee: one table's failure does NOT
    # cancel other for_each iterations.
    error_class = type(exc).__name__
    error_message = str(exc)[:4096]
    tb = traceback.format_exc()

    print(json.dumps({
        "status": "failed",
        "source": f"{source_schema}.{source_table}",
        "error_class": error_class,
        "error_message": error_message,
    }))
    print(f"Traceback:\n{tb}")

    try:
        # Update manifest: mark failed.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.manifest
            SET execution_status = 'failed',
                completed_at     = current_timestamp(),
                error_class      = :error_class,
                error_message    = :error_message
            WHERE run_id         = :run_id
              AND load_group     = :load_group
              AND source_schema  = :source_schema
              AND source_table   = :source_table
            """,
            args={
                "run_id": run_id,
                "load_group": load_group,
                "source_schema": source_schema,
                "source_table": source_table,
                "error_class": error_class,
                "error_message": error_message,
            },
        )

        # Update watermark_registry: mark failed.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.watermark_registry
            SET status        = 'failed',
                error_class   = :error_class,
                error_message = :error_message,
                updated_at    = current_timestamp()
            WHERE run_id      = :run_id
              AND schema_name = :schema_name
              AND table_name  = :table_name
            """,
            args={
                "run_id": run_id,
                "schema_name": source_schema,
                "table_name": source_table,
                "error_class": error_class,
                "error_message": error_message,
            },
        )
    except Exception as state_exc:
        # State update failure is logged but also not re-raised.
        print(f"WARNING: state update failed for {source_schema}.{source_table}: {state_exc}")

# COMMAND ----------

# Final summary for this iteration — surfaced in the for_each task log.
print(json.dumps({
    "iteration_complete": True,
    "source": f"{source_schema}.{source_table}",
    "success": iteration_success,
    "rows_written": rows_written,
    "error_class": error_class,
}))
