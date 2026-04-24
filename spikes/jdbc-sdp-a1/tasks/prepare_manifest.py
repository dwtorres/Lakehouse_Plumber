# Databricks notebook source

# MAGIC %md
# MAGIC ## prepare_manifest — JDBC SDP A1 Spike
# MAGIC
# MAGIC Pre-run bookend task. Inserts rows into the spike manifest and the isolated
# MAGIC spike watermark registry before the SDP pipeline executes.
# MAGIC
# MAGIC **Supports two modes:**
# MAGIC - `fresh`: Normal run — inserts one manifest row per selected source with
# MAGIC   `execution_status='pending'`. Captures the current HWM from the spike
# MAGIC   watermark registry (or epoch-zero if no prior completed run exists).
# MAGIC - `failed_only`: Rerun of a prior failed run — reads failed manifest rows
# MAGIC   for the `parent_run_id` and inserts new pending rows for the current
# MAGIC   `run_id`. Reuses the original `watermark_value_at_start` (does NOT
# MAGIC   advance the HWM on retry, matching L2 §5.3 recovery semantics).
# MAGIC
# MAGIC **Watermark state machine:** this notebook implements `insert_new` logic
# MAGIC inline as plain `spark.sql` INSERTs — no dependency on `lhp_watermark`.
# MAGIC
# MAGIC **Security:** T-tkf-01 — `run_id` and `parent_run_id` are validated by
# MAGIC regex (alphanumeric + dash + underscore) before any SQL composition.
# MAGIC All dynamic values are bound via `spark.sql(..., args={...})`.

# COMMAND ----------

import json
import re

from pyspark.sql import functions as F

# COMMAND ----------

# Widgets — operator overrides these at job trigger time.
dbutils.widgets.text("run_id", "", "Run ID (required, non-empty)")
dbutils.widgets.text("load_group", "spike_a1", "Load group label")
dbutils.widgets.text(
    "rerun_mode", "fresh", "Rerun mode: fresh | failed_only"
)
dbutils.widgets.text(
    "parent_run_id",
    "",
    "Parent run ID (required when rerun_mode=failed_only)",
)

# COMMAND ----------

# Read and validate widget values.
# T-tkf-01: run_id / parent_run_id must match alphanumeric-dash-underscore
# to prevent SQL injection via f-string composition in any fallback paths.

_RUN_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,256}$")


def _validate_run_id(value: str, name: str) -> str:
    """Raise ValueError if value is empty or contains unsafe characters."""
    if not value:
        raise ValueError(f"Widget '{name}' must not be empty.")
    if not _RUN_ID_PATTERN.match(value):
        raise ValueError(
            f"Widget '{name}' value {value!r} is invalid. "
            "Only alphanumeric, dash, and underscore characters are allowed."
        )
    return value


run_id = _validate_run_id(dbutils.widgets.get("run_id"), "run_id")
load_group = dbutils.widgets.get("load_group") or "spike_a1"
rerun_mode = dbutils.widgets.get("rerun_mode") or "fresh"
parent_run_id_raw = dbutils.widgets.get("parent_run_id")

if rerun_mode not in ("fresh", "failed_only"):
    raise ValueError(
        f"rerun_mode must be 'fresh' or 'failed_only', got {rerun_mode!r}"
    )

if rerun_mode == "failed_only":
    if not parent_run_id_raw:
        raise ValueError(
            "parent_run_id must be set when rerun_mode='failed_only'."
        )
    parent_run_id = _validate_run_id(parent_run_id_raw, "parent_run_id")
else:
    parent_run_id = None

print(
    json.dumps(
        {
            "run_id": run_id,
            "load_group": load_group,
            "rerun_mode": rerun_mode,
            "parent_run_id": parent_run_id,
        }
    )
)

# COMMAND ----------

# Shared helper: look up the latest completed HWM for a given table from the
# spike watermark registry.  Returns epoch-zero string if no completed run.
EPOCH_ZERO = "1900-01-01T00:00:00"


def get_current_hwm(schema_name: str, table_name: str) -> str:
    """Return the latest completed HWM for this table, or EPOCH_ZERO."""
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name   = :schema_name
          AND table_name    = :table_name
          AND status        = 'completed'
        """,
        args={"schema_name": schema_name, "table_name": table_name},
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO


# COMMAND ----------

manifest_rows_inserted = 0
registry_rows_inserted = 0

if rerun_mode == "fresh":
    # Read the fixture selection populated by fixtures_discovery.py.
    sources_df = spark.read.table(
        "devtest_edp_orchestration.jdbc_spike.selected_sources"
    )
    sources = sources_df.collect()

    for row in sources:
        src_catalog = row["source_catalog"]
        src_schema = row["source_schema"]
        src_table = row["source_table"]
        watermark_col = row["watermark_column"]
        target_table = row["target_table"]

        hwm_at_start = get_current_hwm(src_schema, src_table)
        extraction_type = (
            "incremental" if hwm_at_start != EPOCH_ZERO else "full"
        )
        prev_hwm = hwm_at_start if hwm_at_start != EPOCH_ZERO else None

        # Insert manifest row.
        spark.sql(
            """
            INSERT INTO devtest_edp_orchestration.jdbc_spike.manifest
              (run_id, load_group, source_catalog, source_schema, source_table,
               target_table, watermark_column, watermark_value_at_start,
               execution_status, started_at, completed_at,
               error_class, error_message, retry_count, parent_run_id,
               rows_written)
            VALUES
              (:run_id, :load_group, :source_catalog, :source_schema, :source_table,
               :target_table, :watermark_column, :hwm_at_start,
               'pending', NULL, NULL,
               NULL, NULL, 0, NULL,
               NULL)
            """,
            args={
                "run_id": run_id,
                "load_group": load_group,
                "source_catalog": src_catalog,
                "source_schema": src_schema,
                "source_table": src_table,
                "target_table": target_table,
                "watermark_column": watermark_col,
                "hwm_at_start": hwm_at_start,
            },
        )
        manifest_rows_inserted += 1

        # Insert spike watermark registry row (insert_new equivalent).
        spark.sql(
            """
            INSERT INTO devtest_edp_orchestration.jdbc_spike.watermark_registry
              (run_id, source_system_id, schema_name, table_name,
               watermark_column_name, watermark_value,
               previous_watermark_value, row_count,
               extraction_type, status, error_class, error_message,
               created_at, updated_at)
            VALUES
              (:run_id, :source_system_id, :schema_name, :table_name,
               :watermark_column_name, :watermark_value,
               :previous_watermark_value, 0,
               :extraction_type, 'pending', NULL, NULL,
               current_timestamp(), current_timestamp())
            """,
            args={
                "run_id": run_id,
                "source_system_id": src_catalog,
                "schema_name": src_schema,
                "table_name": src_table,
                "watermark_column_name": watermark_col,
                "watermark_value": hwm_at_start,
                "previous_watermark_value": prev_hwm,
                "extraction_type": extraction_type,
            },
        )
        registry_rows_inserted += 1

elif rerun_mode == "failed_only":
    # Rerun mode: pick up only the failed rows from the parent run.
    # parent_run_id is already validated above.
    failed_df = spark.sql(
        """
        SELECT run_id, load_group, source_catalog, source_schema, source_table,
               target_table, watermark_column, watermark_value_at_start,
               retry_count
        FROM devtest_edp_orchestration.jdbc_spike.manifest
        WHERE run_id            = :parent_run_id
          AND execution_status  = 'failed'
        """,
        args={"parent_run_id": parent_run_id},
    )

    failed_rows = failed_df.collect()
    if not failed_rows:
        print(
            json.dumps(
                {
                    "warning": "no failed rows found for parent_run_id",
                    "parent_run_id": parent_run_id,
                }
            )
        )

    for row in failed_rows:
        src_catalog = row["source_catalog"]
        src_schema = row["source_schema"]
        src_table = row["source_table"]
        watermark_col = row["watermark_column"]
        target_table = row["target_table"]
        # Reuse original HWM — do NOT advance on retry (L2 §5.3 recovery).
        hwm_at_start = row["watermark_value_at_start"]
        new_retry_count = (row["retry_count"] or 0) + 1

        # Determine extraction_type from prior registry entry.
        prior_type_row = spark.sql(
            """
            SELECT extraction_type
            FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
            WHERE run_id       = :parent_run_id
              AND schema_name  = :schema_name
              AND table_name   = :table_name
            LIMIT 1
            """,
            args={
                "parent_run_id": parent_run_id,
                "schema_name": src_schema,
                "table_name": src_table,
            },
        ).first()
        extraction_type = (
            prior_type_row["extraction_type"]
            if prior_type_row
            else "incremental"
        )
        prev_hwm = (
            hwm_at_start
            if hwm_at_start and hwm_at_start != EPOCH_ZERO
            else None
        )

        # Insert fresh manifest row for the new run_id.
        spark.sql(
            """
            INSERT INTO devtest_edp_orchestration.jdbc_spike.manifest
              (run_id, load_group, source_catalog, source_schema, source_table,
               target_table, watermark_column, watermark_value_at_start,
               execution_status, started_at, completed_at,
               error_class, error_message, retry_count, parent_run_id,
               rows_written)
            VALUES
              (:run_id, :load_group, :source_catalog, :source_schema, :source_table,
               :target_table, :watermark_column, :hwm_at_start,
               'pending', NULL, NULL,
               NULL, NULL, :retry_count, :parent_run_id,
               NULL)
            """,
            args={
                "run_id": run_id,
                "load_group": load_group,
                "source_catalog": src_catalog,
                "source_schema": src_schema,
                "source_table": src_table,
                "target_table": target_table,
                "watermark_column": watermark_col,
                "hwm_at_start": hwm_at_start,
                "retry_count": new_retry_count,
                "parent_run_id": parent_run_id,
            },
        )
        manifest_rows_inserted += 1

        # Insert fresh pending registry row for the retry run.
        spark.sql(
            """
            INSERT INTO devtest_edp_orchestration.jdbc_spike.watermark_registry
              (run_id, source_system_id, schema_name, table_name,
               watermark_column_name, watermark_value,
               previous_watermark_value, row_count,
               extraction_type, status, error_class, error_message,
               created_at, updated_at)
            VALUES
              (:run_id, :source_system_id, :schema_name, :table_name,
               :watermark_column_name, :watermark_value,
               :previous_watermark_value, 0,
               :extraction_type, 'pending', NULL, NULL,
               current_timestamp(), current_timestamp())
            """,
            args={
                "run_id": run_id,
                "source_system_id": src_catalog,
                "schema_name": src_schema,
                "table_name": src_table,
                "watermark_column_name": watermark_col,
                "watermark_value": hwm_at_start,
                "previous_watermark_value": prev_hwm,
                "extraction_type": extraction_type,
            },
        )
        registry_rows_inserted += 1

# COMMAND ----------

print(
    json.dumps(
        {
            "run_id": run_id,
            "rerun_mode": rerun_mode,
            "manifest_rows_inserted": manifest_rows_inserted,
            "registry_rows_inserted": registry_rows_inserted,
        }
    )
)
