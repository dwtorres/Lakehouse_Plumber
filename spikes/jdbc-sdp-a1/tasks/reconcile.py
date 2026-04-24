# Databricks notebook source

# MAGIC %md
# MAGIC ## reconcile — JDBC SDP A1 Spike
# MAGIC
# MAGIC Post-run bookend task. Reads the SDP pipeline event log to determine
# MAGIC which flows completed or failed, then updates the spike manifest and
# MAGIC isolated watermark registry accordingly.
# MAGIC
# MAGIC **Implements the mark_landed / mark_failed side of L2 §5.3** (inline via
# MAGIC plain `spark.sql`, no `lhp_watermark` dependency).
# MAGIC
# MAGIC **Event log query:** uses `event_log(pipeline('<pipeline_id>'))` TVF with
# MAGIC `event_type='flow_progress'` and terminal status IN ('COMPLETED','FAILED').
# MAGIC Deduplicates by `flow_name`, keeping the latest event per flow.
# MAGIC
# MAGIC **T-tkf-02:** `run_id` and `pipeline_id` are bound via `spark.sql args={}`.
# MAGIC `error_message` is truncated to 4096 chars before writing.

# COMMAND ----------

import json
import re

from pyspark.sql import functions as F
from pyspark.sql import window as W

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run ID (required)")
dbutils.widgets.text("pipeline_id", "", "SDP Pipeline ID (required)")

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
pipeline_id = dbutils.widgets.get("pipeline_id")

if not run_id:
    raise ValueError("Widget 'run_id' must not be empty.")
if not pipeline_id:
    raise ValueError("Widget 'pipeline_id' must not be empty.")

print(json.dumps({"run_id": run_id, "pipeline_id": pipeline_id}))

# COMMAND ----------

# Step 1: Query the SDP pipeline event log for terminal flow events.
# event_log() is a table-valued function. On current DBR runtimes the
# pipeline identifier is passed as a string literal — there is no
# `pipeline(...)` wrapping routine. Flow status lives under
# `details.flow_progress.status`, not `details.status`.
#
# T-tkf-02: pipeline_id was validated upstream (UUID regex), so
# f-string interpolation into the TVF argument is safe here — the TVF
# does not accept parameter markers in its positional argument slot.
_PIPELINE_ID_UUID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
if not _PIPELINE_ID_UUID_PATTERN.match(pipeline_id):
    raise ValueError(
        f"pipeline_id {pipeline_id!r} is not a valid UUID; aborting reconcile"
    )

event_log_df = spark.sql(
    f"""
    SELECT
        origin.flow_name                                        AS flow_name,
        details:flow_progress:status                            AS status,
        CAST(details:flow_progress:metrics:num_output_rows AS BIGINT)
                                                                AS rows_written,
        message                                                 AS error_message,
        timestamp
    FROM event_log('{pipeline_id}')
    WHERE event_type = 'flow_progress'
      AND details:flow_progress:status IN ('COMPLETED', 'FAILED')
    """
)

# Deduplicate: keep only the latest terminal event per flow (handles in-pipeline
# retries where a flow might emit multiple FAILED events before a COMPLETED).
window_spec = W.Window.partitionBy("flow_name").orderBy(F.desc("timestamp"))
latest_events_df = (
    event_log_df.withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Truncate error_message to 4096 chars to prevent log-injection blow-up.
# T-tkf-02 mitigation — matches production template behaviour.
latest_events_df = latest_events_df.withColumn(
    "error_message", F.substring(F.col("error_message"), 1, 4096)
)

display(latest_events_df)

# COMMAND ----------

# Step 2: Load this run's manifest rows and left-join with event-log outcomes.
manifest_df = spark.sql(
    """
    SELECT target_table, watermark_column, watermark_value_at_start,
           source_schema, source_table
    FROM devtest_edp_orchestration.jdbc_spike.manifest
    WHERE run_id = :run_id
    """,
    args={"run_id": run_id},
)

joined_df = manifest_df.join(
    latest_events_df,
    on=manifest_df["target_table"] == latest_events_df["flow_name"],
    how="left",
)

# Collect for imperative UPDATE loop.
# Row counts are small (5–100 per run) so collect is safe.
joined_rows = joined_df.collect()

count_completed = 0
count_failed = 0
count_untouched = 0

# COMMAND ----------

# Step 3: Reconcile each manifest row based on event-log outcome.

for row in joined_rows:
    target_table = row["target_table"]
    flow_status = row["status"]
    rows_written = row["rows_written"]
    error_msg = row["error_message"]
    watermark_col = row["watermark_column"]
    hwm_at_start = row["watermark_value_at_start"]
    src_schema = row["source_schema"]
    src_table = row["source_table"]

    if flow_status == "COMPLETED":
        # --- mark_landed equivalent ---

        # UPDATE manifest: mark completed.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.manifest
            SET execution_status  = 'completed',
                completed_at      = current_timestamp(),
                rows_written      = :rows_written
            WHERE run_id          = :run_id
              AND target_table    = :target_table
            """,
            args={
                "run_id": run_id,
                "target_table": target_table,
                "rows_written": rows_written,
            },
        )

        # Compute the new HWM from the landed bronze table.
        # Fall back to watermark_value_at_start if the table is empty.
        try:
            hwm_result = spark.sql(
                f"SELECT MAX({watermark_col}) AS new_hwm "
                f"FROM devtest_edp_bronze.jdbc_spike.{target_table}"
            ).first()
            new_hwm = str(hwm_result["new_hwm"]) if hwm_result["new_hwm"] is not None else hwm_at_start
        except Exception:
            # Table may not exist yet if 0 rows written.
            new_hwm = hwm_at_start

        # UPDATE spike watermark_registry — mark_landed step.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.watermark_registry
            SET status          = 'landed',
                watermark_value = :new_hwm,
                row_count       = :row_count,
                updated_at      = current_timestamp()
            WHERE run_id        = :run_id
              AND schema_name   = :schema_name
              AND table_name    = :table_name
            """,
            args={
                "run_id": run_id,
                "schema_name": src_schema,
                "table_name": src_table,
                "new_hwm": new_hwm,
                "row_count": rows_written,
            },
        )

        # mark_complete step — two sequential UPDATEs mirrors production
        # mark_landed → mark_complete two-step so intent is visible.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.watermark_registry
            SET status     = 'completed',
                updated_at = current_timestamp()
            WHERE run_id   = :run_id
              AND schema_name = :schema_name
              AND table_name  = :table_name
            """,
            args={
                "run_id": run_id,
                "schema_name": src_schema,
                "table_name": src_table,
            },
        )

        count_completed += 1

    elif flow_status == "FAILED":
        # --- mark_failed equivalent ---

        err_class = "FlowFailure"

        # UPDATE manifest: mark failed.
        spark.sql(
            """
            UPDATE devtest_edp_orchestration.jdbc_spike.manifest
            SET execution_status  = 'failed',
                completed_at      = current_timestamp(),
                error_class       = :error_class,
                error_message     = :error_message
            WHERE run_id          = :run_id
              AND target_table    = :target_table
            """,
            args={
                "run_id": run_id,
                "target_table": target_table,
                "error_class": err_class,
                "error_message": (error_msg or "")[:4096],
            },
        )

        # UPDATE spike watermark_registry: mark failed.
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
                "schema_name": src_schema,
                "table_name": src_table,
                "error_class": err_class,
                "error_message": (error_msg or "")[:4096],
            },
        )

        count_failed += 1

    else:
        # Flow never emitted a terminal event (QUEUED / SKIPPED / no event).
        # Leave manifest row as 'pending' so the natural next fresh run will
        # pick it up. A failed_only rerun will NOT include it (status != failed).
        print(
            f"WARNING: flow '{target_table}' has no terminal event in the "
            f"event log for pipeline_id={pipeline_id!r}. "
            "Manifest row remains 'pending'."
        )
        count_untouched += 1

# COMMAND ----------

print(
    json.dumps(
        {
            "run_id": run_id,
            "pipeline_id": pipeline_id,
            "completed": count_completed,
            "failed": count_failed,
            "untouched": count_untouched,
        }
    )
)
