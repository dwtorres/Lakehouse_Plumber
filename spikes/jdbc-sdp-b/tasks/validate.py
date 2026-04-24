# Databricks notebook source

# MAGIC %md
# MAGIC ## validate — JDBC SDP Spike B
# MAGIC
# MAGIC Acceptance gate for the for_each_task approach.
# MAGIC
# MAGIC Asserts that `completed_count >= min_completed` for this run_id and
# MAGIC load_group='spike_b'. Fails the Databricks job task if the threshold is not met.
# MAGIC
# MAGIC Runs with `run_if: ALL_DONE` so it executes even when some for_each
# MAGIC iterations fail. This mirrors Spike A1's validate behavior.
# MAGIC
# MAGIC **Contrast with Spike A1:** A1 validate runs after reconcile, which
# MAGIC translates SDP event_log() outcomes into manifest rows. Spike B validate
# MAGIC runs directly after for_each_ingest because ingest_one.py writes manifest
# MAGIC state inline — no reconcile step needed.

# COMMAND ----------

import json

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run ID (required)")
dbutils.widgets.text("load_group", "spike_b", "Load group (default spike_b)")
dbutils.widgets.text("min_completed", "60", "Minimum completed table count")

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
load_group = dbutils.widgets.get("load_group") or "spike_b"
min_completed_str = dbutils.widgets.get("min_completed") or "60"

if not run_id:
    raise ValueError("Widget 'run_id' must not be empty.")

min_completed = int(min_completed_str)

# COMMAND ----------

manifest_df = spark.sql(
    """
    SELECT source_schema, source_table, target_table,
           execution_status, rows_written, completed_at,
           error_class, error_message
    FROM devtest_edp_orchestration.jdbc_spike.manifest
    WHERE run_id     = :run_id
      AND load_group = :load_group
    ORDER BY source_schema, source_table
    """,
    args={"run_id": run_id, "load_group": load_group},
)

completed_df = manifest_df.filter(F.col("execution_status") == "completed")
failed_df = manifest_df.filter(F.col("execution_status") == "failed")
pending_df = manifest_df.filter(F.col("execution_status") == "pending")

count_total = manifest_df.count()
count_completed = completed_df.count()
count_failed = failed_df.count()
count_pending = pending_df.count()

total_rows_written = (
    completed_df.agg(F.sum("rows_written").alias("total")).first()["total"] or 0
)

print(json.dumps({
    "run_id": run_id,
    "load_group": load_group,
    "total": count_total,
    "completed": count_completed,
    "failed": count_failed,
    "pending": count_pending,
    "total_rows_written": total_rows_written,
    "min_completed": min_completed,
}))

# COMMAND ----------

# Acceptance gate.
assert count_completed >= min_completed, (
    f"Spike B validation FAILED for run_id={run_id!r}: "
    f"only {count_completed} table(s) completed (load_group={load_group!r}), "
    f"expected >= {min_completed}. "
    f"Failed: {count_failed}, Pending: {count_pending}."
)

print(
    f"Spike B validation PASSED: {count_completed}/{count_total} completed "
    f"({count_failed} failed, {count_pending} pending) for run_id={run_id!r}. "
    f"Total rows written: {total_rows_written:,}."
)

# COMMAND ----------

# Full manifest snapshot for this run.
display(manifest_df)
