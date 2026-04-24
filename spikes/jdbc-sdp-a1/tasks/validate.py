# Databricks notebook source

# MAGIC %md
# MAGIC ## validate — JDBC SDP A1 Spike
# MAGIC
# MAGIC Acceptance gate: asserts that at least `min_completed` tables completed
# MAGIC successfully in this run. Fails the Databricks job task (via `assert`) if
# MAGIC the count is below the threshold.
# MAGIC
# MAGIC This is a placeholder — not a comprehensive data-quality suite.
# MAGIC Pass/fail is determined by manifest `execution_status = 'completed'` count.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run ID (required)")
dbutils.widgets.text("min_completed", "4", "Minimum number of completed tables")

# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
min_completed_str = dbutils.widgets.get("min_completed") or "4"

if not run_id:
    raise ValueError("Widget 'run_id' must not be empty.")

min_completed = int(min_completed_str)

# COMMAND ----------

# Query manifest for this run's completed rows.
manifest_df = spark.sql(
    """
    SELECT target_table, execution_status, rows_written, completed_at
    FROM devtest_edp_metadata.jdbc_spike.manifest
    WHERE run_id = :run_id
    ORDER BY target_table
    """,
    args={"run_id": run_id},
)

completed_df = manifest_df.filter(F.col("execution_status") == "completed")
count_completed = completed_df.count()

# COMMAND ----------

# Acceptance gate.
assert count_completed >= min_completed, (
    f"Spike validation FAILED for run_id={run_id!r}: "
    f"only {count_completed} table(s) completed, "
    f"expected >= {min_completed}."
)

print(
    f"Spike validation PASSED: {count_completed}/{min_completed} "
    f"minimum completed for run_id={run_id!r}."
)

# COMMAND ----------

# Display the full manifest snapshot for this run.
display(manifest_df)
