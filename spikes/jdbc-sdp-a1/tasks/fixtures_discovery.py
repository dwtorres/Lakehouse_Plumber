# Databricks notebook source

# MAGIC %md
# MAGIC ## fixtures_discovery — JDBC SDP A1 Spike
# MAGIC
# MAGIC Discover 5 PG AdventureWorks tables accessible via the `freesql` foreign
# MAGIC catalog that carry a timestamp/ModifiedDate-style watermark column.
# MAGIC
# MAGIC Persist the selection to `devtest_edp_metadata.jdbc_spike.selected_sources`
# MAGIC for downstream tasks (`prepare_manifest`, `sdp_pipeline`).
# MAGIC
# MAGIC **Strategy:** query `freesql.information_schema.columns` for columns whose
# MAGIC name matches `%modified%`, `%updated%`, or `%date%` and whose data_type is
# MAGIC timestamp-like. First match per table wins. Cap at 100 candidate tables
# MAGIC (T-tkf-04 DoS guard), then pick the first 5 with a watermark column.
# MAGIC
# MAGIC Run once per spike deployment, or re-run to refresh the selection.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

# Step 1: List all user tables in the freesql foreign catalog.
# information_schema.tables is available in Databricks Unity Catalog federation.
all_tables_df = spark.sql(
    """
    SELECT table_schema, table_name
    FROM freesql.information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER BY table_schema, table_name
    LIMIT 100
    """
)

display(all_tables_df)

# COMMAND ----------

# Step 2: Query freesql.information_schema.columns for each schema to find
# columns whose name matches timestamp-like patterns.
# We query once across all columns and filter in Spark — avoids per-table
# round-trips across the federation boundary.

all_columns_df = spark.sql(
    """
    SELECT table_schema, table_name, column_name, data_type,
           ordinal_position
    FROM freesql.information_schema.columns
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      AND (
            lower(data_type) LIKE 'timestamp%'
         OR lower(data_type) = 'date'
      )
      AND (
            lower(column_name) LIKE '%modified%'
         OR lower(column_name) LIKE '%updated%'
         OR lower(column_name) LIKE '%date%'
      )
    ORDER BY table_schema, table_name, ordinal_position
    """
)

display(all_columns_df)

# COMMAND ----------

# Step 3: For each (table_schema, table_name) keep only the first watermark
# column match (lowest ordinal_position).
first_match_df = (
    all_columns_df.withColumn(
        "rn",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("table_schema", "table_name")
            .orderBy("ordinal_position")
        ),
    )
    .filter(F.col("rn") == 1)
    .drop("rn", "ordinal_position", "data_type")
    .withColumnRenamed("table_schema", "source_schema")
    .withColumnRenamed("table_name", "source_table")
    .withColumnRenamed("column_name", "watermark_column")
)

# Cap at 5 distinct (schema, table, watermark_column) rows.
selected_5 = first_match_df.limit(5)
display(selected_5)

# COMMAND ----------

# Step 4: Derive a safe Bronze target_table name.
# Format: {source_schema}_{source_table} lowercased, non-alphanumerics → '_'.
# Example: HumanResources.Employee → humanresources_employee

target_table_udf = F.udf(
    lambda schema, table: re.sub(
        r"[^a-z0-9]+", "_", f"{schema}_{table}".lower()
    ).strip("_"),
    StringType(),
)

selection_df = selected_5.withColumn(
    "target_table", target_table_udf(F.col("source_schema"), F.col("source_table"))
)

display(selection_df)

# COMMAND ----------

# Step 5: Persist to devtest_edp_metadata.jdbc_spike.selected_sources.
# Overwrite so re-runs are idempotent.
# Schema: source_schema STRING, source_table STRING,
#         watermark_column STRING, target_table STRING

(
    selection_df.select(
        "source_schema", "source_table", "watermark_column", "target_table"
    )
    .write.mode("overwrite")
    .saveAsTable("devtest_edp_metadata.jdbc_spike.selected_sources")
)

# COMMAND ----------

# Step 6: Display the persisted selection for visual verification.
final_df = spark.read.table("devtest_edp_metadata.jdbc_spike.selected_sources")
display(final_df)

rows = final_df.collect()
print(
    f"fixtures_discovery complete: {len(rows)} tables selected for spike run."
)
for r in rows:
    print(
        f"  {r['source_schema']}.{r['source_table']}"
        f" (watermark={r['watermark_column']}"
        f" → target={r['target_table']})"
    )
