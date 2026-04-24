# Databricks notebook source

# MAGIC %md
# MAGIC ## fixtures_discovery — JDBC SDP A1 Spike
# MAGIC
# MAGIC Select 5 tables from the `freesql_catalog.PROJECTS` Oracle schema for the
# MAGIC spike run. Each table uses column `UPDATED` as its incremental watermark.
# MAGIC
# MAGIC Persist the selection to `devtest_edp_orchestration.jdbc_spike.selected_sources`
# MAGIC for downstream tasks (`prepare_manifest`, `sdp_pipeline`).
# MAGIC
# MAGIC **Note on discovery strategy:** the foreign catalog backend is Oracle 23ai
# MAGIC and does not expose a queryable `information_schema.tables` through the
# MAGIC federation the way a UC-native catalog would. `SHOW SCHEMAS IN
# MAGIC freesql_catalog` also trips UC_RESOURCE_QUOTA_EXCEEDED because the source
# MAGIC has >50 schemas. The spike therefore pins the source schema (`PROJECTS`)
# MAGIC and watermark column (`UPDATED`) and enumerates tables via
# MAGIC `SHOW TABLES IN freesql_catalog.PROJECTS`. Operator swaps the schema
# MAGIC constant below to retarget a different source.
# MAGIC
# MAGIC Run once per spike deployment, or re-run to refresh the selection.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# Spike configuration — operator-curated, not auto-discovered.
# Schema and watermark column pinned for the federated Oracle source.
SOURCE_CATALOG = "freesql_catalog"
SOURCE_SCHEMA = "PROJECTS"
WATERMARK_COLUMN = "UPDATED"
MAX_TABLES = 5

# COMMAND ----------

# Step 1: list base tables (not views) in the pinned source schema.
all_tables_df = spark.sql(
    f"SHOW TABLES IN {SOURCE_CATALOG}.{SOURCE_SCHEMA}"
).filter(F.col("isTemporary") == False)  # noqa: E712

display(all_tables_df)

# COMMAND ----------

# Step 2: exclude views (`SHOW TABLES` returns them alongside base tables on
# foreign catalogs). Views end in `_v` by local convention; drop explicitly.
base_tables_df = all_tables_df.filter(~F.col("tableName").endswith("_v"))

display(base_tables_df)

# COMMAND ----------

# Step 3: cap at MAX_TABLES and attach the watermark column + bronze target.
# target_table format: {source_schema_lower}_{source_table_lower}, non-alphanumerics → '_'.
target_table_udf = F.udf(
    lambda schema, table: re.sub(
        r"[^a-z0-9]+", "_", f"{schema}_{table}".lower()
    ).strip("_"),
    StringType(),
)

selection_df = (
    base_tables_df.orderBy("tableName")
    .limit(MAX_TABLES)
    .withColumnRenamed("database", "source_schema")
    .withColumnRenamed("tableName", "source_table")
    .withColumn("watermark_column", F.lit(WATERMARK_COLUMN))
    .withColumn(
        "target_table",
        target_table_udf(F.col("source_schema"), F.col("source_table")),
    )
    .select("source_schema", "source_table", "watermark_column", "target_table")
)

display(selection_df)

# COMMAND ----------

# Step 4: normalize source_schema to upstream casing (Oracle catalogs often
# return lowercased schema names from SHOW TABLES; Oracle identifiers are
# case-sensitive without quotes — the source is `PROJECTS`, not `projects`).
# Upper-case here so downstream queries use the same identifier as the source.

selection_df = selection_df.withColumn(
    "source_schema", F.upper(F.col("source_schema"))
)

display(selection_df)

# COMMAND ----------

# Step 5: persist to devtest_edp_orchestration.jdbc_spike.selected_sources.
# Overwrite so re-runs are idempotent.
# Schema: source_schema STRING, source_table STRING,
#         watermark_column STRING, target_table STRING

(
    selection_df.write.mode("overwrite").saveAsTable(
        "devtest_edp_orchestration.jdbc_spike.selected_sources"
    )
)

# COMMAND ----------

# Step 6: display the persisted selection for visual verification.
final_df = spark.read.table(
    "devtest_edp_orchestration.jdbc_spike.selected_sources"
)
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
