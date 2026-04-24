# Databricks notebook source

# MAGIC %md
# MAGIC ## fixtures_discovery — JDBC SDP A1 Spike (PG AW Scale N=68)
# MAGIC
# MAGIC Enumerate tables across all 5 AdventureWorks schemas in the `pg_supabase`
# MAGIC foreign catalog. Apply the skip list (4 binary/XML tables + `person.password`
# MAGIC for HIPAA). Working set: 68 tables. All carry `ModifiedDate` as the
# MAGIC incremental watermark column.
# MAGIC
# MAGIC Persist the selection to `devtest_edp_orchestration.jdbc_spike.selected_sources`
# MAGIC for downstream tasks (`prepare_manifest`, `sdp_pipeline`).
# MAGIC
# MAGIC **Discovery strategy:** `pg_supabase` is a Postgres foreign catalog. Postgres
# MAGIC table names are lowercase by default. `SHOW TABLES IN pg_supabase.<schema>`
# MAGIC returns all objects including views. Views are identified by matching the
# MAGIC known AW view-name suffix patterns; any remaining rows are treated as base
# MAGIC tables. The SKIP_TABLES list below applies before persisting.
# MAGIC
# MAGIC Run once per spike deployment, or re-run to refresh the selection.

# COMMAND ----------

import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# Spike configuration for PG AdventureWorks scale test.
SOURCE_CATALOG = "pg_supabase"
SOURCE_SCHEMAS = ["humanresources", "person", "production", "purchasing", "sales"]
WATERMARK_COLUMN = "ModifiedDate"
MAX_TABLES = 73  # expected ceiling; actual working set is 68 after skips

# Tables to exclude:
#   - binary/XML heavy: document, illustration, productmodel, productphoto
#   - HIPAA credential table: person.password
# Keys are "<schema>.<table>" (both lowercase, matching PG output).
SKIP_TABLES = {
    "production.document",
    "production.illustration",
    "production.productmodel",
    "production.productphoto",
    "person.password",
}

# COMMAND ----------

# Step 1: enumerate base tables across all 5 AW schemas.
# SHOW TABLES IN <catalog>.<schema> returns: namespace, tableName, isTemporary.
# We union across schemas and tag each row with its schema name.

schema_dfs = []
for schema in SOURCE_SCHEMAS:
    df = (
        spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{schema}")
        .filter(F.col("isTemporary") == False)  # noqa: E712
        # Drop the `namespace` column (contains catalog.schema); replace with
        # a clean `source_schema` string literal so all schemas align.
        .withColumn("source_schema", F.lit(schema))
        .select("source_schema", "tableName")
    )
    schema_dfs.append(df)

all_tables_df = schema_dfs[0]
for df in schema_dfs[1:]:
    all_tables_df = all_tables_df.union(df)

display(all_tables_df)

# COMMAND ----------

# Step 2: filter out views.
# pg_supabase AW views follow the naming pattern `v<TableName>` (e.g.
# `vemployee`, `vperson`). Some also have a `_v` suffix convention carried
# from the Oracle spike. Drop any tableName that starts with "v" followed by
# a letter (matches AW view convention) or ends with "_v".
# Also drop any tableName that contains "view" (belt-and-suspenders).
base_tables_df = all_tables_df.filter(
    ~(
        F.col("tableName").rlike(r"^v[a-z]")
        | F.col("tableName").endswith("_v")
        | F.lower(F.col("tableName")).contains("view")
    )
)

display(base_tables_df)

# COMMAND ----------

# Step 3: apply SKIP_TABLES list.
# Build a combined skip key "<schema>.<table>" for set membership check.
skip_set = SKIP_TABLES  # already lowercase


def is_skipped(schema: str, table: str) -> bool:
    return f"{schema}.{table}".lower() in skip_set


is_skipped_udf = F.udf(
    lambda schema, table: is_skipped(schema, table),
    StringType(),  # returns string; we filter on != "true"
)

# Use a literal set comparison via Spark filter (UDF returns python bool;
# wrap in a string udf to keep Spark happy).
from pyspark.sql.types import BooleanType  # noqa: E402

is_skipped_bool_udf = F.udf(
    lambda schema, table: f"{schema}.{table}".lower() in skip_set,
    BooleanType(),
)

base_tables_df = base_tables_df.filter(
    ~is_skipped_bool_udf(F.col("source_schema"), F.col("tableName"))
)

display(base_tables_df)

# COMMAND ----------

# Step 4: attach watermark column + bronze target name.
# target_table format: {source_schema}_{source_table}, non-alphanumerics → '_'.
target_table_udf = F.udf(
    lambda schema, table: re.sub(
        r"[^a-z0-9]+", "_", f"{schema}_{table}".lower()
    ).strip("_"),
    StringType(),
)

selection_df = (
    base_tables_df.orderBy("source_schema", "tableName")
    .limit(MAX_TABLES)
    .withColumnRenamed("tableName", "source_table")
    .withColumn("source_catalog", F.lit(SOURCE_CATALOG))
    .withColumn("watermark_column", F.lit(WATERMARK_COLUMN))
    .withColumn(
        "target_table",
        target_table_udf(F.col("source_schema"), F.col("source_table")),
    )
    .select(
        "source_catalog",
        "source_schema",
        "source_table",
        "watermark_column",
        "target_table",
    )
)

display(selection_df)

# COMMAND ----------

# Step 5: Postgres returns lowercase identifiers. No case-normalization needed
# (unlike Oracle where PROJECTS schema was uppercase). source_schema stays as-is.

# Sanity-print the working set size for operator review.
working_set_count = selection_df.count()
print(f"Working set: {working_set_count} tables (expected 68, max cap {MAX_TABLES})")

# COMMAND ----------

# Step 6: persist to devtest_edp_orchestration.jdbc_spike.selected_sources.
# Overwrite so re-runs are idempotent. overwriteSchema=true handles the case
# where the existing table has the old 4-column schema (no source_catalog)
# from a prior Oracle run — without it, Delta raises DELTA_METADATA_MISMATCH.
# Schema: source_catalog STRING, source_schema STRING, source_table STRING,
#         watermark_column STRING, target_table STRING

(
    selection_df.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "devtest_edp_orchestration.jdbc_spike.selected_sources"
    )
)

# COMMAND ----------

# Step 7: display the persisted selection for visual verification.
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
