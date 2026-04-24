# Databricks notebook source

# MAGIC %md
# MAGIC ## prepare_manifest — JDBC SDP Spike B
# MAGIC
# MAGIC Pre-run bookend task for the for_each_task approach.
# MAGIC
# MAGIC **Responsibilities:**
# MAGIC 1. Builds the 73-table AdventureWorks selection from pg_supabase.
# MAGIC 2. Inserts pending rows into the SHARED manifest table tagged
# MAGIC    `load_group='spike_b'`.
# MAGIC 3. Inserts pending rows into the SHARED watermark_registry tagged
# MAGIC    `load_group='spike_b'` (via source_system_id field).
# MAGIC 4. Emits the manifest rows as a task value `manifest_rows` (JSON array)
# MAGIC    which the for_each_task reads as its `inputs`.
# MAGIC
# MAGIC **Skip list (binary/credential tables):**
# MAGIC   - production.document    (XML/binary)
# MAGIC   - production.illustration (XML/binary)
# MAGIC   - production.productphoto (binary photo)
# MAGIC   - production.productmodel (XML)
# MAGIC   - person.password         (hashed credentials — HIPAA-skip in devtest)
# MAGIC
# MAGIC **Shared catalog note:** manifest + watermark_registry are shared with A1.
# MAGIC Rows are isolated by load_group='spike_b'. DO NOT drop or truncate these
# MAGIC tables from this notebook.
# MAGIC
# MAGIC **Contrast with Spike A1:** A1 reads from a pre-populated selected_sources
# MAGIC table (built by fixtures_discovery.py). Spike B builds the selection
# MAGIC inline from the HANDOFF spec, eliminating a separate discovery task.

# COMMAND ----------

import json
import re

# COMMAND ----------

dbutils.widgets.text("run_id", "", "Run ID (required, non-empty)")
dbutils.widgets.text("load_group", "spike_b", "Load group label")
dbutils.widgets.text("rerun_mode", "fresh", "Rerun mode: fresh | failed_only")
dbutils.widgets.text("parent_run_id", "", "Parent run ID (required when rerun_mode=failed_only)")

# COMMAND ----------

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
load_group = dbutils.widgets.get("load_group") or "spike_b"
rerun_mode = dbutils.widgets.get("rerun_mode") or "fresh"
parent_run_id_raw = dbutils.widgets.get("parent_run_id")

if rerun_mode not in ("fresh", "failed_only"):
    raise ValueError(f"rerun_mode must be 'fresh' or 'failed_only', got {rerun_mode!r}")

if rerun_mode == "failed_only":
    if not parent_run_id_raw:
        raise ValueError("parent_run_id must be set when rerun_mode='failed_only'.")
    parent_run_id = _validate_run_id(parent_run_id_raw, "parent_run_id")
else:
    parent_run_id = None

print(json.dumps({"run_id": run_id, "load_group": load_group, "rerun_mode": rerun_mode}))

# COMMAND ----------

# 73 AdventureWorks base tables from HANDOFF-PG-SOURCES.md.
# Skip list: 4 binary/XML tables + 1 password table = 5 excluded.
# All remaining tables have ModifiedDate watermark.

SOURCE_CATALOG = "pg_supabase"
WATERMARK_COLUMN = "ModifiedDate"
TARGET_CATALOG = "devtest_edp_bronze"
TARGET_SCHEMA = "jdbc_spike_b"

SKIP_TABLES = {
    ("production", "document"),
    ("production", "illustration"),
    ("production", "productphoto"),
    ("production", "productmodel"),
    ("person", "password"),
}

# Full 73-table list from HANDOFF-PG-SOURCES.md.
# Format: (source_schema, source_table)
AW_TABLES = [
    ("humanresources", "department"),
    ("humanresources", "employee"),
    ("humanresources", "employeedepartmenthistory"),
    ("humanresources", "employeepayhistory"),
    ("humanresources", "jobcandidate"),
    ("humanresources", "shift"),
    ("person", "address"),
    ("person", "addresstype"),
    ("person", "businessentity"),
    ("person", "businessentityaddress"),
    ("person", "businessentitycontact"),
    ("person", "contacttype"),
    ("person", "countryregion"),
    ("person", "emailaddress"),
    ("person", "password"),      # skip — hashed credentials
    ("person", "person"),
    ("person", "personphone"),
    ("person", "phonenumbertype"),
    ("person", "stateprovince"),
    ("production", "billofmaterials"),
    ("production", "culture"),
    ("production", "document"),  # skip — XML/binary
    ("production", "illustration"),  # skip — XML/binary
    ("production", "location"),
    ("production", "product"),
    ("production", "productcategory"),
    ("production", "productcosthistory"),
    ("production", "productdescription"),
    ("production", "productdocument"),
    ("production", "productinventory"),
    ("production", "productlistpricehistory"),
    ("production", "productmodel"),  # skip — XML
    ("production", "productmodelillustration"),
    ("production", "productmodelproductdescriptionculture"),
    ("production", "productphoto"),  # skip — binary
    ("production", "productproductphoto"),
    ("production", "productreview"),
    ("production", "productsubcategory"),
    ("production", "scrapreason"),
    ("production", "transactionhistory"),
    ("production", "transactionhistoryarchive"),
    ("production", "unitmeasure"),
    ("production", "workorder"),
    ("production", "workorderrouting"),
    ("purchasing", "productvendor"),
    ("purchasing", "purchaseorderdetail"),
    ("purchasing", "purchaseorderheader"),
    ("purchasing", "shipmethod"),
    ("purchasing", "vendor"),
    ("sales", "countryregioncurrency"),
    ("sales", "creditcard"),
    ("sales", "currency"),
    ("sales", "currencyrate"),
    ("sales", "customer"),
    ("sales", "personcreditcard"),
    ("sales", "salesorderdetail"),
    ("sales", "salesorderheader"),
    ("sales", "salesorderheadersalesreason"),
    ("sales", "salesperson"),
    ("sales", "salespersonquotahistory"),
    ("sales", "salesreason"),
    ("sales", "salestaxrate"),
    ("sales", "salesterritory"),
    ("sales", "salesterritoryhistory"),
    ("sales", "shoppingcartitem"),
    ("sales", "specialoffer"),
    ("sales", "specialofferproduct"),
    ("sales", "store"),
]

# Apply skip list — build the active selection.
selected_tables = [
    (schema, table)
    for schema, table in AW_TABLES
    if (schema, table) not in SKIP_TABLES
]

print(json.dumps({"total_aw_tables": len(AW_TABLES), "selected": len(selected_tables), "skipped": len(SKIP_TABLES)}))

# COMMAND ----------

EPOCH_ZERO = "1900-01-01T00:00:00"


def get_current_hwm(
    schema_name: str, table_name: str, source_system_id: str
) -> str:
    """Return the latest completed HWM for this (source, schema, table), or EPOCH_ZERO.

    Tier 1 HWM isolation: filter by source_system_id so HWMs written by a
    different federation or pipeline against the same (schema, table) pair
    cannot poison this lookup. See docs/planning/tier-1-hwm-fix.md.
    """
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name      = :schema_name
          AND table_name       = :table_name
          AND source_system_id = :source_system_id
          AND status           = 'completed'
        """,
        args={
            "schema_name": schema_name,
            "table_name": table_name,
            "source_system_id": source_system_id,
        },
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO


# COMMAND ----------

manifest_rows_for_foreach = []
manifest_rows_inserted = 0
registry_rows_inserted = 0

if rerun_mode == "fresh":
    for src_schema, src_table in selected_tables:
        target_table = f"{src_schema}__{src_table}"
        hwm_at_start = get_current_hwm(src_schema, src_table, SOURCE_CATALOG)
        extraction_type = "incremental" if hwm_at_start != EPOCH_ZERO else "full"
        prev_hwm = hwm_at_start if hwm_at_start != EPOCH_ZERO else None

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
               NULL, NULL, 0, NULL, NULL)
            """,
            args={
                "run_id": run_id,
                "load_group": load_group,
                "source_catalog": SOURCE_CATALOG,
                "source_schema": src_schema,
                "source_table": src_table,
                "target_table": target_table,
                "watermark_column": WATERMARK_COLUMN,
                "hwm_at_start": hwm_at_start,
            },
        )
        manifest_rows_inserted += 1

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
                "source_system_id": SOURCE_CATALOG,
                "schema_name": src_schema,
                "table_name": src_table,
                "watermark_column_name": WATERMARK_COLUMN,
                "watermark_value": hwm_at_start,
                "previous_watermark_value": prev_hwm,
                "extraction_type": extraction_type,
            },
        )
        registry_rows_inserted += 1

        manifest_rows_for_foreach.append({
            "run_id": run_id,
            "load_group": load_group,
            "source_catalog": SOURCE_CATALOG,
            "source_schema": src_schema,
            "source_table": src_table,
            "target_catalog": TARGET_CATALOG,
            "target_schema": TARGET_SCHEMA,
            "target_table": target_table,
            "watermark_column": WATERMARK_COLUMN,
            "watermark_value_at_start": hwm_at_start,
            "extraction_type": extraction_type,
        })

elif rerun_mode == "failed_only":
    failed_rows = spark.sql(
        """
        SELECT source_schema, source_table, target_table,
               watermark_column, watermark_value_at_start, retry_count
        FROM devtest_edp_orchestration.jdbc_spike.manifest
        WHERE run_id           = :parent_run_id
          AND load_group       = :load_group
          AND execution_status = 'failed'
        """,
        args={"parent_run_id": parent_run_id, "load_group": load_group},
    ).collect()

    for row in failed_rows:
        src_schema = row["source_schema"]
        src_table = row["source_table"]
        hwm_at_start = row["watermark_value_at_start"]
        new_retry_count = (row["retry_count"] or 0) + 1
        target_table = row["target_table"]

        spark.sql(
            """
            INSERT INTO devtest_edp_orchestration.jdbc_spike.manifest
              (run_id, load_group, source_catalog, source_schema, source_table,
               target_table, watermark_column, watermark_value_at_start,
               execution_status, started_at, completed_at,
               error_class, error_message, retry_count, parent_run_id, rows_written)
            VALUES
              (:run_id, :load_group, :source_catalog, :source_schema, :source_table,
               :target_table, :watermark_column, :hwm_at_start,
               'pending', NULL, NULL,
               NULL, NULL, :retry_count, :parent_run_id, NULL)
            """,
            args={
                "run_id": run_id,
                "load_group": load_group,
                "source_catalog": SOURCE_CATALOG,
                "source_schema": src_schema,
                "source_table": src_table,
                "target_table": target_table,
                "watermark_column": WATERMARK_COLUMN,
                "hwm_at_start": hwm_at_start,
                "retry_count": new_retry_count,
                "parent_run_id": parent_run_id,
            },
        )
        manifest_rows_inserted += 1

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
               'incremental', 'pending', NULL, NULL,
               current_timestamp(), current_timestamp())
            """,
            args={
                "run_id": run_id,
                "source_system_id": SOURCE_CATALOG,
                "schema_name": src_schema,
                "table_name": src_table,
                "watermark_column_name": WATERMARK_COLUMN,
                "watermark_value": hwm_at_start,
                "previous_watermark_value": hwm_at_start if hwm_at_start != EPOCH_ZERO else None,
            },
        )
        registry_rows_inserted += 1

        manifest_rows_for_foreach.append({
            "run_id": run_id,
            "load_group": load_group,
            "source_catalog": SOURCE_CATALOG,
            "source_schema": src_schema,
            "source_table": src_table,
            "target_catalog": TARGET_CATALOG,
            "target_schema": TARGET_SCHEMA,
            "target_table": target_table,
            "watermark_column": WATERMARK_COLUMN,
            "watermark_value_at_start": hwm_at_start,
            "extraction_type": "incremental",
        })

# COMMAND ----------

print(json.dumps({
    "run_id": run_id,
    "rerun_mode": rerun_mode,
    "manifest_rows_inserted": manifest_rows_inserted,
    "registry_rows_inserted": registry_rows_inserted,
    "foreach_rows_emitted": len(manifest_rows_for_foreach),
}))

# COMMAND ----------

# Emit manifest rows as a task value for the for_each_task.
# The for_each_task reads this via {{tasks.prepare_manifest.values.manifest_rows}}.
# Each element is a JSON object with all fields needed by ingest_one.py.
dbutils.jobs.taskValues.set(
    key="manifest_rows",
    value=json.dumps(manifest_rows_for_foreach),
)

print(f"Task value 'manifest_rows' set with {len(manifest_rows_for_foreach)} rows.")
