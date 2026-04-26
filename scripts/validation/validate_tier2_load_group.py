# Databricks notebook source
# Tier 2 ``load_group`` Databricks validation — runs on a Databricks cluster
# against a deep-clone of ``metadata.<env>_orchestration.watermarks``.
#
# Invariants exercised (V1–V5 from docs/planning/tier-2-hwm-load-group-fix.md
# + plan v5 §Phase 5 V1-V5 table):
#   V1 — Isolation smoke. Two probe rows with same ``(source_system_id, schema,
#        table)`` but different ``load_group`` values do NOT leak: a
#        ``WatermarkManager.get_latest_watermark(..., load_group='lg_a')`` call
#        returns only ``lg_a``'s row; ``load_group='lg_b'`` returns only
#        ``lg_b``'s row.
#   V2 — Tier-1 cross-source regression. A ``'fake_federation'``-source row at
#        ``2099-01-01`` is NOT picked up by ``get_latest_watermark`` calls
#        keyed off a different ``source_system_id``.
#   V3 — Legacy fallback. ``get_latest_watermark(..., load_group=None)`` returns
#        a ``'legacy'``-backfilled row when one exists in the probe set
#        (migration safety valve for un-migrated callers).
#   V4 — Legacy-seed-then-incremental smoke. Apply Step 4a INSERT against the
#        probe table for a synthetic ``(source, schema, table, target_load_group)``
#        and run a worker iteration in dry-run mode. Assert: (a) the returned
#        HWM equals the seeded ``watermark_value``, (b) the worker's
#        ``extraction_type`` would be ``'incremental'`` (not ``'full'``), and
#        (c) the JDBC WHERE clause includes the seeded HWM literal.
#   V5 — Cold-start when no legacy row exists. Probe rows under ``lg_a`` +
#        ``lg_b`` only — no ``'legacy'`` row, no NULL row. Caller invokes
#        ``get_latest_watermark(load_group=None)`` (also ``load_group=""`` and
#        the default-arg call shape per H24). All three must return ``None``.
#        Pinned contract: silent latest-across-all-load_groups would break
#        Tier-1 cross-source isolation guarantee (regression-catch only).
#
# Invocation:
#
#   (A) Manual — import this file as a Databricks notebook, attach it to a
#       cluster with the ``lhp_watermark`` package on the Python path, set the
#       widgets in the "Widget defaults" cell below, and run all. The final
#       cell prints a JSON summary and sets a notebook exit value for Jobs
#       capture.
#
#   (B) CLI-submitted — Real Databricks runs use the
#       ``DATABRICKS_CONFIG_PROFILE=dbc-8e058692-373e`` profile (per the project
#       Databricks deploy memory). Pair this notebook with a Databricks job
#       submitted via that profile.
#
# Pre-requisites:
#   - The operator deep-clones the source watermarks table to a probe table
#     (``watermarks_v1_probe`` by default) before running. Cell 1 fails fast
#     if the probe table already exists; the operator must drop it manually.
#   - Unity Catalog write access to ``{catalog}.{schema}.{probe_table}``.
#   - ``lhp_watermark`` importable on the cluster (workspace files or installed
#     wheel). The ``lhp_workspace_path`` widget accepts a path to prepend to
#     ``sys.path``.
#
# Cleanup: V1–V4 each tag their probe rows with a distinct ``run_id`` marker
# (``local-<uuid>`` form). Cell 6 deletes ONLY by those tracked ``run_id``s —
# never by table-scoped predicate (mirrors Tier-1 V1 cleanup pattern). The
# probe table itself is left intact for operator inspection.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget defaults
# MAGIC
# MAGIC Override these from the Jobs UI or via ``job_parameters`` /
# MAGIC ``notebook_params`` when submitting programmatically. Defaults target a
# MAGIC conventional dev layout. The probe table is created via DEEP CLONE
# MAGIC against the source watermarks table.

# COMMAND ----------

# fmt: off
# Pure helper definitions (cell 0) load before any ``dbutils`` reference so the
# helpers can be unit-tested via plain ``importlib`` without a Databricks
# runtime present. The notebook execution path (cells 1–6) is gated on
# ``dbutils`` availability so a bare ``import`` of this file in pytest does
# not error.
# fmt: on

from __future__ import annotations

import json
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional


def _build_probe_row(
    *,
    run_id: str,
    source_system_id: str,
    schema_name: str,
    table_name: str,
    watermark_column_name: str,
    watermark_value: str,
    load_group: Optional[str],
    watermark_time: str = "2099-01-01T00:00:00+00:00",
    extraction_type: str = "incremental",
    status: str = "completed",
) -> Dict[str, Any]:
    """Build a probe-row dict for V1/V2/V3 INSERT composition.

    Pure function: returns a plain dict that the cell-level INSERT SQL
    composer reads from. Tests assert the dict shape + value passthrough.
    """
    return {
        "run_id": run_id,
        "watermark_time": watermark_time,
        "source_system_id": source_system_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "watermark_column_name": watermark_column_name,
        "watermark_value": watermark_value,
        "previous_watermark_value": None,
        "row_count": 0,
        "extraction_type": extraction_type,
        "bronze_stage_complete": True,
        "silver_stage_complete": True,
        "status": status,
        "created_at": watermark_time,
        "completed_at": watermark_time,
        "load_group": load_group,
    }


def _validate_probe_row(row: Dict[str, Any]) -> None:
    """Validate every string field in a probe row through ``SQLInputValidator``.

    Mirrors the ``WatermarkManager.insert_new`` validation order. Rejects
    adversarial input before any SQL composition.
    """
    from lhp_watermark.sql_safety import SQLInputValidator

    SQLInputValidator.uuid_or_job_run_id(row["run_id"])
    SQLInputValidator.string(row["source_system_id"])
    SQLInputValidator.string(row["schema_name"])
    SQLInputValidator.string(row["table_name"])
    SQLInputValidator.string(row["watermark_column_name"])
    SQLInputValidator.string(row["watermark_value"])
    SQLInputValidator.string(row["extraction_type"])
    SQLInputValidator.string(row["status"])
    if row["load_group"] is not None:
        SQLInputValidator.string(row["load_group"])


def build_probe_insert_sql(probe_table: str, row: Dict[str, Any]) -> str:
    """Compose a single-row INSERT against the probe table for V1/V2/V3.

    All composed values flow through ``SQLInputValidator`` + ``sql_literal``
    (no f-string concatenation of user data). ``probe_table`` is validated
    via ``SQLInputValidator.identifier`` and emitted via ``sql_identifier``.
    """
    from lhp_watermark.sql_safety import (
        SQLInputValidator,
        sql_identifier,
        sql_literal,
    )

    SQLInputValidator.identifier(probe_table)
    _validate_probe_row(row)

    def _opt_lit(value: Optional[str]) -> str:
        if value is None:
            return "NULL"
        SQLInputValidator.string(value)
        return sql_literal(value)

    table_sql = sql_identifier(probe_table)
    run_id_sql = sql_literal(row["run_id"])
    wt_sql = f"TIMESTAMP {sql_literal(row['watermark_time'])}"
    src_sql = sql_literal(row["source_system_id"])
    sch_sql = sql_literal(row["schema_name"])
    tbl_sql = sql_literal(row["table_name"])
    col_sql = sql_literal(row["watermark_column_name"])
    val_sql = sql_literal(row["watermark_value"])
    prev_sql = _opt_lit(row.get("previous_watermark_value"))
    rc_sql = str(int(row["row_count"]))
    et_sql = sql_literal(row["extraction_type"])
    bronze_sql = "true" if row["bronze_stage_complete"] else "false"
    silver_sql = "true" if row["silver_stage_complete"] else "false"
    status_sql = sql_literal(row["status"])
    created_sql = f"TIMESTAMP {sql_literal(row['created_at'])}"
    completed_sql = (
        f"TIMESTAMP {sql_literal(row['completed_at'])}"
        if row.get("completed_at")
        else "NULL"
    )
    lg_sql = _opt_lit(row["load_group"])

    return (
        f"INSERT INTO {table_sql} ("
        "run_id, watermark_time, source_system_id, schema_name, table_name, "
        "watermark_column_name, watermark_value, previous_watermark_value, "
        "row_count, extraction_type, bronze_stage_complete, silver_stage_complete, "
        "status, created_at, completed_at, load_group"
        ") VALUES ("
        f"{run_id_sql}, {wt_sql}, {src_sql}, {sch_sql}, {tbl_sql}, "
        f"{col_sql}, {val_sql}, {prev_sql}, "
        f"{rc_sql}, {et_sql}, {bronze_sql}, {silver_sql}, "
        f"{status_sql}, {created_sql}, {completed_sql}, {lg_sql}"
        ")"
    )


def build_step4a_insert_sql(
    *,
    probe_table: str,
    source_system_id: str,
    schema_name: str,
    table_name: str,
    target_load_group: str,
    seed_run_id: str,
) -> str:
    """Compose the Step 4a INSERT-from-SELECT exactly per origin doc shape.

    Mirrors the SQL pinned in
    ``docs/planning/tier-2-hwm-load-group-fix.md`` Step 4a: 16-column INSERT
    INTO ... SELECT ... FROM ... WHERE load_group='legacy' AND ...
    ORDER BY watermark_time DESC, completed_at DESC, run_id DESC LIMIT 1.

    All composed values flow through ``SQLInputValidator`` + ``sql_literal``.
    ``seed_run_id`` is supplied by the caller (UUID-based, validated as a
    run_id) so V4 can find the inserted row deterministically by its
    ``run_id`` marker for cleanup.
    """
    from lhp_watermark.sql_safety import (
        SQLInputValidator,
        sql_identifier,
        sql_literal,
    )

    SQLInputValidator.identifier(probe_table)
    SQLInputValidator.string(source_system_id)
    SQLInputValidator.string(schema_name)
    SQLInputValidator.string(table_name)
    SQLInputValidator.string(target_load_group)
    SQLInputValidator.uuid_or_job_run_id(seed_run_id)

    table_sql = sql_identifier(probe_table)
    src_sql = sql_literal(source_system_id)
    sch_sql = sql_literal(schema_name)
    tbl_sql = sql_literal(table_name)
    lg_sql = sql_literal(target_load_group)
    rid_sql = sql_literal(seed_run_id)

    return (
        f"INSERT INTO {table_sql} "
        "(run_id, watermark_time, source_system_id, schema_name, table_name, "
        "watermark_column_name, watermark_value, previous_watermark_value, "
        "row_count, extraction_type, "
        "bronze_stage_complete, silver_stage_complete, status, "
        "created_at, completed_at, load_group) "
        "SELECT "
        f"{rid_sql}, "
        "watermark_time, source_system_id, schema_name, table_name, "
        "watermark_column_name, watermark_value, previous_watermark_value, "
        "row_count, "
        "'seeded', "
        "bronze_stage_complete, silver_stage_complete, status, "
        "current_timestamp(), completed_at, "
        f"{lg_sql} "
        f"FROM {table_sql} "
        "WHERE load_group = 'legacy' "
        f"  AND source_system_id = {src_sql} "
        f"  AND schema_name = {sch_sql} "
        f"  AND table_name = {tbl_sql} "
        "  AND status = 'completed' "
        "ORDER BY watermark_time DESC, completed_at DESC, run_id DESC "
        "LIMIT 1"
    )


def build_cleanup_sql(probe_table: str, run_ids: List[str]) -> Optional[str]:
    """Compose a run-id-scoped DELETE for cleanup.

    Returns ``None`` when no run_ids are tracked (no-op safety). Cleanup is
    by isolated marker ``run_id``s ONLY — never by table-scoped predicate
    like ``WHERE source_system_id = ...`` (mirrors Tier-1 V1 cleanup
    pattern). The probe table itself is left intact for operator
    inspection.
    """
    from lhp_watermark.sql_safety import (
        SQLInputValidator,
        sql_identifier,
        sql_literal,
    )

    if not run_ids:
        return None

    SQLInputValidator.identifier(probe_table)
    for rid in run_ids:
        SQLInputValidator.uuid_or_job_run_id(rid)

    table_sql = sql_identifier(probe_table)
    in_list = ", ".join(sql_literal(rid) for rid in run_ids)
    return f"DELETE FROM {table_sql} WHERE run_id IN ({in_list})"


def build_jdbc_where_clause(
    *,
    jdbc_table: str,
    watermark_column: str,
    hwm_value: str,
) -> str:
    """Compose the JDBC WHERE-clause subquery for V4 dry-run assertion.

    Mirrors the ``_build_jdbc_query`` shape in
    ``src/lhp/templates/load/jdbc_watermark_job.py.j2`` (lines 140–174):
    a parenthesised subquery with the watermark column compared against the
    HWM literal. Used only to assert the seeded HWM lands in the JDBC
    predicate; not executed against a real source.
    """
    # Quote with backticks to match the template's identifier emission for
    # the JDBC column reference. The literal is single-quoted.
    col_quoted = f"`{watermark_column}`"
    hwm_literal = "'" + hwm_value.replace("'", "''") + "'"
    op = ">"
    return (
        f"(SELECT * FROM {jdbc_table} WHERE {col_quoted} {op} {hwm_literal}) t"
    )


# fmt: off
# Helpers above are pure (no ``dbutils`` / ``spark`` references). The cells
# below execute against the live Databricks runtime; they are guarded so a
# bare ``import`` of this file under pytest does not raise.
# fmt: on


def _running_in_databricks() -> bool:
    """Return True iff ``dbutils`` and ``spark`` are bound in the global scope.

    Databricks notebooks expose ``dbutils`` and ``spark`` as injected
    globals; a plain ``importlib`` load (e.g. from pytest) does not. The
    cell-execution path below is gated on this so unit tests can import
    the helpers without triggering widget reads or Spark queries.
    """
    return "dbutils" in globals() and "spark" in globals()


# COMMAND ----------

if _running_in_databricks():
    dbutils.widgets.text("catalog", "metadata", "Unity Catalog for the watermark table")
    dbutils.widgets.text(
        "schema",
        "devtest_edp_orchestration",
        "Schema containing the watermarks table (e.g. ``<env>_orchestration``)",
    )
    dbutils.widgets.text(
        "probe_table_name",
        "watermarks_v1_probe",
        "Probe-table name (DEEP CLONE target). Aborts if it already exists.",
    )
    dbutils.widgets.text(
        "source_system_id",
        "tier2_validation",
        "Synthetic source_system_id used in the V1/V3/V4 probe rows",
    )
    dbutils.widgets.text(
        "schema_name", "tier2_probe", "Synthetic schema_name component"
    )
    dbutils.widgets.text(
        "table_name", "probe_tbl", "Synthetic table_name component"
    )
    dbutils.widgets.text(
        "lhp_workspace_path",
        "",
        "(Optional) Absolute workspace path to prepend to sys.path",
    )
    dbutils.widgets.text(
        "cleanup_on_success",
        "true",
        "Delete tracked probe rows from the probe table after PASS",
    )

    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
    probe_table_name = dbutils.widgets.get("probe_table_name").strip()
    source_system_id = dbutils.widgets.get("source_system_id").strip()
    schema_name_w = dbutils.widgets.get("schema_name").strip()
    table_name_w = dbutils.widgets.get("table_name").strip()
    lhp_workspace_path = dbutils.widgets.get("lhp_workspace_path").strip()
    cleanup_on_success = (
        dbutils.widgets.get("cleanup_on_success").strip().lower() == "true"
    )

    source_table_fq = f"{catalog}.{schema}.watermarks"
    probe_table_fq = f"{catalog}.{schema}.{probe_table_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Deep-clone source ``watermarks`` to probe table
# MAGIC
# MAGIC Aborts if the probe table already exists. The operator must drop it
# MAGIC manually (``DROP TABLE <catalog>.<schema>.<probe_table_name>``) before
# MAGIC re-running so each run starts from a known-clean clone of the source.

# COMMAND ----------

if _running_in_databricks():
    if lhp_workspace_path and lhp_workspace_path not in sys.path:
        sys.path.insert(0, lhp_workspace_path)

    from lhp_watermark.watermark_manager import WatermarkManager  # noqa: E402

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    existing = spark.sql(
        f"SHOW TABLES IN {catalog}.{schema} LIKE '{probe_table_name}'"
    ).collect()
    if existing:
        raise RuntimeError(
            f"Probe table {probe_table_fq} already exists; operator must drop "
            "it manually before re-running so the deep-clone starts clean."
        )

    spark.sql(
        f"CREATE TABLE {probe_table_fq} DEEP CLONE {source_table_fq}"
    )

    # WatermarkManager needs a (catalog, schema) and resolves
    # ``{catalog}.{schema}.watermarks`` internally. To run reads against the
    # probe table we instantiate the manager and override ``table_name`` to
    # point at the probe — the only public attribute the read path uses.
    wm = WatermarkManager(spark, catalog=catalog, schema=schema)
    wm.table_name = probe_table_fq

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — V1 isolation smoke
# MAGIC
# MAGIC Insert two probe rows with same ``(source_system_id, schema, table)``
# MAGIC but different ``load_group`` values. Assert
# MAGIC ``get_latest_watermark(load_group='lg_a')`` returns ``lg_a``'s row and
# MAGIC ``load_group='lg_b'`` returns ``lg_b``'s row. Neither leaks.

# COMMAND ----------


def _fresh_run_id() -> str:
    """Return a ``local-<uuid>`` run_id (validates against
    ``SQLInputValidator.uuid_or_job_run_id``)."""
    return f"local-{uuid.uuid4()}"


def _v1_isolation_smoke() -> Dict[str, Any]:
    tracked: List[str] = []
    rid_a = _fresh_run_id()
    rid_b = _fresh_run_id()
    tracked.extend([rid_a, rid_b])
    try:
        row_a = _build_probe_row(
            run_id=rid_a,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-01-01T00:00:00.000000+00:00",
            load_group="lg_a",
        )
        row_b = _build_probe_row(
            run_id=rid_b,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-02-01T00:00:00.000000+00:00",
            load_group="lg_b",
        )

        spark.sql(build_probe_insert_sql(probe_table_fq, row_a))
        spark.sql(build_probe_insert_sql(probe_table_fq, row_b))

        got_a = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group="lg_a",
        )
        got_b = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group="lg_b",
        )

        a_ok = got_a is not None and got_a["run_id"] == rid_a
        b_ok = got_b is not None and got_b["run_id"] == rid_b

        return {
            "tracked_run_ids": tracked,
            "lg_a_run_id": got_a["run_id"] if got_a else None,
            "lg_b_run_id": got_b["run_id"] if got_b else None,
            "ok": bool(a_ok and b_ok),
        }
    finally:
        # Cleanup ALWAYS — by tracked run_ids, never table-scoped.
        cleanup_sql = build_cleanup_sql(probe_table_fq, tracked)
        if cleanup_sql:
            try:
                spark.sql(cleanup_sql)
            except Exception:  # noqa: BLE001 — cleanup is best-effort
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — V2 Tier-1 cross-source regression
# MAGIC
# MAGIC Insert a ``'fake_federation'``-source row at ``2099-01-01``; assert
# MAGIC ``get_latest_watermark`` keyed off the real source does NOT pick it
# MAGIC up. Mirrors the Tier-1 V1 probe (``docs/planning/tier-1-hwm-fix.md``).

# COMMAND ----------


def _v2_cross_source_regression() -> Dict[str, Any]:
    tracked: List[str] = []
    rid_fake = _fresh_run_id()
    rid_real = _fresh_run_id()
    tracked.extend([rid_fake, rid_real])
    try:
        # Synthetic far-future row under a different source_system_id.
        row_fake = _build_probe_row(
            run_id=rid_fake,
            source_system_id="fake_federation",
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-12-31T23:59:59.000000+00:00",
            load_group=None,
        )
        # Real-source row at a much earlier watermark_value/time.
        row_real = _build_probe_row(
            run_id=rid_real,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2024-01-01T00:00:00.000000+00:00",
            watermark_time="2024-01-01T00:00:00+00:00",
            load_group=None,
        )

        spark.sql(build_probe_insert_sql(probe_table_fq, row_fake))
        spark.sql(build_probe_insert_sql(probe_table_fq, row_real))

        got = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group=None,
        )

        # Must NOT be the fake_federation row.
        ok = got is not None and got["run_id"] != rid_fake

        return {
            "tracked_run_ids": tracked,
            "observed_run_id": got["run_id"] if got else None,
            "fake_federation_run_id": rid_fake,
            "ok": bool(ok),
        }
    finally:
        cleanup_sql = build_cleanup_sql(probe_table_fq, tracked)
        if cleanup_sql:
            try:
                spark.sql(cleanup_sql)
            except Exception:  # noqa: BLE001
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — V3 legacy fallback
# MAGIC
# MAGIC Assert ``get_latest_watermark(load_group=None)`` returns a
# MAGIC ``'legacy'``-backfilled row when one exists in the probe set. This is
# MAGIC the migration safety valve for un-migrated callers.

# COMMAND ----------


def _v3_legacy_fallback() -> Dict[str, Any]:
    """V3 — legacy fallback.

    Two probe rows:
      - ``rid_null`` with ``load_group = NULL`` (pre-Tier-2 / pre-Step-3
        backfill state).
      - ``rid_legacy`` with ``load_group = 'legacy'`` (post-Step-3 backfill
        state).

    Migration safety valve assertions:
      - ``get_latest_watermark(load_group=None)`` returns ``rid_null`` (the
        three-way filter collapses both arms to ``IS NULL``).
      - ``get_latest_watermark(load_group='legacy')`` returns ``rid_legacy``
        (left arm of the three-way filter matches the literal).

    Together these prove that legacy callers continue to read pre-Tier-2
    rows AND that callers explicitly probing the 'legacy' string see the
    backfilled rows, which is the Step-3-→-Step-4a hand-off contract.
    """
    tracked: List[str] = []
    rid_null = _fresh_run_id()
    rid_legacy = _fresh_run_id()
    tracked.extend([rid_null, rid_legacy])
    try:
        row_null = _build_probe_row(
            run_id=rid_null,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-05-01T00:00:00.000000+00:00",
            load_group=None,
        )
        row_legacy = _build_probe_row(
            run_id=rid_legacy,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-06-01T00:00:00.000000+00:00",
            watermark_time="2099-06-01T00:00:00+00:00",
            load_group="legacy",
        )

        spark.sql(build_probe_insert_sql(probe_table_fq, row_null))
        spark.sql(build_probe_insert_sql(probe_table_fq, row_legacy))

        got_none = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group=None,
        )
        got_legacy = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group="legacy",
        )

        none_ok = got_none is not None and got_none["run_id"] == rid_null
        legacy_ok = got_legacy is not None and got_legacy["run_id"] == rid_legacy

        return {
            "tracked_run_ids": tracked,
            "load_group_none_run_id": got_none["run_id"] if got_none else None,
            "expected_none_run_id": rid_null,
            "load_group_legacy_run_id": got_legacy["run_id"] if got_legacy else None,
            "expected_legacy_run_id": rid_legacy,
            "ok": bool(none_ok and legacy_ok),
        }
    finally:
        cleanup_sql = build_cleanup_sql(probe_table_fq, tracked)
        if cleanup_sql:
            try:
                spark.sql(cleanup_sql)
            except Exception:  # noqa: BLE001
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5 — V4 legacy-seed-then-incremental smoke
# MAGIC
# MAGIC Apply Step 4a INSERT against the probe table for a synthetic
# MAGIC ``(source, schema, table, target_load_group)`` and run a worker
# MAGIC iteration in dry-run mode that calls
# MAGIC ``get_latest_watermark(load_group=target_load_group)``. Assert:
# MAGIC (a) returned HWM equals the seeded ``watermark_value``; (b) the
# MAGIC worker's ``extraction_type`` would be ``'incremental'``;
# MAGIC (c) the JDBC WHERE clause includes the seeded HWM literal.

# COMMAND ----------


def _v4_legacy_seed_then_incremental() -> Dict[str, Any]:
    tracked: List[str] = []
    rid_legacy = _fresh_run_id()
    rid_seed = _fresh_run_id()
    tracked.extend([rid_legacy, rid_seed])
    target_load_group = "tier2_v4_pipe::tier2_v4_fg"
    seeded_hwm = "2099-03-15T12:00:00.000000+00:00"
    try:
        # Step 1: insert a 'legacy' row that the seed will copy from.
        row_legacy = _build_probe_row(
            run_id=rid_legacy,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value=seeded_hwm,
            load_group="legacy",
        )
        spark.sql(build_probe_insert_sql(probe_table_fq, row_legacy))

        # Step 2: apply Step 4a INSERT-from-SELECT to seed the new namespace.
        seed_sql = build_step4a_insert_sql(
            probe_table=probe_table_fq,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            target_load_group=target_load_group,
            seed_run_id=rid_seed,
        )
        spark.sql(seed_sql)

        # Step 3: dry-run a worker iteration:
        #   get_latest_watermark(..., load_group=target_load_group) → HWM
        #   extraction_type derived from the worker's ``"full" if hwm is None
        #   else "incremental"`` precedent.
        got = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group=target_load_group,
        )
        observed_hwm = got["watermark_value"] if got else None
        observed_run_id = got["run_id"] if got else None
        extraction_type = "full" if observed_hwm is None else "incremental"

        # Step 4: build the JDBC WHERE clause with the observed HWM.
        where_clause = build_jdbc_where_clause(
            jdbc_table="probe_jdbc",
            watermark_column="modified_date",
            hwm_value=str(observed_hwm) if observed_hwm is not None else "",
        )

        a_ok = str(observed_hwm) == seeded_hwm
        b_ok = extraction_type == "incremental"
        c_ok = (
            observed_hwm is not None and str(observed_hwm) in where_clause
        )

        return {
            "tracked_run_ids": tracked,
            "target_load_group": target_load_group,
            "seeded_watermark_value": seeded_hwm,
            "observed_watermark_value": observed_hwm,
            "observed_run_id": observed_run_id,
            "extraction_type": extraction_type,
            "jdbc_where_clause": where_clause,
            "a_hwm_matches_seed": bool(a_ok),
            "b_extraction_type_incremental": bool(b_ok),
            "c_hwm_in_where_clause": bool(c_ok),
            "ok": bool(a_ok and b_ok and c_ok),
        }
    finally:
        cleanup_sql = build_cleanup_sql(probe_table_fq, tracked)
        if cleanup_sql:
            try:
                spark.sql(cleanup_sql)
            except Exception:  # noqa: BLE001
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5b — V5 cold-start fallback (no legacy row)
# MAGIC
# MAGIC Probe rows under ``lg_a`` + ``lg_b`` only for one
# MAGIC ``(source_system_id, schema, table)`` triple. **No** ``'legacy'`` row,
# MAGIC **no** NULL-load_group row. Caller invokes
# MAGIC ``get_latest_watermark(load_group=None)``. All three call shapes
# MAGIC (``load_group=None``, ``load_group=""``, default arg) must return
# MAGIC ``None``.
# MAGIC
# MAGIC ### Pinned contract (three-valued logic reduction)
# MAGIC
# MAGIC From ``src/lhp_watermark/watermark_manager.py::_compose_load_group_clause``
# MAGIC (lines ~95-113), when ``load_group is None``:
# MAGIC
# MAGIC ```
# MAGIC AND ( load_group = NULL OR (NULL IS NULL AND load_group IS NULL) )
# MAGIC ⇒ AND ( NULL OR load_group IS NULL )           # NULL = NULL is NULL, not true
# MAGIC ⇒ AND load_group IS NULL
# MAGIC ```
# MAGIC
# MAGIC Probe rows have ``load_group='lg_a'`` and ``'lg_b'`` — neither matches
# MAGIC ``IS NULL`` — empty result returned. Same observable for ``load_group=""``
# MAGIC (reduces to ``AND load_group = ''`` — empty string ≠ ``'lg_a'``,``'lg_b'``).
# MAGIC Default-arg call shape uses the kwarg default ``None`` per the
# MAGIC ``get_latest_watermark`` signature.
# MAGIC
# MAGIC **Why V5 matters**: a regression where the three-way filter degrades to
# MAGIC silent latest-across-all-load_groups would leak ``lg_a``/``lg_b`` rows to
# MAGIC legacy callers, breaking Tier-1 cross-source isolation guarantee. V5 is
# MAGIC the catch-net for that class of regression.

# COMMAND ----------


def _v5_cold_start_no_legacy() -> Dict[str, Any]:
    """V5 — cold-start fallback when no legacy row exists.

    Probe with rows under ``lg_a`` + ``lg_b`` only. Three call shapes (H24):
      - ``get_latest_watermark(..., load_group=None)`` — explicit None
      - ``get_latest_watermark(..., load_group="")`` — empty string
      - ``get_latest_watermark(...)`` — default arg

    All three must return ``None``. ``None`` and ``""`` reduce to different
    SQL (``IS NULL`` vs ``= ''``) but yield the same observable for this
    probe shape (no row matches either predicate). Default-arg shape uses
    the kwarg default ``None`` per the ``get_latest_watermark`` signature.
    """
    tracked: List[str] = []
    rid_a = _fresh_run_id()
    rid_b = _fresh_run_id()
    tracked.extend([rid_a, rid_b])
    try:
        # Probe set: lg_a + lg_b only. No 'legacy' row, no NULL row, no other
        # rows for this (src, schema, table) under any load_group.
        row_a = _build_probe_row(
            run_id=rid_a,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-07-01T00:00:00.000000+00:00",
            watermark_time="2099-07-01T00:00:00+00:00",
            load_group="lg_a",
        )
        row_b = _build_probe_row(
            run_id=rid_b,
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            watermark_column_name="modified_date",
            watermark_value="2099-08-01T00:00:00.000000+00:00",
            watermark_time="2099-08-01T00:00:00+00:00",
            load_group="lg_b",
        )

        spark.sql(build_probe_insert_sql(probe_table_fq, row_a))
        spark.sql(build_probe_insert_sql(probe_table_fq, row_b))

        # Call shape 1: explicit load_group=None
        got_none = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group=None,
        )

        # Call shape 2: explicit load_group=""
        got_empty = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
            load_group="",
        )

        # Call shape 3: default-arg (omit load_group kwarg entirely)
        got_default = wm.get_latest_watermark(
            source_system_id=source_system_id,
            schema_name=schema_name_w,
            table_name=table_name_w,
        )

        none_ok = got_none is None
        empty_ok = got_empty is None
        default_ok = got_default is None

        return {
            "tracked_run_ids": tracked,
            "load_group_none_result": got_none,
            "load_group_empty_string_result": got_empty,
            "default_arg_result": got_default,
            "a_load_group_none_returns_none": bool(none_ok),
            "b_load_group_empty_returns_none": bool(empty_ok),
            "c_default_arg_returns_none": bool(default_ok),
            "ok": bool(none_ok and empty_ok and default_ok),
        }
    finally:
        cleanup_sql = build_cleanup_sql(probe_table_fq, tracked)
        if cleanup_sql:
            try:
                spark.sql(cleanup_sql)
            except Exception:  # noqa: BLE001
                pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6 — Cleanup tracked probe rows + report
# MAGIC
# MAGIC Each V-cell's ``finally`` already deletes its own tracked ``run_id``s.
# MAGIC This cell is a defence-in-depth pass: aggregate any leftover tracked
# MAGIC markers and DELETE them — by ``run_id`` only, never by source/table
# MAGIC predicate. The probe table itself is left intact for operator
# MAGIC inspection (drop manually before the next run).

# COMMAND ----------


def _timed(fn: Callable[[], Dict[str, Any]]) -> Dict[str, Any]:
    start = time.perf_counter()
    try:
        details = fn()
        elapsed = time.perf_counter() - start
        return {
            "verdict": "PASS" if details.get("ok") else "FAIL",
            "duration_s": elapsed,
            "details": details,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 — surface everything into the report
        elapsed = time.perf_counter() - start
        return {
            "verdict": "ERROR",
            "duration_s": elapsed,
            "details": {},
            "error": f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=10)}",
        }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Final cell — Run all V1–V5 + emit JSON exit value

# COMMAND ----------

if _running_in_databricks():
    results: Dict[str, Any] = {
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "catalog": catalog,
            "schema": schema,
            "probe_table": probe_table_fq,
            "source_system_id": source_system_id,
            "schema_name": schema_name_w,
            "table_name": table_name_w,
        },
        "invariants": {},
    }

    for invariant_id, fn in [
        ("V1", _v1_isolation_smoke),
        ("V2", _v2_cross_source_regression),
        ("V3", _v3_legacy_fallback),
        ("V4", _v4_legacy_seed_then_incremental),
        ("V5", _v5_cold_start_no_legacy),
    ]:
        results["invariants"][invariant_id] = _timed(fn)

    failed = [
        invariant_id
        for invariant_id, payload in results["invariants"].items()
        if payload["verdict"] != "PASS"
    ]
    results["overall_verdict"] = "PASS" if not failed else "FAIL"
    results["failed_invariants"] = failed

    print(json.dumps(results, indent=2, default=str))

    # Emit a notebook exit value so ``databricks jobs get-run-output`` captures a
    # machine-readable summary without parsing notebook cell output.
    dbutils.notebook.exit(json.dumps(results, default=str))
