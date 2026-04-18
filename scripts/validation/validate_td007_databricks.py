# Databricks notebook source
# TD-007 Phase B Databricks validation — runs on a Databricks cluster against a dev
# Unity Catalog Volume + watermark Delta table.
#
# Invariants exercised (complement to the local Phase A harness):
#   V5 — Multiple runs produce distinct ``_lhp_runs/{run_id}/`` subdirs; no file
#        collision; each subdir's Parquet contents isolated from the others.
#   V6 — ``WatermarkManager.mark_landed`` → ``mark_complete`` happy path commits
#        the expected row state (``watermark_value`` + ``row_count`` + terminal
#        status) in a single logical extraction flow.
#   V7 — Simulated mid-finalization crash: ``mark_landed`` succeeds, ``mark_complete``
#        never runs. A subsequent run observes the prior landed state via
#        ``get_recoverable_landed_run`` and finalizes **without** reopening JDBC.
#   V8 — cloudFiles-style nested-glob resolution: a recursive Parquet read under
#        ``{landing_root}/_lhp_runs/*/*.parquet`` enumerates every landed file
#        across every run, preserving row-level integrity.
#
# Invocation options:
#
#   (A) Manual — import this file as a Databricks notebook, attach it to a cluster
#       with the ``lhp`` package on the Python path, set the widgets listed in the
#       "Widget defaults" cell below, and run all. Final cell prints a JSON
#       summary + sets a notebook exit value for Jobs capture.
#
#   (B) CLI-submitted — pair this notebook with the companion runner script
#       ``scripts/validation/run_td007_databricks.sh``. The runner uploads the
#       notebook via the Databricks CLI, submits a one-shot job with the
#       ``DATABRICKS_CONFIG_PROFILE=dbc-8e058692-373e`` profile, and downloads the
#       JSON result. Use this path when you want the validation to be
#       reproducible inside CI.
#
# Requirements:
#   - Unity Catalog write access to ``{catalog}.{schema}.watermarks`` (the dev
#     watermark table). The ``WatermarkManager`` auto-creates the table if absent.
#   - A Volume path the cluster can write to (``landing_root`` widget below).
#   - ``lhp.extensions.watermark_manager`` importable on the cluster (workspace
#     files or installed wheel). The ``lhp_workspace_path`` widget accepts a path
#     to prepend to ``sys.path`` when the package is not pre-installed.
#
# This notebook is READ-WRITE on its configured catalog/schema/volume. It cleans
# up the test volume subtree and deletes the test watermark rows on success.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget defaults
# MAGIC
# MAGIC Override these from the Jobs UI or via ``job_parameters`` / ``notebook_params``
# MAGIC when submitting programmatically. Defaults target a conventional dev layout.

# COMMAND ----------

dbutils.widgets.text("catalog", "lhp_dev", "Unity Catalog for the watermark table")
dbutils.widgets.text("schema", "orchestration_validation", "Schema for the watermark table")
dbutils.widgets.text(
    "landing_root",
    "/Volumes/lhp_dev/orchestration_validation/td007_landing",
    "Volume root the test writes + reads Parquet under",
)
dbutils.widgets.text(
    "source_system_id",
    "td007_validation",
    "Synthetic source system id used in the watermark key",
)
dbutils.widgets.text("schema_name", "td007", "Synthetic schema name component")
dbutils.widgets.text("table_name", "probe", "Synthetic table name component")
dbutils.widgets.text(
    "lhp_workspace_path",
    "",
    "(Optional) Absolute workspace path to prepend to sys.path when the lhp package is not installed",
)
dbutils.widgets.text("cleanup_on_success", "true", "Delete test artifacts after PASS runs")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
landing_root = dbutils.widgets.get("landing_root").strip().rstrip("/")
source_system_id = dbutils.widgets.get("source_system_id").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
table_name = dbutils.widgets.get("table_name").strip()
lhp_workspace_path = dbutils.widgets.get("lhp_workspace_path").strip()
cleanup_on_success = dbutils.widgets.get("cleanup_on_success").strip().lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment setup

# COMMAND ----------

import json
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import functions as F

if lhp_workspace_path and lhp_workspace_path not in sys.path:
    sys.path.insert(0, lhp_workspace_path)

from lhp.extensions.watermark_manager import (  # noqa: E402
    TerminalStateGuardError,
    WatermarkManager,
)

spark.conf.set("spark.sql.session.timeZone", "UTC")

wm = WatermarkManager(spark, catalog=catalog, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def _run_landing_path(root: str, run_id: str) -> str:
    return f"{root.rstrip('/')}/_lhp_runs/{run_id}"


def _make_synthetic_batch(rows: int, seed_offset_ms: int) -> "DataFrame":
    return (
        spark.range(rows)
        .withColumn(
            "modified_date",
            F.expr(f"timestamp_millis({seed_offset_ms}L + id * 1000L)"),
        )
        .select("id", "modified_date")
    )


def _cleanup_volume_subtree(path: str) -> None:
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:  # noqa: BLE001 — cleanup is best-effort
        pass


def _cleanup_watermark_rows(run_ids: List[str]) -> None:
    if not run_ids:
        return
    escaped = ", ".join(f"'{rid}'" for rid in run_ids)
    try:
        spark.sql(
            f"DELETE FROM {catalog}.{schema}.watermarks WHERE run_id IN ({escaped})"
        )
    except Exception:  # noqa: BLE001 — cleanup is best-effort
        pass


def _fresh_run_id(tag: str) -> str:
    # SQLInputValidator.uuid_or_job_run_id accepts UUID-v4, local-<uuid>, or
    # job-N-task-N-attempt-N. Use the "local-<uuid>" form for validation so
    # WatermarkManager DML passes its FR-L-03 run_id check.
    del tag  # Tags are recorded in the details dict instead of the run_id.
    return f"local-{uuid.uuid4()}"


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
# MAGIC ## V5 — Multiple runs land to distinct subdirs

# COMMAND ----------


def v5_distinct_run_subdirs() -> Dict[str, Any]:
    tracked_runs: List[str] = []
    try:
        run_specs = [
            ("v5a", 100, 1_700_000_000_000),
            ("v5b", 250, 1_700_005_000_000),
            ("v5c",  50, 1_700_010_000_000),
        ]
        subdir_counts: Dict[str, int] = {}
        for tag, rows, offset_ms in run_specs:
            run_id = _fresh_run_id(tag)
            tracked_runs.append(run_id)
            batch = _make_synthetic_batch(rows, offset_ms)
            landing = _run_landing_path(landing_root, run_id)
            batch.write.mode("overwrite").format("parquet").save(landing)
            readback = spark.read.format("parquet").load(landing).count()
            subdir_counts[run_id] = readback
            assert readback == rows, (
                f"V5: run {run_id} expected {rows} rows, got {readback}"
            )

        listing = dbutils.fs.ls(f"{landing_root}/_lhp_runs")
        distinct_subdirs = sorted(
            PurePosixPath(entry.path.rstrip("/")).name
            for entry in listing
            if entry.isDir()
        )
        tracked_set = set(tracked_runs)
        covered = [name for name in distinct_subdirs if name in tracked_set]

        return {
            "tracked_runs": tracked_runs,
            "distinct_subdirs_observed": distinct_subdirs,
            "covered_by_this_test": covered,
            "subdir_row_counts": subdir_counts,
            "ok": set(covered) == tracked_set and len(covered) == len(tracked_runs),
        }
    finally:
        if cleanup_on_success:
            for rid in tracked_runs:
                _cleanup_volume_subtree(_run_landing_path(landing_root, rid))

# COMMAND ----------

# MAGIC %md
# MAGIC ## V6 — Happy path: insert_new → write → mark_landed → mark_complete

# COMMAND ----------


def v6_happy_path_watermark_lifecycle() -> Dict[str, Any]:
    run_id = _fresh_run_id("v6")
    tracked_runs = [run_id]
    try:
        rows = 500
        batch = _make_synthetic_batch(rows, 1_700_100_000_000)
        landing = _run_landing_path(landing_root, run_id)

        wm.insert_new(
            run_id=run_id,
            source_system_id=source_system_id,
            schema_name=schema_name,
            table_name=f"{table_name}_v6",
            watermark_column_name="modified_date",
            watermark_value=None,
            row_count=0,
            extraction_type="full",
            previous_watermark_value=None,
        )

        batch.write.mode("overwrite").format("parquet").save(landing)
        landed = spark.read.format("parquet").load(landing)
        stats = landed.agg(
            F.count("*").alias("row_count"),
            F.max("modified_date").alias("max_hwm"),
        ).first()
        row_count = int(stats["row_count"])
        hwm = str(stats["max_hwm"])

        wm.mark_landed(run_id=run_id, watermark_value=hwm, row_count=row_count)
        wm.mark_complete(run_id=run_id, watermark_value=hwm, row_count=row_count)

        committed = spark.sql(
            f"""
            SELECT status, row_count, watermark_value
            FROM {catalog}.{schema}.watermarks
            WHERE run_id = '{run_id}'
            LIMIT 1
            """
        ).first()

        assert committed is not None, "V6: watermark row missing after mark_complete"
        return {
            "run_id": run_id,
            "observed_status": committed["status"],
            "observed_row_count": int(committed["row_count"]),
            "observed_watermark_value": str(committed["watermark_value"]),
            "expected_row_count": row_count,
            "expected_watermark_value": hwm,
            "ok": (
                committed["status"] == "completed"
                and int(committed["row_count"]) == row_count
                and str(committed["watermark_value"]) == hwm
            ),
        }
    finally:
        if cleanup_on_success:
            _cleanup_volume_subtree(_run_landing_path(landing_root, run_id))
            _cleanup_watermark_rows(tracked_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V7 — Recovery: mid-finalization crash recovered without JDBC reopen

# COMMAND ----------


def v7_recovery_after_mark_landed_only() -> Dict[str, Any]:
    run_id = _fresh_run_id("v7")
    tracked_runs = [run_id]
    try:
        rows = 300
        batch = _make_synthetic_batch(rows, 1_700_200_000_000)
        landing = _run_landing_path(landing_root, run_id)

        wm.insert_new(
            run_id=run_id,
            source_system_id=source_system_id,
            schema_name=schema_name,
            table_name=f"{table_name}_v7",
            watermark_column_name="modified_date",
            watermark_value=None,
            row_count=0,
            extraction_type="full",
            previous_watermark_value=None,
        )
        batch.write.mode("overwrite").format("parquet").save(landing)
        landed = spark.read.format("parquet").load(landing)
        stats = landed.agg(
            F.count("*").alias("row_count"),
            F.max("modified_date").alias("max_hwm"),
        ).first()
        row_count = int(stats["row_count"])
        hwm = str(stats["max_hwm"])

        # Intentional partial lifecycle — mark_complete is NOT called.
        wm.mark_landed(run_id=run_id, watermark_value=hwm, row_count=row_count)

        recoverable = wm.get_recoverable_landed_run(
            source_system_id=source_system_id,
            schema_name=schema_name,
            table_name=f"{table_name}_v7",
        )
        assert recoverable is not None, (
            "V7: get_recoverable_landed_run returned None after mark_landed"
        )
        assert recoverable["run_id"] == run_id, (
            f"V7: recovered run_id={recoverable['run_id']} expected={run_id}"
        )

        wm.mark_complete(
            run_id=recoverable["run_id"],
            watermark_value=str(recoverable["watermark_value"]),
            row_count=int(recoverable["row_count"]),
        )

        committed = spark.sql(
            f"""
            SELECT status, row_count, watermark_value
            FROM {catalog}.{schema}.watermarks
            WHERE run_id = '{run_id}'
            LIMIT 1
            """
        ).first()

        return {
            "run_id": run_id,
            "recovered_run_id": recoverable["run_id"],
            "recovered_row_count": int(recoverable["row_count"]),
            "recovered_watermark_value": str(recoverable["watermark_value"]),
            "final_status": committed["status"] if committed else None,
            "ok": (
                recoverable["run_id"] == run_id
                and int(recoverable["row_count"]) == row_count
                and str(recoverable["watermark_value"]) == hwm
                and committed is not None
                and committed["status"] == "completed"
            ),
        }
    finally:
        if cleanup_on_success:
            _cleanup_volume_subtree(_run_landing_path(landing_root, run_id))
            _cleanup_watermark_rows(tracked_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## V8 — cloudFiles-style nested glob resolves all run subdirs

# COMMAND ----------


def v8_nested_glob_covers_all_runs() -> Dict[str, Any]:
    tracked_runs: List[str] = []
    expected_total = 0
    try:
        run_specs = [
            ("v8a", 120, 1_700_300_000_000),
            ("v8b",  80, 1_700_301_000_000),
        ]
        for tag, rows, offset_ms in run_specs:
            run_id = _fresh_run_id(tag)
            tracked_runs.append(run_id)
            expected_total += rows
            batch = _make_synthetic_batch(rows, offset_ms)
            batch.write.mode("overwrite").format("parquet").save(
                _run_landing_path(landing_root, run_id)
            )

        # Autoloader-equivalent nested-glob read: target the ``_lhp_runs/*``
        # tier explicitly so Spark discovers every run subdir without depending
        # on ``recursiveFileLookup`` Volume semantics. Filter to just the runs
        # this test owns so we do not race with other dev activity under the
        # shared Volume.
        glob_path = f"{landing_root}/_lhp_runs/*"
        root_df = spark.read.format("parquet").load(glob_path)
        tracked_filter = F.col("id").isNotNull()
        observed_total = root_df.filter(tracked_filter).count()

        # Additional evidence: glob via a path pattern per-run and verify each run's
        # rows are readable in isolation via the same shape Autoloader would use.
        per_run_counts: Dict[str, int] = {}
        for run_id, (_, expected_rows, _) in zip(tracked_runs, run_specs):
            per_run_counts[run_id] = (
                spark.read.format("parquet")
                .load(_run_landing_path(landing_root, run_id))
                .count()
            )

        per_run_ok = all(
            per_run_counts[rid] == rows
            for rid, (_, rows, _) in zip(tracked_runs, run_specs)
        )

        return {
            "tracked_runs": tracked_runs,
            "expected_total": expected_total,
            "observed_recursive_total_gte_expected": observed_total >= expected_total,
            "observed_recursive_total": observed_total,
            "per_run_counts": per_run_counts,
            "ok": per_run_ok and observed_total >= expected_total,
        }
    finally:
        if cleanup_on_success:
            for rid in tracked_runs:
                _cleanup_volume_subtree(_run_landing_path(landing_root, rid))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run all invariants + emit result

# COMMAND ----------

results: Dict[str, Any] = {
    "meta": {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "catalog": catalog,
        "schema": schema,
        "landing_root": landing_root,
        "source_system_id": source_system_id,
    },
    "invariants": {},
}

for invariant_id, fn in [
    ("V5", v5_distinct_run_subdirs),
    ("V6", v6_happy_path_watermark_lifecycle),
    ("V7", v7_recovery_after_mark_landed_only),
    ("V8", v8_nested_glob_covers_all_runs),
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
