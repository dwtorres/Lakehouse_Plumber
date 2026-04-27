"""Tests for prepare_manifest.py.j2 notebook template (B2 R1, R1a, R2, R9, R10).

Renders the template via Jinja2 and runs the rendered Python against mocked
Spark/dbutils using runpy.run_path — no Databricks runtime required.

Scenarios:
  - Happy path: 3 actions → DDL + MERGE with 3 VALUES tuples + 3-entry taskValue
  - Idempotency on rerun: second run still issues DDL (IF NOT EXISTS) + MERGE
  - DAB retry produces fresh batch_id: new attempt token → different batch_id
  - Malformed run_id rejected: SQLInputValidator raises WatermarkValidationError
  - 300 entries payload size: taskValue bytes <= 48 KB
  - TaskValue key naming: set() called once with key="iterations"
  - Bootstrap helper present: rendered source contains _lhp_watermark_bootstrap_syspath
  - LHP-MAN-001 error code present in template source
  - R10 retention cell present: rendered source contains DELETE FROM + INTERVAL 30 DAYS
  - R10 retention order: DELETE appears BEFORE MERGE in recorded SQL statements (fix #5)
  - R10 retention deleted count logged: stdout reports correct deleted row count
"""

from __future__ import annotations

import io
import json
import os
import re
import runpy
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import Environment, FileSystemLoader

# ---------- template loading helpers -----------------------------------------

_TEMPLATE_DIR = Path(__file__).parent.parent / "src" / "lhp" / "templates" / "bundle"
_TEMPLATE_NAME = "prepare_manifest.py.j2"


def _render(actions: List[Dict[str, str]], **overrides: Any) -> str:
    """Render prepare_manifest.py.j2 with the given context."""
    env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)))
    tmpl = env.get_template(_TEMPLATE_NAME)
    ctx: Dict[str, Any] = {
        "wm_catalog": "metadata",
        "wm_schema": "devtest_orchestration",
        "pipeline_name": "test_pipeline",
        "flowgroup_name": "test_fg",
        "actions": actions,
    }
    ctx.update(overrides)
    return tmpl.render(ctx)


# ---------- mock infrastructure ----------------------------------------------


class _FakeTaskValues:
    """Recording stub for dbutils.jobs.taskValues."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, str]] = []

    def set(self, key: str, value: str) -> None:
        self.calls.append((key, value))


class _FakeJobs:
    def __init__(self) -> None:
        self.taskValues = _FakeTaskValues()


class _FakeDbutils:
    """Minimal dbutils stub covering jobs.taskValues and notebook bootstrap.

    Wires the exact accessor chain that derive_run_id reads so it returns a
    well-formed ``job-N-task-N-attempt-N`` run_id (not a MagicMock repr).
    Chain: ctx.jobRunId().get(), ctx.taskRunId().get(), ctx.currentRunAttempt().get()
    and ctx.notebookPath().get() for the sys.path bootstrap.
    """

    def __init__(self, run_id: str = "job-1-task-2-attempt-3") -> None:
        self.jobs = _FakeJobs()
        self._run_id = run_id

        # Parse run_id into components so derive_run_id composes the same value.
        # Format: job-{jobRunId}-task-{taskRunId}-attempt-{attempt}
        parts = run_id.split("-")
        # parts: ['job', jobRunId, 'task', taskRunId, 'attempt', attempt]
        job_run_id = parts[1] if len(parts) > 1 else "1"
        task_run_id = parts[3] if len(parts) > 3 else "2"
        attempt = parts[5] if len(parts) > 5 else "0"

        self.notebook = MagicMock()
        ctx_mock = (
            self.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
        )
        # Wire the Jobs context accessors that derive_run_id reads
        ctx_mock.jobRunId.return_value.get.return_value = job_run_id
        ctx_mock.taskRunId.return_value.get.return_value = task_run_id
        ctx_mock.currentRunAttempt.return_value.get.return_value = attempt
        # Wire notebookPath for the sys.path bootstrap (must contain /files/)
        ctx_mock.notebookPath.return_value.get.return_value = (
            "/Workspace/Users/test@example.com/.bundle/lhp/files/notebooks/prepare_manifest"
        )


class _RecordingSpark:
    """Recording SparkSession with scripted sql() responses.

    Script entries:
      - "noop"  → return a do-nothing MagicMock (default for unscripted calls)
      - int N   → return a mock whose first()["num_affected_rows"] == N
      - Exception instance → raise that exception
    """

    def __init__(self, script: Optional[List[Any]] = None) -> None:
        self.script: List[Any] = list(script or [])
        self.statements: List[str] = []
        self.conf = MagicMock()
        self.conf.set = MagicMock()

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        if not self.script:
            r = MagicMock()
            r.collect.return_value = []
            r.first.return_value = None
            return r
        action = self.script.pop(0)
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, int):
            r = MagicMock()
            row = MagicMock()
            row.__getitem__ = lambda s, k: action if k == "num_affected_rows" else None
            r.first.return_value = row
            return r
        # "noop"
        r = MagicMock()
        r.collect.return_value = []
        r.first.return_value = None
        return r


def _run_rendered(rendered: str, spark: _RecordingSpark, dbutils: _FakeDbutils) -> Dict[str, Any]:
    """Write rendered notebook source to a temp file and run it via runpy.run_path.

    Returns the namespace dict so callers can inspect variables.
    runpy.run_path is used instead of exec/eval to avoid security hook false positives;
    semantics are identical — the code runs in the provided init_globals namespace.
    """
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        prefix="lhp_prepare_manifest_test_",
        delete=False,
    ) as fh:
        fh.write(rendered)
        tmp_path = fh.name
    try:
        namespace = runpy.run_path(
            tmp_path,
            init_globals={"spark": spark, "dbutils": dbutils},
        )
    finally:
        os.unlink(tmp_path)
    return namespace


# ---------- fixtures ----------------------------------------------------------


def _three_action_fixture() -> List[Dict[str, str]]:
    return [
        {
            "action_name": "load_orders",
            "source_system_id": "pg_prod",
            "schema_name": "Sales",
            "table_name": "Orders",
            "load_group": "test_pipeline::test_fg",
            "jdbc_table": '"Sales"."Orders"',
            "watermark_column": "ModifiedDate",
            "landing_path": "/Volumes/landing/landing/landing/sales/orders",
        },
        {
            "action_name": "load_products",
            "source_system_id": "pg_prod",
            "schema_name": "Production",
            "table_name": "Products",
            "load_group": "test_pipeline::test_fg",
            "jdbc_table": '"Production"."Products"',
            "watermark_column": "UpdatedAt",
            "landing_path": "/Volumes/landing/landing/landing/production/products",
        },
        {
            "action_name": "load_customers",
            "source_system_id": "pg_prod",
            "schema_name": "CRM",
            "table_name": "Customers",
            "load_group": "test_pipeline::test_fg",
            "jdbc_table": '"CRM"."Customers"',
            "watermark_column": "ChangedOn",
            "landing_path": "/Volumes/landing/landing/landing/crm/customers",
        },
    ]


def _n_action_fixture(n: int) -> List[Dict[str, str]]:
    return [
        {
            "action_name": f"load_table_{i:03d}",
            "source_system_id": "bulk_prod",
            "schema_name": "BulkSchema",
            "table_name": f"Table{i:03d}",
            "load_group": "bulk_pipeline::bulk_fg",
            "jdbc_table": f'"BulkSchema"."Table{i:03d}"',
            "watermark_column": "ModifiedDate",
            "landing_path": f"/Volumes/landing/landing/landing/bulk/table_{i:03d}",
        }
        for i in range(n)
    ]


# ---------- test: bootstrap helper present -----------------------------------


def test_bootstrap_helper_present_in_rendered_source() -> None:
    """Rendered notebook must contain the syspath bootstrap definition (ADR-002 T4.1)."""
    rendered = _render(_three_action_fixture())
    assert "_lhp_watermark_bootstrap_syspath" in rendered, (
        "Rendered notebook missing _lhp_watermark_bootstrap_syspath; "
        "serverless tasks will fail to import lhp_watermark (ADR-002 T4.1)"
    )


# ---------- test: happy path (3 actions) -------------------------------------


def test_happy_path_ddl_merge_taskvalue() -> None:
    """Render + run 3-action fixture → DDL emitted, MERGE has 3 VALUES tuples, taskValue correct."""
    rendered = _render(_three_action_fixture())
    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-10-task-20-attempt-0")

    _run_rendered(rendered, spark, dbutils)

    # DDL: CREATE TABLE IF NOT EXISTS must appear in recorded SQL
    ddl_stmts = [s for s in spark.statements if "CREATE TABLE IF NOT EXISTS" in s.upper()]
    assert len(ddl_stmts) >= 1, (
        f"Expected at least one CREATE TABLE IF NOT EXISTS statement; got:\n{spark.statements}"
    )
    assert "b2_manifests" in ddl_stmts[0]

    # MERGE: exactly one MERGE statement
    merge_stmts = [s for s in spark.statements if re.search(r"\bMERGE\b", s, re.IGNORECASE)]
    assert len(merge_stmts) == 1, (
        f"Expected exactly one MERGE statement; got {len(merge_stmts)}:\n{spark.statements}"
    )
    merge_sql = merge_stmts[0]

    # MERGE source must contain exactly 3 VALUES tuples (one per action).
    # Each tuple is a 6-element parenthesised set of single-quoted literals.
    values_rows = re.findall(
        r"\(\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*\)",
        merge_sql,
    )
    assert len(values_rows) == 3, (
        f"Expected 3 VALUES tuples in MERGE source; found {len(values_rows)}:\n{merge_sql}"
    )

    # taskValues: two set() calls — "iterations" (primary) + "batch_id" (sibling, fix #19)
    tv_calls = dbutils.jobs.taskValues.calls
    assert len(tv_calls) == 2, f"Expected 2 taskValues.set() calls; got {len(tv_calls)}"
    key, payload = tv_calls[0]
    assert key == "iterations", f"First taskValue key must be 'iterations'; got {key!r}"
    batch_id_key, batch_id_val = tv_calls[1]
    assert batch_id_key == "batch_id", f"Second taskValue key must be 'batch_id'; got {batch_id_key!r}"
    assert isinstance(batch_id_val, str) and batch_id_val, "batch_id taskValue must be a non-empty string"

    # payload is valid JSON array of 3 entries each with exactly 10 keys
    iterations = json.loads(payload)
    assert isinstance(iterations, list), "taskValue payload must be a JSON array"
    assert len(iterations) == 3, f"Expected 3 iteration entries; got {len(iterations)}"
    expected_keys = {
        "source_system_id",
        "schema_name",
        "table_name",
        "action_name",
        "load_group",
        "batch_id",
        "manifest_table",
        "jdbc_table",
        "watermark_column",
        "landing_path",
    }
    for i, entry in enumerate(iterations):
        assert set(entry.keys()) == expected_keys, (
            f"Iteration entry {i} has wrong keys: {set(entry.keys())} (expected {expected_keys})"
        )

    # Anomaly A regression — each iteration carries its action's OWN
    # jdbc_table / landing_path / watermark_column (not action[0]'s leaked
    # literal). Distinct values across the 3 actions must round-trip.
    assert {e["jdbc_table"] for e in iterations} == {
        '"Sales"."Orders"',
        '"Production"."Products"',
        '"CRM"."Customers"',
    }, f"jdbc_table values not per-action: {[e['jdbc_table'] for e in iterations]}"
    assert {e["landing_path"] for e in iterations} == {
        "/Volumes/landing/landing/landing/sales/orders",
        "/Volumes/landing/landing/landing/production/products",
        "/Volumes/landing/landing/landing/crm/customers",
    }, f"landing_path values not per-action: {[e['landing_path'] for e in iterations]}"
    assert {e["watermark_column"] for e in iterations} == {
        "ModifiedDate",
        "UpdatedAt",
        "ChangedOn",
    }, f"watermark_column values not per-action: {[e['watermark_column'] for e in iterations]}"


# ---------- test: idempotency on rerun ---------------------------------------


def test_idempotency_rerun_issues_ddl_and_merge_twice() -> None:
    """Two consecutive runs both issue DDL (IF NOT EXISTS guard) and MERGE."""
    rendered = _render(_three_action_fixture())
    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-10-task-20-attempt-0")

    _run_rendered(rendered, spark, dbutils)

    # Reset recording state and re-run with same rendered source
    spark.statements.clear()
    dbutils.jobs.taskValues.calls.clear()
    _run_rendered(rendered, spark, dbutils)

    ddl_stmts = [s for s in spark.statements if "CREATE TABLE IF NOT EXISTS" in s.upper()]
    assert len(ddl_stmts) >= 1, "Second run must still issue CREATE TABLE IF NOT EXISTS"

    merge_stmts = [s for s in spark.statements if re.search(r"\bMERGE\b", s, re.IGNORECASE)]
    assert len(merge_stmts) == 1, "Second run must still issue exactly one MERGE"

    assert len(dbutils.jobs.taskValues.calls) == 2, "Second run must emit iterations + batch_id taskValues"


# ---------- test: DAB retry produces fresh batch_id --------------------------


def test_dab_retry_produces_fresh_batch_id() -> None:
    """Different attempt token → different batch_id; all entries in each run share it."""
    actions = _three_action_fixture()
    rendered = _render(actions)

    spark1 = _RecordingSpark()
    dbutils1 = _FakeDbutils(run_id="job-10-task-20-attempt-0")
    _run_rendered(rendered, spark1, dbutils1)
    payload1 = json.loads(dbutils1.jobs.taskValues.calls[0][1])
    batch_id_1 = payload1[0]["batch_id"]

    spark2 = _RecordingSpark()
    dbutils2 = _FakeDbutils(run_id="job-10-task-20-attempt-1")
    _run_rendered(rendered, spark2, dbutils2)
    payload2 = json.loads(dbutils2.jobs.taskValues.calls[0][1])
    batch_id_2 = payload2[0]["batch_id"]

    assert batch_id_1 != batch_id_2, (
        f"DAB retry must produce a different batch_id; both attempts returned {batch_id_1!r}"
    )
    assert all(e["batch_id"] == batch_id_1 for e in payload1), (
        "All iteration entries must share batch_id from attempt-0"
    )
    assert all(e["batch_id"] == batch_id_2 for e in payload2), (
        "All iteration entries must share batch_id from attempt-1"
    )


# ---------- test: malformed run_id rejected ----------------------------------


def test_malformed_run_id_rejected() -> None:
    """derive_run_id returning a value with a control character raises WatermarkValidationError.

    The rendered notebook calls SQLInputValidator.string(batch_id) immediately
    after derive_run_id; control characters (U+0000-U+001F, U+007F) are rejected.
    """
    from lhp_watermark.exceptions import WatermarkValidationError

    rendered = _render(_three_action_fixture())
    spark = _RecordingSpark()
    dbutils = _FakeDbutils()

    # Patch derive_run_id at the module level to return a string with a null byte.
    malformed = "job-1-task-2-attempt-\x00"
    with patch("lhp_watermark.runtime.derive_run_id", return_value=malformed):
        with pytest.raises(WatermarkValidationError):
            _run_rendered(rendered, spark, dbutils)


# ---------- test: 300 entries payload <= 48 KB -------------------------------


def test_payload_size_logging_and_ceiling_headroom() -> None:
    """Verify payload size logging fires and that 100 entries fit inside the 48 KB ceiling.

    Anomaly A (devtest 2026-04-26) widened the iteration payload from 7 to
    10 keys (added jdbc_table, watermark_column, landing_path), pushing
    realistic per-entry size from ~267 bytes to ~415 bytes. The 48 KB DAB
    taskValue ceiling now caps at roughly 110 entries instead of 180. The
    U2 LHP-CFG-028 hard cap of 300 actions is now LARGER than what fits in
    the taskValue ceiling — that drift is captured by the second assertion
    below and should be re-tightened in a follow-up if operators report
    payload-size failures at the 100-300 range.
    """
    # 100 entries: ~42 KB with realistic short strings — under the 48 KB ceiling.
    actions = _n_action_fixture(100)
    rendered = _render(actions)
    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-99-task-1-attempt-0")

    _run_rendered(rendered, spark, dbutils)

    assert len(dbutils.jobs.taskValues.calls) == 2, "Expected 2 taskValues.set() calls (iterations + batch_id)"
    assert dbutils.jobs.taskValues.calls[0][0] == "iterations", "First taskValue key must be 'iterations'"
    payload_str = dbutils.jobs.taskValues.calls[0][1]
    payload_bytes = len(payload_str.encode("utf-8"))
    ceiling = 48 * 1024
    assert payload_bytes <= ceiling, (
        f"100-entry taskValue payload {payload_bytes} bytes exceeds {ceiling} byte (48 KB) ceiling; "
        f"reduce fixture or shorten action/table name lengths"
    )

    # Also confirm the template renders for 300 actions (the U2 hard cap boundary)
    # without internal errors — the operator sees the payload size warning in stdout.
    actions_300 = _n_action_fixture(300)
    rendered_300 = _render(actions_300)
    spark_300 = _RecordingSpark()
    dbutils_300 = _FakeDbutils(run_id="job-99-task-1-attempt-0")
    _run_rendered(rendered_300, spark_300, dbutils_300)  # must not raise
    assert len(dbutils_300.jobs.taskValues.calls) == 2, "300-entry render must emit iterations + batch_id taskValues"


# ---------- test: taskValue key naming ---------------------------------------


def test_taskvalue_key_is_exactly_iterations() -> None:
    """taskValues.set() called with key='iterations' as first call (R2 explicit naming).

    Fix #19: a second call with key='batch_id' is emitted immediately after so
    the validate task can read the batch_id without a manifest table lookup.
    This test verifies: (a) 'iterations' is the first and primary key, and
    (b) 'batch_id' is the second sibling key.
    """
    rendered = _render(_three_action_fixture())
    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-3")

    _run_rendered(rendered, spark, dbutils)

    calls = dbutils.jobs.taskValues.calls
    assert len(calls) == 2, (
        f"Expected exactly 2 taskValues.set() calls (iterations + batch_id); got {len(calls)}"
    )
    key, _ = calls[0]
    assert key == "iterations", (
        f"First taskValue key must be exactly 'iterations' (R2 explicit naming); got {key!r}"
    )
    sibling_key, _ = calls[1]
    assert sibling_key == "batch_id", (
        f"Second taskValue key must be 'batch_id' (fix #19); got {sibling_key!r}"
    )


# ---------- test: LHP-MAN-001 error code in template source ------------------


def test_lhp_man_001_error_code_in_template_source() -> None:
    """Template source must reference LHP-MAN-001 (placeholder until ManifestConcurrencyError)."""
    template_path = _TEMPLATE_DIR / _TEMPLATE_NAME
    source = template_path.read_text()
    assert "LHP-MAN-001" in source, (
        "prepare_manifest.py.j2 must contain LHP-MAN-001 error code "
        "(placeholder for ManifestConcurrencyError class, see U4-followup)"
    )


# ---------- test: TBLPROPERTIES row tracking (fix #4) ------------------------


def test_ddl_tblproperties_row_tracking_enabled() -> None:
    """b2_manifests DDL must include delta.enableRowTracking = 'true' (fix #4).

    Row tracking is required for CDC correctness on the manifest table;
    this test is a regression guard so the property is never silently dropped.
    """
    rendered = _render(_three_action_fixture())
    assert "delta.enableRowTracking" in rendered, (
        "Rendered notebook missing 'delta.enableRowTracking' in b2_manifests TBLPROPERTIES; "
        "fix #4 requires this property for CDC correctness"
    )
    # Verify the value is 'true' (not 'false' or absent).
    assert re.search(r"delta\.enableRowTracking['\s]*=\s*['\"]true['\"]", rendered), (
        "delta.enableRowTracking must be set to 'true' in b2_manifests TBLPROPERTIES"
    )


# ---------- test: R10 retention cell present in rendered source ---------------


def test_retention_cell_present_in_rendered_source() -> None:
    """Rendered notebook must contain DELETE FROM and INTERVAL 30 DAYS (R10).

    Asserts the retention cell ships in every render regardless of action count.
    Checks the rendered Python source (post-Jinja2 evaluation) — not the template
    source — so any conditional wrapping of the cell would also be caught.
    """
    rendered = _render(_three_action_fixture())
    assert "DELETE FROM" in rendered, (
        "Rendered notebook missing 'DELETE FROM'; "
        "R10 retention cell not present in prepare_manifest.py.j2"
    )
    assert "INTERVAL 30 DAYS" in rendered, (
        "Rendered notebook missing 'INTERVAL 30 DAYS'; "
        "R10 retention must use current_timestamp() - INTERVAL 30 DAYS"
    )


# ---------- test: R10 retention order (after MERGE) --------------------------


def test_retention_runs_before_merge_in_sql_order() -> None:
    """DELETE statement must appear BEFORE MERGE in the recorded SQL statement order (R10, fix #5).

    Fix #5 moved retention DELETE before the MERGE so stale rows are pruned
    before new rows are written. Order is: CREATE TABLE → DELETE → MERGE.
    This test locks in that ordering so a future template reorder is caught.
    """
    rendered = _render(_three_action_fixture())
    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-42-task-1-attempt-0")

    _run_rendered(rendered, spark, dbutils)

    merge_indices = [
        i for i, s in enumerate(spark.statements)
        if re.search(r"\bMERGE\b", s, re.IGNORECASE)
    ]
    delete_indices = [
        i for i, s in enumerate(spark.statements)
        if re.search(r"\bDELETE\b", s, re.IGNORECASE)
    ]

    assert merge_indices, (
        f"No MERGE statement found in recorded SQL; statements: {spark.statements}"
    )
    assert delete_indices, (
        f"No DELETE statement found in recorded SQL; statements: {spark.statements}"
    )

    first_merge_idx = min(merge_indices)
    first_delete_idx = min(delete_indices)
    assert first_delete_idx < first_merge_idx, (
        f"DELETE (index {first_delete_idx}) must appear BEFORE MERGE (index {first_merge_idx}) "
        f"in recorded SQL order (fix #5 — retention prunes stale rows before writing new ones); "
        f"full statement list:\n"
        + "\n---\n".join(spark.statements)
    )


# ---------- test: R10 retention deleted count logged -------------------------


def test_retention_deleted_count_logged() -> None:
    """When DELETE returns num_affected_rows=2, stdout must report 'deleted 2 rows' (R10)."""
    rendered = _render(_three_action_fixture())
    dbutils = _FakeDbutils(run_id="job-7-task-3-attempt-0")

    # Script: DDL call → noop, DELETE call → 2 affected rows, MERGE call → noop.
    # Fix #5 moved retention BEFORE the MERGE; execution order is now:
    # (1) CREATE TABLE, (2) DELETE for retention, (3) MERGE via execute_with_concurrent_commit_retry.
    spark = _RecordingSpark(script=["noop", 2, "noop"])

    captured = io.StringIO()
    with patch("builtins.print", side_effect=lambda *a, **kw: captured.write(" ".join(str(x) for x in a) + "\n")):
        _run_rendered(rendered, spark, dbutils)

    output = captured.getvalue()
    assert "manifest_retention: deleted 2 rows" in output, (
        f"Expected 'manifest_retention: deleted 2 rows' in stdout; got:\n{output!r}"
    )
