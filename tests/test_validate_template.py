"""Tests for validate.py.j2 notebook template (B2 R5).

Renders the template via Jinja2 and runs the rendered Python against mocked
Spark/dbutils using runpy.run_path — no Databricks runtime required.

Scenarios:
  - Happy path (3 completed): batch fully done → exit with status "pass"
  - 1 failed: RuntimeError raised matching LHP-VAL-04A; failed_actions populated
  - 1 unfinished (running): RuntimeError raised (unfinished_n > 0)
  - Manifest-only pending (worker never started): final_status='pending' → raise
  - Empty batch: validate query returns (0,0,0,0) → exit with status "noop_pass"
  - Concurrent batches isolated: WHERE clause filters by load_group exactly
  - Parity flag pass: parity_check_enabled=True, no mismatches → exit pass
  - Parity flag fail: parity_check_enabled=True, mismatch found → raises LHP-VAL-04B
  - Bootstrap helper present: rendered output contains _lhp_watermark_bootstrap_syspath
  - batch_id MAX lookup: rendered SQL contains MAX(batch_id)
"""

from __future__ import annotations

import json
import os
import re
import runpy
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest
from jinja2 import Environment, FileSystemLoader

# ---------- template loading helpers -----------------------------------------

_TEMPLATE_DIR = Path(__file__).parent.parent / "src" / "lhp" / "templates" / "bundle"
_TEMPLATE_NAME = "validate.py.j2"


def _render(**overrides: Any) -> str:
    """Render validate.py.j2 with the given context overrides."""
    env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)))
    tmpl = env.get_template(_TEMPLATE_NAME)
    ctx: Dict[str, Any] = {
        "wm_catalog": "metadata",
        "wm_schema": "devtest_orchestration",
        "pipeline_name": "test_pipeline",
        "flowgroup_name": "test_fg",
        "manifest_table": "metadata.devtest_orchestration.b2_manifests",
        "watermarks_table": "metadata.devtest_orchestration.watermarks",
        "load_group": "test_pipeline::test_fg",
        "parity_check_enabled": False,
    }
    ctx.update(overrides)
    return tmpl.render(ctx)


# ---------- mock infrastructure ----------------------------------------------


class _FakeNotebook:
    """Stub for dbutils.notebook with exit() recording."""

    def __init__(self) -> None:
        self.exit_calls: List[str] = []
        # Wire bootstrap path chain.
        ctx_mock = MagicMock()
        ctx_mock.notebookPath.return_value.get.return_value = (
            "/Workspace/Users/test@example.com/.bundle/lhp/files/notebooks/validate"
        )
        self.entry_point = MagicMock()
        self.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value = (
            ctx_mock
        )

    def exit(self, value: str) -> None:
        self.exit_calls.append(value)
        # Mimic Databricks behaviour: notebook.exit() stops execution by raising.
        raise _NotebookExitSentinel(value)


class _NotebookExitSentinel(Exception):
    """Raised by _FakeNotebook.exit() to halt script execution, as Databricks does."""

    def __init__(self, value: str) -> None:
        self.value = value
        super().__init__(value)


class _FakeTaskValues:
    """Recording stub for dbutils.jobs.taskValues."""

    def get(self, taskKey: str, key: str, default: Any = None) -> Any:
        return default


class _FakeJobs:
    def __init__(self) -> None:
        self.taskValues = _FakeTaskValues()


class _FakeDbutils:
    """Minimal dbutils stub covering notebook.exit(), jobs.taskValues, and bootstrap."""

    def __init__(self) -> None:
        self.notebook = _FakeNotebook()
        self.jobs = _FakeJobs()
        self.widgets = MagicMock()
        self.widgets.get.return_value = None


class _ScriptedSpark:
    """Recording SparkSession with scripted sql().collect() responses in order.

    Each entry in `script` corresponds to one spark.sql(...).collect() call:
      - list[dict]  → return that as the collect() result (rows as dicts
                       accessible via __getitem__)
      - Exception   → raise that exception from sql()
      - "noop"      → return [] from collect()

    Calls that exceed the script length default to returning [].
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
            return r

        action = self.script.pop(0)

        if isinstance(action, BaseException):
            raise action

        if action == "noop":
            r = MagicMock()
            r.collect.return_value = []
            return r

        # action is a list of row-dicts → wrap in a mock with row-like access.
        rows = [_DictRow(d) for d in action]
        r = MagicMock()
        r.collect.return_value = rows
        return r


class _DictRow:
    """Row-like object supporting dict-style key access."""

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __repr__(self) -> str:
        return f"_DictRow({self._data!r})"


def _run_rendered(
    rendered: str,
    spark: _ScriptedSpark,
    dbutils: _FakeDbutils,
) -> Dict[str, Any]:
    """Write rendered notebook source to a temp file and execute via runpy.run_path.

    Returns the namespace dict. Callers must catch _NotebookExitSentinel or
    RuntimeError as appropriate for the scenario under test.
    """
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        prefix="lhp_validate_test_",
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


# ---------- scripted response helpers ----------------------------------------


def _batch_id_row(batch_id: Optional[str]) -> List[Dict[str, Any]]:
    """Script entry for the MAX(batch_id) lookup query."""
    return [{"batch_id": batch_id}]


def _validate_row(
    expected: int,
    completed_n: int,
    failed_n: int,
    unfinished_n: int,
) -> List[Dict[str, Any]]:
    """Script entry for the main validate aggregate query."""
    return [
        {
            "expected": expected,
            "completed_n": completed_n,
            "failed_n": failed_n,
            "unfinished_n": unfinished_n,
        }
    ]


def _failed_actions_rows(names: List[str]) -> List[Dict[str, Any]]:
    """Script entry for the failed-action-names query."""
    return [{"action_name": n} for n in names]


# ---------- test: bootstrap helper present -----------------------------------


def test_bootstrap_helper_present_in_rendered_source() -> None:
    """Rendered notebook must contain the syspath bootstrap definition (ADR-002 T4.1)."""
    rendered = _render()
    assert "_lhp_watermark_bootstrap_syspath" in rendered, (
        "Rendered notebook missing _lhp_watermark_bootstrap_syspath; "
        "serverless tasks will fail to import lhp_watermark (ADR-002 T4.1)"
    )


# ---------- test: batch_id MAX lookup in rendered SQL ------------------------


def test_batch_id_max_lookup_in_rendered_source() -> None:
    """Rendered SQL must use MAX(batch_id) for batch_id resolution (not derive_run_id)."""
    rendered = _render()
    assert "MAX(batch_id)" in rendered or "max(batch_id)" in rendered.lower(), (
        "validate.py.j2 must resolve batch_id via MAX(batch_id) lookup on b2_manifests; "
        "derive_run_id(dbutils) would produce a different token in this task."
    )


# ---------- test: happy path (3 completed) -----------------------------------


def test_happy_path_3_completed_exits_pass() -> None:
    """3-action batch fully completed → notebook.exit called with status='pass'; no raise."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),  # MAX(batch_id) lookup
            _validate_row(3, 3, 0, 0),                  # validate aggregate
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(_NotebookExitSentinel) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    exit_payload = json.loads(exc_info.value.value)
    assert exit_payload["status"] == "pass", f"Expected status=pass; got {exit_payload}"
    assert exit_payload["expected"] == 3
    assert exit_payload["completed_n"] == 3
    assert exit_payload["failed_n"] == 0
    assert exit_payload["unfinished_n"] == 0
    assert exit_payload["batch_id"] == "job-10-task-20-attempt-0"


# ---------- test: 1 failed ---------------------------------------------------


def test_one_failed_raises_lhp_val_04a() -> None:
    """1 failed action → RuntimeError with LHP-VAL-04A; failed_actions list populated."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),  # MAX(batch_id) lookup
            _validate_row(3, 2, 1, 0),                  # validate aggregate
            _failed_actions_rows(["load_orders"]),       # failed action names query
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-04A" in msg, f"Expected LHP-VAL-04A error code; got: {msg}"

    # Extract the JSON summary from the error message.
    json_match = re.search(r"\{.*\}", msg, re.DOTALL)
    assert json_match, "RuntimeError must embed JSON summary"
    summary = json.loads(json_match.group())
    assert summary["status"] == "fail"
    assert summary["failed_n"] == 1
    assert "load_orders" in summary.get("failed_actions", [])


# ---------- test: 1 unfinished (running) -------------------------------------


def test_one_unfinished_running_raises() -> None:
    """1 running action → RuntimeError with LHP-VAL-04A (unfinished_n > 0)."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),
            _validate_row(3, 2, 0, 1),       # unfinished_n=1 (running)
            _failed_actions_rows([]),         # no failed actions in manifest
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-04A" in msg

    json_match = re.search(r"\{.*\}", msg, re.DOTALL)
    assert json_match
    summary = json.loads(json_match.group())
    assert summary["unfinished_n"] == 1


# ---------- test: manifest-only pending (worker never started) ---------------


def test_manifest_pending_worker_null_raises() -> None:
    """action with manifest_status='pending' and NULL worker_status → final='pending' → raise.

    The validate aggregate counts this as unfinished_n=1 (pending is in the
    unfinished bucket). The scripted validate query simulates the coalesced result.
    """
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),
            _validate_row(3, 2, 0, 1),    # 1 pending → unfinished
            _failed_actions_rows([]),
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    assert "LHP-VAL-04A" in str(exc_info.value)


# ---------- test: empty batch (expected == 0) --------------------------------


def test_empty_batch_noop_pass() -> None:
    """Validate query returns expected=0 → notebook.exit with status='noop_pass'; no raise."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),
            _validate_row(0, 0, 0, 0),
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(_NotebookExitSentinel) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    exit_payload = json.loads(exc_info.value.value)
    assert exit_payload["status"] == "noop_pass", f"Expected noop_pass; got {exit_payload}"
    assert exit_payload["expected"] == 0


# ---------- test: null batch_id from MAX (no rows in window) -----------------


def test_null_batch_id_from_max_is_noop_pass() -> None:
    """MAX(batch_id) returns NULL (no rows in 24h window) → noop_pass; no raise."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _batch_id_row(None),  # NULL from MAX — no manifest rows in window
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(_NotebookExitSentinel) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    exit_payload = json.loads(exc_info.value.value)
    assert exit_payload["status"] == "noop_pass"
    assert exit_payload["batch_id"] is None


# ---------- test: concurrent batches isolated by load_group ------------------


def test_concurrent_batches_isolated_by_load_group() -> None:
    """Rendered batch_id lookup SQL filters by the exact load_group, not a global scan."""
    load_group = "bronze::orders_fg"
    rendered = _render(
        load_group=load_group,
        pipeline_name="bronze",
        flowgroup_name="orders_fg",
    )
    # The MAX(batch_id) lookup must contain the literal load_group value.
    assert f"'bronze::orders_fg'" in rendered or f'"bronze::orders_fg"' in rendered, (
        "Rendered SQL must contain the load_group literal to isolate concurrent batches; "
        f"load_group={load_group!r} not found as a SQL literal in rendered source."
    )
    # Also verify MAX(batch_id) is scoped (not a global scan).
    assert "load_group" in rendered, "Rendered SQL must filter on load_group column"


# ---------- test: parity check — pass ----------------------------------------


def test_parity_check_enabled_pass() -> None:
    """parity_check_enabled=True, no parity mismatches → exit with status='pass'."""
    rendered = _render(parity_check_enabled=True)
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),
            _validate_row(2, 2, 0, 0),       # validate passes
            # parity query: both columns equal → no mismatches
            [
                {"action_name": "load_a", "jdbc_rows_read": 100, "landed_rows": 100},
                {"action_name": "load_b", "jdbc_rows_read": 50, "landed_rows": 50},
            ],
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(_NotebookExitSentinel) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    exit_payload = json.loads(exc_info.value.value)
    assert exit_payload["status"] == "pass"


# ---------- test: parity check — fail ----------------------------------------


def test_parity_check_enabled_fail_raises_lhp_val_04b() -> None:
    """parity_check_enabled=True, mismatch found → raises RuntimeError with LHP-VAL-04B."""
    rendered = _render(parity_check_enabled=True)
    spark = _ScriptedSpark(
        script=[
            _batch_id_row("job-10-task-20-attempt-0"),
            _validate_row(2, 2, 0, 0),       # validate passes
            # parity query: load_a has a mismatch (stub: same column, demo only)
            # In production the two columns will differ when the real source is wired.
            # For this stub test we simulate a mismatch by using patched data.
            [
                {"action_name": "load_a", "jdbc_rows_read": 100, "landed_rows": 80},
                {"action_name": "load_b", "jdbc_rows_read": 50, "landed_rows": 50},
            ],
        ]
    )
    dbutils = _FakeDbutils()

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-04B" in msg, f"Expected LHP-VAL-04B error code; got: {msg}"

    json_match = re.search(r"\{.*\}", msg, re.DOTALL)
    assert json_match, "RuntimeError must embed JSON summary"
    summary = json.loads(json_match.group())
    assert "parity_mismatches" in summary
    mismatches = summary["parity_mismatches"]
    assert len(mismatches) == 1
    assert mismatches[0]["action"] == "load_a"


# ---------- test: parity block absent when disabled --------------------------


def test_parity_block_absent_when_disabled() -> None:
    """parity_check_enabled=False → rendered source must not contain LHP-VAL-04B."""
    rendered = _render(parity_check_enabled=False)
    assert "LHP-VAL-04B" not in rendered, (
        "parity_check_enabled=False must suppress the parity block; "
        "LHP-VAL-04B found in rendered source"
    )


# ---------- test: parity block present when enabled --------------------------


def test_parity_block_present_when_enabled() -> None:
    """parity_check_enabled=True → rendered source must contain LHP-VAL-04B."""
    rendered = _render(parity_check_enabled=True)
    assert "LHP-VAL-04B" in rendered, (
        "parity_check_enabled=True must include the parity block; "
        "LHP-VAL-04B not found in rendered source"
    )
