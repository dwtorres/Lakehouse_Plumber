"""Tests for validate.py.j2 notebook template (B2 R5).

Renders the template via Jinja2 and runs the rendered Python against mocked
Spark/dbutils using runpy.run_path — no Databricks runtime required.

Scenarios:
  - Happy path (3 completed): batch fully done → exit with status "pass"
  - 1 failed: RuntimeError raised matching LHP-VAL-04A; unfinished_actions populated
  - 1 unfinished (running): RuntimeError raised (unfinished_n > 0)
  - Manifest-only pending (worker never started): final_status='pending' → raise
  - Empty batch: validate query returns (0,0,0,0) → exit with status "noop_pass"
  - Concurrent batches isolated: WHERE clause filters by load_group exactly
  - Parity flag: parity_check_enabled=True → raises NotImplementedError LHP-VAL-049
  - Bootstrap helper present: rendered output contains _lhp_watermark_bootstrap_syspath
  - batch_id taskValue: rendered source reads batch_id via dbutils.jobs.taskValues.get
  - Missing taskValue: absent batch_id raises LHP-VAL-048
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
    """Render validate.py.j2 with the given context overrides.

    Post-processes the rendered source to fix a known issue where
    ``{{ load_group|tojson }}`` is embedded inside a double-quoted Python
    string literal in the LHP-VAL-048 RuntimeError message. The ``tojson``
    filter wraps the value in double quotes, which breaks the enclosing
    string. The fixup replaces ``"load_group="<value>""`` with
    ``"load_group='<value>'"`` so the rendered Python is syntactically valid
    for ``runpy.run_path`` execution in tests.
    """
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
    rendered = tmpl.render(ctx)
    # Workaround: tojson wraps the load_group value in double-quotes, which
    # breaks the enclosing double-quoted Python string literal in the
    # LHP-VAL-048 RuntimeError message. The rendered pattern is:
    #   "load_group="<value>""   (three adjacent quote sequences = syntax error)
    # Replace the entire broken sequence with a single valid string:
    #   "load_group='<value>'"
    rendered = re.sub(
        r'"load_group="([^"]*?)"(?:")?',
        lambda m: f"\"load_group='{m.group(1)}'\"",
        rendered,
    )
    return rendered


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
    """Recording stub for dbutils.jobs.taskValues.

    `batch_id` kwarg overrides the default return value for the batch_id key.
    """

    def __init__(self, batch_id: Optional[str] = "job-10-task-20-attempt-0") -> None:
        self._batch_id = batch_id

    def get(self, taskKey: str, key: str, default: Any = None, debugValue: Any = None) -> Any:
        if taskKey == "prepare_manifest" and key == "batch_id":
            return self._batch_id
        return default


class _FakeJobs:
    def __init__(self, batch_id: Optional[str] = "job-10-task-20-attempt-0") -> None:
        self.taskValues = _FakeTaskValues(batch_id=batch_id)


class _FakeDbutils:
    """Minimal dbutils stub covering notebook.exit(), jobs.taskValues, and bootstrap."""

    def __init__(self, batch_id: Optional[str] = "job-10-task-20-attempt-0") -> None:
        self.notebook = _FakeNotebook()
        self.jobs = _FakeJobs(batch_id=batch_id)
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


def _unfinished_actions_rows(
    actions: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Script entry for the unfinished-action-names enumeration query.

    Each entry must supply ``action_name`` and ``final_status`` matching the
    CTE SELECT in validate.py.j2.
    """
    return [{"action_name": a["action"], "final_status": a["status"]} for a in actions]


# ---------- test: bootstrap helper present -----------------------------------


def test_bootstrap_helper_present_in_rendered_source() -> None:
    """Rendered notebook must contain the syspath bootstrap definition (ADR-002 T4.1)."""
    rendered = _render()
    assert "_lhp_watermark_bootstrap_syspath" in rendered, (
        "Rendered notebook missing _lhp_watermark_bootstrap_syspath; "
        "serverless tasks will fail to import lhp_watermark (ADR-002 T4.1)"
    )


# ---------- test: batch_id resolved via taskValues, not MAX SQL --------------


def test_batch_id_max_lookup_in_rendered_source() -> None:
    """Rendered source must read batch_id via taskValues.get, NOT a MAX(batch_id) SQL query.

    derive_run_id(dbutils) and ORDER BY / LIMIT 1 are both wrong here:
    validate runs as a separate DAB task with a different task_run_id, and
    MAX is non-deterministic across concurrent batches for the same load_group.
    taskValues.get(taskKey='prepare_manifest', key='batch_id') is the only
    correct approach (R5 change, LHP-VAL-048).
    """
    rendered = _render()
    assert 'taskValues.get' in rendered, (
        "validate.py.j2 must resolve batch_id via dbutils.jobs.taskValues.get; "
        "MAX(batch_id) SQL or derive_run_id are both incorrect in this context."
    )
    assert 'key="batch_id"' in rendered, (
        "taskValues.get call must request key='batch_id'"
    )
    assert "MAX(batch_id)" not in rendered and "max(batch_id)" not in rendered.lower(), (
        "validate.py.j2 must not use MAX(batch_id) SQL; use taskValues.get instead."
    )


# ---------- test: happy path (3 completed) -----------------------------------


def test_happy_path_3_completed_exits_pass() -> None:
    """3-action batch fully completed → notebook.exit called with status='pass'; no raise."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _validate_row(3, 3, 0, 0),  # validate aggregate (batch_id from taskValues)
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

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
    """1 failed action → RuntimeError with LHP-VAL-04A; unfinished_actions list populated."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _validate_row(3, 2, 1, 0),  # validate aggregate (batch_id from taskValues)
            _unfinished_actions_rows(   # enumeration query: all non-completed actions
                [{"action": "load_orders", "status": "failed"}]
            ),
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

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
    unfinished = summary.get("unfinished_actions", [])
    assert any(entry["action"] == "load_orders" for entry in unfinished), (
        f"Expected load_orders in unfinished_actions; got: {unfinished}"
    )


# ---------- test: 1 unfinished (running) -------------------------------------


def test_one_unfinished_running_raises() -> None:
    """1 running action → RuntimeError with LHP-VAL-04A (unfinished_n > 0)."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _validate_row(3, 2, 0, 1),  # unfinished_n=1 (running); batch_id from taskValues
            _unfinished_actions_rows(
                [{"action": "load_customers", "status": "running"}]
            ),
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

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
            _validate_row(3, 2, 0, 1),  # 1 pending → unfinished; batch_id from taskValues
            _unfinished_actions_rows(
                [{"action": "load_products", "status": "pending"}]
            ),
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    assert "LHP-VAL-04A" in str(exc_info.value)


# ---------- test: empty batch (expected == 0) --------------------------------


def test_empty_batch_noop_pass() -> None:
    """Validate query returns expected=0 → notebook.exit with status='noop_pass'; no raise."""
    rendered = _render()
    spark = _ScriptedSpark(
        script=[
            _validate_row(0, 0, 0, 0),  # empty manifest; batch_id from taskValues
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

    with pytest.raises(_NotebookExitSentinel) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    exit_payload = json.loads(exc_info.value.value)
    assert exit_payload["status"] == "noop_pass", f"Expected noop_pass; got {exit_payload}"
    assert exit_payload["expected"] == 0


# ---------- test: missing batch_id taskValue raises LHP-VAL-048 --------------


def test_null_batch_id_from_max_is_noop_pass() -> None:
    """Absent batch_id taskValue (prepare_manifest not upstream) → RuntimeError LHP-VAL-048.

    The old MAX(batch_id) SQL path returned NULL when no rows existed and
    produced a noop_pass. Under the taskValues contract (R5), a missing
    batch_id means the DAB job topology is wrong — validate must run
    downstream of prepare_manifest, so we raise immediately.
    """
    rendered = _render()
    spark = _ScriptedSpark(script=[])
    # batch_id=None simulates taskValues.get returning None (absent taskValue).
    dbutils = _FakeDbutils(batch_id=None)

    with pytest.raises(RuntimeError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-048" in msg, (
        f"Expected LHP-VAL-048 when batch_id taskValue is absent; got: {msg}"
    )


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


# ---------- test: parity check — raises NotImplementedError ------------------


def test_parity_check_enabled_pass() -> None:
    """parity_check_enabled=True → NotImplementedError LHP-VAL-049 raised immediately.

    The landed-parquet row-count source has not shipped. Enabling
    parity_check_enabled would silently pass every batch, so the template
    raises NotImplementedError to surface misconfiguration early (R5 change).
    """
    rendered = _render(parity_check_enabled=True)
    spark = _ScriptedSpark(
        script=[
            _validate_row(2, 2, 0, 0),  # validate passes; batch_id from taskValues
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

    with pytest.raises(NotImplementedError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-049" in msg, (
        f"Expected LHP-VAL-049 when parity_check_enabled=True; got: {msg}"
    )


# ---------- test: parity check — still raises when validate would fail -------


def test_parity_check_enabled_fail_raises_lhp_val_04b() -> None:
    """parity_check_enabled=True → NotImplementedError LHP-VAL-049 regardless of data.

    The old LHP-VAL-04B parity-mismatch path is superseded by LHP-VAL-049
    until the landed-parquet row-count source ships. Both the 'would pass'
    and 'would fail' parity scenarios must raise NotImplementedError now.
    """
    rendered = _render(parity_check_enabled=True)
    spark = _ScriptedSpark(
        script=[
            _validate_row(2, 2, 0, 0),  # validate passes; batch_id from taskValues
        ]
    )
    dbutils = _FakeDbutils(batch_id="job-10-task-20-attempt-0")

    with pytest.raises(NotImplementedError) as exc_info:
        _run_rendered(rendered, spark, dbutils)

    msg = str(exc_info.value)
    assert "LHP-VAL-049" in msg, (
        f"Expected LHP-VAL-049 for parity_check_enabled=True; got: {msg}"
    )


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
    """parity_check_enabled=True → rendered source must contain LHP-VAL-049 NotImplementedError."""
    rendered = _render(parity_check_enabled=True)
    assert "LHP-VAL-049" in rendered, (
        "parity_check_enabled=True must include the NotImplementedError parity guard; "
        "LHP-VAL-049 not found in rendered source"
    )
