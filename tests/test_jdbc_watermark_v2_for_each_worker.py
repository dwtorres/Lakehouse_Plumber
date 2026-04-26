"""Tests for jdbc_watermark_job.py.j2 B2 for_each worker path (U5: R3, R4, R12).

Renders the template via Jinja2 directly (no full LHP orchestrator) and
verifies conditional header, manifest UPDATE block, R12 operator defaults,
HIPAA hook marker, and raise-on-failure verbatim invariant.

Scenarios:
  - Happy path legacy: no execution_mode → static literals, no manifest UPDATE,
    ">=" watermark predicate
  - Happy path B2: execution_mode=for_each → taskValues header, manifest UPDATE,
    no static JSON literals
  - R12 strict-gt B2: no explicit operator → ">" rendered in notebook
  - R12 legacy regression: no execution_mode, no explicit operator → ">=" retained
  - R12 operator override: explicit "<=" survives both modes
  - R12 caveat documentation: Sub-second precision / Late-arriving / UTC
    normalization strings present in rendered output
  - HIPAA hook marker: present in both legacy and B2 renders
  - Raise-on-failure invariant (R4): mark_failed + raise block is byte-identical
    between legacy and B2 renders
"""

from __future__ import annotations

import json
import os
import re
import runpy
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from jinja2 import Environment, FileSystemLoader

# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------

_TEMPLATE_DIR = Path(__file__).parent.parent / "src" / "lhp" / "templates" / "load"
_TEMPLATE_NAME = "jdbc_watermark_job.py.j2"


def _render(
    *,
    execution_mode: Optional[str] = None,
    watermark_type: str = "timestamp",
    watermark_operator: str = ">=",
    source_system_id: str = "pg_prod",
    schema_name: str = "Sales",
    table_name: str = "Orders",
    action_name: str = "load_orders",
    load_group: str = "test_pipeline::test_fg",
    wm_catalog: str = "metadata",
    wm_schema: str = "devtest_orchestration",
    landing_path: str = "/Volumes/bronze/landing/orders",
    watermark_column: str = "ModifiedDate",
    jdbc_url: str = 'dbutils.secrets.get("scope", "url")',
    jdbc_user: str = 'dbutils.secrets.get("scope", "user")',
    jdbc_password: str = 'dbutils.secrets.get("scope", "password")',
    jdbc_driver: str = "org.postgresql.Driver",
    jdbc_table: str = '"Sales"."Orders"',
    pipeline_name: str = "test_pipeline",
    **overrides: Any,
) -> str:
    env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)))
    tmpl = env.get_template(_TEMPLATE_NAME)
    ctx: Dict[str, Any] = {
        "execution_mode": execution_mode,
        "watermark_type": watermark_type,
        "watermark_operator": watermark_operator,
        "source_system_id": source_system_id,
        "schema_name": schema_name,
        "table_name": table_name,
        "action_name": action_name,
        "load_group": load_group,
        "wm_catalog": wm_catalog,
        "wm_schema": wm_schema,
        "landing_path": landing_path,
        "watermark_column": watermark_column,
        "jdbc_url": jdbc_url,
        "jdbc_user": jdbc_user,
        "jdbc_password": jdbc_password,
        "jdbc_driver": jdbc_driver,
        "jdbc_table": jdbc_table,
        "pipeline_name": pipeline_name,
    }
    ctx.update(overrides)
    return tmpl.render(ctx)


# ---------------------------------------------------------------------------
# Mock infrastructure (copied from test_prepare_manifest_template.py harness)
# ---------------------------------------------------------------------------


class _FakeWidgets:
    """Stub for dbutils.widgets — returns JSON-encoded iteration kwargs."""

    def __init__(self, iteration: Dict[str, str]) -> None:
        self._iteration = iteration

    def get(self, key: str) -> str:
        if key == "__lhp_iteration":
            return json.dumps(self._iteration)
        raise KeyError(f"unknown widget key: {key!r}")


class _FakeNotebook:
    """Stub for dbutils.notebook — supports entry_point + exit()."""

    def __init__(self) -> None:
        self.exit_value: Optional[str] = None
        # Wire notebookPath for sys.path bootstrap.
        self.entry_point = MagicMock()
        ctx = self.entry_point.getDbutils().notebook().getContext()
        ctx.notebookPath.return_value.get.return_value = (
            "/Workspace/Users/test@example.com/.bundle/lhp/files/notebooks/worker"
        )

    def exit(self, value: str) -> None:
        # Raise SystemExit so runpy stops cleanly when recovery path fires.
        raise SystemExit(f"notebook.exit: {value}")


class _FakeFs:
    """Stub for dbutils.fs.ls — always returns empty (no parquet landed)."""

    def ls(self, path: str) -> List[Any]:
        return []


class _FakeDbutils:
    """Minimal dbutils stub for the worker notebook.

    Wires the exact accessor chain that derive_run_id reads so it returns a
    well-formed job-N-task-N-attempt-N run_id (not a MagicMock repr).
    """

    def __init__(
        self,
        run_id: str = "job-1-task-2-attempt-0",
        iteration: Optional[Dict[str, str]] = None,
    ) -> None:
        parts = run_id.split("-")
        job_run_id = parts[1] if len(parts) > 1 else "1"
        task_run_id = parts[3] if len(parts) > 3 else "2"
        attempt = parts[5] if len(parts) > 5 else "0"

        self.notebook = _FakeNotebook()
        self.fs = _FakeFs()
        self.widgets = _FakeWidgets(iteration or {})

        ctx = self.notebook.entry_point.getDbutils().notebook().getContext()
        ctx.jobRunId.return_value.get.return_value = job_run_id
        ctx.taskRunId.return_value.get.return_value = task_run_id
        ctx.currentRunAttempt.return_value.get.return_value = attempt

        # secrets stub for the secret-ref pattern used in JDBC options
        self.secrets = MagicMock()
        self.secrets.get.return_value = "fake_secret_value"


class _RecordingSpark:
    """Recording SparkSession with scripted sql() responses and a read stub.

    sql() responses:
      - None in script → return a do-nothing MagicMock
      - Exception instance → raise it
      - any other value → return MagicMock
    """

    def __init__(self, sql_responses: Optional[List[Any]] = None) -> None:
        self.statements: List[str] = []
        self._responses = list(sql_responses or [])
        self.conf = MagicMock()
        self.conf.set = MagicMock()
        self.read = MagicMock()

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        if not self._responses:
            r = MagicMock()
            r.collect.return_value = []
            r.first.return_value = None
            return r
        action = self._responses.pop(0)
        if isinstance(action, BaseException):
            raise action
        r = MagicMock()
        r.collect.return_value = []
        r.first.return_value = None
        return r

    def createDataFrame(self, data: Any, schema: Any = None) -> MagicMock:
        return MagicMock()


def _build_wm_mock() -> MagicMock:
    """Build a WatermarkManager mock that returns no existing watermark."""
    wm = MagicMock()
    wm.get_latest_watermark.return_value = None
    wm.get_recoverable_landed_run.return_value = None
    wm.insert_new.return_value = None
    wm.mark_failed.return_value = None
    wm.mark_landed.return_value = None
    wm.mark_complete.return_value = None
    return wm


def _run_rendered(
    rendered: str,
    spark: _RecordingSpark,
    dbutils: _FakeDbutils,
    wm: Optional[MagicMock] = None,
) -> Dict[str, Any]:
    """Write rendered notebook to a temp file and run via runpy.run_path.

    Patches WatermarkManager constructor to return the provided mock so the
    notebook can run without a real Databricks cluster.
    """
    if wm is None:
        wm = _build_wm_mock()

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".py",
        prefix="lhp_worker_test_",
        delete=False,
    ) as fh:
        fh.write(rendered)
        tmp_path = fh.name
    try:
        with patch(
            "lhp_watermark.WatermarkManager",
            return_value=wm,
        ):
            namespace = runpy.run_path(
                tmp_path,
                init_globals={"spark": spark, "dbutils": dbutils},
            )
    except SystemExit:
        namespace = {}
    finally:
        os.unlink(tmp_path)
    return namespace


def _default_b2_iteration() -> Dict[str, str]:
    return {
        "source_system_id": "pg_prod",
        "schema_name": "Sales",
        "table_name": "Orders",
        "load_group": "test_pipeline::test_fg",
        "batch_id": "job-10-task-20-attempt-0",
        "manifest_table": "metadata.devtest_orchestration.b2_manifests",
        "action_name": "load_orders",
        # Anomaly A — these MUST be passed per-iteration so the shared B2 worker
        # operates on the correct table/landing/watermark column for each spawn.
        "jdbc_table": '"Sales"."Orders"',
        "watermark_column": "ModifiedDate",
        "landing_path": "/Volumes/landing/landing/landing/sales/orders",
    }


# ---------------------------------------------------------------------------
# Tests: happy path — legacy (no execution_mode)
# ---------------------------------------------------------------------------


def test_legacy_contains_static_literals() -> None:
    """Legacy render: contains static source_system_id JSON literal."""
    rendered = _render()
    assert 'SQLInputValidator.string("pg_prod")' in rendered or '"pg_prod"' in rendered, (
        "Legacy render must embed source_system_id JSON literal"
    )


def test_legacy_no_iteration_widget() -> None:
    """Legacy render must NOT contain the B2 widget accessor."""
    rendered = _render()
    assert 'dbutils.widgets.get("__lhp_iteration")' not in rendered, (
        "Legacy render must not reference __lhp_iteration widget"
    )


def test_legacy_no_manifest_update_block() -> None:
    """Legacy render must NOT contain the manifest UPDATE block."""
    rendered = _render()
    assert "worker_run_id" not in rendered, (
        "Legacy render must not contain manifest UPDATE / worker_run_id"
    )


def test_legacy_gte_watermark_predicate() -> None:
    """Legacy render with default operator: op-assignment contains '>=' (tojson-escaped or literal)."""
    rendered = _render(watermark_operator=">=")
    # tojson encodes '>' as '>'; accept either representation.
    assert ('op = "\\u003e="' in rendered or 'op = ">="' in rendered), (
        "Legacy render with default operator must have op = '>=' assignment (tojson form or literal)"
    )


# ---------------------------------------------------------------------------
# Tests: happy path — B2 for_each
# ---------------------------------------------------------------------------


def test_b2_contains_iteration_widget() -> None:
    """B2 render must contain the __lhp_iteration widget accessor."""
    rendered = _render(execution_mode="for_each")
    assert 'dbutils.widgets.get("__lhp_iteration")' in rendered, (
        "B2 render must unpack __lhp_iteration from dbutils.widgets"
    )


def test_b2_contains_iteration_key_access() -> None:
    """B2 render must access iteration['source_system_id']."""
    rendered = _render(execution_mode="for_each")
    assert 'iteration["source_system_id"]' in rendered, (
        "B2 render must read source_system_id from iteration dict"
    )


def test_b2_contains_manifest_update() -> None:
    """B2 render must contain the manifest UPDATE block."""
    rendered = _render(execution_mode="for_each")
    assert "UPDATE" in rendered and "worker_run_id" in rendered, (
        "B2 render must contain manifest UPDATE block with worker_run_id"
    )


def test_b2_contains_optimistic_concurrency_guard() -> None:
    """B2 render must contain the NULL-or-self worker_run_id WHERE clause."""
    rendered = _render(execution_mode="for_each")
    assert "worker_run_id IS NULL OR worker_run_id" in rendered, (
        "B2 render must contain optimistic-concurrency WHERE guard"
    )


def test_b2_no_static_source_system_id_json_literal() -> None:
    """B2 render must NOT contain the hardcoded JSON literal for source_system_id."""
    rendered = _render(execution_mode="for_each", source_system_id="pg_prod")
    # The static literal form is: SQLInputValidator.string("pg_prod")
    assert 'SQLInputValidator.string("pg_prod")' not in rendered, (
        "B2 render must not embed static source_system_id literal"
    )


# ---------------------------------------------------------------------------
# Anomaly A regression — B2 worker must source per-action attrs from iteration
# kwargs, not from action[0]'s codegen-time literal. Three fields:
#   jdbc_table, watermark_column, landing_path
# ---------------------------------------------------------------------------


def test_b2_jdbc_table_read_from_iteration_not_literal() -> None:
    """Anomaly A: jdbc_table must be sourced from iteration[...] in B2 mode."""
    rendered = _render(
        execution_mode="for_each",
        jdbc_table='"Sales"."Orders"',  # render-time literal that must NOT leak
    )
    assert 'iteration["jdbc_table"]' in rendered, (
        "B2 render must read jdbc_table from iteration kwargs"
    )
    assert 'SQLInputValidator.string("\\"Sales\\".\\"Orders\\"")' not in rendered, (
        "B2 render must NOT embed action[0]'s jdbc_table as a static literal"
    )


def test_b2_watermark_column_read_from_iteration_not_literal() -> None:
    """Anomaly A: watermark_column must come from iteration in B2 mode."""
    rendered = _render(
        execution_mode="for_each",
        watermark_column="ModifiedDate",
    )
    assert 'iteration["watermark_column"]' in rendered, (
        "B2 render must read watermark_column from iteration kwargs"
    )
    assert 'SQLInputValidator.string("ModifiedDate")' not in rendered, (
        "B2 render must NOT embed action[0]'s watermark_column as a static literal"
    )


def test_b2_landing_path_read_from_iteration_not_literal() -> None:
    """Anomaly A: landing_root must come from iteration in B2 mode."""
    leaked = "/Volumes/landing/landing/landing/sales/orders"
    rendered = _render(execution_mode="for_each", landing_path=leaked)
    assert 'iteration["landing_path"]' in rendered, (
        "B2 render must read landing_path from iteration kwargs"
    )
    # Permit the path to appear in template comments only — but not as a Python
    # literal landing_root assignment. The cheap, durable check is the
    # assignment form rendered by the legacy branch.
    assert f'landing_root = "{leaked}"' not in rendered, (
        "B2 render must NOT assign landing_root from action[0]'s static literal"
    )


def test_legacy_jdbc_table_still_static_literal() -> None:
    """Legacy mode keeps the per-action static literal — no regression."""
    rendered = _render(execution_mode=None, jdbc_table='"Sales"."Orders"')
    assert 'iteration["jdbc_table"]' not in rendered, (
        "Legacy render must not introduce iteration kwargs lookups"
    )
    assert '"Sales"."Orders"' in rendered or '\\"Sales\\".\\"Orders\\"' in rendered, (
        "Legacy render must embed jdbc_table as a Python literal"
    )


# ---------------------------------------------------------------------------
# Anomaly B regression — worker must transition manifest execution_status to
# 'completed' on the success path and 'failed' on the except path so operators
# (and downstream tooling) can read the manifest as an authoritative log.
# ---------------------------------------------------------------------------


def test_b2_worker_updates_manifest_to_completed_on_success() -> None:
    """B2 success path: worker must emit UPDATE … SET execution_status = 'completed'."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    assert "execution_status = 'completed'" in rendered, (
        "B2 worker must UPDATE manifest execution_status to 'completed' "
        "after mark_complete on the success path"
    )


def test_b2_worker_updates_manifest_to_failed_on_except() -> None:
    """B2 except path: worker must emit UPDATE … SET execution_status = 'failed'."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    assert "execution_status = 'failed'" in rendered, (
        "B2 worker must UPDATE manifest execution_status to 'failed' before "
        "re-raising in the except branch"
    )


def test_b2_runtime_emits_completed_update_on_success(monkeypatch: Any) -> None:
    """Runtime: spark.sql receives an UPDATE … 'completed' statement on happy path.

    Drives the rendered notebook through the recovery branch (parquet absent)
    is too narrow; instead, mock _landing_has_parquet to True and stub the
    JDBC read to return an empty schema. We assert the SQL stream contains the
    completion UPDATE — this is the only signal the operator + validate task
    has that the worker reached terminal-success state.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    spark = _RecordingSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    # We only care about SQL recording; if the rendered notebook attempts a
    # real JDBC read, the test will fail with a recognisable error before
    # the assertion — that's acceptable failure-first behaviour.
    try:
        _run_rendered(rendered, spark, dbutils)
    except Exception:
        # Either the bug is still present (no completion UPDATE issued) or
        # JDBC read fails — the assertion below disambiguates.
        pass

    completed_updates = [
        s for s in spark.statements
        if "UPDATE" in s.upper() and "execution_status = 'completed'" in s
    ]
    assert len(completed_updates) >= 1, (
        f"B2 worker must issue an UPDATE … 'completed' SQL statement on "
        f"success; recorded SQL was:\n{spark.statements}"
    )


# ---------------------------------------------------------------------------
# Tests: R12 operator defaults
# ---------------------------------------------------------------------------


def test_r12_b2_default_operator_is_strict_gt() -> None:
    """R12 B2 path with default '>=' operator flips to '>' in rendered notebook."""
    # Template context: execution_mode=for_each, operator=">" (B2 default from generator)
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    # tojson encodes '>' as '>'; accept either representation.
    has_strict_gt = ('op = "\\u003e"' in rendered or 'op = ">"' in rendered)
    has_gte = ('op = "\\u003e="' in rendered or 'op = ">="' in rendered)
    assert has_strict_gt, (
        "B2 render must assign op = '>' (strict) on the op-assignment line"
    )
    assert not has_gte, (
        "B2 render must NOT assign op = '>=' on the op-assignment line"
    )


def test_r12_legacy_default_operator_is_gte() -> None:
    """R12 legacy path keeps '>=' when no explicit override."""
    rendered = _render(execution_mode=None, watermark_operator=">=")
    assert ('op = "\\u003e="' in rendered or 'op = ">="' in rendered), (
        "Legacy render must retain '>=' on the op-assignment line (tojson form or literal)"
    )


def test_r12_operator_override_explicit_value_wins() -> None:
    """R12 explicit operator '>' survives in legacy mode on the op-assignment line."""
    rendered = _render(execution_mode=None, watermark_operator=">")
    # tojson encodes '>' as '>'; accept either representation.
    has_strict_gt = ('op = "\\u003e"' in rendered or 'op = ">"' in rendered)
    has_gte = ('op = "\\u003e="' in rendered or 'op = ">="' in rendered)
    assert has_strict_gt, (
        "Explicit '>' override must appear on op-assignment line in legacy render"
    )
    assert not has_gte, (
        "Explicit '>' override must suppress '>=' on op-assignment line in legacy render"
    )


def test_r12_generator_b2_default_flips_operator() -> None:
    """R12 generator: _build_watermark_operator returns '>' for B2 flowgroup with default watermark."""
    from lhp.generators.load.jdbc_watermark_job import _build_watermark_operator
    from lhp.models.pipeline_config import WatermarkConfig, WatermarkType

    # Simulate a watermark with Pydantic default operator ">="
    wm = WatermarkConfig(column="ModifiedDate", type=WatermarkType.TIMESTAMP)
    assert wm.operator == ">=", "Pydantic default must be '>=' (precondition)"

    # Flowgroup mock with for_each execution_mode
    fg = MagicMock()
    fg.workflow = {"execution_mode": "for_each"}

    result = _build_watermark_operator(wm, fg)
    assert result == ">", (
        f"_build_watermark_operator must return '>' for B2 + default watermark; got {result!r}"
    )


def test_r12_generator_legacy_keeps_gte() -> None:
    """R12 generator: _build_watermark_operator returns '>=' for legacy (no execution_mode)."""
    from lhp.generators.load.jdbc_watermark_job import _build_watermark_operator
    from lhp.models.pipeline_config import WatermarkConfig, WatermarkType

    wm = WatermarkConfig(column="col", type=WatermarkType.NUMERIC)
    fg = MagicMock()
    fg.workflow = None

    result = _build_watermark_operator(wm, fg)
    assert result == ">=", (
        f"Legacy flowgroup must keep '>=' default; got {result!r}"
    )


def test_r12_generator_explicit_gt_override_wins_in_both_modes() -> None:
    """R12 explicit operator '>' in WatermarkConfig survives in both modes."""
    from lhp.generators.load.jdbc_watermark_job import _build_watermark_operator
    from lhp.models.pipeline_config import WatermarkConfig, WatermarkType

    wm = WatermarkConfig(column="col", type=WatermarkType.TIMESTAMP, operator=">")

    fg_b2 = MagicMock()
    fg_b2.workflow = {"execution_mode": "for_each"}

    fg_legacy = MagicMock()
    fg_legacy.workflow = None

    assert _build_watermark_operator(wm, fg_b2) == ">", "Explicit '>' must survive in B2 mode"
    assert _build_watermark_operator(wm, fg_legacy) == ">", "Explicit '>' must survive in legacy mode"


# ---------------------------------------------------------------------------
# Tests: R12 caveat documentation in rendered output
# ---------------------------------------------------------------------------


def test_r12_caveat_sub_second_precision_present() -> None:
    """Rendered notebook (both modes) contains 'Sub-second precision' caveat."""
    for execution_mode in (None, "for_each"):
        rendered = _render(
            execution_mode=execution_mode,
            watermark_operator=">" if execution_mode == "for_each" else ">=",
        )
        assert "Sub-second precision" in rendered, (
            f"R12 caveat 'Sub-second precision' missing from render with "
            f"execution_mode={execution_mode!r}"
        )


def test_r12_caveat_late_arriving_present() -> None:
    """Rendered notebook contains 'Late-arriving' caveat."""
    for execution_mode in (None, "for_each"):
        rendered = _render(
            execution_mode=execution_mode,
            watermark_operator=">" if execution_mode == "for_each" else ">=",
        )
        assert "Late-arriving" in rendered, (
            f"R12 caveat 'Late-arriving' missing from render with "
            f"execution_mode={execution_mode!r}"
        )


def test_r12_caveat_utc_normalization_present() -> None:
    """Rendered notebook contains 'UTC normalization' caveat."""
    for execution_mode in (None, "for_each"):
        rendered = _render(
            execution_mode=execution_mode,
            watermark_operator=">" if execution_mode == "for_each" else ">=",
        )
        assert "UTC normalization" in rendered, (
            f"R12 caveat 'UTC normalization' missing from render with "
            f"execution_mode={execution_mode!r}"
        )


# ---------------------------------------------------------------------------
# Tests: HIPAA hook marker
# ---------------------------------------------------------------------------


def test_hipaa_hook_marker_legacy() -> None:
    """Legacy render must contain the HIPAA hook insertion point comment."""
    rendered = _render()
    assert "HIPAA hook insertion point" in rendered, (
        "Legacy render must contain 'HIPAA hook insertion point' comment"
    )


def test_hipaa_hook_marker_b2() -> None:
    """B2 render must contain the HIPAA hook insertion point comment."""
    rendered = _render(execution_mode="for_each")
    assert "HIPAA hook insertion point" in rendered, (
        "B2 render must contain 'HIPAA hook insertion point' comment"
    )


# ---------------------------------------------------------------------------
# Tests: raise-on-failure invariant (R4)
# ---------------------------------------------------------------------------


def _extract_raise_on_failure_block(rendered: str) -> str:
    """Extract the mark_failed + raise slice from the except block.

    Scans for the ``except Exception as e:`` line, then finds the first
    ``wm.mark_failed(`` call within that except block, and returns the
    text from there through the bare ``raise`` that terminates the block.
    """
    lines = rendered.splitlines()
    in_except = False
    start = None
    end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("except Exception as e:"):
            in_except = True
            continue
        if in_except and "wm.mark_failed(" in line and start is None:
            start = i
        if start is not None and stripped == "raise":
            end = i
            break
        # If we encounter a new top-level block after the except, stop searching.
        if in_except and start is None and stripped and not stripped.startswith("#") and not line.startswith(" ") and not line.startswith("\t"):
            in_except = False
    if start is None or end is None:
        raise AssertionError(
            f"Could not locate wm.mark_failed...raise block in except clause:\n{rendered[:800]}"
        )
    return "\n".join(line.rstrip() for line in lines[start : end + 1])


def test_r4_raise_on_failure_block_verbatim_between_modes() -> None:
    """R4: mark_failed + raise block is byte-identical in legacy and B2 renders."""
    legacy_rendered = _render(execution_mode=None, watermark_operator=">=")
    b2_rendered = _render(execution_mode="for_each", watermark_operator=">")

    legacy_block = _extract_raise_on_failure_block(legacy_rendered)
    b2_block = _extract_raise_on_failure_block(b2_rendered)

    assert legacy_block == b2_block, (
        "R4 violated: mark_failed+raise block differs between legacy and B2 renders.\n"
        f"Legacy block:\n{legacy_block}\n\nB2 block:\n{b2_block}"
    )


# ---------------------------------------------------------------------------
# Tests: runtime execution — B2 happy path
# ---------------------------------------------------------------------------


def test_b2_runtime_manifest_update_sql_issued() -> None:
    """B2 runtime: spark.sql() receives an UPDATE statement for manifest claim."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    spark = _RecordingSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    _run_rendered(rendered, spark, dbutils)

    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    assert len(update_stmts) >= 1, (
        f"B2 runtime must issue at least one UPDATE SQL; statements were:\n{spark.statements}"
    )
    update_sql = update_stmts[0]
    assert "worker_run_id" in update_sql, (
        f"UPDATE statement must set worker_run_id; got:\n{update_sql}"
    )


def test_b2_runtime_readback_sql_issued() -> None:
    """B2 runtime: spark.sql() receives a SELECT worker_run_id readback query."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    spark = _RecordingSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    _run_rendered(rendered, spark, dbutils)

    select_stmts = [
        s for s in spark.statements
        if "SELECT" in s.upper() and "worker_run_id" in s
    ]
    assert len(select_stmts) >= 1, (
        f"B2 runtime must issue SELECT worker_run_id readback; statements were:\n{spark.statements}"
    )


def test_b2_runtime_competing_owner_raises() -> None:
    """B2 runtime: if readback shows a different owner, RuntimeError with LHP-MAN-002 is raised."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")

    # Script: first sql() is the UPDATE (returns mock), second is the readback
    # which returns a row with a DIFFERENT worker_run_id.
    competing_run_id = "job-99-task-99-attempt-0"
    readback_row = MagicMock()
    readback_row.__getitem__ = lambda self, key: competing_run_id if key == "worker_run_id" else None

    class _ScriptedSpark(_RecordingSpark):
        def __init__(self) -> None:
            super().__init__()
            self._call_index = 0

        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            self._call_index += 1
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                # Return row indicating a competing worker owns this row
                r.collect.return_value = [readback_row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    spark = _ScriptedSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    with pytest.raises(RuntimeError, match="LHP-MAN-002"):
        _run_rendered(rendered, spark, dbutils)
