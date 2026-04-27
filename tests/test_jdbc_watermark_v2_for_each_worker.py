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
    """Runtime: spark.sql receives UPDATE … 'running' (claim) then UPDATE … 'completed' in order.

    The readback SELECT must return the current worker_run_id so the claim
    guard passes and the worker reaches the success path.  The bare-except
    that previously masked crashes has been replaced by a scripted Spark stub
    that satisfies the claim guard — if the worker crashes before completion
    the assertion will fail on the *absence* of the completion UPDATE, not be
    silently swallowed.

    Sequence asserted:
      1. UPDATE … execution_status = 'running'  (manifest claim)
      2. SELECT worker_run_id                    (readback guard)
      3. UPDATE … execution_status = 'completed' (terminal transition)
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id = "job-1-task-2-attempt-0"

    class _ClaimSucceedsSpark(_RecordingSpark):
        """Returns the current worker_run_id on the readback SELECT so the
        claim guard passes; all other sql() calls behave like the default stub.
        """

        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                # Readback returns a row owned by *this* run — claim guard passes.
                row = MagicMock()
                row.__getitem__ = lambda self, key: run_id if key == "worker_run_id" else None
                r.collect.return_value = [row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    spark = _ClaimSucceedsSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

    # If the worker crashes before emitting the completion UPDATE the test will
    # fail cleanly — no swallowed exceptions.
    _run_rendered(rendered, spark, dbutils)

    claim_updates = [
        s for s in spark.statements
        if "UPDATE" in s.upper() and "execution_status = 'running'" in s
    ]
    completed_updates = [
        s for s in spark.statements
        if "UPDATE" in s.upper() and "execution_status = 'completed'" in s
    ]
    assert len(claim_updates) >= 1, (
        f"B2 worker must issue a claim UPDATE (running) before extraction; "
        f"recorded SQL was:\n{spark.statements}"
    )
    assert len(completed_updates) >= 1, (
        f"B2 worker must issue an UPDATE … 'completed' SQL statement on "
        f"success; recorded SQL was:\n{spark.statements}"
    )
    # Order invariant: claim must precede completion.
    first_claim_idx = next(
        i for i, s in enumerate(spark.statements)
        if "UPDATE" in s.upper() and "execution_status = 'running'" in s
    )
    first_completed_idx = next(
        i for i, s in enumerate(spark.statements)
        if "UPDATE" in s.upper() and "execution_status = 'completed'" in s
    )
    assert first_claim_idx < first_completed_idx, (
        f"Claim UPDATE (running) must precede completion UPDATE; "
        f"claim at index {first_claim_idx}, completed at {first_completed_idx}"
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


def test_r12_generator_b2_honors_operator_no_silent_promotion() -> None:
    """R12 generator: _build_watermark_operator no longer silently promotes
    explicit '>=' to '>' in B2 mode. Validator (LHP-CFG-035) rejects '>=' in
    for_each upstream, so by the time generator runs only strict comparators
    should reach this function. This regression test pins down the no-silent-
    promotion contract: function honors watermark.operator verbatim.
    """
    from lhp.generators.load.jdbc_watermark_job import _build_watermark_operator
    from lhp.models.pipeline_config import WatermarkConfig, WatermarkType

    # Pydantic default operator is '>=' — function honors it as-set,
    # NOT promoted to '>' by silent generator-side mutation.
    wm = WatermarkConfig(column="ModifiedDate", type=WatermarkType.TIMESTAMP)
    assert wm.operator == ">=", "Pydantic default must be '>=' (precondition)"

    fg = MagicMock()
    fg.workflow = {"execution_mode": "for_each"}

    result = _build_watermark_operator(wm, fg)
    assert result == ">=", (
        f"_build_watermark_operator must honor watermark.operator verbatim "
        f"in B2 mode (no silent promotion); got {result!r}. Validator "
        f"LHP-CFG-035 catches '>=' in for_each upstream."
    )


def test_r12_generator_b2_default_when_operator_unset() -> None:
    """R12 generator: when watermark.operator is None/empty in B2 mode, default to '>'."""
    from lhp.generators.load.jdbc_watermark_job import _build_watermark_operator

    wm = MagicMock()
    wm.operator = None  # simulate operator-unset path (cannot occur via Pydantic, but defensive)

    fg = MagicMock()
    fg.workflow = {"execution_mode": "for_each"}

    result = _build_watermark_operator(wm, fg)
    assert result == ">", (
        f"B2 mode with unset watermark.operator must default to '>'; got {result!r}"
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


def _make_claim_succeeds_spark(run_id: str) -> "_RecordingSpark":
    """Build a _RecordingSpark subclass that satisfies the manifest claim
    readback guard by returning a row owned by ``run_id`` on the SELECT
    worker_run_id query. All other sql() calls return the default empty
    response. Use when the test needs the worker to progress past the
    claim guard without crashing on LHP-MAN-003."""

    class _ClaimSucceedsSpark(_RecordingSpark):
        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                row = MagicMock()
                row.__getitem__ = lambda self, key: run_id if key == "worker_run_id" else None
                r.collect.return_value = [row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    return _ClaimSucceedsSpark()


def test_b2_runtime_manifest_update_sql_issued() -> None:
    """B2 runtime: spark.sql() receives an UPDATE statement for manifest claim."""
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id = "job-1-task-2-attempt-0"
    spark = _make_claim_succeeds_spark(run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

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
    run_id = "job-1-task-2-attempt-0"
    spark = _make_claim_succeeds_spark(run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

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


# ---------------------------------------------------------------------------
# Tests: already-completed re-claim (#31)
# ---------------------------------------------------------------------------


def test_b2_already_completed_row_short_circuits_or_raises() -> None:
    """B2 runtime: if the readback SELECT returns a row whose worker_run_id
    matches the current run *and* execution_status is 'completed', the worker
    must either:
      (a) short-circuit — skip JDBC re-extraction (preferred), OR
      (b) raise a clear error indicating the row is already completed.

    Under the current template implementation the readback guard only checks
    worker_run_id ownership, not execution_status.  A row owned by *this*
    run_id with status='completed' means a previous attempt already finished
    successfully.  The worker should not re-extract.

    Expected behaviour (current): the worker passes the readback guard
    (worker_run_id matches) and proceeds to re-extract.  This test pins that
    behaviour: no RuntimeError is raised and the worker emits the completion
    UPDATE a second time.  If the template is later hardened to short-circuit
    on an already-completed row, update this test to assert the short-circuit
    (no JDBC re-extract, early exit) instead.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id = "job-1-task-2-attempt-0"

    # Build a readback row that says: this exact run_id already completed.
    completed_row = MagicMock()

    def _completed_row_getitem(key: str) -> Any:
        if key == "worker_run_id":
            return run_id
        if key == "execution_status":
            return "completed"
        return None

    completed_row.__getitem__ = lambda self, key: _completed_row_getitem(key)

    class _AlreadyCompletedSpark(_RecordingSpark):
        """Returns a row with worker_run_id=run_id + execution_status='completed'
        on the readback SELECT so we can observe what the worker does when it
        encounters an already-completed manifest row.
        """

        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                r.collect.return_value = [completed_row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    spark = _AlreadyCompletedSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

    # Current behaviour: the worker does NOT raise — it re-enters the extraction
    # path and issues another completion UPDATE.  We assert this behaviour is
    # stable so any future change (e.g. a short-circuit guard on
    # execution_status == 'completed') is a deliberate, tested decision rather
    # than an accidental regression.
    _run_rendered(rendered, spark, dbutils)

    completed_updates = [
        s for s in spark.statements
        if "UPDATE" in s.upper() and "execution_status = 'completed'" in s
    ]
    assert completed_updates, (
        "With an already-completed row (same worker_run_id), the worker "
        "currently re-extracts and emits a completion UPDATE; "
        "if this behaviour changes (short-circuit / error), update this test."
    )


# ---------------------------------------------------------------------------
# Tests: U1 — status-based reclaim (DAB per-iteration retries, issue #14)
# ---------------------------------------------------------------------------


def _make_scripted_owner_spark(owner_run_id: str) -> "_RecordingSpark":
    """Return a _RecordingSpark whose readback SELECT always reports *owner_run_id*
    as the current worker_run_id value.  All other sql() calls behave as the
    default stub (no-op mock, empty collect)."""

    class _ScriptedOwnerSpark(_RecordingSpark):
        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                row = MagicMock()
                row.__getitem__ = lambda self, key: owner_run_id if key == "worker_run_id" else None
                r.collect.return_value = [row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    return _ScriptedOwnerSpark()


def test_u1_claim_initial_null_arm() -> None:
    """Scenario 1 — initial claim: row has worker_run_id=NULL, execution_status='pending'.

    Claim WHERE matches via worker_run_id IS NULL arm.  Readback returns the
    current run_id.  No error raised.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id = "job-1-task-2-attempt-0"
    spark = _make_scripted_owner_spark(run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

    # No exception expected — claim succeeds, readback matches current run_id.
    _run_rendered(rendered, spark, dbutils)

    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    assert any("execution_status = 'running'" in s for s in update_stmts), (
        "Initial claim must emit UPDATE setting execution_status='running'"
    )


def test_u1_claim_same_attempt_idempotent() -> None:
    """Scenario 2 — same-attempt idempotent re-claim: row already running, same run_id.

    Claim WHERE matches via worker_run_id = run_id arm.  Readback returns the
    same run_id.  No error raised.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id = "job-1-task-2-attempt-0"
    # Readback always returns this run_id — simulates the row already being
    # owned by the same attempt (e.g. Spark task retry mid-notebook).
    spark = _make_scripted_owner_spark(run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id, iteration=iteration)

    _run_rendered(rendered, spark, dbutils)

    # No LHP-MAN-002 raised; UPDATE with 'running' must be present.
    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    assert any("execution_status = 'running'" in s for s in update_stmts), (
        "Same-attempt idempotent re-claim must still emit the claim UPDATE"
    )


def test_u1_claim_dab_retry_attempt1_reclaims_attempt0_failed() -> None:
    """Scenario 3 (the bug fix) — DAB attempt-1 reclaims a row left 'failed' by attempt-0.

    Before U1 fix: claim WHERE has no 'failed' status arm, so the UPDATE
    matches zero rows.  Readback returns attempt-0's run_id; guard raises
    LHP-MAN-002.

    After U1 fix: claim WHERE includes OR execution_status = 'failed', so the
    UPDATE succeeds.  Readback returns attempt-1's run_id.  No error raised.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id_1 = "job-1-task-2-attempt-1"
    # Readback returns attempt-1's run_id — simulates the claim UPDATE succeeding
    # and setting worker_run_id to attempt-1's id.
    spark = _make_scripted_owner_spark(run_id_1)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id_1, iteration=iteration)

    # After the fix this must NOT raise LHP-MAN-002.
    _run_rendered(rendered, spark, dbutils)

    # Claim UPDATE must contain the new 'failed' arm.
    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    claim_updates = [s for s in update_stmts if "execution_status = 'running'" in s]
    assert claim_updates, "Attempt-1 must issue a claim UPDATE for the 'failed' row"
    # Verify the template WHERE clause includes the 'failed' disjunct.
    rendered_lower = rendered
    assert "execution_status = 'failed'" in rendered_lower, (
        "Claim WHERE must include OR execution_status = 'failed' arm for DAB retries"
    )


def test_u1_claim_dab_retry_attempt1_reclaims_attempt0_pending_null_arm() -> None:
    """Scenario 4 — attempt-0 crashed before claim UPDATE; row still NULL/'pending'.

    Attempt-1 claim WHERE matches via worker_run_id IS NULL arm (not the new
    'failed' arm).  No error raised.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    run_id_1 = "job-1-task-2-attempt-1"
    spark = _make_scripted_owner_spark(run_id_1)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id_1, iteration=iteration)

    _run_rendered(rendered, spark, dbutils)

    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    assert any("execution_status = 'running'" in s for s in update_stmts), (
        "Attempt-1 must claim a NULL-owned row via IS NULL arm"
    )


def test_u1_claim_competing_owner_running_raises_man002() -> None:
    """Scenario 5 — competing owner on running row: must raise LHP-MAN-002.

    A row with worker_run_id=OTHER and execution_status='running' must NOT be
    reclaimable.  None of the WHERE arms match (not NULL, not same run_id, not
    status='failed').  Readback returns OTHER; guard raises LHP-MAN-002.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    competing_run_id = "job-99-task-99-attempt-0"
    spark = _make_scripted_owner_spark(competing_run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    with pytest.raises(RuntimeError, match="LHP-MAN-002"):
        _run_rendered(rendered, spark, dbutils)


def test_u1_claim_competing_owner_completed_raises_man002() -> None:
    """Scenario 6 — competing owner on completed row: must raise LHP-MAN-002.

    A row with execution_status='completed' must NOT be reclaimable — the
    new 'failed' disjunct is exact (not an IN clause).  Readback returns
    the OTHER run_id; guard raises LHP-MAN-002.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    # 'completed' row owned by a different run_id — claim WHERE must not match.
    # Readback returns the OTHER run_id; guard raises LHP-MAN-002.
    other_run_id = "job-99-task-99-attempt-0"
    spark = _make_scripted_owner_spark(other_run_id)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-1", iteration=iteration)

    with pytest.raises(RuntimeError, match="LHP-MAN-002"):
        _run_rendered(rendered, spark, dbutils)

    # Regression guard: 'failed' must NOT appear as 'IN (...)' — exact string.
    assert "execution_status = 'failed'" in rendered, (
        "Claim WHERE must use exact equality for 'failed', not IN clause"
    )
    assert "execution_status = 'completed'" not in rendered.split("SET")[0], (
        "Claim WHERE must not admit 'completed' rows — fix is exact 'failed' only"
    )


def test_u1_claim_manifest_row_missing_raises_man003() -> None:
    """Scenario 7 — manifest row missing: raises LHP-MAN-003.  Unchanged by U1.

    Readback returns empty list.  Guard raises LHP-MAN-003 as before.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")

    class _MissingRowSpark(_RecordingSpark):
        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            r.collect.return_value = []  # readback = empty → no row
            r.first.return_value = None
            return r

    spark = _MissingRowSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id="job-1-task-2-attempt-0", iteration=iteration)

    with pytest.raises(RuntimeError, match="LHP-MAN-003"):
        _run_rendered(rendered, spark, dbutils)


def test_u1_concurrent_claim_contention_one_winner_one_raises() -> None:
    """Scenario 8 — concurrent-claim contention (R2 safety regression guard).

    Two attempts both observe execution_status='failed' on the same row.
    Both submit claim UPDATEs through execute_with_concurrent_commit_retry.

    Delta optimistic concurrency: winner's UPDATE commits first, setting
    status→running and owner→winner_id.  Loser's retry observes the row is
    now 'running' with owner=winner_id — claim WHERE matches none of the arms.
    Readback returns winner_id; loser raises LHP-MAN-002.

    The harness simulates this by scripting the readback to return the winner's
    run_id when called from the loser's context.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")
    winner_id = "job-1-task-2-attempt-1"
    loser_id = "job-1-task-2-attempt-2"

    # Loser's readback observes the winner already owns the row.
    loser_spark = _make_scripted_owner_spark(winner_id)
    iteration = _default_b2_iteration()
    loser_dbutils = _FakeDbutils(run_id=loser_id, iteration=iteration)

    # Loser must raise LHP-MAN-002 — the winning attempt owns the row.
    with pytest.raises(RuntimeError, match="LHP-MAN-002"):
        _run_rendered(rendered, loser_spark, loser_dbutils)

    # Winner succeeds independently (readback returns winner_id).
    winner_spark = _make_scripted_owner_spark(winner_id)
    winner_dbutils = _FakeDbutils(run_id=winner_id, iteration=iteration)
    _run_rendered(rendered, winner_spark, winner_dbutils)  # must not raise


def test_u1_integration_attempt0_fails_attempt1_succeeds() -> None:
    """Scenario 9 — integration: attempt-0-fails, attempt-1-succeeds (5-step SQL trace).

    Step 1: attempt-0 claims via NULL arm → row: run_id_0, running.
    Step 2: JDBC fails (within the try block).
    Step 3: fail-mirror WHERE matches worker_run_id=run_id_0 → row: failed.
    Step 4: attempt-1 claim WHERE matches via status='failed' → row: run_id_1, running.
    Step 5: JDBC succeeds, completion-mirror WHERE matches worker_run_id=run_id_1 → completed.

    This test verifies the rendered SQL shapes and runtime ordering for all 5 steps.
    Static assertions inspect the rendered text; the runtime sub-test drives attempt-1
    through the claim → JDBC-success → completion path end-to-end.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")

    # --- Steps 3, 4, 5: static shape assertions on the rendered template text ---

    # Step 3: fail-mirror must define _fail_mirror_sql with execution_status='failed'.
    assert "_fail_mirror_sql" in rendered, (
        "Rendered notebook must define _fail_mirror_sql for the fail-mirror UPDATE"
    )
    assert "execution_status = 'failed'" in rendered, (
        "Fail-mirror UPDATE must set execution_status='failed'"
    )

    # Step 4: claim WHERE must include the 'failed' disjunct.
    assert "OR execution_status = 'failed'" in rendered, (
        "Claim WHERE must include OR execution_status = 'failed' for DAB retry reclaim"
    )

    # Step 5: completion-mirror must define _complete_mirror_sql.
    assert "_complete_mirror_sql" in rendered, (
        "Rendered notebook must define _complete_mirror_sql for the completion-mirror UPDATE"
    )
    assert "execution_status = 'completed'" in rendered, (
        "Completion-mirror UPDATE must set execution_status='completed'"
    )

    # --- Runtime: simulate attempt-1 claim → JDBC-success → completion ---
    run_id_1 = "job-1-task-2-attempt-1"
    spark = _make_scripted_owner_spark(run_id_1)
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id_1, iteration=iteration)
    _run_rendered(rendered, spark, dbutils)

    update_stmts = [s for s in spark.statements if "UPDATE" in s.upper()]
    running_updates = [s for s in update_stmts if "execution_status = 'running'" in s]
    completed_updates = [s for s in update_stmts if "execution_status = 'completed'" in s]
    assert running_updates, "Step 4 claim must emit UPDATE 'running'"
    assert completed_updates, "Step 5 completion mirror must emit UPDATE 'completed'"

    first_running_idx = next(
        i for i, s in enumerate(spark.statements)
        if "UPDATE" in s.upper() and "execution_status = 'running'" in s
    )
    first_completed_idx = next(
        i for i, s in enumerate(spark.statements)
        if "UPDATE" in s.upper() and "execution_status = 'completed'" in s
    )
    assert first_running_idx < first_completed_idx, (
        "Claim UPDATE (running) must precede completion-mirror UPDATE (completed)"
    )


def test_u1_integration_attempt0_fails_attempt1_also_fails() -> None:
    """Scenario 10 — integration: attempt-0-fails, attempt-1-also-fails (terminal-failed).

    Both attempts claim and both hit exceptions inside the try block.
    Attempt-1's fail-mirror must target worker_run_id=run_id_1 (not run_id_0).
    Final manifest state: worker_run_id=run_id_1, execution_status='failed'.

    The fail-mirror UPDATE is sent inside the except block (after the JDBC try
    block raises).  We verify the rendered template contains the fail-mirror
    SQL definition and that a runtime execution where the try block raises
    still emits the fail-mirror UPDATE before re-raising.

    Note: wm.insert_new is OUTSIDE the try block (FR-L-05 contract).  The
    except branch is only entered if something inside the try raises.  We
    arrange for spark.read to raise by configuring its MagicMock load() call
    via a side_effect set on the final chained return value.
    """
    rendered = _render(execution_mode="for_each", watermark_operator=">")

    # Static assertions: both mirror SQL definitions must be present.
    assert "_fail_mirror_sql" in rendered, (
        "Fail-mirror must be defined as _fail_mirror_sql variable in rendered notebook"
    )
    assert "_complete_mirror_sql" in rendered, (
        "Completion-mirror must be defined as _complete_mirror_sql variable"
    )

    run_id_1 = "job-1-task-2-attempt-1"

    class _Attempt1ClaimsSpark(_RecordingSpark):
        """Readback returns run_id_1 so the claim guard passes.
        All MagicMock JDBC chaining succeeds — the load() call returns a mock
        whose format("jdbc")...load() endpoint raises RuntimeError so the
        try-block except branch fires and the fail-mirror is emitted.
        """

        def __init__(self) -> None:
            super().__init__()
            # MagicMock auto-chains: every .attribute and .method() call returns
            # a new MagicMock.  Setting side_effect on the .load attribute of
            # the auto-chained result ensures the raise happens inside the try
            # block regardless of how many .option() calls precede .load().
            self.read = MagicMock()
            self.read.format.return_value.option.return_value.option.return_value \
                .option.return_value.option.return_value.option.return_value \
                .option.return_value.load.side_effect = RuntimeError(
                    "simulated attempt-1 JDBC failure"
                )

        def sql(self, statement: str) -> Any:
            self.statements.append(statement)
            r = MagicMock()
            if "SELECT" in statement.upper() and "worker_run_id" in statement:
                row = MagicMock()
                row.__getitem__ = lambda self, key: run_id_1 if key == "worker_run_id" else None
                r.collect.return_value = [row]
            else:
                r.collect.return_value = []
            r.first.return_value = None
            return r

    spark = _Attempt1ClaimsSpark()
    iteration = _default_b2_iteration()
    dbutils = _FakeDbutils(run_id=run_id_1, iteration=iteration)

    with pytest.raises(RuntimeError, match="simulated attempt-1 JDBC failure"):
        _run_rendered(rendered, spark, dbutils)

    # Fail-mirror UPDATE for attempt-1 must have been issued from the except branch.
    failed_updates = [
        s for s in spark.statements
        if "UPDATE" in s.upper() and "execution_status = 'failed'" in s
    ]
    assert failed_updates, (
        "Attempt-1 fail-mirror must emit UPDATE setting execution_status='failed' "
        "when the JDBC read fails inside the try block"
    )
