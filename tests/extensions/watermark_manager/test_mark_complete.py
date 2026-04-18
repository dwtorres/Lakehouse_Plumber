"""Tests for hardened mark_complete (FR-L-01, FR-L-06, FR-L-07).

Covers AC-SA-01..03, AC-SA-23, AC-SA-24:
- watermark_value is a required, validated parameter
- UPDATE WHERE clause refuses terminal-failure rows
  (status NOT IN ('failed','timed_out','landed_not_committed'))
- zero-affected → TerminalStateGuardError(run_id, current_status) where
  current_status is read back from the table
- completed_at is a UTC ISO-8601 microsecond literal (FR-L-07)
"""

from __future__ import annotations

import re
from typing import Any, List, Optional
from unittest.mock import MagicMock

import pytest


class _FakeUpdateResult:
    def __init__(self, num_affected_rows: int) -> None:
        self._n = num_affected_rows

    def first(self) -> Any:
        row = MagicMock()
        row.__getitem__.side_effect = lambda key: (
            self._n if key == "num_affected_rows" else None
        )
        return row

    def collect(self) -> List[Any]:
        return [self.first()]


class _ScriptedSpark:
    """Spark double whose sql() consumes an entry from script per call.

    Entries:
      - int N        : UPDATE-style result with num_affected_rows=N
      - dict R       : SELECT-style result returning a single row R
      - "noop"       : empty result
      - Exception    : raised
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
            return _FakeUpdateResult(num_affected_rows=action)
        if isinstance(action, dict):
            r = MagicMock()
            row = MagicMock()
            row.__getitem__.side_effect = lambda key, _r=action: _r.get(key)
            r.first.return_value = row
            r.collect.return_value = [row]
            return r
        if action == "noop":
            r = MagicMock()
            r.collect.return_value = []
            r.first.return_value = None
            return r
        raise AssertionError(f"unsupported script action: {action!r}")


def _make_wm(spark: _ScriptedSpark) -> Any:
    from lhp.extensions.watermark_manager import WatermarkManager

    pending = list(spark.script)
    spark.script.clear()
    wm = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    spark.statements.clear()
    spark.script.extend(pending)
    return wm


def _kwargs(**overrides: Any) -> dict:
    base = dict(
        run_id="job-1-task-2-attempt-3",
        watermark_value="2025-06-01T12:00:00.000000+00:00",
        row_count=10,
    )
    base.update(overrides)
    return base


# ---------- happy path -------------------------------------------------------


def test_mark_complete_emits_terminal_failure_guard_in_where() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)

    wm.mark_complete(**_kwargs())

    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # Guard must exclude all terminal-failure statuses.
    pattern = re.compile(
        r"status\s+NOT\s+IN\s*\(\s*'failed'\s*,\s*'timed_out'\s*,\s*'landed_not_committed'\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(
        update
    ), f"missing terminal-failure guard in UPDATE WHERE; SQL: {update}"


def test_mark_complete_writes_utc_iso8601_microsecond_completed_at() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_complete(**_kwargs())
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # AC-SA-03: TIMESTAMP 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00'
    pattern = re.compile(
        r"TIMESTAMP\s+'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00'"
    )
    assert pattern.search(
        update
    ), f"completed_at must be a UTC ISO-8601 microsecond literal; SQL: {update}"


def test_mark_complete_sets_status_completed() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_complete(**_kwargs())
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert re.search(r"status\s*=\s*'completed'", update, re.IGNORECASE), update


# ---------- guard behaviour --------------------------------------------------


def test_mark_complete_raises_terminal_state_guard_error_when_no_rows_affected() -> (
    None
):
    """Row was already in a terminal-failure state; UPDATE matches zero rows."""
    from lhp.extensions.watermark_manager import TerminalStateGuardError

    # Script: UPDATE returns 0, then read-back SELECT returns the current
    # status so the exception can carry it.
    spark = _ScriptedSpark(script=[0, {"status": "failed"}])
    wm = _make_wm(spark)

    with pytest.raises(TerminalStateGuardError) as exc:
        wm.mark_complete(**_kwargs())
    assert exc.value.run_id == "job-1-task-2-attempt-3"
    assert exc.value.current_status == "failed"
    assert exc.value.error_code == "LHP-WM-002"


def test_mark_complete_carries_current_status_none_when_row_missing() -> None:
    """Zero-affected with no row found in read-back: current_status=None."""
    from lhp.extensions.watermark_manager import TerminalStateGuardError

    spark = _ScriptedSpark(script=[0, "noop"])
    wm = _make_wm(spark)
    with pytest.raises(TerminalStateGuardError) as exc:
        wm.mark_complete(**_kwargs())
    assert exc.value.current_status is None


def test_mark_complete_does_not_raise_on_one_row_affected() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_complete(**_kwargs())  # must not raise


# ---------- input validation -------------------------------------------------


def test_mark_complete_rejects_invalid_run_id_before_any_sql() -> None:
    from lhp.extensions.watermark_manager import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.mark_complete(**_kwargs(run_id="job-1 OR 1=1 --"))
    assert spark.statements == []


def test_mark_complete_rejects_float_watermark_value() -> None:
    from lhp.extensions.watermark_manager import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.mark_complete(**_kwargs(watermark_value=3.14))
    assert spark.statements == []


def test_mark_complete_accepts_decimal_watermark_value() -> None:
    from decimal import Decimal

    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_complete(**_kwargs(watermark_value=Decimal("99.99")))
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # Decimal must emit unquoted (sql_numeric_literal), not '99.99'.
    assert re.search(r"watermark_value\s*=\s*99\.99(?!\s*')", update), update


def test_mark_complete_signature_requires_watermark_value() -> None:
    """L3 §4.2.1: watermark_value is required (was Optional)."""
    import inspect

    from lhp.extensions.watermark_manager import WatermarkManager

    sig = inspect.signature(WatermarkManager.mark_complete)
    param = sig.parameters["watermark_value"]
    assert (
        param.default is inspect.Parameter.empty
    ), "watermark_value must be required; remove the Optional default"
