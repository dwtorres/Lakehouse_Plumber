"""Tests for hardened mark_failed (FR-L-02, FR-L-06a).

Covers AC-SA-05, AC-SA-06, AC-SA-25, AC-SA-26, AC-SA-27:
- signature (run_id, error_class, error_message)
- error_message truncated at 4096 characters
- WHERE clause restricts to status IN ('running','failed','timed_out')
- zero-affected on a 'completed' row → TerminalStateGuardError
- idempotent on 'failed' → 'failed' (latest-wins on error_class/message/failed_at)
"""

from __future__ import annotations

import inspect
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


class _ScriptedSpark:
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


# ---------- signature -------------------------------------------------------


def test_signature_carries_error_class_required_param() -> None:
    from lhp.extensions.watermark_manager import WatermarkManager

    sig = inspect.signature(WatermarkManager.mark_failed)
    assert "error_class" in sig.parameters
    param = sig.parameters["error_class"]
    assert param.default is inspect.Parameter.empty, "error_class must be required"


# ---------- WHERE clause ---------------------------------------------------


def test_where_restricts_to_non_terminal_success_statuses() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_failed(
        run_id="job-1-task-2-attempt-3",
        error_class="IOError",
        error_message="boom",
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    pattern = re.compile(
        r"status\s+IN\s*\(\s*'running'\s*,\s*'failed'\s*,\s*'timed_out'\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(
        update
    ), f"missing terminal-success guard in WHERE; SQL: {update}"


def test_update_writes_status_failed_and_error_class_and_message() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_failed(
        run_id="job-1-task-2-attempt-3",
        error_class="IOError",
        error_message="connection reset",
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert re.search(r"status\s*=\s*'failed'", update, re.IGNORECASE), update
    assert "IOError" in update
    assert "connection reset" in update


# ---------- error_message truncation ---------------------------------------


def test_error_message_is_truncated_at_4096_characters() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    long_msg = "x" * 10_000
    wm.mark_failed(
        run_id="job-1-task-2-attempt-3",
        error_class="RuntimeError",
        error_message=long_msg,
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # The literal in SQL must contain at most 4096 'x' characters.
    matches = re.findall(r"x+", update)
    longest_run = max((len(m) for m in matches), default=0)
    assert (
        longest_run == 4096
    ), f"error_message must be truncated to 4096 chars; got run length {longest_run}"


# ---------- guard behaviour ------------------------------------------------


def test_zero_affected_on_completed_raises_terminal_state_guard_error() -> None:
    """Caller invoked mark_failed on a row already in status='completed'."""
    from lhp.extensions.watermark_manager import TerminalStateGuardError

    spark = _ScriptedSpark(script=[0, {"status": "completed"}])
    wm = _make_wm(spark)
    with pytest.raises(TerminalStateGuardError) as exc:
        wm.mark_failed(
            run_id="job-1-task-2-attempt-3",
            error_class="IOError",
            error_message="boom",
        )
    assert exc.value.run_id == "job-1-task-2-attempt-3"
    assert exc.value.current_status == "completed"
    assert exc.value.error_code == "LHP-WM-002"


def test_idempotent_on_failed_to_failed_with_latest_wins() -> None:
    """Calling mark_failed on a row already in 'failed' updates the error
    fields and succeeds (latest failure wins, FR-L-06a)."""
    spark = _ScriptedSpark(script=[1])  # UPDATE finds the row, fields refreshed
    wm = _make_wm(spark)
    wm.mark_failed(
        run_id="job-1-task-2-attempt-3",
        error_class="TimeoutError",
        error_message="read timed out",
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert "TimeoutError" in update
    assert "read timed out" in update


# ---------- input validation -----------------------------------------------


def test_rejects_invalid_run_id_before_any_sql() -> None:
    from lhp.extensions.watermark_manager import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.mark_failed(
            run_id="job-1 OR 1=1 --",
            error_class="IOError",
            error_message="boom",
        )
    assert spark.statements == []


def test_rejects_error_class_with_control_chars() -> None:
    from lhp.extensions.watermark_manager import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.mark_failed(
            run_id="job-1-task-2-attempt-3",
            error_class="IO\x00Error",
            error_message="boom",
        )
    assert spark.statements == []


def test_error_message_with_embedded_single_quote_is_safely_quoted() -> None:
    """The validator allows quotes (legitimate error text); the sql_literal
    emitter doubles them so the resulting SQL is well-formed."""
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    wm.mark_failed(
        run_id="job-1-task-2-attempt-3",
        error_class="ValueError",
        error_message="o'reilly's batch failed",
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # Doubled-quote form: 'o''reilly''s batch failed'
    assert "'o''reilly''s batch failed'" in update
