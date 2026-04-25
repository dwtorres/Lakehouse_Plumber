"""Tests for mark_bronze_complete + mark_silver_complete guards (FR-L-06).

Both stage-complete methods must:
- Refuse to overwrite terminal-failure rows
  (status NOT IN ('failed', 'timed_out', 'landed_not_committed')).
- Raise TerminalStateGuardError on zero affected rows, with the current
  status read back from the table.
- Pass run_id through SQLInputValidator.uuid_or_job_run_id and emitters
  rather than the legacy _validate_identifier shortcut.

Covers AC-SA-23 + AC-SA-24 stage-method branches.
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
    from lhp_watermark import WatermarkManager

    pending = list(spark.script)
    spark.script.clear()
    wm = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    spark.statements.clear()
    spark.script.extend(pending)
    return wm


_GUARD_PATTERN = re.compile(
    r"status\s+NOT\s+IN\s*\(\s*'failed'\s*,\s*'timed_out'\s*,\s*'landed_not_committed'\s*\)",
    re.IGNORECASE,
)


# ---------- guard appears in both methods ------------------------------------


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_method_emits_terminal_failure_guard_in_where(method_name: str) -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    getattr(wm, method_name)(run_id="job-1-task-2-attempt-3")
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert _GUARD_PATTERN.search(
        update
    ), f"{method_name}: missing terminal-failure guard; SQL: {update}"


# ---------- guard fires on zero affected -------------------------------------


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_method_raises_terminal_state_guard_error_on_zero_affected(
    method_name: str,
) -> None:
    from lhp_watermark import TerminalStateGuardError

    spark = _ScriptedSpark(script=[0, {"status": "failed"}])
    wm = _make_wm(spark)
    with pytest.raises(TerminalStateGuardError) as exc:
        getattr(wm, method_name)(run_id="job-1-task-2-attempt-3")
    assert exc.value.run_id == "job-1-task-2-attempt-3"
    assert exc.value.current_status == "failed"
    assert exc.value.error_code == "LHP-WM-002"


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_method_succeeds_on_one_affected(method_name: str) -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    getattr(wm, method_name)(run_id="job-1-task-2-attempt-3")  # must not raise


# ---------- signature unchanged ----------------------------------------------


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_signature_takes_run_id_and_load_group(method_name: str) -> None:
    """Tier 2 (R3): adds ``load_group`` as the last kwarg with default
    ``None`` so legacy callers continue to work unchanged. No positional
    churn — ``run_id`` stays the only positional parameter."""
    import inspect

    from lhp_watermark import WatermarkManager

    sig = inspect.signature(getattr(WatermarkManager, method_name))
    assert list(sig.parameters.keys()) == [
        "self",
        "run_id",
        "load_group",
    ], f"{method_name}: signature must be (self, run_id, load_group)"
    lg = sig.parameters["load_group"]
    assert lg.default is None, f"{method_name}: load_group must default to None"


# ---------- input validation -------------------------------------------------


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_method_rejects_invalid_run_id_before_any_sql(method_name: str) -> None:
    from lhp_watermark import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        getattr(wm, method_name)(run_id="job-1 OR 1=1 --")
    assert spark.statements == []


# ---------- Tier 2 load_group store-only ------------------------------------


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
@pytest.mark.parametrize(
    "load_group",
    [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"],
)
def test_method_accepts_load_group_kwarg_without_changing_where(
    method_name: str,
    load_group: Optional[str],
) -> None:
    """Tier 2 (R3): ``load_group`` is store-only on stage UPDATEs across
    every cell of the migration matrix. Filter remains by ``run_id`` +
    terminal-failure guard only — adding a ``load_group`` predicate here
    would silently fail recovery when the row's stored ``load_group``
    differs from the caller's (e.g. legacy NULL row, B2-rewritten run).
    """
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    getattr(wm, method_name)(
        run_id="job-1-task-2-attempt-3", load_group=load_group
    )
    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert "load_group" not in update.lower(), (
        f"{method_name} UPDATE must not reference load_group "
        f"(store-only kwarg) for load_group={load_group!r}; SQL: {update}"
    )
    # Terminal-failure guard preserved across every cell.
    assert _GUARD_PATTERN.search(update), (
        f"{method_name}: terminal-failure guard must be preserved "
        f"for load_group={load_group!r}; SQL: {update}"
    )


@pytest.mark.parametrize(
    "method_name", ["mark_bronze_complete", "mark_silver_complete"]
)
def test_method_rejects_adversarial_load_group(method_name: str) -> None:
    """Validator rejects control chars in ``load_group`` before SQL."""
    from lhp_watermark import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        getattr(wm, method_name)(
            run_id="job-1-task-2-attempt-3", load_group="bad\x00value"
        )
    assert spark.statements == []
