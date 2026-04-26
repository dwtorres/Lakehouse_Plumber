"""Tests for ``lhp_watermark._merge_helpers`` (B2 plan U3 / R1).

Covers the retry-loop extraction shared between ``WatermarkManager`` and
B2 ``b2_manifests`` writes:

- happy path → returns num_affected_rows from result row
- concurrent-commit retry → exhausts up to retry_budget attempts
- on_exhausted callback fires on budget exhaustion
- non-concurrent-commit exception bypasses retry
- Py4J-wrapped concurrent-commit detected via message text
- result.first() is None → treated as success (older Spark / mocks)
- num_affected_rows missing from row → treated as 1
"""

from __future__ import annotations

from typing import Any, List, Optional
from unittest.mock import MagicMock

import pytest

from lhp_watermark._merge_helpers import (
    CONCURRENT_COMMIT_NAMES,
    DEFAULT_RETRY_BUDGET,
    execute_with_concurrent_commit_retry,
    is_concurrent_commit_exception,
)


# --------- helpers replicate test_insert_new.py patterns -----------------


class _FakeMergeResult:
    def __init__(self, num_affected_rows: int) -> None:
        self._n = num_affected_rows

    def first(self) -> Any:
        row = MagicMock()
        row.__getitem__.side_effect = lambda key: (
            self._n if key == "num_affected_rows" else None
        )
        return row


class _NoMetricResult:
    """Result whose row does not expose ``num_affected_rows``."""

    def first(self) -> Any:
        row = MagicMock()
        row.__getitem__.side_effect = KeyError("num_affected_rows")
        return row


class _FirstReturnsNoneResult:
    """Result whose ``first()`` returns ``None`` (older Spark / mocks)."""

    def first(self) -> Any:
        return None


class _ScriptedSpark:
    def __init__(self, script: List[Any]) -> None:
        self.script = list(script)
        self.statements: List[str] = []

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        action = self.script.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action


def _delta_concurrent_append() -> Exception:
    cls = type("ConcurrentAppendException", (Exception,), {})
    cls.__module__ = "io.delta.exceptions"
    return cls("commit failed: concurrent append")


def _delta_concurrent_delete_read() -> Exception:
    cls = type("ConcurrentDeleteReadException", (Exception,), {})
    cls.__module__ = "io.delta.exceptions"
    return cls("commit failed: concurrent delete-read")


def _py4j_wrapped(inner: Exception) -> Exception:
    cls = type("Py4JJavaError", (Exception,), {})
    cls.__module__ = "py4j.protocol"
    msg = (
        "An error occurred while calling something. "
        f"Trace: {type(inner).__name__}: {inner}"
    )
    return cls(msg)


def _raise_marker(last_exc: Optional[BaseException]) -> None:
    raise RuntimeError("budget-exhausted") from last_exc


# --------- is_concurrent_commit_exception ---------------------------------


def test_is_concurrent_commit_exception_direct_class() -> None:
    assert is_concurrent_commit_exception(_delta_concurrent_append()) is True
    assert is_concurrent_commit_exception(_delta_concurrent_delete_read()) is True


def test_is_concurrent_commit_exception_py4j_wrapped() -> None:
    assert is_concurrent_commit_exception(
        _py4j_wrapped(_delta_concurrent_append())
    ) is True


def test_is_concurrent_commit_exception_unrelated_returns_false() -> None:
    assert is_concurrent_commit_exception(RuntimeError("syntax error")) is False
    assert is_concurrent_commit_exception(KeyError("x")) is False


def test_class_names_constant_includes_both() -> None:
    assert "ConcurrentAppendException" in CONCURRENT_COMMIT_NAMES
    assert "ConcurrentDeleteReadException" in CONCURRENT_COMMIT_NAMES


# --------- happy path -----------------------------------------------------


def test_happy_path_returns_num_affected_rows() -> None:
    spark = _ScriptedSpark([_FakeMergeResult(num_affected_rows=3)])

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 3
    assert len(spark.statements) == 1


def test_happy_path_zero_affected_rows_returned_to_caller() -> None:
    """Zero is a meaningful value (DuplicateRunError trigger); helper does
    not interpret it — caller decides.
    """
    spark = _ScriptedSpark([_FakeMergeResult(num_affected_rows=0)])

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 0


def test_first_none_returns_one() -> None:
    """Older Spark / mocks may surface ``first() is None`` — treat as success."""
    spark = _ScriptedSpark([_FirstReturnsNoneResult()])

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 1


def test_missing_num_affected_rows_returns_one() -> None:
    """Row exists but lacks the metric → fall back to 1."""
    spark = _ScriptedSpark([_NoMetricResult()])

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 1


# --------- retry loop -----------------------------------------------------


def test_retries_on_concurrent_append_then_succeeds(monkeypatch: Any) -> None:
    sleeps: List[float] = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))

    spark = _ScriptedSpark(
        [
            _delta_concurrent_append(),
            _delta_concurrent_delete_read(),
            _FakeMergeResult(num_affected_rows=1),
        ]
    )

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 1
    assert len(spark.statements) == 3, "two retries + one success = 3 attempts"
    assert len(sleeps) == 2
    # Default backoff base 100ms, factor 2, ±50% jitter.
    assert 0.05 <= sleeps[0] <= 0.15
    assert 0.10 <= sleeps[1] <= 0.30


def test_retries_on_py4j_wrapped_concurrent_exception(monkeypatch: Any) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    spark = _ScriptedSpark(
        [
            _py4j_wrapped(_delta_concurrent_append()),
            _FakeMergeResult(num_affected_rows=1),
        ]
    )

    affected = execute_with_concurrent_commit_retry(
        spark, "MERGE INTO t ...", on_exhausted=_raise_marker
    )

    assert affected == 1


# --------- exhaustion -----------------------------------------------------


def test_exhaustion_invokes_on_exhausted_with_last_exception(monkeypatch: Any) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    final = _delta_concurrent_append()
    spark = _ScriptedSpark(
        [
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            final,
        ]
    )

    captured: List[Optional[BaseException]] = []

    def _capture(last_exc: Optional[BaseException]) -> None:
        captured.append(last_exc)
        raise RuntimeError("budget-exhausted") from last_exc

    with pytest.raises(RuntimeError, match="budget-exhausted") as exc:
        execute_with_concurrent_commit_retry(
            spark, "MERGE INTO t ...", on_exhausted=_capture
        )

    assert len(spark.statements) == DEFAULT_RETRY_BUDGET
    assert captured == [final]
    assert exc.value.__cause__ is final


def test_custom_retry_budget_respected(monkeypatch: Any) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    spark = _ScriptedSpark(
        [
            _delta_concurrent_append(),
            _delta_concurrent_append(),
        ]
    )

    def _raise(last_exc: Optional[BaseException]) -> None:
        raise RuntimeError("custom-budget") from last_exc

    with pytest.raises(RuntimeError, match="custom-budget"):
        execute_with_concurrent_commit_retry(
            spark, "MERGE INTO t ...", on_exhausted=_raise, retry_budget=2
        )

    assert len(spark.statements) == 2


def test_on_exhausted_returning_without_raise_triggers_assertion(
    monkeypatch: Any,
) -> None:
    """Defensive: callbacks typed NoReturn must raise. If they don't, the
    helper raises AssertionError rather than silently returning a sentinel.
    """
    monkeypatch.setattr("time.sleep", lambda s: None)
    spark = _ScriptedSpark(
        [_delta_concurrent_append() for _ in range(DEFAULT_RETRY_BUDGET)]
    )

    def _bad_callback(last_exc: Optional[BaseException]) -> None:
        # intentionally does not raise
        return None

    with pytest.raises(AssertionError, match="on_exhausted"):
        execute_with_concurrent_commit_retry(
            spark, "MERGE INTO t ...", on_exhausted=_bad_callback  # type: ignore[arg-type]
        )


# --------- non-concurrent exceptions bypass retry ------------------------


def test_unrelated_exception_raises_immediately(monkeypatch: Any) -> None:
    monkeypatch.setattr("time.sleep", lambda s: pytest.fail("must not sleep"))
    spark = _ScriptedSpark([RuntimeError("syntax error in MERGE")])

    with pytest.raises(RuntimeError, match="syntax error"):
        execute_with_concurrent_commit_retry(
            spark, "MERGE INTO t ...", on_exhausted=_raise_marker
        )

    assert len(spark.statements) == 1, "no retry for unrelated exception"


def test_keyboard_interrupt_propagates(monkeypatch: Any) -> None:
    """BLE001 we catch BaseException to handle Py4J wrappers; verify
    KeyboardInterrupt still escapes (concurrent-commit detection rejects it).
    """
    monkeypatch.setattr("time.sleep", lambda s: pytest.fail("must not sleep"))
    spark = _ScriptedSpark([KeyboardInterrupt()])

    with pytest.raises(KeyboardInterrupt):
        execute_with_concurrent_commit_retry(
            spark, "MERGE INTO t ...", on_exhausted=_raise_marker
        )
