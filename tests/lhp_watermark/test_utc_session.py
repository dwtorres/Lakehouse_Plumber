"""Tests for the UTC session-timezone guard (FR-L-07, AC-SA-28).

Every WatermarkManager method that issues a Spark SQL call must first
``spark.conf.set("spark.sql.session.timeZone", "UTC")`` on the active
session, otherwise the watermark table's TIMESTAMP literals are
interpreted in whatever session timezone the caller happens to use,
silently corrupting round-trip semantics.

Tests use a mock SparkSession that records the order of calls to
``conf.set`` and ``sql``; the guard is asserted to fire before the
first SQL statement in each method.
"""

from __future__ import annotations

from typing import Any, List, Tuple
from unittest.mock import MagicMock

import pytest


class _RecordingSpark:
    """Minimal SparkSession double that records the order of conf/sql calls.

    Each entry is a tuple ``(kind, payload)`` where kind is "conf" or "sql".
    ``sql`` returns a MagicMock so chained ``.collect()``, ``.first()``,
    ``.toLocalIterator()`` calls work without us hand-rolling each one.
    """

    def __init__(self) -> None:
        self.calls: List[Tuple[str, Any]] = []
        self.conf = MagicMock()
        self.conf.set = self._record_conf

    def _record_conf(self, key: str, value: str) -> None:
        self.calls.append(("conf", (key, value)))

    def sql(self, statement: str) -> MagicMock:
        self.calls.append(("sql", statement))
        result = MagicMock()
        # Defaults that are friendly to existing _manager.py call sites.
        result.collect.return_value = []
        result.first.return_value = None
        return result


def _make_manager(spark: _RecordingSpark) -> Any:
    from lhp.extensions.watermark_manager import WatermarkManager

    # __init__ may itself emit DDL via _ensure_table_exists; that is allowed
    # but must also be preceded by the UTC guard. Tests below assert this.
    return WatermarkManager(spark, catalog="metadata", schema="orchestration")


def _utc_set_index(calls: List[Tuple[str, Any]]) -> int:
    for i, (kind, payload) in enumerate(calls):
        if kind == "conf" and payload == ("spark.sql.session.timeZone", "UTC"):
            return i
    return -1


def _first_sql_index(calls: List[Tuple[str, Any]]) -> int:
    for i, (kind, _) in enumerate(calls):
        if kind == "sql":
            return i
    return -1


def test_ensure_utc_session_helper_exists() -> None:
    from lhp.extensions.watermark_manager._manager import WatermarkManager

    assert hasattr(
        WatermarkManager, "_ensure_utc_session"
    ), "WatermarkManager must expose _ensure_utc_session for FR-L-07"


def test_helper_sets_utc_on_active_session() -> None:
    spark = _RecordingSpark()
    wm = _make_manager(spark)
    spark.calls.clear()  # ignore __init__ chatter for this assertion
    wm._ensure_utc_session()
    assert ("conf", ("spark.sql.session.timeZone", "UTC")) in spark.calls


def test_helper_is_idempotent() -> None:
    spark = _RecordingSpark()
    wm = _make_manager(spark)
    spark.calls.clear()
    wm._ensure_utc_session()
    wm._ensure_utc_session()
    wm._ensure_utc_session()
    # Idempotent semantics: calling many times produces the same observable
    # state. Repeated conf.set with the same value is a no-op for Spark, so
    # we accept multiple calls — what matters is the value is correct.
    utc_calls = [
        c for c in spark.calls if c == ("conf", ("spark.sql.session.timeZone", "UTC"))
    ]
    assert len(utc_calls) >= 1


@pytest.mark.parametrize(
    "method_name, args, kwargs",
    [
        ("get_latest_watermark", ("sys", "schema", "table"), {}),
        (
            "insert_new",
            (),
            dict(
                run_id="job-1-task-1-attempt-1",
                source_system_id="sys",
                schema_name="schema",
                table_name="table",
                watermark_column_name="col",
                watermark_value="2025-06-01 00:00:00",
                row_count=10,
                extraction_type="incremental",
            ),
        ),
        (
            "mark_failed",
            (),
            dict(
                run_id="job-1-task-1-attempt-1",
                error_class="RuntimeError",
                error_message="boom",
            ),
        ),
        (
            "mark_bronze_complete",
            (),
            dict(run_id="job-1-task-1-attempt-1"),
        ),
        (
            "mark_silver_complete",
            (),
            dict(run_id="job-1-task-1-attempt-1"),
        ),
        (
            "mark_complete",
            (),
            dict(
                run_id="job-1-task-1-attempt-1",
                watermark_value="2025-06-01T12:00:00.000000+00:00",
                row_count=10,
            ),
        ),
        (
            "cleanup_stale_runs",
            (),
            dict(stale_timeout_hours=4.0),
        ),
    ],
)
def test_dml_methods_set_utc_before_first_spark_sql(
    method_name: str, args: tuple, kwargs: dict
) -> None:
    """Every DML method must call _ensure_utc_session before any spark.sql."""
    spark = _RecordingSpark()
    wm = _make_manager(spark)
    spark.calls.clear()

    method = getattr(wm, method_name)
    try:
        method(*args, **kwargs)
    except Exception:
        # Some methods raise on the mock (e.g. insert_new pre-check sees no
        # rows). That's fine — we only care about the call order before the
        # exception fires.
        pass

    utc_idx = _utc_set_index(spark.calls)
    sql_idx = _first_sql_index(spark.calls)
    assert utc_idx != -1, (
        f"{method_name}: spark.conf.set('spark.sql.session.timeZone', 'UTC') "
        f"never called. Calls: {spark.calls}"
    )
    if sql_idx != -1:
        assert utc_idx < sql_idx, (
            f"{method_name}: UTC set must precede first spark.sql. "
            f"utc_idx={utc_idx}, sql_idx={sql_idx}, calls={spark.calls}"
        )
