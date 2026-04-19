"""Tests for landed batch recovery helpers on WatermarkManager."""

from __future__ import annotations

import re
from typing import Any, List, Optional
from unittest.mock import MagicMock


class _FakeResult:
    def __init__(self, payload: Any) -> None:
        self.payload = payload

    def first(self) -> Any:
        if self.payload is None:
            return None
        row = MagicMock()
        if isinstance(self.payload, dict):
            row.__getitem__.side_effect = lambda key: self.payload.get(key)
        else:
            row.__getitem__.side_effect = lambda key: (
                self.payload if key == "num_affected_rows" else None
            )
        return row

    def collect(self) -> List[Any]:
        first = self.first()
        return [] if first is None else [first]


class _ScriptedSpark:
    def __init__(self, script: Optional[List[Any]] = None) -> None:
        self.script = list(script or [])
        self.statements: List[str] = []
        self.conf = MagicMock()
        self.conf.set = MagicMock()

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        if not self.script:
            return _FakeResult(None)
        action = self.script.pop(0)
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, (int, dict)) or action is None:
            return _FakeResult(action)
        raise AssertionError(f"unsupported script action: {action!r}")


def _make_wm(spark: _ScriptedSpark) -> Any:
    from lhp_watermark import WatermarkManager

    pending = list(spark.script)
    spark.script.clear()
    wm = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    spark.statements.clear()
    spark.script.extend(pending)
    return wm


def test_mark_landed_sets_recoverable_status() -> None:
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)

    wm.mark_landed(
        run_id="job-1-task-2-attempt-3",
        watermark_value="2025-06-01T12:00:00.000000+00:00",
        row_count=42,
    )

    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    assert "status = 'landed_not_committed'" in update
    assert re.search(r"row_count\s*=\s*42", update), update


def test_get_recoverable_landed_run_filters_by_status() -> None:
    spark = _ScriptedSpark(
        script=[
            {
                "run_id": "job-1-task-2-attempt-3",
                "watermark_value": "2025-06-01T12:00:00.000000+00:00",
                "row_count": 42,
                "status": "landed_not_committed",
                "created_at": "2025-06-01T12:05:00.000000+00:00",
            }
        ]
    )
    wm = _make_wm(spark)

    recoverable = wm.get_recoverable_landed_run(
        source_system_id="postgres_prod",
        schema_name="Production",
        table_name="Product",
    )

    assert recoverable is not None
    assert recoverable["status"] == "landed_not_committed"
    query = next(s for s in spark.statements if "SELECT" in s.upper())
    assert "status = 'landed_not_committed'" in query
