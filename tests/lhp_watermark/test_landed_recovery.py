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


# ---------- Tier 2 load_group threading -------------------------------------


def test_mark_landed_accepts_load_group_kwarg_without_changing_where() -> None:
    """``load_group`` is store-only on UPDATEs — accepted to keep call-site
    signatures uniform but the WHERE clause keeps filtering by ``run_id``
    (and ``status='running'``) only.
    """
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)

    wm.mark_landed(
        run_id="job-1-task-2-attempt-3",
        watermark_value="2025-06-01T12:00:00.000000+00:00",
        row_count=42,
        load_group="pipe_a::fg_a",
    )

    update = next(s for s in spark.statements if "UPDATE" in s.upper())
    # WHERE shape unchanged — no load_group filter.
    assert "load_group" not in update.lower(), (
        "mark_landed UPDATE must not reference load_group "
        f"(store-only kwarg); SQL: {update}"
    )


def test_get_recoverable_landed_run_with_composite_load_group_emits_three_way() -> None:
    """B2 caller filters to a specific composite — the three-way clause
    appears alongside the existing status filter."""
    spark = _ScriptedSpark(script=[None])
    wm = _make_wm(spark)

    wm.get_recoverable_landed_run(
        source_system_id="postgres_prod",
        schema_name="Production",
        table_name="Product",
        load_group="pipe_a::fg_a",
    )

    query = next(s for s in spark.statements if "SELECT" in s.upper())
    assert "status = 'landed_not_committed'" in query
    pattern = re.compile(
        r"AND\s*\(\s*load_group\s*=\s*'pipe_a::fg_a'\s+OR\s+"
        r"\(\s*'pipe_a::fg_a'\s+IS\s+NULL\s+AND\s+load_group\s+IS\s+NULL\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(query), (
        f"missing three-way load_group clause; SQL: {query}"
    )


def test_get_recoverable_landed_run_with_load_group_none_collapses_to_null() -> None:
    """Legacy caller — ``load_group=None`` (default) → both substitutions
    are SQL ``NULL`` so the right arm matches NULL-load_group rows."""
    spark = _ScriptedSpark(script=[None])
    wm = _make_wm(spark)

    wm.get_recoverable_landed_run(
        source_system_id="postgres_prod",
        schema_name="Production",
        table_name="Product",
    )

    query = next(s for s in spark.statements if "SELECT" in s.upper())
    pattern = re.compile(
        r"AND\s*\(\s*load_group\s*=\s*NULL\s+OR\s+"
        r"\(\s*NULL\s+IS\s+NULL\s+AND\s+load_group\s+IS\s+NULL\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(query), (
        f"load_group=None must emit both arms with NULL substitutions; SQL: {query}"
    )
