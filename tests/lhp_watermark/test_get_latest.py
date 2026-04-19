"""Tests for get_latest_watermark completion filter (FR-L-04, AC-SA-15..18).

Hardened semantics:
- WHERE status = 'completed' excludes running / failed / timed_out rows.
- ORDER BY watermark_time DESC, run_id DESC LIMIT 1 deterministic tie-break.
- Returns None when no terminal-success row exists.
- WARNING log on the None path so operators see pre-migration full-reload intent.
"""

from __future__ import annotations

import logging
import re
from typing import Any, List, Optional
from unittest.mock import MagicMock

import pytest


class _RecordingSpark:
    def __init__(self, rows: Optional[List[dict]] = None) -> None:
        self.statements: List[str] = []
        self.rows = rows or []
        self.conf = MagicMock()
        self.conf.set = MagicMock()

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        result = MagicMock()
        # Mimic Row.__getitem__
        rows_obj: List[Any] = []
        for r in self.rows:
            row = MagicMock()
            row.__getitem__.side_effect = lambda key, _r=r: _r.get(key)
            rows_obj.append(row)
        result.collect.return_value = rows_obj
        result.first.return_value = rows_obj[0] if rows_obj else None
        return result


def _make_wm(spark: _RecordingSpark) -> Any:
    from lhp_watermark import WatermarkManager

    wm = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    spark.statements.clear()
    return wm


def _kwargs(**overrides: Any) -> dict:
    base = dict(
        source_system_id="postgres_prod",
        schema_name="Production",
        table_name="Product",
    )
    base.update(overrides)
    return base


# ---------- query shape -----------------------------------------------------


def test_select_filters_by_completed_status() -> None:
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs())
    assert spark.statements, "no SQL was issued"
    sql = spark.statements[-1]
    assert re.search(
        r"\bstatus\s*=\s*'completed'", sql, re.IGNORECASE
    ), f"missing WHERE status='completed' filter; SQL: {sql}"


def test_select_orders_by_watermark_time_then_run_id_desc_limit_one() -> None:
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs())
    sql = spark.statements[-1]
    # Must order by watermark_time DESC then run_id DESC then LIMIT 1.
    pattern = re.compile(
        r"ORDER\s+BY\s+watermark_time\s+DESC\s*,\s*run_id\s+DESC\s+LIMIT\s+1",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(sql), f"missing tie-break ORDER BY; SQL: {sql}"


def test_select_does_not_use_max_subquery() -> None:
    """The pre-Slice-A subquery (MAX(watermark_time)) cannot coexist with the
    new ORDER BY tie-break — confirm it was removed."""
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs())
    sql = spark.statements[-1]
    assert "MAX(watermark_time)" not in sql.upper().replace(" ", "").replace(
        "MAX(WATERMARK_TIME)", "MAX(watermark_time)"
    ), "remove the legacy MAX(watermark_time) subquery"


# ---------- return value ----------------------------------------------------


def test_returns_dict_when_completed_row_present() -> None:
    spark = _RecordingSpark(
        rows=[
            {
                "run_id": "job-2-task-1-attempt-1",
                "watermark_value": "2025-06-01T12:00:00.000000+00:00",
                "watermark_time": "2025-06-01T12:00:00.000000+00:00",
                "row_count": 100,
                "status": "completed",
                "bronze_stage_complete": True,
                "silver_stage_complete": True,
            }
        ]
    )
    wm = _make_wm(spark)
    result = wm.get_latest_watermark(**_kwargs())
    assert result is not None
    assert result["run_id"] == "job-2-task-1-attempt-1"
    assert result["status"] == "completed"


def test_returns_none_when_no_completed_row(caplog: Any) -> None:
    """Pre-migration tables have only `running`/`failed` rows; the lookup
    returns None and logs a WARNING so operators see the impending
    full-reload (FR-L-04 migration hazard, FR-L-M1)."""
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    with caplog.at_level(
        logging.WARNING, logger="lhp_watermark.watermark_manager"
    ):
        result = wm.get_latest_watermark(**_kwargs())
    assert result is None
    warning_messages = [
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    ]
    assert any(
        "postgres_prod" in m and "Production" in m and "Product" in m
        for m in warning_messages
    ), f"expected WARNING log naming the key; got: {warning_messages}"


# ---------- input validation ------------------------------------------------


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("source_system_id", "x; DROP TABLE x"),
        ("schema_name", "x\x00null"),
        ("table_name", "x\nnewline"),
    ],
)
def test_rejects_adversarial_inputs_before_any_spark_sql(
    field: str, bad_value: str
) -> None:
    from lhp_watermark import WatermarkValidationError

    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.get_latest_watermark(**_kwargs(**{field: bad_value}))
    assert (
        spark.statements == []
    ), f"validator must reject before any SQL was issued; got {spark.statements}"
