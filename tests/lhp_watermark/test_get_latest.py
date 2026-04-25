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


# ---------- Tier 2 load_group filter ----------------------------------------


def test_load_group_clause_is_three_way_when_caller_passes_composite() -> None:
    """B2 caller passes a composite ``load_group`` (e.g. ``'pipe_a::fg_a'``).

    The composer always emits both arms of the three-way filter; runtime
    collapses based on the substituted literal. Left arm matches the
    composite; right arm degenerates to false because the literal is not
    NULL.
    """
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs(load_group="pipe_a::fg_a"))
    sql = spark.statements[-1]
    pattern = re.compile(
        r"AND\s*\(\s*load_group\s*=\s*'pipe_a::fg_a'\s+OR\s+"
        r"\(\s*'pipe_a::fg_a'\s+IS\s+NULL\s+AND\s+load_group\s+IS\s+NULL\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(sql), f"missing three-way load_group clause; SQL: {sql}"


def test_load_group_clause_collapses_to_is_null_when_caller_passes_none() -> None:
    """Legacy caller passes ``load_group=None`` (default).

    Both literal substitutions become SQL ``NULL`` so the right arm
    collapses to ``NULL IS NULL AND load_group IS NULL`` and matches
    legacy + pre-Tier-2 NULL-load_group rows. The left arm
    (``load_group = NULL``) evaluates to UNKNOWN per three-valued logic
    and never matches.
    """
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs())  # load_group omitted = None
    sql = spark.statements[-1]
    pattern = re.compile(
        r"AND\s*\(\s*load_group\s*=\s*NULL\s+OR\s+"
        r"\(\s*NULL\s+IS\s+NULL\s+AND\s+load_group\s+IS\s+NULL\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(sql), (
        f"load_group=None must emit both arms with NULL substitutions; SQL: {sql}"
    )


def test_get_latest_returns_only_matching_load_group_row() -> None:
    """Probe set spans ``('pipe_a::fg_a', 'pipe_b::fg_b', 'legacy', NULL)``;
    a B2 caller filtered to ``pipe_a::fg_a`` sees only that row.

    The mock returns whatever rows the caller seeds — we assert here that
    the SQL filter is composed correctly so a real Spark would isolate the
    target. The composed SQL is unique to the caller's ``load_group``.
    """
    spark = _RecordingSpark(
        rows=[
            {
                "run_id": "job-1-task-1-attempt-1",
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
    result = wm.get_latest_watermark(**_kwargs(load_group="pipe_a::fg_a"))
    assert result is not None
    sql = spark.statements[-1]
    # The literal must appear exactly twice (left arm + right-arm IS NULL test).
    assert sql.count("'pipe_a::fg_a'") == 2, (
        "three-way filter must substitute the literal in both arms; "
        f"got {sql.count(chr(39) + 'pipe_a::fg_a' + chr(39))} occurrences. SQL: {sql}"
    )


def test_get_latest_load_group_none_emits_null_substitutions() -> None:
    """Legacy probe set (NULL + 'legacy' rows). Both literal substitutions
    are SQL ``NULL`` so both arms involve ``NULL`` checks and runtime
    matches NULL-load_group rows via the right arm."""
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs(load_group=None))
    sql = spark.statements[-1]
    # Two NULL substitutions in the load_group clause specifically.
    # Anchor on the load_group clause to avoid false matches from other
    # places NULL might appear.
    lg_clause = re.search(
        r"AND\s*\(\s*load_group\s*=\s*NULL\s+OR\s+\([^)]*\)\s*\)",
        sql,
        re.IGNORECASE,
    )
    assert lg_clause is not None, f"load_group clause not found in SQL: {sql}"
    assert lg_clause.group(0).count("NULL") >= 3, (
        f"both literal substitutions + IS NULL test expected; got: {lg_clause.group(0)}"
    )


def test_get_latest_rejects_adversarial_load_group() -> None:
    """``load_group`` is data-shaped; ``SQLInputValidator.string`` rejects
    control characters and other injection vectors before any SQL composition."""
    from lhp_watermark import WatermarkValidationError

    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    with pytest.raises(WatermarkValidationError):
        wm.get_latest_watermark(**_kwargs(load_group="bad\x00value"))
    assert spark.statements == [], (
        "validator must reject before any SQL was issued; "
        f"got {spark.statements}"
    )


# ---------- Tier 2 four-cell parametrize matrix -----------------------------


@pytest.mark.parametrize(
    "load_group",
    [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"],
)
def test_get_latest_load_group_matrix_emits_three_way_filter(
    load_group: Optional[str],
) -> None:
    """Across the full migration matrix (None, legacy backfill row, two
    sibling B2 composites), the three-way ``load_group`` clause must
    always render with both arms substituting the same SQL value.

    The probe set below mixes all four values so a real Spark would
    isolate per-cell rows; the mock asserts only on SQL composition,
    which is the load-bearing contract for runtime isolation.
    """
    spark = _RecordingSpark(
        rows=[
            {
                "run_id": f"job-{i}-task-1-attempt-1",
                "watermark_value": f"2025-06-0{i}T12:00:00.000000+00:00",
                "watermark_time": f"2025-06-0{i}T12:00:00.000000+00:00",
                "row_count": 100,
                "status": "completed",
                "bronze_stage_complete": True,
                "silver_stage_complete": True,
            }
            for i in range(1, 5)
        ]
    )
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs(load_group=load_group))
    sql = spark.statements[-1]

    expected_literal = "NULL" if load_group is None else f"'{load_group}'"
    pattern = re.compile(
        r"AND\s*\(\s*load_group\s*=\s*"
        + re.escape(expected_literal)
        + r"\s+OR\s+\(\s*"
        + re.escape(expected_literal)
        + r"\s+IS\s+NULL\s+AND\s+load_group\s+IS\s+NULL\s*\)\s*\)",
        re.IGNORECASE,
    )
    assert pattern.search(sql), (
        f"load_group={load_group!r} must emit three-way clause with "
        f"{expected_literal} substituted in both arms; SQL: {sql}"
    )
    # For non-None composites, the literal must appear exactly twice in
    # the load_group clause (left arm + right-arm IS NULL test). Skip for
    # ``None`` because the substitution is bare ``NULL`` which collides
    # with the SQL ``IS NULL`` keyword and inflates the count.
    if load_group is not None:
        lg_clause = pattern.search(sql)
        assert lg_clause is not None
        assert lg_clause.group(0).count(expected_literal) == 2, (
            f"three-way filter must substitute {expected_literal} in both "
            f"arms; got: {lg_clause.group(0)}"
        )


@pytest.mark.parametrize(
    "load_group",
    [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"],
)
def test_get_latest_load_group_matrix_preserves_tier1_isolation(
    load_group: Optional[str],
) -> None:
    """Tier 1 cross-source isolation must remain intact across every
    Tier 2 ``load_group`` cell — the SQL still filters by
    ``source_system_id``, ``schema_name``, ``table_name``, and
    ``status='completed'`` regardless of the new axis.
    """
    spark = _RecordingSpark(rows=[])
    wm = _make_wm(spark)
    wm.get_latest_watermark(**_kwargs(load_group=load_group))
    sql = spark.statements[-1]

    # Tier 1 invariants: cross-source / cross-schema / cross-table isolation.
    assert "'postgres_prod'" in sql, f"missing source_system_id literal; SQL: {sql}"
    assert "'Production'" in sql, f"missing schema_name literal; SQL: {sql}"
    assert "'Product'" in sql, f"missing table_name literal; SQL: {sql}"
    assert re.search(
        r"\bstatus\s*=\s*'completed'", sql, re.IGNORECASE
    ), f"missing status guard; SQL: {sql}"
