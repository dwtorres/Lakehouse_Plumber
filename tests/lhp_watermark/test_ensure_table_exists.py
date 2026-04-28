"""Tests for Tier 2 ``_ensure_table_exists`` extension (R1, R2).

Covers:
- Brand-new tables: ``CREATE TABLE`` includes ``load_group STRING`` and
  ``CLUSTER BY (source_system_id, load_group, schema_name, table_name)``.
- Pre-existing tables missing the ``load_group`` column: exactly one
  ``ALTER TABLE … ADD COLUMNS (load_group STRING)`` is emitted.
- Pre-existing tables with wrong clustering: exactly one
  ``ALTER TABLE … CLUSTER BY (...)`` is emitted with the target key.
- Pre-existing tables already in target shape: zero ALTER SQL emitted —
  only the two probes (DESCRIBE TABLE / DESCRIBE DETAIL) fire.
- Idempotency: a second ``_ensure_table_exists()`` call against the same
  Spark mock emits zero ALTER SQL.

Mock pattern follows ``_RecordingSpark`` precedent (test_get_latest.py)
and ``_ScriptedSpark`` (test_insert_new.py) — scripted-by-statement-prefix
because ``__init__`` triggers the same code path tests want to assert.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock


class _ScriptedByPrefixSpark:
    """SparkSession double whose ``sql()`` reply is keyed off statement prefix.

    The watermark manager's ``_ensure_table_exists`` issues a sequence of
    distinct SQL statements: ``SHOW TABLES IN <ns>``, ``CREATE TABLE …``,
    ``DESCRIBE TABLE …``, ``DESCRIBE DETAIL …``, ``ALTER TABLE …``.
    Tests register handlers per-prefix and the mock dispatches by ``startswith``;
    unmatched statements get a no-op default (used by CREATE/ALTER paths).

    Each handler is invoked as ``handler(stmt, call_index)`` and must return
    a value with ``collect()`` and ``first()`` methods.
    """

    def __init__(
        self,
        handlers: Optional[Dict[str, Callable[[str, int], Any]]] = None,
    ) -> None:
        self.statements: List[str] = []
        self._handlers = dict(handlers or {})
        self.conf = MagicMock()
        self.conf.set = MagicMock()

    def register(self, prefix: str, handler: Callable[[str, int], Any]) -> None:
        self._handlers[prefix] = handler

    def sql(self, statement: str) -> Any:
        idx = len(self.statements)
        self.statements.append(statement)
        normalized = statement.strip().upper()
        for prefix, handler in self._handlers.items():
            if normalized.startswith(prefix.upper()):
                return handler(statement, idx)
        # Unmatched: default no-op result (CREATE/ALTER do not need a payload).
        result = MagicMock()
        result.collect.return_value = []
        result.first.return_value = None
        return result


def _row(mapping: Dict[str, Any]) -> Any:
    """Build a Row-shaped MagicMock that supports ``row[key]`` access."""
    row = MagicMock()
    row.__getitem__.side_effect = lambda key, _m=mapping: _m.get(key)
    return row


def _result_with_rows(rows: List[Any]) -> Any:
    """Wrap a list-of-rows in a DataFrame-shaped MagicMock."""
    result = MagicMock()
    result.collect.return_value = rows
    result.first.return_value = rows[0] if rows else None
    return result


def _show_tables_handler(table_present: bool) -> Callable[[str, int], Any]:
    """``SHOW TABLES IN <ns>`` → return the configured shape."""
    rows = [_row({"tableName": "watermarks"})] if table_present else []
    return lambda _stmt, _idx, _rows=rows: _result_with_rows(_rows)


def _describe_table_handler(columns: List[str]) -> Callable[[str, int], Any]:
    """``DESCRIBE TABLE …`` → return one row per column with ``col_name``."""
    rows = [_row({"col_name": c, "data_type": "STRING"}) for c in columns]
    return lambda _stmt, _idx, _rows=rows: _result_with_rows(_rows)


def _describe_detail_handler(
    clustering: Optional[Tuple[str, ...]],
) -> Callable[[str, int], Any]:
    """``DESCRIBE DETAIL …`` → return one row with ``clusteringColumns`` field."""
    rows = [_row({"clusteringColumns": list(clustering) if clustering else None})]
    return lambda _stmt, _idx, _rows=rows: _result_with_rows(_rows)


def _build_manager(spark: _ScriptedByPrefixSpark) -> Any:
    """Construct the manager (which triggers ``_ensure_table_exists`` once)."""
    from lhp_watermark import WatermarkManager

    return WatermarkManager(spark, catalog="metadata", schema="orchestration")


# ---------- helpers --------------------------------------------------------


def _statements_starting_with(spark: _ScriptedByPrefixSpark, prefix: str) -> List[str]:
    """Filter recorded statements by case-insensitive prefix match."""
    pfx = prefix.upper()
    return [s for s in spark.statements if s.strip().upper().startswith(pfx)]


# ---------- 1. happy path: new table ---------------------------------------


def test_new_table_create_ddl_includes_load_group_column_and_target_clustering() -> None:
    """Brand-new table: CREATE TABLE IF NOT EXISTS includes load_group +
    target clustering. After review fix #18 _ensure_table_exists no longer
    early-returns on the create path — it runs DESCRIBE TABLE / DESCRIBE
    DETAIL unconditionally and reconciles. In production the freshly-created
    table satisfies both probes (CREATE shape matches target shape) so no
    ALTER fires. The test scripts the DESCRIBE responses to match.
    """
    target_columns = [
        "run_id",
        "watermark_time",
        "source_system_id",
        "schema_name",
        "table_name",
        "watermark_column_name",
        "watermark_value",
        "previous_watermark_value",
        "row_count",
        "extraction_type",
        "bronze_stage_complete",
        "silver_stage_complete",
        "status",
        "error_class",
        "error_message",
        "created_at",
        "completed_at",
        "load_group",
    ]
    target_clustering = (
        "source_system_id",
        "load_group",
        "schema_name",
        "table_name",
    )
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=False))
    spark.register("DESCRIBE TABLE", _describe_table_handler(columns=target_columns))
    spark.register("DESCRIBE DETAIL", _describe_detail_handler(clustering=target_clustering))

    _build_manager(spark)

    creates = _statements_starting_with(spark, "CREATE TABLE")
    assert len(creates) == 1, f"expected exactly one CREATE; got {creates}"
    ddl = creates[0]

    # Column list must include load_group STRING (nullable; no NOT NULL).
    assert re.search(
        r"\bload_group\s+STRING\b", ddl, re.IGNORECASE
    ), f"CREATE missing 'load_group STRING'; SQL: {ddl}"

    # CLUSTER BY must list the four columns in target order.
    assert re.search(
        r"CLUSTER\s+BY\s*\(\s*source_system_id\s*,\s*load_group\s*,\s*"
        r"schema_name\s*,\s*table_name\s*\)",
        ddl,
        re.IGNORECASE,
    ), f"CREATE missing target CLUSTER BY; SQL: {ddl}"

    # Post-CREATE reconciliation must no-op when probes confirm target shape.
    assert not _statements_starting_with(
        spark, "ALTER TABLE"
    ), "CREATE path must not emit ALTER when DESCRIBE confirms target shape"


# ---------- 2. pre-existing table missing load_group column ----------------


def test_preexisting_missing_column_emits_single_alter_add_columns() -> None:
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    # Pre-Tier-2 columns: no load_group.
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id",
                "watermark_time",
                "source_system_id",
                "schema_name",
                "table_name",
                "watermark_value",
                "row_count",
                "extraction_type",
                "status",
                "created_at",
            ]
        ),
    )
    # Clustering already at target so the second probe does NOT trigger an ALTER.
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "load_group", "schema_name", "table_name")
        ),
    )

    _build_manager(spark)

    add_columns = [
        s
        for s in _statements_starting_with(spark, "ALTER TABLE")
        if re.search(r"ADD\s+COLUMNS", s, re.IGNORECASE)
    ]
    assert len(add_columns) == 1, (
        f"expected exactly one ALTER … ADD COLUMNS; got {add_columns}"
    )
    assert re.search(
        r"ADD\s+COLUMNS\s*\(\s*load_group\s+STRING\s*\)",
        add_columns[0],
        re.IGNORECASE,
    ), f"ALTER did not match expected shape; SQL: {add_columns[0]}"

    # No CLUSTER BY ALTER emitted.
    cluster_alters = [
        s
        for s in _statements_starting_with(spark, "ALTER TABLE")
        if re.search(r"CLUSTER\s+BY", s, re.IGNORECASE)
    ]
    assert not cluster_alters, (
        f"clustering matched target — should not emit ALTER; got {cluster_alters}"
    )


# ---------- 3. pre-existing table with wrong clustering --------------------


def test_preexisting_wrong_clustering_emits_single_alter_cluster_by() -> None:
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    # Column already present, so no ADD COLUMNS.
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id",
                "source_system_id",
                "schema_name",
                "table_name",
                "load_group",
                "status",
            ]
        ),
    )
    # Old Tier-1 clustering: missing load_group.
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "schema_name", "table_name")
        ),
    )

    _build_manager(spark)

    cluster_alters = [
        s
        for s in _statements_starting_with(spark, "ALTER TABLE")
        if re.search(r"CLUSTER\s+BY", s, re.IGNORECASE)
    ]
    assert len(cluster_alters) == 1, (
        f"expected exactly one ALTER … CLUSTER BY; got {cluster_alters}"
    )
    assert re.search(
        r"CLUSTER\s+BY\s*\(\s*source_system_id\s*,\s*load_group\s*,\s*"
        r"schema_name\s*,\s*table_name\s*\)",
        cluster_alters[0],
        re.IGNORECASE,
    ), f"ALTER CLUSTER BY did not match target; SQL: {cluster_alters[0]}"

    # No ADD COLUMNS emitted because column was already present.
    add_columns = [
        s
        for s in _statements_starting_with(spark, "ALTER TABLE")
        if re.search(r"ADD\s+COLUMNS", s, re.IGNORECASE)
    ]
    assert not add_columns, (
        f"column was present — should not emit ADD COLUMNS; got {add_columns}"
    )


# ---------- 4. already in target shape: zero ALTERs ------------------------


def test_already_in_target_shape_emits_no_alter_sql() -> None:
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id",
                "source_system_id",
                "schema_name",
                "table_name",
                "load_group",
                "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "load_group", "schema_name", "table_name")
        ),
    )

    _build_manager(spark)

    alters = _statements_starting_with(spark, "ALTER TABLE")
    assert not alters, f"target-shape table must emit zero ALTER; got {alters}"

    # Sanity: both probes fire (the only SQL beyond SHOW TABLES).
    assert _statements_starting_with(spark, "DESCRIBE TABLE"), (
        "DESCRIBE TABLE probe must fire"
    )
    assert _statements_starting_with(spark, "DESCRIBE DETAIL"), (
        "DESCRIBE DETAIL probe must fire"
    )


# ---------- 5. idempotency: second call emits zero ALTERs ------------------


def test_second_call_is_idempotent_and_emits_no_alter() -> None:
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id",
                "source_system_id",
                "schema_name",
                "table_name",
                "load_group",
                "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "load_group", "schema_name", "table_name")
        ),
    )

    wm = _build_manager(spark)
    spark.statements.clear()

    # Manually re-invoke; should issue exactly: SHOW TABLES, DESCRIBE TABLE,
    # DESCRIBE DETAIL — and NO ALTER.
    wm._ensure_table_exists()

    alters = _statements_starting_with(spark, "ALTER TABLE")
    assert not alters, f"second call must emit zero ALTER; got {alters}"

    # The second call must have issued the two probes (column + clustering).
    assert _statements_starting_with(spark, "DESCRIBE TABLE"), (
        "second call must re-probe column list"
    )
    assert _statements_starting_with(spark, "DESCRIBE DETAIL"), (
        "second call must re-probe clustering"
    )


# ---------- 6. U4 / Issue #22: ALTER paths retry on ConcurrentAppendException


class _RetryConcurrentSpark(_ScriptedByPrefixSpark):
    """Variant that fails the first N ``ALTER TABLE`` statements with a
    fake ``ConcurrentAppendException`` before letting subsequent calls succeed.

    Used to assert U4: ``_alter_with_retry`` recovers from a transient
    fleet-deploy race, and the budget is correctly bounded.
    """

    def __init__(self, fail_alter_n_times: int) -> None:
        super().__init__()
        self._remaining_fails = fail_alter_n_times
        self.alter_attempts = 0

    def sql(self, statement: str) -> Any:
        if statement.strip().upper().startswith("ALTER TABLE"):
            self.alter_attempts += 1
            if self._remaining_fails > 0:
                self._remaining_fails -= 1
                self.statements.append(statement)
                # Class name match is what `is_concurrent_commit_exception`
                # detects in test environments.
                exc = type("ConcurrentAppendException", (Exception,), {})(
                    "transient fleet-deploy race"
                )
                raise exc
        return super().sql(statement)


def test_alter_cluster_by_retries_on_concurrent_commit_then_succeeds(monkeypatch: Any) -> None:
    """U4 / #22: a single transient ConcurrentAppendException is retried; a
    second ALTER attempt succeeds. Verifies the fleet-deploy recovery path."""
    # Skip the production backoff sleep in tests.
    monkeypatch.setattr("lhp_watermark._merge_helpers.time.sleep", lambda _s: None)
    spark = _RetryConcurrentSpark(fail_alter_n_times=1)
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    # Column present; clustering wrong → ALTER CLUSTER BY fires.
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id", "source_system_id", "schema_name", "table_name",
                "load_group", "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "schema_name", "table_name")
        ),
    )

    _build_manager(spark)

    # Two attempts: first failed (concurrent-commit), second succeeded.
    assert spark.alter_attempts == 2, (
        f"expected exactly 2 ALTER attempts (1 fail + 1 success); "
        f"got {spark.alter_attempts}"
    )


def test_alter_cluster_by_exhausts_budget_and_raises_typed(monkeypatch: Any) -> None:
    """U4 / #22: 11 consecutive ConcurrentAppendException → exhaustion →
    WatermarkConcurrencyError with ``attempts`` reflecting the actual
    retry count (10), not the constant 1 of the pre-U4 implementation."""
    from lhp_watermark.exceptions import WatermarkConcurrencyError
    import pytest

    # Skip backoff sleep — exhaustion path would otherwise sleep ~256s
    # cumulative (0.5 × 2^N for N=0..8) before raising.
    monkeypatch.setattr("lhp_watermark._merge_helpers.time.sleep", lambda _s: None)
    spark = _RetryConcurrentSpark(fail_alter_n_times=20)
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id", "source_system_id", "schema_name", "table_name",
                "load_group", "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "schema_name", "table_name")
        ),
    )

    with pytest.raises(WatermarkConcurrencyError) as exc_info:
        _build_manager(spark)

    err = exc_info.value
    # Pre-U4 implementation hard-coded `attempts=1`. Post-U4 reports the
    # real retry count (10). Accept ``>= 2`` defensively in case the
    # budget is tuned later, but require it to be more than the trivial 1.
    assert err.attempts >= 2, (
        f"WatermarkConcurrencyError.attempts must reflect real retry count; "
        f"got attempts={err.attempts}"
    )
    assert spark.alter_attempts == 10, (
        f"expected exactly _ALTER_RETRY_BUDGET (10) ALTER attempts; "
        f"got {spark.alter_attempts}"
    )


def test_alter_add_columns_retries_on_concurrent_commit(monkeypatch: Any) -> None:
    """U4 / #22: same retry behavior on ADD COLUMNS path."""
    monkeypatch.setattr("lhp_watermark._merge_helpers.time.sleep", lambda _s: None)
    spark = _RetryConcurrentSpark(fail_alter_n_times=1)
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    # Column missing → ADD COLUMNS fires; clustering already correct.
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id", "source_system_id", "schema_name", "table_name",
                "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "load_group", "schema_name", "table_name")
        ),
    )

    _build_manager(spark)

    assert spark.alter_attempts == 2, (
        f"ADD COLUMNS should retry once on concurrent-commit; "
        f"got {spark.alter_attempts} attempts"
    )


def test_alter_non_concurrent_exception_raises_first_attempt_no_retry() -> None:
    """U4 / #22: non-concurrent-commit exception raises on first attempt
    without retry — the helper distinguishes by exception type."""
    import pytest

    class _AnalysisErrorSpark(_ScriptedByPrefixSpark):
        def __init__(self) -> None:
            super().__init__()
            self.alter_attempts = 0

        def sql(self, statement: str) -> Any:
            if statement.strip().upper().startswith("ALTER TABLE"):
                self.alter_attempts += 1
                raise RuntimeError("AnalysisException: invalid clustering column")
            return super().sql(statement)

    spark = _AnalysisErrorSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=True))
    spark.register(
        "DESCRIBE TABLE",
        _describe_table_handler(
            columns=[
                "run_id", "source_system_id", "schema_name", "table_name",
                "load_group", "status",
            ]
        ),
    )
    spark.register(
        "DESCRIBE DETAIL",
        _describe_detail_handler(
            clustering=("source_system_id", "schema_name", "table_name")
        ),
    )

    with pytest.raises(RuntimeError, match="AnalysisException"):
        _build_manager(spark)

    assert spark.alter_attempts == 1, (
        f"non-concurrent-commit exception must NOT retry; "
        f"got {spark.alter_attempts} attempts"
    )
