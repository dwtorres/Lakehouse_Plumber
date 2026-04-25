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
    spark = _ScriptedByPrefixSpark()
    spark.register("SHOW TABLES", _show_tables_handler(table_present=False))
    # No DESCRIBE handlers needed: CREATE path returns before probes fire.

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

    # No ALTER SQL on the create path.
    assert not _statements_starting_with(
        spark, "ALTER TABLE"
    ), "CREATE path must not emit ALTER"


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
