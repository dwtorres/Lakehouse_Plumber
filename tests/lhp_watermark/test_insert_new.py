"""Tests for hardened WatermarkManager.insert_new (FR-L-05, AC-SA-19..22).

Covers:
- single MERGE statement (no pre-check SELECT)
- DuplicateRunError on zero num_affected_rows
- Delta ConcurrentAppendException / ConcurrentDeleteReadException retry loop
  (jittered exp backoff, budget 5)
- WatermarkConcurrencyError on budget exhaustion with __cause__ chained
- WatermarkValidationError on adversarial input *before* any spark.sql
"""

from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest


class _FakeMergeResult:
    """Stub for ``spark.sql(merge_sql)`` return value.

    Mimics ``DataFrame.first()`` returning a Row with ``num_affected_rows``.
    """

    def __init__(self, num_affected_rows: int) -> None:
        self._n = num_affected_rows

    def first(self) -> Any:
        row = MagicMock()
        row.__getitem__.side_effect = lambda key: (
            self._n if key == "num_affected_rows" else None
        )
        return row

    def collect(self) -> List[Any]:
        return [self.first()]


class _ScriptedSpark:
    """Recording SparkSession with scripted sql() responses.

    Each call to spark.sql(stmt) consumes the next entry in ``script``.
    Entry types:
      - int N           → return _FakeMergeResult(num_affected_rows=N)
      - Exception inst  → raise that exception
      - "noop"          → return MagicMock with collect=[]/first=None defaults
    """

    def __init__(self, script: Optional[List[Any]] = None) -> None:
        self.script: List[Any] = list(script or [])
        self.statements: List[str] = []
        self.conf = MagicMock()
        self.conf.set = MagicMock()

    def sql(self, statement: str) -> Any:
        self.statements.append(statement)
        if not self.script:
            # Default: no-op result for any unscripted call (e.g. __init__ DDL).
            r = MagicMock()
            r.collect.return_value = []
            r.first.return_value = None
            return r
        action = self.script.pop(0)
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, int):
            return _FakeMergeResult(num_affected_rows=action)
        if action == "noop":
            r = MagicMock()
            r.collect.return_value = []
            r.first.return_value = None
            return r
        raise AssertionError(f"unsupported script action: {action!r}")


def _make_wm(spark: _ScriptedSpark) -> Any:
    """Build a manager and discard any script/statement chatter from __init__.

    __init__ may run DDL via _ensure_table_exists which consumes script
    entries; tests intend their script to apply only to the method under
    test. Reset both lists after construction so test assertions see a
    clean slate.
    """
    from lhp_watermark import WatermarkManager

    pending_script = list(spark.script)
    spark.script.clear()  # __init__ DDL takes the no-op default path
    wm = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    spark.statements.clear()
    spark.script.extend(pending_script)
    return wm


def _valid_kwargs(**overrides: Any) -> dict:
    base = dict(
        run_id="job-1-task-2-attempt-3",
        source_system_id="postgres_prod",
        schema_name="Production",
        table_name="Product",
        watermark_column_name="ModifiedDate",
        watermark_value="2025-06-01 00:00:00",
        row_count=10,
        extraction_type="incremental",
    )
    base.update(overrides)
    return base


# ---------- happy path -------------------------------------------------------


def test_insert_new_issues_a_merge_and_returns_none_on_success() -> None:
    spark = _ScriptedSpark(script=[1])  # MERGE affects 1 row
    wm = _make_wm(spark)
    spark.statements.clear()

    result = wm.insert_new(**_valid_kwargs())

    assert result is None
    # Only the MERGE statement should be issued — no pre-check SELECT.
    select_stmts = [
        s
        for s in spark.statements
        if re.search(r"\bSELECT\b", s, re.IGNORECASE) and "MERGE" not in s.upper()
    ]
    assert select_stmts == [], (
        "insert_new must not issue a pre-check SELECT; the MERGE atomicity "
        "comes from num_affected_rows alone (FR-L-05). Found: " + repr(select_stmts)
    )
    merge_stmts = [
        s for s in spark.statements if re.search(r"\bMERGE\b", s, re.IGNORECASE)
    ]
    assert (
        len(merge_stmts) == 1
    ), f"expected exactly one MERGE, got {len(merge_stmts)}: {spark.statements}"


# ---------- duplicate detection ---------------------------------------------


def test_insert_new_raises_duplicate_run_error_when_merge_affects_zero_rows() -> None:
    from lhp_watermark import DuplicateRunError

    spark = _ScriptedSpark(script=[0])  # MERGE matched, no insert
    wm = _make_wm(spark)

    with pytest.raises(DuplicateRunError) as exc:
        wm.insert_new(**_valid_kwargs())
    assert exc.value.run_id == "job-1-task-2-attempt-3"
    assert exc.value.error_code == "LHP-WM-001"


# ---------- Delta concurrent retry loop -------------------------------------


def _delta_concurrent_append() -> Exception:
    """Simulate a Delta ConcurrentAppendException by name (Delta not installed)."""
    cls = type("ConcurrentAppendException", (Exception,), {})
    cls.__module__ = "io.delta.exceptions"
    return cls("commit failed: concurrent append")


def _delta_concurrent_delete_read() -> Exception:
    cls = type("ConcurrentDeleteReadException", (Exception,), {})
    cls.__module__ = "io.delta.exceptions"
    return cls("commit failed: concurrent delete-read")


def _py4j_wrapped(inner: Exception) -> Exception:
    """Simulate the Py4J wrapper Databricks surfaces in many runtimes."""
    cls = type("Py4JJavaError", (Exception,), {})
    cls.__module__ = "py4j.protocol"
    msg = f"An error occurred while calling something. Trace: {type(inner).__name__}: {inner}"
    return cls(msg)


def test_insert_new_retries_on_concurrent_append_then_succeeds(
    monkeypatch: Any,
) -> None:
    sleeps: List[float] = []
    monkeypatch.setattr("time.sleep", lambda s: sleeps.append(s))

    # First two attempts raise; third succeeds with affected=1.
    spark = _ScriptedSpark(
        script=[
            _delta_concurrent_append(),
            _delta_concurrent_delete_read(),
            1,
        ]
    )
    wm = _make_wm(spark)
    spark.statements.clear()

    result = wm.insert_new(**_valid_kwargs())

    assert result is None
    merge_stmts = [s for s in spark.statements if "MERGE" in s.upper()]
    assert len(merge_stmts) == 3, "two retries + one success = 3 MERGE attempts"
    # Exponential backoff: base 100ms, factor 2, ±50% jitter. Two sleeps
    # between attempts. The base values before jitter are 0.1 and 0.2.
    assert len(sleeps) == 2, f"expected sleeps between retries, got {sleeps}"
    assert 0.05 <= sleeps[0] <= 0.15
    assert 0.10 <= sleeps[1] <= 0.30


def test_insert_new_retries_on_py4j_wrapped_concurrent_exception(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr("time.sleep", lambda s: None)
    spark = _ScriptedSpark(
        script=[_py4j_wrapped(_delta_concurrent_append()), 1],
    )
    wm = _make_wm(spark)

    wm.insert_new(**_valid_kwargs())  # must not raise


def test_insert_new_raises_watermark_concurrency_error_on_budget_exhaustion(
    monkeypatch: Any,
) -> None:
    from lhp_watermark import WatermarkConcurrencyError

    monkeypatch.setattr("time.sleep", lambda s: None)
    final = _delta_concurrent_append()
    spark = _ScriptedSpark(
        script=[
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            _delta_concurrent_append(),
            final,
        ]
    )
    wm = _make_wm(spark)

    with pytest.raises(WatermarkConcurrencyError) as exc:
        wm.insert_new(**_valid_kwargs())

    assert exc.value.run_id == "job-1-task-2-attempt-3"
    assert exc.value.attempts == 5
    assert exc.value.error_code == "LHP-WM-004"
    # __cause__ must chain the underlying Delta exception (FR-L-05 + AC-SA-22).
    assert exc.value.__cause__ is final


def test_insert_new_does_not_retry_on_unrelated_exception(monkeypatch: Any) -> None:
    monkeypatch.setattr("time.sleep", lambda s: pytest.fail("must not sleep"))
    spark = _ScriptedSpark(script=[RuntimeError("syntax error in MERGE")])
    wm = _make_wm(spark)
    with pytest.raises(RuntimeError, match="syntax error"):
        wm.insert_new(**_valid_kwargs())


# ---------- input validation fires before spark.sql -------------------------


@pytest.mark.parametrize(
    "field, bad_value",
    [
        ("run_id", "job-1 DROP TABLE x --"),
        ("run_id", "' OR 1=1 --"),
        ("source_system_id", "x\x00null"),
        ("schema_name", "a" * 1000),  # over default max_len
        ("table_name", "tab\nle"),
    ],
)
def test_insert_new_rejects_adversarial_input_before_any_spark_sql(
    field: str, bad_value: str
) -> None:
    from lhp_watermark import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    spark.statements.clear()

    kwargs = _valid_kwargs(**{field: bad_value})
    with pytest.raises(WatermarkValidationError):
        wm.insert_new(**kwargs)
    # No spark.sql may have been issued by insert_new.
    assert (
        spark.statements == []
    ), f"validator must reject before SQL composition; statements: {spark.statements}"


def test_insert_new_rejects_float_watermark_value() -> None:
    from lhp_watermark import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    spark.statements.clear()

    with pytest.raises(WatermarkValidationError):
        wm.insert_new(**_valid_kwargs(watermark_value=3.14))
    assert spark.statements == []


def test_insert_new_accepts_decimal_and_int_watermark_values() -> None:
    from decimal import Decimal

    for value in [42, Decimal("99.99")]:
        spark = _ScriptedSpark(script=[1])
        wm = _make_wm(spark)
        wm.insert_new(**_valid_kwargs(watermark_value=value))


# ---------- defence in depth: no f-string of run_id into SQL ----------------


def test_run_id_is_emitted_via_sql_literal_with_doubled_quotes() -> None:
    """If a malicious operator forced a run_id past the validator (e.g. via
    a future signature change), sql_literal must still escape embedded
    single quotes so the MERGE remains syntactically intact.

    This test does not need to fire today — the validator already rejects
    values containing apostrophes — but pins the contract that future
    refactors must preserve.
    """
    from lhp_watermark.sql_safety import sql_literal

    assert sql_literal("o'reilly") == "'o''reilly'"


# ---------- Tier 2 load_group threading -------------------------------------


def test_insert_new_writes_load_group_into_merge_source_row() -> None:
    """Composite load_group lands in MERGE source row + INSERT column list."""
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    spark.statements.clear()

    wm.insert_new(**_valid_kwargs(load_group="pipe_a::fg_a"))

    merge = next(s for s in spark.statements if "MERGE" in s.upper())
    # Source row carries the literal under the load_group alias.
    assert re.search(
        r"'pipe_a::fg_a'\s+AS\s+load_group", merge, re.IGNORECASE
    ), f"MERGE source row missing load_group literal; SQL: {merge}"
    # INSERT column list includes load_group.
    insert_cols = re.search(
        r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\(([^)]+)\)",
        merge,
        re.IGNORECASE | re.DOTALL,
    )
    assert insert_cols is not None, f"could not find INSERT column list; SQL: {merge}"
    assert "load_group" in insert_cols.group(1), (
        f"INSERT column list must include load_group; got: {insert_cols.group(1)}"
    )
    # VALUES clause references s.load_group.
    assert re.search(
        r"s\.load_group", merge, re.IGNORECASE
    ), f"VALUES clause missing s.load_group reference; SQL: {merge}"


def test_insert_new_emits_null_load_group_when_caller_passes_none() -> None:
    """Legacy caller passes no ``load_group`` (default None) → SQL NULL.

    This is the back-compat path: existing flowgroups continue to write
    NULL-load_group rows until U3 threads the composite through.
    """
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    spark.statements.clear()

    wm.insert_new(**_valid_kwargs())  # load_group omitted

    merge = next(s for s in spark.statements if "MERGE" in s.upper())
    assert re.search(
        r"NULL\s+AS\s+load_group", merge, re.IGNORECASE
    ), f"load_group=None must emit NULL literal in source row; SQL: {merge}"


def test_insert_new_rejects_adversarial_load_group_before_any_sql() -> None:
    """``SQLInputValidator.string`` rejects control chars and injection
    vectors via the existing LHP-WM-003 validation surface."""
    from lhp_watermark import WatermarkValidationError

    spark = _ScriptedSpark(script=[])
    wm = _make_wm(spark)
    spark.statements.clear()

    with pytest.raises(WatermarkValidationError):
        wm.insert_new(**_valid_kwargs(load_group="bad\x00value"))
    assert spark.statements == [], (
        "validator must reject before SQL composition; "
        f"statements: {spark.statements}"
    )


def test_insert_new_load_group_emitted_via_sql_literal_with_doubled_quotes() -> None:
    """If a load_group somehow contained a single quote, ``sql_literal``
    must double it. The validator does not strip quotes, so this is a
    pure SQL-safety contract."""
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    spark.statements.clear()

    wm.insert_new(**_valid_kwargs(load_group="o'reilly::fg"))

    merge = next(s for s in spark.statements if "MERGE" in s.upper())
    assert "'o''reilly::fg'" in merge, (
        f"sql_literal must double embedded quotes in load_group; SQL: {merge}"
    )


# ---------- Tier 2 four-cell parametrize matrix -----------------------------


@pytest.mark.parametrize(
    "load_group",
    [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"],
)
def test_insert_new_load_group_matrix_threads_into_merge_source_row(
    load_group: Optional[str],
) -> None:
    """Every cell in the migration matrix renders the correct literal
    into the MERGE source row's ``load_group`` column:

    - ``None`` (legacy caller) → ``NULL AS load_group``
    - ``'legacy'`` (post-Step-3 backfill row identity)
    - ``'pipe_a::fg_a'``, ``'pipe_a::fg_b'`` (sibling B2 composites that
      collide on ``(schema, table)`` but isolate via load_group)

    The assertion shape is shared so a regression in any cell surfaces
    immediately rather than only in the cell currently exercised by a
    focused test.
    """
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    spark.statements.clear()

    wm.insert_new(**_valid_kwargs(load_group=load_group))

    merge = next(s for s in spark.statements if "MERGE" in s.upper())
    expected_literal = "NULL" if load_group is None else f"'{load_group}'"
    pattern = re.compile(
        re.escape(expected_literal) + r"\s+AS\s+load_group", re.IGNORECASE
    )
    assert pattern.search(merge), (
        f"load_group={load_group!r} must render as {expected_literal} "
        f"in MERGE source row; SQL: {merge}"
    )

    # INSERT column list always contains load_group regardless of value.
    insert_cols = re.search(
        r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\(([^)]+)\)",
        merge,
        re.IGNORECASE | re.DOTALL,
    )
    assert insert_cols is not None
    assert "load_group" in insert_cols.group(1), (
        f"INSERT column list must include load_group regardless of value; "
        f"got: {insert_cols.group(1)}"
    )
    # VALUES references s.load_group regardless of value.
    assert re.search(
        r"s\.load_group", merge, re.IGNORECASE
    ), f"VALUES clause missing s.load_group reference; SQL: {merge}"


@pytest.mark.parametrize(
    "load_group",
    [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"],
)
def test_insert_new_load_group_matrix_preserves_run_id_match_predicate(
    load_group: Optional[str],
) -> None:
    """Tier 2 (R3) is store-only on writes — the MERGE ``ON`` clause must
    keep matching by ``run_id`` only across every load_group cell. Adding
    a ``load_group`` axis to the match predicate would break L2 §5.3 and
    invite duplicate rows under cross-pipeline run_id collisions.
    """
    spark = _ScriptedSpark(script=[1])
    wm = _make_wm(spark)
    spark.statements.clear()

    wm.insert_new(**_valid_kwargs(load_group=load_group))

    merge = next(s for s in spark.statements if "MERGE" in s.upper())
    on_clause = re.search(
        r"\bON\b\s+(.+?)WHEN\s+", merge, re.IGNORECASE | re.DOTALL
    )
    assert on_clause is not None, f"could not locate MERGE ON clause; SQL: {merge}"
    on_text = on_clause.group(1)
    assert "run_id" in on_text, (
        f"MERGE ON clause must match by run_id; got: {on_text}"
    )
    assert "load_group" not in on_text, (
        f"MERGE ON clause must NOT match by load_group (store-only on writes); "
        f"got: {on_text}"
    )
