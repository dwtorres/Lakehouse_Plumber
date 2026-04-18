"""Unit + CLI tests for the Slice A orphan-run backfill migration.

Task 13 per slice-A-lifecycle PLAN.md. Covers AC-SA-36..39.

Tests drive a ``FakeSpark`` stand-in rather than a real SparkSession so
they run in the standard unit-test environment without Databricks or
Delta. The fake records every SQL call and routes per-query mock
responses via caller-supplied predicates so each test can stub precisely
the statements it cares about and leave everything else defaulted.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

# Exit codes per L3 §4.4.
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_SOFT_FAIL = 2


# --------------------------- fakes ----------------------------------


class _FakeRow(dict):
    """Read-only mapping that supports ``row["col"]`` Spark-style access."""

    def __getitem__(self, key: Any) -> Any:  # type: ignore[override]
        return dict.__getitem__(self, key)


class _FakeResult:
    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self._rows = [_FakeRow(r) for r in (rows or [])]

    def collect(self) -> List[_FakeRow]:
        return list(self._rows)

    def first(self) -> Optional[_FakeRow]:
        return self._rows[0] if self._rows else None


class _FakeConf:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, str]] = []

    def set(self, key: str, value: str) -> None:
        self.calls.append((key, value))


class FakeSpark:
    """Predicate-routed mock of a Spark session.

    ``on(predicate, response)`` registers a (lambda query: bool,
    response) pair evaluated in order; the first match wins. Unmatched
    queries return an empty ``_FakeResult``.
    """

    def __init__(self) -> None:
        self.conf = _FakeConf()
        self.queries: List[str] = []
        self._handlers: List[
            Tuple[Callable[[str], bool], Callable[[str], _FakeResult]]
        ] = []

    def on(
        self,
        predicate: Callable[[str], bool],
        response: Any,
    ) -> None:
        if not callable(response):
            rows = response
            resolver: Callable[[str], _FakeResult] = lambda _q: _FakeResult(rows)
        else:
            resolver = lambda q: _FakeResult(response(q))
        self._handlers.append((predicate, resolver))

    def sql(self, query: str) -> _FakeResult:
        self.queries.append(query)
        for pred, resp in self._handlers:
            if pred(query):
                return resp(query)
        return _FakeResult()


def _orphan_rows_handler(
    rows: List[Dict[str, Any]],
) -> Callable[[str], List[Dict[str, Any]]]:
    """Return a handler that serves ``rows`` then ``[]`` on subsequent calls.

    Mimics the idempotent second-run contract: after the migration UPDATEs
    the rows the next SELECT for status='running' returns empty.
    """
    state = {"first": True}

    def _inner(_q: str) -> List[Dict[str, Any]]:
        if state["first"]:
            state["first"] = False
            return rows
        return []

    return _inner


# ---------------------- import + module shape -----------------------


class TestImportAndShape:
    def test_module_importable(self):
        """AC-SA-36: script imports as a module."""
        from lhp.extensions.migrations import slice_a_backfill  # noqa: F401

        assert hasattr(slice_a_backfill, "main")
        assert hasattr(slice_a_backfill, "run_migration")

    def test_exit_codes_declared(self):
        from lhp.extensions.migrations import slice_a_backfill as sab

        assert sab.EXIT_OK == 0
        assert sab.EXIT_ERROR == 1
        assert sab.EXIT_SOFT_FAIL == 2


# ---------------------- CLI surface ---------------------------------


class TestCLI:
    def test_help_lists_all_flags(self):
        """AC-SA-39: --help lists every flag + exit codes."""
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "lhp.extensions.migrations.slice_a_backfill",
                "--help",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        out = result.stdout
        for flag in (
            "--wm-catalog",
            "--wm-schema",
            "--bronze-catalog",
            "--bronze-schema",
            "--bronze-max-view",
            "--assume-empty",
            "--dry-run",
        ):
            assert flag in out, f"Flag {flag} missing from --help"
        assert "Exit codes" in out or "exit codes" in out.lower()


# ---------------------- run_migration core --------------------------


def _run(spark, **overrides):
    """Invoke run_migration with defaults that the tests can override."""
    from lhp.extensions.migrations.slice_a_backfill import run_migration

    kwargs: Dict[str, Any] = dict(
        wm_catalog="metadata",
        wm_schema="orchestration",
    )
    kwargs.update(overrides)
    return run_migration(spark, **kwargs)


def _sample_orphan(
    run_id: str = "job-1-task-1-attempt-1",
    sys_id: str = "pg_crm",
    schema: str = "Production",
    table: str = "Product",
    watermark_value: str = "2025-06-01T00:00:00+00:00",
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "source_system_id": sys_id,
        "schema_name": schema,
        "table_name": table,
        "watermark_value": watermark_value,
        "watermark_time": "2025-06-01 00:00:00",
        "watermark_column_name": "ModifiedDate",
    }


class TestEmptyTable:
    def test_no_orphans_logs_and_exits_clean(self, caplog):
        """AC-SA-36: idempotent second run reports no orphans; promoted=0."""
        spark = FakeSpark()
        spark.on(
            lambda q: "status = 'running'" in q.lower()
            or "status='running'" in q.lower(),
            [],
        )
        caplog.set_level(
            logging.INFO, logger="lhp.extensions.migrations.slice_a_backfill"
        )

        result = _run(spark, assume_empty=True)

        assert result.promoted == 0
        assert result.held == 0
        assert any("no orphans" in rec.message.lower() for rec in caplog.records), [
            rec.message for rec in caplog.records
        ]


class TestAssumeEmpty:
    def test_assume_empty_promotes_without_bronze_query(self):
        """AC-SA-39: --assume-empty skips bronze verification."""
        spark = FakeSpark()
        orphan = _sample_orphan()
        spark.on(
            lambda q: "status = 'running'" in q.lower()
            or "status='running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )

        result = _run(spark, assume_empty=True)

        assert result.promoted == 1
        assert result.held == 0
        # No bronze_max query should have been issued.
        assert not any(
            "bronze_max_watermark" in q.lower() for q in spark.queries
        ), spark.queries


class TestBronzeVerificationPromotes:
    def test_orphan_with_matching_bronze_is_promoted(self):
        """AC-SA-37: orphan whose bronze_max >= watermark_value - 1s promotes."""
        spark = FakeSpark()
        orphan = _sample_orphan()
        spark.on(
            lambda q: "from metadata.orchestration.watermarks" in q.lower()
            and "status = 'running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )
        # Bronze verification returns a matching max.
        spark.on(
            lambda q: "bronze_max_watermark" in q.lower(),
            [{"bronze_max_watermark": "2025-06-01T00:00:01+00:00"}],
        )

        result = _run(
            spark,
            bronze_max_view="metadata.bronze_views.max_watermark",
        )

        assert result.promoted == 1
        assert result.held == 0
        # UPDATE emits migration_note + status='completed'.
        updates = [
            q
            for q in spark.queries
            if "update metadata.orchestration.watermarks" in q.lower()
            and "status = 'completed'" in q.lower()
        ]
        assert updates, spark.queries
        assert "migration_note" in updates[0].lower()
        assert "slice-a-backfill" in updates[0].lower()


class TestBronzeVerificationHolds:
    def test_orphan_with_null_bronze_is_held(self, caplog):
        """AC-SA-38: bronze_max IS NULL → hold with WARNING."""
        spark = FakeSpark()
        orphan = _sample_orphan()
        spark.on(
            lambda q: "status = 'running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )
        spark.on(
            lambda q: "bronze_max_watermark" in q.lower(),
            [{"bronze_max_watermark": None}],
        )
        caplog.set_level(
            logging.WARNING, logger="lhp.extensions.migrations.slice_a_backfill"
        )

        result = _run(
            spark,
            bronze_max_view="metadata.bronze_views.max_watermark",
        )

        assert result.promoted == 0
        assert result.held == 1
        # No UPDATE emitted for the held orphan.
        updates = [
            q
            for q in spark.queries
            if "update metadata.orchestration.watermarks" in q.lower()
            and "status = 'completed'" in q.lower()
        ]
        assert not updates, spark.queries
        # Warning message names the run.
        assert any("not promoted" in rec.message.lower() for rec in caplog.records), [
            rec.message for rec in caplog.records
        ]

    def test_orphan_with_older_bronze_is_held(self):
        """bronze_max older than watermark_value by > 1s → hold."""
        spark = FakeSpark()
        orphan = _sample_orphan(watermark_value="2025-06-01T00:00:10+00:00")
        spark.on(
            lambda q: "status = 'running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )
        # 2 seconds behind the orphan's watermark → fails 1-second skew tolerance.
        spark.on(
            lambda q: "bronze_max_watermark" in q.lower(),
            [{"bronze_max_watermark": "2025-06-01T00:00:08+00:00"}],
        )

        result = _run(
            spark,
            bronze_max_view="metadata.bronze_views.max_watermark",
        )

        assert result.promoted == 0
        assert result.held == 1


class TestIdempotency:
    def test_second_run_is_noop(self):
        """AC-SA-36: running after a clean migration reports zero orphans."""
        spark = FakeSpark()
        spark.on(
            lambda q: "status = 'running'" in q.lower(),
            [],
        )

        first = _run(spark, assume_empty=True)
        second = _run(spark, assume_empty=True)

        assert first.promoted == 0 and first.held == 0
        assert second.promoted == 0 and second.held == 0


class TestDryRun:
    def test_dry_run_does_not_issue_updates(self):
        """--dry-run reports decisions without writing."""
        spark = FakeSpark()
        orphan = _sample_orphan()
        spark.on(
            lambda q: "status = 'running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )

        result = _run(spark, assume_empty=True, dry_run=True)

        assert result.promoted == 1
        # No completion UPDATE while in dry-run.
        updates = [
            q
            for q in spark.queries
            if "update metadata.orchestration.watermarks" in q.lower()
            and "status = 'completed'" in q.lower()
        ]
        assert not updates, spark.queries


# ---------------------- main() exit codes ---------------------------


class TestMainExitCodes:
    def test_main_exit_zero_when_no_orphans(self, monkeypatch):
        from lhp.extensions.migrations import slice_a_backfill as sab

        spark = FakeSpark()
        spark.on(lambda q: "status = 'running'" in q.lower(), [])
        monkeypatch.setattr(sab, "_get_spark", lambda: spark)

        code = sab.main(
            [
                "--wm-catalog",
                "metadata",
                "--wm-schema",
                "orchestration",
                "--assume-empty",
            ]
        )
        assert code == EXIT_OK

    def test_main_exit_two_when_some_held(self, monkeypatch):
        from lhp.extensions.migrations import slice_a_backfill as sab

        spark = FakeSpark()
        orphan = _sample_orphan()
        spark.on(
            lambda q: "status = 'running'" in q.lower(),
            _orphan_rows_handler([orphan]),
        )
        spark.on(
            lambda q: "bronze_max_watermark" in q.lower(),
            [{"bronze_max_watermark": None}],
        )
        monkeypatch.setattr(sab, "_get_spark", lambda: spark)

        code = sab.main(
            [
                "--wm-catalog",
                "metadata",
                "--wm-schema",
                "orchestration",
                "--bronze-max-view",
                "metadata.bronze_views.max_watermark",
            ]
        )
        assert code == EXIT_SOFT_FAIL

    def test_main_exit_one_on_unrecoverable_error(self, monkeypatch):
        from lhp.extensions.migrations import slice_a_backfill as sab

        class Blowup:
            conf = _FakeConf()

            def sql(self, _q):
                raise RuntimeError("connection refused")

        monkeypatch.setattr(sab, "_get_spark", lambda: Blowup())

        code = sab.main(
            [
                "--wm-catalog",
                "metadata",
                "--wm-schema",
                "orchestration",
                "--assume-empty",
            ]
        )
        assert code == EXIT_ERROR
