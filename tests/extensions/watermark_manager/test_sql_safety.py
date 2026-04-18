"""Tests for sql_safety: SQLInputValidator + SQL literal emitters.

Covers L2 FR-L-03 (SQL-Safe Watermark and Identifier Interpolation) and
AC-SA-07..AC-SA-13. Validators must reject adversarial input *before* any
SQL composition; emitters must produce dialect-correct, properly-escaped
fragments. Together they replace the deleted ``.replace("'", "")`` shortcut
that motivated this slice.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from lhp.extensions.watermark_manager import WatermarkValidationError
from lhp.extensions.watermark_manager.sql_safety import (
    SQLInputValidator,
    sql_identifier,
    sql_literal,
    sql_numeric_literal,
    sql_timestamp_literal,
)

# ---------- string validator -------------------------------------------------


class TestStringValidator:
    def test_accepts_normal_string(self) -> None:
        assert SQLInputValidator.string("hello world") == "hello world"

    @pytest.mark.parametrize(
        "value",
        [
            "abc\x00def",  # null byte
            "line1\nline2",  # newline (control char)
            "tab\there",  # tab (control char)
            "carriage\rreturn",  # CR
            "bell\x07",
            "esc\x1b",
        ],
    )
    def test_rejects_control_chars(self, value: str) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.string(value)
        assert "control" in exc.value.reason.lower()

    def test_rejects_overlength(self) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.string("x" * 1000, max_len=512)
        assert (
            "length" in exc.value.reason.lower()
            or "max_len" in exc.value.reason.lower()
        )

    def test_respects_custom_max_len(self) -> None:
        assert SQLInputValidator.string("x" * 100, max_len=200) == "x" * 100

    def test_rejects_non_string(self) -> None:
        with pytest.raises(WatermarkValidationError):
            SQLInputValidator.string(123)  # type: ignore[arg-type]


# ---------- numeric validator ------------------------------------------------


class TestNumericValidator:
    @pytest.mark.parametrize(
        "value", [0, 1, -1, 2**62, Decimal("1.5"), Decimal("-99.99")]
    )
    def test_accepts_int_and_decimal(self, value) -> None:
        assert SQLInputValidator.numeric(value) == value

    @pytest.mark.parametrize("value", [1.5, -0.0, float("1e10")])
    def test_rejects_float(self, value: float) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.numeric(value)
        assert "float" in exc.value.reason.lower()

    def test_rejects_decimal_nan(self) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.numeric(Decimal("NaN"))
        assert "nan" in exc.value.reason.lower()

    @pytest.mark.parametrize("value", [Decimal("Infinity"), Decimal("-Infinity")])
    def test_rejects_decimal_infinity(self, value: Decimal) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.numeric(value)
        assert "infinit" in exc.value.reason.lower()

    @pytest.mark.parametrize("value", ["1", None, True])
    def test_rejects_non_numeric(self, value) -> None:
        with pytest.raises(WatermarkValidationError):
            SQLInputValidator.numeric(value)  # type: ignore[arg-type]


# ---------- timestamp validator ----------------------------------------------


class TestTimestampValidator:
    def test_accepts_tz_aware_utc(self) -> None:
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert SQLInputValidator.timestamp(ts) == ts

    def test_accepts_tz_aware_non_utc(self) -> None:
        # FR-L-07 emitter normalises to UTC; validator accepts any tz-aware datetime.
        chi = timezone(timedelta(hours=-5))
        ts = datetime(2025, 6, 1, 7, 0, 0, tzinfo=chi)
        assert SQLInputValidator.timestamp(ts) == ts

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(WatermarkValidationError) as exc:
            SQLInputValidator.timestamp(datetime(2025, 6, 1, 12, 0, 0))
        assert "tz" in exc.value.reason.lower() or "naive" in exc.value.reason.lower()

    @pytest.mark.parametrize("value", ["2025-06-01", 1717245600, None])
    def test_rejects_non_datetime(self, value) -> None:
        with pytest.raises(WatermarkValidationError):
            SQLInputValidator.timestamp(value)  # type: ignore[arg-type]


# ---------- identifier validator ---------------------------------------------


class TestIdentifierValidator:
    @pytest.mark.parametrize(
        "value",
        ["users", "Production", "_underscore", "a1b2", "schema.table", "a.b.c"],
    )
    def test_accepts_bare_identifiers(self, value: str) -> None:
        assert SQLInputValidator.identifier(value) == value

    @pytest.mark.parametrize(
        "value",
        ['"Production"', '"Production"."Product"', '"a"."b"."c"'],
    )
    def test_accepts_prequoted_identifiers(self, value: str) -> None:
        assert SQLInputValidator.identifier(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "users; DROP TABLE x",
            "users--",
            "users' OR '1'='1",
            "1users",  # leading digit
            "us ers",  # space
            "users\x00",
            "",  # empty
            "schema..table",  # double dot
            ".table",
        ],
    )
    def test_rejects_invalid_identifiers(self, value: str) -> None:
        with pytest.raises(WatermarkValidationError):
            SQLInputValidator.identifier(value)


# ---------- uuid_or_job_run_id validator -------------------------------------


class TestRunIdValidator:
    @pytest.mark.parametrize(
        "value",
        [
            "550e8400-e29b-41d4-a716-446655440000",  # uuid v4
            "job-1-task-2-attempt-3",
            "job-1234567890-task-9876543210-attempt-1",
            "local-550e8400-e29b-41d4-a716-446655440000",  # L3 §7 OQ-4 fallback
        ],
    )
    def test_accepts_valid_run_ids(self, value: str) -> None:
        assert SQLInputValidator.uuid_or_job_run_id(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "job-1 DROP TABLE x --",
            "job-task-attempt",  # missing numbers
            "job-1-task-2",  # missing attempt
            "job-a-task-b-attempt-c",  # non-numeric
            "JOB-1-TASK-2-ATTEMPT-3",  # uppercase
            "' OR 1=1 --",
            "550e8400",  # truncated uuid
            "",
        ],
    )
    def test_rejects_invalid_run_ids(self, value: str) -> None:
        with pytest.raises(WatermarkValidationError):
            SQLInputValidator.uuid_or_job_run_id(value)


# ---------- emitters ---------------------------------------------------------


class TestSqlLiteralEmitter:
    def test_wraps_in_single_quotes(self) -> None:
        assert sql_literal("hello") == "'hello'"

    def test_doubles_embedded_single_quote(self) -> None:
        assert sql_literal("o'reilly") == "'o''reilly'"

    def test_doubles_multiple_embedded_quotes(self) -> None:
        assert sql_literal("a'b'c") == "'a''b''c'"

    def test_empty_string(self) -> None:
        assert sql_literal("") == "''"


class TestSqlNumericLiteralEmitter:
    @pytest.mark.parametrize("value, expected", [(0, "0"), (42, "42"), (-7, "-7")])
    def test_int(self, value: int, expected: str) -> None:
        assert sql_numeric_literal(value) == expected

    def test_decimal(self) -> None:
        assert sql_numeric_literal(Decimal("3.14")) == "3.14"


class TestSqlTimestampLiteralEmitter:
    def test_emits_iso8601_utc_timestamp_literal(self) -> None:
        ts = datetime(2025, 6, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
        out = sql_timestamp_literal(ts)
        assert out.startswith("TIMESTAMP '")
        assert out.endswith("'")
        # Microsecond precision + +00:00 UTC suffix
        assert "2025-06-01T12:00:00.123456+00:00" in out

    def test_normalises_non_utc_to_utc(self) -> None:
        chi = timezone(timedelta(hours=-5))
        ts = datetime(2025, 6, 1, 7, 0, 0, tzinfo=chi)  # = 2025-06-01T12:00:00Z
        out = sql_timestamp_literal(ts)
        assert "2025-06-01T12:00:00" in out
        assert "+00:00" in out

    def test_rejects_naive_datetime(self) -> None:
        with pytest.raises(WatermarkValidationError):
            sql_timestamp_literal(datetime(2025, 6, 1))


class TestSqlIdentifierEmitter:
    def test_quotes_bare_identifier(self) -> None:
        # Default Spark identifier quoting uses backticks; document the contract.
        assert sql_identifier("users") == "`users`"

    def test_quotes_dotted_identifier_each_segment(self) -> None:
        assert sql_identifier("schema.table") == "`schema`.`table`"

    def test_passes_prequoted_through_verbatim(self) -> None:
        assert sql_identifier('"Production"."Product"') == '"Production"."Product"'

    def test_rejects_invalid_identifier(self) -> None:
        with pytest.raises(WatermarkValidationError):
            sql_identifier("users; DROP TABLE x")


# ---------- defence-in-depth: no replace-quote shortcut ----------------------


def test_no_replace_quote_shortcut_in_extensions_package() -> None:
    """AC-SA-13 (light static check). The .replace(\"'\", \"\") shortcut from the
    pre-Slice-A code silently corrupts legitimate input; it must not creep
    back into the package.
    """
    import pathlib

    pkg_root = (
        pathlib.Path(__file__).resolve().parents[3]
        / "src"
        / "lhp"
        / "extensions"
        / "watermark_manager"
    )
    needle = '.replace("\'", "")'
    offenders = []
    for py in pkg_root.rglob("*.py"):
        if needle in py.read_text(encoding="utf-8"):
            offenders.append(str(py))
    assert not offenders, f"Forbidden replace-quote shortcut found in: {offenders}"
