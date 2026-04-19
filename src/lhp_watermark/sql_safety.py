"""SQL safety primitives for watermark_manager (L2 FR-L-03, AC-SA-07..14).

Two layers cooperate to prevent SQL injection in the watermark code paths:

1. ``SQLInputValidator`` — typed validators that reject adversarial input
   *before* SQL composition. Each method accepts the raw value, returns the
   validated value unchanged, or raises ``WatermarkValidationError``.
2. Emitter functions — ``sql_literal``, ``sql_numeric_literal``,
   ``sql_timestamp_literal``, ``sql_identifier`` — produce dialect-correct,
   properly-escaped SQL fragments. Callers must pass values through the
   matching validator first; emitters do *not* re-validate (so a defensive
   call site cannot accidentally swallow a validator error by wrapping it).

The pre-Slice-A code stripped single quotes from inputs as a "sanitiser".
That shortcut is forbidden going forward (see
test_no_replace_quote_shortcut_in_extensions_package): stripping quotes
does not prevent injection and silently corrupts legitimate input.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Union

from lhp_watermark.exceptions import WatermarkValidationError

# Bare identifier:  word [ . word ]*   where word matches Python-ident-like rules.
_IDENT_BARE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
# Pre-quoted:       "..."  [ . "..." ]*    no embedded double-quote characters.
_IDENT_QUOTED = re.compile(r'^"[^"]+"(\."[^"]+")*$')

# Run-id taxonomy:
_UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
_LOCAL_UUID = re.compile(
    r"^local-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)
_JOB_RUN_ID = re.compile(r"^job-\d+-task-\d+-attempt-\d+$")


def _bool_is_int_subtype() -> bool:
    """``isinstance(True, int)`` is True in Python — guard explicitly elsewhere."""
    return isinstance(True, int)


class SQLInputValidator:
    """Per-type input validators (FR-L-03). Static-method-only by design."""

    @staticmethod
    def string(value: Any, max_len: int = 512) -> str:
        if not isinstance(value, str):
            raise WatermarkValidationError(
                field="string",
                value=value,
                reason=f"expected str, got {type(value).__name__}",
            )
        if len(value) > max_len:
            raise WatermarkValidationError(
                field="string",
                value=value,
                reason=f"length {len(value)} exceeds max_len {max_len}",
            )
        # Reject any control character (C0 + DEL). Legitimate watermark strings
        # do not contain control chars; rejecting them eliminates a class of
        # log-injection and SQL-comment bypass tricks.
        for ch in value:
            if ord(ch) < 0x20 or ord(ch) == 0x7F:
                raise WatermarkValidationError(
                    field="string",
                    value=value,
                    reason=f"contains control character U+{ord(ch):04X}",
                )
        return value

    @staticmethod
    def numeric(value: Any) -> Union[int, Decimal]:
        # bool is an int subtype; reject explicitly so True/False can't sneak in.
        if isinstance(value, bool):
            raise WatermarkValidationError(
                field="numeric",
                value=value,
                reason="bool is not a valid numeric watermark",
            )
        if isinstance(value, float):
            raise WatermarkValidationError(
                field="numeric",
                value=value,
                reason="float is not accepted (use Decimal for fractional values)",
            )
        if isinstance(value, int):
            return value
        if isinstance(value, Decimal):
            if value.is_nan():
                raise WatermarkValidationError(
                    field="numeric",
                    value=value,
                    reason="Decimal NaN is not a valid watermark",
                )
            if value.is_infinite():
                raise WatermarkValidationError(
                    field="numeric",
                    value=value,
                    reason="Decimal Infinity is not a valid watermark",
                )
            return value
        raise WatermarkValidationError(
            field="numeric",
            value=value,
            reason=f"expected int or Decimal, got {type(value).__name__}",
        )

    @staticmethod
    def timestamp(value: Any) -> datetime:
        if not isinstance(value, datetime):
            raise WatermarkValidationError(
                field="timestamp",
                value=value,
                reason=f"expected datetime, got {type(value).__name__}",
            )
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            raise WatermarkValidationError(
                field="timestamp",
                value=value,
                reason="datetime must be tz-aware (naive datetimes are forbidden)",
            )
        return value

    @staticmethod
    def identifier(value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise WatermarkValidationError(
                field="identifier", value=value, reason="must be a non-empty str"
            )
        if _IDENT_BARE.match(value) or _IDENT_QUOTED.match(value):
            return value
        raise WatermarkValidationError(
            field="identifier",
            value=value,
            reason="must match bare ident or pre-quoted form",
        )

    @staticmethod
    def uuid_or_job_run_id(value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise WatermarkValidationError(
                field="run_id", value=value, reason="must be a non-empty str"
            )
        if (
            _UUID_V4.match(value)
            or _LOCAL_UUID.match(value)
            or _JOB_RUN_ID.match(value)
        ):
            return value
        raise WatermarkValidationError(
            field="run_id",
            value=value,
            reason="must be UUID-v4, local-<uuid>, or job-N-task-N-attempt-N",
        )


# ---------- emitters ---------------------------------------------------------


def sql_literal(value: str) -> str:
    """Wrap value in single quotes; double any embedded single quote.

    Caller must have passed value through ``SQLInputValidator.string`` (or
    equivalent) first.
    """
    if not isinstance(value, str):
        raise WatermarkValidationError(
            field="sql_literal",
            value=value,
            reason=f"expected str, got {type(value).__name__}",
        )
    return "'" + value.replace("'", "''") + "'"


def sql_numeric_literal(value: Union[int, Decimal]) -> str:
    """Emit a bare numeric SQL literal."""
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise WatermarkValidationError(
            field="sql_numeric_literal",
            value=value,
            reason=f"expected int or Decimal, got {type(value).__name__}",
        )
    return str(value)


def sql_timestamp_literal(value: datetime) -> str:
    """Emit ``TIMESTAMP 'YYYY-MM-DDTHH:MM:SS.ffffff+00:00'`` in UTC.

    Normalises non-UTC tz-aware datetimes to UTC (FR-L-07). Naive datetimes
    are rejected via ``SQLInputValidator.timestamp`` (re-applied here so the
    emitter is safe to call directly in template/runtime code).
    """
    SQLInputValidator.timestamp(value)
    utc_value = value.astimezone(timezone.utc)
    return f"TIMESTAMP '{utc_value.isoformat(timespec='microseconds')}'"


def sql_identifier(value: str) -> str:
    """Emit a quoted identifier.

    Pre-quoted identifiers (matching ``"a"."b"``) pass through verbatim so
    callers can supply explicit case-sensitive identifiers (e.g. Postgres-style
    ``"Production"."Product"``). Bare identifiers (matching the dotted-word
    grammar) are wrapped segment-by-segment in backticks (Spark default).
    """
    SQLInputValidator.identifier(value)
    if _IDENT_QUOTED.match(value):
        return value
    return ".".join(f"`{seg}`" for seg in value.split("."))


__all__ = [
    "SQLInputValidator",
    "sql_literal",
    "sql_numeric_literal",
    "sql_timestamp_literal",
    "sql_identifier",
]
