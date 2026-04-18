"""Watermark manager exception taxonomy (L2 §5.2, AC-SA-42).

Four typed exceptions cover the watermark manager's failure surface. All
inherit ``LHPError`` so existing error-handling infrastructure (the CLI
error boundary, logging) continues to format them. ``error_code`` is a
class attribute so callers can dispatch on type without instantiating.

``__str__`` is overridden to return a single-line message. The watermark
template propagates exceptions via
``mark_failed(error_class=type(e).__name__, error_message=str(e)[:4096])``;
a multi-line message would corrupt the audit trail and is harder to scan
in observability dashboards.
"""

from __future__ import annotations

from typing import Optional

from lhp.utils.error_formatter import ErrorCategory, LHPError


class _WatermarkError(LHPError):
    """Internal base for the four LHP-WM-* exceptions.

    Wires the LHPError constructor so subclasses only need to supply a
    short title and a context dict.
    """

    error_code: str = ""  # overridden by subclass
    code_number: str = ""  # overridden by subclass
    title: str = ""  # overridden by subclass

    def __init__(self, *, details: str, context: Optional[dict] = None) -> None:
        super().__init__(
            category=ErrorCategory.WATERMARK,
            code_number=self.code_number,
            title=self.title,
            details=details,
            context=context or {},
        )
        self._compact = self._build_compact_message(context or {})

    def _build_compact_message(self, context: dict) -> str:
        """Single-line form: ``[LHP-WM-NNN] <Title>: k1=v1 k2=v2``."""
        parts = " ".join(f"{k}={v!r}" for k, v in context.items())
        body = f": {parts}" if parts else ""
        return f"[{self.error_code}] {self.title}{body}"

    def __str__(self) -> str:  # noqa: D401
        return self._compact


class DuplicateRunError(_WatermarkError):
    """Raised when ``insert_new`` detects an existing row for the run_id (LHP-WM-001)."""

    error_code = "LHP-WM-001"
    code_number = "001"
    title = "Duplicate run_id"

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        super().__init__(
            details=(
                f"A watermark row for run_id={run_id!r} already exists. "
                "MERGE matched an existing target row, so the insert was a no-op."
            ),
            context={"run_id": run_id},
        )


class TerminalStateGuardError(_WatermarkError):
    """Raised when a state-transition method refuses to overwrite a terminal state (LHP-WM-002)."""

    error_code = "LHP-WM-002"
    code_number = "002"
    title = "Terminal state guard"

    def __init__(self, run_id: str, current_status: Optional[str]) -> None:
        self.run_id = run_id
        self.current_status = current_status
        super().__init__(
            details=(
                f"run_id={run_id!r} is currently in status={current_status!r}; "
                "the requested transition would overwrite a terminal state and "
                "was refused at the WHERE-clause guard."
            ),
            context={"run_id": run_id, "current_status": current_status},
        )


class WatermarkValidationError(_WatermarkError):
    """Raised by SQLInputValidator when an input fails the per-type rule (LHP-WM-003)."""

    error_code = "LHP-WM-003"
    code_number = "003"
    title = "Watermark input validation failed"

    def __init__(self, field: str, value: object, reason: str) -> None:
        self.field = field
        self.value = value
        self.reason = reason
        super().__init__(
            details=(
                f"Field {field!r} rejected by SQLInputValidator: {reason}. "
                "Inputs to SQL composition must pass the typed validator before "
                "the emitter is called."
            ),
            context={"field": field, "value": value, "reason": reason},
        )


class WatermarkConcurrencyError(_WatermarkError):
    """Raised when the Delta concurrent-commit retry budget is exhausted (LHP-WM-004)."""

    error_code = "LHP-WM-004"
    code_number = "004"
    title = "Watermark concurrency retry exhausted"

    def __init__(self, run_id: str, attempts: int) -> None:
        self.run_id = run_id
        self.attempts = attempts
        super().__init__(
            details=(
                f"insert_new for run_id={run_id!r} exhausted its retry budget "
                f"after {attempts} attempts against Delta concurrent-commit "
                "exceptions. Reduce shard size; do not raise the budget."
            ),
            context={"run_id": run_id, "attempts": attempts},
        )


__all__ = [
    "DuplicateRunError",
    "TerminalStateGuardError",
    "WatermarkValidationError",
    "WatermarkConcurrencyError",
]
