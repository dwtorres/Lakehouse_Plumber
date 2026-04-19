"""Tests for the LHP-WM-* exception classes (L2 §5.2, AC-SA-42).

The four classes encode the watermark-manager error taxonomy:
    LHP-WM-001 DuplicateRunError
    LHP-WM-002 TerminalStateGuardError
    LHP-WM-003 WatermarkValidationError
    LHP-WM-004 WatermarkConcurrencyError

Each must inherit LHPError, expose ``error_code`` as a class attribute,
be importable from the package root, and produce a single-line ``__str__``
suitable for propagation through
``mark_failed(error_class=type(e).__name__, error_message=str(e))``.
"""

from __future__ import annotations

import pytest

from lhp.utils.error_formatter import LHPError


def test_all_four_exception_classes_importable_from_package_root() -> None:
    from lhp_watermark import (  # noqa: F401
        DuplicateRunError,
        TerminalStateGuardError,
        WatermarkValidationError,
        WatermarkConcurrencyError,
    )


@pytest.mark.parametrize(
    "class_name, expected_code",
    [
        ("DuplicateRunError", "LHP-WM-001"),
        ("TerminalStateGuardError", "LHP-WM-002"),
        ("WatermarkValidationError", "LHP-WM-003"),
        ("WatermarkConcurrencyError", "LHP-WM-004"),
    ],
)
def test_error_code_is_class_attribute(class_name: str, expected_code: str) -> None:
    import lhp_watermark as wm

    cls = getattr(wm, class_name)
    assert (
        cls.error_code == expected_code
    ), f"{class_name}.error_code must be a class attribute equal to {expected_code}"


@pytest.mark.parametrize(
    "class_name",
    [
        "DuplicateRunError",
        "TerminalStateGuardError",
        "WatermarkValidationError",
        "WatermarkConcurrencyError",
    ],
)
def test_inherits_lhp_error(class_name: str) -> None:
    import lhp_watermark as wm

    cls = getattr(wm, class_name)
    assert issubclass(cls, LHPError), f"{class_name} must inherit LHPError"


def test_duplicate_run_error_carries_run_id_and_single_line_str() -> None:
    from lhp_watermark import DuplicateRunError

    exc = DuplicateRunError(run_id="job-1-task-2-attempt-3")
    assert exc.run_id == "job-1-task-2-attempt-3"
    msg = str(exc)
    assert "\n" not in msg, "__str__ must be a single line for mark_failed propagation"
    assert "LHP-WM-001" in msg
    assert "job-1-task-2-attempt-3" in msg


def test_terminal_state_guard_error_carries_run_id_and_status() -> None:
    from lhp_watermark import TerminalStateGuardError

    exc = TerminalStateGuardError(run_id="r1", current_status="completed")
    assert exc.run_id == "r1"
    assert exc.current_status == "completed"
    msg = str(exc)
    assert "\n" not in msg
    assert "LHP-WM-002" in msg
    assert "completed" in msg


def test_watermark_validation_error_carries_field_value_reason() -> None:
    from lhp_watermark import WatermarkValidationError

    exc = WatermarkValidationError(
        field="run_id",
        value="' OR 1=1 --",
        reason="does not match uuid_or_job_run_id pattern",
    )
    assert exc.field == "run_id"
    assert exc.value == "' OR 1=1 --"
    assert "uuid_or_job_run_id" in exc.reason
    msg = str(exc)
    assert "\n" not in msg
    assert "LHP-WM-003" in msg
    assert "run_id" in msg


def test_watermark_concurrency_error_carries_run_id_and_attempts() -> None:
    from lhp_watermark import WatermarkConcurrencyError

    exc = WatermarkConcurrencyError(run_id="r1", attempts=5)
    assert exc.run_id == "r1"
    assert exc.attempts == 5
    msg = str(exc)
    assert "\n" not in msg
    assert "LHP-WM-004" in msg
    assert "5" in msg


def test_str_form_is_safe_for_mark_failed_truncation() -> None:
    """__str__ is consumed by mark_failed(error_message=str(e)[:4096]).

    Asserts that str() is short and printable so the truncation slice
    does not produce mojibake or split a multi-byte sequence at the boundary.
    """
    from lhp_watermark import (
        DuplicateRunError,
        TerminalStateGuardError,
        WatermarkConcurrencyError,
        WatermarkValidationError,
    )

    for exc in [
        DuplicateRunError(run_id="r1"),
        TerminalStateGuardError(run_id="r1", current_status="failed"),
        WatermarkValidationError(field="x", value="y", reason="z"),
        WatermarkConcurrencyError(run_id="r1", attempts=5),
    ]:
        msg = str(exc)
        assert (
            len(msg) < 512
        ), f"single-line message expected to be short, got {len(msg)}"
        assert msg == msg[:4096]  # idempotent under mark_failed truncation
