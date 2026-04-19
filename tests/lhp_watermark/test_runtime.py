"""Tests for ``lhp_watermark.runtime.derive_run_id``.

Covers FR-L-09 / L3 §4.2.4 / AC-SA-32..34:

* Widget ``lhp_run_id_override`` wins over Jobs context and is validated.
* Jobs context path returns ``job-{jobRunId}-task-{taskRunId}-attempt-{N}``.
* Fallback returns ``local-<uuid4>`` and logs a WARNING flagging
  NON-PRODUCTION use.

Tests mock ``dbutils`` so they run without a live Databricks runtime.
"""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from lhp_watermark import derive_run_id
from lhp_watermark.exceptions import WatermarkValidationError

_RUNTIME_LOGGER = "lhp_watermark.runtime"
_LOCAL_UUID_RE = re.compile(r"^local-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _make_dbutils(
    *,
    widget: Optional[str] = "",
    widget_raises: bool = False,
    context_raises: bool = False,
    job_run_id: Optional[str] = None,
    task_run_id: Optional[str] = None,
    attempt: Optional[str] = None,
) -> Any:
    """Build a mock dbutils.

    * ``widget=""`` simulates "widget declared but empty".
    * ``widget="value"`` simulates "widget set to value".
    * ``widget_raises=True`` simulates "widget not defined" (raises).
    * ``context_raises=True`` simulates "no Jobs context" (raises).
    * ``job_run_id=None`` simulates accessor returning None.
    """
    dbutils = MagicMock()

    if widget_raises:
        dbutils.widgets.get.side_effect = Exception("InputWidgetNotDefined")
    else:
        dbutils.widgets.get.return_value = widget

    ctx_chain = dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value
    if context_raises:
        ctx_chain.getContext.side_effect = Exception("no Jobs context")
    else:
        ctx = MagicMock()
        ctx.jobRunId.return_value = job_run_id
        ctx.taskRunId.return_value = task_run_id
        ctx.currentRunAttempt.return_value = attempt
        ctx_chain.getContext.return_value = ctx

    return dbutils


# -------------------- Jobs-context path (AC-SA-32) --------------------


class TestJobsContextPath:
    def test_returns_job_format_when_accessors_succeed(self, caplog):
        dbutils = _make_dbutils(
            widget_raises=True,
            job_run_id="12345",
            task_run_id="67890",
            attempt="2",
        )
        with caplog.at_level(logging.WARNING, logger=_RUNTIME_LOGGER):
            result = derive_run_id(dbutils)
        assert result == "job-12345-task-67890-attempt-2"
        assert [r for r in caplog.records if r.name == _RUNTIME_LOGGER] == []

    def test_empty_widget_falls_through_to_jobs_context(self):
        dbutils = _make_dbutils(
            widget="",
            job_run_id="1",
            task_run_id="2",
            attempt="1",
        )
        assert derive_run_id(dbutils) == "job-1-task-2-attempt-1"

    def test_undefined_widget_falls_through_to_jobs_context(self):
        dbutils = _make_dbutils(
            widget_raises=True,
            job_run_id="1",
            task_run_id="2",
            attempt="1",
        )
        assert derive_run_id(dbutils) == "job-1-task-2-attempt-1"


# -------------------- Widget-override path (AC-SA-34) -----------------


class TestWidgetOverridePath:
    def test_widget_override_wins_over_jobs_context(self, caplog):
        override = str(uuid.uuid4())
        dbutils = _make_dbutils(
            widget=override,
            job_run_id="12345",
            task_run_id="67890",
            attempt="2",
        )
        with caplog.at_level(logging.WARNING, logger=_RUNTIME_LOGGER):
            result = derive_run_id(dbutils)
        assert result == override
        messages = [r.message for r in caplog.records if r.name == _RUNTIME_LOGGER]
        assert any("override widget" in m for m in messages), messages

    def test_widget_override_accepts_job_run_id_form(self):
        override = "job-99-task-88-attempt-1"
        dbutils = _make_dbutils(widget=override)
        assert derive_run_id(dbutils) == override

    def test_widget_override_invalid_raises_validation_error(self):
        dbutils = _make_dbutils(widget="'; DROP TABLE t;--")
        with pytest.raises(WatermarkValidationError):
            derive_run_id(dbutils)

    def test_widget_override_local_uuid_form_accepted(self):
        override = f"local-{uuid.uuid4()}"
        dbutils = _make_dbutils(widget=override)
        assert derive_run_id(dbutils) == override


# -------------------- Fallback path (AC-SA-33) ------------------------


class TestFallbackPath:
    def test_fallback_when_context_raises(self, caplog):
        dbutils = _make_dbutils(widget_raises=True, context_raises=True)
        with caplog.at_level(logging.WARNING, logger=_RUNTIME_LOGGER):
            result = derive_run_id(dbutils)
        assert _LOCAL_UUID_RE.match(result), result
        messages = [r.message for r in caplog.records if r.name == _RUNTIME_LOGGER]
        assert any("NON-PRODUCTION" in m for m in messages), messages

    def test_fallback_when_accessors_return_none(self, caplog):
        dbutils = _make_dbutils(
            widget_raises=True,
            job_run_id=None,
            task_run_id=None,
            attempt=None,
        )
        with caplog.at_level(logging.WARNING, logger=_RUNTIME_LOGGER):
            result = derive_run_id(dbutils)
        assert _LOCAL_UUID_RE.match(result), result

    def test_fallback_result_matches_uuid_or_job_run_id_validator(self):
        from lhp_watermark import SQLInputValidator

        dbutils = _make_dbutils(widget_raises=True, context_raises=True)
        result = derive_run_id(dbutils)
        # Must not raise — validator recognises local-<uuid> form.
        assert SQLInputValidator.uuid_or_job_run_id(result) == result

    def test_fallback_when_only_partial_context(self):
        """Partial Jobs context (missing one accessor) must not produce a malformed run_id."""
        dbutils = _make_dbutils(
            widget_raises=True,
            job_run_id="1",
            task_run_id="2",
            attempt=None,
        )
        result = derive_run_id(dbutils)
        assert _LOCAL_UUID_RE.match(result), result
