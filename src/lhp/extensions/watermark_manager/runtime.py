"""Runtime helpers for watermark_manager (L3 §4.2.4, FR-L-09, AC-SA-32..34).

``derive_run_id`` centralises the rule for picking a ``run_id`` so the
extraction template never calls ``uuid.uuid4()`` at the top level (see
AC-SA-35). Resolution order:

1. Widget ``lhp_run_id_override`` — operator-supplied for backfill/test
   scenarios. Validated via ``SQLInputValidator.uuid_or_job_run_id`` and
   logged at WARNING so the override leaves an audit trail.
2. Databricks Jobs context — returns
   ``job-{jobRunId}-task-{taskRunId}-attempt-{N}``; this is the only
   production path and is silent on success.
3. Fallback ``local-<uuid4>`` — only reached on interactive clusters
   without a Jobs context. Logged at WARNING with ``NON-PRODUCTION`` so
   the operator sees the provenance gap.

All exceptions from ``dbutils`` accessors are caught locally; the
function never propagates Databricks-specific errors to the caller.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Optional

from lhp.extensions.watermark_manager.sql_safety import SQLInputValidator

logger = logging.getLogger(__name__)

_WIDGET_NAME = "lhp_run_id_override"
_OVERRIDE_WARN = (
    "derive_run_id: override widget supplied; intended for backfill/test"
)
_FALLBACK_WARN = (
    "derive_run_id: no Jobs context; fell back to local-<uuid>; NON-PRODUCTION"
)


def derive_run_id(dbutils: Any) -> str:
    """Return a deterministic ``run_id`` for the current notebook invocation.

    Resolution order documented at module level. Widget values are
    validated and will raise ``WatermarkValidationError`` when an
    adversarial operator supplies an injection payload; non-widget
    paths cannot raise.
    """
    override = _read_override_widget(dbutils)
    if override:
        SQLInputValidator.uuid_or_job_run_id(override)
        logger.warning(_OVERRIDE_WARN)
        return override

    jobs_id = _read_jobs_context(dbutils)
    if jobs_id is not None:
        return jobs_id

    fallback = f"local-{uuid.uuid4()}"
    logger.warning(_FALLBACK_WARN)
    return fallback


def _read_override_widget(dbutils: Any) -> Optional[str]:
    """Return the widget value or ``None`` when undeclared/empty.

    ``dbutils.widgets.get`` raises when the widget is not declared and
    returns ``""`` when declared-but-empty; both map to "no override".
    """
    try:
        value = dbutils.widgets.get(_WIDGET_NAME)
    except Exception:
        return None
    if not value:
        return None
    return str(value)


def _read_jobs_context(dbutils: Any) -> Optional[str]:
    """Return ``job-{jobRunId}-task-{taskRunId}-attempt-{N}`` or ``None``.

    All three accessors must return non-empty values; a partial context
    (e.g. ``currentRunAttempt`` missing) is treated as "no context" so
    the caller falls back to the ``local-<uuid>`` path instead of
    emitting a malformed ``run_id`` that the validator would later
    reject.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return None

    job_run_id = _safe_call(ctx, "jobRunId")
    task_run_id = _safe_call(ctx, "taskRunId")
    attempt = _safe_call(ctx, "currentRunAttempt")
    if not (job_run_id and task_run_id and attempt):
        return None
    return f"job-{job_run_id}-task-{task_run_id}-attempt-{attempt}"


def _safe_call(ctx: Any, name: str) -> Optional[str]:
    """Call ``ctx.<name>()`` and normalise the return value.

    Databricks context accessors return either a bare string or a
    scala ``Option[String]`` depending on DBR version and whether the
    field is populated. ``None`` / empty / undefined Option all map to
    ``None`` here.
    """
    try:
        accessor = getattr(ctx, name)
        value = accessor()
    except Exception:
        return None
    if value is None:
        return None
    # Scala Option surfaced via py4j.
    is_defined = getattr(value, "isDefined", None)
    if callable(is_defined):
        try:
            if not is_defined():
                return None
            unwrapped = value.get()
        except Exception:
            return None
        if unwrapped is None:
            return None
        text = str(unwrapped)
        return text or None
    text = str(value)
    return text or None


__all__ = ["derive_run_id"]
