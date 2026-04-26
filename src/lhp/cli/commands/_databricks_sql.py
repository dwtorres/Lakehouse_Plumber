"""Shared Databricks SQL Statement Execution helper for tier-2 ops CLIs.

Wraps ``databricks.sdk.WorkspaceClient.statement_execution`` with the polling
+ SIGINT-cancel + max-wait + backoff semantics required by plan v5
§Polling helper. Used by ``init_registry`` and ``validate_tier2`` commands;
also intended for the eventual ``lhp tier2-rollout`` umbrella (Workstream B
S5/S8).

Design choices:
- SDK is imported lazily so dry-run paths in callers don't pay the import.
- ``execute_and_wait`` always submits async (``wait_timeout='0s'``,
  ``on_wait_timeout='CONTINUE'``) and polls explicitly. The SDK's own
  blocking ``wait_timeout`` would mask PENDING-after-timeout cases the
  plan flagged in B1.
- SIGINT during poll cancels the in-flight statement before raising
  ``KeyboardInterrupt`` (H7). UPDATE / OPTIMIZE cancellation rolls back
  atomically per Delta semantics.
- Exponential backoff 5→10→20→30s cap; per-phase ``max_wait_seconds``
  cap (default 1h, callers override per H25).
- Top-level ``error_code`` in response body is treated as failure even on
  HTTP 200 (H19).
"""

from __future__ import annotations

import logging
import signal
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional

logger = logging.getLogger(__name__)

# Default Databricks Connect / SDK profile per project memory.
DEFAULT_DATABRICKS_PROFILE = "dbc-8e058692-373e"

# Backoff schedule + cap.
_INITIAL_DELAY_SEC = 5
_MAX_DELAY_SEC = 30


class StatementExecutionError(RuntimeError):
    """Raised when a SQL Statement Execution API call fails or times out."""


def _import_sdk() -> tuple[Any, Any, Any]:
    """Return ``(WorkspaceClient, StatementState, ExecuteStatementRequestOnWaitTimeout)``.

    Raises a friendly error if the SDK is not installed. The enum is needed
    because ``execute_statement(on_wait_timeout=...)`` requires the typed
    enum value, not a string (SDK 0.105+).
    """
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import (
            ExecuteStatementRequestOnWaitTimeout,
            StatementState,
        )
    except ImportError as exc:  # pragma: no cover - exercised via integration only
        raise StatementExecutionError(
            "This command requires the databricks-sdk. Install via:\n"
            "    pip install databricks-sdk\n"
            f"Original import error: {exc}"
        ) from exc
    return WorkspaceClient, StatementState, ExecuteStatementRequestOnWaitTimeout


def make_workspace_client(profile: Optional[str] = None) -> Any:
    """Return a configured ``WorkspaceClient``. Lazy SDK import."""
    WorkspaceClient, _, _ = _import_sdk()
    return WorkspaceClient(profile=profile or DEFAULT_DATABRICKS_PROFILE)


@contextmanager
def _sigint_cancel(client: Any, statement_id: str) -> Iterator[None]:
    """Install a SIGINT handler that cancels the in-flight statement.

    Restores the prior handler on exit. If SIGINT fires inside the block,
    the cancel is attempted best-effort; KeyboardInterrupt then propagates
    so the CLI exits cleanly.
    """
    fired = {"flag": False}

    def _handler(signum: int, _frame: Any) -> None:
        fired["flag"] = True
        try:
            client.statement_execution.cancel_execution(statement_id=statement_id)
            logger.info("Cancelled statement_id=%s on SIGINT", statement_id)
        except Exception as exc:  # noqa: BLE001 - cancel is best-effort
            logger.warning(
                "Cancel failed for statement_id=%s: %s", statement_id, exc
            )
        # Re-raise as KeyboardInterrupt so the CLI exits non-zero.
        raise KeyboardInterrupt()

    prior = signal.signal(signal.SIGINT, _handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, prior)
        if fired["flag"]:
            # _handler already raised; defensive logging only.
            logger.debug("SIGINT handler returned without raise")


def execute_and_wait(
    *,
    client: Any,
    warehouse_id: str,
    statement: str,
    max_wait_seconds: int = 3600,
    sleep_fn: Any = None,
) -> Any:
    """Submit a SQL statement, poll until terminal, return the response.

    Args:
        client: ``WorkspaceClient`` (use ``make_workspace_client``).
        warehouse_id: SQL warehouse id.
        statement: SQL text. Multiline + jq-escaped strings are fine.
        max_wait_seconds: Hard cap on poll duration. On exceed, the in-flight
            statement is cancelled and ``StatementExecutionError`` raised.
        sleep_fn: Override for unit tests (default ``time.sleep``).

    Returns:
        SDK response object on SUCCEEDED.

    Raises:
        StatementExecutionError on FAILED, CANCELED, max-wait timeout, or
        top-level ``error_code`` in response body.
        KeyboardInterrupt on SIGINT (after best-effort cancel).
    """
    if sleep_fn is None:
        import time as _time

        sleep_fn = _time.sleep

    _, StatementState, OnWaitTimeout = _import_sdk()

    response = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="0s",
        on_wait_timeout=OnWaitTimeout.CONTINUE,
    )

    statement_id = getattr(response, "statement_id", None)
    if not statement_id:
        # Defensive: SDK should always populate this; surface clear error.
        raise StatementExecutionError(
            f"submit returned no statement_id; response={response!r}"
        )
    logger.debug("submitted statement_id=%s", statement_id)

    delay = _INITIAL_DELAY_SEC
    elapsed = 0
    initial_state = (
        response.status.state if response.status else None
    )
    if initial_state == StatementState.SUCCEEDED:
        return response
    if initial_state in (StatementState.FAILED, StatementState.CANCELED):
        _raise_failed(response, statement_id)

    with _sigint_cancel(client, statement_id):
        while True:
            sleep_fn(delay)
            elapsed += delay
            resp = client.statement_execution.get_statement(
                statement_id=statement_id
            )

            # H19: top-level error_code overrides state interpretation.
            err_code = _extract_error_code(resp)
            if err_code:
                raise StatementExecutionError(
                    f"statement_id={statement_id} returned api error_code={err_code}"
                )

            state = resp.status.state if resp.status else None
            if state == StatementState.SUCCEEDED:
                return resp
            if state in (StatementState.FAILED, StatementState.CANCELED):
                _raise_failed(resp, statement_id)
            # PENDING / RUNNING — keep polling
            if elapsed >= max_wait_seconds:
                # Best-effort cancel before raising
                try:
                    client.statement_execution.cancel_execution(
                        statement_id=statement_id
                    )
                except Exception:  # noqa: BLE001
                    pass
                raise StatementExecutionError(
                    f"statement_id={statement_id} exceeded max_wait_seconds="
                    f"{max_wait_seconds}; cancelled"
                )
            # Exponential backoff with cap
            next_delay = delay * 2
            delay = next_delay if next_delay <= _MAX_DELAY_SEC else _MAX_DELAY_SEC


def _raise_failed(response: Any, statement_id: str) -> None:
    """Surface FAILED/CANCELED state as a typed error."""
    err = response.status.error if response.status else None
    msg = err.message if err and getattr(err, "message", None) else ""
    state = response.status.state if response.status else "<unknown>"
    raise StatementExecutionError(
        f"statement_id={statement_id} terminated state={state} message={msg}"
    )


def _extract_error_code(response: Any) -> Optional[str]:
    """Return top-level ``error_code`` if present in the response body.

    The SDK's get_statement response is a typed object; older SDK versions
    have surfaced top-level error fields distinct from ``status.error``.
    Probe defensively without coupling to a specific SDK version.
    """
    err_code = getattr(response, "error_code", None)
    if err_code:
        return str(err_code)
    return None


def collect_rows(response: Any) -> List[List[Any]]:
    """Extract ``data_array`` rows from a SUCCEEDED execute response.

    Returns ``[]`` when the result has no rows (e.g., a DDL statement).
    """
    result = getattr(response, "result", None)
    if result is None:
        return []
    rows = getattr(result, "data_array", None)
    if rows is None:
        return []
    # SDK returns list[list[str|None]]; pass through.
    return list(rows)
