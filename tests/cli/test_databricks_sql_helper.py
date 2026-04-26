"""Tests for the shared Databricks SQL Statement Execution helper.

Covers:
- SUCCEEDED on initial submit (fast path, no poll)
- FAILED on initial submit
- PENDING → SUCCEEDED via poll
- top-level error_code in poll response → typed failure (H19)
- max_wait_seconds exceeded → cancel + raise (B1, H25)
- exponential backoff applied (5→10→20→30 cap)
- collect_rows handles empty / missing data_array
"""

from __future__ import annotations

import sys
import types
from typing import Any, List
from unittest.mock import MagicMock

import pytest


def _install_fake_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a minimal fake databricks.sdk so the helper imports cleanly."""

    class _FakeStatementState:
        SUCCEEDED = "SUCCEEDED"
        FAILED = "FAILED"
        CANCELED = "CANCELED"
        PENDING = "PENDING"
        RUNNING = "RUNNING"

    class _FakeWorkspaceClient:
        def __init__(self, profile: str | None = None) -> None:  # noqa: ARG002
            pass

    sdk_pkg = types.ModuleType("databricks.sdk")
    sdk_pkg.WorkspaceClient = _FakeWorkspaceClient

    class _FakeOnWaitTimeout:
        CANCEL = "CANCEL"
        CONTINUE = "CONTINUE"

    sql_mod = types.ModuleType("databricks.sdk.service.sql")
    sql_mod.StatementState = _FakeStatementState
    sql_mod.ExecuteStatementRequestOnWaitTimeout = _FakeOnWaitTimeout

    service_pkg = types.ModuleType("databricks.sdk.service")
    service_pkg.sql = sql_mod

    databricks_pkg = types.ModuleType("databricks")
    databricks_pkg.sdk = sdk_pkg

    monkeypatch.setitem(sys.modules, "databricks", databricks_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service", service_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sql", sql_mod)


def _make_response(state: str, *, statement_id: str = "stmt-1", error: Any = None) -> MagicMock:
    resp = MagicMock()
    resp.statement_id = statement_id
    resp.status = MagicMock()
    resp.status.state = state
    resp.status.error = error
    resp.error_code = None
    return resp


def _make_client(initial_state: str, poll_states: List[str]) -> MagicMock:
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = _make_response(initial_state)

    poll_iter = iter(poll_states)

    def _get(statement_id: str) -> MagicMock:  # noqa: ARG001
        try:
            state = next(poll_iter)
        except StopIteration:
            raise AssertionError("get_statement called more times than expected")
        return _make_response(state, statement_id=statement_id)

    client.statement_execution.get_statement.side_effect = _get
    client.statement_execution.cancel_execution.return_value = None
    return client


def test_succeeded_on_initial_submit_no_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import execute_and_wait

    client = _make_client(initial_state="SUCCEEDED", poll_states=[])
    sleep_calls: List[float] = []

    resp = execute_and_wait(
        client=client,
        warehouse_id="wh1",
        statement="SELECT 1",
        max_wait_seconds=600,
        sleep_fn=sleep_calls.append,
    )

    assert resp.status.state == "SUCCEEDED"
    assert sleep_calls == []  # no polling needed
    client.statement_execution.get_statement.assert_not_called()


def test_failed_on_initial_submit_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import (
        StatementExecutionError,
        execute_and_wait,
    )

    err = MagicMock()
    err.message = "syntax error"
    resp = _make_response("FAILED", error=err)
    client = MagicMock()
    client.statement_execution.execute_statement.return_value = resp

    with pytest.raises(StatementExecutionError, match="syntax error"):
        execute_and_wait(
            client=client,
            warehouse_id="wh1",
            statement="BAD SQL",
            max_wait_seconds=60,
            sleep_fn=lambda _x: None,
        )


def test_pending_then_succeeded_polls(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import execute_and_wait

    client = _make_client(
        initial_state="PENDING",
        poll_states=["RUNNING", "RUNNING", "SUCCEEDED"],
    )
    sleep_calls: List[float] = []

    resp = execute_and_wait(
        client=client,
        warehouse_id="wh1",
        statement="OPTIMIZE t",
        max_wait_seconds=600,
        sleep_fn=sleep_calls.append,
    )

    assert resp.status.state == "SUCCEEDED"
    # Three polls × backoff 5, 10, 20
    assert sleep_calls == [5, 10, 20]


def test_backoff_caps_at_30s(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import execute_and_wait

    client = _make_client(
        initial_state="PENDING",
        poll_states=["RUNNING"] * 5 + ["SUCCEEDED"],
    )
    sleep_calls: List[float] = []

    execute_and_wait(
        client=client,
        warehouse_id="wh1",
        statement="OPTIMIZE t",
        max_wait_seconds=3600,
        sleep_fn=sleep_calls.append,
    )

    # 5, 10, 20, 30 (cap), 30, 30
    assert sleep_calls == [5, 10, 20, 30, 30, 30]


def test_top_level_error_code_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import (
        StatementExecutionError,
        execute_and_wait,
    )

    submit = _make_response("PENDING")
    poll_resp = _make_response("RUNNING")
    poll_resp.error_code = "INVALID_PARAMETER_VALUE"

    client = MagicMock()
    client.statement_execution.execute_statement.return_value = submit
    client.statement_execution.get_statement.return_value = poll_resp

    with pytest.raises(StatementExecutionError, match="INVALID_PARAMETER_VALUE"):
        execute_and_wait(
            client=client,
            warehouse_id="wh1",
            statement="X",
            max_wait_seconds=60,
            sleep_fn=lambda _x: None,
        )


def test_max_wait_exceeded_cancels_and_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    from lhp.cli.commands._databricks_sql import (
        StatementExecutionError,
        execute_and_wait,
    )

    # Poll forever — never reaches SUCCEEDED
    client = _make_client(
        initial_state="PENDING",
        poll_states=["RUNNING"] * 100,
    )

    with pytest.raises(StatementExecutionError, match="exceeded max_wait_seconds"):
        execute_and_wait(
            client=client,
            warehouse_id="wh1",
            statement="OPTIMIZE big_table",
            max_wait_seconds=10,  # tiny cap, first sleep(5) + sleep(10) crosses it
            sleep_fn=lambda _x: None,  # simulate instant time-pass
        )

    # Cancel attempted before raising
    client.statement_execution.cancel_execution.assert_called()


def test_collect_rows_empty_response() -> None:
    from lhp.cli.commands._databricks_sql import collect_rows

    resp = MagicMock()
    resp.result = None
    assert collect_rows(resp) == []


def test_collect_rows_returns_data_array() -> None:
    from lhp.cli.commands._databricks_sql import collect_rows

    resp = MagicMock()
    resp.result.data_array = [["a", "b"], ["c", "d"]]
    assert collect_rows(resp) == [["a", "b"], ["c", "d"]]


def test_collect_rows_missing_data_array() -> None:
    from lhp.cli.commands._databricks_sql import collect_rows

    resp = MagicMock()
    resp.result.data_array = None
    assert collect_rows(resp) == []
