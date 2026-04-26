"""Tests for ValidateTier2Command.

Covers:
- PASS path: notebook upload + run submit + poll-success + parse exit JSON
- FAIL path: V1-V5 not all PASS, raises with diagnostic
- Missing cluster_id raises
- Probe schema split: catalog.schema → (catalog, schema)
- Notebook source location resolution
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest


def _install_fake_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeStatementState:
        SUCCEEDED = "SUCCEEDED"
        FAILED = "FAILED"
        CANCELED = "CANCELED"
        PENDING = "PENDING"
        RUNNING = "RUNNING"

    class _FakeRunLifeCycleState:
        TERMINATED = "TERMINATED"
        INTERNAL_ERROR = "INTERNAL_ERROR"
        SKIPPED = "SKIPPED"
        PENDING = "PENDING"
        RUNNING = "RUNNING"

    class _FakeRunResultState:
        SUCCESS = "SUCCESS"
        FAILED = "FAILED"
        TIMEDOUT = "TIMEDOUT"
        CANCELED = "CANCELED"

    class _FakeImportFormat:
        SOURCE = "SOURCE"

    class _FakeLanguage:
        PYTHON = "PYTHON"

    class _FakeNotebookTask:
        def __init__(self, *, notebook_path: str, base_parameters: dict) -> None:
            self.notebook_path = notebook_path
            self.base_parameters = base_parameters

    class _FakeSubmitTask:
        def __init__(self, *, task_key: str, notebook_task, existing_cluster_id: str) -> None:
            self.task_key = task_key
            self.notebook_task = notebook_task
            self.existing_cluster_id = existing_cluster_id

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

    workspace_mod = types.ModuleType("databricks.sdk.service.workspace")
    workspace_mod.ImportFormat = _FakeImportFormat
    workspace_mod.Language = _FakeLanguage

    jobs_mod = types.ModuleType("databricks.sdk.service.jobs")
    jobs_mod.NotebookTask = _FakeNotebookTask
    jobs_mod.SubmitTask = _FakeSubmitTask
    jobs_mod.RunLifeCycleState = _FakeRunLifeCycleState
    jobs_mod.RunResultState = _FakeRunResultState

    service_pkg = types.ModuleType("databricks.sdk.service")
    service_pkg.sql = sql_mod
    service_pkg.workspace = workspace_mod
    service_pkg.jobs = jobs_mod

    databricks_pkg = types.ModuleType("databricks")
    databricks_pkg.sdk = sdk_pkg

    monkeypatch.setitem(sys.modules, "databricks", databricks_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk", sdk_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service", service_pkg)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.sql", sql_mod)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.workspace", workspace_mod)
    monkeypatch.setitem(sys.modules, "databricks.sdk.service.jobs", jobs_mod)


def _patch_namespace(monkeypatch: pytest.MonkeyPatch, catalog: str, schema: str) -> None:
    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    monkeypatch.setattr(
        ValidateTier2Command,
        "_resolve_namespace",
        lambda self, env, catalog_override, schema_override: (catalog, schema),
    )


def _patch_notebook_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Stub _locate_notebook_source to a tmp file (skip project-root walk)."""
    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    notebook = tmp_path / "fake_notebook.py"
    notebook.write_text("# Databricks notebook source\nprint('stub')\n")
    monkeypatch.setattr(
        ValidateTier2Command,
        "_locate_notebook_source",
        lambda self: notebook,
    )
    return notebook


def _make_terminated_run(result_state: str, exit_value: str | None) -> tuple[MagicMock, MagicMock]:
    """Build (get_run_response, get_run_output_response) pair."""
    run = MagicMock()
    run.state = MagicMock()
    run.state.life_cycle_state = "TERMINATED"
    run.state.result_state = result_state
    run.state.state_message = ""

    output = MagicMock()
    if exit_value is None:
        output.notebook_output = None
    else:
        output.notebook_output = MagicMock()
        output.notebook_output.result = exit_value
    return run, output


def test_pass_path_emits_summary_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")
    _patch_notebook_source(monkeypatch, tmp_path)

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    fake_client = MagicMock()
    submit_resp = MagicMock()
    submit_resp.run_id = 12345
    fake_client.jobs.submit.return_value = submit_resp

    exit_payload = json.dumps({
        "overall_verdict": "PASS",
        "failed_invariants": [],
        "invariants": {
            "V1": {"verdict": "PASS"},
            "V2": {"verdict": "PASS"},
            "V3": {"verdict": "PASS"},
            "V4": {"verdict": "PASS"},
            "V5": {"verdict": "PASS"},
        },
    })
    run_resp, output_resp = _make_terminated_run("SUCCESS", exit_payload)
    fake_client.jobs.get_run.return_value = run_resp
    fake_client.jobs.get_run_output.return_value = output_resp

    cmd = ValidateTier2Command()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.validate_tier2_command.make_workspace_client",
        return_value=fake_client,
    ):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            cluster_id="cluster-x",
            poll_interval_seconds=0,
        )

    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["overall_verdict"] == "PASS"
    fake_client.jobs.submit.assert_called_once()


def test_fail_path_raises_with_failed_invariants(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")
    _patch_notebook_source(monkeypatch, tmp_path)

    import click

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    fake_client = MagicMock()
    submit_resp = MagicMock()
    submit_resp.run_id = 99
    fake_client.jobs.submit.return_value = submit_resp

    exit_payload = json.dumps({
        "overall_verdict": "FAIL",
        "failed_invariants": ["V5"],
        "invariants": {
            "V5": {"verdict": "FAIL", "details": {}},
        },
    })
    run_resp, output_resp = _make_terminated_run("SUCCESS", exit_payload)
    fake_client.jobs.get_run.return_value = run_resp
    fake_client.jobs.get_run_output.return_value = output_resp

    cmd = ValidateTier2Command()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.validate_tier2_command.make_workspace_client",
        return_value=fake_client,
    ):
        with pytest.raises(click.ClickException, match="V5"):
            cmd.execute(
                catalog="metadata",
                schema="devtest_orchestration",
                cluster_id="cluster-x",
                poll_interval_seconds=0,
            )


def test_missing_cluster_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")
    monkeypatch.delenv("DATABRICKS_CLUSTER_ID", raising=False)

    import click

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    cmd = ValidateTier2Command()
    cmd.setup_from_context = lambda: None
    with pytest.raises(click.ClickException, match="DATABRICKS_CLUSTER_ID"):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            cluster_id=None,
        )


def test_probe_schema_split_two_part() -> None:
    from lhp.cli.commands.validate_tier2_command import _split_two_part

    assert _split_two_part("metadata.devtest_validation") == (
        "metadata", "devtest_validation",
    )
    assert _split_two_part("solo_schema") == (None, "solo_schema")
    assert _split_two_part(None) == (None, None)
    assert _split_two_part("") == (None, None)


def test_default_probe_schema_uses_env_validation_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    cmd = ValidateTier2Command()
    cmd.setup_from_context = lambda: None
    assert cmd._default_probe_schema("devtest", "metadata") == (
        "metadata.devtest_validation"
    )
    assert cmd._default_probe_schema(None, "metadata") == "metadata.validation"


def test_parse_exit_value_invalid_json_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)

    import click

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    cmd = ValidateTier2Command()
    with pytest.raises(click.ClickException, match="not valid JSON"):
        cmd._parse_exit_value("not-json{")


def test_run_terminates_failed_state_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")
    _patch_notebook_source(monkeypatch, tmp_path)

    import click

    from lhp.cli.commands.validate_tier2_command import ValidateTier2Command

    fake_client = MagicMock()
    submit_resp = MagicMock()
    submit_resp.run_id = 1
    fake_client.jobs.submit.return_value = submit_resp

    run_resp = MagicMock()
    run_resp.state = MagicMock()
    run_resp.state.life_cycle_state = "TERMINATED"
    run_resp.state.result_state = "FAILED"
    run_resp.state.state_message = "notebook raised"
    fake_client.jobs.get_run.return_value = run_resp

    cmd = ValidateTier2Command()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.validate_tier2_command.make_workspace_client",
        return_value=fake_client,
    ):
        with pytest.raises(click.ClickException, match="FAILED"):
            cmd.execute(
                catalog="metadata",
                schema="devtest_orchestration",
                cluster_id="cluster-x",
                poll_interval_seconds=0,
            )
