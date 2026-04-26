"""Tests for Tier2RolloutCommand subcommands.

Covers preflight, backfill, optimize, rehearse paths with mocked SDK.
"""

from __future__ import annotations

import json
import sys
import types
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


class _FakeManifestColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeManifest:
    def __init__(self, names: List[str]) -> None:
        self.schema = MagicMock()
        self.schema.columns = [_FakeManifestColumn(n) for n in names]


def _success(data_array=None, manifest=None) -> MagicMock:
    resp = MagicMock()
    resp.statement_id = "stmt-y"
    resp.status = MagicMock()
    resp.status.state = "SUCCEEDED"
    resp.status.error = None
    resp.error_code = None
    resp.result = MagicMock()
    resp.result.data_array = data_array or []
    resp.manifest = manifest
    return resp


def _describe_detail(
    *, partitions: List[str], clustering: List[str], reader: int = 3, writer: int = 7,
    num_files: int = 5,
) -> MagicMock:
    cols = [
        "format", "id", "name", "description", "location", "createdAt",
        "lastModified", "clusteringColumns", "partitionColumns", "numFiles",
        "minReaderVersion", "minWriterVersion", "sizeInBytes",
    ]
    manifest = _FakeManifest(cols)
    row = [
        "delta", "id1", "watermarks", "", "/path", "2024-01-01", "2024-01-01",
        json.dumps(clustering),
        json.dumps(partitions),
        str(num_files),
        str(reader),
        str(writer),
        "1024",
    ]
    return _success(data_array=[row], manifest=manifest)


def _patch_namespace(monkeypatch, catalog: str, schema: str) -> None:
    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    monkeypatch.setattr(
        Tier2RolloutCommand,
        "_resolve_namespace",
        lambda self, env, c, s: (catalog, schema),
    )


def _make_run_sql(statements: List[str], handler):
    def _run(client, warehouse_id, statement, max_wait_seconds, sleep_fn=None):  # noqa: ARG001
        statements.append(statement)
        return handler(statement)

    return _run


# ---------- preflight -------------------------------------------------------


def test_preflight_pass_on_tier1_baseline(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    statements: List[str] = []

    def _handle(stmt: str):
        return _describe_detail(
            partitions=[],
            clustering=["source_system_id", "schema_name", "table_name"],
        )

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.preflight(
            env=None,
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
            profile=None,
        )

    out = capsys.readouterr()
    payload = json.loads(out.out)
    assert payload["partitionColumns"] == []
    assert payload["clusteringColumns"] == [
        "source_system_id", "schema_name", "table_name"
    ]
    assert "preflight gate: PASS" in out.err


def test_preflight_pass_on_already_tier2(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    def _handle(stmt: str):
        return _describe_detail(
            partitions=[],
            clustering=[
                "source_system_id", "load_group", "schema_name", "table_name"
            ],
        )

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql([], _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.preflight(
            env=None,
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
            profile=None,
        )

    err = capsys.readouterr().err
    assert "preflight gate: PASS" in err
    assert "already Tier-2-shaped" in err


def test_preflight_stop_on_partitioned_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    import click

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    def _handle(stmt: str):
        return _describe_detail(
            partitions=["source_system_id"],
            clustering=[],
        )

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql([], _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        with pytest.raises(click.ClickException, match="partitioned table"):
            cmd.preflight(
                env=None,
                catalog="metadata",
                schema="devtest_orchestration",
                warehouse_id="wh-1",
                profile=None,
            )


def test_preflight_stop_on_old_protocol_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """H23: minReaderVersion < 3 OR minWriterVersion < 7 must STOP."""
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    import click

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    def _handle(stmt: str):
        return _describe_detail(
            partitions=[],
            clustering=["source_system_id", "schema_name", "table_name"],
            reader=2,  # too low
            writer=5,  # too low
        )

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql([], _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        with pytest.raises(click.ClickException, match="minReaderVersion=2"):
            cmd.preflight(
                env=None,
                catalog="metadata",
                schema="devtest_orchestration",
                warehouse_id="wh-1",
                profile=None,
            )


# ---------- backfill --------------------------------------------------------


def test_backfill_runs_pre_check_update_post_check(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    statements: List[str] = []

    def _handle(stmt: str):
        if "COUNT(DISTINCT load_group)" in stmt:
            # pre-check: 0 distinct non-null, 5 null rows, 0 unexpected
            return _success(data_array=[["0", "5", "0"]])
        if "UPDATE" in stmt:
            return _success()
        if "COUNT_IF(load_group IS NULL)" in stmt:
            # post-check: total=5, legacy=5, null=0
            return _success(data_array=[["5", "5", "0"]])
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.backfill(
            env=None,
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
            profile=None,
        )

    assert any("UPDATE" in s for s in statements)
    out = capsys.readouterr()
    payload = json.loads(out.out)
    assert payload["post_check"]["null_rows"] == 0
    assert payload["post_check"]["legacy_rows"] == 5


def test_backfill_pre_check_blocks_on_unexpected_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    import click

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    def _handle(stmt: str):
        if "COUNT(DISTINCT load_group)" in stmt:
            # 2 unexpected rows
            return _success(data_array=[["1", "5", "2"]])
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql([], _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        with pytest.raises(click.ClickException, match="2 unexpected rows"):
            cmd.backfill(
                env=None,
                catalog="metadata",
                schema="devtest_orchestration",
                warehouse_id="wh-1",
                profile=None,
            )


def test_backfill_post_check_fails_if_null_rows_remain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    import click

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    def _handle(stmt: str):
        if "COUNT(DISTINCT load_group)" in stmt:
            return _success(data_array=[["0", "5", "0"]])
        if "UPDATE" in stmt:
            return _success()
        if "COUNT_IF(load_group IS NULL)" in stmt:
            return _success(data_array=[["5", "3", "2"]])  # 2 NULLs remain!
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql([], _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        with pytest.raises(click.ClickException, match="null_rows=2"):
            cmd.backfill(
                env=None,
                catalog="metadata",
                schema="devtest_orchestration",
                warehouse_id="wh-1",
                profile=None,
            )


# ---------- optimize --------------------------------------------------------


def test_optimize_full_emits_optimize_full_statement(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    statements: List[str] = []
    call_count = {"i": 0}

    def _handle(stmt: str):
        if stmt.strip().startswith("DESCRIBE DETAIL"):
            call_count["i"] += 1
            num = 100 if call_count["i"] == 1 else 5  # post < pre
            return _describe_detail(
                partitions=[], clustering=[], num_files=num,
            )
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.optimize(
            env=None,
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
            profile=None,
            full=True,
        )

    assert any(s == "OPTIMIZE metadata.devtest_orchestration.watermarks FULL"
               for s in statements)
    payload = json.loads(capsys.readouterr().out)
    assert payload["full"] is True
    assert payload["numFiles_pre"] == 100
    assert payload["numFiles_post"] == 5


def test_optimize_without_full_emits_plain_optimize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    statements: List[str] = []

    def _handle(stmt: str):
        if stmt.strip().startswith("DESCRIBE DETAIL"):
            return _describe_detail(partitions=[], clustering=[])
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.optimize(
            env=None,
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
            profile=None,
            full=False,
        )

    assert any(s == "OPTIMIZE metadata.devtest_orchestration.watermarks"
               for s in statements)
    assert not any("FULL" in s for s in statements if "OPTIMIZE" in s)


# ---------- rehearse --------------------------------------------------------


def test_rehearse_pins_baseline_version_and_clones(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    statements: List[str] = []

    def _handle(stmt: str):
        if "DESCRIBE HISTORY" in stmt:
            # Baseline version = 42
            return _success(data_array=[["42", "ts", "user"]])
        if "DROP TABLE IF EXISTS" in stmt:
            return _success()
        if "DEEP CLONE" in stmt:
            return _success()
        # init-registry probes
        if "SHOW TABLES" in stmt:
            return _success(data_array=[["metadata", "watermarks", False]])
        if stmt.strip().startswith("DESCRIBE TABLE"):
            return _success(data_array=[
                [c, "string", ""] for c in [
                    "run_id", "watermark_time", "source_system_id",
                    "schema_name", "table_name", "load_group",
                ]
            ])
        if stmt.strip().startswith("DESCRIBE DETAIL"):
            return _describe_detail(
                partitions=[],
                clustering=[
                    "source_system_id", "load_group", "schema_name", "table_name"
                ],
            )
        # backfill pre-check
        if "COUNT(DISTINCT load_group)" in stmt:
            return _success(data_array=[["1", "0", "0"]])
        # backfill post-check
        if "COUNT_IF(load_group IS NULL)" in stmt:
            return _success(data_array=[["100", "100", "0"]])
        return _success()

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ), patch(
        "lhp.cli.commands.init_registry_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.init_registry_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        cmd.rehearse(
            source_table="metadata.prod_orchestration.watermarks",
            target_schema="metadata.prod_dryrun_orchestration",
            warehouse_id="wh-1",
            profile=None,
            skip_optimize=True,
        )

    # Baseline version captured + DEEP CLONE issued at version 42
    assert any("DESCRIBE HISTORY" in s for s in statements)
    assert any(
        "DEEP CLONE metadata.prod_orchestration.watermarks VERSION AS OF 42" in s
        for s in statements
    )
    # Clone target = <target_schema>.watermarks (NOT renamed per L17)
    assert any(
        "CREATE TABLE metadata.prod_dryrun_orchestration.watermarks" in s
        for s in statements
    )


def test_rehearse_target_schema_must_be_two_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)

    import click

    from lhp.cli.commands.tier2_rollout_command import Tier2RolloutCommand

    cmd = Tier2RolloutCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.tier2_rollout_command.make_workspace_client",
        return_value=MagicMock(),
    ):
        with pytest.raises(click.UsageError, match="catalog.schema"):
            cmd.rehearse(
                source_table="metadata.prod_orchestration.watermarks",
                target_schema="single_part_no_dot",
                warehouse_id="wh-1",
                profile=None,
            )


# ---------- helper functions ------------------------------------------------


def test_parse_string_list_handles_json_and_list_and_garbage() -> None:
    from lhp.cli.commands.tier2_rollout_command import _parse_string_list

    assert _parse_string_list('["a", "b"]') == ["a", "b"]
    assert _parse_string_list(["a", "b"]) == ["a", "b"]
    assert _parse_string_list("") == []
    assert _parse_string_list(None) == []
    assert _parse_string_list("not-json") == []


def test_coerce_int_handles_str_int_none_bool() -> None:
    from lhp.cli.commands.tier2_rollout_command import _coerce_int

    assert _coerce_int("42") == 42
    assert _coerce_int(42) == 42
    assert _coerce_int(None) is None
    assert _coerce_int(True) is None  # bool rejected
    assert _coerce_int("not-int") is None
