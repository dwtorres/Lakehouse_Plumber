"""Tests for InitRegistryCommand.

Covers:
- Brand-new table path: SHOW returns empty, CREATE TABLE issued
- Pre-existing Tier-1 table: load_group missing, ALTER ADD COLUMNS issued
- Pre-existing Tier-1 table: clustering wrong, ALTER CLUSTER BY issued
- Pre-existing Tier-2 table: zero ALTERs (idempotent, M14)
- Namespace resolution from substitutions vs explicit catalog/schema (B13)
- Missing warehouse_id raises
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
    def __init__(self, column_names: List[str]) -> None:
        self.schema = MagicMock()
        self.schema.columns = [_FakeManifestColumn(n) for n in column_names]


def _success_response(data_array: List[List[Any]] = None, manifest: Any = None) -> MagicMock:
    resp = MagicMock()
    resp.statement_id = "stmt-x"
    resp.status = MagicMock()
    resp.status.state = "SUCCEEDED"
    resp.status.error = None
    resp.error_code = None
    resp.result = MagicMock()
    resp.result.data_array = data_array or []
    resp.manifest = manifest
    return resp


def _make_describe_detail_response(clustering: List[str] | None) -> MagicMock:
    """DESCRIBE DETAIL: ``clusteringColumns`` at index 7 in column manifest."""
    cols = [
        "format", "id", "name", "description", "location", "createdAt",
        "lastModified", "clusteringColumns", "partitionColumns", "numFiles",
    ]
    manifest = _FakeManifest(cols)
    cell = json.dumps(clustering) if clustering is not None else None
    row = ["delta", "id1", "watermarks", "", "/path", "2024-01-01", "2024-01-01",
           cell, "[]", "5"]
    return _success_response(data_array=[row], manifest=manifest)


def _make_describe_table_response(columns: List[str]) -> MagicMock:
    rows = [[c, "string", ""] for c in columns]
    return _success_response(data_array=rows)


def _make_show_tables_response(table_present: bool) -> MagicMock:
    rows = [["metadata", "watermarks", False]] if table_present else []
    return _success_response(data_array=rows)


def _patch_namespace(monkeypatch: pytest.MonkeyPatch, catalog: str, schema: str) -> None:
    """Stub _resolve_namespace to skip project-root + substitution lookup."""
    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    monkeypatch.setattr(
        InitRegistryCommand,
        "_resolve_namespace",
        lambda self, env, catalog_override, schema_override: (catalog, schema),
    )


def _make_run_sql(statements: List[str], handler):
    """Build a side_effect that records statements + delegates to handler."""

    def _run(client, warehouse_id, statement, max_wait_seconds, sleep_fn=None):  # noqa: ARG001
        statements.append(statement)
        return handler(statement)

    return _run


def test_brand_new_table_creates_with_tier2_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    statements: List[str] = []

    def _handle(statement: str) -> Any:
        if "SHOW TABLES" in statement:
            return _make_show_tables_response(table_present=False)
        return _success_response()

    fake_client = MagicMock()
    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.init_registry_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.init_registry_command.make_workspace_client",
        return_value=fake_client,
    ):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
        )

    assert any("SHOW TABLES" in s for s in statements)
    assert any("CREATE TABLE IF NOT EXISTS" in s for s in statements)
    assert any(
        "CLUSTER BY (source_system_id, load_group, schema_name, table_name)" in s
        for s in statements
    )
    assert not any("ALTER TABLE" in s for s in statements)


def test_pre_existing_tier1_adds_load_group_and_alters_clustering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    statements: List[str] = []
    tier1_cols = [
        "run_id", "watermark_time", "source_system_id", "schema_name",
        "table_name", "watermark_column_name", "watermark_value",
        "previous_watermark_value", "row_count", "extraction_type",
        "bronze_stage_complete", "silver_stage_complete", "status",
        "error_class", "error_message", "created_at", "completed_at",
    ]
    tier1_clustering = ["source_system_id", "schema_name", "table_name"]

    def _handle(statement: str) -> Any:
        if "SHOW TABLES" in statement:
            return _make_show_tables_response(table_present=True)
        if statement.strip().startswith("DESCRIBE TABLE"):
            return _make_describe_table_response(tier1_cols)
        if statement.strip().startswith("DESCRIBE DETAIL"):
            return _make_describe_detail_response(tier1_clustering)
        return _success_response()

    fake_client = MagicMock()
    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.init_registry_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.init_registry_command.make_workspace_client",
        return_value=fake_client,
    ):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
        )

    assert any(
        "ALTER TABLE" in s and "ADD COLUMNS (load_group STRING)" in s
        for s in statements
    )
    assert any(
        "ALTER TABLE" in s
        and "CLUSTER BY (source_system_id, load_group, schema_name, table_name)" in s
        for s in statements
    )
    assert not any("CREATE TABLE IF NOT EXISTS" in s for s in statements)


def test_already_tier2_table_is_idempotent_no_alters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M14: re-init on Tier-2 shape issues zero new ALTERs."""
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    statements: List[str] = []
    tier2_cols = [
        "run_id", "watermark_time", "source_system_id", "schema_name",
        "table_name", "watermark_column_name", "watermark_value",
        "previous_watermark_value", "row_count", "extraction_type",
        "bronze_stage_complete", "silver_stage_complete", "status",
        "error_class", "error_message", "created_at", "completed_at",
        "load_group",
    ]
    tier2_clustering = [
        "source_system_id", "load_group", "schema_name", "table_name",
    ]

    def _handle(statement: str) -> Any:
        if "SHOW TABLES" in statement:
            return _make_show_tables_response(table_present=True)
        if statement.strip().startswith("DESCRIBE TABLE"):
            return _make_describe_table_response(tier2_cols)
        if statement.strip().startswith("DESCRIBE DETAIL"):
            return _make_describe_detail_response(tier2_clustering)
        return _success_response()

    fake_client = MagicMock()
    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    with patch(
        "lhp.cli.commands.init_registry_command.execute_and_wait",
        side_effect=_make_run_sql(statements, _handle),
    ), patch(
        "lhp.cli.commands.init_registry_command.make_workspace_client",
        return_value=fake_client,
    ):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id="wh-1",
        )

    assert any("SHOW TABLES" in s for s in statements)
    assert any("DESCRIBE TABLE" in s for s in statements)
    assert any("DESCRIBE DETAIL" in s for s in statements)
    assert not any("ALTER TABLE" in s for s in statements)
    assert not any("CREATE TABLE" in s for s in statements)


def test_missing_warehouse_id_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_sdk(monkeypatch)
    _patch_namespace(monkeypatch, "metadata", "devtest_orchestration")
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)

    import click

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    with pytest.raises(click.ClickException, match="DATABRICKS_WAREHOUSE_ID"):
        cmd.execute(
            catalog="metadata",
            schema="devtest_orchestration",
            warehouse_id=None,
        )


def test_explicit_catalog_schema_bypasses_env_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """B13: rehearsal path uses explicit --catalog + --schema, no env file."""
    _install_fake_sdk(monkeypatch)

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    catalog, schema = cmd._resolve_namespace(
        env=None,
        catalog_override="metadata",
        schema_override="prod_dryrun_orchestration",
    )
    assert (catalog, schema) == ("metadata", "prod_dryrun_orchestration")


def test_missing_env_and_explicit_args_raises_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_sdk(monkeypatch)

    import click

    from lhp.cli.commands.init_registry_command import InitRegistryCommand

    cmd = InitRegistryCommand()
    cmd.setup_from_context = lambda: None
    with pytest.raises(click.UsageError):
        cmd._resolve_namespace(env=None, catalog_override=None, schema_override=None)


def test_clustering_columns_parses_json_and_list_forms() -> None:
    from lhp.cli.commands.init_registry_command import _parse_clustering_columns

    assert _parse_clustering_columns('["a", "b", "c"]') == ["a", "b", "c"]
    assert _parse_clustering_columns(["a", "b"]) == ["a", "b"]
    assert _parse_clustering_columns("") == []
    assert _parse_clustering_columns(None) == []
    assert _parse_clustering_columns("not-json") == []
