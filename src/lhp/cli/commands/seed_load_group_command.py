"""Seed-load-group command: emit Step 4a SQL for a Tier 2 flowgroup migration.

Materialises the per-table SELECT-preview + INSERT-seed SQL needed to
copy the legacy HWM ceiling into a flowgroup's per-(pipeline, flowgroup)
``load_group`` namespace before its first B2 run. SQL is composed via
``lhp_watermark.sql_safety`` primitives so user data never lands in an
f-string.

Typical use (dry-run / paste workflow), from a project root:

    $ lhp seed-load-group --env devtest --flowgroup pipelines/.../customer_bronze.yaml

The ``--apply`` path executes via the Databricks SQL Statement Execution
API (databricks-sdk). The SDK is imported lazily so the dry-run path has
no extra dependency.

See: docs/planning/tier-2-hwm-load-group-fix.md §Step 4a
     docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md §U5
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import List, Optional

import click

from ...utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
)
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


# Composite separator — must match generator (U3) and B2 plan.
_LOAD_GROUP_SEP = "::"

# Default Databricks Connect / SDK profile per project memory; only used
# when --apply is set and the SDK is available.
_DEFAULT_DATABRICKS_PROFILE = "dbc-8e058692-373e"


class SeedLoadGroupCommand(BaseCommand):
    """Emit Step 4a legacy-HWM seed SQL for a Tier 2 flowgroup."""

    def execute(
        self,
        env: str,
        flowgroup: str,
        apply: bool = False,
        dry_run: bool = True,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> None:
        """Execute the seed-load-group command.

        Args:
            env: Environment name (must have a ``substitutions/<env>.yaml``).
            flowgroup: Path to the flowgroup YAML (relative or absolute).
            apply: When True, execute the SQL via the Databricks SQL SDK.
            dry_run: When True (default), only emit SQL to stdout.
            catalog: Optional override of ``${watermark_catalog}``.
            schema: Optional override of ``${watermark_schema}``.
            warehouse_id: Databricks SQL warehouse ID for ``--apply`` (or
                set ``DATABRICKS_WAREHOUSE_ID`` env var).
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Substitution file (raises LHPFileError on miss).
        substitution_file = self.check_substitution_file(env)

        flowgroup_path = self._resolve_flowgroup_path(flowgroup, project_root)
        flowgroups = self._parse_flowgroups(flowgroup_path)

        # Resolve registry namespace from substitutions (or CLI overrides).
        wm_catalog, wm_schema = self._resolve_registry_namespace(
            substitution_file, env, catalog, schema
        )
        registry_table = f"{wm_catalog}.{wm_schema}.watermarks"

        for fg in flowgroups:
            self._emit_for_flowgroup(
                fg, registry_table, apply=apply, warehouse_id=warehouse_id
            )

    # ------------------------------------------------------------------
    # Resolution helpers
    # ------------------------------------------------------------------

    def _resolve_flowgroup_path(self, flowgroup: str, project_root: Path) -> Path:
        """Resolve a user-provided flowgroup arg to a concrete YAML path.

        Accepts: absolute path, path relative to cwd, or path relative to
        project root. Falls back to a recursive name-match under
        ``pipelines/``.
        """
        candidate = Path(flowgroup)
        if candidate.is_absolute() and candidate.exists():
            return candidate
        rel_to_cwd = (Path.cwd() / candidate).resolve()
        if rel_to_cwd.exists():
            return rel_to_cwd
        rel_to_project = (project_root / candidate).resolve()
        if rel_to_project.exists():
            return rel_to_project

        # Last-resort: name match under pipelines/.
        pipelines_dir = project_root / "pipelines"
        if pipelines_dir.is_dir():
            stem = Path(flowgroup).stem
            for yaml_file in pipelines_dir.rglob("*.yaml"):
                if yaml_file.stem == stem:
                    return yaml_file
            for yaml_file in pipelines_dir.rglob("*.yml"):
                if yaml_file.stem == stem:
                    return yaml_file

        raise LHPFileError(
            category=ErrorCategory.IO,
            code_number="007",
            title=f"Flowgroup YAML not found: {flowgroup}",
            details=(
                "Could not resolve the --flowgroup argument to an existing "
                f"YAML file. Looked in cwd, project root ({project_root}), "
                "and recursively under pipelines/."
            ),
            suggestions=[
                "Pass --flowgroup as a path (relative or absolute) to a "
                "YAML file in pipelines/.",
                "Or pass the flowgroup file's stem (e.g. 'customer_bronze') "
                "for a recursive name-match.",
            ],
            context={"argument": flowgroup, "project_root": str(project_root)},
        )

    def _parse_flowgroups(self, flowgroup_path: Path):
        """Parse a flowgroup YAML into FlowGroup objects.

        Wraps parser exceptions into LHPConfigError so they flow cleanly
        through ``cli_error_boundary``.
        """
        from ...parsers.yaml_parser import YAMLParser

        try:
            return YAMLParser().parse_flowgroups_from_file(flowgroup_path)
        except LHPError:
            raise
        except Exception as e:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="023",
                title="Failed to parse flowgroup YAML",
                details=f"Error parsing {flowgroup_path}: {e}",
                suggestions=[
                    "Check the YAML syntax",
                    "Verify the file is a valid flowgroup definition",
                ],
                context={"file": str(flowgroup_path)},
            ) from e

    def _resolve_registry_namespace(
        self,
        substitution_file: Path,
        env: str,
        catalog_override: Optional[str],
        schema_override: Optional[str],
    ):
        """Return (catalog, schema) for the watermark registry."""
        from ...utils.yaml_loader import load_yaml_file

        if catalog_override and schema_override:
            return catalog_override, schema_override

        config = load_yaml_file(
            substitution_file, error_context=f"substitution file {substitution_file}"
        )
        env_tokens = (config or {}).get(env, {}) if isinstance(config, dict) else {}
        if not isinstance(env_tokens, dict):
            env_tokens = {}

        catalog = catalog_override or env_tokens.get("watermark_catalog") or "metadata"
        schema = schema_override or env_tokens.get("watermark_schema")
        if not schema:
            # ADR-004 default: per-env orchestration schema in shared metadata catalog.
            schema = f"{env}_orchestration"

        return str(catalog), str(schema)

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

    def _emit_for_flowgroup(
        self,
        fg,
        registry_table: str,
        *,
        apply: bool,
        warehouse_id: Optional[str] = None,
    ) -> None:
        from ...models.config import LoadSourceType

        load_group = f"{fg.pipeline}{_LOAD_GROUP_SEP}{fg.flowgroup}"

        # Cosmetic separator-collision warning (Tier 2 cosmetic; B2 enforces).
        for label, value in (("pipeline", fg.pipeline), ("flowgroup", fg.flowgroup)):
            if _LOAD_GROUP_SEP in value:
                click.echo(
                    f"WARNING: cosmetic — flowgroup field '{label}' contains "
                    f"'{_LOAD_GROUP_SEP}' separator literal: {value!r}. "
                    "Tier 2 does not fatal-abort on this; B2 (LHP-CFG-019) "
                    "will. Verify the composite load_group below is what "
                    "you intend.",
                    err=True,
                )

        jdbc_actions = []
        for action in fg.actions or []:
            source = action.source if isinstance(action.source, dict) else None
            if not source:
                continue
            if source.get("type") != LoadSourceType.JDBC_WATERMARK_V2.value:
                continue
            schema_name = source.get("schema_name") or (
                action.watermark.schema if action.watermark else None
            )
            table_name = source.get("table_name")
            source_system_id = (
                action.watermark.source_system_id if action.watermark else None
            )
            if not (schema_name and table_name and source_system_id):
                # Skip actions with incomplete watermark/source metadata; the
                # generator's validators flag these elsewhere. Step 4a SQL
                # cannot be composed without all three keys.
                continue
            jdbc_actions.append(
                {
                    "action_name": action.name,
                    "source_system_id": source_system_id,
                    "schema_name": schema_name,
                    "table_name": table_name,
                }
            )

        if not jdbc_actions:
            click.echo(
                f"-- Flowgroup '{fg.pipeline}::{fg.flowgroup}' has no "
                "jdbc_watermark_v2 actions. No seed SQL emitted."
            )
            return

        click.echo(f"-- seed-load-group: {fg.pipeline}::{fg.flowgroup}")
        click.echo(f"-- registry: {registry_table}")
        click.echo(f"-- load_group target: {load_group}")
        click.echo(f"-- actions: {len(jdbc_actions)}")
        click.echo("")

        all_sql: List[str] = []
        for entry in jdbc_actions:
            preview = _build_select_preview_sql(
                registry_table=registry_table,
                source_system_id=entry["source_system_id"],
                schema_name=entry["schema_name"],
                table_name=entry["table_name"],
            )
            insert = _build_insert_seed_sql(
                registry_table=registry_table,
                source_system_id=entry["source_system_id"],
                schema_name=entry["schema_name"],
                table_name=entry["table_name"],
                load_group_target=load_group,
            )

            click.echo(f"-- action: {entry['action_name']}")
            click.echo(f"-- table: {entry['schema_name']}.{entry['table_name']}")
            click.echo(preview)
            click.echo("")
            click.echo(insert)
            click.echo("")

            all_sql.append(preview)
            all_sql.append(insert)

        if apply:
            self._apply_via_sdk(all_sql, warehouse_id=warehouse_id)

    # ------------------------------------------------------------------
    # --apply path (lazy SDK import)
    # ------------------------------------------------------------------

    def _apply_via_sdk(
        self,
        statements: List[str],
        *,
        warehouse_id: Optional[str] = None,
    ) -> None:
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.service.sql import StatementState
        except ImportError as exc:
            raise click.ClickException(
                "--apply requires the databricks-sdk. Install it via:\n"
                "    pip install databricks-sdk\n"
                "Then re-run with --apply, or omit --apply to print SQL "
                "to stdout for manual execution.\n"
                f"Original import error: {exc}"
            ) from exc

        wh_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not wh_id:
            raise click.ClickException(
                "--apply requires a SQL warehouse. Set --warehouse-id <id> "
                "or export DATABRICKS_WAREHOUSE_ID=<id>."
            )

        client = WorkspaceClient(profile=_DEFAULT_DATABRICKS_PROFILE)
        for stmt in statements:
            # Skip dry-run-only SELECT preview blocks; Step 4a docs callout
            # is "Preview before execute". The INSERT statements are the
            # mutations.
            if stmt.lstrip().upper().startswith("SELECT"):
                continue
            click.echo(f"-- applying: {stmt.splitlines()[0]} ...", err=True)
            response = client.statement_execution.execute_statement(  # type: ignore[attr-defined]
                warehouse_id=wh_id,
                statement=stmt,
                wait_timeout="30s",
            )
            state = response.status.state if response.status else None
            if state != StatementState.SUCCEEDED:
                err = response.status.error if response.status else None
                err_msg = err.message if err else f"state={state}"
                raise click.ClickException(
                    f"Databricks statement failed: {err_msg}"
                )
            click.echo(f"-- ok: state={state.value}", err=True)


# ----------------------------------------------------------------------
# SQL composers — composed through SQLInputValidator + sql_literal.
# ----------------------------------------------------------------------


def _build_select_preview_sql(
    *,
    registry_table: str,
    source_system_id: str,
    schema_name: str,
    table_name: str,
) -> str:
    """Compose the Step 4a dry-run gate SELECT statement.

    Mirrors the origin doc's preview block verbatim, with all user-data
    values flowing through ``lhp_watermark.sql_safety``.
    """
    from lhp_watermark.sql_safety import (
        SQLInputValidator,
        sql_identifier,
        sql_literal,
    )

    src_lit = sql_literal(SQLInputValidator.string(source_system_id))
    schema_lit = sql_literal(SQLInputValidator.string(schema_name))
    table_lit = sql_literal(SQLInputValidator.string(table_name))
    table_ident = sql_identifier(registry_table)

    return (
        "SELECT count(*), max(watermark_time), max(watermark_value)\n"
        f"FROM   {table_ident}\n"
        f"WHERE  load_group = 'legacy'\n"
        f"  AND  source_system_id = {src_lit}\n"
        f"  AND  schema_name = {schema_lit}\n"
        f"  AND  table_name = {table_lit}\n"
        "  AND  status = 'completed';"
    )


def _build_insert_seed_sql(
    *,
    registry_table: str,
    source_system_id: str,
    schema_name: str,
    table_name: str,
    load_group_target: str,
) -> str:
    """Compose the Step 4a INSERT statement.

    Column list, ORDER BY, and LIMIT 1 are pinned to the origin doc and
    must not drift. ``error_class``/``error_message`` are intentionally
    omitted (default NULL) per the verified DDL ground truth in
    ``WatermarkManager._ensure_table_exists``.
    """
    from lhp_watermark.sql_safety import (
        SQLInputValidator,
        sql_identifier,
        sql_literal,
    )

    src_lit = sql_literal(SQLInputValidator.string(source_system_id))
    schema_lit = sql_literal(SQLInputValidator.string(schema_name))
    table_lit = sql_literal(SQLInputValidator.string(table_name))
    target_lit = sql_literal(SQLInputValidator.string(load_group_target))
    table_ident = sql_identifier(registry_table)

    return (
        f"INSERT INTO {table_ident}\n"
        "    (run_id, watermark_time, source_system_id, schema_name, table_name,\n"
        "     watermark_column_name, watermark_value, previous_watermark_value,\n"
        "     row_count, extraction_type,\n"
        "     bronze_stage_complete, silver_stage_complete, status,\n"
        "     created_at, completed_at, load_group)\n"
        "SELECT\n"
        "    concat('seed-', uuid()),\n"
        "    watermark_time, source_system_id, schema_name, table_name,\n"
        "    watermark_column_name, watermark_value, previous_watermark_value,\n"
        "    row_count,\n"
        "    'seeded',\n"
        "    bronze_stage_complete, silver_stage_complete, status,\n"
        "    current_timestamp(), completed_at,\n"
        f"    {target_lit}\n"
        f"FROM {table_ident}\n"
        "WHERE load_group = 'legacy'\n"
        f"  AND source_system_id = {src_lit}\n"
        f"  AND schema_name = {schema_lit}\n"
        f"  AND table_name = {table_lit}\n"
        "  AND status = 'completed'\n"
        "ORDER BY watermark_time DESC, completed_at DESC, run_id DESC\n"
        "LIMIT 1;"
    )
