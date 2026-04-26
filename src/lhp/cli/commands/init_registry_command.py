"""Init-registry command: trigger Tier 2 auto-DDL on the watermark registry.

Replaces the fragile pipeline-run trigger from plan v5 §Phase 2.2 (B4) and
the workspace-cluster fallback notebook (H10, H14). Issues the same
``_ensure_table_exists`` SQL that ``WatermarkManager.__init__`` would run,
but via the SQL Statement Execution API — no Spark session, no wheel
attachment, no JDBC noise.

Mirrors ``WatermarkManager._ensure_table_exists`` (src/lhp_watermark/
watermark_manager.py:202-276):

  1. ``SHOW TABLES IN <ns> LIKE 'watermarks'`` — table existence probe
  2. If absent → ``CREATE TABLE … CLUSTER BY (source_system_id, load_group,
     schema_name, table_name)`` (Tier-2-shaped from the start)
  3. If present:
     a. ``DESCRIBE TABLE`` — probe column list; ALTER ADD COLUMNS load_group
        if missing
     b. ``DESCRIBE DETAIL`` — probe clustering; ALTER CLUSTER BY if drifted

Ground truth for the DDL strings is ``WatermarkManager._ensure_table_exists``
— the CREATE/ALTER strings here MUST stay in lockstep with that source.

Idempotency: the conditional ALTERs short-circuit when the target shape is
already in place — zero ALTER SQL on subsequent inits, zero new history
versions (per plan v5 §Phase 2.3 + M14).

For the dress-rehearsal path (plan v5 §Phase 8.1, B13), ``--catalog`` +
``--schema`` bypass env-allowlist resolution.

See:
  docs/plans/pr-approved-into-watermark-hazy-lamport.md §Phase 2.2 + §8.0
  src/lhp_watermark/watermark_manager.py::_ensure_table_exists
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional, Tuple

import click

from ...utils.error_formatter import ErrorCategory, LHPError
from ._databricks_sql import (
    DEFAULT_DATABRICKS_PROFILE,
    StatementExecutionError,
    collect_rows,
    execute_and_wait,
    make_workspace_client,
)
from .base_command import BaseCommand

logger = logging.getLogger(__name__)

# Target Tier 2 clustering key — must match
# WatermarkManager._TARGET_CLUSTERING in src/lhp_watermark/watermark_manager.py.
_TARGET_CLUSTERING: Tuple[str, ...] = (
    "source_system_id",
    "load_group",
    "schema_name",
    "table_name",
)


class InitRegistryCommand(BaseCommand):
    """Trigger Tier 2 auto-DDL on a watermark registry via SQL Statement API."""

    def execute(
        self,
        *,
        env: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        profile: Optional[str] = None,
        max_wait_seconds: int = 600,
    ) -> None:
        """Resolve registry namespace and run the auto-DDL SQL probes + ALTERs.

        Either ``--env`` (substitution-resolved) OR explicit ``--catalog`` +
        ``--schema`` must be provided. Explicit args bypass env allowlist
        per plan §Phase 8.1 (B13 — dress rehearsal path).
        """
        self.setup_from_context()
        wm_catalog, wm_schema = self._resolve_namespace(env, catalog, schema)
        registry_table = f"{wm_catalog}.{wm_schema}.watermarks"

        wh_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not wh_id:
            raise click.ClickException(
                "Set --warehouse-id <id> or export DATABRICKS_WAREHOUSE_ID=<id>."
            )

        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        click.echo(f"-- init-registry: {registry_table}", err=True)
        try:
            self._ensure_table_exists(
                client=client,
                warehouse_id=wh_id,
                catalog=wm_catalog,
                schema=wm_schema,
                registry_table=registry_table,
                max_wait_seconds=max_wait_seconds,
            )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

    # ------------------------------------------------------------------
    # Namespace resolution
    # ------------------------------------------------------------------

    def _resolve_namespace(
        self,
        env: Optional[str],
        catalog_override: Optional[str],
        schema_override: Optional[str],
    ) -> Tuple[str, str]:
        """Return ``(catalog, schema)`` for the watermark registry.

        Two paths:
          - Explicit ``--catalog`` + ``--schema`` (rehearsal / B13 bypass).
          - ``--env`` → look up ``substitutions/<env>.yaml`` for
            ``watermark_catalog`` / ``watermark_schema``; fall back to
            ``metadata`` / ``<env>_orchestration`` (matches
            ``SeedLoadGroupCommand._resolve_registry_namespace``).
        """
        if catalog_override and schema_override:
            return catalog_override, schema_override

        if not env:
            raise click.UsageError(
                "Must provide either --env OR both --catalog and --schema."
            )

        substitution_file = self.check_substitution_file(env)

        from ...utils.yaml_loader import load_yaml_file

        config = load_yaml_file(
            substitution_file,
            error_context=f"substitution file {substitution_file}",
        )
        env_tokens = (config or {}).get(env, {}) if isinstance(config, dict) else {}
        if not isinstance(env_tokens, dict):
            env_tokens = {}

        catalog = catalog_override or env_tokens.get("watermark_catalog") or "metadata"
        schema = schema_override or env_tokens.get("watermark_schema")
        if not schema:
            schema = f"{env}_orchestration"

        return str(catalog), str(schema)

    # ------------------------------------------------------------------
    # Auto-DDL — mirrors WatermarkManager._ensure_table_exists
    # ------------------------------------------------------------------

    def _ensure_table_exists(
        self,
        *,
        client,
        warehouse_id: str,
        catalog: str,
        schema: str,
        registry_table: str,
        max_wait_seconds: int,
    ) -> None:
        namespace = f"{catalog}.{schema}"

        # Probe: does ``watermarks`` exist in the namespace?
        show_resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=f"SHOW TABLES IN {namespace} LIKE 'watermarks'",
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(show_resp)
        table_exists = bool(rows)

        if not table_exists:
            click.echo(f"-- creating watermarks table: {registry_table}", err=True)
            execute_and_wait(
                client=client,
                warehouse_id=warehouse_id,
                statement=_create_table_sql(registry_table),
                max_wait_seconds=max_wait_seconds,
            )
            click.echo(f"-- created (Tier-2-shaped)", err=True)
            return

        # Pre-existing table: conditional ALTERs.
        if not self._has_load_group_column(
            client=client,
            warehouse_id=warehouse_id,
            registry_table=registry_table,
            max_wait_seconds=max_wait_seconds,
        ):
            click.echo(f"-- ALTER ADD COLUMNS (load_group STRING)", err=True)
            execute_and_wait(
                client=client,
                warehouse_id=warehouse_id,
                statement=(
                    f"ALTER TABLE {registry_table} ADD COLUMNS (load_group STRING)"
                ),
                max_wait_seconds=max_wait_seconds,
            )

        if not self._clustering_matches_target(
            client=client,
            warehouse_id=warehouse_id,
            registry_table=registry_table,
            max_wait_seconds=max_wait_seconds,
        ):
            target_csv = ", ".join(_TARGET_CLUSTERING)
            click.echo(
                f"-- ALTER CLUSTER BY ({target_csv})", err=True
            )
            execute_and_wait(
                client=client,
                warehouse_id=warehouse_id,
                statement=(
                    f"ALTER TABLE {registry_table} CLUSTER BY ({target_csv})"
                ),
                max_wait_seconds=max_wait_seconds,
            )

        click.echo(f"-- init-registry complete: {registry_table}", err=True)

    def _has_load_group_column(
        self,
        *,
        client,
        warehouse_id: str,
        registry_table: str,
        max_wait_seconds: int,
    ) -> bool:
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE TABLE {registry_table}",
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        for row in rows:
            # DESCRIBE TABLE row shape: [col_name, data_type, comment].
            if not row:
                continue
            col_name = row[0] if row[0] is not None else ""
            if col_name == "load_group":
                return True
        return False

    def _clustering_matches_target(
        self,
        *,
        client,
        warehouse_id: str,
        registry_table: str,
        max_wait_seconds: int,
    ) -> bool:
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE DETAIL {registry_table}",
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        if not rows:
            return False
        # Parse clustering columns from the response. DESCRIBE DETAIL returns
        # one wide row whose schema columns include ``clusteringColumns``.
        # Locate that column index from response.manifest.schema.columns.
        manifest = getattr(resp, "manifest", None)
        if manifest is None:
            return False
        schema_obj = getattr(manifest, "schema", None)
        if schema_obj is None:
            return False
        columns = getattr(schema_obj, "columns", None)
        if not columns:
            return False
        try:
            idx = next(
                i for i, c in enumerate(columns) if c.name == "clusteringColumns"
            )
        except StopIteration:
            return False
        first = rows[0]
        if idx >= len(first):
            return False
        current = first[idx]
        if current is None:
            return False
        # SQL Statement API serialises array<string> as a JSON-encoded string.
        parsed = _parse_clustering_columns(current)
        return tuple(parsed) == _TARGET_CLUSTERING


def _parse_clustering_columns(value) -> List[str]:
    """Parse a ``clusteringColumns`` cell from DESCRIBE DETAIL.

    The SQL Statement API serialises the ``array<string>`` column as a
    JSON-encoded string (e.g. ``'["source_system_id", "schema_name"]'``).
    Tolerates already-parsed lists (defensive, in case SDK changes shape).
    """
    import json

    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(v) for v in parsed]
        return []
    return []


def _create_table_sql(registry_table: str) -> str:
    """Tier-2-shaped CREATE TABLE — pinned to WatermarkManager DDL."""
    return f"""
        CREATE TABLE IF NOT EXISTS {registry_table} (
            run_id STRING NOT NULL,
            watermark_time TIMESTAMP NOT NULL,
            source_system_id STRING NOT NULL,
            schema_name STRING NOT NULL,
            table_name STRING NOT NULL,
            watermark_column_name STRING,
            watermark_value STRING,
            previous_watermark_value STRING,
            row_count BIGINT,
            extraction_type STRING NOT NULL,
            bronze_stage_complete BOOLEAN NOT NULL,
            silver_stage_complete BOOLEAN NOT NULL,
            status STRING NOT NULL,
            error_class STRING,
            error_message STRING,
            created_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            load_group STRING,
            CONSTRAINT pk_watermarks PRIMARY KEY (run_id)
        )
        USING DELTA
        CLUSTER BY (source_system_id, load_group, schema_name, table_name)
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """.strip()
