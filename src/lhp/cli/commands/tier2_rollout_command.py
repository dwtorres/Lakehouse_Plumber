"""Tier 2 rollout umbrella subcommands: preflight | backfill | optimize | rehearse | seed.

Per plan v5 §Workstream B (S5/S8). Single Python orchestrator that uses
the shared ``_databricks_sql.execute_and_wait`` polling helper. Subcommands
correspond to discrete phases of the operator runbook:

  preflight  — §Phase 1: DESCRIBE DETAIL shape gate + Delta protocol gate
               (H23) + extraction_type consumer audit hint.
  backfill   — §Phase 3: B6 pre-check + UPDATE legacy + post-verify.
  optimize   — §Phase 4: OPTIMIZE [FULL] (B5 first-run-only via --full flag,
               M19 cost note via --skip-if-clustered).
  rehearse   — §Phase 8.1: dress rehearsal — version-pinned deep clone into
               schema-redirect target (B9), then init + backfill + optimize
               + validate against the rehearsal namespace. Does NOT bundle-
               deploy (B14: rehearsal subset only).
  seed       — wraps existing ``lhp seed-load-group`` for umbrella parity.

Subcommands NOT shipped here:
  pause-tier1 / resume-tier1 — depend on Tier-1 schedule inventory which is
  per-deployment, not per-bundle. Track separately (M32 + S7).

Namespace resolution mirrors init-registry / validate-tier2: env-substituted
OR explicit --catalog/--schema (B13 rehearsal bypass).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, List, Optional, Tuple

import click

from ._databricks_sql import (
    DEFAULT_DATABRICKS_PROFILE,
    StatementExecutionError,
    collect_rows,
    execute_and_wait,
    make_workspace_client,
)
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


_TARGET_CLUSTERING: Tuple[str, ...] = (
    "source_system_id",
    "load_group",
    "schema_name",
    "table_name",
)

_MIN_READER_VERSION = 3
_MIN_WRITER_VERSION = 7


class Tier2RolloutCommand(BaseCommand):
    """Umbrella for Tier 2 rollout subcommands."""

    # ------------------------------------------------------------------
    # Shared resolution + client setup
    # ------------------------------------------------------------------

    def _resolve_namespace(
        self,
        env: Optional[str],
        catalog_override: Optional[str],
        schema_override: Optional[str],
    ) -> Tuple[str, str]:
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

    def _require_warehouse(self, warehouse_id: Optional[str]) -> str:
        wh_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not wh_id:
            raise click.ClickException(
                "Set --warehouse-id <id> or export DATABRICKS_WAREHOUSE_ID=<id>."
            )
        return wh_id

    # ------------------------------------------------------------------
    # preflight
    # ------------------------------------------------------------------

    def preflight(
        self,
        *,
        env: Optional[str],
        catalog: Optional[str],
        schema: Optional[str],
        warehouse_id: Optional[str],
        profile: Optional[str],
        max_wait_seconds: int = 600,
    ) -> None:
        """§Phase 1.A + §Phase 1.D + H23 protocol gate. Read-only."""
        self.setup_from_context()
        catalog, schema = self._resolve_namespace(env, catalog, schema)
        registry = f"{catalog}.{schema}.watermarks"
        wh_id = self._require_warehouse(warehouse_id)
        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        try:
            shape = self._capture_describe_detail(
                client=client,
                warehouse_id=wh_id,
                registry=registry,
                max_wait_seconds=max_wait_seconds,
            )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

        click.echo(json.dumps(shape, indent=2))

        gate = self._evaluate_preflight_gate(shape)
        click.echo(f"-- preflight gate: {gate['verdict']}", err=True)
        for note in gate["notes"]:
            click.echo(f"--   {note}", err=True)
        if gate["verdict"] != "PASS":
            raise click.ClickException(
                f"preflight gate {gate['verdict']}: {'; '.join(gate['notes'])}"
            )

    def _capture_describe_detail(
        self, *, client: Any, warehouse_id: str, registry: str, max_wait_seconds: int
    ) -> dict:
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE DETAIL {registry}",
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        if not rows:
            return {"registry": registry, "found": False}

        manifest = getattr(resp, "manifest", None)
        col_names: List[str] = []
        if manifest is not None:
            schema_obj = getattr(manifest, "schema", None)
            if schema_obj is not None:
                cols = getattr(schema_obj, "columns", None) or []
                col_names = [c.name for c in cols]

        first = rows[0]
        cell_map = {}
        for i, name in enumerate(col_names):
            cell_map[name] = first[i] if i < len(first) else None

        # Parse list-shaped fields
        clustering = _parse_string_list(cell_map.get("clusteringColumns"))
        partitions = _parse_string_list(cell_map.get("partitionColumns"))

        # Coerce reader/writer versions to ints when present.
        reader_v = _coerce_int(cell_map.get("minReaderVersion"))
        writer_v = _coerce_int(cell_map.get("minWriterVersion"))

        return {
            "registry": registry,
            "found": True,
            "partitionColumns": partitions,
            "clusteringColumns": clustering,
            "numFiles": _coerce_int(cell_map.get("numFiles")),
            "sizeInBytes": _coerce_int(cell_map.get("sizeInBytes")),
            "minReaderVersion": reader_v,
            "minWriterVersion": writer_v,
        }

    def _evaluate_preflight_gate(self, shape: dict) -> dict:
        notes: List[str] = []
        if not shape.get("found"):
            return {
                "verdict": "STOP",
                "notes": [f"registry {shape.get('registry')} does not exist"],
            }

        partitions = shape.get("partitionColumns") or []
        clustering = tuple(shape.get("clusteringColumns") or [])
        reader_v = shape.get("minReaderVersion")
        writer_v = shape.get("minWriterVersion")

        if partitions:
            notes.append(
                f"partitioned table (partitionColumns={partitions}); "
                "OPTIMIZE FULL will rewrite every file. Plan maintenance window."
            )
            return {"verdict": "STOP", "notes": notes}

        if reader_v is not None and reader_v < _MIN_READER_VERSION:
            notes.append(
                f"minReaderVersion={reader_v} < {_MIN_READER_VERSION}; "
                "Tier 2 ALTER may silently upgrade protocol (irreversible). "
                "Coordinate Delta protocol upgrade as separate change."
            )
            return {"verdict": "STOP", "notes": notes}
        if writer_v is not None and writer_v < _MIN_WRITER_VERSION:
            notes.append(
                f"minWriterVersion={writer_v} < {_MIN_WRITER_VERSION}; "
                "ALTER TABLE CLUSTER BY requires writer >= 7."
            )
            return {"verdict": "STOP", "notes": notes}

        if clustering == _TARGET_CLUSTERING:
            notes.append("already Tier-2-shaped (clustering matches target)")
            return {"verdict": "PASS", "notes": notes}

        # Tier-1 baseline expected: (source_system_id, schema_name, table_name)
        tier1_clustering = (
            "source_system_id",
            "schema_name",
            "table_name",
        )
        if clustering == tier1_clustering:
            notes.append(
                "Tier-1 baseline clustering detected; Tier 2 ALTERs will fire on init"
            )
            return {"verdict": "PASS", "notes": notes}

        notes.append(
            f"unexpected clusteringColumns={list(clustering)}; "
            "confirm with ADR-004 owner before proceeding"
        )
        return {"verdict": "INVESTIGATE", "notes": notes}

    # ------------------------------------------------------------------
    # backfill
    # ------------------------------------------------------------------

    def backfill(
        self,
        *,
        env: Optional[str],
        catalog: Optional[str],
        schema: Optional[str],
        warehouse_id: Optional[str],
        profile: Optional[str],
        max_wait_seconds: int = 1800,
        skip_pre_check: bool = False,
    ) -> None:
        """§Phase 3: B6 pre-check + UPDATE legacy + post-verify."""
        self.setup_from_context()
        catalog, schema = self._resolve_namespace(env, catalog, schema)
        registry = f"{catalog}.{schema}.watermarks"
        wh_id = self._require_warehouse(warehouse_id)
        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        try:
            if not skip_pre_check:
                pre = self._backfill_pre_check(
                    client=client,
                    warehouse_id=wh_id,
                    registry=registry,
                    max_wait_seconds=max_wait_seconds,
                )
                click.echo(json.dumps({"pre_check": pre}, indent=2), err=True)
                if pre["unexpected_rows"] > 0:
                    raise click.ClickException(
                        f"pre-check found {pre['unexpected_rows']} unexpected rows "
                        "with non-NULL non-'legacy' load_group; investigate before "
                        "running UPDATE"
                    )

            click.echo(f"-- backfill UPDATE on {registry}", err=True)
            execute_and_wait(
                client=client,
                warehouse_id=wh_id,
                statement=(
                    f"UPDATE {registry} "
                    "SET load_group = 'legacy' "
                    "WHERE load_group IS NULL"
                ),
                max_wait_seconds=max_wait_seconds,
            )

            post = self._backfill_post_check(
                client=client,
                warehouse_id=wh_id,
                registry=registry,
                max_wait_seconds=max_wait_seconds,
            )
            click.echo(json.dumps({"post_check": post}, indent=2))
            if post["null_rows"] != 0:
                raise click.ClickException(
                    f"post-update null_rows={post['null_rows']} (expected 0)"
                )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

    def _backfill_pre_check(
        self, *, client: Any, warehouse_id: str, registry: str, max_wait_seconds: int
    ) -> dict:
        # B6: COUNT DISTINCT excludes NULL — use COUNT_IF for null counting.
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=(
                "SELECT "
                "COUNT(DISTINCT load_group) AS distinct_non_null, "
                "COUNT_IF(load_group IS NULL) AS null_rows, "
                "COUNT_IF(load_group IS NOT NULL AND load_group <> 'legacy') "
                "AS unexpected_rows "
                f"FROM {registry}"
            ),
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        if not rows:
            return {"distinct_non_null": 0, "null_rows": 0, "unexpected_rows": 0}
        first = rows[0]
        return {
            "distinct_non_null": _coerce_int(first[0]) or 0,
            "null_rows": _coerce_int(first[1]) or 0,
            "unexpected_rows": _coerce_int(first[2]) or 0,
        }

    def _backfill_post_check(
        self, *, client: Any, warehouse_id: str, registry: str, max_wait_seconds: int
    ) -> dict:
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=(
                "SELECT "
                "COUNT(*) AS total_rows, "
                "COUNT_IF(load_group = 'legacy') AS legacy_rows, "
                "COUNT_IF(load_group IS NULL) AS null_rows "
                f"FROM {registry}"
            ),
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        if not rows:
            return {"total_rows": 0, "legacy_rows": 0, "null_rows": 0}
        first = rows[0]
        return {
            "total_rows": _coerce_int(first[0]) or 0,
            "legacy_rows": _coerce_int(first[1]) or 0,
            "null_rows": _coerce_int(first[2]) or 0,
        }

    # ------------------------------------------------------------------
    # optimize
    # ------------------------------------------------------------------

    def optimize(
        self,
        *,
        env: Optional[str],
        catalog: Optional[str],
        schema: Optional[str],
        warehouse_id: Optional[str],
        profile: Optional[str],
        full: bool = False,
        max_wait_seconds: int = 7200,
    ) -> None:
        """§Phase 4: OPTIMIZE [FULL]. B5 — use --full on first run post-clustering-change."""
        self.setup_from_context()
        catalog, schema = self._resolve_namespace(env, catalog, schema)
        registry = f"{catalog}.{schema}.watermarks"
        wh_id = self._require_warehouse(warehouse_id)
        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        # Capture pre-OPTIMIZE numFiles for delta reporting (L15).
        pre_shape = self._capture_describe_detail(
            client=client,
            warehouse_id=wh_id,
            registry=registry,
            max_wait_seconds=max_wait_seconds,
        )

        statement = f"OPTIMIZE {registry}" + (" FULL" if full else "")
        click.echo(f"-- {statement}", err=True)

        try:
            execute_and_wait(
                client=client,
                warehouse_id=wh_id,
                statement=statement,
                max_wait_seconds=max_wait_seconds,
            )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

        post_shape = self._capture_describe_detail(
            client=client,
            warehouse_id=wh_id,
            registry=registry,
            max_wait_seconds=max_wait_seconds,
        )
        delta = {
            "registry": registry,
            "full": full,
            "numFiles_pre": pre_shape.get("numFiles"),
            "numFiles_post": post_shape.get("numFiles"),
        }
        click.echo(json.dumps(delta, indent=2))

    # ------------------------------------------------------------------
    # rehearse
    # ------------------------------------------------------------------

    def rehearse(
        self,
        *,
        source_table: str,
        target_schema: str,
        warehouse_id: Optional[str],
        profile: Optional[str],
        max_wait_seconds: int = 7200,
        skip_optimize: bool = False,
    ) -> None:
        """§Phase 8.1 dress rehearsal: version-pinned deep clone + Tier 2 phases on clone.

        ``source_table`` is FQN of live registry (e.g. ``metadata.prod_orchestration.watermarks``).
        ``target_schema`` is FQN of the rehearsal-clone schema (e.g. ``metadata.prod_dryrun_orchestration``).
        Clone is created at ``<target_schema>.watermarks`` (table name MUST be
        ``watermarks`` per WatermarkManager constructor — L17).

        Does NOT run ``databricks bundle deploy`` (B14: rehearsal subset only).
        Does NOT exercise concurrent writers (H21: rehearsal PASS does NOT
        imply prod-safe under concurrent writers).
        """
        self.setup_from_context()
        wh_id = self._require_warehouse(warehouse_id)
        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        # Validate target_schema is two-part; clone into <target_schema>.watermarks
        if "." not in target_schema:
            raise click.UsageError(
                f"--target-schema must be 'catalog.schema'; got {target_schema!r}"
            )
        clone_table = f"{target_schema}.watermarks"

        try:
            # H15: pin source version BEFORE clone.
            version = self._capture_baseline_version(
                client=client,
                warehouse_id=wh_id,
                source_table=source_table,
                max_wait_seconds=max_wait_seconds,
            )
            click.echo(
                f"-- rehearsal: source={source_table} version={version} "
                f"target={clone_table}",
                err=True,
            )

            # Drop prior clone (L14: retention N=1).
            execute_and_wait(
                client=client,
                warehouse_id=wh_id,
                statement=f"DROP TABLE IF EXISTS {clone_table}",
                max_wait_seconds=max_wait_seconds,
            )

            # Create version-pinned deep clone.
            click.echo(
                f"-- DEEP CLONE {source_table} VERSION AS OF {version} "
                f"-> {clone_table}",
                err=True,
            )
            execute_and_wait(
                client=client,
                warehouse_id=wh_id,
                statement=(
                    f"CREATE TABLE {clone_table} "
                    f"DEEP CLONE {source_table} VERSION AS OF {version}"
                ),
                max_wait_seconds=max_wait_seconds,
            )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

        # Now run init + backfill + optimize + (validate) against the clone.
        # Use the schema-redirect approach: pass --catalog/--schema explicitly
        # to bypass env allowlist (B13).
        clone_catalog, clone_schema_only = target_schema.split(".", 1)

        # init via init-registry command (re-use)
        from .init_registry_command import InitRegistryCommand

        init_cmd = InitRegistryCommand()
        init_cmd.setup_from_context = lambda: None
        init_cmd.execute(
            catalog=clone_catalog,
            schema=clone_schema_only,
            warehouse_id=wh_id,
            profile=profile,
            max_wait_seconds=max_wait_seconds,
        )

        # backfill
        self.backfill(
            env=None,
            catalog=clone_catalog,
            schema=clone_schema_only,
            warehouse_id=wh_id,
            profile=profile,
            max_wait_seconds=max_wait_seconds,
        )

        # optimize FULL (B5 first-run)
        if not skip_optimize:
            self.optimize(
                env=None,
                catalog=clone_catalog,
                schema=clone_schema_only,
                warehouse_id=wh_id,
                profile=profile,
                full=True,
                max_wait_seconds=max_wait_seconds,
            )

        click.echo(
            "-- rehearsal phases complete (init + backfill + optimize); "
            "run validate-tier2 separately for V1-V5",
            err=True,
        )
        click.echo(
            "-- WARNING (H21): rehearsal PASS does NOT imply prod-safe under "
            "concurrent writers; pause Tier 1 fully for real cutover",
            err=True,
        )

    def _capture_baseline_version(
        self,
        *,
        client: Any,
        warehouse_id: str,
        source_table: str,
        max_wait_seconds: int,
    ) -> int:
        resp = execute_and_wait(
            client=client,
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE HISTORY {source_table} LIMIT 1",
            max_wait_seconds=max_wait_seconds,
        )
        rows = collect_rows(resp)
        if not rows or not rows[0]:
            raise StatementExecutionError(
                f"DESCRIBE HISTORY {source_table} returned no rows"
            )
        v = _coerce_int(rows[0][0])
        if v is None:
            raise StatementExecutionError(
                f"DESCRIBE HISTORY first column not int: {rows[0][0]!r}"
            )
        return v


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _parse_string_list(value: Any) -> List[str]:
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


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
