"""Validate-tier2 command: run V1-V5 validation against a Tier 2 registry.

Replaces the manual notebook-upload + widget-set workflow from plan v5
§Phase 5 (S3): operator runs ``lhp validate-tier2 --env devtest`` and gets
exit-0-on-PASS / non-zero-on-FAIL with per-V diagnostic.

Implementation: workspace-import the existing notebook
``scripts/validation/validate_tier2_load_group.py`` to a shared workspace
path (``/Shared/lhp_validation/...``), submit it as a one-off Jobs API run
with widget params bound, poll until terminal, parse the
``dbutils.notebook.exit(json.dumps(...))`` payload, exit-code per
``overall_verdict``.

Notebook is the source of truth for V1-V5 logic — this command does not
duplicate validation logic, only orchestrates the run + parses the result.
Workstream A's V5 ships transparently because the notebook is the same.

For dress rehearsal (plan v5 §Phase 8.1 + B13), ``--catalog`` + ``--schema``
+ ``--probe-schema`` bypass env resolution.

``lhp_watermark`` import path (per ADR-002): the notebook prepends
``lhp_workspace_path`` (widget) to ``sys.path`` before importing
``lhp_watermark``. Operator runs ``lhp sync-runtime`` + ``databricks bundle
deploy`` to vendor the package into the bundle's workspace files, then
passes ``--lhp-workspace-path /Workspace/.../files`` here. NO wheel
install required — vendoring path is the supported flow.

See:
  docs/plans/pr-approved-into-watermark-hazy-lamport.md §Phase 5 + §8.0
  scripts/validation/validate_tier2_load_group.py — V1-V5 source
  ADR-002 — lhp_watermark vendoring (Path 5 Option A)
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import click

from ._databricks_sql import (
    DEFAULT_DATABRICKS_PROFILE,
    StatementExecutionError,
    make_workspace_client,
)
from .base_command import BaseCommand

logger = logging.getLogger(__name__)

# Default workspace target for the imported notebook (H4 + plan §Phase 5.4).
DEFAULT_WORKSPACE_TARGET = "/Shared/lhp_validation/validate_tier2_load_group"

# Source notebook path in the LHP repo.
_NOTEBOOK_RELATIVE = Path("scripts") / "validation" / "validate_tier2_load_group.py"


class ValidateTier2Command(BaseCommand):
    """Run the V1-V5 validation notebook against a Tier 2 registry."""

    def execute(
        self,
        *,
        env: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        probe_schema: Optional[str] = None,
        probe_table_name: str = "watermarks_v1_probe",
        cluster_id: Optional[str] = None,
        serverless: bool = False,
        serverless_environment_version: str = "3",
        lhp_workspace_path: Optional[str] = None,
        workspace_target: str = DEFAULT_WORKSPACE_TARGET,
        profile: Optional[str] = None,
        max_wait_seconds: int = 1800,
        poll_interval_seconds: int = 10,
    ) -> None:
        """Upload + execute the V1-V5 notebook; emit JSON; exit-code per verdict.

        Compute selection:
          - --serverless (default for envs without provisioned clusters): use
            serverless notebook compute via JobEnvironment + environment_key.
          - --cluster-id: use existing interactive/job cluster.
        Mutually exclusive; --serverless wins if both set.
        """
        self.setup_from_context()
        wm_catalog, wm_schema = self._resolve_namespace(env, catalog, schema)
        probe_namespace = probe_schema or self._default_probe_schema(env, wm_catalog)

        if not serverless:
            if not cluster_id:
                cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
            if not cluster_id:
                raise click.ClickException(
                    "Provide --serverless OR --cluster-id <id> "
                    "(or set DATABRICKS_CLUSTER_ID)."
                )

        # ADR-002 vendoring path: notebook imports lhp_watermark by prepending
        # lhp_workspace_path to sys.path. Operator runs `lhp sync-runtime` +
        # `databricks bundle deploy` to vendor the package, then passes the
        # workspace path here. Empty string = expect package on default
        # sys.path (rare; only valid if cluster has site-packages match).
        if lhp_workspace_path is None:
            lhp_workspace_path = os.environ.get("LHP_WORKSPACE_PATH", "")

        notebook_source = self._locate_notebook_source()
        client = make_workspace_client(profile=profile or DEFAULT_DATABRICKS_PROFILE)

        click.echo(
            f"-- validate-tier2: registry={wm_catalog}.{wm_schema}.watermarks "
            f"probe={probe_namespace}.{probe_table_name}",
            err=True,
        )

        try:
            self._upload_notebook(
                client=client,
                source_path=notebook_source,
                target_path=workspace_target,
            )
            run_id = self._submit_run(
                client=client,
                workspace_target=workspace_target,
                cluster_id=cluster_id,
                serverless=serverless,
                serverless_environment_version=serverless_environment_version,
                catalog=wm_catalog,
                schema=wm_schema,
                probe_namespace=probe_namespace,
                probe_table_name=probe_table_name,
                lhp_workspace_path=lhp_workspace_path,
            )
            click.echo(f"-- run submitted: run_id={run_id}", err=True)
            exit_value = self._wait_for_run(
                client=client,
                run_id=run_id,
                max_wait_seconds=max_wait_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )
        except StatementExecutionError as exc:
            raise click.ClickException(str(exc)) from exc

        verdict, summary = self._parse_exit_value(exit_value)
        click.echo(json.dumps(summary, indent=2))

        if verdict != "PASS":
            failed = summary.get("failed_invariants", [])
            raise click.ClickException(
                f"validate-tier2 verdict={verdict} failed_invariants={failed}"
            )
        click.echo("-- validate-tier2 PASS", err=True)

    # ------------------------------------------------------------------
    # Resolution helpers
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

    def _default_probe_schema(self, env: Optional[str], catalog: str) -> str:
        """Default probe schema: ``<catalog>.<env>_validation`` (plan H3)."""
        if env:
            return f"{catalog}.{env}_validation"
        return f"{catalog}.validation"

    def _locate_notebook_source(self) -> Path:
        """Find the validation notebook in the LHP repo (project root or installed)."""
        # Prefer project-root copy (development path).
        try:
            project_root = self.ensure_project_root()
        except Exception:  # noqa: BLE001
            project_root = None
        if project_root:
            candidate = project_root / _NOTEBOOK_RELATIVE
            if candidate.exists():
                return candidate
        # Fallback: search up from this file (installed wheel layout).
        here = Path(__file__).resolve()
        for parent in here.parents:
            candidate = parent / _NOTEBOOK_RELATIVE
            if candidate.exists():
                return candidate
        raise click.ClickException(
            f"Cannot locate {_NOTEBOOK_RELATIVE} in project root or package layout."
        )

    # ------------------------------------------------------------------
    # Workspace import + Jobs run
    # ------------------------------------------------------------------

    def _upload_notebook(
        self,
        *,
        client: Any,
        source_path: Path,
        target_path: str,
    ) -> None:
        """Workspace-import the notebook source as PYTHON / SOURCE.

        Uses ``client.workspace.upload`` if available (modern SDK) or falls
        back to ``client.workspace.import_`` for older SDK versions.
        """
        click.echo(f"-- uploading notebook → {target_path}", err=True)
        # Ensure parent directory exists (best-effort; SDK creates as needed).
        parent_dir = target_path.rsplit("/", 1)[0]
        try:
            client.workspace.mkdirs(path=parent_dir)
        except Exception as exc:  # noqa: BLE001
            logger.debug("workspace.mkdirs(%s) ignored: %s", parent_dir, exc)

        content = source_path.read_bytes()
        try:
            from databricks.sdk.service.workspace import (
                ImportFormat,
                Language,
            )
        except ImportError as exc:
            raise StatementExecutionError(
                f"databricks-sdk missing workspace types: {exc}"
            ) from exc

        # Modern SDK: client.workspace.upload(path, content, format=..., overwrite=True)
        upload = getattr(client.workspace, "upload", None)
        if callable(upload):
            upload(
                path=target_path,
                content=content,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )
            return
        # Fallback: import_ with base64-encoded content
        import base64

        client.workspace.import_(
            path=target_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=base64.b64encode(content).decode("ascii"),
            overwrite=True,
        )

    def _submit_run(
        self,
        *,
        client: Any,
        workspace_target: str,
        cluster_id: Optional[str],
        serverless: bool,
        serverless_environment_version: str,
        catalog: str,
        schema: str,
        probe_namespace: str,
        probe_table_name: str,
        lhp_workspace_path: str = "",
    ) -> int:
        """Submit a one-off Jobs run executing the notebook with widget params.

        Compute selection: serverless (JobEnvironment + environment_key) or
        existing_cluster_id. Probe table FQN is composed by the notebook from
        ``<catalog>.<schema>.<probe_table_name>``; B9 rehearsal redirect uses
        the probe schema's catalog/schema components.
        """
        try:
            from databricks.sdk.service.jobs import (
                JobEnvironment,
                NotebookTask,
                SubmitTask,
            )
        except ImportError as exc:
            raise StatementExecutionError(
                f"databricks-sdk missing jobs types: {exc}"
            ) from exc

        probe_catalog, probe_schema = _split_two_part(probe_namespace)
        notebook_params = {
            "catalog": probe_catalog or catalog,
            "schema": probe_schema or schema,
            "probe_table_name": probe_table_name,
            "cleanup_on_success": "true",
            "lhp_workspace_path": lhp_workspace_path,
        }

        environments = None
        if serverless:
            try:
                from databricks.sdk.service.compute import Environment
            except ImportError as exc:
                raise StatementExecutionError(
                    f"databricks-sdk missing compute.Environment: {exc}"
                ) from exc
            env_key = "serverless_env"
            environments = [
                JobEnvironment(
                    environment_key=env_key,
                    spec=Environment(client=serverless_environment_version),
                )
            ]
            task = SubmitTask(
                task_key="validate_tier2",
                notebook_task=NotebookTask(
                    notebook_path=workspace_target,
                    base_parameters=notebook_params,
                ),
                environment_key=env_key,
            )
        else:
            task = SubmitTask(
                task_key="validate_tier2",
                notebook_task=NotebookTask(
                    notebook_path=workspace_target,
                    base_parameters=notebook_params,
                ),
                existing_cluster_id=cluster_id,
            )

        submit_kwargs: dict[str, Any] = {
            "run_name": f"lhp-validate-tier2-{int(time.time())}",
            "tasks": [task],
        }
        if environments is not None:
            submit_kwargs["environments"] = environments
        run = client.jobs.submit(**submit_kwargs)
        run_id = getattr(run, "run_id", None)
        if run_id is None and hasattr(run, "response"):
            run_id = getattr(run.response, "run_id", None)
        if run_id is None:
            raise StatementExecutionError(
                f"jobs.submit returned no run_id; response={run!r}"
            )
        return int(run_id)

    def _wait_for_run(
        self,
        *,
        client: Any,
        run_id: int,
        max_wait_seconds: int,
        poll_interval_seconds: int,
    ) -> str:
        """Poll the run until terminal; return the notebook exit value string."""
        try:
            from databricks.sdk.service.jobs import (
                RunLifeCycleState,
                RunResultState,
            )
        except ImportError as exc:
            raise StatementExecutionError(
                f"databricks-sdk missing jobs state types: {exc}"
            ) from exc

        elapsed = 0
        while True:
            run = client.jobs.get_run(run_id=run_id)
            state = run.state
            life_cycle = getattr(state, "life_cycle_state", None) if state else None
            result = getattr(state, "result_state", None) if state else None

            if life_cycle == RunLifeCycleState.TERMINATED:
                if result == RunResultState.SUCCESS:
                    output = client.jobs.get_run_output(run_id=run_id)
                    notebook_exit = getattr(output, "notebook_output", None)
                    exit_value = (
                        getattr(notebook_exit, "result", None)
                        if notebook_exit
                        else None
                    )
                    if exit_value is None:
                        raise StatementExecutionError(
                            f"run_id={run_id} SUCCESS but no notebook_output.result"
                        )
                    return str(exit_value)
                err = (
                    getattr(state, "state_message", "") if state else ""
                )
                raise StatementExecutionError(
                    f"run_id={run_id} terminated result_state={result} "
                    f"msg={err}"
                )
            if life_cycle in (
                RunLifeCycleState.INTERNAL_ERROR,
                RunLifeCycleState.SKIPPED,
            ):
                raise StatementExecutionError(
                    f"run_id={run_id} life_cycle_state={life_cycle}"
                )

            if elapsed >= max_wait_seconds:
                # Best-effort cancel
                try:
                    client.jobs.cancel_run(run_id=run_id)
                except Exception:  # noqa: BLE001
                    pass
                raise StatementExecutionError(
                    f"run_id={run_id} exceeded max_wait_seconds={max_wait_seconds}"
                )

            time.sleep(poll_interval_seconds)
            elapsed += poll_interval_seconds

    def _parse_exit_value(self, exit_value: str) -> Tuple[str, Dict[str, Any]]:
        """Parse the notebook exit JSON; return ``(verdict, summary)``."""
        try:
            payload = json.loads(exit_value)
        except json.JSONDecodeError as exc:
            raise click.ClickException(
                f"notebook exit value is not valid JSON: {exit_value!r} ({exc})"
            ) from exc
        if not isinstance(payload, dict):
            raise click.ClickException(
                f"notebook exit JSON is not a dict: {payload!r}"
            )
        verdict = str(payload.get("overall_verdict", "UNKNOWN"))
        return verdict, payload


def _split_two_part(fqn: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Split ``catalog.schema`` into ``(catalog, schema)``. Tolerates None."""
    if not fqn:
        return None, None
    parts = fqn.split(".")
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return None, parts[0]
    return None, None
