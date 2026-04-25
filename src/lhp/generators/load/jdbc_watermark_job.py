"""Generator for JDBC watermark v2 extraction Job notebook.

Produces two artifacts from a single jdbc_watermark_v2 action:
1. An extraction notebook that uses WatermarkManager for HWM tracking
   and writes Parquet to a landing path (stored as auxiliary file)
2. A CloudFiles DLT stub that reads from the landing path (returned as
   the primary output, delegated to CloudFilesLoadGenerator)
"""

import logging
import re
from pathlib import PurePosixPath
from typing import Any, Dict

import black

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorCategory, LHPConfigError

logger = logging.getLogger(__name__)

_CLOUDFILES_PASSTHROUGH_KEYS = {
    "options",
    "reader_options",
    "format_options",
    "schema",
    "schema_file",
    "readMode",
    "schema_location",
    "schema_infer_column_types",
    "max_files_per_trigger",
    "schema_evolution_mode",
    "rescue_data_column",
}

# Pattern to match ${secret:scope/key} references
_SECRET_REF_RE = re.compile(r"\$\{secret:([^/]+)/([^}]+)\}")

# Matches UC managed volume paths: /Volumes/<catalog>/<schema>/...
_UC_VOLUME_RE = re.compile(r"^/Volumes/([^/]+)/([^/]+)/")


def _resolve_secret_refs(value: str) -> str:
    """Convert ${secret:scope/key} to dbutils.secrets.get() call.

    Returns a Python expression string (not a literal).
    """
    match = _SECRET_REF_RE.match(value)
    if match:
        scope, key = match.group(1), match.group(2)
        return f'dbutils.secrets.get(scope="{scope}", key="{key}")'
    return f'"{value}"'


def _default_schema_location(landing_path: str, action_name: str) -> str:
    """Build a stable Auto Loader schemaLocation sibling to the landing path."""
    landing = PurePosixPath(landing_path)
    parent = (
        str(landing.parent) if str(landing.parent) not in ("", ".") else landing_path
    )
    return f"{parent.rstrip('/')}/_lhp_schema/{action_name}"


# ADR-001 §Decision: the extraction template writes Parquet at
# ``{landing_path}/_lhp_runs/{run_id}/`` (see
# ``src/lhp/templates/load/jdbc_watermark_job.py.j2`` helper
# ``_run_landing_path``). AutoLoader's schema-inference listing on the bare
# landing path does not recurse into ``_lhp_runs/`` — Phase B V8 evidence
# recorded ``UNABLE_TO_INFER_SCHEMA`` with ``recursiveFileLookup=true`` at
# the bare root but confirmed that the explicit ``{root}/_lhp_runs/*`` glob
# reads all runs correctly on Unity Catalog volumes. The suffix below is the
# narrow read-side fix ADR-001 §Consequences §Negative flagged as an
# outstanding ``CloudFilesLoadGenerator`` spot-check.
#
# ADR-003 tracks the long-term question of whether the run-scoped directory
# should remain ``_lhp_runs/`` (hidden-prefixed) or migrate to a different
# shape (e.g. Hive-style ``run_id=<uuid>/``). If that ADR changes the
# contract, update ``_LANDING_READ_SUFFIX`` alongside the extractor template
# helper — they must stay synchronized.
_LANDING_READ_SUFFIX = "_lhp_runs/*"


def _cloudfiles_read_path(landing_path: str) -> str:
    """Return the AutoLoader ``.load(...)`` path for a jdbc_watermark_v2 landing.

    Appends the run-scoped glob that matches how the extraction template
    structures its Parquet writes. Returns an empty string for an empty
    ``landing_path`` so downstream validation can surface the missing field.
    """
    landing = (landing_path or "").rstrip("/")
    if not landing:
        return ""
    return f"{landing}/{_LANDING_READ_SUFFIX}"


def _has_schema_location(source_config: Dict[str, Any]) -> bool:
    """True when any supported CloudFiles schemaLocation surface is already set."""
    if source_config.get("schema_location"):
        return True
    options = source_config.get("options")
    if isinstance(options, dict) and options.get("cloudFiles.schemaLocation"):
        return True
    reader_options = source_config.get("reader_options")
    if isinstance(reader_options, dict) and reader_options.get(
        "cloudFiles.schemaLocation"
    ):
        return True
    return False


def _check_landing_schema_overlap(
    action: Action, flowgroup: object
) -> None:
    """Raise LHPConfigError when landing_path shares catalog+schema with bronze write target.

    Only applies to UC managed volume paths (/Volumes/<catalog>/<schema>/...).
    Cross-catalog placements are unusual but legitimate; emit a warning only.
    """
    landing_path = (action.landing_path or "").strip()
    volume_match = _UC_VOLUME_RE.match(landing_path)
    if not volume_match:
        return

    landing_catalog = volume_match.group(1)
    landing_schema = volume_match.group(2)

    write_action = next(
        (
            a
            for a in getattr(flowgroup, "actions", [])
            if getattr(a, "type", None) == "write"
            and getattr(a, "write_target", None) is not None
        ),
        None,
    )
    if write_action is None:
        return

    # write_target may be parsed as a dict (raw YAML) or as a WriteTarget
    # pydantic model depending on call site. Support both — getattr on a dict
    # always returns None, which silently disables the overlap check.
    wt = write_action.write_target
    if isinstance(wt, dict):
        write_catalog = (wt.get("catalog") or "")
        write_schema = (wt.get("schema") or "")
    else:
        write_catalog = getattr(wt, "catalog", None) or ""
        write_schema = getattr(wt, "schema", None) or ""

    if landing_catalog != write_catalog:
        logger.warning(
            f"Action '{action.name}': landing_path catalog '{landing_catalog}' differs "
            f"from bronze write catalog '{write_catalog}' — cross-catalog landing is "
            f"unusual; verify this is intentional for the {'{env}_edp_{medallion}'} shape."
        )
        return

    if landing_schema == write_schema:
        raise LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="018",
            title="landing_path overlaps bronze write schema",
            details=(
                f"Action '{action.name}' writes landing Parquet to "
                f"'/Volumes/{landing_catalog}/{landing_schema}/...' which is the same "
                f"Unity Catalog schema as its bronze write target "
                f"('{write_catalog}.{write_schema}'). "
                "AutoLoader and managed Delta tables must not share a UC schema — "
                "this causes LOCATION_OVERLAP errors at pipeline startup."
            ),
            context={
                "action": action.name,
                "landing_path": landing_path,
                "landing_schema": f"{landing_catalog}.{landing_schema}",
                "bronze_schema": f"{write_catalog}.{write_schema}",
            },
            suggestions=[
                f"Move landing_path to a dedicated schema, e.g. "
                f"'/Volumes/{landing_catalog}/landing_{landing_schema}/...'",
                "Or use an external location (abfss://) for the landing zone.",
            ],
        )


class JDBCWatermarkJobGenerator(BaseActionGenerator):
    """Generator for jdbc_watermark_v2 source type.

    Generates an extraction Job notebook (auxiliary file) and delegates
    the DLT pipeline file to CloudFilesLoadGenerator.
    """

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate extraction notebook and CloudFiles stub.

        Args:
            action: The jdbc_watermark_v2 load action.
            context: Generation context dict.

        Returns:
            CloudFiles DLT stub code (primary output).
        """
        source_config = action.source if isinstance(action.source, dict) else {}
        watermark = action.watermark
        flowgroup = context.get("flowgroup")

        _check_landing_schema_overlap(action, flowgroup)

        # Derive context values
        source_system_id = (
            getattr(watermark, "source_system_id", None) if watermark else None
        )
        if not source_system_id:
            # Derive from JDBC URL host
            url = source_config.get("url", "")
            host_match = re.search(r"//([^:/]+)", url)
            source_system_id = host_match.group(1) if host_match else "unknown"

        wm_catalog = (
            getattr(watermark, "catalog", None) if watermark else None
        ) or "metadata"
        wm_schema = (
            getattr(watermark, "schema", None) if watermark else None
        ) or "orchestration"

        pipeline_name = flowgroup.pipeline if flowgroup else "unknown"

        # Resolve secret references for JDBC credentials
        jdbc_url = _resolve_secret_refs(source_config.get("url", ""))
        jdbc_user = _resolve_secret_refs(source_config.get("user", ""))
        jdbc_password = _resolve_secret_refs(source_config.get("password", ""))

        template_context: Dict[str, Any] = {
            "action_name": action.name,
            "pipeline_name": pipeline_name,
            "source_system_id": source_system_id,
            "schema_name": source_config.get("schema_name", ""),
            "table_name": source_config.get("table_name", ""),
            "wm_catalog": wm_catalog,
            "wm_schema": wm_schema,
            "watermark_column": watermark.column if watermark else "",
            "watermark_type": watermark.type.value if watermark else "timestamp",
            "watermark_operator": watermark.operator if watermark else ">=",
            "jdbc_url": jdbc_url,
            "jdbc_user": jdbc_user,
            "jdbc_password": jdbc_password,
            "jdbc_driver": source_config.get("driver", ""),
            "jdbc_table": source_config.get("table", ""),
            "landing_path": action.landing_path or "",
            "load_group": (
                f"{flowgroup.pipeline}::{flowgroup.flowgroup}"
                if flowgroup
                else ""
            ),
        }

        # Render extraction notebook
        extraction_code = self.render_template(
            "load/jdbc_watermark_job.py.j2", template_context
        )

        # Resolve secret placeholders (__SECRET_scope_key__) to dbutils.secrets.get()
        # NOTE: Any generator producing auxiliary files with secret placeholders
        # must apply SecretCodeGenerator here — aux files bypass the primary
        # code path's secret resolution in code_generator.py.
        sub_mgr = context.get("substitution_manager")
        if sub_mgr:
            from ...utils.secret_code_generator import SecretCodeGenerator

            secret_refs = sub_mgr.get_secret_references()
            if secret_refs:
                extraction_code = SecretCodeGenerator().generate_python_code(
                    extraction_code, secret_refs
                )

        # Format with Black (after secret resolution — resolved calls may be longer)
        try:
            extraction_code = black.format_str(
                extraction_code, mode=black.Mode(line_length=88)
            )
        except black.InvalidInput:
            logger.warning(
                f"Action '{action.name}': extraction notebook failed Black formatting"
            )

        # Store as auxiliary file on the flowgroup
        if flowgroup:
            aux_key = f"__lhp_extract_{action.name}.py"
            flowgroup._auxiliary_files[aux_key] = extraction_code
            logger.debug(f"Stored extraction notebook as auxiliary file '{aux_key}'")

        # Delegate CloudFiles stub to CloudFilesLoadGenerator
        from .cloudfiles import CloudFilesLoadGenerator

        cloudfiles_gen = CloudFilesLoadGenerator()

        # Build a synthetic cloudfiles action pointing at the landing path
        from ...models.config import Action as ActionModel

        cloudfiles_source = {
            key: value
            for key, value in source_config.items()
            if key in _CLOUDFILES_PASSTHROUGH_KEYS
        }
        cloudfiles_source.update(
            {
                "type": "cloudfiles",
                "path": _cloudfiles_read_path(action.landing_path or ""),
                "format": "parquet",
            }
        )

        # ADR-001 read-path fix: the landing glob uses an explicit ``*`` wildcard.
        # AutoLoader's default ``useStrictGlobber=true`` can mis-match across
        # directory separators on Unity Catalog volumes, so ensure it is
        # disabled unless the user bundle has explicitly opted back in.
        existing_options = dict(cloudfiles_source.get("options") or {})
        existing_options.setdefault("cloudFiles.useStrictGlobber", "false")
        cloudfiles_source["options"] = existing_options

        if not _has_schema_location(cloudfiles_source):
            cloudfiles_source["schema_location"] = _default_schema_location(
                action.landing_path or "", action.name
            )

        cloudfiles_action = ActionModel(
            name=action.name,
            type="load",
            target=action.target,
            source=cloudfiles_source,
            description=action.description
            or f"CloudFiles load from landing zone: {action.name}",
            readMode=action.readMode,
        )

        return cloudfiles_gen.generate(cloudfiles_action, context)
