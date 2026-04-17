"""Generator for JDBC watermark v2 extraction Job notebook.

Produces two artifacts from a single jdbc_watermark_v2 action:
1. An extraction notebook that uses WatermarkManager for HWM tracking
   and writes Parquet to a landing path (stored as auxiliary file)
2. A CloudFiles DLT stub that reads from the landing path (returned as
   the primary output, delegated to CloudFilesLoadGenerator)
"""

import logging
import re
from typing import Any, Dict

import black

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action

logger = logging.getLogger(__name__)

# Pattern to match ${secret:scope/key} references
_SECRET_REF_RE = re.compile(r"\$\{secret:([^/]+)/([^}]+)\}")


def _resolve_secret_refs(value: str) -> str:
    """Convert ${secret:scope/key} to dbutils.secrets.get() call.

    Returns a Python expression string (not a literal).
    """
    match = _SECRET_REF_RE.match(value)
    if match:
        scope, key = match.group(1), match.group(2)
        return f'dbutils.secrets.get(scope="{scope}", key="{key}")'
    return f'"{value}"'


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

        # Derive context values
        source_system_id = (
            getattr(watermark, "source_system_id", None)
            if watermark
            else None
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
            logger.debug(
                f"Stored extraction notebook as auxiliary file '{aux_key}'"
            )

        # Delegate CloudFiles stub to CloudFilesLoadGenerator
        from .cloudfiles import CloudFilesLoadGenerator

        cloudfiles_gen = CloudFilesLoadGenerator()

        # Build a synthetic cloudfiles action pointing at the landing path
        from ...models.config import Action as ActionModel

        cloudfiles_action = ActionModel(
            name=action.name,
            type="load",
            target=action.target,
            source={
                "type": "cloudfiles",
                "path": action.landing_path or "",
                "format": "parquet",
            },
            description=action.description
            or f"CloudFiles load from landing zone: {action.name}",
        )

        return cloudfiles_gen.generate(cloudfiles_action, context)
