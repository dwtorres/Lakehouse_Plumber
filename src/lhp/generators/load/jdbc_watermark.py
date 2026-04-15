"""JDBC watermark (high-water mark) load generator."""

import logging
from typing import Any, Dict

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter

logger = logging.getLogger(__name__)


class JDBCWatermarkLoadGenerator(BaseActionGenerator):
    """Generate JDBC incremental load actions driven by a high-water mark column.

    Reads MAX(watermark_column) from the Bronze target table, then filters
    the JDBC source query so only rows newer than that high-water mark are
    fetched on each pipeline run.
    """

    def __init__(self) -> None:
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate JDBC watermark load code.

        Args:
            action: Action configuration including watermark settings.
            context: Runtime context with substitution_manager, flowgroup, etc.

        Returns:
            Rendered PySpark/DLT source code string.
        """
        source_config = action.source
        if isinstance(source_config, str):
            raise ErrorFormatter.invalid_source_format(
                action_name=action.name,
                action_type="jdbc watermark load",
                expected_formats=[
                    "A configuration object (dict) with JDBC connection details",
                    (
                        "source:\n"
                        "  type: jdbc_watermark\n"
                        "  url: 'jdbc:postgresql://host:5432/db'\n"
                        "  table: 'my_table'\n"
                        "  driver: 'org.postgresql.Driver'"
                    ),
                ],
            )

        logger.debug(
            f"Generating JDBC watermark load for target '{action.target}',"
            f" action '{action.name}'"
        )

        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        # JDBC watermark is always batch
        readMode = action.readMode or "batch"
        if readMode != "batch":
            raise ErrorFormatter.invalid_read_mode(
                action_name=action.name,
                action_type="jdbc_watermark",
                provided=readMode,
                valid_modes=["batch"],
            )

        watermark = action.watermark
        watermark_column = watermark.column if watermark else None
        watermark_type = watermark.type.value if watermark else None
        watermark_operator = watermark.operator if watermark else ">="

        bronze_target = self._resolve_bronze_target(action, context)

        jdbc_table = source_config.get("table")
        jdbc_driver = source_config.get("driver")
        logger.debug(
            f"JDBC watermark load '{action.name}': table='{jdbc_table}',"
            f" driver='{jdbc_driver}', watermark_column='{watermark_column}'"
        )

        template_context: Dict[str, Any] = {
            "action_name": action.name,
            "target_view": action.target,
            "description": action.description
            or f"JDBC watermark source: {action.name}",
            "bronze_target": bronze_target,
            "watermark_column": watermark_column,
            "watermark_type": watermark_type,
            "watermark_operator": watermark_operator,
            "jdbc_url": source_config.get("url"),
            "jdbc_user": source_config.get("user"),
            "jdbc_password": source_config.get("password"),
            "jdbc_driver": jdbc_driver,
            "jdbc_table": jdbc_table,
            "num_partitions": source_config.get("num_partitions"),
            "partition_column": source_config.get("partition_column"),
            "lower_bound": source_config.get("lower_bound"),
            "upper_bound": source_config.get("upper_bound"),
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/jdbc_watermark.py.j2", template_context)

    def _resolve_bronze_target(self, action: Action, context: Dict[str, Any]) -> str:
        """Resolve the Bronze target table name for self-watermark HWM query.

        Args:
            action: Action configuration.
            context: Runtime context with substitution_manager or catalog/schema keys.

        Returns:
            Fully-qualified table name string (e.g. catalog.bronze_schema.table).
        """
        sub_mgr = context.get("substitution_manager")
        if sub_mgr:
            catalog = sub_mgr.mappings.get("catalog", "")
            bronze_schema = sub_mgr.mappings.get("bronze_schema", "")
        else:
            catalog = context.get("catalog", "")
            bronze_schema = context.get("bronze_schema", "")

        target_table = self._extract_target_table(action, context)

        if catalog and bronze_schema:
            return f"{catalog}.{bronze_schema}.{target_table}"
        elif bronze_schema:
            return f"{bronze_schema}.{target_table}"
        return target_table

    def _extract_target_table(self, action: Action, context: Dict[str, Any]) -> str:
        """Extract the target table name from the flowgroup context.

        Prefers the write action's declared table; falls back to deriving the
        name from the JDBC source table string with a warning.

        Args:
            action: Action configuration.
            context: Runtime context with an optional flowgroup object.

        Returns:
            Unqualified table name string.
        """
        flowgroup = context.get("flowgroup")
        if flowgroup and hasattr(flowgroup, "actions"):
            for fg_action in flowgroup.actions:
                if fg_action.type.value == "write" and fg_action.write_target:
                    wt = fg_action.write_target
                    if hasattr(wt, "table") and wt.table:
                        return wt.table
                    elif isinstance(wt, dict) and wt.get("table"):
                        return wt["table"]

        # Fallback: derive from JDBC source table name
        source_config = action.source if isinstance(action.source, dict) else {}
        source_table = source_config.get("table", "")
        if "." in source_table:
            derived = source_table.split(".")[-1].strip('"').strip("'")
        else:
            derived = source_table.strip('"').strip("'")

        logger.warning(
            f"JDBC watermark action '{action.name}': no write action with a "
            f"'table' field found in the flowgroup. Deriving Bronze target "
            f"table name '{derived}' from the JDBC source table "
            f"'{source_table}'. If the Bronze table has a different name, "
            f"add a write action to the same flowgroup or the self-watermark "
            f"HWM query will target the wrong table."
        )
        return derived
