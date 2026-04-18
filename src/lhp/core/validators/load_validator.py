"""Load action validator."""

import logging
import re
from typing import List

from ...models.config import Action, ActionType, LoadSourceType
from ...utils.error_formatter import LHPError
from .base_validator import BaseActionValidator, ValidationError

logger = logging.getLogger(__name__)

_REMOVED_WATERMARK_SOURCE = "jdbc_watermark"
_REMOVED_WATERMARK_MESSAGE = (
    "jdbc_watermark has been removed; use jdbc_watermark_v2 instead "
    "(requires landing_path and watermark.source_system_id)"
)


class LoadActionValidator(BaseActionValidator):
    """Validator for load actions."""

    def validate(self, action: Action, prefix: str) -> List[ValidationError]:
        """Validate load action configuration."""
        logger.debug(f"Validating load action '{action.name}'")
        errors = []

        # Load actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Load actions must have a 'target' view name")

        # Load actions must have source configuration
        if not action.source:
            errors.append(f"{prefix}: Load actions must have a 'source' configuration")
            return errors

        # Source must be a dict for load actions
        if not isinstance(action.source, dict):
            errors.append(
                f"{prefix}: Load action source must be a configuration object"
            )
            return errors

        # Must have source type
        source_type = action.source.get("type")
        if not source_type:
            errors.append(f"{prefix}: Load action source must have a 'type' field")
            return errors

        if source_type == _REMOVED_WATERMARK_SOURCE:
            errors.append(f"{prefix}: {_REMOVED_WATERMARK_MESSAGE}")
            return errors

        # Validate source type is supported
        if not self.action_registry.is_generator_available(
            ActionType.LOAD, source_type
        ):
            errors.append(f"{prefix}: Unknown load source type '{source_type}'")
            return errors

        # Strict field validation for source configuration
        try:
            self.field_validator.validate_load_source(action.source, action.name)
        except LHPError as e:
            errors.append(e)
            return errors
        except Exception as e:
            errors.append(str(e))
            return errors

        # Type-specific validation
        errors.extend(self._validate_source_type(action, prefix, source_type))

        return errors

    def _validate_source_type(
        self, action: Action, prefix: str, source_type: str
    ) -> List[str]:
        """Validate specific source type requirements."""
        logger.debug(
            f"Validating source type '{source_type}' for action '{action.name}'"
        )
        errors = []

        try:
            load_type = LoadSourceType(source_type)

            if load_type == LoadSourceType.CLOUDFILES:
                errors.extend(self._validate_cloudfiles_source(action, prefix))
            elif load_type == LoadSourceType.DELTA:
                errors.extend(self._validate_delta_source(action, prefix))
            elif load_type == LoadSourceType.JDBC:
                errors.extend(self._validate_jdbc_source(action, prefix))
            elif load_type == LoadSourceType.PYTHON:
                errors.extend(self._validate_python_source(action, prefix))
            elif load_type == LoadSourceType.KAFKA:
                errors.extend(self._validate_kafka_source(action, prefix))
            elif load_type == LoadSourceType.JDBC_WATERMARK_V2:
                errors.extend(self._validate_jdbc_watermark_v2_source(action, prefix))

        except ValueError as e:
            logger.debug(f"Unrecognized load source type for '{action.name}': {e}")
            pass  # Already handled above

        return errors

    def _validate_cloudfiles_source(self, action: Action, prefix: str) -> List[str]:
        """Validate CloudFiles source configuration."""
        errors = []
        if not action.source.get("path"):
            errors.append(f"{prefix}: CloudFiles source must have 'path'")
        if not action.source.get("format"):
            errors.append(f"{prefix}: CloudFiles source must have 'format'")
        return errors

    def _validate_delta_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Delta source configuration."""
        errors = []
        if not action.source.get("catalog"):
            errors.append(f"{prefix}: Delta source must have 'catalog'")
        if not action.source.get("schema"):
            errors.append(f"{prefix}: Delta source must have 'schema'")
        if not action.source.get("table"):
            errors.append(f"{prefix}: Delta source must have 'table'")
        return errors

    def _validate_jdbc_source(self, action: Action, prefix: str) -> List[str]:
        """Validate JDBC source configuration."""
        errors = []
        required_fields = ["url", "user", "password", "driver"]
        for field in required_fields:
            if not action.source.get(field):
                errors.append(f"{prefix}: JDBC source must have '{field}'")

        # Must have either query or table
        if not action.source.get("query") and not action.source.get("table"):
            errors.append(f"{prefix}: JDBC source must have either 'query' or 'table'")

        return errors

    def _validate_python_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Python source configuration."""
        errors = []
        if not action.source.get("module_path"):
            errors.append(f"{prefix}: Python source must have 'module_path'")
        return errors

    def _validate_kafka_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Kafka source configuration."""
        errors = []

        # Must have bootstrap_servers
        if not action.source.get("bootstrap_servers"):
            errors.append(f"{prefix}: Kafka source must have 'bootstrap_servers'")

        # Must have exactly one subscription method
        subscription_methods = [
            action.source.get("subscribe"),
            action.source.get("subscribePattern"),
            action.source.get("assign"),
        ]

        provided_methods = [m for m in subscription_methods if m is not None]

        if len(provided_methods) == 0:
            errors.append(
                f"{prefix}: Kafka source must have one of: 'subscribe', 'subscribePattern', or 'assign'"
            )
        elif len(provided_methods) > 1:
            errors.append(
                f"{prefix}: Kafka source can only have ONE of: 'subscribe', 'subscribePattern', or 'assign'"
            )

        return errors

    # SQL identifier pattern: letters, digits, underscores — no special chars
    _SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def _validate_jdbc_watermark_v2_source(
        self, action: Action, prefix: str
    ) -> List[str]:
        """Validate JDBC watermark v2 source configuration (LHP-VAL-04x)."""
        errors = []

        # LHP-VAL-040: landing_path is required
        if not action.landing_path:
            errors.append(
                f"{prefix}: landing_path is required for jdbc_watermark_v2 source"
            )
        elif not action.landing_path.startswith("/Volumes/"):
            logger.warning(
                f"Action '{action.name}': landing_path '{action.landing_path}' "
                "does not start with /Volumes/. Recommended to use Unity Catalog "
                "Volumes for landing zone paths."
            )

        # LHP-VAL-041: watermark config required
        if not action.watermark:
            errors.append(
                f"{prefix}: watermark configuration is required for "
                "jdbc_watermark_v2 source"
            )
        else:
            # LHP-VAL-042: watermark.column required
            if not action.watermark.column:
                errors.append(
                    f"{prefix}: watermark.column is required for "
                    "jdbc_watermark_v2 source"
                )
            elif not self._SQL_IDENTIFIER_RE.match(action.watermark.column):
                errors.append(
                    f"{prefix}: watermark.column '{action.watermark.column}' "
                    "must be a valid SQL identifier "
                    "(letters, digits, underscores only)"
                )

            # LHP-VAL-044: watermark.source_system_id required
            if not getattr(action.watermark, "source_system_id", None):
                errors.append(
                    f"{prefix}: watermark.source_system_id is required for "
                    "jdbc_watermark_v2 source"
                )

        # LHP-VAL-045: source.schema_name required
        if not action.source.get("schema_name"):
            errors.append(
                f"{prefix}: source.schema_name is required for "
                "jdbc_watermark_v2 source"
            )

        # LHP-VAL-046: source.table_name required
        if not action.source.get("table_name"):
            errors.append(
                f"{prefix}: source.table_name is required for "
                "jdbc_watermark_v2 source"
            )

        # LHP-VAL-047: core JDBC fields (url, user, password, driver, table)
        errors.extend(self._validate_jdbc_source(action, prefix))

        # Warning: num_partitions is not yet supported in v2
        if action.source.get("num_partitions"):
            logger.warning(
                f"Action '{action.name}': num_partitions is set but "
                "partitioned JDBC reads are not yet supported in v2. "
                "The extraction Job will use a single-threaded read."
            )

        return errors
