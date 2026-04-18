"""Tests for removed JDBC watermark routing in ActionRegistry."""

import pytest

from lhp.core.action_registry import ActionRegistry
from lhp.generators.load.jdbc import JDBCLoadGenerator
from lhp.models.config import Action, ActionType
from lhp.utils.error_formatter import LHPValidationError


@pytest.mark.unit
class TestActionRegistryWatermarkRouting:
    """Verify removed jdbc_watermark fails with a migration error."""

    def test_jdbc_watermark_raises_migration_error(self):
        registry = ActionRegistry()
        with pytest.raises(LHPValidationError, match="jdbc_watermark_v2"):
            registry.get_generator(ActionType.LOAD, "jdbc_watermark")

    def test_standard_jdbc_routes_unchanged(self):
        registry = ActionRegistry()
        generator = registry.get_generator(ActionType.LOAD, "jdbc")
        assert isinstance(generator, JDBCLoadGenerator)

    def test_jdbc_watermark_is_not_available(self):
        registry = ActionRegistry()
        assert not registry.is_generator_available(ActionType.LOAD, "jdbc_watermark")


@pytest.mark.unit
class TestRemovedJDBCWatermarkIntegration:
    """Legacy configs should fail loudly with a migration target."""

    def test_legacy_action_fails_with_migration_target(self):
        action = Action(
            name="load_orders_jdbc",
            type="load",
            source={
                "type": "jdbc_watermark",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "test_user",
                "password": "test_pass",
                "driver": "org.postgresql.Driver",
                "table": '"sales"."orders"',
            },
            target="v_orders_raw",
            watermark={"column": "modified_date", "type": "timestamp"},
        )
        registry = ActionRegistry()
        with pytest.raises(LHPValidationError, match="jdbc_watermark_v2"):
            registry.get_generator(ActionType.LOAD, action.source["type"])
