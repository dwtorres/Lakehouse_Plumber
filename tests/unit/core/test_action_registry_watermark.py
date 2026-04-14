"""Tests for JDBC watermark routing in ActionRegistry and basic integration."""

import pytest

from lhp.core.action_registry import ActionRegistry
from lhp.generators.load.jdbc import JDBCLoadGenerator
from lhp.generators.load.jdbc_watermark import JDBCWatermarkLoadGenerator
from lhp.models.config import Action, ActionType


@pytest.mark.unit
class TestActionRegistryWatermarkRouting:
    """Verify ActionRegistry routes jdbc_watermark to the correct generator."""

    def test_jdbc_watermark_routes_to_correct_generator(self):
        registry = ActionRegistry()
        generator = registry.get_generator(ActionType.LOAD, "jdbc_watermark")
        assert isinstance(generator, JDBCWatermarkLoadGenerator)

    def test_standard_jdbc_routes_unchanged(self):
        registry = ActionRegistry()
        generator = registry.get_generator(ActionType.LOAD, "jdbc")
        assert isinstance(generator, JDBCLoadGenerator)

    def test_jdbc_watermark_is_available(self):
        registry = ActionRegistry()
        assert registry.is_generator_available(ActionType.LOAD, "jdbc_watermark")


@pytest.mark.unit
class TestJDBCWatermarkIntegration:
    """End-to-end code generation for a JDBC watermark action."""

    def test_end_to_end_code_generation(self):
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

        generator = JDBCWatermarkLoadGenerator()
        code = generator.generate(action, {})

        # Verify key self-watermark pattern elements
        assert "@dp.temporary_view()" in code
        assert "def v_orders_raw():" in code
        assert "SELECT MAX(modified_date) AS _hwm" in code
        assert "except AnalysisException:" in code
        assert "_hwm = None" in code
        assert 'format("jdbc")' in code
        assert "modified_date >=" in code

        # Verify valid Python syntax
        compile(code, "<integration_test>", "exec")
