"""Unit tests for the JDBC watermark (high-water mark) load generator."""

import pytest
from lhp.generators.load.jdbc_watermark import JDBCWatermarkLoadGenerator
from lhp.models.config import Action, FlowGroup, WriteTarget


class MockSubstitutionManager:
    """Minimal substitution manager for testing bronze target resolution."""

    def __init__(self, mappings):
        self.mappings = mappings

    def substitute_yaml(self, data):
        return data


def _make_watermark_action(
    name="load_orders_jdbc",
    target="v_orders_raw",
    watermark_column="modified_date",
    watermark_type="timestamp",
    watermark_operator=">=",
    jdbc_url="jdbc:postgresql://host:5432/db",
    jdbc_table='"sales"."orders"',
    **overrides,
):
    source = {
        "type": "jdbc_watermark",
        "url": jdbc_url,
        "user": "test_user",
        "password": "test_pass",
        "driver": "org.postgresql.Driver",
        "table": jdbc_table,
    }
    source.update(overrides.pop("source_overrides", {}))
    return Action(
        name=name,
        type="load",
        source=source,
        target=target,
        watermark={
            "column": watermark_column,
            "type": watermark_type,
            "operator": watermark_operator,
        },
        **overrides,
    )


@pytest.mark.unit
class TestJDBCWatermarkLoadGenerator:
    """Tests for JDBCWatermarkLoadGenerator.generate()."""

    def setup_method(self):
        self.generator = JDBCWatermarkLoadGenerator()

    def test_timestamp_watermark_generation(self):
        """Timestamp watermark wraps HWM value in single quotes."""
        action = _make_watermark_action()
        code = self.generator.generate(action, {})

        assert "def v_orders_raw():" in code
        assert "@dp.temporary_view()" in code
        assert "SELECT MAX(modified_date) AS _hwm" in code
        assert "WHERE modified_date >= '{}'" in code
        assert "except Exception:" in code
        assert "_hwm = None" in code

    def test_numeric_watermark_generation(self):
        """Numeric watermark does NOT wrap HWM value in quotes."""
        action = _make_watermark_action(
            watermark_column="product_id",
            watermark_type="numeric",
        )
        code = self.generator.generate(action, {})

        assert "WHERE product_id >= {}" in code
        assert "WHERE product_id >= '{}'" not in code

    def test_first_run_handling(self):
        """Generated code handles first run when target table does not exist."""
        action = _make_watermark_action()
        code = self.generator.generate(action, {})

        assert "try:" in code
        assert "except Exception:" in code
        assert "_hwm = None" in code

    def test_partitioning_present(self):
        """JDBC partitioning options appear when partition params are provided."""
        action = _make_watermark_action(
            source_overrides={
                "num_partitions": "4",
                "partition_column": "order_id",
                "lower_bound": "1",
                "upper_bound": "1000000",
            },
        )
        code = self.generator.generate(action, {})

        assert "numPartitions" in code
        assert "partitionColumn" in code
        assert "lowerBound" in code
        assert "upperBound" in code

    def test_partitioning_absent(self):
        """No partition options when partition params are omitted."""
        action = _make_watermark_action()
        code = self.generator.generate(action, {})

        assert "numPartitions" not in code

    def test_no_operational_metadata_by_default(self):
        """Without project config, operational metadata columns are not added."""
        action = _make_watermark_action()
        code = self.generator.generate(action, {})

        assert "withColumn" not in code

    def test_custom_operator_gt(self):
        """Custom '>' operator renders instead of default '>='."""
        action = _make_watermark_action(watermark_operator=">")
        code = self.generator.generate(action, {})

        assert "WHERE modified_date > " in code
        assert "WHERE modified_date >= " not in code

    def test_string_source_raises_error(self):
        """A plain string source raises an error."""
        action = Action.model_construct(
            name="bad_action",
            type="load",
            source="just_a_string",
            target="v_bad",
            watermark=None,
        )
        with pytest.raises(Exception):
            self.generator.generate(action, {})

    def test_generated_code_is_valid_python(self):
        """Generated code must compile without SyntaxError."""
        action = _make_watermark_action()
        code = self.generator.generate(action, {})

        compile(code, "<test>", "exec")

    def test_bronze_target_with_substitution_context(self):
        """Substitution manager catalog/schema appear in HWM query target."""
        sub_mgr = MockSubstitutionManager(
            mappings={"catalog": "dev_catalog", "bronze_schema": "bronze"},
        )
        action = _make_watermark_action(jdbc_table='"schema"."orders"')
        code = self.generator.generate(action, {"substitution_manager": sub_mgr})

        assert "dev_catalog.bronze.orders" in code

    def test_bronze_target_from_write_action(self):
        """Write action table name is used for the bronze target in HWM query."""
        write_action = Action(
            name="write_customers",
            type="write",
            write_target=WriteTarget(type="streaming_table", table="customers"),
        )
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_fg",
            actions=[
                _make_watermark_action(),
                write_action,
            ],
        )
        code = self.generator.generate(
            _make_watermark_action(),
            {"flowgroup": flowgroup},
        )

        assert "customers" in code
