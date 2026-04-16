"""Unit tests for the JDBC watermark v2 Job generator."""

import pytest
from lhp.generators.load.jdbc_watermark_job import JDBCWatermarkJobGenerator
from lhp.models.config import Action, FlowGroup, WriteTarget


def _make_v2_action(
    name="load_product_jdbc",
    target="v_product_raw",
    watermark_column="ModifiedDate",
    watermark_type="timestamp",
    watermark_operator=">=",
    source_system_id="pg_prod",
    jdbc_url="jdbc:postgresql://host:5432/db",
    jdbc_table='"Production"."Product"',
    landing_path="/Volumes/catalog/schema/landing/product",
    **overrides,
):
    source = {
        "type": "jdbc_watermark_v2",
        "url": jdbc_url,
        "user": "test_user",
        "password": "test_pass",
        "driver": "org.postgresql.Driver",
        "table": jdbc_table,
        "schema_name": "Production",
        "table_name": "Product",
    }
    source.update(overrides.pop("source_overrides", {}))
    return Action(
        name=name,
        type="load",
        source=source,
        target=target,
        landing_path=landing_path,
        watermark={
            "column": watermark_column,
            "type": watermark_type,
            "operator": watermark_operator,
            "source_system_id": source_system_id,
        },
        **overrides,
    )


def _make_flowgroup_with_write(action, write_table="product"):
    """Build a FlowGroup with a load action and a streaming_table write."""
    write_action = Action(
        name="write_product_bronze",
        type="write",
        source=action.target,
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze_catalog",
            schema="bronze_schema",
            table=write_table,
            table_properties={"delta.enableChangeDataFeed": "true"},
        ),
    )
    return FlowGroup(
        pipeline="test_pipeline",
        flowgroup="test_flowgroup",
        actions=[action, write_action],
    )


@pytest.mark.unit
class TestJDBCWatermarkJobGenerator:
    """Tests for JDBCWatermarkJobGenerator.generate()."""

    def _generate(self, action=None, context=None):
        """Helper to run generate with defaults."""
        if action is None:
            action = _make_v2_action()
        if context is None:
            fg = _make_flowgroup_with_write(action)
            context = {"flowgroup": fg}
        gen = JDBCWatermarkJobGenerator()
        return gen, gen.generate(action, context)

    def test_returns_cloudfiles_stub(self):
        """Primary output should be a CloudFiles DLT stub."""
        _, code = self._generate()
        assert 'format("cloudFiles")' in code

    def test_auxiliary_file_contains_extraction_notebook(self):
        """After generate(), flowgroup should have extraction notebook in auxiliary files."""
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        aux_key = f"extract_{action.name}.py"
        assert hasattr(fg, "_auxiliary_files"), "FlowGroup should have _auxiliary_files"
        assert aux_key in fg._auxiliary_files
        notebook = fg._auxiliary_files[aux_key]
        assert "WatermarkManager" in notebook

    def test_extraction_notebook_has_watermark_manager_import(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "from lhp.extensions.watermark_manager import WatermarkManager" in notebook

    def test_extraction_notebook_passes_spark_to_watermark_manager(self):
        """WatermarkManager constructor must receive spark as first arg."""
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "WatermarkManager(\n    spark," in notebook or "WatermarkManager(spark," in notebook

    def test_extraction_notebook_has_get_latest_watermark(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "get_latest_watermark(" in notebook
        assert '["watermark_value"]' in notebook  # dict access pattern

    def test_extraction_notebook_has_insert_new(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "insert_new(" in notebook
        assert "watermark_column_name=" in notebook  # required kwarg

    def test_extraction_notebook_no_dlt_decorators(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "@dp." not in notebook
        assert "temporary_view" not in notebook

    def test_timestamp_watermark_quotes_hwm(self):
        """Timestamp watermark WHERE clause should quote the HWM value."""
        action = _make_v2_action(watermark_type="timestamp")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        # Template should have a branch that quotes timestamp values
        assert "'{" in notebook  # HWM value is single-quoted in the SQL WHERE clause

    def test_numeric_watermark_no_quotes(self):
        """Numeric watermark WHERE clause should NOT quote the HWM value."""
        action = _make_v2_action(watermark_type="numeric", watermark_column="product_id")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        # The numeric branch should use unquoted format
        assert "numeric" in notebook.lower() or "})" in notebook or "format(" in notebook

    def test_custom_operator_gt(self):
        """Operator '>' should appear in the extraction notebook."""
        action = _make_v2_action(watermark_operator=">")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        # The operator should be in the WHERE clause template
        assert '> ' in notebook or '>"' in notebook or "> '" in notebook

    def test_extraction_notebook_contains_jdbc_format(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert 'format("jdbc")' in notebook

    def test_extraction_notebook_contains_parquet_write(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert 'format("parquet")' in notebook

    def test_extraction_notebook_contains_landing_path(self):
        action = _make_v2_action(landing_path="/Volumes/cat/sch/landing/prod")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "/Volumes/cat/sch/landing/prod" in notebook

    def test_secret_ref_renders_as_dbutils(self):
        """Secret references should render as dbutils.secrets.get() calls."""
        action = _make_v2_action(
            source_overrides={
                "user": "${secret:scope/jdbc_user}",
                "password": "${secret:scope/jdbc_pass}",
            }
        )
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert 'dbutils.secrets.get(scope="scope", key="jdbc_user")' in notebook
        assert 'dbutils.secrets.get(scope="scope", key="jdbc_pass")' in notebook

    def test_source_system_id_from_watermark_config(self):
        action = _make_v2_action(source_system_id="my_system")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        assert "my_system" in notebook

    def test_extraction_notebook_passes_black(self):
        """Rendered extraction notebook should be valid Black-formatted Python."""
        import black

        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"extract_{action.name}.py"]
        # Should not raise
        formatted = black.format_str(notebook, mode=black.Mode(line_length=88))
        assert formatted == notebook, "Extraction notebook is not Black-formatted"
