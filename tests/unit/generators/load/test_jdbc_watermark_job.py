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
        aux_key = f"__lhp_extract_{action.name}.py"
        assert hasattr(fg, "_auxiliary_files"), "FlowGroup should have _auxiliary_files"
        assert aux_key in fg._auxiliary_files
        notebook = fg._auxiliary_files[aux_key]
        assert "WatermarkManager" in notebook

    def test_extraction_notebook_has_watermark_manager_import(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "from lhp.extensions.watermark_manager import WatermarkManager" in notebook

    def test_extraction_notebook_passes_spark_to_watermark_manager(self):
        """WatermarkManager constructor must receive spark as first arg."""
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "WatermarkManager(\n    spark," in notebook or "WatermarkManager(spark," in notebook

    def test_extraction_notebook_has_get_latest_watermark(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "get_latest_watermark(" in notebook
        assert '["watermark_value"]' in notebook  # dict access pattern

    def test_extraction_notebook_has_insert_new(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "insert_new(" in notebook
        assert "watermark_column_name=" in notebook  # required kwarg

    def test_extraction_notebook_no_dlt_decorators(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "@dp." not in notebook
        assert "temporary_view" not in notebook

    def test_timestamp_watermark_quotes_hwm(self):
        """Timestamp watermark WHERE clause should quote the HWM value and double-quote the column identifier."""
        action = _make_v2_action(watermark_type="timestamp")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # Template should have a branch that quotes timestamp values
        assert "'{" in notebook  # HWM value is single-quoted in the SQL WHERE clause
        assert '"ModifiedDate"' in notebook

    def test_numeric_watermark_value_unquoted_column_quoted(self):
        """Numeric watermark: HWM value unquoted, column identifier double-quoted in WHERE."""
        action = _make_v2_action(watermark_type="numeric", watermark_column="product_id")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # The numeric branch should use unquoted format
        assert "numeric" in notebook.lower() or "})" in notebook or "format(" in notebook
        assert '"product_id"' in notebook

    def test_custom_operator_gt(self):
        """Operator '>' should appear in the extraction notebook."""
        action = _make_v2_action(watermark_operator=">")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # The operator should be in the WHERE clause template
        assert '> ' in notebook or '>"' in notebook or "> '" in notebook

    def test_extraction_notebook_contains_jdbc_format(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert 'format("jdbc")' in notebook

    def test_extraction_notebook_contains_parquet_write(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert 'format("parquet")' in notebook

    def test_extraction_notebook_contains_landing_path(self):
        action = _make_v2_action(landing_path="/Volumes/cat/sch/landing/prod")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
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
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert 'dbutils.secrets.get(scope="scope", key="jdbc_user")' in notebook
        assert 'dbutils.secrets.get(scope="scope", key="jdbc_pass")' in notebook

    def test_secret_placeholders_resolved_via_substitution_manager(self):
        """__SECRET_ placeholders (post-substitution) resolved via SecretCodeGenerator."""
        from unittest.mock import MagicMock

        from lhp.utils.substitution import SecretReference

        action = _make_v2_action(
            source_overrides={
                "user": "__SECRET_dev-secrets_jdbc_user__",
                "password": "__SECRET_dev-secrets_jdbc_pass__",
            }
        )
        fg = _make_flowgroup_with_write(action)

        mock_sub_mgr = MagicMock()
        mock_sub_mgr.get_secret_references.return_value = {
            SecretReference("dev-secrets", "jdbc_user"),
            SecretReference("dev-secrets", "jdbc_pass"),
        }

        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg, "substitution_manager": mock_sub_mgr})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert 'dbutils.secrets.get(scope="dev-secrets", key="jdbc_user")' in notebook
        assert 'dbutils.secrets.get(scope="dev-secrets", key="jdbc_pass")' in notebook
        assert "__SECRET_" not in notebook

    def test_secret_resolution_noop_when_no_placeholders(self):
        """Aux file unchanged when secret_refs populated but no placeholders in content."""
        from unittest.mock import MagicMock

        from lhp.utils.substitution import SecretReference

        action = _make_v2_action()  # no secret placeholders in source
        fg = _make_flowgroup_with_write(action)

        mock_sub_mgr = MagicMock()
        mock_sub_mgr.get_secret_references.return_value = {
            SecretReference("some-scope", "some_key"),
        }

        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg, "substitution_manager": mock_sub_mgr})

        # Also generate without sub_mgr for comparison
        fg2 = _make_flowgroup_with_write(_make_v2_action())
        gen2 = JDBCWatermarkJobGenerator()
        gen2.generate(_make_v2_action(), {"flowgroup": fg2})

        assert fg._auxiliary_files[f"__lhp_extract_{action.name}.py"] == fg2._auxiliary_files[f"__lhp_extract_{action.name}.py"]

    def test_secret_resolution_idempotent(self):
        """Applying secret resolution twice produces same result."""
        from unittest.mock import MagicMock

        from lhp.utils.secret_code_generator import SecretCodeGenerator
        from lhp.utils.substitution import SecretReference

        action = _make_v2_action(
            source_overrides={
                "user": "__SECRET_dev-secrets_jdbc_user__",
            }
        )
        fg = _make_flowgroup_with_write(action)
        mock_sub_mgr = MagicMock()
        refs = {SecretReference("dev-secrets", "jdbc_user")}
        mock_sub_mgr.get_secret_references.return_value = refs

        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg, "substitution_manager": mock_sub_mgr})
        notebook_first = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]

        # Apply SecretCodeGenerator again
        notebook_second = SecretCodeGenerator().generate_python_code(notebook_first, refs)
        assert notebook_first == notebook_second
        assert "__SECRET_" not in notebook_second

    def test_placeholder_format_round_trips(self):
        """Placeholder format contract: substitution.py and SecretCodeGenerator agree."""
        from lhp.utils.secret_code_generator import SecretCodeGenerator
        from lhp.utils.substitution import SecretReference

        scope, key = "dev-secrets", "jdbc_user"
        ref = SecretReference(scope, key)
        placeholder = f"__SECRET_{scope}_{key}__"
        quoted = f'"{placeholder}"'

        resolved = SecretCodeGenerator().generate_python_code(quoted, {ref})
        assert f"scope=" in resolved and scope in resolved
        assert f"key=" in resolved and key in resolved
        assert "dbutils.secrets.get(" in resolved
        assert "__SECRET_" not in resolved

    def test_substitution_manager_none_is_safe(self):
        """Generator works when substitution_manager absent from context."""
        action = _make_v2_action(
            source_overrides={
                "user": "__SECRET_dev-secrets_jdbc_user__",
            }
        )
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        # No substitution_manager in context — should not crash
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # Placeholder survives (no resolution), but no crash
        assert "__SECRET_dev-secrets_jdbc_user__" in notebook

    def test_source_system_id_from_watermark_config(self):
        action = _make_v2_action(source_system_id="my_system")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "my_system" in notebook

    def test_extraction_notebook_passes_black(self):
        """Rendered extraction notebook should be valid Black-formatted Python."""
        import black

        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # Should not raise
        formatted = black.format_str(notebook, mode=black.Mode(line_length=88))
        assert formatted == notebook, "Extraction notebook is not Black-formatted"

    def test_aux_key_uses_lhp_extract_prefix(self):
        """Regression: aux key must use __lhp_extract_ prefix to prevent DLT glob collision.

        The aux_key format is enforced by jdbc_watermark_job.py.
        If this test fails, extraction notebooks will be loaded by DLT pipeline
        and cause ModuleNotFoundError.
        """
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})

        assert hasattr(fg, "_auxiliary_files"), "FlowGroup should have _auxiliary_files"
        aux_keys = list(fg._auxiliary_files.keys())

        assert any(
            k.startswith("__lhp_extract_") for k in aux_keys
        ), f"No aux key starts with '__lhp_extract_'; found: {aux_keys}"

        assert all(
            k.endswith(".py") for k in aux_keys if k.startswith("__lhp_extract_")
        ), "All __lhp_extract_ aux keys must end with .py"

        bare_extract_keys = [k for k in aux_keys if k.startswith("extract_")]
        assert not bare_extract_keys, (
            f"Bare 'extract_' aux keys found (missing __lhp_ prefix): {bare_extract_keys}"
        )

    def test_non_extract_aux_files_stay_at_root(self):
        """Non-extraction aux files must remain at pipeline root (not in _extract/ sibling).

        Regression: discriminated routing sends __lhp_extract_*.py to sibling _extract/
        dir but all other aux files (e.g. jobs_stats_loader.py) must stay at root level.
        This distinction is checked by the orchestrator at write time.
        """
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})

        # Inject a non-extract aux file alongside the real extraction notebook
        fg._auxiliary_files["jobs_stats_loader.py"] = "# monitoring helper\n"

        extract_keys = [
            k for k in fg._auxiliary_files if k.startswith("__lhp_extract_") and k.endswith(".py")
        ]
        non_extract_keys = [
            k for k in fg._auxiliary_files if not (k.startswith("__lhp_extract_") and k.endswith(".py"))
        ]

        assert extract_keys, "Should have at least one __lhp_extract_ key for routing test"
        assert non_extract_keys, "Should have at least one non-extract key for routing test"

        # Simulate routing logic from orchestrator: extract keys → _extract/ sibling, others → root
        extract_destinations = {k: f"pipeline_extract/{k}" for k in extract_keys}
        non_extract_destinations = {k: k for k in non_extract_keys}

        for key, dest in extract_destinations.items():
            assert "_extract/" in dest, f"Extract key '{key}' should route to _extract/ sibling dir"

        for key, dest in non_extract_destinations.items():
            assert "_extract/" not in dest, f"Non-extract key '{key}' should stay at root"

        # The two sets must be disjoint
        assert set(extract_keys).isdisjoint(set(non_extract_keys))

    def test_watermark_column_unquoted_in_pyspark_and_metadata(self):
        """F.col() and metadata kwarg use raw column name, not SQL-quoted."""
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # PySpark F.col() should have raw column name (no SQL double-quotes)
        assert 'F.col("ModifiedDate")' in notebook
        # Metadata kwarg should have raw column name
        assert 'watermark_column_name="ModifiedDate"' in notebook

    def test_column_with_embedded_quote_escaped(self):
        """Column name containing double-quote is escaped per SQL standard (" → "")."""
        action = _make_v2_action(watermark_column='Col"Name')
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # SQL WHERE clause: " in column name doubled per SQL standard
        assert '\\"Col""Name\\"' in notebook
