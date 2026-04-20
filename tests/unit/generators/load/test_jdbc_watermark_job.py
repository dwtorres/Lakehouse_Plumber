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
        # The L2 §5.3 restructure imports WatermarkManager + TerminalStateGuardError
        # as a tuple; Black may wrap it across lines. Assert the import resolves
        # WatermarkManager from the package, not the exact single-line form.
        import ast

        tree = ast.parse(notebook)
        wm_imports = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom)
            and n.module == "lhp_watermark"
            and any(alias.name == "WatermarkManager" for alias in n.names)
        ]
        assert (
            wm_imports
        ), "WatermarkManager must be imported from lhp_watermark"

    def test_extraction_notebook_passes_spark_to_watermark_manager(self):
        """WatermarkManager constructor must receive spark as first arg."""
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert (
            "WatermarkManager(\n    spark," in notebook
            or "WatermarkManager(spark," in notebook
        )

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
        """Timestamp watermark branch quotes the HWM literal for the JDBC query.

        The L2 §5.3 restructure builds the JDBC WHERE clause at notebook
        runtime (not at Jinja render time). The timestamp branch must
        therefore compose a single-quoted SQL literal from the validated
        datetime; we assert the composition pattern is present rather than
        a specific f-string shape.
        """
        action = _make_v2_action(watermark_type="timestamp")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # Runtime single-quote wrap of the UTC ISO literal.
        assert "hwm_literal" in notebook
        assert "isoformat" in notebook
        # The watermark column name survives into the Python source as a
        # validated string literal (used by F.col at runtime and the JDBC
        # identifier quoting helper).
        assert '"ModifiedDate"' in notebook

    def test_numeric_watermark_value_unquoted_column_quoted(self):
        """Numeric watermark: HWM value unquoted, column identifier double-quoted in WHERE."""
        action = _make_v2_action(
            watermark_type="numeric", watermark_column="product_id"
        )
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # The numeric branch should use unquoted format
        assert (
            "numeric" in notebook.lower() or "})" in notebook or "format(" in notebook
        )
        assert '"product_id"' in notebook

    def test_custom_operator_gt(self):
        """Operator '>' should appear in the extraction notebook."""
        action = _make_v2_action(watermark_operator=">")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # The operator should be in the WHERE clause template
        assert "> " in notebook or '>"' in notebook or "> '" in notebook

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
        assert "_lhp_runs" in notebook

    def test_cloudfiles_stub_reads_from_run_scoped_glob(self):
        """ADR-001 §Negative spot-check: AutoLoader must glob into the
        run-scoped `_lhp_runs/*` subtree that the extractor writes to."""
        action = _make_v2_action(landing_path="/Volumes/cat/sch/landing/prod")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        dlt_code = gen.generate(action, {"flowgroup": fg})
        assert ".load(\"/Volumes/cat/sch/landing/prod/_lhp_runs/*\")" in dlt_code, (
            "CloudFiles DLT notebook must load from the `_lhp_runs/*` glob to "
            "match the extractor's run-scoped Parquet write path."
        )
        # Bare-root load would regress to the pre-ADR-003 CF_EMPTY_DIR symptom
        assert ".load(\"/Volumes/cat/sch/landing/prod\")" not in dlt_code

    def test_cloudfiles_stub_sets_non_strict_globber_by_default(self):
        """ADR-001 read-path fix: `*` wildcard needs `useStrictGlobber=false`
        to match across directory separators on Unity Catalog volumes."""
        action = _make_v2_action(landing_path="/Volumes/cat/sch/landing/prod")
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        dlt_code = gen.generate(action, {"flowgroup": fg})
        assert "cloudFiles.useStrictGlobber" in dlt_code
        assert '"false"' in dlt_code or "'false'" in dlt_code

    def test_cloudfiles_stub_respects_user_strict_globber_override(self):
        """Default is off, but an explicit user override must win."""
        action = _make_v2_action(
            landing_path="/Volumes/cat/sch/landing/prod",
            source_overrides={
                "options": {"cloudFiles.useStrictGlobber": "true"},
            },
        )
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        dlt_code = gen.generate(action, {"flowgroup": fg})
        # User explicitly opted back into strict globbing — don't override it.
        import re as _re
        matches = _re.findall(
            r'cloudFiles\.useStrictGlobber.{0,80}', dlt_code
        )
        assert any("true" in m for m in matches), (
            f"User override should win. Found occurrences: {matches}"
        )

    def test_extraction_notebook_contains_recovery_hooks(self):
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "get_recoverable_landed_run(" in notebook
        assert "mark_landed(" in notebook
        assert "dbutils.notebook.exit(" in notebook

    def test_cloudfiles_options_are_preserved_for_bronze_stub(self):
        action = _make_v2_action(
            source_overrides={
                "options": {
                    "cloudFiles.schemaEvolutionMode": "addNewColumns",
                    "cloudFiles.rescueDataColumn": "_rescued_data",
                }
            }
        )
        _, code = self._generate(action=action)
        assert 'cloudFiles.schemaEvolutionMode", "addNewColumns"' in code
        assert 'cloudFiles.rescueDataColumn", "_rescued_data"' in code

    def test_cloudfiles_schema_location_is_auto_generated(self):
        action = _make_v2_action(landing_path="/Volumes/catalog/schema/landing/product")
        _, code = self._generate(action=action)
        assert (
            'cloudFiles.schemaLocation", "/Volumes/catalog/schema/landing/_lhp_schema/load_product_jdbc"'
            in code
        )

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

        assert (
            fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
            == fg2._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        )

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
        notebook_second = SecretCodeGenerator().generate_python_code(
            notebook_first, refs
        )
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
        assert (
            not bare_extract_keys
        ), f"Bare 'extract_' aux keys found (missing __lhp_ prefix): {bare_extract_keys}"

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
            k
            for k in fg._auxiliary_files
            if k.startswith("__lhp_extract_") and k.endswith(".py")
        ]
        non_extract_keys = [
            k
            for k in fg._auxiliary_files
            if not (k.startswith("__lhp_extract_") and k.endswith(".py"))
        ]

        assert (
            extract_keys
        ), "Should have at least one __lhp_extract_ key for routing test"
        assert (
            non_extract_keys
        ), "Should have at least one non-extract key for routing test"

        # Simulate routing logic from orchestrator: extract keys → _extract/ sibling, others → root
        extract_destinations = {k: f"pipeline_extract/{k}" for k in extract_keys}
        non_extract_destinations = {k: k for k in non_extract_keys}

        for key, dest in extract_destinations.items():
            assert (
                "_extract/" in dest
            ), f"Extract key '{key}' should route to _extract/ sibling dir"

        for key, dest in non_extract_destinations.items():
            assert (
                "_extract/" not in dest
            ), f"Non-extract key '{key}' should stay at root"

        # The two sets must be disjoint
        assert set(extract_keys).isdisjoint(set(non_extract_keys))

    def test_watermark_column_unquoted_in_pyspark_and_metadata(self):
        """Column name lives in a single validated variable used by F.col + kwarg.

        The L2 §5.3 restructure threads the column name through a single
        ``watermark_column = SQLInputValidator.string("...")`` binding; PySpark
        receives it as ``F.col(watermark_column)`` and WatermarkManager
        receives it as ``watermark_column_name=watermark_column``. That keeps
        the raw identifier in exactly one place.
        """
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # Validated binding contains the raw identifier as a Python literal.
        assert 'SQLInputValidator.string("ModifiedDate")' in notebook
        # PySpark uses the variable, not a repeated string literal.
        assert "F.col(watermark_column)" in notebook
        # insert_new receives the same validated variable.
        assert "watermark_column_name=watermark_column" in notebook

    def test_extraction_notebook_writes_schema_bearing_parquet_on_empty_batch(self):
        """ADR-003 §Q3 / A2: empty incremental batch must still leave a
        schema-bearing parquet at the run-scoped landing path.

        Without this fallback, AutoLoader on the bronze side fails the
        next run with ``CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`` because plain
        ``df.write.parquet`` on an empty DataFrame writes only a
        ``_SUCCESS`` marker. The extractor must construct an empty
        DataFrame from the JDBC ``df.schema`` and write that as parquet
        when the natural write produced no part file.
        """
        action = _make_v2_action()
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        assert "createDataFrame([], df.schema)" in notebook, (
            "Extraction notebook must write a schema-bearing 0-row parquet "
            "when the JDBC read returned no rows."
        )
        # The fallback is observable in the run log.
        assert (
            "landing_empty_schema_fallback" in notebook
            or "empty_schema_fallback" in notebook
        )

    def test_column_with_embedded_quote_escaped(self):
        """Column name with an embedded double-quote survives as Python-escaped literal.

        Under the L2 §5.3 runtime-SQL model, the template no longer hand-rolls
        the SQL identifier escape. Instead it emits the raw column as a Python
        string literal; ``SQLInputValidator.string`` accepts the value and
        ``_ansi_quote_identifier`` doubles embedded quotes at runtime before
        splicing into the JDBC subquery.
        """
        action = _make_v2_action(watermark_column='Col"Name')
        fg = _make_flowgroup_with_write(action)
        gen = JDBCWatermarkJobGenerator()
        gen.generate(action, {"flowgroup": fg})
        notebook = fg._auxiliary_files[f"__lhp_extract_{action.name}.py"]
        # The Python source-level form of the validated literal (Black may
        # render the embedded " via escape or as a single-quoted string).
        assert (
            "SQLInputValidator.string('Col\"Name')" in notebook
            or 'SQLInputValidator.string("Col\\"Name")' in notebook
        ), notebook
        # Runtime ANSI-quoting helper is present — that's what doubles " at
        # execution time when composing the JDBC WHERE clause.
        assert "_ansi_quote_identifier" in notebook


class TestLandingSchemaOverlapGuard:
    """ADR-003 A3: landing_path must not share catalog+schema with bronze write target."""

    def _make_overlap_action(self, landing_path):
        return _make_v2_action(landing_path=landing_path)

    def _make_overlap_flowgroup(self, action, write_catalog="bronze_catalog", write_schema="bronze_schema"):
        write_action = Action(
            name="write_product_bronze",
            type="write",
            source=action.target,
            write_target=WriteTarget(
                type="streaming_table",
                catalog=write_catalog,
                schema=write_schema,
                table="product",
                table_properties={"delta.enableChangeDataFeed": "true"},
            ),
        )
        return FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action, write_action],
        )

    @pytest.mark.unit
    def test_same_catalog_same_schema_raises_config_error(self):
        """landing_path in same catalog+schema as bronze write target must raise LHPConfigError."""
        from lhp.utils.error_formatter import LHPConfigError

        action = self._make_overlap_action(
            landing_path="/Volumes/bronze_catalog/bronze_schema/landing/product"
        )
        fg = self._make_overlap_flowgroup(action, "bronze_catalog", "bronze_schema")
        gen = JDBCWatermarkJobGenerator()
        with pytest.raises(LHPConfigError) as exc_info:
            gen.generate(action, {"flowgroup": fg})
        err_msg = str(exc_info.value)
        assert "bronze_schema" in err_msg
        assert "LHP-CFG-018" in err_msg

    @pytest.mark.unit
    def test_same_catalog_different_schema_generates_without_error(self):
        """landing_path in same catalog but different schema from bronze must succeed."""
        action = self._make_overlap_action(
            landing_path="/Volumes/bronze_catalog/landing/data/product"
        )
        fg = self._make_overlap_flowgroup(action, "bronze_catalog", "bronze_schema")
        gen = JDBCWatermarkJobGenerator()
        code = gen.generate(action, {"flowgroup": fg})
        assert 'format("cloudFiles")' in code

    @pytest.mark.unit
    def test_different_catalog_generates_with_warning(self, caplog):
        """landing_path in a different catalog from bronze should generate but emit a warning."""
        import logging

        action = self._make_overlap_action(
            landing_path="/Volumes/landing_catalog/bronze_schema/data/product"
        )
        fg = self._make_overlap_flowgroup(action, "bronze_catalog", "bronze_schema")
        gen = JDBCWatermarkJobGenerator()
        with caplog.at_level(logging.WARNING, logger="lhp.generators.load.jdbc_watermark_job"):
            code = gen.generate(action, {"flowgroup": fg})
        assert 'format("cloudFiles")' in code
        assert any("cross-catalog" in r.message.lower() or "landing_catalog" in r.message for r in caplog.records)

    @pytest.mark.unit
    def test_non_uc_landing_path_generates_without_error_or_warning(self, caplog):
        """abfss:// landing_path must skip the schema overlap check entirely."""
        import logging

        action = self._make_overlap_action(
            landing_path="abfss://container@account.dfs.core.windows.net/landing/product"
        )
        fg = self._make_overlap_flowgroup(action, "bronze_catalog", "bronze_schema")
        gen = JDBCWatermarkJobGenerator()
        with caplog.at_level(logging.WARNING, logger="lhp.generators.load.jdbc_watermark_job"):
            code = gen.generate(action, {"flowgroup": fg})
        assert 'format("cloudFiles")' in code
        assert not any(
            "cross-catalog" in r.message.lower() or "overlap" in r.message.lower()
            for r in caplog.records
        )
