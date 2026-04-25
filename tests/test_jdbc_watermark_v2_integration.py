"""Integration test: JDBC watermark v2 YAML → 3 generated artifacts.

Verifies end-to-end generation from a flowgroup YAML config with
source.type: jdbc_watermark_v2 through ActionOrchestrator, producing:
  1. CloudFiles DLT stub (primary pipeline file)
  2. Extraction Job notebook (auxiliary file with WatermarkManager)
  3. DAB Workflow resource YAML (extraction → DLT orchestration)
"""

import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

from lhp.core.orchestrator import ActionOrchestrator

FLOWGROUP_YAML = """\
pipeline: crm_bronze
flowgroup: product_ingestion

actions:
  - name: load_product_jdbc
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://host:5432/db"
      user: "test_user"
      password: "test_pass"
      driver: "org.postgresql.Driver"
      table: '"Production"."Product"'
      schema_name: "Production"
      table_name: "Product"
    watermark:
      column: "ModifiedDate"
      type: "timestamp"
      operator: ">="
      source_system_id: "pg_crm"
    target: v_product_raw
    landing_path: "/Volumes/bronze_catalog/landing/landing/product"

  - name: write_product_bronze
    type: write
    source: v_product_raw
    write_target:
      type: streaming_table
      catalog: "bronze_catalog"
      schema: "bronze"
      table: "product"
      table_properties:
        delta.enableChangeDataFeed: "true"
"""


@pytest.fixture
def v2_project(tmp_path):
    """Create a minimal LHP project with a jdbc_watermark_v2 flowgroup."""
    project = tmp_path / "v2_project"
    project.mkdir()

    (project / "lhp.yaml").write_text("name: v2_test_project\nversion: '1.0'\n")
    for d in ("presets", "templates", "substitutions", "generated"):
        (project / d).mkdir()

    (project / "substitutions" / "dev.yaml").write_text(
        "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
    )

    pipeline_dir = project / "pipelines" / "crm_bronze"
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / "product_ingestion.yaml").write_text(FLOWGROUP_YAML)

    yield project
    shutil.rmtree(tmp_path, ignore_errors=True)


@pytest.mark.integration
class TestJDBCWatermarkV2Integration:
    """End-to-end: YAML config → lhp generate → 3 output artifacts."""

    def _generate(self, project: Path):
        output_dir = project / "generated"
        orchestrator = ActionOrchestrator(project)
        generated = orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env="dev",
            output_dir=output_dir,
        )
        return generated, output_dir

    # ------------------------------------------------------------------
    # Artifact 1: CloudFiles DLT stub (primary output)
    # ------------------------------------------------------------------

    def test_generates_cloudfiles_dlt_stub(self, v2_project):
        """Primary output is a CloudFiles DLT pipeline file."""
        generated, _ = self._generate(v2_project)
        assert "product_ingestion.py" in generated
        code = generated["product_ingestion.py"]
        assert 'format("cloudFiles")' in code
        assert "spark.readStream" in code

    def test_dlt_stub_references_landing_path(self, v2_project):
        generated, _ = self._generate(v2_project)
        code = generated["product_ingestion.py"]
        assert "/Volumes/bronze_catalog/landing/landing/product" in code
        assert (
            "/Volumes/bronze_catalog/landing/landing/_lhp_schema/load_product_jdbc"
            in code
        )

    def test_dlt_stub_has_streaming_table_write(self, v2_project):
        generated, _ = self._generate(v2_project)
        code = generated["product_ingestion.py"]
        assert "create_streaming_table" in code or "streaming_table" in code.lower()

    # ------------------------------------------------------------------
    # Artifact 2: Extraction Job notebook (auxiliary file)
    # ------------------------------------------------------------------

    def test_extraction_notebook_written_to_disk(self, v2_project):
        """Auxiliary extraction notebook is written to generated/{pipeline}/."""
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        assert notebook.exists(), f"Extraction notebook not found at {notebook}"

    def test_extraction_notebook_has_watermark_manager(self, v2_project):
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        assert "WatermarkManager" in content
        assert "get_latest_watermark(" in content
        assert "insert_new(" in content
        assert "get_recoverable_landed_run(" in content
        assert "mark_landed(" in content
        assert '["watermark_value"]' in content  # dict access pattern
        assert "watermark_column_name=" in content  # required kwarg
        assert '"ModifiedDate"' in content  # WHERE clause quotes column for JDBC

    # ------------------------------------------------------------------
    # Tier 2 (U3): load_group composite threaded into the extraction notebook
    # ------------------------------------------------------------------

    def test_extraction_notebook_has_load_group_literal(self, v2_project):
        """Generator emits ``load_group = "{pipeline}::{flowgroup}"`` at module
        scope so every ``wm.<method>(...)`` call inside the notebook can pass
        ``load_group=load_group``."""
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        assert (
            'load_group = "crm_bronze::product_ingestion"' in content
        ), "Expected composite load_group literal at module scope"

    @staticmethod
    def _extract_call_block(content: str, call_prefix: str) -> str:
        """Return the substring spanning ``call_prefix( ... )`` with correctly
        balanced parentheses (so nested ``str(hwm_value)`` etc. don't fool a
        naive ``content.find(")", idx)``).
        """
        idx = content.find(call_prefix)
        assert idx >= 0, f"{call_prefix} not found in generated notebook"
        # Walk forward from the opening '(' tracking depth.
        depth = 0
        start_paren = content.find("(", idx)
        assert start_paren > idx
        i = start_paren
        while i < len(content):
            ch = content[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return content[idx : i + 1]
            i += 1
        raise AssertionError(f"unterminated call block for {call_prefix}")

    def test_extraction_notebook_threads_load_group_to_insert_new(self, v2_project):
        """``wm.insert_new(...)`` must pass ``load_group=load_group``."""
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        block = self._extract_call_block(content, "wm.insert_new(")
        assert (
            "load_group=load_group" in block
        ), f"insert_new must thread load_group=load_group; got block:\n{block}"

    def test_extraction_notebook_threads_load_group_to_get_latest(self, v2_project):
        """``wm.get_latest_watermark(...)`` must pass ``load_group=load_group``."""
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        block = self._extract_call_block(content, "wm.get_latest_watermark(")
        assert (
            "load_group=load_group" in block
        ), f"get_latest_watermark must thread load_group=load_group; got block:\n{block}"

    def test_extraction_notebook_has_jdbc_read(self, v2_project):
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        assert 'format("jdbc")' in content

    def test_extraction_notebook_has_parquet_write(self, v2_project):
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        assert 'format("parquet")' in content

    def test_extraction_notebook_no_dlt_decorators(self, v2_project):
        """Extraction notebook must NOT contain DLT-specific code."""
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        assert "@dp." not in content
        assert "dlt." not in content

    # ------------------------------------------------------------------
    # Artifact 3: DAB Workflow resource YAML
    # ------------------------------------------------------------------

    def test_workflow_yaml_written_to_disk(self, v2_project):
        """Workflow resource YAML is written to resources/lhp/."""
        self._generate(v2_project)
        workflow = v2_project / "resources" / "lhp" / "crm_bronze_workflow.yml"
        assert workflow.exists(), f"Workflow YAML not found at {workflow}"

    def test_workflow_yaml_is_parseable(self, v2_project):
        self._generate(v2_project)
        workflow = v2_project / "resources" / "lhp" / "crm_bronze_workflow.yml"
        data = yaml.safe_load(workflow.read_text())
        assert data is not None

    def test_workflow_has_extraction_task(self, v2_project):
        self._generate(v2_project)
        workflow = v2_project / "resources" / "lhp" / "crm_bronze_workflow.yml"
        content = workflow.read_text()
        assert "extract_load_product_jdbc" in content

    def test_workflow_has_dlt_pipeline_task(self, v2_project):
        self._generate(v2_project)
        workflow = v2_project / "resources" / "lhp" / "crm_bronze_workflow.yml"
        content = workflow.read_text()
        assert "dlt_crm_bronze" in content

    def test_workflow_has_depends_on(self, v2_project):
        """DLT pipeline task must depend on extraction tasks."""
        self._generate(v2_project)
        workflow = v2_project / "resources" / "lhp" / "crm_bronze_workflow.yml"
        content = workflow.read_text()
        assert "depends_on" in content

    # ------------------------------------------------------------------
    # Cross-cutting: non-v2 flowgroups are unaffected
    # ------------------------------------------------------------------

    def test_non_v2_flowgroup_produces_no_workflow_yaml(self, v2_project):
        """A standard CloudFiles flowgroup should NOT produce a Workflow YAML."""
        # Add a standard CloudFiles flowgroup alongside the v2 one
        pipeline_dir = v2_project / "pipelines" / "sales_bronze"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "customer_ingestion.yaml").write_text("""\
pipeline: sales_bronze
flowgroup: customer_ingestion

actions:
  - name: load_customer_raw
    type: load
    target: v_customer_raw
    source:
      type: cloudfiles
      path: "/mnt/landing/customer/*.json"
      format: json

  - name: write_customer_bronze
    type: write
    source: v_customer_raw
    write_target:
      type: streaming_table
      catalog: "bronze_catalog"
      schema: "bronze"
      table: "customer_raw"
""")
        orchestrator = ActionOrchestrator(v2_project)
        orchestrator.generate_pipeline_by_field(
            pipeline_field="sales_bronze",
            env="dev",
            output_dir=v2_project / "generated",
        )
        workflow = v2_project / "resources" / "lhp" / "sales_bronze_workflow.yml"
        assert not workflow.exists(), "Non-v2 pipeline should not produce workflow YAML"

    # ------------------------------------------------------------------
    # Secret resolution in extraction notebooks
    # ------------------------------------------------------------------

    def test_extraction_notebook_secrets_resolved(self, tmp_path):
        """Secret refs in extraction notebook resolve to dbutils.secrets.get()."""
        project = tmp_path / "secret_project"
        project.mkdir()

        (project / "lhp.yaml").write_text("name: secret_test\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()

        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n"
            "  catalog: bronze_catalog\n"
            "  schema: bronze\n"
            "secrets:\n"
            "  default_scope: dev-secrets\n"
            "  scopes:\n"
            "    jdbc: dev-secrets\n"
        )

        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "product_ingestion.yaml").write_text("""\
pipeline: crm_bronze
flowgroup: product_ingestion

actions:
  - name: load_product_jdbc
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://host:5432/db"
      user: "${secret:jdbc/jdbc_user}"
      password: "${secret:jdbc/jdbc_pass}"
      driver: "org.postgresql.Driver"
      table: '"Production"."Product"'
      schema_name: "Production"
      table_name: "Product"
    watermark:
      column: "ModifiedDate"
      type: "timestamp"
      operator: ">="
      source_system_id: "pg_crm"
    target: v_product_raw
    landing_path: "/Volumes/bronze_catalog/landing/landing/product"

  - name: write_product_bronze
    type: write
    source: v_product_raw
    write_target:
      type: streaming_table
      catalog: "bronze_catalog"
      schema: "bronze"
      table: "product"
      table_properties:
        delta.enableChangeDataFeed: "true"
""")
        output_dir = project / "generated"
        orchestrator = ActionOrchestrator(project)
        orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env="dev",
            output_dir=output_dir,
        )

        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        assert notebook.exists(), f"Extraction notebook not found at {notebook}"
        content = notebook.read_text()

        # Positive: secrets resolved
        assert "dbutils.secrets.get(" in content
        assert "dev-secrets" in content
        assert "jdbc_user" in content

        # Negative: no surviving placeholders
        assert (
            "__SECRET_" not in content
        ), f"Unresolved secret placeholder found in notebook:\n{content}"
