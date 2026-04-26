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
from lhp.utils.error_formatter import LHPConfigError

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

    @classmethod
    def _assert_load_group_threaded(
        cls, content: str, call_prefixes: tuple
    ) -> None:
        """Assert every ``wm.<method>(...)`` call in ``call_prefixes`` passes
        ``load_group=load_group``.

        Centralizes the per-call assertion so a regression in any single
        ``WatermarkManager`` call site fails on this one test rather than
        only on whichever focused test happens to cover that method.
        Mirrors the four-cell parametrize matrix in
        ``tests/lhp_watermark/`` by exercising all manager call sites the
        generated notebook should thread ``load_group`` through.
        """
        missing: list = []
        for prefix in call_prefixes:
            block = cls._extract_call_block(content, prefix)
            if "load_group=load_group" not in block:
                missing.append((prefix, block))
        assert not missing, (
            "Generated notebook must thread load_group=load_group to every "
            f"WatermarkManager call site. Missing kwarg in: "
            + "; ".join(p for p, _ in missing)
            + "\nFirst missing block:\n"
            + (missing[0][1] if missing else "")
        )

    def test_extraction_notebook_threads_load_group_to_all_wm_calls(self, v2_project):
        """Composite ``load_group`` must be threaded to every
        ``WatermarkManager`` call site emitted by the JDBC template:
        read-side (``get_latest_watermark``, ``get_recoverable_landed_run``),
        write-side (``insert_new``, ``mark_landed``), and any state-machine
        completion (``mark_complete``) the template renders.

        Consolidates the U3-era per-method assertions (insert_new +
        get_latest_watermark) into a single matrix-style assertion so a
        regression in any one site surfaces here rather than depending on
        which focused test happens to cover it.
        """
        _, output_dir = self._generate(v2_project)
        notebook = (
            output_dir / "crm_bronze_extract" / "__lhp_extract_load_product_jdbc.py"
        )
        content = notebook.read_text()
        # Discover which wm.<method>(...) call sites the template renders;
        # run the threaded-kwarg assertion against each present site.
        candidate_prefixes = (
            "wm.insert_new(",
            "wm.get_latest_watermark(",
            "wm.get_recoverable_landed_run(",
            "wm.mark_landed(",
            "wm.mark_complete(",
        )
        present = tuple(p for p in candidate_prefixes if p in content)
        assert present, (
            "Expected the JDBC extraction notebook to call at least one "
            f"WatermarkManager method; none of {candidate_prefixes} found"
        )
        self._assert_load_group_threaded(content, present)

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


# ---------------------------------------------------------------------------
# B2 for_each integration test fixtures
# ---------------------------------------------------------------------------

def _b2_action_yaml(
    name: str,
    table_name: str,
    schema_name: str = "pub",
    source_system_id: str = "pg_crm",
    landing_path: str = "/Volumes/cat/land/root",
    wm_catalog: str = "metadata",
    wm_schema: str = "dev_orchestration",
) -> str:
    """Return a single jdbc_watermark_v2 LOAD action as a YAML block."""
    return f"""\
  - name: {name}
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://host:5432/db"
      user: "u"
      password: "p"
      driver: "org.postgresql.Driver"
      table: '"pub"."{table_name}"'
      schema_name: "{schema_name}"
      table_name: "{table_name}"
    watermark:
      column: updated_at
      type: timestamp
      operator: ">"
      source_system_id: "{source_system_id}"
      catalog: "{wm_catalog}"
      schema: "{wm_schema}"
    target: v_{name}
    landing_path: "{landing_path}"
"""


def _b2_write_action_yaml(name: str = "write_bronze", source: str = "v_load_a") -> str:
    return f"""\
  - name: {name}
    type: write
    source: {source}
    write_target:
      type: streaming_table
      catalog: bronze_catalog
      schema: bronze
      table: product_bronze
      table_properties:
        delta.enableChangeDataFeed: "true"
"""


def _build_b2_project(
    tmp_path: Path,
    flowgroup_yaml: str,
    pipeline: str = "crm_bronze",
    flowgroup: str = "product_ingestion",
    subst_dev: str = "dev:\n  catalog: bronze_catalog\n  schema: bronze\n",
    subst_prod: str = "prod:\n  catalog: bronze_catalog\n  schema: bronze\n",
) -> Path:
    """Create a minimal LHP project with the given flowgroup YAML."""
    project = tmp_path / "b2_project"
    project.mkdir(parents=True, exist_ok=True)
    (project / "lhp.yaml").write_text("name: b2_test_project\nversion: '1.0'\n")
    for d in ("presets", "templates", "substitutions", "generated"):
        (project / d).mkdir()
    (project / "substitutions" / "dev.yaml").write_text(subst_dev)
    (project / "substitutions" / "prod.yaml").write_text(subst_prod)
    pipeline_dir = project / "pipelines" / pipeline
    pipeline_dir.mkdir(parents=True)
    (pipeline_dir / f"{flowgroup}.yaml").write_text(flowgroup_yaml)
    return project


def _make_b2_flowgroup_yaml(
    pipeline: str = "crm_bronze",
    flowgroup: str = "product_ingestion",
    execution_mode: str = "for_each",
    concurrency: int = 3,
    actions_yaml: str = "",
    extra_workflow: str = "",
) -> str:
    workflow_block = f"workflow:\n  execution_mode: {execution_mode}\n  concurrency: {concurrency}\n"
    if extra_workflow:
        workflow_block += extra_workflow
    return (
        f"pipeline: {pipeline}\n"
        f"flowgroup: {flowgroup}\n"
        f"{workflow_block}"
        "actions:\n"
        + actions_yaml
    )


# ---------------------------------------------------------------------------
# Three-action B2 fixture (shared across most tests)
# ---------------------------------------------------------------------------

_B2_ACTIONS_3 = (
    _b2_action_yaml("load_a", "table_a")
    + _b2_action_yaml("load_b", "table_b")
    + _b2_action_yaml("load_c", "table_c")
    + _b2_write_action_yaml("write_bronze", "v_load_a")
)

_B2_FLOWGROUP_YAML_3 = _make_b2_flowgroup_yaml(actions_yaml=_B2_ACTIONS_3)


@pytest.mark.integration
class TestJDBCWatermarkV2ForEachIntegration:
    """End-to-end: B2 for_each YAML → three-task DAB topology + aux artifacts."""

    def _generate(self, project: Path, env: str = "dev"):
        output_dir = project / "generated"
        orchestrator = ActionOrchestrator(project)
        generated = orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env=env,
            output_dir=output_dir,
        )
        return generated, output_dir

    def _workflow_yaml(self, project: Path) -> str:
        return (project / "resources" / "lhp" / "crm_bronze_workflow.yml").read_text()

    # ------------------------------------------------------------------
    # Scenario 1: Happy path B2 — three-task topology + aux files
    # ------------------------------------------------------------------

    def test_b2_workflow_has_three_tasks(self, tmp_path):
        """B2 flowgroup → workflow YAML has prepare_manifest, for_each_ingest, validate tasks."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        self._generate(project)
        content = self._workflow_yaml(project)
        assert "prepare_manifest" in content
        assert "for_each_ingest" in content
        assert "validate" in content

    def test_b2_workflow_for_each_task_topology(self, tmp_path):
        """for_each_ingest task uses DAB for_each_task with iterations taskValue ref."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        self._generate(project)
        content = self._workflow_yaml(project)
        assert "for_each_task" in content
        # DAB runtime reference (escaped by {% raw %} in the Jinja template)
        assert "{{tasks.prepare_manifest.values.iterations}}" in content

    def test_b2_prepare_manifest_aux_file_exists(self, tmp_path):
        """prepare_manifest aux file is written to generated/crm_bronze/."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        _, output_dir = self._generate(project)
        pm = output_dir / "crm_bronze_extract" / "__lhp_prepare_manifest_product_ingestion.py"
        assert pm.exists(), f"prepare_manifest aux file not found at {pm}"

    def test_b2_validate_aux_file_exists(self, tmp_path):
        """validate aux file is written to generated/crm_bronze/."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        _, output_dir = self._generate(project)
        val = output_dir / "crm_bronze_extract" / "__lhp_validate_product_ingestion.py"
        assert val.exists(), f"validate aux file not found at {val}"

    def test_b2_extraction_notebook_has_taskvale_header(self, tmp_path):
        """B2 worker notebook contains the taskValues unpack header block."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        _, output_dir = self._generate(project)
        nb = (
            output_dir
            / "crm_bronze_extract"
            / "__lhp_extract_product_ingestion.py"
        )
        assert nb.exists(), f"B2 worker notebook not found at {nb}"
        content = nb.read_text()
        # B2 conditional header: unpacks iteration kwargs from widgets
        assert "dbutils.widgets.get" in content
        assert "__lhp_iteration" in content
        assert 'iteration["source_system_id"]' in content

    # ------------------------------------------------------------------
    # Scenario 2: Legacy regression — byte-identical workflow YAML
    # ------------------------------------------------------------------

    def test_legacy_workflow_yaml_no_b2_tasks(self, tmp_path):
        """Legacy flowgroup (no execution_mode) → workflow YAML has per-action tasks, not B2 topology."""
        project = tmp_path / "legacy_proj"
        project.mkdir()
        (project / "lhp.yaml").write_text("name: legacy\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()
        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
        )
        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "product_ingestion.yaml").write_text(FLOWGROUP_YAML)

        orchestrator = ActionOrchestrator(project)
        orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env="dev",
            output_dir=project / "generated",
        )
        content = self._workflow_yaml(project)
        # Legacy topology has per-action extract tasks
        assert "extract_load_product_jdbc" in content
        # B2 tasks must NOT appear
        assert "prepare_manifest" not in content
        assert "for_each_ingest" not in content

    def test_legacy_no_prepare_manifest_or_validate_files(self, tmp_path):
        """Legacy flowgroup emits no prepare_manifest.py or validate.py aux files."""
        project = tmp_path / "legacy_proj2"
        project.mkdir()
        (project / "lhp.yaml").write_text("name: legacy\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()
        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
        )
        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "product_ingestion.yaml").write_text(FLOWGROUP_YAML)

        orchestrator = ActionOrchestrator(project)
        orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env="dev",
            output_dir=project / "generated",
        )
        output_dir = project / "generated"
        # No B2 aux files should be emitted for legacy mode
        assert not (
            output_dir / "crm_bronze_extract" / "__lhp_prepare_manifest_product_ingestion.py"
        ).exists()
        assert not (
            output_dir / "crm_bronze_extract" / "__lhp_validate_product_ingestion.py"
        ).exists()

    # ------------------------------------------------------------------
    # Scenario 3: LHP-CFG-031 separator collision
    # ------------------------------------------------------------------

    def test_cfg_031_separator_in_pipeline_name(self, tmp_path):
        """pipeline='bronze::core' + execution_mode=for_each → raises LHP-CFG-031."""
        actions = (
            _b2_action_yaml("load_a", "table_a")
            + _b2_write_action_yaml("write_a", "v_load_a")
        )
        flowgroup_yaml = _make_b2_flowgroup_yaml(
            pipeline="bronze::core",
            flowgroup="product_ingestion",
            actions_yaml=actions,
        )
        project = _build_b2_project(
            tmp_path, flowgroup_yaml, pipeline="bronze::core", flowgroup="product_ingestion"
        )
        orchestrator = ActionOrchestrator(project)
        with pytest.raises(LHPConfigError) as exc_info:
            orchestrator.generate_pipeline_by_field(
                pipeline_field="bronze::core",
                env="dev",
                output_dir=project / "generated",
            )
        assert "LHP-CFG-031" in str(exc_info.value)

    # ------------------------------------------------------------------
    # Scenario 4: LHP-CFG-032 composite uniqueness
    # ------------------------------------------------------------------

    def test_cfg_032_duplicate_composite_detected(self, tmp_path):
        """Two for_each flowgroups with same pipeline::flowgroup composite → LHP-CFG-032."""
        project = tmp_path / "dup_proj"
        project.mkdir()
        (project / "lhp.yaml").write_text("name: dup\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()
        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
        )
        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)

        # Two YAML files that declare the same pipeline + flowgroup combination
        actions_a = (
            _b2_action_yaml("load_a", "table_a")
            + _b2_write_action_yaml("write_a", "v_load_a")
        )
        yaml_a = _make_b2_flowgroup_yaml(
            pipeline="crm_bronze",
            flowgroup="product_ingestion",
            actions_yaml=actions_a,
        )
        actions_b = (
            _b2_action_yaml("load_b", "table_b")
            + _b2_write_action_yaml("write_b", "v_load_b")
        )
        yaml_b = _make_b2_flowgroup_yaml(
            pipeline="crm_bronze",
            flowgroup="product_ingestion",
            actions_yaml=actions_b,
        )
        (pipeline_dir / "product_ingestion.yaml").write_text(yaml_a)
        (pipeline_dir / "product_ingestion_dup.yaml").write_text(yaml_b)

        orchestrator = ActionOrchestrator(project)
        errors, _ = orchestrator.validate_pipeline_by_field(
            pipeline_field="crm_bronze", env="dev"
        )
        cfg032_errors = [e for e in errors if "LHP-CFG-032" in e]
        assert cfg032_errors, (
            "Expected LHP-CFG-032 error for duplicate composite; got errors:\n"
            + "\n".join(errors)
        )
        # Error message must name both flowgroups by their composite key
        assert "crm_bronze::product_ingestion" in cfg032_errors[0]

    # ------------------------------------------------------------------
    # Scenario 5: LHP-CFG-033 >300 actions
    # ------------------------------------------------------------------

    def test_cfg_033_cap_exceeded_301_actions(self, tmp_path):
        """flowgroup expanding to 301 actions → raises LHP-CFG-033 with split suggestion."""
        # The validator counts ALL actions (load + write). Use 300 JDBC loads + 1 write
        # = 301 total, which exceeds the cap of 300.
        # All load actions share source_system_id + landing_path so the shared-keys
        # check does not fire before the count check.
        load_actions = "".join(
            _b2_action_yaml(
                name=f"load_tbl_{i:03d}",
                table_name=f"table_{i:03d}",
            )
            for i in range(300)
        )
        flowgroup_yaml = _make_b2_flowgroup_yaml(
            actions_yaml=load_actions + _b2_write_action_yaml("write_z", "v_load_tbl_000"),
        )
        project = _build_b2_project(tmp_path, flowgroup_yaml)
        orchestrator = ActionOrchestrator(project)
        with pytest.raises(LHPConfigError) as exc_info:
            orchestrator.generate_pipeline_by_field(
                pipeline_field="crm_bronze",
                env="dev",
                output_dir=project / "generated",
            )
        error_text = str(exc_info.value)
        assert "LHP-CFG-033" in error_text
        assert "Split" in error_text, (
            "Expected split-suggestion in error text; got:\n" + error_text
        )

    # ------------------------------------------------------------------
    # Scenario 6: LHP-CFG-033 mixed-mode (validator catches)
    # ------------------------------------------------------------------

    def test_cfg_033_mixed_mode_in_pipeline(self, tmp_path):
        """Pipeline with one for_each + one default flowgroup → LHP-CFG-033 mixed-mode error."""
        project = tmp_path / "mixed_proj"
        project.mkdir()
        (project / "lhp.yaml").write_text("name: mixed\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()
        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
        )
        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)

        # for_each flowgroup
        for_each_yaml = _make_b2_flowgroup_yaml(
            pipeline="crm_bronze",
            flowgroup="product_ingestion",
            actions_yaml=(
                _b2_action_yaml("load_a", "table_a")
                + _b2_write_action_yaml("write_a", "v_load_a")
            ),
        )
        # legacy (default) flowgroup in the same pipeline
        legacy_yaml = (
            "pipeline: crm_bronze\n"
            "flowgroup: order_ingestion\n"
            "actions:\n"
            + _b2_action_yaml(
                "load_order",
                "orders",
                source_system_id="pg_crm",
                landing_path="/Volumes/cat/land/root",
            )
            + "  - name: write_order\n"
            "    type: write\n"
            "    source: v_load_order\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            "      catalog: bronze_catalog\n"
            "      schema: bronze\n"
            "      table: orders\n"
        )
        (pipeline_dir / "product_ingestion.yaml").write_text(for_each_yaml)
        (pipeline_dir / "order_ingestion.yaml").write_text(legacy_yaml)

        orchestrator = ActionOrchestrator(project)
        errors, _ = orchestrator.validate_pipeline_by_field(
            pipeline_field="crm_bronze", env="dev"
        )
        # Accept either CFG-033 (validator) or CFG-034 (orchestrator codegen guard)
        mixed_mode_errors = [
            e for e in errors if "LHP-CFG-033" in e or "LHP-CFG-034" in e
        ]
        assert mixed_mode_errors, (
            "Expected mixed-mode error (LHP-CFG-033 or LHP-CFG-034); got:\n"
            + "\n".join(errors)
        )

    # ------------------------------------------------------------------
    # Scenario 7: 300 cap inclusive — no error, manifest has 300 entries
    # ------------------------------------------------------------------

    def test_300_actions_inclusive_no_error(self, tmp_path):
        """Exactly 300 total actions (299 JDBC loads + 1 write) → no error; prepare_manifest has 299 entries."""
        # The validator counts ALL actions (load + write) in the flowgroup.
        # 299 JDBC loads + 1 write = 300 total = exactly at the cap (inclusive).
        load_actions = "".join(
            _b2_action_yaml(
                name=f"load_tbl_{i:03d}",
                table_name=f"table_{i:03d}",
            )
            for i in range(299)
        )
        flowgroup_yaml = _make_b2_flowgroup_yaml(
            actions_yaml=load_actions + _b2_write_action_yaml("write_z", "v_load_tbl_000"),
        )
        project = _build_b2_project(tmp_path, flowgroup_yaml)
        _, output_dir = self._generate(project)
        pm = output_dir / "crm_bronze_extract" / "__lhp_prepare_manifest_product_ingestion.py"
        assert pm.exists(), "prepare_manifest aux file not found for 300-action flowgroup"
        content = pm.read_text()
        # Each of the 299 JDBC load actions contributes one entry in _manifest_rows
        # and one in _iterations. The write action is not a JDBC v2 action so it
        # does not appear in the manifest. Count distinct load_tbl_ tokens.
        assert content.count("load_tbl_") >= 299, (
            f"Expected 299 JDBC action entries in prepare_manifest; "
            f"found {content.count('load_tbl_')} occurrences"
        )

    # ------------------------------------------------------------------
    # Scenario 8: Concurrency default — 7 actions, no concurrency set
    # ------------------------------------------------------------------

    def test_concurrency_default_equals_action_count(self, tmp_path):
        """7 actions, no concurrency in workflow → workflow YAML concurrency: 7."""
        load_actions = "".join(
            _b2_action_yaml(f"load_{i}", f"tbl_{i}") for i in range(7)
        )
        flowgroup_yaml = (
            "pipeline: crm_bronze\n"
            "flowgroup: product_ingestion\n"
            "workflow:\n"
            "  execution_mode: for_each\n"
            "actions:\n"
            + load_actions
            + _b2_write_action_yaml("write_z", "v_load_0")
        )
        project = _build_b2_project(tmp_path, flowgroup_yaml)
        self._generate(project)
        workflow_content = self._workflow_yaml(project)
        data = yaml.safe_load(workflow_content)
        # Navigate to the for_each_task concurrency field
        tasks = data["resources"]["jobs"]["crm_bronze_workflow"]["tasks"]
        for_each_task = next(t for t in tasks if t.get("task_key") == "for_each_ingest")
        assert for_each_task["for_each_task"]["concurrency"] == 7, (
            f"Expected concurrency=7 for 7 actions; got {for_each_task}"
        )

    # ------------------------------------------------------------------
    # Scenario 9: Per-env catalog substitution
    # ------------------------------------------------------------------

    def test_per_env_catalog_in_prepare_manifest(self, tmp_path):
        """Different wm_catalog per env lands correctly in prepare_manifest and validate."""
        # Embed env-specific catalog/schema directly in the watermark config so
        # the generator picks them up (wm_catalog/wm_schema come from watermark.catalog
        # and watermark.schema, not from the substitution file).
        dev_yaml = _make_b2_flowgroup_yaml(
            actions_yaml=(
                _b2_action_yaml(
                    "load_a",
                    "table_a",
                    wm_catalog="dev_metadata",
                    wm_schema="dev_orchestration",
                )
                + _b2_write_action_yaml("write_a", "v_load_a")
            ),
        )
        prod_yaml = _make_b2_flowgroup_yaml(
            actions_yaml=(
                _b2_action_yaml(
                    "load_a",
                    "table_a",
                    wm_catalog="prod_metadata",
                    wm_schema="prod_orchestration",
                )
                + _b2_write_action_yaml("write_a", "v_load_a")
            ),
        )

        # Dev project
        dev_project = _build_b2_project(tmp_path / "dev_proj", dev_yaml)
        _, dev_out = self._generate(dev_project, env="dev")
        dev_pm = (dev_out / "crm_bronze_extract" / "__lhp_prepare_manifest_product_ingestion.py").read_text()
        assert "dev_metadata" in dev_pm, "dev wm_catalog not found in prepare_manifest"
        assert "dev_orchestration" in dev_pm, "dev wm_schema not found in prepare_manifest"
        dev_val = (dev_out / "crm_bronze_extract" / "__lhp_validate_product_ingestion.py").read_text()
        assert "dev_metadata" in dev_val, "dev wm_catalog not found in validate"

        # Prod project
        prod_project = _build_b2_project(tmp_path / "prod_proj", prod_yaml)
        _, prod_out = self._generate(prod_project, env="prod")
        prod_pm = (prod_out / "crm_bronze_extract" / "__lhp_prepare_manifest_product_ingestion.py").read_text()
        assert "prod_metadata" in prod_pm, "prod wm_catalog not found in prepare_manifest"
        assert "prod_orchestration" in prod_pm, "prod wm_schema not found in prepare_manifest"

    # ------------------------------------------------------------------
    # Scenario 10: Parity flag — validate.py contains parity SQL block
    # ------------------------------------------------------------------

    def test_parity_flag_enables_parity_sql_block(self, tmp_path):
        """workflow.parity_check: true → validate.py contains parity_mismatches SQL block."""
        parity_yaml = (
            "pipeline: crm_bronze\n"
            "flowgroup: product_ingestion\n"
            "workflow:\n"
            "  execution_mode: for_each\n"
            "  concurrency: 3\n"
            "  parity_check: true\n"
            "actions:\n"
            + _b2_action_yaml("load_a", "table_a")
            + _b2_write_action_yaml("write_a", "v_load_a")
        )
        project = _build_b2_project(tmp_path, parity_yaml)
        _, output_dir = self._generate(project)
        val = (output_dir / "crm_bronze_extract" / "__lhp_validate_product_ingestion.py").read_text()
        assert "parity_mismatches" in val, (
            "Expected parity SQL block in validate.py when parity_check: true; "
            "did not find 'parity_mismatches'"
        )

    # ------------------------------------------------------------------
    # Scenario 11: HIPAA hook marker survives full B2 render
    # ------------------------------------------------------------------

    def test_hipaa_hook_marker_in_b2_worker_notebook(self, tmp_path):
        """B2 worker notebook contains HIPAA hook insertion point marker (U5 contract)."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        _, output_dir = self._generate(project)
        nb = (
            output_dir
            / "crm_bronze_extract"
            / "__lhp_extract_product_ingestion.py"
        )
        assert nb.exists()
        content = nb.read_text()
        assert "HIPAA hook insertion point" in content, (
            "B2 worker notebook must contain 'HIPAA hook insertion point' marker; "
            "U5 template comment was stripped or missing"
        )

    # ------------------------------------------------------------------
    # Scenario 12: Per-flowgroup worker path resolution
    # ------------------------------------------------------------------

    def test_b2_worker_path_is_per_flowgroup_not_per_action(self, tmp_path):
        """B2 mode: workflow YAML worker_path refs per-flowgroup name; only ONE extract file."""
        project = _build_b2_project(tmp_path, _B2_FLOWGROUP_YAML_3)
        _, output_dir = self._generate(project)
        workflow_content = self._workflow_yaml(project)
        # Worker path should reference the flowgroup name, not any action name
        assert "__lhp_extract_product_ingestion" in workflow_content
        # Per-action keys must NOT appear in B2 workflow
        assert "__lhp_extract_load_a" not in workflow_content
        assert "__lhp_extract_load_b" not in workflow_content
        assert "__lhp_extract_load_c" not in workflow_content
        # Exactly ONE extract file on disk (per-flowgroup)
        extract_dir = output_dir / "crm_bronze_extract"
        extract_files = [
            f.name for f in extract_dir.iterdir()
            if f.name.startswith("__lhp_extract_") and f.name.endswith(".py")
        ]
        assert extract_files == ["__lhp_extract_product_ingestion.py"], (
            f"Expected exactly one per-flowgroup extract file; found {extract_files}"
        )

    def test_legacy_worker_path_is_per_action(self, tmp_path):
        """Legacy mode: workflow YAML has per-action extract task keys (N files, not 1)."""
        project = tmp_path / "legacy_proj3"
        project.mkdir()
        (project / "lhp.yaml").write_text("name: legacy\nversion: '1.0'\n")
        for d in ("presets", "templates", "substitutions", "generated"):
            (project / d).mkdir()
        (project / "substitutions" / "dev.yaml").write_text(
            "dev:\n  catalog: bronze_catalog\n  schema: bronze\n"
        )
        pipeline_dir = project / "pipelines" / "crm_bronze"
        pipeline_dir.mkdir(parents=True)
        (pipeline_dir / "product_ingestion.yaml").write_text(FLOWGROUP_YAML)

        orchestrator = ActionOrchestrator(project)
        orchestrator.generate_pipeline_by_field(
            pipeline_field="crm_bronze",
            env="dev",
            output_dir=project / "generated",
        )
        workflow_content = self._workflow_yaml(project)
        # Legacy uses extract_{action_name} task keys
        assert "extract_load_product_jdbc" in workflow_content
        # Extract file uses per-action name
        extract_dir = project / "generated" / "crm_bronze_extract"
        extract_files = [
            f.name for f in extract_dir.iterdir()
            if f.name.startswith("__lhp_extract_") and f.name.endswith(".py")
        ]
        assert "__lhp_extract_load_product_jdbc.py" in extract_files
