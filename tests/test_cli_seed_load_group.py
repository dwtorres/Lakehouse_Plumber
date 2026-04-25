"""Tests for the `lhp seed-load-group` CLI command (Tier 2 / U5).

The command emits per-table SELECT-preview + INSERT-seed SQL for the
Step 4a legacy-HWM seed migration of a single jdbc_watermark_v2
flowgroup. Tests cover the `--dry-run` path; the `--apply` path (real
Databricks SQL execution) is intentionally out of scope here.

See: docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md
     §U5; origin Step 4a in docs/planning/tier-2-hwm-load-group-fix.md.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _write_project(
    project_root: Path,
    *,
    flowgroup_yaml: str,
    env: str = "devtest",
    substitutions_yaml: str | None = None,
) -> Path:
    """Materialise a minimal LHP project with a single flowgroup.

    Returns the path to the flowgroup YAML.
    """
    (project_root / "pipelines" / "edp_bronze").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: tier2_seed_test\nversion: '1.0'\n"
    )

    fg_path = project_root / "pipelines" / "edp_bronze" / "customer_bronze.yaml"
    fg_path.write_text(flowgroup_yaml)

    sub_path = project_root / "substitutions" / f"{env}.yaml"
    sub_path.write_text(
        substitutions_yaml
        if substitutions_yaml is not None
        else _DEFAULT_SUBSTITUTIONS
    )
    return fg_path


_DEFAULT_SUBSTITUTIONS = """\
devtest:
  env: devtest
  watermark_catalog: metadata
  watermark_schema: devtest_orchestration
"""


_TWO_ACTION_FLOWGROUP = """\
pipeline: edp_bronze_jdbc_ingestion
flowgroup: customer_bronze

actions:
  - name: load_department_jdbc
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://example/db"
      user: "u"
      password: "p"
      driver: "org.postgresql.Driver"
      table: '"HumanResources"."Department"'
      schema_name: "HumanResources"
      table_name: "Department"
    target: v_department_raw
    landing_path: "/Volumes/x/y/landing/department"
    watermark:
      column: ModifiedDate
      type: timestamp
      operator: ">="
      source_system_id: pg_supabase_aw
      catalog: "${watermark_catalog}"
      schema: "${watermark_schema}"

  - name: load_employee_jdbc
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://example/db"
      user: "u"
      password: "p"
      driver: "org.postgresql.Driver"
      table: '"HumanResources"."Employee"'
      schema_name: "HumanResources"
      table_name: "Employee"
    target: v_employee_raw
    landing_path: "/Volumes/x/y/landing/employee"
    watermark:
      column: ModifiedDate
      type: timestamp
      operator: ">="
      source_system_id: pg_supabase_aw
      catalog: "${watermark_catalog}"
      schema: "${watermark_schema}"

  - name: write_department_bronze
    type: write
    source: v_department_raw
    write_target:
      type: streaming_table
      catalog: bronze_cat
      schema: bronze
      table: department
"""


_NO_JDBC_WM_V2_FLOWGROUP = """\
pipeline: edp_bronze_jdbc_ingestion
flowgroup: only_writes
actions:
  - name: load_static
    type: load
    source:
      type: sql
      sql: "SELECT 1 AS x"
    target: v_static
  - name: write_static
    type: write
    source: v_static
    write_target:
      type: streaming_table
      catalog: bronze_cat
      schema: bronze
      table: static
"""


_PIPELINE_WITH_SEPARATOR_FLOWGROUP = """\
pipeline: edp::bronze
flowgroup: customer_bronze

actions:
  - name: load_department_jdbc
    type: load
    source:
      type: jdbc_watermark_v2
      url: "jdbc:postgresql://example/db"
      user: "u"
      password: "p"
      driver: "org.postgresql.Driver"
      table: '"HumanResources"."Department"'
      schema_name: "HumanResources"
      table_name: "Department"
    target: v_department_raw
    landing_path: "/Volumes/x/y/landing/department"
    watermark:
      column: ModifiedDate
      type: timestamp
      operator: ">="
      source_system_id: pg_supabase_aw
      catalog: "${watermark_catalog}"
      schema: "${watermark_schema}"
"""


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _invoke(
    runner: CliRunner,
    project_root: Path,
    *args: str,
):
    """Invoke the CLI with cwd set to project_root.

    The `lhp seed-load-group` command resolves the project root via
    `lhp.yaml`, so we chdir into the project before invoking.
    """
    cwd = os.getcwd()
    try:
        os.chdir(str(project_root))
        return runner.invoke(cli, ["seed-load-group", *args], catch_exceptions=False)
    finally:
        os.chdir(cwd)


# ---------------------------------------------------------------------------
# Tests — written first; verify each fails before the command body lands.
# ---------------------------------------------------------------------------


class TestSeedLoadGroupHappyPath:
    """Test 1: fixture flowgroup with two `jdbc_watermark_v2` actions
    emits exactly two SELECT-preview blocks + two INSERT statements with
    the composite `load_group` value."""

    def test_emits_select_preview_and_insert_for_each_jdbc_action(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code == 0, result.output

        # Two SELECT previews + two INSERT statements (one per action).
        assert result.output.count("SELECT count(") == 2, result.output
        assert result.output.count("INSERT INTO ") == 2, result.output

        # Composite load_group must appear in both INSERTs.
        composite = "edp_bronze_jdbc_ingestion::customer_bronze"
        assert result.output.count(composite) >= 2, result.output

        # Both source tables must be referenced.
        assert "Department" in result.output
        assert "Employee" in result.output

        # Resolved registry namespace from substitutions.
        assert "metadata.devtest_orchestration.watermarks" in result.output

    def test_dry_run_is_default_and_emits_no_apply_warning(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code == 0, result.output
        # SQL is emitted on stdout only — no "applying" / "executing" message.
        lower = result.output.lower()
        assert "applied" not in lower
        assert "executed" not in lower


class TestSeedLoadGroupNoJdbcWatermarkActions:
    """Test 2: flowgroup with no jdbc_watermark_v2 actions emits no SQL
    and prints an informative message; exits 0."""

    def test_informative_message_when_no_jdbc_actions(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(
            project_root, flowgroup_yaml=_NO_JDBC_WM_V2_FLOWGROUP
        )

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code == 0, result.output
        assert "INSERT INTO " not in result.output
        assert "SELECT count(" not in result.output
        assert "no jdbc_watermark_v2" in result.output.lower()


class TestSeedLoadGroupMissingEnv:
    """Test 3: env not in substitutions/ → LHPFileError pointing to the
    missing file via cli_error_boundary."""

    def test_missing_env_substitution_file(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        # Env "qa" has no substitutions/qa.yaml file.
        result = _invoke(
            runner,
            project_root,
            "--env",
            "qa",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code != 0, result.output
        # Error output goes through cli_error_boundary; LHPFileError formats
        # with the LHP-IO-006 code from `check_substitution_file`.
        combined = result.output + (result.stderr if result.stderr_bytes else "")
        assert "qa.yaml" in combined or "qa" in combined
        assert "Substitution file not found" in combined or "LHP-IO" in combined


class TestSeedLoadGroupMalformedYaml:
    """Test 4: malformed flowgroup YAML → cli_error_boundary formatted
    error (non-zero exit, error printed)."""

    def test_malformed_yaml_handled_via_error_boundary(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        # Valid env file, but flowgroup YAML is broken.
        (project_root / "pipelines" / "edp_bronze").mkdir(parents=True)
        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "lhp.yaml").write_text(
            "name: tier2_seed_test\nversion: '1.0'\n"
        )
        (project_root / "substitutions" / "devtest.yaml").write_text(
            _DEFAULT_SUBSTITUTIONS
        )
        broken = project_root / "pipelines" / "edp_bronze" / "broken.yaml"
        broken.write_text(
            "pipeline: x\nflowgroup: y\nactions:\n  - name: load_x\n    type: load\n"
            "    source:\n      type: jdbc_watermark_v2\n      table: ['unbalanced'\n"
        )

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(broken),
        )
        assert result.exit_code != 0, result.output


class TestSeedLoadGroupCosmeticSeparatorWarning:
    """Test 5: pipeline or flowgroup contains '::' literal — warns but
    does not fatal-abort (B2 plan is the place that fatal-aborts)."""

    def test_warns_when_pipeline_contains_separator(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(
            project_root, flowgroup_yaml=_PIPELINE_WITH_SEPARATOR_FLOWGROUP
        )

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        # Cosmetic, not fatal.
        assert result.exit_code == 0, result.output
        # SQL is still emitted.
        assert "INSERT INTO " in result.output
        # Warning is surfaced.
        assert "::" in result.output  # appears in load_group + warning
        lower = result.output.lower()
        assert "warn" in lower or "cosmetic" in lower or "separator" in lower


class TestSeedLoadGroupSqlFidelity:
    """Test 7: SQL fidelity — emitted column list matches origin doc
    Step 4a verbatim; UUID-based run_id; extraction_type='seeded';
    ORDER BY tiebreakers; LIMIT 1."""

    def test_insert_column_list_matches_origin_doc(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code == 0, result.output

        # Origin doc Step 4a INSERT column list (pinned to verified DDL):
        expected_columns = (
            "(run_id, watermark_time, source_system_id, schema_name, table_name,\n"
            "     watermark_column_name, watermark_value, previous_watermark_value,\n"
            "     row_count, extraction_type,\n"
            "     bronze_stage_complete, silver_stage_complete, status,\n"
            "     created_at, completed_at, load_group)"
        )
        assert expected_columns in result.output, (
            "INSERT column list must match origin doc Step 4a verbatim."
            f"\n\nCommand output:\n{result.output}"
        )

        # UUID-based run_id for the seed insert.
        assert "concat('seed-', uuid())" in result.output, result.output

        # New extraction_type value; safe since column is STRING NOT NULL.
        assert "'seeded'" in result.output, result.output

        # Tiebreaker ordering and LIMIT 1.
        assert (
            "ORDER BY watermark_time DESC, completed_at DESC, run_id DESC"
            in result.output
        ), result.output
        assert "LIMIT 1" in result.output, result.output

        # Source filter matches origin doc Step 4a precondition: 'legacy' rows
        # with status = 'completed' for the target (source, schema, table).
        assert "WHERE load_group = 'legacy'" in result.output, result.output
        assert "status = 'completed'" in result.output, result.output

        # Both INSERTs must reference both schema/table values from the YAML.
        assert "schema_name = 'HumanResources'" in result.output, result.output
        assert "table_name = 'Department'" in result.output, result.output
        assert "table_name = 'Employee'" in result.output, result.output

    def test_select_preview_matches_origin_doc(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
        )
        assert result.exit_code == 0, result.output

        # SELECT preview matches origin doc:
        # SELECT count(*), max(watermark_time), max(watermark_value)
        # FROM   metadata.<env>_orchestration.watermarks
        # WHERE  load_group = 'legacy' AND ...
        assert (
            "SELECT count(*), max(watermark_time), max(watermark_value)"
            in result.output
        ), result.output


class TestSeedLoadGroupCatalogSchemaOverride:
    """Test 6 (extra coverage): --catalog / --schema overrides bypass
    the substitution lookup. Useful when running against a non-standard
    namespace (e.g. a deep-clone probe table)."""

    def test_catalog_schema_overrides_apply(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        project_root = tmp_path / "project"
        project_root.mkdir()
        fg_path = _write_project(project_root, flowgroup_yaml=_TWO_ACTION_FLOWGROUP)

        result = _invoke(
            runner,
            project_root,
            "--env",
            "devtest",
            "--flowgroup",
            str(fg_path),
            "--catalog",
            "metadata",
            "--schema",
            "qa_orchestration",
        )
        assert result.exit_code == 0, result.output
        assert "metadata.qa_orchestration.watermarks" in result.output, result.output
        assert "metadata.devtest_orchestration.watermarks" not in result.output, (
            result.output
        )
