"""Tests for orchestrator workflow resource generation hook."""

import yaml
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import Action, FlowGroup, WriteTarget
from lhp.utils.error_formatter import LHPConfigError


def _make_v2_flowgroup(pipeline_name="test_pipeline"):
    """Build a FlowGroup with a jdbc_watermark_v2 action + streaming_table write."""
    load_action = Action(
        name="load_product_jdbc",
        type="load",
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
            "table_name": "Product",
        },
        target="v_product_raw",
        landing_path="/Volumes/catalog/schema/landing/product",
        watermark={
            "column": "ModifiedDate",
            "type": "timestamp",
            "source_system_id": "pg_prod",
        },
    )
    write_action = Action(
        name="write_product_bronze",
        type="write",
        source="v_product_raw",
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze_catalog",
            schema="bronze_schema",
            table="product",
            table_properties={"delta.enableChangeDataFeed": "true"},
        ),
    )
    return FlowGroup(
        pipeline=pipeline_name,
        flowgroup="test_flowgroup",
        actions=[load_action, write_action],
    )


def _make_cloudfiles_flowgroup(pipeline_name="cf_pipeline"):
    """Build a non-v2 FlowGroup (cloudfiles source)."""
    load_action = Action(
        name="load_data",
        type="load",
        source={"type": "cloudfiles", "path": "/data/input", "format": "json"},
        target="v_data_raw",
    )
    return FlowGroup(
        pipeline=pipeline_name,
        flowgroup="cf_flowgroup",
        actions=[load_action],
    )


@pytest.mark.unit
class TestGenerateWorkflowResources:
    """Tests for _generate_workflow_resources hook."""

    def test_v2_flowgroup_generates_workflow_yaml(self, tmp_path):
        """jdbc_watermark_v2 flowgroup should produce a workflow YAML file."""
        smart_writer = MagicMock()
        smart_writer.write_if_changed = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg = _make_v2_flowgroup()
        # Call the real method
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [fg], smart_writer
        )

        # Should have written one workflow file
        assert smart_writer.write_if_changed.call_count == 1
        call_args = smart_writer.write_if_changed.call_args
        written_path = call_args[0][0]
        written_content = call_args[0][1]

        assert str(written_path).endswith("test_pipeline_workflow.yml")
        assert "resources/lhp" in str(written_path)

        # Content should be valid YAML
        parsed = yaml.safe_load(written_content)
        assert "resources" in parsed
        assert "jobs" in parsed["resources"]

    def test_non_v2_flowgroup_does_not_generate_workflow(self, tmp_path):
        """cloudfiles-only flowgroup should NOT produce a workflow YAML."""
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg = _make_cloudfiles_flowgroup()
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [fg], smart_writer
        )

        smart_writer.write_if_changed.assert_not_called()

    def test_mixed_flowgroups_only_generates_for_v2(self, tmp_path):
        """Only v2 flowgroups get workflow YAML, others are skipped."""
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        v2_fg = _make_v2_flowgroup(pipeline_name="v2_pipe")
        cf_fg = _make_cloudfiles_flowgroup(pipeline_name="cf_pipe")
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [v2_fg, cf_fg], smart_writer
        )

        assert smart_writer.write_if_changed.call_count == 1
        written_path = smart_writer.write_if_changed.call_args[0][0]
        assert "v2_pipe_workflow.yml" in str(written_path)

    def test_workflow_yaml_has_correct_depends_on(self, tmp_path):
        """Generated workflow YAML should have pipeline task depending on extraction."""
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg = _make_v2_flowgroup()
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [fg], smart_writer
        )

        written_content = smart_writer.write_if_changed.call_args[0][1]
        parsed = yaml.safe_load(written_content)
        tasks = parsed["resources"]["jobs"]["test_pipeline_workflow"]["tasks"]

        # Last task should be the DLT pipeline task with depends_on
        pipeline_task = tasks[-1]
        assert "pipeline_task" in pipeline_task
        assert "depends_on" in pipeline_task
        dep_keys = [d["task_key"] for d in pipeline_task["depends_on"]]
        assert "extract_load_product_jdbc" in dep_keys

    def test_workflow_notebook_path_uses_extract_sibling_dir(self, tmp_path):
        """Regression: extraction notebook paths must use sibling _extract/ dir.

        Extraction notebooks are written to <pipeline>_extract/__lhp_extract_<name>.py
        (outside the DLT pipeline glob scope) to prevent DLT from loading them.
        The workflow notebook_path must reference this sibling directory, and the
        task_key must stay clean (no __lhp_ prefix) for readability in the DAB UI.
        """
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg = _make_v2_flowgroup()
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [fg], smart_writer
        )

        written_content = smart_writer.write_if_changed.call_args[0][1]
        parsed = yaml.safe_load(written_content)
        tasks = parsed["resources"]["jobs"]["test_pipeline_workflow"]["tasks"]

        # Extraction tasks are all tasks except the final pipeline_task
        extraction_tasks = [t for t in tasks if "notebook_task" in t]
        assert extraction_tasks, "Expected at least one notebook_task in workflow"

        for task in extraction_tasks:
            notebook_path = task["notebook_task"]["notebook_path"]
            task_key = task["task_key"]

            assert "_extract/__lhp_extract_" in notebook_path, (
                f"notebook_path must use sibling _extract/ dir with __lhp_extract_ prefix; "
                f"got: {notebook_path}"
            )
            assert not task_key.startswith("__lhp_"), (
                f"task_key must NOT contain __lhp_ prefix (keeps DAB UI clean); "
                f"got: {task_key}"
            )


def _make_for_each_v2_flowgroup(pipeline_name: str, flowgroup_name: str) -> FlowGroup:
    """Build a for_each FlowGroup with a jdbc_watermark_v2 load action."""
    load_action = Action(
        name=f"load_{flowgroup_name}",
        type="load",
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "u",
            "password": "p",
            "driver": "org.postgresql.Driver",
            "table": '"Sales"."Orders"',
            "schema_name": "Sales",
            "table_name": "Orders",
        },
        target=f"v_{flowgroup_name}_raw",
        landing_path=f"/Volumes/cat/land/{flowgroup_name}",
        watermark={
            "column": "updated_at",
            "type": "timestamp",
            "source_system_id": "pg_prod",
        },
    )
    write_action = Action(
        name=f"write_{flowgroup_name}",
        type="write",
        source=f"v_{flowgroup_name}_raw",
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze_catalog",
            schema="bronze_schema",
            table=flowgroup_name,
        ),
    )
    return FlowGroup(
        pipeline=pipeline_name,
        flowgroup=flowgroup_name,
        workflow={"execution_mode": "for_each"},
        actions=[load_action, write_action],
    )


@pytest.mark.unit
class TestMultiForEachGeneratePath:
    """Generate-path bypass guard for LHP-CFG-036 (U4).

    Verifies that _generate_workflow_resources raises LHP-CFG-036 when a pipeline
    contains 2+ for_each flowgroups, even when validate_project_invariants was never
    called (i.e., the operator ran `lhp generate` directly without `lhp validate`).

    This is the load-bearing guard: validate_project_invariants is NOT called from
    generate_pipeline_by_field (orchestrator.py:802).  Without the orchestrator-side
    raise, a broken topology would silently emit a workflow YAML referencing only the
    first flowgroup's aux files — the original #16 bug.
    """

    def test_two_for_each_same_pipeline_raises_cfg036(self, tmp_path):
        """2 for_each flowgroups in same pipeline → LHP-CFG-036 from orchestrator.

        AE: Generate-path bypass guard — CFG-036 must fire from _generate_workflow_resources
        even when validate_project_invariants was skipped.
        """
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg_a = _make_for_each_v2_flowgroup(pipeline_name="bronze", flowgroup_name="fg_a")
        fg_b = _make_for_each_v2_flowgroup(pipeline_name="bronze", flowgroup_name="fg_b")

        with pytest.raises(LHPConfigError) as exc_info:
            ActionOrchestrator._generate_workflow_resources(
                orchestrator, [fg_a, fg_b], smart_writer
            )

        err = exc_info.value
        assert err.code == "LHP-CFG-036", (
            f"Expected LHP-CFG-036 from orchestrator-side guard; got {err.code}"
        )
        assert "bronze" in err.details, "Error must name the pipeline"
        assert "fg_a" in err.details, "Error must list fg_a"
        assert "fg_b" in err.details, "Error must list fg_b"

        # Workflow YAML must NOT be emitted — broken topology should never reach disk.
        smart_writer.write_if_changed.assert_not_called()

    def test_one_for_each_per_pipeline_does_not_raise(self, tmp_path):
        """One for_each flowgroup per pipeline → no CFG-036 from orchestrator.

        AE: Regression guard — two for_each in different pipelines must still pass.
        """
        smart_writer = MagicMock()

        orchestrator = MagicMock(spec=ActionOrchestrator)
        orchestrator.project_root = tmp_path
        orchestrator.logger = MagicMock()

        fg_a = _make_for_each_v2_flowgroup(pipeline_name="pipeline_a", flowgroup_name="fg_alpha")
        fg_b = _make_for_each_v2_flowgroup(pipeline_name="pipeline_b", flowgroup_name="fg_beta")

        # Should not raise; each pipeline has exactly one for_each flowgroup.
        ActionOrchestrator._generate_workflow_resources(
            orchestrator, [fg_a, fg_b], smart_writer
        )

        # Two pipelines → two workflow YAMLs should be written.
        assert smart_writer.write_if_changed.call_count == 2
