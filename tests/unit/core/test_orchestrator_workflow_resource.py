"""Tests for orchestrator workflow resource generation hook."""

import yaml
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import Action, FlowGroup, WriteTarget


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
