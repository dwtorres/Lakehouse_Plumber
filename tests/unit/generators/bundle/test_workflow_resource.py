"""Unit tests for WorkflowResourceGenerator."""

import yaml

import pytest
from lhp.generators.bundle.workflow_resource import WorkflowResourceGenerator
from lhp.models.config import Action, FlowGroup, WriteTarget


def _make_v2_action(name="load_product_jdbc", target="v_product_raw"):
    return Action(
        name=name,
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
        target=target,
        landing_path="/Volumes/catalog/schema/landing/product",
        watermark={
            "column": "ModifiedDate",
            "type": "timestamp",
            "source_system_id": "pg_prod",
        },
    )


def _make_write_action():
    return Action(
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


def _make_flowgroup(actions, pipeline_name="my_pipeline"):
    return FlowGroup(
        pipeline=pipeline_name,
        flowgroup="test_flowgroup",
        actions=actions,
    )


@pytest.mark.unit
class TestWorkflowResourceGenerator:
    """Tests for WorkflowResourceGenerator."""

    def test_generate_returns_nonempty_string(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert isinstance(result, str)
        assert len(result) > 0

    def test_generated_yaml_is_parseable(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        assert parsed is not None

    def test_single_action_has_correct_structure(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        job = parsed["resources"]["jobs"]["my_pipeline_workflow"]
        assert job["name"] == "my_pipeline_workflow"
        tasks = job["tasks"]
        # Should have 2 tasks: one extraction notebook + one DLT pipeline
        assert len(tasks) == 2

    def test_notebook_task_has_correct_fields(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["my_pipeline_workflow"]["tasks"]
        notebook_task = tasks[0]
        assert "notebook_task" in notebook_task
        assert "task_key" in notebook_task

    def test_pipeline_task_depends_on_extraction(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["my_pipeline_workflow"]["tasks"]
        pipeline_task = tasks[-1]
        assert "pipeline_task" in pipeline_task
        assert "depends_on" in pipeline_task
        dep_keys = [d["task_key"] for d in pipeline_task["depends_on"]]
        assert len(dep_keys) == 1

    def test_pipeline_id_references_resources(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "${resources.pipelines." in result

    def test_libraries_contains_whl(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "whl" in result

    def test_two_actions_produces_two_notebook_tasks(self):
        action1 = _make_v2_action(name="load_product", target="v_product_raw")
        action2 = _make_v2_action(name="load_order", target="v_order_raw")
        fg = _make_flowgroup([action1, action2, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["my_pipeline_workflow"]["tasks"]
        # 2 extraction tasks + 1 DLT task
        assert len(tasks) == 3
        pipeline_task = tasks[-1]
        dep_keys = [d["task_key"] for d in pipeline_task["depends_on"]]
        assert len(dep_keys) == 2

    def test_jdbc_driver_jar_comment_present(self):
        action = _make_v2_action()
        fg = _make_flowgroup([action, _make_write_action()])
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "JDBC driver" in result or "jdbc driver" in result.lower()
