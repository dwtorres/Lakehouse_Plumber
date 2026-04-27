"""Unit tests for WorkflowResourceGenerator B2 for_each branch (U7).

Scenarios:
- Legacy byte-identical: no execution_mode → output unchanged.
- B2 three-task topology: for_each flowgroup renders prepare_manifest,
  for_each_ingest, validate tasks.
- max_retries override: workflow.max_retries respected.
- Concurrency default: action_count ≤ 10 → concurrency = action_count.
- Concurrency cap: action_count > 10 → concurrency capped at 10.
- Mixed-mode error: orchestrator guard raises LHPConfigError for pipeline
  mixing for_each and default flowgroups.
- raw-escape preservation: literal {{ }} reference preserved in rendered YAML.
- Notebook paths: all three paths substituted correctly in B2 output.
"""

import pytest
import yaml
from pathlib import Path
from unittest.mock import MagicMock

from lhp.generators.bundle.workflow_resource import WorkflowResourceGenerator
from lhp.models.config import Action, FlowGroup, WriteTarget
from lhp.utils.error_formatter import LHPConfigError


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _v2_action(name: str, target: str = "v_raw") -> Action:
    return Action(
        name=name,
        type="load",
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "u",
            "password": "p",
            "driver": "org.postgresql.Driver",
            "table": '"Schema"."Table"',
            "schema_name": "Schema",
            "table_name": "Table",
        },
        target=target,
        landing_path="/Volumes/cat/sch/land/tbl",
        watermark={"column": "updated_at", "type": "timestamp", "source_system_id": "sys"},
    )


def _write_action() -> Action:
    return Action(
        name="write_bronze",
        type="write",
        source="v_raw",
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze",
            schema="schema",
            table="tbl",
        ),
    )


def _legacy_flowgroup(actions=None, pipeline="bronze_pipeline"):
    """FlowGroup with no execution_mode — legacy path."""
    actions = actions or [_v2_action("load_orders"), _write_action()]
    return FlowGroup(pipeline=pipeline, flowgroup="orders_fg", actions=actions)


def _for_each_flowgroup(
    actions=None,
    pipeline="bronze_pipeline",
    flowgroup="orders_fg",
    concurrency=None,
    max_retries=None,
):
    """FlowGroup with execution_mode: for_each."""
    actions = actions or [_v2_action("load_orders"), _write_action()]
    workflow: dict = {"execution_mode": "for_each"}
    if concurrency is not None:
        workflow["concurrency"] = concurrency
    if max_retries is not None:
        workflow["max_retries"] = max_retries
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        workflow=workflow,
        actions=actions,
    )


# ---------------------------------------------------------------------------
# Legacy byte-identical
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestLegacyByteIdentical:
    """Legacy flowgroup must produce output unchanged from pre-B2 baseline."""

    def test_legacy_output_structure(self):
        """No execution_mode → legacy N-task DLT topology, parseable YAML."""
        fg = _legacy_flowgroup()
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        job = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]
        tasks = job["tasks"]
        # 1 extraction + 1 DLT = 2 tasks
        assert len(tasks) == 2
        task_keys = [t["task_key"] for t in tasks]
        assert "extract_load_orders" in task_keys
        assert "dlt_bronze_pipeline" in task_keys

    def test_legacy_output_no_for_each_task(self):
        """Legacy output must not contain for_each_task key."""
        fg = _legacy_flowgroup()
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "for_each_task" not in result

    def test_legacy_output_has_dlt_pipeline_ref(self):
        """Legacy output contains pipeline_id reference."""
        fg = _legacy_flowgroup()
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "${resources.pipelines." in result

    def test_legacy_output_is_parseable_yaml(self):
        fg = _legacy_flowgroup()
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        assert parsed is not None

    def test_serial_extraction_chains_unchanged(self):
        """Serial extraction_mode still chains tasks in legacy path."""
        action1 = _v2_action("load_a")
        action2 = _v2_action("load_b")
        fg = FlowGroup(
            pipeline="bronze_pipeline",
            flowgroup="fg",
            workflow={"extraction_mode": "serial"},
            actions=[action1, action2, _write_action()],
        )
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        second = tasks[1]
        assert second["depends_on"] == [{"task_key": "extract_load_a"}]


# ---------------------------------------------------------------------------
# B2 three-task topology
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestForEachThreeTaskTopology:
    """for_each flowgroup emits prepare_manifest → for_each_ingest → validate."""

    def test_three_tasks_present(self):
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        job = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]
        task_keys = [t["task_key"] for t in job["tasks"]]
        assert task_keys == ["prepare_manifest", "for_each_ingest", "validate"]

    def test_for_each_ingest_depends_on_prepare_manifest(self):
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["depends_on"] == [{"task_key": "prepare_manifest"}]

    def test_validate_depends_on_for_each_ingest(self):
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        validate = next(t for t in tasks if t["task_key"] == "validate")
        assert validate["depends_on"] == [{"task_key": "for_each_ingest"}]

    def test_concurrency_respected(self):
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["for_each_task"]["concurrency"] == 5

    def test_default_max_retries_is_1(self):
        fg = _for_each_flowgroup(concurrency=3)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["for_each_task"]["task"]["max_retries"] == 1

    def test_no_dlt_pipeline_task(self):
        """B2 topology must not include a DLT pipeline task."""
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "pipeline_task" not in result


# ---------------------------------------------------------------------------
# max_retries override
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestMaxRetriesOverride:
    def test_max_retries_3(self):
        fg = _for_each_flowgroup(concurrency=4, max_retries=3)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["for_each_task"]["task"]["max_retries"] == 3


# ---------------------------------------------------------------------------
# Concurrency default and cap
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestConcurrencyDefaults:
    def test_concurrency_default_equals_action_count(self):
        """5 actions, no concurrency declared → concurrency = 5."""
        actions = [_v2_action(f"load_{i}") for i in range(5)] + [_write_action()]
        fg = _for_each_flowgroup(actions=actions)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["for_each_task"]["concurrency"] == 5

    def test_concurrency_capped_at_10_for_large_action_count(self):
        """50 actions, no concurrency declared → concurrency capped at 10."""
        actions = [_v2_action(f"load_{i}") for i in range(50)] + [_write_action()]
        fg = _for_each_flowgroup(actions=actions)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        assert ingest["for_each_task"]["concurrency"] == 10


# ---------------------------------------------------------------------------
# Mixed-mode orchestrator guard
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestMixedModeOrchestratorGuard:
    """_generate_workflow_resources raises LHPConfigError for mixed-mode pipelines."""

    def _build_orchestrator(self, tmp_path: Path):
        from lhp.core.orchestrator import ActionOrchestrator

        project_root = tmp_path / "proj"
        project_root.mkdir()
        (project_root / "lhp.yaml").write_text("name: test\nversion: '1.0'\n")
        return ActionOrchestrator(project_root)

    def test_mixed_mode_raises_lhp_config_error(self, tmp_path):
        orch = self._build_orchestrator(tmp_path)

        for_each_fg = _for_each_flowgroup(
            pipeline="shared_pipeline", flowgroup="fg_for_each"
        )
        legacy_fg = FlowGroup(
            pipeline="shared_pipeline",
            flowgroup="fg_legacy",
            actions=[_v2_action("load_legacy"), _write_action()],
        )

        smart_writer = MagicMock()
        smart_writer.write_if_changed = MagicMock()

        with pytest.raises(LHPConfigError) as exc_info:
            orch._generate_workflow_resources(
                [for_each_fg, legacy_fg], smart_writer
            )

        err = exc_info.value
        assert "034" in str(err)
        assert "shared_pipeline" in str(err)
        # Both modes must be mentioned
        assert "for_each" in str(err)
        assert "<default>" in str(err)

    def test_single_mode_pipeline_does_not_raise(self, tmp_path):
        """Single-mode pipelines (not mixed) must not raise CFG-034 OR CFG-036.

        Post-U4 (#16): two for_each flowgroups in the SAME pipeline are
        rejected by LHP-CFG-036. To test the original CFG-034 single-mode
        invariant (no mixed-mode error), use distinct pipeline names so
        each pipeline contains exactly one for_each flowgroup — this is
        the legitimate single-mode-per-pipeline topology.
        """
        orch = self._build_orchestrator(tmp_path)

        fg1 = _for_each_flowgroup(
            pipeline="for_each_pipeline_a",
            flowgroup="fg1",
            concurrency=3,
        )
        fg2 = _for_each_flowgroup(
            pipeline="for_each_pipeline_b",
            flowgroup="fg2",
            concurrency=3,
        )

        smart_writer = MagicMock()
        smart_writer.write_if_changed = MagicMock()
        (orch.project_root / "resources" / "lhp").mkdir(parents=True, exist_ok=True)

        # Should not raise — each pipeline has exactly one for_each flowgroup,
        # all with consistent for_each mode (no CFG-034 mixed-mode trigger; no
        # CFG-036 multi-for_each-per-pipeline trigger).
        orch._generate_workflow_resources([fg1, fg2], smart_writer)


# ---------------------------------------------------------------------------
# {% raw %} escape preservation
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestRawEscapePreservation:
    def test_iterations_taskvalue_ref_preserved_literal(self):
        """Rendered YAML must contain the literal DAB taskValue reference."""
        fg = _for_each_flowgroup(concurrency=3)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        # Quoted form: "{{tasks.prepare_manifest.values.iterations}}" —
        # DAB resolves this at bundle deploy time, YAML sees it as a string.
        assert "{{tasks.prepare_manifest.values.iterations}}" in result

    def test_iterations_ref_not_interpreted_as_jinja(self):
        """The iterations ref must not be evaluated/replaced during rendering."""
        fg = _for_each_flowgroup(concurrency=3)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        # Confirm it's the literal string, not an empty or substituted value.
        assert "tasks.prepare_manifest.values.iterations" in result


# ---------------------------------------------------------------------------
# Notebook paths
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestNotebookPaths:
    def test_prepare_manifest_path_correct(self):
        fg = _for_each_flowgroup(
            pipeline="bronze",
            flowgroup="orders_fg",
            concurrency=3,
        )
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        expected = (
            "${workspace.file_path}/generated/${bundle.target}"
            "/bronze_extract/__lhp_prepare_manifest_orders_fg"
        )
        assert expected in result

    def test_validate_path_correct(self):
        fg = _for_each_flowgroup(
            pipeline="bronze",
            flowgroup="orders_fg",
            concurrency=3,
        )
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        expected = (
            "${workspace.file_path}/generated/${bundle.target}"
            "/bronze_extract/__lhp_validate_orders_fg"
        )
        assert expected in result

    def test_worker_path_is_per_flowgroup_not_per_action(self):
        """B2 worker path uses flowgroup name (single notebook), not action name."""
        fg = _for_each_flowgroup(
            pipeline="bronze",
            flowgroup="orders_fg",
            concurrency=3,
        )
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        expected_worker = (
            "${workspace.file_path}/generated/${bundle.target}"
            "/bronze_extract/__lhp_extract_orders_fg"
        )
        assert expected_worker in result
        # Must NOT reference per-action path.
        assert "__lhp_extract_load_orders" not in result

    def test_paths_have_no_py_suffix(self):
        """ADR-002: notebook paths must not include .py extension."""
        fg = _for_each_flowgroup(pipeline="bronze", flowgroup="orders_fg", concurrency=3)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        assert "__lhp_prepare_manifest_orders_fg.py" not in result
        assert "__lhp_validate_orders_fg.py" not in result
        assert "__lhp_extract_orders_fg.py" not in result


# ---------------------------------------------------------------------------
# U3: prepare_manifest task max_retries: 0
# ---------------------------------------------------------------------------

@pytest.mark.unit
class TestPrepareManifestMaxRetriesZero:
    """U3 — prepare_manifest task must declare max_retries: 0.

    LHP-MAN-005 (payload ceiling breach) is deterministic: same payload →
    same overflow → same failure.  DAB retrying the prepare_manifest task
    burns retry budget without progress.  max_retries: 0 prevents this.

    The worker (for_each_ingest / ingest_one subtask) and validate tasks
    keep their existing retry policies — only prepare_manifest is suppressed.
    """

    def test_prepare_manifest_task_has_max_retries_zero(self):
        """for_each workflow: prepare_manifest task must declare max_retries: 0.

        AE: U3 LHP-MAN-005 retry suppression — deterministic failures must not
        consume DAB retry budget.
        """
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        prepare = next(
            (t for t in tasks if t["task_key"] == "prepare_manifest"), None
        )
        assert prepare is not None, "prepare_manifest task not found in rendered YAML"
        assert prepare.get("max_retries") == 0, (
            f"prepare_manifest task must declare max_retries: 0 (LHP-MAN-005 is "
            f"deterministic — retries waste budget); "
            f"got max_retries={prepare.get('max_retries')!r}"
        )

    def test_worker_task_retries_not_affected(self):
        """Worker ingest_one subtask (for_each_task.task) retains its own max_retries.

        Only prepare_manifest is suppressed. The worker's max_retries is independent
        (controlled by workflow.max_retries in the flowgroup config).
        """
        fg = _for_each_flowgroup(concurrency=5, max_retries=2)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        ingest = next(t for t in tasks if t["task_key"] == "for_each_ingest")
        worker_max_retries = ingest["for_each_task"]["task"]["max_retries"]
        assert worker_max_retries == 2, (
            f"Worker ingest_one max_retries must be 2 (from flowgroup config); "
            f"got {worker_max_retries!r}"
        )

    def test_validate_task_has_no_max_retries_override(self):
        """validate task has no max_retries override — inherits DAB default.

        Only prepare_manifest is suppressed to 0. validate task is not changed.
        """
        fg = _for_each_flowgroup(concurrency=5)
        gen = WorkflowResourceGenerator()
        result = gen.generate(fg, {})
        parsed = yaml.safe_load(result)
        tasks = parsed["resources"]["jobs"]["bronze_pipeline_workflow"]["tasks"]
        validate = next(t for t in tasks if t["task_key"] == "validate")
        # validate task must NOT have max_retries: 0 set at the top-level task scope
        assert validate.get("max_retries") is None, (
            f"validate task must not have max_retries overridden; "
            f"only prepare_manifest is suppressed. Got: {validate.get('max_retries')!r}"
        )
