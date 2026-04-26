"""Generator for DAB Workflow resource YAML.

Produces a Databricks Asset Bundle workflow that chains extraction Job
tasks (one per jdbc_watermark_v2 action) followed by a DLT pipeline task.

B2 mode (execution_mode: for_each) emits a three-task DAB topology:
prepare_manifest → for_each_ingest → validate.
"""

import logging
from typing import Any, Dict, List, Optional

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action, FlowGroup, LoadSourceType

logger = logging.getLogger(__name__)


class WorkflowResourceGenerator(BaseActionGenerator):
    """Generate DAB Workflow YAML for jdbc_watermark_v2 pipelines."""

    def generate(self, flowgroup: FlowGroup, context: Dict[str, Any]) -> str:
        """Generate Workflow resource YAML.

        Branches on execution_mode: for_each → B2 three-task DAB topology;
        all other modes → legacy per-action static topology (byte-identical).

        Args:
            flowgroup: FlowGroup containing jdbc_watermark_v2 load actions.
            context: Generation context dict.

        Returns:
            Rendered YAML string for the DAB Workflow resource.
        """
        execution_mode: Optional[str] = (
            flowgroup.workflow.get("execution_mode")
            if isinstance(flowgroup.workflow, dict)
            else None
        )
        if execution_mode == "for_each":
            return self._generate_for_each(flowgroup, context)

        # Legacy static per-action topology — unchanged.
        return self._generate_legacy(flowgroup, context)

    def _generate_for_each(
        self, flowgroup: FlowGroup, context: Dict[str, Any]
    ) -> str:
        """Emit B2 three-task DAB topology for for_each flowgroups.

        Tasks: prepare_manifest → for_each_ingest (DAB for_each_task) → validate.
        The per-iteration worker is a single per-flowgroup notebook; U8 owns
        emitting the __lhp_extract_<flowgroup>.py aux file for B2 mode.

        Args:
            flowgroup: FlowGroup with execution_mode == "for_each".
            context: Generation context dict.

        Returns:
            Rendered YAML string with B2 three-task topology.
        """
        assert isinstance(flowgroup.workflow, dict)
        pipeline_name = flowgroup.pipeline
        flowgroup_name = flowgroup.flowgroup

        # Count v2 actions for concurrency default.
        action_count = sum(
            1
            for a in flowgroup.actions
            if isinstance(a.source, dict)
            and a.source.get("type") == LoadSourceType.JDBC_WATERMARK_V2.value
        )

        base_path = (
            f"${{workspace.file_path}}/generated/${{bundle.target}}"
            f"/{pipeline_name}_extract"
        )
        prepare_manifest_path = f"{base_path}/__lhp_prepare_manifest_{flowgroup_name}"
        validate_path = f"{base_path}/__lhp_validate_{flowgroup_name}"
        # Single per-flowgroup worker notebook for B2 mode.
        # U8 must emit __lhp_extract_<flowgroup>.py (one file, not one per action).
        worker_path = f"{base_path}/__lhp_extract_{flowgroup_name}"

        concurrency: int = flowgroup.workflow.get("concurrency") or min(
            action_count if action_count > 0 else 1, 10
        )
        max_retries: int = flowgroup.workflow.get("max_retries") or 1

        template_context: Dict[str, Any] = {
            "execution_mode": "for_each",
            "pipeline_name": pipeline_name,
            "flowgroup_name": flowgroup_name,
            "prepare_manifest_path": prepare_manifest_path,
            "validate_path": validate_path,
            "worker_path": worker_path,
            "concurrency": concurrency,
            "max_retries": max_retries,
        }
        return self.render_template("bundle/workflow_resource.yml.j2", template_context)

    def _generate_legacy(
        self, flowgroup: FlowGroup, context: Dict[str, Any]
    ) -> str:
        """Emit legacy per-action static topology (unchanged from pre-B2)."""
        pipeline_name = flowgroup.pipeline

        extraction_tasks: List[Dict[str, str]] = []
        serial_extraction = bool(
            isinstance(flowgroup.workflow, dict)
            and flowgroup.workflow.get("extraction_mode") == "serial"
        )
        previous_task_name = None
        for action in flowgroup.actions:
            if not isinstance(action.source, dict):
                continue
            source_type = action.source.get("type", "")
            if source_type == LoadSourceType.JDBC_WATERMARK_V2.value:
                # task_key uses extract_<name> for readability in DAB UI/logs.
                # notebook_path points to sibling _extract/ dir (outside pipeline
                # glob scope) using __lhp_extract_ prefix from jdbc_watermark_job.py.
                task_name = f"extract_{action.name}"
                aux_filename = f"__lhp_extract_{action.name}"
                notebook_path = (
                    f"${{workspace.file_path}}/generated/${{bundle.target}}"
                    f"/{pipeline_name}_extract/{aux_filename}"
                )
                extraction_tasks.append(
                    {
                        "task_name": task_name,
                        "notebook_path": notebook_path,
                        "depends_on": (
                            [previous_task_name]
                            if previous_task_name and serial_extraction
                            else []
                        ),
                    }
                )
                previous_task_name = task_name

        template_context: Dict[str, Any] = {
            "execution_mode": None,
            "pipeline_name": pipeline_name,
            "extraction_tasks": extraction_tasks,
            "dlt_depends_on": (
                [extraction_tasks[-1]["task_name"]]
                if serial_extraction and extraction_tasks
                else [t["task_name"] for t in extraction_tasks]
            ),
            "dlt_task_name": f"dlt_{pipeline_name}",
            "dlt_pipeline_ref": (
                f"${{resources.pipelines.{pipeline_name}_pipeline.id}}"
            ),
            "serial_extraction": serial_extraction,
        }

        return self.render_template("bundle/workflow_resource.yml.j2", template_context)
