"""Generator for DAB Workflow resource YAML.

Produces a Databricks Asset Bundle workflow that chains extraction Job
tasks (one per jdbc_watermark_v2 action) followed by a DLT pipeline task.
"""

import logging
from typing import Any, Dict, List

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action, FlowGroup, LoadSourceType

logger = logging.getLogger(__name__)


class WorkflowResourceGenerator(BaseActionGenerator):
    """Generate DAB Workflow YAML for jdbc_watermark_v2 pipelines."""

    def generate(self, flowgroup: FlowGroup, context: Dict[str, Any]) -> str:
        """Generate Workflow resource YAML.

        Args:
            flowgroup: FlowGroup containing jdbc_watermark_v2 load actions.
            context: Generation context dict.

        Returns:
            Rendered YAML string for the DAB Workflow resource.
        """
        pipeline_name = flowgroup.pipeline

        # Collect v2 extraction tasks
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
