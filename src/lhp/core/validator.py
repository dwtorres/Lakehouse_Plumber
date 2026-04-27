"""Configuration validator for LakehousePlumber."""

from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from ..models.config import Action, ActionType, BATCH_ONLY_SOURCE_TYPES, FlowGroup, LoadSourceType, ProjectConfig, WriteTargetType
from ..utils.error_formatter import ErrorCategory, LHPConfigError, LHPError
from .action_registry import ActionRegistry
from .config_field_validator import ConfigFieldValidator
from .dependency_resolver import DependencyResolver
from .validators.base_validator import ValidationError

logger = logging.getLogger(__name__)

from .validators import (
    CdcFanInCompatibilityValidator,
    LoadActionValidator,
    TableCreationValidator,
    TestActionValidator,
    TransformActionValidator,
    WriteActionValidator,
)

if TYPE_CHECKING:
    from ..models.config import WriteTarget


class ConfigValidator:
    """Validate LakehousePlumber configurations."""

    def __init__(
        self,
        project_root: Optional[Path] = None,
        project_config: Optional[ProjectConfig] = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.project_root = project_root
        self.project_config = project_config
        self.action_registry = ActionRegistry()
        self.dependency_resolver = DependencyResolver()
        self.field_validator = ConfigFieldValidator()

        # Initialize action validators
        self.load_validator = LoadActionValidator(
            self.action_registry, self.field_validator
        )
        self.transform_validator = TransformActionValidator(
            self.action_registry,
            self.field_validator,
            self.project_root,
            self.project_config,
        )
        self.write_validator = WriteActionValidator(
            self.action_registry, self.field_validator, self.logger
        )
        self.test_validator = TestActionValidator(
            self.action_registry, self.field_validator
        )
        self.table_creation_validator = TableCreationValidator()
        self.cdc_fanin_validator = CdcFanInCompatibilityValidator()

    def validate_flowgroup(self, flowgroup: FlowGroup) -> List[ValidationError]:
        """Validate flowgroups and actions.

        Args:
            flowgroup: FlowGroup to validate

        Returns:
            List of validation errors (strings or LHPError objects)
        """
        errors = []

        # Validate basic fields
        if not flowgroup.pipeline:
            errors.append("FlowGroup must have a 'pipeline' name")

        if not flowgroup.flowgroup:
            errors.append("FlowGroup must have a 'flowgroup' name")

        if not flowgroup.actions:
            errors.append("FlowGroup must have at least one action")

        # Validate each action
        action_names = set()
        target_names = set()

        for i, action in enumerate(flowgroup.actions):
            action_errors = self.validate_action(action, i)
            errors.extend(action_errors)

            # Check for duplicate action names
            if action.name in action_names:
                errors.append(f"Duplicate action name: '{action.name}'")
            action_names.add(action.name)

            # Check for duplicate target names
            if action.target and action.target in target_names:
                errors.append(
                    f"Duplicate target name: '{action.target}' in action '{action.name}'"
                )
            if action.target:
                target_names.add(action.target)

        # Validate dependencies
        if flowgroup.actions:
            try:
                dependency_errors = self.dependency_resolver.validate_relationships(
                    flowgroup.actions
                )
                errors.extend(dependency_errors)
            except LHPError as e:
                logger.debug(f"Dependency validation error: {e.title}")
                errors.append(e)
            except Exception as e:
                logger.debug(f"Dependency validation error: {e}")
                errors.append(str(e))

        # Cross-action readMode compatibility check
        self._validate_readmode_compatibility(flowgroup.actions)

        # for_each execution-mode invariants (single-flowgroup scope)
        self._validate_for_each_invariants(flowgroup)

        # Validate template usage
        if flowgroup.use_template and not flowgroup.template_parameters:
            self.logger.warning(
                f"FlowGroup uses template '{flowgroup.use_template}' but no parameters provided"
            )

        return errors

    def validate_action(self, action: Action, index: int) -> List[ValidationError]:
        """Validate action types and required fields.

        Args:
            action: Action to validate
            index: Action index in the flowgroup

        Returns:
            List of validation errors (strings or LHPError objects)
        """
        errors = []
        prefix = f"Action[{index}] '{action.name}'"

        # Basic validation
        if not action.name:
            errors.append(f"Action[{index}]: Missing 'name' field")
            return errors  # Can't continue without name

        if not action.type:
            errors.append(f"{prefix}: Missing 'type' field")
            return errors  # Can't continue without type

        # Strict field validation - validate action-level fields
        try:
            action_dict = action.model_dump()
            self.field_validator.validate_action_fields(action_dict, action.name)
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            errors.append(str(e))
            return errors

        # Type-specific validation using action validators
        if action.type == ActionType.LOAD:
            errors.extend(self.load_validator.validate(action, prefix))

        elif action.type == ActionType.TRANSFORM:
            errors.extend(self.transform_validator.validate(action, prefix))

        elif action.type == ActionType.WRITE:
            errors.extend(self.write_validator.validate(action, prefix))

        elif action.type == ActionType.TEST:
            errors.extend(self.test_validator.validate(action, prefix))

        else:
            errors.append(f"{prefix}: Unknown action type '{action.type}'")

        return errors

    def _validate_readmode_compatibility(self, actions: List[Action]) -> None:
        """Warn when a streaming_table write reads from a batch-only JDBC source."""
        # Build set of view names produced by batch-only load actions
        batch_only_views: set = set()
        for action in actions:
            if action.type != ActionType.LOAD:
                continue
            source_type_str = (
                action.source.get("type")
                if isinstance(action.source, dict)
                else None
            )
            if not source_type_str:
                continue
            try:
                if LoadSourceType(source_type_str) in BATCH_ONLY_SOURCE_TYPES:
                    if action.target:
                        batch_only_views.add(action.target)
            except ValueError:
                continue

        if not batch_only_views:
            return

        # Check write actions that consume batch-only views
        for action in actions:
            if action.type != ActionType.WRITE:
                continue
            if not action.write_target or action.write_target.get("type") != "streaming_table":
                continue

            # Check if source references a batch-only view
            sources = (
                [action.source]
                if isinstance(action.source, str)
                else action.source
                if isinstance(action.source, list)
                else []
            )
            for source in sources:
                if source in batch_only_views:
                    logger.warning(
                        f"Action '{action.name}': reads from '{source}' which is "
                        f"produced by a batch-only JDBC source. Ensure the pipeline "
                        f"configuration includes "
                        f"'pipelines.incompatibleViewCheck.enabled: false' "
                        f"to allow DLT to consume batch views in append flows."
                    )

    def validate_action_references(self, actions: List[Action]) -> List[str]:
        """Validate that all action references are valid."""
        errors = []

        # Build set of all available views/targets
        available_views = set()
        for action in actions:
            if action.target:
                available_views.add(action.target)

        # Check all references
        for action in actions:
            sources = self._extract_all_sources(action)
            for source in sources:
                # Skip external sources
                if not source.startswith("v_") and "." in source:
                    continue  # Likely an external table like bronze.customers

                if source.startswith("v_") and source not in available_views:
                    errors.append(
                        f"Action '{action.name}' references view '{source}' which is not defined"
                    )

        return errors

    def _extract_all_sources(self, action: Action) -> List[str]:
        """Extract all source references from an action."""
        sources = []

        if isinstance(action.source, str):
            sources.append(action.source)
        elif isinstance(action.source, list):
            sources.extend(action.source)
        elif isinstance(action.source, dict):
            # Check various fields that might contain source references
            for field in ["view", "source", "views", "sources"]:
                value = action.source.get(field)
                if isinstance(value, str):
                    sources.append(value)
                elif isinstance(value, list):
                    sources.extend(value)

        return sources

    def validate_table_creation_rules(self, flowgroups: List[FlowGroup]) -> List[str]:
        """Validate table creation rules across the entire pipeline.

        Delegates to TableCreationValidator for the actual validation logic.

        Args:
            flowgroups: List of all flowgroups in the pipeline

        Returns:
            List of validation error messages
        """
        return self.table_creation_validator.validate(flowgroups)

    def validate_cdc_fanin_compatibility(
        self, flowgroups: List[FlowGroup]
    ) -> List[str]:
        """Validate compatibility across CDC actions sharing a target.

        Delegates to CdcFanInCompatibilityValidator. Mismatches on shared
        fields surface as ``LHPConfigError`` (raised from the delegate);
        mode-mixing between CDC and non-CDC contributors returns plain error
        strings in the result list.

        Args:
            flowgroups: List of all flowgroups in the pipeline

        Returns:
            List of validation error messages
        """
        return self.cdc_fanin_validator.validate(flowgroups)

    def _validate_for_each_invariants(self, flowgroup: FlowGroup) -> None:
        """Raise LHPConfigError when a for_each flowgroup violates structural rules.

        Checks run only when ``workflow.execution_mode == "for_each"`` so legacy
        flowgroups that happen to use ``::`` in their names are not affected.

        Args:
            flowgroup: Fully-expanded (post-template, post-substitution) FlowGroup.

        Raises:
            LHPConfigError: LHP-CFG-031 (separator collision),
                            LHP-CFG-033 (post-expansion structure).
        """
        execution_mode = (
            (flowgroup.workflow or {}).get("execution_mode") if flowgroup.workflow else None
        )
        if execution_mode != "for_each":
            return

        # LHP-CFG-031: separator collision
        for field_name, field_value in (
            ("pipeline", flowgroup.pipeline),
            ("flowgroup", flowgroup.flowgroup),
        ):
            if "::" in (field_value or ""):
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="031",
                    title="Separator collision: '::' reserved for load_group composite",
                    details=(
                        f"The {field_name} '{field_value}' contains '::', which is "
                        "reserved as the separator in the composite load_group key "
                        "<pipeline>::<flowgroup> used by for_each B2 manifests. "
                        "Rename the field to remove '::'."
                    ),
                    context={
                        "field": field_name,
                        "value": field_value,
                        "execution_mode": "for_each",
                    },
                    suggestions=[
                        f"Rename {field_name} to remove '::' so the composite "
                        "load_group '<pipeline>::<flowgroup>' parses unambiguously.",
                    ],
                )

        # LHP-CFG-033: post-expansion structural checks
        actions = flowgroup.actions
        action_count = len(actions)

        # Action count bounds: 1–300 inclusive
        if action_count == 0:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="033",
                title="for_each flowgroup has no actions after expansion",
                details=(
                    "A for_each flowgroup must expand to at least one action. "
                    "The flowgroup is empty after template expansion and preset "
                    "application — check use_template / template_parameters."
                ),
                context={
                    "pipeline": flowgroup.pipeline,
                    "flowgroup": flowgroup.flowgroup,
                    "action_count": action_count,
                },
                suggestions=[
                    "Verify that use_template references a valid template name.",
                    "Ensure template_parameters produces at least one action.",
                ],
            )

        if action_count > 300:
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="033",
                title="for_each flowgroup exceeds 300-action limit",
                details=(
                    f"The flowgroup '{flowgroup.flowgroup}' in pipeline "
                    f"'{flowgroup.pipeline}' expanded to {action_count} actions, "
                    "which exceeds the maximum of 300 per for_each flowgroup."
                ),
                context={
                    "pipeline": flowgroup.pipeline,
                    "flowgroup": flowgroup.flowgroup,
                    "action_count": action_count,
                    "limit": 300,
                },
                suggestions=[
                    "Split by source schema prefix or table subset into multiple "
                    "flowgroups; batch-scoped manifest isolates them.",
                ],
            )

        # Concurrency bounds when explicitly set (1–100 inclusive)
        concurrency = (flowgroup.workflow or {}).get("concurrency")
        if concurrency is not None:
            if not isinstance(concurrency, int) or concurrency < 1 or concurrency > 100:
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="033",
                    title="for_each workflow.concurrency out of bounds",
                    details=(
                        f"workflow.concurrency must be an integer between 1 and 100 "
                        f"inclusive; got {concurrency!r}."
                    ),
                    context={
                        "pipeline": flowgroup.pipeline,
                        "flowgroup": flowgroup.flowgroup,
                        "concurrency": concurrency,
                    },
                    suggestions=[
                        "Set workflow.concurrency to a value in [1, 100], or omit it "
                        "to use the default min(action_count, 10).",
                    ],
                )

        # LHP-CFG-035: strict comparison required for for_each watermark actions.
        # Re-run boundary rows occur when operator is non-strict (>= or <=).
        # WatermarkConfig.operator only permits ">=" or ">" (field_validator
        # rejects "<" and "<="), so in practice only ">=" needs to be caught.
        #
        # Pydantic cannot distinguish an explicit "operator: >=" from the model
        # default of ">=" — both look identical after construction.  We therefore
        # reject ANY ">=" (or "<=", defensively) on a for_each action rather than
        # allow the silent-promote behaviour that causes duplicate rows on re-run.
        # The false-positive rate is low: users who truly want ">=" must switch to
        # non-for_each execution mode or explicitly set "operator: >".
        for action in actions:
            if not isinstance(action.source, dict):
                continue
            if action.source.get("type") != "jdbc_watermark_v2":
                continue
            if action.watermark is None:
                continue
            op = action.watermark.operator
            if op in (">=", "<="):
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="035",
                    title="Strict comparison required for execution_mode: for_each",
                    details=(
                        f"Action '{action.name}' uses watermark.operator '{op}', "
                        "which is a non-strict comparison. Non-strict operators "
                        "include the current high-water-mark boundary row on every "
                        "re-run, causing duplicate rows in for_each batches. "
                        "Use '>' (ascending) or '<' (descending) instead."
                    ),
                    context={
                        "pipeline": flowgroup.pipeline,
                        "flowgroup": flowgroup.flowgroup,
                        "action": action.name,
                        "operator": op,
                        "execution_mode": "for_each",
                    },
                    suggestions=[
                        "Set watermark.operator to '>' for ascending watermarks "
                        "(the common case for timestamp/numeric columns).",
                        "Set watermark.operator to '<' for descending watermarks.",
                        "Strict comparison prevents max-watermark boundary row "
                        "duplication on re-run.",
                    ],
                )

        # Shared-keys check: only jdbc_watermark_v2 actions carry these fields
        wm_actions = [
            a for a in actions
            if isinstance(a.source, dict) and a.source.get("type") == "jdbc_watermark_v2"
        ]
        if len(wm_actions) > 1:
            self._validate_for_each_shared_wm_keys(flowgroup, wm_actions)

    def _validate_for_each_shared_wm_keys(
        self, flowgroup: FlowGroup, wm_actions: List[Action]
    ) -> None:
        """Raise LHP-CFG-033 when jdbc_watermark_v2 actions disagree on shared keys.

        All actions in a for_each flowgroup must share source_system_id,
        landing_path root prefix, wm_catalog, and wm_schema.

        Args:
            flowgroup: FlowGroup being validated (used only for error context).
            wm_actions: Subset of actions with source.type == jdbc_watermark_v2.

        Raises:
            LHPConfigError: LHP-CFG-033 when any shared key differs across actions.
        """
        def _wm_value(action: Action, key: str) -> Optional[str]:
            """Extract a watermark config value; returns None when absent."""
            if key in ("source_system_id",):
                return getattr(action.watermark, "source_system_id", None)
            if key == "wm_catalog":
                return getattr(action.watermark, "catalog", None)
            if key == "wm_schema":
                return getattr(action.watermark, "schema", None)
            if key == "watermark_type":
                wm_type = getattr(action.watermark, "type", None)
                # Pydantic stores enums as Enum instances; normalise to .value
                # so set comparison works on plain strings across versions.
                return getattr(wm_type, "value", wm_type)
            if key == "watermark_operator":
                return getattr(action.watermark, "operator", None)
            if key == "watermark_column":
                return getattr(action.watermark, "column", None)
            return None

        def _landing_root(action: Action) -> Optional[str]:
            """Return the first two path segments of landing_path as the root prefix."""
            path = action.landing_path or ""
            parts = path.lstrip("/").split("/")
            # /Volumes/<catalog>/<schema>/... → root = /Volumes/<catalog>/<schema>
            if len(parts) >= 3 and parts[0] == "Volumes":
                return "/" + "/".join(parts[:3])
            return path or None

        # ``watermark_type``, ``watermark_operator``, and ``watermark_column``
        # join the homogeneity check (devtest 2026-04-26 Anomaly A safety net):
        # the worker template branches statically on all three — heterogeneous
        # values across actions would silently apply action[0]'s rendered branch
        # to every iteration.  ``watermark_column`` heterogeneity is also
        # forbidden because the manifest stores a single wm_column per batch;
        # divergent columns would produce incorrect high-water-mark bookkeeping.
        checks: List[Tuple[str, Any]] = [
            ("source_system_id", [_wm_value(a, "source_system_id") for a in wm_actions]),
            ("landing_path root", [_landing_root(a) for a in wm_actions]),
            ("wm_catalog", [_wm_value(a, "wm_catalog") for a in wm_actions]),
            ("wm_schema", [_wm_value(a, "wm_schema") for a in wm_actions]),
            ("watermark_type", [_wm_value(a, "watermark_type") for a in wm_actions]),
            ("watermark_operator", [_wm_value(a, "watermark_operator") for a in wm_actions]),
            ("watermark_column", [_wm_value(a, "watermark_column") for a in wm_actions]),
        ]

        for key_name, values in checks:
            distinct = set(v for v in values if v is not None)
            if len(distinct) > 1:
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="033",
                    title=f"for_each actions disagree on shared key '{key_name}'",
                    details=(
                        f"All jdbc_watermark_v2 actions in a for_each flowgroup must "
                        f"share the same '{key_name}'. Found distinct values: "
                        f"{sorted(distinct)}."
                    ),
                    context={
                        "pipeline": flowgroup.pipeline,
                        "flowgroup": flowgroup.flowgroup,
                        "key": key_name,
                        "distinct_values": sorted(distinct),
                    },
                    suggestions=[
                        f"Ensure all actions in this for_each flowgroup use the same "
                        f"'{key_name}'. Split into separate flowgroups if the actions "
                        f"belong to different source systems.",
                    ],
                )

    def validate_project_invariants(
        self, all_flowgroups: List[FlowGroup]
    ) -> List[LHPError]:
        """Run project-scope for_each validation across all flowgroups.

        Covers checks that require visibility across the full project:

        * **LHP-CFG-032**: Two for_each flowgroups produce the same composite
          ``<pipeline>::<flowgroup>`` (renamed mid-development collision).
        * **LHP-CFG-033**: A pipeline mixes for_each and non-for_each flowgroups.
        * **LHP-CFG-036**: A pipeline contains 2+ for_each flowgroups (the DAB
          workflow generator emits one workflow YAML per pipeline keyed on the
          first flowgroup; additional for_each flowgroups would silently not
          execute).

        Per-flowgroup checks (LHP-CFG-031, count, concurrency, shared-keys) are
        handled earlier inside ``_validate_for_each_invariants`` which is called
        from ``validate_flowgroup``.

        Args:
            all_flowgroups: Every FlowGroup discovered in the project.

        Returns:
            List of LHPConfigError instances; empty when no violations found.
        """
        errors: List[LHPError] = []

        for_each_fgs = [
            fg for fg in all_flowgroups
            if (fg.workflow or {}).get("execution_mode") == "for_each"
        ]

        # LHP-CFG-032: composite uniqueness
        seen_composites: Dict[str, FlowGroup] = {}
        for fg in for_each_fgs:
            composite = f"{fg.pipeline}::{fg.flowgroup}"
            if composite in seen_composites:
                first = seen_composites[composite]
                errors.append(
                    LHPConfigError(
                        category=ErrorCategory.CONFIG,
                        code_number="032",
                        title="Composite load_group is not unique within project",
                        details=(
                            f"Two for_each flowgroups produce the same composite key "
                            f"'{composite}'. The B2 manifest uses this key as a "
                            "unique row identifier; duplicates corrupt manifest state."
                        ),
                        context={
                            "composite": composite,
                            "first": f"pipeline='{first.pipeline}', flowgroup='{first.flowgroup}'",
                            "second": f"pipeline='{fg.pipeline}', flowgroup='{fg.flowgroup}'",
                        },
                        suggestions=[
                            "Rename the pipeline or flowgroup so the composite "
                            "'<pipeline>::<flowgroup>' is unique across the project.",
                        ],
                    )
                )
            else:
                seen_composites[composite] = fg

        # LHP-CFG-033 (same-pipeline same-mode): pipeline may not mix for_each
        # and non-for_each flowgroups
        pipeline_modes: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
        for fg in all_flowgroups:
            mode = (fg.workflow or {}).get("execution_mode") or "default"
            pipeline_modes[fg.pipeline].append((fg.flowgroup, mode))

        for pipeline_name, fg_modes in pipeline_modes.items():
            modes_present = {mode for _, mode in fg_modes}
            if "for_each" in modes_present and len(modes_present) > 1:
                for_each_names = [n for n, m in fg_modes if m == "for_each"]
                other_names = [n for n, m in fg_modes if m != "for_each"]
                errors.append(
                    LHPConfigError(
                        category=ErrorCategory.CONFIG,
                        code_number="033",
                        title="Pipeline mixes for_each and non-for_each flowgroups",
                        details=(
                            f"Pipeline '{pipeline_name}' contains both for_each "
                            "flowgroups and flowgroups without execution_mode. "
                            "Mixed-mode pipelines are not supported because the "
                            "B2 orchestrator cannot issue a single iterations "
                            "taskValue that covers both execution paths."
                        ),
                        context={
                            "pipeline": pipeline_name,
                            "for_each_flowgroups": for_each_names,
                            "other_flowgroups": other_names,
                        },
                        suggestions=[
                            "Move non-for_each flowgroups to a separate pipeline, or "
                            "convert all flowgroups in this pipeline to for_each.",
                        ],
                    )
                )

        # LHP-CFG-036: at most one for_each flowgroup per pipeline.
        # The DAB workflow generator keys pipeline_name_map by pipeline name;
        # only the first for_each flowgroup's aux files are referenced in the
        # emitted workflow YAML.  Additional for_each flowgroups in the same
        # pipeline would silently never execute.
        errors.extend(self._validate_for_each_per_pipeline_uniqueness(for_each_fgs))

        return errors

    def _validate_for_each_per_pipeline_uniqueness(
        self, for_each_flowgroups: List[FlowGroup]
    ) -> List[LHPError]:
        """Return LHP-CFG-036 errors for pipelines with 2+ for_each flowgroups.

        Empty-action flowgroups raise LHP-CFG-033 earlier in per-flowgroup
        validation (``_validate_for_each_invariants``).  This method checks
        purely at project scope and fires regardless of action count so that
        operators who bypass per-flowgroup validation still see the error.

        Args:
            for_each_flowgroups: All FlowGroup objects whose execution_mode is
                ``for_each``, across all pipelines in the project.

        Returns:
            List of LHPConfigError instances; one per violating pipeline.
        """
        errors: List[LHPError] = []

        pipeline_for_each_fgs: Dict[str, List[str]] = defaultdict(list)
        for fg in for_each_flowgroups:
            pipeline_for_each_fgs[fg.pipeline].append(fg.flowgroup)

        for pipeline_name, fg_names in pipeline_for_each_fgs.items():
            if len(fg_names) < 2:
                continue
            count = len(fg_names)
            fg_list = ", ".join(fg_names)
            errors.append(
                LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="036",
                    title="Pipeline has multiple for_each flowgroups",
                    details=(
                        f"Pipeline '{pipeline_name}' has {count} flowgroups with "
                        f"execution_mode: for_each ({fg_list}). LHP currently supports "
                        "at most one for_each flowgroup per pipeline (the DAB workflow "
                        "generator emits one workflow YAML per pipeline, keyed on the "
                        "first flowgroup; remaining flowgroups would not execute). "
                        "To fix, either: (a) consolidate the for_each actions into a "
                        "single flowgroup if they share orchestration concerns; or "
                        "(b) split each flowgroup into its own pipeline if they are "
                        "organizationally distinct (e.g., separate source systems). "
                        "If you have a use case requiring multiple for_each flowgroups "
                        "per pipeline, please file an issue describing the scenario."
                    ),
                    context={
                        "pipeline": pipeline_name,
                        "for_each_flowgroup_count": count,
                        "for_each_flowgroups": fg_names,
                    },
                    suggestions=[
                        "Consolidate the for_each actions into a single flowgroup if "
                        "they share orchestration concerns (same source system, same "
                        "watermark catalog, same landing path root).",
                        "Split each flowgroup into its own pipeline if they are "
                        "organizationally distinct (e.g., separate source systems).",
                    ],
                )
            )

        return errors

    def validate_duplicate_pipeline_flowgroup(
        self, flowgroups: List[FlowGroup]
    ) -> List[str]:
        """Validate that there are no duplicate pipeline+flowgroup combinations.

        Args:
            flowgroups: List of all flowgroups to validate

        Returns:
            List of validation error messages
        """
        errors = []
        seen_combinations = set()

        for flowgroup in flowgroups:
            # Create a unique key from pipeline and flowgroup
            combination_key = f"{flowgroup.pipeline}.{flowgroup.flowgroup}"

            if combination_key in seen_combinations:
                errors.append(
                    f"Duplicate pipeline+flowgroup combination: '{combination_key}'. "
                    f"Each pipeline+flowgroup combination must be unique across all YAML files."
                )
            else:
                seen_combinations.add(combination_key)

        return errors
