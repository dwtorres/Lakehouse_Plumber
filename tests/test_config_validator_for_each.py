"""Tests for for_each invariant validation — U2 of B2 watermark scale-out.

Covers LHP-CFG-031 (separator collision), LHP-CFG-032 (composite uniqueness),
and LHP-CFG-033 (post-expansion structure: action count, shared keys,
concurrency bounds, same-pipeline mixed-mode).

AE cross-links per plan requirement:
  LHP-CFG-031 → R6 §separator_collision
  LHP-CFG-032 → R6 §composite_uniqueness
  LHP-CFG-033 → R6 §post_expansion_structure
"""

import pytest

from lhp.core.validator import ConfigValidator
from lhp.models.config import Action, ActionType, FlowGroup
from lhp.models.pipeline_config import WatermarkConfig, WatermarkType
from lhp.utils.error_formatter import LHPConfigError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_load_action(name: str = "load_1") -> Action:
    """Return a minimal LOAD action that passes per-action validation."""
    return Action(
        name=name,
        type=ActionType.LOAD,
        target=f"v_{name}",
        source={"type": "cloudfiles", "path": "/mnt/data", "format": "json"},
    )


def _wm_action(
    name: str = "load_wm",
    source_system_id: str = "db1",
    landing_path: str = "/Volumes/cat/land/tbl",
    wm_catalog: str = "metadata",
    wm_schema: str = "orchestration",
    wm_column: str = "updated_at",
    operator: str = ">",
) -> Action:
    """Return a jdbc_watermark_v2 action with fully-specified shared keys.

    Defaults to operator='>' (strict) so CFG-035 does not fire before
    shared-keys tests can exercise CFG-033.
    """
    return Action(
        name=name,
        type=ActionType.LOAD,
        target=f"v_{name}",
        landing_path=landing_path,
        watermark=WatermarkConfig(
            column=wm_column,
            type=WatermarkType.TIMESTAMP,
            operator=operator,
            source_system_id=source_system_id,
            catalog=wm_catalog,
            schema=wm_schema,
        ),
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://db1:5432/mydb",
            "user": "u",
            "password": "p",
            "driver": "org.postgresql.Driver",
            "schema_name": "public",
            "table_name": "orders",
        },
    )


def _fg_for_each(
    pipeline: str = "bronze",
    flowgroup: str = "customers_daily",
    actions: list | None = None,
    concurrency: int | None = None,
) -> FlowGroup:
    """Return a for_each FlowGroup with sensible defaults."""
    workflow: dict = {"execution_mode": "for_each"}
    if concurrency is not None:
        workflow["concurrency"] = concurrency
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        workflow=workflow,
        actions=actions if actions is not None else [_minimal_load_action()],
    )


def _fg_default(pipeline: str = "bronze", flowgroup: str = "other_fg") -> FlowGroup:
    """Return a FlowGroup without execution_mode (default/legacy)."""
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        actions=[_minimal_load_action()],
    )


# ---------------------------------------------------------------------------
# LHP-CFG-031: separator collision
# ---------------------------------------------------------------------------


class TestSeparatorCollision:
    """AE: LHP-CFG-031 — R6 §separator_collision."""

    def test_happy_path_no_separator(self):
        """for_each flowgroup with clean pipeline + flowgroup names → no CFG-031.

        AE: LHP-CFG-031 inactive — names do not contain '::'.
        Calls _validate_for_each_invariants directly to isolate the separator
        check from unrelated structural requirements (e.g. Write action).
        """
        validator = ConfigValidator()
        fg = _fg_for_each(pipeline="bronze", flowgroup="customers_daily")
        # Should not raise LHP-CFG-031
        validator._validate_for_each_invariants(fg)

    def test_happy_path_no_execution_mode(self):
        """pipeline contains '::' but execution_mode unset → no error.

        AE: LHP-CFG-031 inactive for legacy flowgroups not using for_each.
        """
        validator = ConfigValidator()
        fg = FlowGroup(
            pipeline="bronze::core",
            flowgroup="customers_daily",
            actions=[_minimal_load_action()],
        )
        errors = validator.validate_flowgroup(fg)
        # No CFG-031 error; other errors unrelated to separator may appear
        cfg031_errors = [
            e for e in errors
            if isinstance(e, LHPConfigError) and e.code == "LHP-CFG-031"
        ]
        assert cfg031_errors == []

    def test_error_pipeline_contains_separator(self):
        """for_each flowgroup with '::' in pipeline → LHP-CFG-031, mentions 'pipeline'.

        AE: LHP-CFG-031 error path (pipeline field).
        """
        validator = ConfigValidator()
        fg = _fg_for_each(pipeline="bronze::core", flowgroup="customers_daily")
        with pytest.raises(LHPConfigError) as exc_info:
            validator.validate_flowgroup(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-031"
        assert "pipeline" in err.context.get("field", "")

    def test_error_flowgroup_contains_separator(self):
        """for_each flowgroup with '::' in flowgroup → LHP-CFG-031, mentions 'flowgroup'.

        AE: LHP-CFG-031 error path (flowgroup field).
        """
        validator = ConfigValidator()
        fg = _fg_for_each(pipeline="bronze", flowgroup="customers::daily")
        with pytest.raises(LHPConfigError) as exc_info:
            validator.validate_flowgroup(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-031"
        assert "flowgroup" in err.context.get("field", "")


# ---------------------------------------------------------------------------
# LHP-CFG-032: composite uniqueness (project-scope)
# ---------------------------------------------------------------------------


class TestCompositeUniqueness:
    """AE: LHP-CFG-032 — R6 §composite_uniqueness."""

    def test_error_duplicate_composite(self):
        """Two for_each flowgroups produce same composite → LHP-CFG-032 listing both.

        AE: LHP-CFG-032 error path.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="bronze", flowgroup="customers_daily")
        fg_b = _fg_for_each(pipeline="bronze", flowgroup="customers_daily")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        assert len(project_errors) >= 1
        cfg032 = [e for e in project_errors if e.code == "LHP-CFG-032"]
        assert len(cfg032) == 1
        err = cfg032[0]
        # Both flowgroup identifiers must appear in the error context
        assert "bronze" in str(err.context)
        assert "customers_daily" in str(err.context)

    def test_happy_path_different_composites(self):
        """Two for_each flowgroups with different composites → no error.

        AE: LHP-CFG-032 inactive — composites are distinct.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="bronze", flowgroup="customers_daily")
        fg_b = _fg_for_each(pipeline="bronze", flowgroup="orders_daily")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg032 = [e for e in project_errors if e.code == "LHP-CFG-032"]
        assert cfg032 == []

    def test_happy_path_no_for_each_flowgroups(self):
        """Project with no for_each flowgroups → no composite uniqueness error."""
        validator = ConfigValidator()
        fg_a = _fg_default(pipeline="bronze", flowgroup="alpha")
        fg_b = _fg_default(pipeline="bronze", flowgroup="beta")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg032 = [e for e in project_errors if e.code == "LHP-CFG-032"]
        assert cfg032 == []

    def test_integration_composite_uniqueness_across_files(self):
        """Simulates two flowgroups from separate files sharing the same composite.

        AE: integration — full project validation surfaces composite collision.
        """
        validator = ConfigValidator()
        # Mimic loading from two separate YAML files
        fg_file1 = _fg_for_each(pipeline="gold", flowgroup="agg_daily")
        fg_file2 = _fg_for_each(pipeline="gold", flowgroup="agg_daily")
        project_errors = validator.validate_project_invariants([fg_file1, fg_file2])
        cfg032 = [e for e in project_errors if e.code == "LHP-CFG-032"]
        assert len(cfg032) == 1
        # Error details must reference the conflicting composite
        assert "gold::agg_daily" in cfg032[0].details


# ---------------------------------------------------------------------------
# LHP-CFG-033: post-expansion structure — action count
# ---------------------------------------------------------------------------


class TestActionCount:
    """AE: LHP-CFG-033 §action_count — R6."""

    def test_error_zero_actions(self):
        """for_each with 0 actions → LHP-CFG-033.

        AE: LHP-CFG-033 action count error path (empty).
        """
        validator = ConfigValidator()
        fg = FlowGroup(
            pipeline="bronze",
            flowgroup="empty_fg",
            workflow={"execution_mode": "for_each"},
            actions=[],
        )
        # validate_flowgroup raises before reaching for_each checks when actions=[],
        # but the for_each check also raises — ensure CFG-033 fires when only
        # the for_each path can see an empty expansion.  We call the helper directly.
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        assert exc_info.value.code == "LHP-CFG-033"

    def test_error_301_actions(self):
        """for_each with 301 actions → LHP-CFG-033; suggestion includes 'Split'.

        AE: LHP-CFG-033 action count error path (>300).
        """
        validator = ConfigValidator()
        actions = [_minimal_load_action(name=f"a{i}") for i in range(301)]
        fg = _fg_for_each(actions=actions)
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert any("Split" in s for s in err.suggestions)

    def test_edge_one_action(self):
        """for_each with exactly 1 action → no error (lower bound inclusive).

        AE: LHP-CFG-033 edge case — 1 action.
        """
        validator = ConfigValidator()
        fg = _fg_for_each(actions=[_minimal_load_action()])
        # Should not raise
        validator._validate_for_each_invariants(fg)

    def test_edge_300_actions(self):
        """for_each with exactly 300 actions → no error (upper bound inclusive).

        AE: LHP-CFG-033 edge case — 300 actions.
        """
        validator = ConfigValidator()
        actions = [_minimal_load_action(name=f"a{i}") for i in range(300)]
        fg = _fg_for_each(actions=actions)
        # Should not raise
        validator._validate_for_each_invariants(fg)


# ---------------------------------------------------------------------------
# LHP-CFG-033: post-expansion structure — shared keys
# ---------------------------------------------------------------------------


class TestSharedKeys:
    """AE: LHP-CFG-033 §shared_keys — R6."""

    def test_error_disagreeing_source_system_id(self):
        """Two jdbc_watermark_v2 actions with different source_system_id → LHP-CFG-033.

        AE: LHP-CFG-033 shared keys error path (source_system_id).
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", source_system_id="db1")
        a2 = _wm_action(name="load_2", source_system_id="db2")
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "source_system_id" in err.context.get("key", "")

    def test_error_disagreeing_landing_path_root(self):
        """Two jdbc_watermark_v2 actions with different landing_path roots → LHP-CFG-033.

        AE: LHP-CFG-033 shared keys error path (landing_path root).
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", landing_path="/Volumes/cat/land_a/tbl")
        a2 = _wm_action(name="load_2", landing_path="/Volumes/cat/land_b/tbl")
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "landing_path" in err.context.get("key", "")

    def test_error_disagreeing_wm_catalog(self):
        """Two jdbc_watermark_v2 actions with different wm_catalog → LHP-CFG-033.

        AE: LHP-CFG-033 shared keys error path (wm_catalog).
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", wm_catalog="meta_a")
        a2 = _wm_action(name="load_2", wm_catalog="meta_b")
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "wm_catalog" in err.context.get("key", "")

    def test_error_disagreeing_wm_schema(self):
        """Two jdbc_watermark_v2 actions with different wm_schema → LHP-CFG-033.

        AE: LHP-CFG-033 shared keys error path (wm_schema).
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", wm_schema="orch_a")
        a2 = _wm_action(name="load_2", wm_schema="orch_b")
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "wm_schema" in err.context.get("key", "")

    def test_happy_path_matching_shared_keys(self):
        """Two jdbc_watermark_v2 actions that agree on all shared keys → no error.

        AE: LHP-CFG-033 shared keys happy path.
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1")
        a2 = _wm_action(name="load_2")
        fg = _fg_for_each(actions=[a1, a2])
        # Should not raise
        validator._validate_for_each_invariants(fg)

    def test_non_wm_actions_not_checked(self):
        """Non-jdbc_watermark_v2 actions are not subject to shared-key checks.

        AE: LHP-CFG-033 shared keys — only jdbc_watermark_v2 source type checked.
        """
        validator = ConfigValidator()
        a1 = _minimal_load_action(name="load_1")
        a2 = _minimal_load_action(name="load_2")
        fg = _fg_for_each(actions=[a1, a2])
        # Should not raise
        validator._validate_for_each_invariants(fg)

    def test_error_disagreeing_watermark_type(self):
        """Different watermark.type across actions in for_each → LHP-CFG-033.

        Anomaly A safety net: the worker template branches statically on
        watermark_type ({% if watermark_type == "timestamp" %}…), so all
        actions in a for_each flowgroup MUST share the same type.
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1")
        a2 = _wm_action(name="load_2")
        # Force a2's watermark to numeric to disagree.
        a2.watermark.type = WatermarkType.NUMERIC
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "watermark_type" in err.context.get("key", "")

    def test_error_disagreeing_watermark_operator(self):
        """Different watermark.operator across actions in for_each → LHP-CFG-033.

        Anomaly A safety net: operator is rendered into the worker once at
        codegen time. Heterogeneous operators across actions would silently
        apply the first action's operator to every iteration.

        LHP-CFG-035 fires first in _validate_for_each_invariants when any
        action has operator '>=', so this test calls the shared-keys method
        directly to isolate the CFG-033 heterogeneity path.
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", operator=">")
        a2 = _wm_action(name="load_2", operator=">")
        # Bypass Pydantic field_validator to force '>=' on a2 without triggering
        # the model-construction error — direct attribute assignment skips
        # field_validator re-execution on already-constructed instances.
        a2.watermark.operator = ">="
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_shared_wm_keys(fg, [a1, a2])
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "watermark_operator" in err.context.get("key", "")

    def test_error_disagreeing_watermark_column(self):
        """Different watermark.column across actions in for_each → LHP-CFG-033.

        Added in wave-1 review fix #27: the manifest stores a single wm_column
        per batch, so divergent columns across actions produce incorrect
        high-water-mark bookkeeping.

        AE: LHP-CFG-033 shared keys error path (watermark_column).
        """
        validator = ConfigValidator()
        a1 = _wm_action(name="load_1", wm_column="updated_at")
        a2 = _wm_action(name="load_2", wm_column="created_at")
        fg = _fg_for_each(actions=[a1, a2])
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert "watermark_column" in err.context.get("key", "")


# ---------------------------------------------------------------------------
# LHP-CFG-033: post-expansion structure — concurrency bounds
# ---------------------------------------------------------------------------


class TestConcurrencyBounds:
    """AE: LHP-CFG-033 §concurrency_bounds — R6."""

    def test_error_concurrency_zero(self):
        """workflow.concurrency: 0 → LHP-CFG-033.

        AE: LHP-CFG-033 concurrency error path (below minimum).
        """
        validator = ConfigValidator()
        fg = _fg_for_each(concurrency=0)
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert err.context.get("concurrency") == 0

    def test_error_concurrency_101(self):
        """workflow.concurrency: 101 → LHP-CFG-033.

        AE: LHP-CFG-033 concurrency error path (exceeds maximum).
        """
        validator = ConfigValidator()
        fg = _fg_for_each(concurrency=101)
        with pytest.raises(LHPConfigError) as exc_info:
            validator._validate_for_each_invariants(fg)
        err = exc_info.value
        assert err.code == "LHP-CFG-033"
        assert err.context.get("concurrency") == 101

    def test_edge_concurrency_1(self):
        """workflow.concurrency: 1 → no error (lower bound inclusive).

        AE: LHP-CFG-033 concurrency edge — minimum valid value.
        """
        validator = ConfigValidator()
        fg = _fg_for_each(concurrency=1)
        validator._validate_for_each_invariants(fg)

    def test_edge_concurrency_100(self):
        """workflow.concurrency: 100 → no error (upper bound inclusive).

        AE: LHP-CFG-033 concurrency edge — maximum valid value.
        """
        validator = ConfigValidator()
        fg = _fg_for_each(concurrency=100)
        validator._validate_for_each_invariants(fg)

    def test_edge_concurrency_absent(self):
        """concurrency absent, action_count=5 → no error; codegen applies default.

        AE: LHP-CFG-033 concurrency default (absent = no validator error).
        """
        validator = ConfigValidator()
        actions = [_minimal_load_action(name=f"a{i}") for i in range(5)]
        fg = _fg_for_each(actions=actions)
        # Should not raise — concurrency is optional
        validator._validate_for_each_invariants(fg)


# ---------------------------------------------------------------------------
# LHP-CFG-033: same-pipeline mixed-mode (project-scope)
# ---------------------------------------------------------------------------


class TestSamePipelineMixedMode:
    """AE: LHP-CFG-033 §same_pipeline_same_mode — R6."""

    def test_error_mixed_mode_pipeline(self):
        """Pipeline with one for_each fg and one default fg → LHP-CFG-033.

        AE: LHP-CFG-033 same-pipeline mixed-mode error path.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="bronze", flowgroup="fg_a")
        fg_b = _fg_default(pipeline="bronze", flowgroup="fg_b")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg033 = [e for e in project_errors if e.code == "LHP-CFG-033"]
        assert len(cfg033) >= 1
        err = cfg033[0]
        assert err.context.get("pipeline") == "bronze"

    def test_happy_path_all_for_each(self):
        """Pipeline with all for_each flowgroups → no mixed-mode error.

        AE: LHP-CFG-033 same-mode happy path.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="bronze", flowgroup="fg_a")
        fg_b = _fg_for_each(pipeline="bronze", flowgroup="fg_b")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        mixed_mode_errors = [e for e in project_errors if e.code == "LHP-CFG-033"]
        assert mixed_mode_errors == []

    def test_happy_path_all_default(self):
        """Pipeline with all default flowgroups → no mixed-mode error.

        AE: LHP-CFG-033 same-mode happy path (no for_each at all).
        """
        validator = ConfigValidator()
        fg_a = _fg_default(pipeline="silver", flowgroup="fg_a")
        fg_b = _fg_default(pipeline="silver", flowgroup="fg_b")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg033 = [e for e in project_errors if e.code == "LHP-CFG-033"]
        assert cfg033 == []

    def test_happy_path_different_pipelines(self):
        """for_each in pipeline A and default in pipeline B → no error.

        AE: LHP-CFG-033 — mixed mode check is per-pipeline.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="bronze", flowgroup="fg_a")
        fg_b = _fg_default(pipeline="silver", flowgroup="fg_b")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg033 = [e for e in project_errors if e.code == "LHP-CFG-033"]
        assert cfg033 == []

    def test_mixed_mode_error_names_pipeline(self):
        """Mixed-mode error context includes the pipeline name.

        AE: LHP-CFG-033 — error must name the conflicting pipeline.
        """
        validator = ConfigValidator()
        fg_a = _fg_for_each(pipeline="gold_pipeline", flowgroup="fg_for_each")
        fg_b = _fg_default(pipeline="gold_pipeline", flowgroup="fg_legacy")
        project_errors = validator.validate_project_invariants([fg_a, fg_b])
        cfg033 = [e for e in project_errors if e.code == "LHP-CFG-033"]
        assert len(cfg033) >= 1
        assert cfg033[0].context.get("pipeline") == "gold_pipeline"
