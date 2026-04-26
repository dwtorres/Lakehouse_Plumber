"""Tests for core data models of LakehousePlumber."""

import pytest
from lhp.models.config import ActionType, LoadSourceType, TransformType, WriteTargetType, Action, FlowGroup, Template, Preset, TestActionType, ViolationAction


class TestModels:
    """Test the core data models."""
    
    def test_action_type_enum(self):
        """Test ActionType enum values."""
        assert ActionType.LOAD.value == "load"
        assert ActionType.TRANSFORM.value == "transform"
        assert ActionType.WRITE.value == "write"
        # Test for new TEST action type
        assert ActionType.TEST.value == "test"
    
    def test_test_type_enum(self):
        """Test TestActionType enum exists with all required test types."""
        # Test that TestActionType enum exists
        assert TestActionType is not None
        
        # Test all 9 test types exist
        assert TestActionType.ROW_COUNT.value == "row_count"
        assert TestActionType.UNIQUENESS.value == "uniqueness"
        assert TestActionType.REFERENTIAL_INTEGRITY.value == "referential_integrity"
        assert TestActionType.COMPLETENESS.value == "completeness"
        assert TestActionType.RANGE.value == "range"
        assert TestActionType.SCHEMA_MATCH.value == "schema_match"
        assert TestActionType.ALL_LOOKUPS_FOUND.value == "all_lookups_found"
        assert TestActionType.CUSTOM_SQL.value == "custom_sql"
        assert TestActionType.CUSTOM_EXPECTATIONS.value == "custom_expectations"
    
    def test_violation_action_enum(self):
        """Test ViolationAction enum exists with required values."""
        # Test that ViolationAction enum exists
        assert ViolationAction is not None
        
        # Test violation action values
        assert ViolationAction.FAIL.value == "fail"
        assert ViolationAction.WARN.value == "warn"
    
    def test_action_model(self):
        """Test Action model creation."""
        action = Action(
            name="test_action",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/test/path"},
            target="test_view",
            description="Test action"
        )
        assert action.name == "test_action"
        assert action.type == ActionType.LOAD
        assert action.target == "test_view"
    
    def test_flowgroup_model(self):
        """Test FlowGroup model creation."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="raw_data"),
                Action(name="clean_data", type=ActionType.TRANSFORM, source="raw_data", target="clean_data")
            ]
        )
        assert flowgroup.pipeline == "test_pipeline"
        assert len(flowgroup.actions) == 2
        assert flowgroup.presets == ["bronze_layer"]
    
    def test_preset_model(self):
        """Test Preset model creation."""
        preset = Preset(
            name="bronze_layer",
            version="1.0",
            extends="base_preset",
            description="Bronze layer preset",
            defaults={"schema_evolution": "addNewColumns"}
        )
        assert preset.name == "bronze_layer"
        assert preset.extends == "base_preset"
        assert preset.defaults.get("schema_evolution") == "addNewColumns"

    def test_flowgroup_workflow_for_each_keys(self):
        """B2 (R7): FlowGroup.workflow accepts execution_mode + concurrency + max_retries."""
        flowgroup = FlowGroup(
            pipeline="bronze",
            flowgroup="customers_daily",
            workflow={
                "execution_mode": "for_each",
                "concurrency": 10,
                "max_retries": 1,
            },
            actions=[
                Action(name="load", type=ActionType.LOAD, target="raw"),
            ],
        )
        assert flowgroup.workflow is not None
        assert flowgroup.workflow.get("execution_mode") == "for_each"
        assert flowgroup.workflow.get("concurrency") == 10
        assert flowgroup.workflow.get("max_retries") == 1

    def test_flowgroup_workflow_absent(self):
        """B2 (R7): FlowGroup.workflow defaults to None when omitted."""
        flowgroup = FlowGroup(
            pipeline="bronze",
            flowgroup="legacy",
            actions=[Action(name="load", type=ActionType.LOAD, target="raw")],
        )
        assert flowgroup.workflow is None

    def test_flowgroup_workflow_coexists_with_extraction_mode(self):
        """B2 (R7): execution_mode + existing extraction_mode key coexist in same workflow dict."""
        flowgroup = FlowGroup(
            pipeline="bronze",
            flowgroup="hybrid",
            workflow={
                "extraction_mode": "serial",
                "execution_mode": "for_each",
                "concurrency": 5,
            },
            actions=[Action(name="load", type=ActionType.LOAD, target="raw")],
        )
        assert flowgroup.workflow.get("extraction_mode") == "serial"
        assert flowgroup.workflow.get("execution_mode") == "for_each"

    def test_flowgroup_workflow_unsupported_execution_mode_not_rejected_at_model(self):
        """B2 (R7): Model layer accepts arbitrary execution_mode string. Validator (U2) rejects unsupported values."""
        flowgroup = FlowGroup(
            pipeline="bronze",
            flowgroup="invalid",
            workflow={"execution_mode": "invalid_mode"},
            actions=[Action(name="load", type=ActionType.LOAD, target="raw")],
        )
        assert flowgroup.workflow.get("execution_mode") == "invalid_mode"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])