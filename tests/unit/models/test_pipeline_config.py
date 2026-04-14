"""Tests for watermark models in pipeline_config and Action integration."""

import pytest
from pydantic import ValidationError

from lhp.models.config import Action, ActionType
from lhp.models.pipeline_config import WatermarkConfig, WatermarkType


@pytest.mark.unit
class TestWatermarkType:
    """Tests for WatermarkType enum values and string coercion."""

    def test_timestamp_value(self):
        assert WatermarkType.TIMESTAMP.value == "timestamp"

    def test_numeric_value(self):
        assert WatermarkType.NUMERIC.value == "numeric"

    def test_from_string_timestamp(self):
        assert WatermarkType("timestamp") is WatermarkType.TIMESTAMP

    def test_from_string_numeric(self):
        assert WatermarkType("numeric") is WatermarkType.NUMERIC


@pytest.mark.unit
class TestWatermarkConfig:
    """Tests for WatermarkConfig Pydantic model validation."""

    def test_valid_timestamp_config(self):
        config = WatermarkConfig(column="updated_at", type=WatermarkType.TIMESTAMP)
        assert config.column == "updated_at"
        assert config.type is WatermarkType.TIMESTAMP

    def test_valid_numeric_config(self):
        config = WatermarkConfig(column="id", type=WatermarkType.NUMERIC)
        assert config.column == "id"
        assert config.type is WatermarkType.NUMERIC

    def test_default_operator_is_gte(self):
        config = WatermarkConfig(column="updated_at", type=WatermarkType.TIMESTAMP)
        assert config.operator == ">="

    def test_explicit_operator_gt(self):
        config = WatermarkConfig(
            column="updated_at", type=WatermarkType.TIMESTAMP, operator=">"
        )
        assert config.operator == ">"

    def test_invalid_type_raises_validation_error(self):
        with pytest.raises(ValidationError):
            WatermarkConfig(column="uuid_col", type="uuid")

    def test_invalid_operator_lt_raises_validation_error(self):
        with pytest.raises(ValidationError, match="operator must be '>=' or '>'"):
            WatermarkConfig(
                column="updated_at", type=WatermarkType.TIMESTAMP, operator="<"
            )

    def test_invalid_operator_eq_raises_validation_error(self):
        with pytest.raises(ValidationError, match="operator must be '>=' or '>'"):
            WatermarkConfig(
                column="updated_at", type=WatermarkType.TIMESTAMP, operator="=="
            )

    def test_missing_column_raises_validation_error(self):
        with pytest.raises(ValidationError):
            WatermarkConfig(type=WatermarkType.TIMESTAMP)

    def test_missing_type_raises_validation_error(self):
        with pytest.raises(ValidationError):
            WatermarkConfig(column="updated_at")


@pytest.mark.unit
class TestActionWatermark:
    """Tests for the watermark field on the Action model."""

    def test_action_with_watermark_dict(self):
        action = Action(
            name="load_orders",
            type=ActionType.LOAD,
            watermark={"column": "modified_ts", "type": "timestamp"},
        )
        assert isinstance(action.watermark, WatermarkConfig)
        assert action.watermark.column == "modified_ts"
        assert action.watermark.type is WatermarkType.TIMESTAMP
        assert action.watermark.operator == ">="

    def test_action_without_watermark(self):
        action = Action(name="load_static", type=ActionType.LOAD)
        assert action.watermark is None

    def test_action_with_watermark_none_explicitly(self):
        action = Action(name="load_static", type=ActionType.LOAD, watermark=None)
        assert action.watermark is None
