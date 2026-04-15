"""Tests for JDBC watermark v2 model additions: LoadSourceType.JDBC_WATERMARK_V2, Action.landing_path, and WatermarkConfig v2 fields."""

import pytest
from pydantic import ValidationError

from lhp.models.config import Action, ActionType, LoadSourceType
from lhp.models.pipeline_config import WatermarkConfig, WatermarkType


@pytest.mark.unit
class TestLoadSourceTypeV2:
    """Tests for JDBC_WATERMARK_V2 enum value."""

    def test_jdbc_watermark_v2_enum_exists(self):
        assert LoadSourceType.JDBC_WATERMARK_V2.value == "jdbc_watermark_v2"

    def test_jdbc_watermark_v2_from_string(self):
        assert LoadSourceType("jdbc_watermark_v2") is LoadSourceType.JDBC_WATERMARK_V2

    def test_v1_jdbc_watermark_unchanged(self):
        assert LoadSourceType("jdbc_watermark") is LoadSourceType.JDBC_WATERMARK

    def test_cloudfiles_unchanged(self):
        assert LoadSourceType("cloudfiles") is LoadSourceType.CLOUDFILES


@pytest.mark.unit
class TestActionLandingPath:
    """Tests for the landing_path field on Action model."""

    def test_action_with_landing_path(self):
        action = Action(
            name="load_product",
            type=ActionType.LOAD,
            landing_path="/Volumes/catalog/schema/landing/product",
        )
        assert action.landing_path == "/Volumes/catalog/schema/landing/product"

    def test_action_without_landing_path_defaults_none(self):
        action = Action(name="load_static", type=ActionType.LOAD)
        assert action.landing_path is None

    def test_action_with_landing_path_none_explicitly(self):
        action = Action(name="load_static", type=ActionType.LOAD, landing_path=None)
        assert action.landing_path is None

    def test_existing_action_fields_unaffected(self):
        """Backward compat: existing configs without landing_path still work."""
        action = Action(
            name="load_orders",
            type=ActionType.LOAD,
            source={"type": "cloudfiles"},
        )
        assert action.landing_path is None
        assert action.source == {"type": "cloudfiles"}


@pytest.mark.unit
class TestWatermarkConfigV2Fields:
    """Tests for v2 fields on WatermarkConfig: source_system_id, catalog, schema."""

    def test_v1_config_still_validates(self):
        """Existing v1 configs without new fields must continue to work."""
        config = WatermarkConfig(column="ModifiedDate", type="timestamp")
        assert config.column == "ModifiedDate"
        assert config.type is WatermarkType.TIMESTAMP

    def test_source_system_id_field(self):
        config = WatermarkConfig(
            column="ModifiedDate",
            type="timestamp",
            source_system_id="pg_prod",
        )
        assert config.source_system_id == "pg_prod"

    def test_catalog_field(self):
        config = WatermarkConfig(
            column="ModifiedDate",
            type="timestamp",
            catalog="metadata",
        )
        assert config.catalog == "metadata"

    def test_schema_field(self):
        config = WatermarkConfig(
            column="ModifiedDate",
            type="timestamp",
            schema="orchestration",
        )
        assert config.schema == "orchestration"

    def test_all_v2_fields_together(self):
        config = WatermarkConfig(
            column="x",
            type="timestamp",
            source_system_id="pg_prod",
            catalog="metadata",
            schema="orchestration",
        )
        assert config.source_system_id == "pg_prod"
        assert config.catalog == "metadata"
        assert config.schema == "orchestration"

    def test_v2_fields_default_none(self):
        config = WatermarkConfig(column="x", type="timestamp")
        assert config.source_system_id is None
        assert config.catalog is None
        assert config.schema is None
