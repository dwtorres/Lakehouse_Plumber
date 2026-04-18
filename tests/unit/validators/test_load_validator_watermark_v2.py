"""Tests for JDBC watermark v2 validation rules in LoadActionValidator."""

import pytest

from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
from lhp.core.validators.load_validator import LoadActionValidator
from lhp.models.config import Action


@pytest.mark.unit
class TestJDBCWatermarkV2Validation:
    """Test suite for jdbc_watermark_v2 source validation (LHP-VAL-04x)."""

    def setup_method(self):
        self.registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()
        self.validator = LoadActionValidator(self.registry, self.field_validator)

    def _make_valid_v2_action(self, **overrides):
        """Build a fully valid jdbc_watermark_v2 Action, applying overrides."""
        source = {
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
            "table_name": "Product",
        }
        watermark = {
            "column": "ModifiedDate",
            "type": "timestamp",
            "source_system_id": "pg_prod",
        }
        kwargs = {
            "name": "load_product_jdbc",
            "type": "load",
            "source": source,
            "target": "v_product_raw",
            "landing_path": "/Volumes/catalog/schema/landing/product",
            "watermark": watermark,
        }
        kwargs.update(overrides)
        return Action(**kwargs)

    # --- Happy path ---

    def test_fully_valid_v2_config_passes(self):
        action = self._make_valid_v2_action()
        errors = self.validator.validate(action, "test")
        assert errors == [], f"Expected no errors but got: {errors}"

    # --- LHP-VAL-040: landing_path required ---

    def test_missing_landing_path_is_error(self):
        action = self._make_valid_v2_action(landing_path=None)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "landing_path" in str(e).lower() for e in errors
        ), f"Expected landing_path error, got: {errors}"

    # --- LHP-VAL-041: watermark config required ---

    def test_missing_watermark_is_error(self):
        action = self._make_valid_v2_action(watermark=None)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "watermark" in str(e).lower() for e in errors
        ), f"Expected watermark error, got: {errors}"

    # --- LHP-VAL-042: watermark.column required ---

    def test_missing_watermark_column_is_error(self):
        action = self._make_valid_v2_action(
            watermark={"type": "timestamp", "source_system_id": "pg_prod", "column": ""}
        )
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "column" in str(e).lower() for e in errors
        ), f"Expected column error, got: {errors}"

    # --- LHP-VAL-043: watermark.type must be timestamp or numeric ---

    def test_invalid_watermark_type_is_error(self):
        """watermark.type: 'date' is not valid — only timestamp or numeric."""
        with pytest.raises(Exception):
            # Pydantic rejects invalid enum at model creation
            self._make_valid_v2_action(
                watermark={
                    "column": "x",
                    "type": "date",
                    "source_system_id": "pg_prod",
                }
            )

    # --- LHP-VAL-044: watermark.source_system_id required ---

    def test_missing_source_system_id_is_error(self):
        action = self._make_valid_v2_action(
            watermark={"column": "ModifiedDate", "type": "timestamp"}
        )
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "source_system_id" in str(e).lower() for e in errors
        ), f"Expected source_system_id error, got: {errors}"

    # --- LHP-VAL-045: source.schema_name required ---

    def test_missing_schema_name_is_error(self):
        source_no_schema = {
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "table_name": "Product",
        }
        action = self._make_valid_v2_action(source=source_no_schema)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "schema_name" in str(e).lower() for e in errors
        ), f"Expected schema_name error, got: {errors}"

    # --- LHP-VAL-046: source.table_name required ---

    def test_missing_table_name_is_error(self):
        source_no_table_name = {
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
        }
        action = self._make_valid_v2_action(source=source_no_table_name)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "table_name" in str(e).lower() for e in errors
        ), f"Expected table_name error, got: {errors}"

    # --- LHP-VAL-047: core JDBC fields required ---

    def test_missing_jdbc_url_is_error(self):
        source_no_url = {
            "type": "jdbc_watermark_v2",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
            "table_name": "Product",
        }
        action = self._make_valid_v2_action(source=source_no_url)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "url" in str(e).lower() for e in errors
        ), f"Expected url error, got: {errors}"

    # --- Warnings (not errors) ---

    def test_num_partitions_emits_warning_not_error(self, caplog):
        source_with_partitions = {
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
            "table_name": "Product",
            "num_partitions": 4,
        }
        action = self._make_valid_v2_action(source=source_with_partitions)
        import logging

        with caplog.at_level(logging.WARNING):
            errors = self.validator.validate(action, "test")
        assert errors == [], f"num_partitions should not produce errors, got: {errors}"
        assert any(
            "num_partitions" in r.message for r in caplog.records
        ), "Expected warning about num_partitions"

    def test_non_volumes_landing_path_emits_warning_not_error(self, caplog):
        action = self._make_valid_v2_action(
            landing_path="abfss://container@storage/path"
        )
        import logging

        with caplog.at_level(logging.WARNING):
            errors = self.validator.validate(action, "test")
        assert (
            errors == []
        ), f"Non-/Volumes/ path should not produce errors, got: {errors}"
        assert any(
            "volumes" in r.message.lower() or "/Volumes/" in r.message
            for r in caplog.records
        ), "Expected warning about non-/Volumes/ landing_path"
