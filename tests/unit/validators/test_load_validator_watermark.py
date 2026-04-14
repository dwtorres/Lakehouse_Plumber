"""Tests for JDBC watermark validation rules in LoadActionValidator."""

import pytest

from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
from lhp.core.validators.load_validator import LoadActionValidator
from lhp.models.config import Action


@pytest.mark.unit
class TestJDBCWatermarkValidation:
    """Test suite for JDBC watermark source validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()
        self.validator = LoadActionValidator(self.registry, self.field_validator)

    def _make_valid_watermark_action(self, **overrides):
        """Build a valid jdbc_watermark Action, applying any overrides."""
        source = {
            "type": "jdbc_watermark",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"schema"."table"',
        }
        watermark = {"column": "modified_date", "type": "timestamp"}
        kwargs = {
            "name": "load_test_jdbc",
            "type": "load",
            "source": source,
            "target": "v_test_raw",
            "watermark": watermark,
        }
        kwargs.update(overrides)
        return Action(**kwargs)

    def test_valid_jdbc_watermark_passes_validation(self):
        """A fully valid jdbc_watermark action should produce zero errors."""
        action = self._make_valid_watermark_action()
        errors = self.validator.validate(action, "test")
        assert errors == [], f"Expected no errors but got: {errors}"

    def test_missing_watermark_section(self):
        """Omitting the watermark section on a jdbc_watermark source is an error."""
        action = self._make_valid_watermark_action(watermark=None)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "watermark" in str(e).lower() for e in errors
        ), f"Expected watermark-related error, got: {errors}"

    def test_missing_watermark_column(self):
        """An empty watermark column should be rejected."""
        try:
            action = self._make_valid_watermark_action(
                watermark={"column": "", "type": "timestamp"}
            )
        except Exception:
            # Pydantic may reject empty column at model creation time,
            # which is acceptable — the constraint is enforced either way.
            return

        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "column" in str(e).lower() for e in errors
        ), f"Expected column-related error, got: {errors}"

    def test_missing_jdbc_url(self):
        """Omitting the JDBC url should produce a validation error."""
        source_without_url = {
            "type": "jdbc_watermark",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"schema"."table"',
        }
        action = self._make_valid_watermark_action(source=source_without_url)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "url" in str(e).lower() for e in errors
        ), f"Expected url-related error, got: {errors}"

    def test_missing_jdbc_driver(self):
        """Omitting the JDBC driver should produce a validation error."""
        source_without_driver = {
            "type": "jdbc_watermark",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "table": '"schema"."table"',
        }
        action = self._make_valid_watermark_action(source=source_without_driver)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "driver" in str(e).lower() for e in errors
        ), f"Expected driver-related error, got: {errors}"

    def test_missing_jdbc_table(self):
        """Omitting both table and query should produce a validation error."""
        source_without_table = {
            "type": "jdbc_watermark",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
        }
        action = self._make_valid_watermark_action(source=source_without_table)
        errors = self.validator.validate(action, "test")
        assert len(errors) > 0
        assert any(
            "table" in str(e).lower() or "query" in str(e).lower()
            for e in errors
        ), f"Expected table/query-related error, got: {errors}"

    def test_standard_jdbc_unaffected_by_watermark(self):
        """A standard jdbc source with a watermark field should not trigger watermark validation."""
        action = Action(
            name="load_test_jdbc",
            type="load",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "test_user",
                "password": "test_pass",
                "driver": "org.postgresql.Driver",
                "table": '"schema"."table"',
            },
            target="v_test_raw",
            watermark={"column": "modified_date", "type": "timestamp"},
        )
        errors = self.validator.validate(action, "test")
        # Standard JDBC validation should pass; watermark section is simply ignored
        assert errors == [], f"Expected no errors for standard jdbc, got: {errors}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
