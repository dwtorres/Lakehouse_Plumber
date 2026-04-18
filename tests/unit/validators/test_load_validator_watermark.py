"""Tests for removed jdbc_watermark validation behavior."""

import pytest

from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
from lhp.core.validators.load_validator import LoadActionValidator
from lhp.models.config import Action


@pytest.mark.unit
class TestRemovedJDBCWatermarkValidation:
    """Legacy jdbc_watermark configs should fail with a migration message."""

    def setup_method(self):
        """Set up test fixtures."""
        self.registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()
        self.validator = LoadActionValidator(self.registry, self.field_validator)

    def _make_legacy_watermark_action(self, **overrides):
        """Build a legacy jdbc_watermark Action, applying any overrides."""
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

    def test_removed_source_type_fails_validation(self):
        action = self._make_legacy_watermark_action()
        errors = self.validator.validate(action, "test")
        assert len(errors) == 1
        assert "jdbc_watermark_v2" in str(errors[0])

    def test_removed_source_type_short_circuits_other_errors(self):
        action = self._make_legacy_watermark_action(
            watermark=None,
            source={"type": "jdbc_watermark"},
        )
        errors = self.validator.validate(action, "test")
        assert len(errors) == 1
        assert "removed" in str(errors[0]).lower()

    def test_removed_source_type_from_field_validator(self):
        action = self._make_legacy_watermark_action()
        errors = self.validator.validate(action, "test")
        assert len(errors) == 1
        assert "jdbc_watermark_v2" in str(errors[0])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
