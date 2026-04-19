"""LHP watermark manager package.

Re-exports the public API from internal submodules so callers can continue to
use ``from lhp.extensions.watermark_manager import WatermarkManager`` unchanged.
"""

from lhp.extensions.watermark_manager._manager import WatermarkManager
from lhp.extensions.watermark_manager.exceptions import (
    DuplicateRunError,
    TerminalStateGuardError,
    WatermarkConcurrencyError,
    WatermarkValidationError,
)
from lhp.extensions.watermark_manager.runtime import derive_run_id
from lhp.extensions.watermark_manager.sql_safety import (
    SQLInputValidator,
    sql_identifier,
    sql_literal,
    sql_numeric_literal,
    sql_timestamp_literal,
)

__all__ = [
    "WatermarkManager",
    "DuplicateRunError",
    "TerminalStateGuardError",
    "WatermarkValidationError",
    "WatermarkConcurrencyError",
    "SQLInputValidator",
    "sql_literal",
    "sql_numeric_literal",
    "sql_timestamp_literal",
    "sql_identifier",
    "derive_run_id",
]
