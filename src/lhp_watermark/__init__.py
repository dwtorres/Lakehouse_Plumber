"""LHP watermark manager package.

Re-exports the public API from internal submodules so callers can continue to
use ``from lhp_watermark import WatermarkManager`` unchanged.
"""

from lhp_watermark.watermark_manager import WatermarkManager
from lhp_watermark.exceptions import (
    DuplicateRunError,
    TerminalStateGuardError,
    WatermarkConcurrencyError,
    WatermarkValidationError,
)
from lhp_watermark.runtime import derive_run_id
from lhp_watermark.sql_safety import (
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
