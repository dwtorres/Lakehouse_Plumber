"""LHP watermark manager package.

Re-exports the public API from internal submodules so callers can continue to
use ``from lhp.extensions.watermark_manager import WatermarkManager`` unchanged.
"""

from lhp.extensions.watermark_manager._manager import WatermarkManager

__all__ = ["WatermarkManager"]
