"""Structural tests for the lhp.extensions.watermark_manager package.

Task 1 of Slice A lifecycle hardening: convert monolithic
src/lhp/extensions/watermark_manager.py into a package so later tasks can
add submodules (sql_safety, runtime, migrations, exceptions). Public API
must be preserved via re-exports from __init__.py.

These tests assert only structure and identity — not behaviour. Behavioural
regression is covered by tests/test_jdbc_watermark_v2_integration.py.
"""

from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path


def test_watermark_manager_is_a_package() -> None:
    """__init__.py must exist and the module must report package-style origin."""
    spec = importlib.util.find_spec("lhp.extensions.watermark_manager")
    assert spec is not None, "lhp.extensions.watermark_manager is not importable"
    assert spec.submodule_search_locations is not None, (
        "lhp.extensions.watermark_manager is not a package "
        "(submodule_search_locations is None — it is still a single-file module)"
    )
    origin = Path(spec.origin or "")
    assert (
        origin.name == "__init__.py"
    ), f"expected package __init__.py origin, got {origin}"


def test_manager_submodule_holds_class_body() -> None:
    """The class body must live in a dedicated _manager submodule."""
    submodule = importlib.import_module("lhp.extensions.watermark_manager._manager")
    assert hasattr(
        submodule, "WatermarkManager"
    ), "_manager submodule must define WatermarkManager"


def test_public_watermark_manager_reexport_identity() -> None:
    """The public import path must resolve to the same class object as the submodule."""
    public = importlib.import_module("lhp.extensions.watermark_manager")
    internal = importlib.import_module("lhp.extensions.watermark_manager._manager")
    assert public.WatermarkManager is internal.WatermarkManager, (
        "public WatermarkManager must be the same object as _manager.WatermarkManager "
        "(re-export, not a shadow class)"
    )


def test_public_import_surface_unchanged() -> None:
    """No regression: callers that import WatermarkManager at the package level continue to work."""
    from lhp.extensions.watermark_manager import WatermarkManager  # noqa: F401

    assert WatermarkManager.__name__ == "WatermarkManager"
