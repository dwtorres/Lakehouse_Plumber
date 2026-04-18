"""Tests for the Databricks bundle verification helper."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "verify_databricks_bundle.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "verify_databricks_bundle", str(_SCRIPT_PATH)
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_script_exists() -> None:
    assert _SCRIPT_PATH.exists()


def test_resolve_profile_from_cli() -> None:
    mod = _load_script()
    assert mod.resolve_profile("my-profile") == "my-profile"


def test_resolve_profile_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = _load_script()
    monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "env-profile")
    assert mod.resolve_profile(None) == "env-profile"


def test_resolve_profile_requires_explicit_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_script()
    monkeypatch.delenv("DATABRICKS_CONFIG_PROFILE", raising=False)
    with pytest.raises(ValueError):
        mod.resolve_profile(None)


def test_discover_bundle_resources(tmp_path: Path) -> None:
    mod = _load_script()
    resources = tmp_path / "resources" / "lhp"
    resources.mkdir(parents=True)
    (resources / "orders_workflow.yml").write_text("resources:\n")
    (resources / "orders.pipeline.yml").write_text("resources:\n")

    discovered = mod.discover_bundle_resources(tmp_path)
    assert discovered["workflows"] == ["orders_workflow"]
    assert discovered["pipelines"] == ["orders.pipeline"]
