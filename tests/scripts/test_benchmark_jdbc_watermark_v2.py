"""Tests for the jdbc_watermark_v2 generation benchmark harness."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "benchmark_jdbc_watermark_v2.py"
)


def _load_harness():
    spec = importlib.util.spec_from_file_location(
        "benchmark_jdbc_watermark_v2", str(_SCRIPT_PATH)
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_script_file_exists() -> None:
    assert _SCRIPT_PATH.exists()


def test_run_benchmark_returns_expected_keys() -> None:
    mod = _load_harness()
    result = mod.run_benchmark(iterations=10)
    assert result["iterations"] == 10
    assert set(result["artifacts"]) == {
        "load_generator",
        "workflow_generator",
        "combined_generation",
    }


def test_cli_emits_json() -> None:
    proc = subprocess.run(
        [sys.executable, str(_SCRIPT_PATH), "--iterations", "10", "--json"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(proc.stdout)
    assert payload["iterations"] == 10


def test_cli_emits_markdown() -> None:
    proc = subprocess.run(
        [sys.executable, str(_SCRIPT_PATH), "--iterations", "10"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    assert "| artifact |" in out
    assert "load_generator" in out
    assert "workflow_generator" in out
