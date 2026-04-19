"""Tests for the Slice A Python-side overhead benchmark harness.

Task 14 per slice-A-lifecycle PLAN.md. The harness measures the
Python-side cost of the Slice A lifecycle (validators + emitters +
try/except + retry-loop branches not taken), which is what Slice A
added on top of the pre-Slice-A code. Actual cluster/Delta timing is
operator-gated and documented in BENCHMARK.md; these tests only
validate the harness surface so a CI run can prove the tool still
works and its output remains parseable.

Covers AC-SA-40 (harness side).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT_PATH = _REPO_ROOT / "scripts" / "benchmark_slice_a.py"
_SUBPROCESS_ENV = {
    **os.environ,
    "PYTHONPATH": os.pathsep.join(
        [str(_REPO_ROOT / "src"), os.environ.get("PYTHONPATH", "")]
    ),
}


def _load_harness():
    """Import the benchmark harness from scripts/ by path (not on sys.path)."""
    spec = importlib.util.spec_from_file_location(
        "benchmark_slice_a", str(_SCRIPT_PATH)
    )
    assert spec and spec.loader, f"spec load failed for {_SCRIPT_PATH}"
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestHarnessShape:
    def test_script_file_exists(self):
        assert _SCRIPT_PATH.exists(), f"missing {_SCRIPT_PATH}"

    def test_importable_exposes_run_benchmark_and_main(self):
        mod = _load_harness()
        assert hasattr(mod, "run_benchmark")
        assert hasattr(mod, "main")


class TestRunBenchmark:
    def test_returns_expected_result_keys(self):
        mod = _load_harness()
        result = mod.run_benchmark(iterations=50)
        assert "iterations" in result
        assert result["iterations"] == 50
        assert "methods" in result
        assert isinstance(result["methods"], dict)
        # One entry per instrumented lifecycle method.
        for key in (
            "insert_new",
            "mark_complete",
            "mark_failed",
            "get_latest_watermark",
        ):
            assert key in result["methods"], key
            stats = result["methods"][key]
            for stat in ("mean_us", "p50_us", "p95_us", "p99_us"):
                assert stat in stats, f"{key} missing {stat}"
                assert stats[stat] >= 0, f"{key}.{stat} negative"

    def test_verdict_pass_when_overhead_below_threshold(self):
        """Python-side overhead is dominated by in-process validators; per-call
        overhead should always fall under the 5% threshold of any realistic
        extraction time (seconds). The harness reports PASS in that case."""
        mod = _load_harness()
        # Use an assumed extraction budget of 1 second = 1_000_000 microseconds.
        # Slice A's per-lifecycle overhead measured below must be << that.
        result = mod.run_benchmark(
            iterations=100, extraction_budget_us=1_000_000
        )
        assert result["verdict"] in {"PASS", "FAIL"}
        assert result["verdict"] == "PASS", (
            f"Harness reports FAIL with generous 1s extraction budget; "
            f"overhead={result.get('total_overhead_us')}us — investigate."
        )

    def test_overhead_percent_is_reported(self):
        mod = _load_harness()
        result = mod.run_benchmark(
            iterations=50, extraction_budget_us=1_000_000
        )
        assert "overhead_pct" in result
        assert isinstance(result["overhead_pct"], (int, float))
        assert result["overhead_pct"] >= 0


class TestCLI:
    def test_cli_emits_json_when_requested(self):
        proc = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), "--iterations", "25", "--json"],
            capture_output=True,
            text=True,
            check=False,
            env=_SUBPROCESS_ENV,
        )
        assert proc.returncode == 0, proc.stderr
        payload = json.loads(proc.stdout)
        assert payload["iterations"] == 25
        assert "verdict" in payload

    def test_cli_emits_markdown_by_default(self):
        proc = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), "--iterations", "25"],
            capture_output=True,
            text=True,
            check=False,
            env=_SUBPROCESS_ENV,
        )
        assert proc.returncode == 0, proc.stderr
        out = proc.stdout
        # Table header + the four methods should all show up in markdown.
        assert "| method" in out.lower()
        for method in (
            "insert_new",
            "mark_complete",
            "mark_failed",
            "get_latest_watermark",
        ):
            assert method in out, f"{method} missing from markdown output"
        assert "verdict" in out.lower()
