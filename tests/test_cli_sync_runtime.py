"""Tests for the `lhp sync-runtime` CLI command (ADR-002 Path 5 Option A)."""

from __future__ import annotations

import filecmp
import os
import shutil
from pathlib import Path
from typing import Iterator

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def installed_pkg_dir() -> Path:
    """Return the directory of the installed lhp_watermark package.

    Fails the test suite if lhp_watermark is not importable — that would
    mean the packaging config is broken and all sync-runtime tests are
    meaningless.
    """
    import lhp_watermark

    return Path(lhp_watermark.__file__).resolve().parent


def _run(runner: CliRunner, tmp_path: Path, *args: str):
    """Invoke the CLI with cwd set to tmp_path."""
    with runner.isolated_filesystem(temp_dir=str(tmp_path)) as td:
        result = runner.invoke(cli, ["sync-runtime", *args], catch_exceptions=False)
        return result, Path(td)


class TestSyncRuntime:
    def test_copies_to_cwd_by_default(
        self, runner: CliRunner, tmp_path: Path, installed_pkg_dir: Path
    ) -> None:
        result, cwd = _run(runner, tmp_path)
        assert result.exit_code == 0, result.output

        vendored = cwd / "lhp_watermark"
        assert vendored.is_dir(), f"{vendored} not created"
        for name in ("__init__.py", "watermark_manager.py", "exceptions.py", "runtime.py", "sql_safety.py"):
            assert (vendored / name).is_file(), f"{name} missing from vendored copy"

        # File contents match installed package
        for name in ("watermark_manager.py", "exceptions.py", "runtime.py", "sql_safety.py"):
            assert filecmp.cmp(
                installed_pkg_dir / name, vendored / name, shallow=False
            ), f"content mismatch for {name}"

        assert "lhp sync-runtime" in result.output
        assert str(vendored) in result.output

    def test_respects_dest_arg(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        result, cwd = _run(runner, tmp_path, "--dest", "runtime")
        assert result.exit_code == 0, result.output
        assert (cwd / "runtime" / "lhp_watermark" / "__init__.py").is_file()
        # Default location should NOT exist
        assert not (cwd / "lhp_watermark").exists()

    def test_excludes_pycache(
        self, runner: CliRunner, tmp_path: Path, installed_pkg_dir: Path
    ) -> None:
        # Force a __pycache__ to exist in the source by importing a submodule
        # (the interpreter running the test has already done this for us).
        result, cwd = _run(runner, tmp_path)
        assert result.exit_code == 0, result.output

        # __pycache__ directories exist in the installed package but must NOT
        # be copied into the vendored destination.
        vendored = cwd / "lhp_watermark"
        for entry in vendored.rglob("__pycache__"):
            raise AssertionError(f"__pycache__ leaked into vendored copy: {entry}")
        for entry in vendored.rglob("*.pyc"):
            raise AssertionError(f"*.pyc leaked into vendored copy: {entry}")

    def test_replaces_existing_destination(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        with runner.isolated_filesystem(temp_dir=str(tmp_path)) as td:
            vendored = Path(td) / "lhp_watermark"
            vendored.mkdir()
            (vendored / "stale.py").write_text("# stale")
            # First run overwrites the stale file and installs the fresh package
            result = runner.invoke(cli, ["sync-runtime"], catch_exceptions=False)
            assert result.exit_code == 0, result.output
            assert (vendored / "__init__.py").is_file()
            assert not (vendored / "stale.py").exists()

    def test_check_passes_when_matching(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        with runner.isolated_filesystem(temp_dir=str(tmp_path)):
            runner.invoke(cli, ["sync-runtime"], catch_exceptions=False)
            result = runner.invoke(cli, ["sync-runtime", "--check"], catch_exceptions=False)
            assert result.exit_code == 0, result.output
            assert "matches installed" in result.output

    def test_check_reports_drift(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        with runner.isolated_filesystem(temp_dir=str(tmp_path)) as td:
            runner.invoke(cli, ["sync-runtime"], catch_exceptions=False)
            # Corrupt the vendored copy to simulate drift
            vendored = Path(td) / "lhp_watermark"
            (vendored / "__init__.py").write_text("# tampered\n")
            result = runner.invoke(cli, ["sync-runtime", "--check"], catch_exceptions=False)
            assert result.exit_code == 1, result.output
            assert "drift detected" in result.output
            assert "[modified] __init__.py" in result.output

    def test_check_errors_when_missing(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        result = runner.invoke(
            cli, ["sync-runtime", "--check"], catch_exceptions=False
        )
        # No vendored copy in cwd — check should fail with exit != 0
        assert result.exit_code != 0
        assert "No vendored runtime" in result.output or "No vendored runtime" in (result.exception or "") if result.exception else True
