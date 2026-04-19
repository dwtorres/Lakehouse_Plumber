"""Sync-runtime command: vendor the installed lhp_watermark package into a bundle.

Automates ADR-002 Path 5 Option A vendoring. Copies the installed
``lhp_watermark`` package's source tree (located via its ``__file__``)
into ``<dest>/lhp_watermark/`` so that ``databricks bundle deploy`` picks
it up alongside the user bundle's generated notebooks.

Typical use, from a Databricks Asset Bundle root:

    $ lhp sync-runtime          # copies to ./lhp_watermark
    $ lhp sync-runtime --check  # report whether ./lhp_watermark matches installed version (exit 1 on drift)
    $ lhp sync-runtime --dest runtime/  # copies to ./runtime/lhp_watermark
"""

from __future__ import annotations

import filecmp
import logging
import shutil
from pathlib import Path
from typing import List, Optional, Tuple

import click

from .base_command import BaseCommand

logger = logging.getLogger(__name__)

_IGNORED_PATTERNS = shutil.ignore_patterns(
    "__pycache__",
    "*.pyc",
    "*.pyo",
    ".mypy_cache",
    ".pytest_cache",
)


class SyncRuntimeCommand(BaseCommand):
    """Vendor the installed lhp_watermark package into a user bundle."""

    def execute(
        self,
        dest: Optional[str] = None,
        check: bool = False,
    ) -> None:
        """Execute the sync-runtime command.

        Args:
            dest: Destination directory. Defaults to the current working
                directory; the package lands at ``<dest>/lhp_watermark``.
            check: When True, report whether the destination matches the
                installed package without copying. Exits nonzero on drift.
        """
        self.setup_from_context()

        source = self._locate_installed_source()
        dest_root = Path(dest).resolve() if dest else Path.cwd().resolve()
        target = dest_root / "lhp_watermark"

        if check:
            self._run_check(source, target)
            return

        self._run_sync(source, target)

    # ------------------------------------------------------------------

    @staticmethod
    def _locate_installed_source() -> Path:
        """Return the directory of the installed ``lhp_watermark`` package.

        Raises ``click.ClickException`` with actionable guidance if the
        package is not importable.
        """
        try:
            import lhp_watermark  # type: ignore[import-not-found]
        except ImportError as exc:
            raise click.ClickException(
                "Could not import `lhp_watermark`. Install or upgrade the LHP "
                "distribution that contains it:\n"
                "    pip install --upgrade lakehouse-plumber\n"
                f"Original import error: {exc}"
            ) from exc

        source_file = getattr(lhp_watermark, "__file__", None)
        if not source_file:
            raise click.ClickException(
                "`lhp_watermark` is importable but has no file location "
                "(namespace package?). sync-runtime requires a concrete "
                "source directory."
            )

        source_dir = Path(source_file).resolve().parent
        if not source_dir.is_dir():
            raise click.ClickException(
                f"Resolved lhp_watermark source path is not a directory: {source_dir}"
            )
        return source_dir

    # ------------------------------------------------------------------

    def _run_sync(self, source: Path, target: Path) -> None:
        """Copy ``source`` into ``target`` (replacing if it exists)."""
        if source == target:
            raise click.ClickException(
                f"Refusing to sync: destination is the installed package itself ({source}). "
                "Pass --dest to choose a different directory."
            )

        action = "replacing" if target.exists() else "creating"
        if target.exists():
            shutil.rmtree(target)

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, target, ignore=_IGNORED_PATTERNS)

        click.echo(f"lhp sync-runtime: {action} {target}")
        click.echo(f"  source: {source}")
        click.echo("  excluded: __pycache__, *.pyc, *.pyo, .mypy_cache, .pytest_cache")

        # Emit a gentle nudge if we are not obviously inside a DAB bundle
        dab_yaml = target.parent / "databricks.yml"
        if not dab_yaml.exists():
            click.echo(
                f"  note: no databricks.yml at {target.parent}; "
                "run sync-runtime from your bundle root for DAB workspace-file sync to pick it up."
            )

    # ------------------------------------------------------------------

    def _run_check(self, source: Path, target: Path) -> None:
        """Compare ``target`` against ``source`` and report drift."""
        if not target.exists():
            raise click.ClickException(
                f"No vendored runtime found at {target}. Run `lhp sync-runtime` "
                "from the bundle root to create it."
            )

        differences = _diff_trees(source, target)
        if not differences:
            click.echo(f"lhp sync-runtime: {target} matches installed lhp_watermark ({source})")
            return

        click.echo(f"lhp sync-runtime: drift detected ({len(differences)} entries):")
        for kind, path in differences:
            click.echo(f"  [{kind}] {path}")
        click.echo("Run `lhp sync-runtime` (without --check) to refresh.")
        raise click.exceptions.Exit(code=1)


# ----------------------------------------------------------------------


def _diff_trees(source: Path, target: Path) -> List[Tuple[str, str]]:
    """Return a list of (kind, relpath) entries where kind is one of
    ``"missing"``, ``"extra"``, or ``"modified"``. Comparison ignores
    names matched by ``_IGNORED_PATTERNS`` (mirrors the sync behavior).
    """
    def _walk(path: Path) -> List[str]:
        ignore_names = {
            match
            for match in _IGNORED_PATTERNS(
                path, [p.name for p in path.iterdir()]
            )
        }
        rels: List[str] = []
        for entry in path.iterdir():
            if entry.name in ignore_names:
                continue
            rel = entry.name
            if entry.is_dir():
                rels.append(rel + "/")
                for sub in _walk(entry):
                    rels.append(f"{rel}/{sub}")
            else:
                rels.append(rel)
        return sorted(rels)

    src_entries = set(_walk(source))
    tgt_entries = set(_walk(target))

    differences: List[Tuple[str, str]] = []
    for rel in sorted(src_entries - tgt_entries):
        differences.append(("missing", rel))
    for rel in sorted(tgt_entries - src_entries):
        differences.append(("extra", rel))
    for rel in sorted(src_entries & tgt_entries):
        if rel.endswith("/"):
            continue
        src_file = source / rel
        tgt_file = target / rel
        if not filecmp.cmp(src_file, tgt_file, shallow=False):
            differences.append(("modified", rel))
    return differences
