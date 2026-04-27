"""Regression guard: every LHP error-code raise site has a ref entry.

Extracts ``LHP-XXX-NNN`` tokens from non-comment lines and asserts each
appears in ``docs/errors_reference.rst``.  Prevents a recurrence of #17.

Convention: format ``LHP-{CATEGORY}-{NNN}`` with 3 decimal digits; category
must be in ``ErrorCategory`` enum.

Allowed exclusions:
- ``LHP-VAL-04B`` — dead in templates; runbook placeholder superseded by
  LHP-VAL-049.  test_validate_template.py:528-532 asserts not-rendered.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import Iterable

import pytest

_ROOT = Path(__file__).parent.parent

RAISE_SCAN_DIRS: list[Path] = [
    _ROOT / "src" / "lhp" / "templates" / "bundle",
    _ROOT / "src" / "lhp" / "templates" / "load",
    _ROOT / "src" / "lhp" / "core",
    _ROOT / "src" / "lhp" / "utils" / "error_formatter.py",
]

ERRORS_REFERENCE = _ROOT / "docs" / "errors_reference.rst"

STRICT_CODE_REGEX = re.compile(r"LHP-(?P<cat>[A-Z]+)-(?P<num>\d{3})")
LOOSE_CODE_REGEX = re.compile(r"LHP-[A-Z]+-[0-9A-Z]{3,4}")

ALPHANUM_SUFFIX_ALLOWLIST: frozenset[str] = frozenset(
    {
        # Dead in templates; runbook forward-reference placeholder superseded
        # by LHP-VAL-049.
        "LHP-VAL-04B",
    }
)


def _source_files(paths: Iterable[Path]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files.extend(p.rglob("*.py"))
            files.extend(p.rglob("*.j2"))
        elif p.is_file():
            files.append(p)
    return files


def is_comment_only(line: str) -> bool:
    return bool(re.match(r"^\s*#", line))


def discover_raised_codes(scan_dirs: list[Path]) -> set[str]:
    """Return every ``LHP-XXX-NNN`` token on a non-comment line."""
    raised: set[str] = set()
    for fpath in _source_files(scan_dirs):
        for line in fpath.read_text(encoding="utf-8").splitlines():
            if is_comment_only(line):
                continue
            for m in STRICT_CODE_REGEX.finditer(line):
                raised.add(m.group(0))
    return raised


@pytest.mark.unit
def test_all_raised_codes_have_ref_entries() -> None:
    """Every raised LHP-XXX-NNN token must appear in errors_reference.rst."""
    reference_text = ERRORS_REFERENCE.read_text(encoding="utf-8")
    raised = discover_raised_codes(RAISE_SCAN_DIRS)

    missing: list[tuple[str, str]] = []
    for code in sorted(raised):
        if code in reference_text:
            continue
        for fpath in _source_files(RAISE_SCAN_DIRS):
            text = fpath.read_text(encoding="utf-8")
            for line in text.splitlines():
                if not is_comment_only(line) and code in line:
                    missing.append((code, str(fpath.relative_to(_ROOT))))
                    break

    assert not missing, (
        "Raised codes with no entry in docs/errors_reference.rst:\n"
        + "\n".join(f"  {code}  [{fpath}]" for code, fpath in missing)
    )


@pytest.mark.unit
def test_categories_in_enum() -> None:
    """Every category prefix in raised codes must be in ErrorCategory."""
    from lhp.utils.error_formatter import ErrorCategory

    enum_values = {member.value for member in ErrorCategory}
    raised = discover_raised_codes(RAISE_SCAN_DIRS)

    unknown = {
        m.group("cat")
        for code in raised
        if (m := STRICT_CODE_REGEX.match(code)) and m.group("cat") not in enum_values
    }

    assert not unknown, (
        f"Raised codes use category prefix(es) not in ErrorCategory: {sorted(unknown)}"
    )


@pytest.mark.unit
def test_alphanumeric_suffix_violations() -> None:
    """Every LHP token in source must use exactly 3 decimal digits as suffix."""
    violations: list[tuple[str, str]] = []
    for fpath in _source_files(RAISE_SCAN_DIRS):
        for line in fpath.read_text(encoding="utf-8").splitlines():
            for m in LOOSE_CODE_REGEX.finditer(line):
                token = m.group(0)
                if token in ALPHANUM_SUFFIX_ALLOWLIST:
                    continue
                if not STRICT_CODE_REGEX.fullmatch(token):
                    violations.append((token, str(fpath.relative_to(_ROOT))))

    assert not violations, (
        "Non-standard LHP code suffix(es) found (must be exactly 3 decimal digits):\n"
        + "\n".join(f"  {tok}  [{fpath}]" for tok, fpath in violations)
    )


@pytest.mark.unit
def test_synthetic_missing_ref_fails_parity() -> None:
    """Parity check correctly detects a code absent from errors_reference.rst.

    Proves the guard would catch a real regression by using a temp file with
    a synthetic raise — no actual source files modified.
    """
    synthetic_code = "LHP-MAN-099"
    reference_text = ERRORS_REFERENCE.read_text(encoding="utf-8")
    assert synthetic_code not in reference_text, (
        f"{synthetic_code} must not exist in errors_reference.rst for this meta-test"
    )

    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "fake_raise.py"
        fake.write_text(
            f'raise RuntimeError("{synthetic_code}: synthetic test raise")\n',
            encoding="utf-8",
        )
        found = discover_raised_codes([Path(tmp)])

    assert synthetic_code in found, (
        f"discover_raised_codes failed to detect {synthetic_code} in synthetic file"
    )
    # Confirm the code is genuinely absent — parity check would fail for it.
    assert synthetic_code not in reference_text


@pytest.mark.unit
def test_synthetic_unknown_category_fails() -> None:
    """Category check correctly identifies a prefix not in ErrorCategory.

    Proves the guard works end-to-end via a temp file — no source modified.
    """
    from lhp.utils.error_formatter import ErrorCategory

    enum_values = {member.value for member in ErrorCategory}
    synthetic_prefix = "XYZ"
    synthetic_code = f"LHP-{synthetic_prefix}-001"
    assert synthetic_prefix not in enum_values

    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp) / "fake_category.py"
        fake.write_text(
            f'raise RuntimeError("{synthetic_code}: unknown category")\n',
            encoding="utf-8",
        )
        found = discover_raised_codes([Path(tmp)])

    assert synthetic_code in found
    unknown = {
        m.group("cat")
        for code in found
        if (m := STRICT_CODE_REGEX.match(code)) and m.group("cat") not in enum_values
    }
    assert synthetic_prefix in unknown, (
        f"Category check failed to flag {synthetic_prefix!r} as unknown"
    )
