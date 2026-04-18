"""White-box Jinja AST lint for SQL-context interpolations.

Task 12 (slice-A-lifecycle PLAN.md). Parses the real
``jdbc_watermark_job.py.j2`` template plus a broken fixture, walks
every Jinja ``{{ expression }}`` output node, detects whether the
expression sits in a lexical SQL context (the surrounding template
source contains INSERT/MERGE/UPDATE/SELECT/DELETE/WHERE), and verifies
every such expression is wrapped in one of the four sanctioned
emitters:

- ``sql_literal``
- ``sql_numeric_literal``
- ``sql_timestamp_literal``
- ``sql_identifier``

Covers AC-SA-14 + NFR-L-05.

Contrast with the lightweight textual guard in
``test_jdbc_watermark_template.py``. The textual guard rejects any
``{{ ... }}`` sharing a line with a SQL keyword regardless of wrap; this
lint is AST-aware and will accept a properly wrapped emitter call on a
SELECT line if one is ever introduced.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple

from jinja2 import Environment, nodes

_TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "src" / "lhp" / "templates"
_REAL_TEMPLATE = _TEMPLATE_DIR / "load" / "jdbc_watermark_job.py.j2"
_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
_BROKEN_FIXTURE = _FIXTURE_DIR / "broken_template.j2"

EMITTER_NAMES = frozenset(
    {
        "sql_literal",
        "sql_numeric_literal",
        "sql_timestamp_literal",
        "sql_identifier",
    }
)

# Lexical SQL-context signal. Word-boundary to avoid accidental matches
# on identifiers that happen to contain these substrings (e.g. "inserted").
_SQL_KEYWORDS = re.compile(
    r"\b(INSERT|MERGE|UPDATE|SELECT|DELETE|WHERE)\b",
    re.IGNORECASE,
)

# Pure-comment line (Python # at start, possibly indented). A Jinja
# expression on a pure-comment line is never in SQL lexical context
# even when the comment mentions SQL.
_COMMENT_ONLY = re.compile(r"^\s*#")


def _jinja_env() -> Environment:
    # Mirror the generator's env — ``tojson`` is a registered filter so
    # filter-wrapped expressions (``{{ x|tojson }}``) parse cleanly.
    env = Environment(trim_blocks=True, lstrip_blocks=True)
    env.filters["tojson"] = json.dumps
    return env


def _line_in_sql_context(lines: List[str], lineno: int) -> bool:
    """Return True when template line (1-indexed) sits in SQL lexical context.

    The check scans a 3-line window (the line itself + one before + one
    after) after stripping pure-comment lines, then looks for a SQL
    keyword. Window, not single-line, so a keyword on the line
    immediately above a substitution is still caught.
    """
    idx = lineno - 1
    if idx < 0 or idx >= len(lines):
        return False
    if _COMMENT_ONLY.match(lines[idx]):
        return False
    window: List[str] = []
    for j in (idx - 1, idx, idx + 1):
        if 0 <= j < len(lines) and not _COMMENT_ONLY.match(lines[j]):
            window.append(lines[j])
    return bool(_SQL_KEYWORDS.search("\n".join(window)))


def _is_emitter_wrapped(expr: nodes.Node) -> bool:
    """Return True when ``expr`` is ultimately a call to one of the four emitters.

    Unwraps a leading ``Filter`` so ``{{ sql_literal(x)|some_filter }}``
    still counts as wrapped — the sanctioning guarantee is that the
    raw value passed through an emitter before reaching the output.
    """
    node = expr
    # Unwrap nested filters: `x|a|b` is Filter(node=Filter(node=Name('x'),
    # name='a'), name='b'). Keep descending until we hit something that
    # is not a filter.
    while isinstance(node, nodes.Filter):
        node = node.node
    if isinstance(node, nodes.Call):
        func = node.node
        if isinstance(func, nodes.Name) and func.name in EMITTER_NAMES:
            return True
    return False


def _lint_source(source: str) -> List[Tuple[int, str, str]]:
    """Return a list of ``(lineno, source-line, expr-kind)`` problems."""
    lines = source.splitlines()
    env = _jinja_env()
    tree = env.parse(source)
    problems: List[Tuple[int, str, str]] = []
    for output in tree.find_all(nodes.Output):
        for child in output.nodes:
            if isinstance(child, nodes.TemplateData):
                continue
            lineno = child.lineno
            if not _line_in_sql_context(lines, lineno):
                continue
            if _is_emitter_wrapped(child):
                continue
            line = lines[lineno - 1] if 0 < lineno <= len(lines) else "<out-of-range>"
            problems.append((lineno, line, type(child).__name__))
    return problems


def _lint_path(path: Path) -> List[Tuple[int, str, str]]:
    return _lint_source(path.read_text())


# ---------------------- real template ---------------------------------


class TestRealTemplate:
    def test_real_template_has_no_unwrapped_sql_expressions(self):
        """AC-SA-14: production template passes the AST lint."""
        problems = _lint_path(_REAL_TEMPLATE)
        assert problems == [], (
            "jdbc_watermark_job.py.j2 has unwrapped Jinja expressions in "
            "SQL lexical context. Wrap each value with sql_literal / "
            "sql_numeric_literal / sql_timestamp_literal / sql_identifier, "
            "or move the substitution off the SQL keyword line.\n"
            + "\n".join(
                f"  line {lno}: {line!r} ({kind})"
                for lno, line, kind in problems
            )
        )


# ---------------------- broken fixture --------------------------------


class TestBrokenFixture:
    def test_broken_fixture_is_caught(self):
        """NFR-L-05: seeded bare interpolation inside SQL keyword line fails lint."""
        problems = _lint_path(_BROKEN_FIXTURE)
        assert problems, (
            "Broken fixture was not flagged. The lint is a false-negative; "
            "the SQL-context detection or wrapping check regressed."
        )

    def test_broken_fixture_flags_every_offending_line(self):
        """Every seeded offender must be caught with an actionable line."""
        problems = _lint_path(_BROKEN_FIXTURE)
        offenders = {
            "table_name",
            "user_id",
            "new_value",
        }
        flagged_lines = " ".join(line for _, line, _ in problems)
        missing = {name for name in offenders if name not in flagged_lines}
        assert not missing, (
            f"Lint missed offenders {missing}. Problems reported: {problems!r}"
        )

    def test_broken_fixture_reports_actionable_locations(self):
        """Every reported problem carries a positive line number + source snippet."""
        problems = _lint_path(_BROKEN_FIXTURE)
        assert problems  # guarded above; asserted again for isolation
        for lno, line, kind in problems:
            assert lno > 0, f"non-positive lineno: {lno}"
            assert line.strip(), f"empty source line on {lno}"
            assert kind, f"empty expression kind on line {lno}"


# ---------------------- emitter wrap acceptance ------------------------


class TestEmitterWrapAcceptance:
    """The lint must NOT flag expressions wrapped in a sanctioned emitter."""

    def test_sql_literal_in_select_is_accepted(self):
        wrapped = 'SELECT * FROM t WHERE name = {{ sql_literal(user_name) }}\n'
        assert _lint_source(wrapped) == []

    def test_sql_identifier_in_select_is_accepted(self):
        wrapped = "SELECT {{ sql_identifier(col) }} FROM t\n"
        assert _lint_source(wrapped) == []

    def test_sql_timestamp_literal_in_where_is_accepted(self):
        wrapped = "UPDATE t SET c=1 WHERE ts = {{ sql_timestamp_literal(ts) }}\n"
        assert _lint_source(wrapped) == []

    def test_sql_numeric_literal_in_merge_is_accepted(self):
        wrapped = "MERGE INTO t USING s ON s.id = {{ sql_numeric_literal(rid) }}\n"
        assert _lint_source(wrapped) == []

    def test_non_sql_lines_not_required_to_wrap(self):
        """A bare substitution on a non-SQL line passes without wrapping."""
        non_sql = 'catalog = {{ wm_catalog|tojson }}\n'
        assert _lint_source(non_sql) == []

    def test_commented_sql_line_is_not_flagged(self):
        """Comments that happen to mention SQL keywords don't force wrapping."""
        commented = "# SELECT is fine in a comment\nvalue = {{ some_value|tojson }}\n"
        assert _lint_source(commented) == []
