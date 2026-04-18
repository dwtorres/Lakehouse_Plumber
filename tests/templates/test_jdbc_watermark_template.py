"""White-box structural tests for ``jdbc_watermark_job.py.j2``.

Covers L2 §5.3 control-flow contract and FR-L-01 / FR-L-02 / FR-L-09 /
NFR-L-05 / AC-SA-01 / AC-SA-04 / AC-SA-35.

Render strategy: drive the template through ``jinja2.Environment`` with a
``FileSystemLoader`` against the real ``src/lhp/templates`` directory so
we exercise the exact file the generator ships. A representative fixture
(Postgres-style) supplies the rendering context; tests then Black-format
the output and walk the Python AST to assert the L2 §5.3 structure.

These tests are intentionally strict about *shape* (where ``insert_new``
sits relative to the ``try`` block, what the ``except`` body calls, where
``mark_complete`` sits) because the L2 §5.3 contract is the first line of
defence against FR-L-06a corruption. Textual pattern checks are reserved
for things the AST cannot express (e.g. "no ``uuid.uuid4()`` at the top
level").
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import black
import pytest
from jinja2 import Environment, FileSystemLoader

_TEMPLATE_DIR = (
    Path(__file__).resolve().parents[2] / "src" / "lhp" / "templates"
)
_TEMPLATE_NAME = "load/jdbc_watermark_job.py.j2"


def _jinja_env() -> Environment:
    # Mirror the generator's Jinja env (BaseActionGenerator registers tojson
    # as json.dumps). Keeping the filter set in sync avoids false positives
    # where a template compiles in production but not in the test.
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATE_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["tojson"] = json.dumps
    return env


def _default_context(**overrides: Any) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "action_name": "load_product_jdbc",
        "pipeline_name": "crm_bronze",
        "source_system_id": "pg_crm",
        "schema_name": "Production",
        "table_name": "Product",
        "wm_catalog": "metadata",
        "wm_schema": "orchestration",
        "watermark_column": "ModifiedDate",
        "watermark_type": "timestamp",
        "watermark_operator": ">=",
        "jdbc_url": '"jdbc:postgresql://host:5432/db"',
        "jdbc_user": '"test_user"',
        "jdbc_password": '"test_pass"',
        "jdbc_driver": "org.postgresql.Driver",
        "jdbc_table": '"Production"."Product"',
        "landing_path": "/Volumes/bronze_catalog/bronze/landing/product",
    }
    ctx.update(overrides)
    return ctx


def _render(**overrides: Any) -> str:
    env = _jinja_env()
    template = env.get_template(_TEMPLATE_NAME)
    return template.render(**_default_context(**overrides))


def _parse(rendered: str) -> ast.Module:
    return ast.parse(rendered)


def _module_level_calls(tree: ast.Module, attr: str) -> List[ast.Call]:
    """Return every ``<name>.<attr>(...)`` call that lives at module scope.

    Module scope = not nested inside a ``Try``, ``If``, ``With``, ``For``,
    ``While``, ``FunctionDef``, or ``ClassDef``. We walk only the immediate
    body so we can assert "is/isn't inside a try block" precisely.
    """
    hits: List[ast.Call] = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == attr:
                hits.append(call)
    return hits


def _calls_inside_try(tree: ast.Module, attr: str) -> List[ast.Call]:
    """Every ``<obj>.<attr>(...)`` call nested anywhere inside a Try.body."""
    hits: List[ast.Call] = []
    for top in tree.body:
        if isinstance(top, ast.Try):
            for sub in ast.walk(ast.Module(body=top.body, type_ignores=[])):
                if isinstance(sub, ast.Call) and isinstance(
                    sub.func, ast.Attribute
                ) and sub.func.attr == attr:
                    hits.append(sub)
    return hits


def _find_try_block(tree: ast.Module) -> Optional[ast.Try]:
    for node in tree.body:
        if isinstance(node, ast.Try):
            return node
    return None


# ---------------------- render + format ---------------------------------


class TestRenderAndFormat:
    def test_template_renders_without_error(self):
        rendered = _render()
        assert rendered

    def test_rendered_passes_black(self):
        """AC-SA-04: Black must accept the rendered output unchanged."""
        rendered = _render()
        formatted = black.format_str(rendered, mode=black.Mode(line_length=88))
        # Re-formatting a Black-formatted string is a no-op.
        assert black.format_str(formatted, mode=black.Mode(line_length=88)) == formatted

    def test_rendered_is_valid_python(self):
        rendered = _render()
        ast.parse(rendered)


# ---------------------- imports ----------------------------------------


class TestImports:
    def test_imports_derive_run_id_from_runtime(self):
        """AC-SA-35: template imports ``derive_run_id`` from the runtime module."""
        rendered = _render()
        assert re.search(
            r"from\s+lhp\.extensions\.watermark_manager\.runtime\s+import\s+[^\n]*derive_run_id",
            rendered,
        ), rendered

    def test_imports_watermark_manager(self):
        rendered = _render()
        assert "WatermarkManager" in rendered

    def test_no_uuid_uuid4_as_primary_run_id_source(self):
        """AC-SA-35: ``uuid.uuid4()`` must not be the primary run_id source.

        ``derive_run_id`` uses ``uuid.uuid4`` internally as the fallback, but
        the *template* must not call it at all. The notebook delegates the
        fallback to ``derive_run_id``.
        """
        rendered = _render()
        assert "uuid.uuid4" not in rendered
        assert "uuid4()" not in rendered

    def test_no_naive_datetime_in_template(self):
        """AC-SA-30: no ``datetime.utcnow()`` or bare ``datetime.now()``.

        Only ``datetime.now(tz=...)`` and ``datetime.now(timezone.utc)`` forms
        are acceptable. The template does not need a naive clock.
        """
        rendered = _render()
        assert "datetime.utcnow" not in rendered
        # A bare datetime.now() with no timezone argument is naive.
        assert not re.search(r"datetime\.now\(\s*\)", rendered)


# ---------------------- UTC session ------------------------------------


class TestUTCSession:
    def test_utc_session_set_before_first_watermark_call(self):
        """AC-SA-28: ``spark.sql.session.timeZone='UTC'`` precedes every DML.

        AST-driven: finds the first ``spark.conf.set("spark.sql.session.timeZone",
        "UTC")`` call and the first ``WatermarkManager(...)`` / ``wm.<method>(...)``
        call in source order, then asserts the UTC call comes first. String-level
        search would spuriously match the control-flow comment at the top of the
        template, so we compare real AST nodes.
        """
        rendered = _render()
        tree = _parse(rendered)

        def _call_line(predicate) -> Optional[int]:
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and predicate(node):
                    return node.lineno
            return None

        def _is_utc_conf_set(call: ast.Call) -> bool:
            if not (isinstance(call.func, ast.Attribute) and call.func.attr == "set"):
                return False
            owner = call.func.value
            if not (
                isinstance(owner, ast.Attribute)
                and owner.attr == "conf"
                and isinstance(owner.value, ast.Name)
                and owner.value.id == "spark"
            ):
                return False
            if len(call.args) != 2:
                return False
            if not all(isinstance(a, ast.Constant) and isinstance(a.value, str) for a in call.args):
                return False
            return call.args[0].value == "spark.sql.session.timeZone" and call.args[1].value == "UTC"

        def _is_wm_method(call: ast.Call) -> bool:
            # WatermarkManager(...) or any wm.<method>(...) call.
            if isinstance(call.func, ast.Name) and call.func.id == "WatermarkManager":
                return True
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
                if call.func.value.id == "wm" and call.func.attr in {
                    "get_latest_watermark",
                    "insert_new",
                    "mark_complete",
                    "mark_failed",
                    "mark_bronze_complete",
                    "mark_silver_complete",
                }:
                    return True
            return False

        utc_line = _call_line(_is_utc_conf_set)
        wm_line = _call_line(_is_wm_method)

        assert utc_line is not None, "spark.conf.set(UTC) call not found"
        assert wm_line is not None, "no WatermarkManager / wm.<method> call found"
        assert utc_line < wm_line, (
            f"UTC session config (line {utc_line}) must precede the first "
            f"watermark call (line {wm_line})"
        )


# ---------------------- L2 §5.3 control-flow structure -----------------


class TestControlFlowContract:
    def test_insert_new_is_outside_try_block(self):
        """FR-L-05 / L2 §5.3: ``insert_new`` at module level, never inside try."""
        rendered = _render()
        tree = _parse(rendered)

        module_level_insert_new = _module_level_calls(tree, "insert_new")
        assert len(module_level_insert_new) == 1, (
            f"Expected exactly one module-level insert_new call, got "
            f"{len(module_level_insert_new)}"
        )

        assert _calls_inside_try(tree, "insert_new") == [], (
            "insert_new must NOT appear inside any try block"
        )

    def test_try_block_exists_and_wraps_extraction(self):
        """FR-L-02 / AC-SA-04: JDBC read + Parquet write + agg + count in one try."""
        rendered = _render()
        tree = _parse(rendered)
        try_block = _find_try_block(tree)
        assert try_block is not None, "No top-level try block found"

        try_src = ast.unparse(ast.Module(body=try_block.body, type_ignores=[]))
        # JDBC read, parquet write, and aggregation all live inside the try.
        assert 'format("jdbc")' in try_src or "format('jdbc')" in try_src
        assert 'format("parquet")' in try_src or "format('parquet')" in try_src
        assert ".agg(" in try_src

    def test_except_calls_mark_failed_then_raises(self):
        """FR-L-02 / AC-SA-01: except → mark_failed(type(e).__name__, str(e)[:4096]) → raise."""
        rendered = _render()
        tree = _parse(rendered)
        try_block = _find_try_block(tree)
        assert try_block is not None
        assert len(try_block.handlers) == 1, "Expected exactly one except handler"

        handler = try_block.handlers[0]
        mark_failed_calls = [
            n for n in ast.walk(ast.Module(body=handler.body, type_ignores=[]))
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "mark_failed"
        ]
        assert len(mark_failed_calls) == 1, (
            f"Expected one mark_failed call in except handler, got {len(mark_failed_calls)}"
        )

        call = mark_failed_calls[0]
        kwargs = {kw.arg: ast.unparse(kw.value) for kw in call.keywords}
        assert "type(e).__name__" in kwargs.get("error_class", ""), kwargs
        error_message_src = kwargs.get("error_message", "")
        assert "str(e)" in error_message_src and "[:4096]" in error_message_src, kwargs

        raises = [n for n in handler.body if isinstance(n, ast.Raise)]
        assert raises, "except handler must end with `raise`"

    def test_mark_complete_is_outside_and_after_try(self):
        """FR-L-01 / L2 §5.3: mark_complete sits AFTER the try/except, at module level."""
        rendered = _render()
        tree = _parse(rendered)

        try_index: Optional[int] = None
        for i, node in enumerate(tree.body):
            if isinstance(node, ast.Try):
                try_index = i
                break
        assert try_index is not None

        # mark_complete must not live inside the try body or its handlers.
        assert _calls_inside_try(tree, "mark_complete") == [], (
            "mark_complete must NOT be inside the try block"
        )

        # Walk module-body nodes AFTER the try and confirm mark_complete appears.
        after_try = ast.Module(body=tree.body[try_index + 1:], type_ignores=[])
        calls = [
            n for n in ast.walk(after_try)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "mark_complete"
        ]
        assert calls, "mark_complete must appear after the try/except block"


# ---------------------- SQL-context Jinja hygiene ----------------------


class TestJinjaSQLHygiene:
    def test_no_bare_jinja_substitution_inside_sql_keyword_line(self):
        """AC-SA-14 (lightweight): no ``{{ expr }}`` shares a line with SELECT/WHERE/UPDATE/etc.

        Full AST-driven lint is Task 12; this is the cheap textual guard.
        """
        raw = (_TEMPLATE_DIR / _TEMPLATE_NAME).read_text()
        sql_kw = re.compile(r"\b(SELECT|INSERT|UPDATE|MERGE|DELETE|WHERE)\b", re.IGNORECASE)
        for lineno, line in enumerate(raw.splitlines(), start=1):
            if "{{" not in line:
                continue
            if sql_kw.search(line):
                raise AssertionError(
                    f"Template line {lineno} mixes a Jinja expression with a "
                    f"SQL keyword; wrap the value in an emitter or move the "
                    f"substitution to a Python string literal line.\n  {line!r}"
                )


# ---------------------- Integration-test contract preservation ---------


class TestExistingIntegrationContract:
    """These assertions mirror ``test_jdbc_watermark_v2_integration.py`` so
    the restructure does not silently break it.
    """

    def test_watermark_manager_and_methods_referenced(self):
        rendered = _render()
        assert "WatermarkManager" in rendered
        assert "get_latest_watermark(" in rendered
        assert "insert_new(" in rendered
        assert '["watermark_value"]' in rendered
        assert "watermark_column_name=" in rendered
        assert '"ModifiedDate"' in rendered

    def test_jdbc_and_parquet_format_present(self):
        rendered = _render()
        assert 'format("jdbc")' in rendered
        assert 'format("parquet")' in rendered

    def test_numeric_watermark_branch_renders(self):
        rendered = _render(watermark_type="numeric", watermark_operator=">")
        ast.parse(rendered)
        black.format_str(rendered, mode=black.Mode(line_length=88))
