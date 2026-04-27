"""Cross-template contract test for the B2 iteration payload.

Renders ``prepare_manifest.py.j2`` and ``jdbc_watermark_job.py.j2`` from a
common fixture and asserts that every iteration kwarg the worker reads is in
fact emitted by prepare_manifest. This is the regression net for Anomaly A
(devtest 2026-04-26): the worker hardcoded action[0]'s ``jdbc_table`` /
``landing_path`` / ``watermark_column`` literals while iteration kwargs only
carried 7 of the 10 fields the worker needs.

The contract is one-directional and explicit:

    set(worker reads via iteration[…]) ⊆ set(prepare_manifest emits)

The frozen reference set lives in ``lhp.models.b2_iteration.B2_ITERATION_KEYS``
so both halves can never silently drift again.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest
from jinja2 import Environment, FileSystemLoader


_BUNDLE_DIR = Path(__file__).parent.parent / "src" / "lhp" / "templates" / "bundle"
_LOAD_DIR = Path(__file__).parent.parent / "src" / "lhp" / "templates" / "load"


def _render_prepare(actions: List[Dict[str, str]]) -> str:
    env = Environment(loader=FileSystemLoader(str(_BUNDLE_DIR)))
    tmpl = env.get_template("prepare_manifest.py.j2")
    return tmpl.render(
        wm_catalog="metadata",
        wm_schema="devtest_orchestration",
        pipeline_name="test_pipeline",
        flowgroup_name="test_fg",
        actions=actions,
    )


def _render_worker(**ctx: Any) -> str:
    env = Environment(loader=FileSystemLoader(str(_LOAD_DIR)))
    tmpl = env.get_template("jdbc_watermark_job.py.j2")
    base = {
        "execution_mode": "for_each",
        "watermark_type": "timestamp",
        "watermark_operator": ">",
        "source_system_id": "pg_prod",
        "schema_name": "Sales",
        "table_name": "Orders",
        "action_name": "load_orders",
        "load_group": "test_pipeline::test_fg",
        "wm_catalog": "metadata",
        "wm_schema": "devtest_orchestration",
        "landing_path": "/Volumes/x/y/z",
        "watermark_column": "ModifiedDate",
        "jdbc_url": 'dbutils.secrets.get("scope", "url")',
        "jdbc_user": 'dbutils.secrets.get("scope", "user")',
        "jdbc_password": 'dbutils.secrets.get("scope", "password")',
        "jdbc_driver": "org.postgresql.Driver",
        "jdbc_table": '"Sales"."Orders"',
        "pipeline_name": "test_pipeline",
    }
    base.update(ctx)
    return tmpl.render(base)


def _three_action_fixture() -> List[Dict[str, str]]:
    return [
        {
            "action_name": "load_orders",
            "source_system_id": "pg_prod",
            "schema_name": "Sales",
            "table_name": "Orders",
            "load_group": "test_pipeline::test_fg",
            "jdbc_table": '"Sales"."Orders"',
            "watermark_column": "ModifiedDate",
            "landing_path": "/Volumes/landing/landing/landing/sales/orders",
        },
        {
            "action_name": "load_products",
            "source_system_id": "pg_prod",
            "schema_name": "Production",
            "table_name": "Products",
            "load_group": "test_pipeline::test_fg",
            "jdbc_table": '"Production"."Products"',
            "watermark_column": "UpdatedAt",
            "landing_path": "/Volumes/landing/landing/landing/production/products",
        },
    ]


# ---------------------------------------------------------------------------
# Frozen contract — single source of truth for the iteration payload shape.
# ---------------------------------------------------------------------------


def test_b2_iteration_keys_constant_is_authoritative() -> None:
    """``B2_ITERATION_KEYS`` lists every key both templates depend on."""
    from lhp.models.b2_iteration import B2_ITERATION_KEYS

    assert B2_ITERATION_KEYS == frozenset(
        {
            "source_system_id",
            "schema_name",
            "table_name",
            "action_name",
            "load_group",
            "batch_id",
            "manifest_table",
            "jdbc_table",
            "watermark_column",
            "landing_path",
        }
    )


# ---------------------------------------------------------------------------
# prepare_manifest emits every key in the contract for every iteration.
# ---------------------------------------------------------------------------


def _extract_iteration_reads_via_ast(source: str) -> Set[str]:
    """Return every string key read via ``iteration[<key>]`` in *source*.

    Uses ast.parse + a NodeVisitor to walk Subscript nodes where the value is
    a Name node named ``iteration`` and the slice is a string Constant.  This
    is robust to quote style (single vs double) and arbitrary whitespace /
    line-continuation formatting that would fool a regex.
    """

    class _IterationSubscriptVisitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.keys: Set[str] = set()

        def visit_Subscript(self, node: ast.Subscript) -> None:
            if (
                isinstance(node.value, ast.Name)
                and node.value.id == "iteration"
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)
            ):
                self.keys.add(node.slice.value)
            self.generic_visit(node)

    try:
        tree = ast.parse(source)
    except SyntaxError:
        # Rendered template may contain Jinja2 stubs that are not valid Python.
        # Fall back to empty set — the caller will flag the miss.
        return set()

    visitor = _IterationSubscriptVisitor()
    visitor.visit(tree)
    return visitor.keys


# Retain the regex as an internal fallback for non-parseable fragments;
# not used by production tests.
_ITERATION_KEY_RE = re.compile(r"iteration\[\s*['\"]([a-zA-Z_][a-zA-Z0-9_]*)['\"]")


def _emitted_keys(prepare_rendered: str) -> Set[str]:
    """Run the rendered prepare_manifest under a tiny mock harness and read
    the actual iteration entry keys back from the taskValues.set() payload.

    This is the strongest possible check: it asserts the *runtime* payload
    shape, not just template substring presence.
    """
    # Lazy import to keep this file self-contained and avoid coupling to
    # test_prepare_manifest_template's private helpers.
    from tests.test_prepare_manifest_template import (
        _FakeDbutils,
        _RecordingSpark,
        _run_rendered,
    )

    spark = _RecordingSpark()
    dbutils = _FakeDbutils(run_id="job-10-task-20-attempt-0")
    _run_rendered(prepare_rendered, spark, dbutils)

    tv_calls = dbutils.jobs.taskValues.calls
    assert tv_calls, "prepare_manifest must emit at least one taskValue"
    _, payload = tv_calls[0]
    iterations = json.loads(payload)
    assert iterations, "prepare_manifest must emit at least one iteration entry"

    # Take the union across all entries — every entry must use the same
    # frozen shape, but the union is the right set to test against.
    keys: Set[str] = set()
    for entry in iterations:
        keys.update(entry.keys())
    return keys


def test_prepare_manifest_emits_all_contract_keys_per_iteration() -> None:
    from lhp.models.b2_iteration import B2_ITERATION_KEYS

    rendered = _render_prepare(_three_action_fixture())
    emitted = _emitted_keys(rendered)
    missing = B2_ITERATION_KEYS - emitted
    extra = emitted - B2_ITERATION_KEYS
    assert not missing, (
        f"prepare_manifest is missing contract keys: {sorted(missing)}"
    )
    assert not extra, (
        f"prepare_manifest emits keys outside the frozen contract: {sorted(extra)}"
    )


# ---------------------------------------------------------------------------
# Worker reads only contract keys (and only via iteration[...] in B2 mode).
# ---------------------------------------------------------------------------


def test_worker_reads_subset_of_contract_keys() -> None:
    """Worker reads only keys defined in B2_ITERATION_KEYS.

    Uses ast.parse + Subscript-walk (not regex) so the check is robust to
    quote style and whitespace formatting.
    """
    from lhp.models.b2_iteration import B2_ITERATION_KEYS

    rendered = _render_worker()
    reads = _extract_iteration_reads_via_ast(rendered)
    illegal = reads - B2_ITERATION_KEYS
    assert not illegal, (
        f"Worker reads iteration keys outside the frozen contract: "
        f"{sorted(illegal)} — extend B2_ITERATION_KEYS in lockstep with "
        f"prepare_manifest emission, or stop reading them."
    )


def test_worker_reads_anomaly_a_keys_from_iteration() -> None:
    """Anomaly A regression — worker MUST read jdbc_table, landing_path,
    watermark_column from iteration in B2 mode, not from codegen literals.

    Uses ast.parse + Subscript-walk so single-quoted keys or reformatted
    source are detected correctly.
    """
    rendered = _render_worker()
    reads = _extract_iteration_reads_via_ast(rendered)
    for required in ("jdbc_table", "landing_path", "watermark_column"):
        assert required in reads, (
            f"Worker must read {required!r} from iteration kwargs in B2 mode"
        )
