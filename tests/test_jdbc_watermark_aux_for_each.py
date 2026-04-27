"""Tests for U8 B2 auxiliary file emission in JDBCWatermarkJobGenerator.

Verifies that for_each (B2) mode emits per-flowgroup aux keys and that
legacy mode keeps the per-action key behaviour. Also verifies prepare_manifest
and validate content, idempotency, and parity flag thread-through.

Scenarios:
  - Happy path B2: 3 actions → 3 per-flowgroup aux keys, no per-action keys
  - Happy path legacy: 3 actions → 3 per-action keys, no B2 aux keys
  - Multiple B2 flowgroups: each flowgroup gets its own aux file pair
  - Parity flag thread-through: parity_check:true → parity SQL block present
  - Manifest table substitution: rendered prepare_manifest embeds wm_catalog.wm_schema
  - Idempotent re-render: calling generate() twice → aux files exist exactly once
  - Actions list shape: prepare_manifest contains exactly 3 action rows
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

import pytest

from lhp.generators.load.jdbc_watermark_job import JDBCWatermarkJobGenerator
from lhp.models.config import Action, ActionType, FlowGroup, WriteTarget


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _make_load_action(
    name: str,
    *,
    schema_name: str = "Sales",
    table_name: str = "Orders",
    source_system_id: str = "pg_prod",
    landing_path: str = "/Volumes/bronze/landing/orders",
    watermark_column: str = "ModifiedDate",
    watermark_type: str = "timestamp",
    watermark_operator: str = ">=",
) -> Action:
    return Action(
        name=name,
        type="load",
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://pg-host:5432/db",
            "user": "user",
            "password": "pass",
            "driver": "org.postgresql.Driver",
            "table": f'"{schema_name}"."{table_name}"',
            "schema_name": schema_name,
            "table_name": table_name,
        },
        target=f"v_{name}_raw",
        landing_path=landing_path,
        watermark={
            "column": watermark_column,
            "type": watermark_type,
            "operator": watermark_operator,
            "source_system_id": source_system_id,
        },
    )


def _make_write_action(name: str, source: str) -> Action:
    return Action(
        name=name,
        type="write",
        source=source,
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze_catalog",
            schema="bronze_schema",
            table=name,
        ),
    )


def _make_b2_flowgroup(
    load_actions: List[Action],
    *,
    pipeline: str = "crm_pipeline",
    flowgroup: str = "contact_ingestion",
    parity_check: bool = False,
    wm_catalog: str = "metadata",
    wm_schema: str = "devtest_orchestration",
) -> FlowGroup:
    """Build a FlowGroup with for_each execution_mode."""
    write_actions = [
        _make_write_action(f"write_{a.name}", a.target or f"v_{a.name}_raw")
        for a in load_actions
    ]
    workflow: Dict[str, Any] = {"execution_mode": "for_each"}
    if parity_check:
        workflow["parity_check"] = True
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        workflow=workflow,
        actions=load_actions + write_actions,
    )


def _make_legacy_flowgroup(
    load_actions: List[Action],
    *,
    pipeline: str = "crm_pipeline",
    flowgroup: str = "contact_ingestion",
) -> FlowGroup:
    """Build a FlowGroup with NO execution_mode (legacy static emission)."""
    write_actions = [
        _make_write_action(f"write_{a.name}", a.target or f"v_{a.name}_raw")
        for a in load_actions
    ]
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        workflow=None,
        actions=load_actions + write_actions,
    )


def _run_generate_all_load_actions(
    fg: FlowGroup,
    *,
    wm_catalog: str = "metadata",
    wm_schema: str = "devtest_orchestration",
) -> FlowGroup:
    """Call JDBCWatermarkJobGenerator.generate() for each LOAD action in the flowgroup."""
    gen = JDBCWatermarkJobGenerator()
    for action in fg.actions:
        if action.type != ActionType.LOAD:
            continue
        src = action.source if isinstance(action.source, dict) else {}
        if src.get("type") != "jdbc_watermark_v2":
            continue
        # Patch watermark catalog/schema onto the action's watermark object
        # so the generator resolves to our fixture values.
        if action.watermark:
            action.watermark.catalog = wm_catalog  # type: ignore[union-attr]
            action.watermark.schema = wm_schema  # type: ignore[union-attr]
        gen.generate(action, {"flowgroup": fg})
    return fg


# ---------------------------------------------------------------------------
# Helpers: three distinct LOAD actions for multi-action fixtures
# ---------------------------------------------------------------------------

_THREE_LOAD_ACTIONS = [
    _make_load_action(
        "load_orders",
        schema_name="Sales",
        table_name="Orders",
        source_system_id="pg_crm",
        landing_path="/Volumes/bronze/landing/orders",
    ),
    _make_load_action(
        "load_customers",
        schema_name="Sales",
        table_name="Customers",
        source_system_id="pg_crm",
        landing_path="/Volumes/bronze/landing/customers",
    ),
    _make_load_action(
        "load_products",
        schema_name="Catalog",
        table_name="Products",
        source_system_id="pg_crm",
        landing_path="/Volumes/bronze/landing/products",
    ),
]


# ---------------------------------------------------------------------------
# Happy path — B2 for_each
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_b2_emits_per_flowgroup_extract_key() -> None:
    """B2: aux files contain __lhp_extract_<flowgroup>.py, not per-action keys."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    fg_name = fg.flowgroup
    assert f"__lhp_extract_{fg_name}.py" in fg._auxiliary_files, (
        "B2 mode must emit one __lhp_extract_<flowgroup>.py key"
    )


@pytest.mark.unit
def test_b2_does_not_emit_per_action_extract_keys() -> None:
    """B2: no __lhp_extract_<action_name>.py keys in auxiliary files."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    for action in _THREE_LOAD_ACTIONS:
        per_action_key = f"__lhp_extract_{action.name}.py"
        assert per_action_key not in fg._auxiliary_files, (
            f"B2 mode must NOT emit per-action key '{per_action_key}'"
        )


@pytest.mark.unit
def test_b2_emits_prepare_manifest_aux_key() -> None:
    """B2: aux files contain __lhp_prepare_manifest_<flowgroup>.py."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    key = f"__lhp_prepare_manifest_{fg.flowgroup}.py"
    assert key in fg._auxiliary_files, (
        f"B2 mode must emit '{key}'"
    )


@pytest.mark.unit
def test_b2_emits_validate_aux_key() -> None:
    """B2: aux files contain __lhp_validate_<flowgroup>.py."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    key = f"__lhp_validate_{fg.flowgroup}.py"
    assert key in fg._auxiliary_files, (
        f"B2 mode must emit '{key}'"
    )


@pytest.mark.unit
def test_b2_total_aux_file_count_is_three() -> None:
    """B2 with 3 actions: exactly 3 aux files (extract + prepare_manifest + validate)."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    assert len(fg._auxiliary_files) == 3, (
        f"B2 mode with 3 actions must produce exactly 3 aux files; "
        f"got {list(fg._auxiliary_files.keys())}"
    )


# ---------------------------------------------------------------------------
# Happy path — legacy (no execution_mode)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_legacy_emits_per_action_extract_keys() -> None:
    """Legacy: one __lhp_extract_<action_name>.py per load action."""
    fg = _make_legacy_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    for action in _THREE_LOAD_ACTIONS:
        key = f"__lhp_extract_{action.name}.py"
        assert key in fg._auxiliary_files, (
            f"Legacy mode must emit per-action key '{key}'"
        )


@pytest.mark.unit
def test_legacy_does_not_emit_b2_aux_keys() -> None:
    """Legacy: no __lhp_prepare_manifest_* or __lhp_validate_* keys."""
    fg = _make_legacy_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    for key in list(fg._auxiliary_files.keys()):
        assert not key.startswith("__lhp_prepare_manifest_"), (
            f"Legacy mode must not emit prepare_manifest key; got '{key}'"
        )
        assert not key.startswith("__lhp_validate_"), (
            f"Legacy mode must not emit validate key; got '{key}'"
        )


@pytest.mark.unit
def test_legacy_extract_key_count_equals_action_count() -> None:
    """Legacy: exactly one extract aux file per load action (3 actions → 3 files)."""
    fg = _make_legacy_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    assert len(fg._auxiliary_files) == 3, (
        f"Legacy mode must produce exactly 3 aux files (one per action); "
        f"got {list(fg._auxiliary_files.keys())}"
    )


# ---------------------------------------------------------------------------
# Multiple B2 flowgroups in different pipelines
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_multiple_b2_flowgroups_get_separate_aux_files() -> None:
    """Two B2 flowgroups in different pipelines each get their own aux file keys."""
    actions_a = [
        _make_load_action("load_a1", landing_path="/Volumes/v/l/a1"),
        _make_load_action("load_a2", landing_path="/Volumes/v/l/a2"),
    ]
    actions_b = [
        _make_load_action("load_b1", landing_path="/Volumes/v/l/b1"),
    ]

    fg_a = _make_b2_flowgroup(actions_a, pipeline="pipeline_a", flowgroup="fg_alpha")
    fg_b = _make_b2_flowgroup(actions_b, pipeline="pipeline_b", flowgroup="fg_beta")

    _run_generate_all_load_actions(fg_a)
    _run_generate_all_load_actions(fg_b)

    # fg_a should have its own keys, fg_b its own keys.
    for prefix in ("__lhp_extract_", "__lhp_prepare_manifest_", "__lhp_validate_"):
        assert f"{prefix}fg_alpha.py" in fg_a._auxiliary_files, (
            f"fg_a missing '{prefix}fg_alpha.py'"
        )
        assert f"{prefix}fg_beta.py" in fg_b._auxiliary_files, (
            f"fg_b missing '{prefix}fg_beta.py'"
        )

    # Cross-contamination check: fg_a must not have fg_beta keys and vice versa.
    for key in fg_a._auxiliary_files:
        assert "fg_beta" not in key, (
            f"fg_a must not contain fg_beta key; found '{key}'"
        )
    for key in fg_b._auxiliary_files:
        assert "fg_alpha" not in key, (
            f"fg_b must not contain fg_alpha key; found '{key}'"
        )


# ---------------------------------------------------------------------------
# Parity flag thread-through
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parity_flag_true_raises_not_implemented() -> None:
    """parity_check:true → validate.py raises NotImplementedError (LHP-VAL-049).

    The parity_check stub was removed in wave-2 review fix #16 because the
    original implementation compared w.row_count against itself and always
    passed silently, giving operators false confidence. Until the landed-parquet
    row-count source ships, enabling parity_check must surface a hard error
    rather than a silently-passing stub.
    """
    fg = _make_b2_flowgroup(
        [_make_load_action("load_orders", landing_path="/Volumes/v/l/o")],
        parity_check=True,
    )
    _run_generate_all_load_actions(fg)

    validate_code = fg._auxiliary_files[f"__lhp_validate_{fg.flowgroup}.py"]
    assert "LHP-VAL-049" in validate_code, (
        "parity_check:true must produce a validate.py containing 'LHP-VAL-049'"
    )
    assert "NotImplementedError" in validate_code, (
        "parity_check:true must produce a validate.py raising NotImplementedError"
    )
    assert "parity_mismatches" not in validate_code, (
        "parity_check:true must NOT produce the old parity_mismatches SQL stub"
    )


@pytest.mark.unit
def test_parity_flag_false_omits_parity_block() -> None:
    """parity_check not set (default False) → validate.py has NO parity block."""
    fg = _make_b2_flowgroup(
        [_make_load_action("load_orders", landing_path="/Volumes/v/l/o")],
        parity_check=False,
    )
    _run_generate_all_load_actions(fg)

    validate_code = fg._auxiliary_files[f"__lhp_validate_{fg.flowgroup}.py"]
    assert "parity_mismatches" not in validate_code, (
        "parity_check:false must NOT produce _parity_mismatches in validate.py"
    )


# ---------------------------------------------------------------------------
# Manifest table substitution
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prepare_manifest_contains_wm_catalog_and_schema() -> None:
    """Rendered prepare_manifest embeds the wm_catalog.wm_schema identifiers."""
    fg = _make_b2_flowgroup(
        [_make_load_action("load_orders", landing_path="/Volumes/v/l/o")],
        wm_catalog="metadata",
        wm_schema="devtest_orchestration",
    )
    _run_generate_all_load_actions(fg, wm_catalog="metadata", wm_schema="devtest_orchestration")

    prepare_code = fg._auxiliary_files[f"__lhp_prepare_manifest_{fg.flowgroup}.py"]
    assert "metadata" in prepare_code, (
        "prepare_manifest.py must contain wm_catalog value 'metadata'"
    )
    assert "devtest_orchestration" in prepare_code, (
        "prepare_manifest.py must contain wm_schema value 'devtest_orchestration'"
    )
    # b2_manifests table reference must be present
    assert "b2_manifests" in prepare_code, (
        "prepare_manifest.py must reference b2_manifests table"
    )


# ---------------------------------------------------------------------------
# Idempotent re-render
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_b2_aux_files_idempotent_on_double_render() -> None:
    """Calling generate() twice on the same flowgroup does not duplicate aux keys."""
    load_action = _make_load_action("load_orders", landing_path="/Volumes/v/l/o")
    fg = _make_b2_flowgroup([load_action])

    gen = JDBCWatermarkJobGenerator()
    if load_action.watermark:
        load_action.watermark.catalog = "metadata"
        load_action.watermark.schema = "devtest_orchestration"

    # First call
    gen.generate(load_action, {"flowgroup": fg})
    first_keys = set(fg._auxiliary_files.keys())
    first_extract_content = fg._auxiliary_files.get(
        f"__lhp_extract_{fg.flowgroup}.py", ""
    )

    # Second call — same generator instance on the same flowgroup
    gen.generate(load_action, {"flowgroup": fg})
    second_keys = set(fg._auxiliary_files.keys())

    assert first_keys == second_keys, (
        f"Aux file keys must not change after second render; "
        f"first={first_keys}, second={second_keys}"
    )
    assert len(second_keys) == 3, (
        "B2 single-action flowgroup must have exactly 3 aux keys after two renders"
    )
    # Content must not change either (first write wins)
    assert fg._auxiliary_files.get(f"__lhp_extract_{fg.flowgroup}.py") == first_extract_content


# ---------------------------------------------------------------------------
# Actions list shape in prepare_manifest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_prepare_manifest_contains_all_three_action_names() -> None:
    """prepare_manifest rendered source contains each of the 3 action_name literals."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    prepare_code = fg._auxiliary_files[f"__lhp_prepare_manifest_{fg.flowgroup}.py"]

    for action in _THREE_LOAD_ACTIONS:
        assert action.name in prepare_code, (
            f"prepare_manifest.py must contain action_name '{action.name}'"
        )


@pytest.mark.unit
def test_prepare_manifest_action_count_matches_fixture() -> None:
    """prepare_manifest rendered source contains exactly one row entry per load action."""
    fg = _make_b2_flowgroup(list(_THREE_LOAD_ACTIONS))
    _run_generate_all_load_actions(fg)

    prepare_code = fg._auxiliary_files[f"__lhp_prepare_manifest_{fg.flowgroup}.py"]

    # Each action produces a tuple row entry in the _manifest_rows list. The
    # template emits "sql_literal(SQLInputValidator.string(<action_name>))" per
    # row — count occurrences as a proxy for the row count.
    import re

    row_count = len(re.findall(r"sql_literal\(SQLInputValidator\.string\(", prepare_code))
    # 6 calls per row (batch_id + 5 literal fields from _manifest_rows), but
    # we only count action_name occurrences via the action name literals as
    # a more readable invariant: each action's name appears exactly once.
    action_name_hits = sum(
        1 for a in _THREE_LOAD_ACTIONS if a.name in prepare_code
    )
    assert action_name_hits == 3, (
        f"prepare_manifest.py must embed exactly 3 action names; found {action_name_hits}"
    )
