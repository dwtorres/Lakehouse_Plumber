"""Tests for the Tier 2 ``load_group`` Databricks validation notebook helpers.

The notebook ``scripts/validation/validate_tier2_load_group.py`` exposes pure
helper functions (``_build_probe_row``, ``build_probe_insert_sql``,
``build_step4a_insert_sql``, ``build_cleanup_sql``, ``build_jdbc_where_clause``)
that compose SQL fragments from validated inputs. These tests load the notebook
as a plain Python module via ``importlib`` (the cell-execution path is gated on
``dbutils`` availability so a bare import does not raise) and exercise the
helpers without a live Databricks runtime — mirroring the precedent in
``tests/scripts/test_verify_databricks_bundle.py``.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from typing import Any

import pytest

from lhp_watermark.exceptions import WatermarkValidationError

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "validation"
    / "validate_tier2_load_group.py"
)


def _load_notebook() -> Any:
    spec = importlib.util.spec_from_file_location(
        "validate_tier2_load_group", str(_SCRIPT_PATH)
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------- script existence + bare-import safety ----------------------------


def test_script_exists() -> None:
    assert _SCRIPT_PATH.exists()


def test_module_loads_without_dbutils() -> None:
    """A bare ``importlib`` load must not raise even though ``dbutils`` is
    not bound — the cell-execution path is guarded by ``_running_in_databricks``.
    """
    mod = _load_notebook()
    assert mod._running_in_databricks() is False


def test_helpers_exposed() -> None:
    mod = _load_notebook()
    for name in (
        "_build_probe_row",
        "_validate_probe_row",
        "build_probe_insert_sql",
        "build_step4a_insert_sql",
        "build_cleanup_sql",
        "build_jdbc_where_clause",
    ):
        assert hasattr(mod, name), f"helper {name} missing from notebook"


# ---------- _build_probe_row -------------------------------------------------


def test_build_probe_row_dict_shape() -> None:
    mod = _load_notebook()
    row = mod._build_probe_row(
        run_id="local-12345678-1234-1234-1234-123456789abc",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-01-01T00:00:00.000000+00:00",
        load_group="lg_a",
    )
    # 16-column projection per the notebook INSERT column list.
    expected_keys = {
        "run_id",
        "watermark_time",
        "source_system_id",
        "schema_name",
        "table_name",
        "watermark_column_name",
        "watermark_value",
        "previous_watermark_value",
        "row_count",
        "extraction_type",
        "bronze_stage_complete",
        "silver_stage_complete",
        "status",
        "created_at",
        "completed_at",
        "load_group",
    }
    assert set(row.keys()) == expected_keys
    assert row["run_id"] == "local-12345678-1234-1234-1234-123456789abc"
    assert row["source_system_id"] == "tier2_validation"
    assert row["load_group"] == "lg_a"
    # Defaults from helper signature.
    assert row["status"] == "completed"
    assert row["extraction_type"] == "incremental"
    assert row["bronze_stage_complete"] is True
    assert row["silver_stage_complete"] is True
    assert row["previous_watermark_value"] is None


def test_build_probe_row_load_group_none_passthrough() -> None:
    """V2 cell builds a row with ``load_group=None`` (pre-Tier-2 / NULL state).

    The dict must carry ``None`` faithfully so the INSERT composer can emit
    SQL ``NULL`` for the column.
    """
    mod = _load_notebook()
    row = mod._build_probe_row(
        run_id="local-12345678-1234-1234-1234-123456789abc",
        source_system_id="fake_federation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-12-31T23:59:59.000000+00:00",
        load_group=None,
    )
    assert row["load_group"] is None


# ---------- _validate_probe_row ----------------------------------------------


def test_validate_probe_row_accepts_well_formed_row() -> None:
    mod = _load_notebook()
    row = mod._build_probe_row(
        run_id="local-12345678-1234-1234-1234-123456789abc",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-01-01T00:00:00.000000+00:00",
        load_group="lg_a",
    )
    # Should not raise.
    mod._validate_probe_row(row)


def test_validate_probe_row_rejects_control_char_in_load_group() -> None:
    """Control characters in ``load_group`` violate ``SQLInputValidator.string``."""
    mod = _load_notebook()
    row = mod._build_probe_row(
        run_id="local-12345678-1234-1234-1234-123456789abc",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-01-01T00:00:00.000000+00:00",
        load_group="lg_a\x00drop_table",  # control char rejected
    )
    with pytest.raises(WatermarkValidationError):
        mod._validate_probe_row(row)


def test_validate_probe_row_rejects_invalid_run_id() -> None:
    """``run_id`` must match UUID-v4, ``local-<uuid>``, or
    ``job-N-task-N-attempt-N`` (per ``SQLInputValidator.uuid_or_job_run_id``).
    """
    mod = _load_notebook()
    row = mod._build_probe_row(
        run_id="not-a-valid-run-id",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-01-01T00:00:00.000000+00:00",
        load_group="lg_a",
    )
    with pytest.raises(WatermarkValidationError):
        mod._validate_probe_row(row)


# ---------- build_probe_insert_sql (V1/V2/V3 INSERT) -------------------------


def _v1_row(mod: Any, **overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = dict(
        run_id="local-12345678-1234-1234-1234-123456789abc",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        watermark_column_name="modified_date",
        watermark_value="2099-01-01T00:00:00.000000+00:00",
        load_group="lg_a",
    )
    base.update(overrides)
    return mod._build_probe_row(**base)


def test_probe_insert_sql_uses_sql_literal_quoting() -> None:
    """Composed values flow through ``sql_literal`` — no f-string concat of
    raw strings. Embedded single quotes are doubled.
    """
    mod = _load_notebook()
    row = _v1_row(mod, load_group="lg'_a")  # embedded single-quote
    sql = mod.build_probe_insert_sql(
        "metadata.devtest_edp_orchestration.watermarks_v1_probe", row
    )
    # Quoted-and-doubled embedded single-quote.
    assert "'lg''_a'" in sql
    # Backtick-emitted dotted identifier (per ``sql_identifier`` precedent).
    assert "`metadata`.`devtest_edp_orchestration`.`watermarks_v1_probe`" in sql


def test_probe_insert_sql_emits_full_16_column_list_in_order() -> None:
    """Column list must match the verified DDL ordering exactly."""
    mod = _load_notebook()
    row = _v1_row(mod)
    sql = mod.build_probe_insert_sql(
        "metadata.devtest_edp_orchestration.watermarks_v1_probe", row
    )
    expected_cols = (
        "run_id, watermark_time, source_system_id, schema_name, table_name, "
        "watermark_column_name, watermark_value, previous_watermark_value, "
        "row_count, extraction_type, bronze_stage_complete, silver_stage_complete, "
        "status, created_at, completed_at, load_group"
    )
    assert expected_cols in sql


def test_probe_insert_sql_emits_null_for_load_group_none() -> None:
    """``load_group=None`` must render as bare SQL ``NULL`` (not ``'None'``)."""
    mod = _load_notebook()
    row = _v1_row(mod, load_group=None)
    sql = mod.build_probe_insert_sql(
        "metadata.devtest_edp_orchestration.watermarks_v1_probe", row
    )
    # The VALUES tuple must end with `, NULL)` for load_group=None.
    # Match pattern: any whitespace then NULL then ).
    assert re.search(r",\s*NULL\s*\)\s*$", sql), sql


def test_probe_insert_sql_rejects_invalid_probe_table_identifier() -> None:
    mod = _load_notebook()
    row = _v1_row(mod)
    with pytest.raises(WatermarkValidationError):
        mod.build_probe_insert_sql("bad table name with spaces", row)


def test_probe_insert_sql_emits_timestamp_typed_literal() -> None:
    """``watermark_time``, ``created_at``, ``completed_at`` render as
    ``TIMESTAMP '...'``, not bare strings.
    """
    mod = _load_notebook()
    row = _v1_row(mod)
    sql = mod.build_probe_insert_sql(
        "metadata.devtest_edp_orchestration.watermarks_v1_probe", row
    )
    # At least three TIMESTAMP literals in the VALUES clause.
    assert sql.count("TIMESTAMP '") >= 3


# ---------- build_step4a_insert_sql (V4 INSERT-from-SELECT) ------------------


def test_step4a_sql_matches_origin_doc_column_list_order() -> None:
    """V4 helper Step 4a INSERT SQL matches the origin doc shape exactly:
    16-column list per ``docs/planning/tier-2-hwm-load-group-fix.md`` Step 4a.
    """
    mod = _load_notebook()
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group="tier2_v4_pipe::tier2_v4_fg",
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    expected_cols = (
        "(run_id, watermark_time, source_system_id, schema_name, table_name, "
        "watermark_column_name, watermark_value, previous_watermark_value, "
        "row_count, extraction_type, "
        "bronze_stage_complete, silver_stage_complete, status, "
        "created_at, completed_at, load_group)"
    )
    assert expected_cols in sql


def test_step4a_sql_uses_seeded_extraction_type() -> None:
    """The seed row's ``extraction_type`` must be the literal ``'seeded'``
    (new enum value introduced by Tier 2; safe additive — column has no
    enum constraint).
    """
    mod = _load_notebook()
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group="tier2_v4_pipe::tier2_v4_fg",
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    assert "'seeded'" in sql


def test_step4a_sql_filters_legacy_rows_only() -> None:
    """The SELECT side must filter to ``load_group = 'legacy'`` (the
    Step-3-backfilled rows). Running before Step 3 silently inserts zero
    rows — operator footgun guard from origin doc Step 4a.
    """
    mod = _load_notebook()
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group="tier2_v4_pipe::tier2_v4_fg",
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    assert "WHERE load_group = 'legacy'" in sql
    assert "AND status = 'completed'" in sql


def test_step4a_sql_order_by_with_lex_run_id_tiebreaker() -> None:
    """Origin doc Step 4a tiebreakers: timestamps first, lex run_id last."""
    mod = _load_notebook()
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group="tier2_v4_pipe::tier2_v4_fg",
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    expected_order_by = (
        "ORDER BY watermark_time DESC, completed_at DESC, run_id DESC"
    )
    assert expected_order_by in sql
    assert "LIMIT 1" in sql


def test_step4a_sql_uses_current_timestamp_for_created_at() -> None:
    """Per origin doc Step 4a: the seed row's ``created_at`` is set to
    ``current_timestamp()`` (Spark function), not copied from the legacy row.
    """
    mod = _load_notebook()
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group="tier2_v4_pipe::tier2_v4_fg",
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    assert "current_timestamp()" in sql


def test_step4a_sql_emits_target_load_group_via_sql_literal() -> None:
    """The composite ``pipeline::flowgroup`` value flows through
    ``SQLInputValidator.string`` + ``sql_literal`` (no f-string concat).
    """
    mod = _load_notebook()
    target = "tier2_v4_pipe::tier2_v4_fg"
    sql = mod.build_step4a_insert_sql(
        probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
        source_system_id="tier2_validation",
        schema_name="tier2_probe",
        table_name="probe_tbl",
        target_load_group=target,
        seed_run_id="local-12345678-1234-1234-1234-123456789abc",
    )
    assert f"'{target}'" in sql


def test_step4a_sql_rejects_bad_seed_run_id() -> None:
    mod = _load_notebook()
    with pytest.raises(WatermarkValidationError):
        mod.build_step4a_insert_sql(
            probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
            source_system_id="tier2_validation",
            schema_name="tier2_probe",
            table_name="probe_tbl",
            target_load_group="tier2_v4_pipe::tier2_v4_fg",
            seed_run_id="bad-run-id-not-uuid",
        )


def test_step4a_sql_rejects_bad_probe_table_identifier() -> None:
    mod = _load_notebook()
    with pytest.raises(WatermarkValidationError):
        mod.build_step4a_insert_sql(
            probe_table="not a valid; identifier",
            source_system_id="tier2_validation",
            schema_name="tier2_probe",
            table_name="probe_tbl",
            target_load_group="tier2_v4_pipe::tier2_v4_fg",
            seed_run_id="local-12345678-1234-1234-1234-123456789abc",
        )


def test_step4a_sql_rejects_control_char_in_target_load_group() -> None:
    mod = _load_notebook()
    with pytest.raises(WatermarkValidationError):
        mod.build_step4a_insert_sql(
            probe_table="metadata.devtest_edp_orchestration.watermarks_v1_probe",
            source_system_id="tier2_validation",
            schema_name="tier2_probe",
            table_name="probe_tbl",
            target_load_group="tier2_v4_pipe::tier2_v4_fg\x00",
            seed_run_id="local-12345678-1234-1234-1234-123456789abc",
        )


# ---------- build_cleanup_sql (run_id-scoped DELETE) -------------------------


def test_cleanup_sql_uses_run_id_in_clause_not_table_scoped_predicate() -> None:
    """Cleanup helper deletes ONLY by isolated ``run_id`` markers — never
    by table-scoped predicate like ``WHERE source_system_id = ...``. Mirrors
    the Tier-1 V1 cleanup pattern from ``docs/planning/tier-1-hwm-fix.md``.
    """
    mod = _load_notebook()
    rids = [
        "local-11111111-1111-1111-1111-111111111111",
        "local-22222222-2222-2222-2222-222222222222",
    ]
    sql = mod.build_cleanup_sql(
        "metadata.devtest_edp_orchestration.watermarks_v1_probe", rids
    )
    assert sql is not None
    # Run-id-scoped IN clause.
    assert "WHERE run_id IN (" in sql
    # Both literals quoted via ``sql_literal``.
    for rid in rids:
        assert f"'{rid}'" in sql
    # Critically: NO table-scoped predicates.
    assert "WHERE source_system_id" not in sql
    assert "AND source_system_id" not in sql
    assert "schema_name" not in sql
    assert "table_name" not in sql


def test_cleanup_sql_returns_none_for_empty_run_id_list() -> None:
    mod = _load_notebook()
    assert (
        mod.build_cleanup_sql(
            "metadata.devtest_edp_orchestration.watermarks_v1_probe", []
        )
        is None
    )


def test_cleanup_sql_rejects_bad_run_id_in_list() -> None:
    mod = _load_notebook()
    with pytest.raises(WatermarkValidationError):
        mod.build_cleanup_sql(
            "metadata.devtest_edp_orchestration.watermarks_v1_probe",
            ["local-11111111-1111-1111-1111-111111111111", "evil; DROP TABLE"],
        )


def test_cleanup_sql_rejects_bad_probe_table_identifier() -> None:
    mod = _load_notebook()
    with pytest.raises(WatermarkValidationError):
        mod.build_cleanup_sql(
            "not a valid identifier",
            ["local-11111111-1111-1111-1111-111111111111"],
        )


# ---------- build_jdbc_where_clause (V4 dry-run assertion) -------------------


def test_jdbc_where_clause_includes_hwm_literal() -> None:
    """V4 (c): the JDBC WHERE clause must include the seeded HWM literal,
    so the dry-run assertion ``observed_hwm in where_clause`` passes.
    """
    mod = _load_notebook()
    where = mod.build_jdbc_where_clause(
        jdbc_table="probe_jdbc",
        watermark_column="modified_date",
        hwm_value="2099-03-15T12:00:00.000000+00:00",
    )
    assert "2099-03-15T12:00:00.000000+00:00" in where
    assert "`modified_date`" in where
    assert ">" in where


def test_jdbc_where_clause_escapes_embedded_single_quotes() -> None:
    mod = _load_notebook()
    where = mod.build_jdbc_where_clause(
        jdbc_table="probe_jdbc",
        watermark_column="modified_date",
        hwm_value="o'brien",
    )
    # Single quote doubled inside the SQL literal.
    assert "'o''brien'" in where
