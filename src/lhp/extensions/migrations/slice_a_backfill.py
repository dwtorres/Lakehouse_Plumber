"""Slice A orphan-run backfill migration (FR-L-M1 / L3 §4.4).

Standalone CLI. Promotes pre-Slice-A ``status='running'`` watermark rows
to ``status='completed'`` when bronze-table verification confirms the
underlying data landed. Idempotent; safe to re-run after Slice A is
active — the post-Slice-A lifecycle produces no orphan rows so the
second invocation reports "no orphans".

Invocation:

    python -m lhp.extensions.migrations.slice_a_backfill \
        --wm-catalog metadata --wm-schema orchestration \
        [--bronze-catalog <cat>] [--bronze-schema <schema>] \
        [--bronze-max-view <catalog.schema.view>] \
        [--assume-empty] [--dry-run]

Exit codes (L3 §4.4):

    0  success, including no-op second runs and all-held runs when
       ``--assume-empty`` short-circuits verification
    1  unrecoverable error (connection failure, permission error)
    2  soft fail — one or more keys held because bronze verification
       could not confirm the data; warnings are logged per key and no
       update was issued for those rows

Bronze verification (L3 §4.4): for each orphan, the script either
queries an operator-supplied ``bronze_max_watermark`` view or falls
back to a direct ``SELECT MAX(<watermark_column>)`` against
``<bronze_catalog>.<bronze_schema>.<table_name>``. The 1-second skew
tolerance (OQ-3 resolution) handles historical Delta commit-timestamp
granularity drift.

The migration writes ``migration_note`` on promoted rows so an operator
can filter the audit history by Slice rollout date. The column is added
via ``ALTER TABLE ADD COLUMN IF NOT EXISTS`` the first time the script
runs; subsequent runs are no-ops against the schema.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List, Optional, Tuple

from lhp_watermark.sql_safety import (
    SQLInputValidator,
    sql_literal,
    sql_timestamp_literal,
)

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_SOFT_FAIL = 2

MIGRATION_NOTE_PREFIX = "slice-A-backfill"
_BRONZE_SKEW_TOLERANCE_SECONDS = 1.0


@dataclass
class MigrationResult:
    """Structured outcome of a single migration run.

    ``promoted`` / ``held`` counts drive the CLI exit code; the keyed
    lists let callers (and integration tests) inspect which rows moved.
    """

    promoted: int = 0
    held: int = 0
    promoted_keys: List[Tuple[str, str, str, str]] = field(default_factory=list)
    held_keys: List[Tuple[str, str, str, str]] = field(default_factory=list)


def _migration_note(today: Optional[datetime] = None) -> str:
    when = today or datetime.now(tz=timezone.utc)
    return f"{MIGRATION_NOTE_PREFIX}-{when.strftime('%Y%m%d')}"


def _ensure_migration_note_column(spark: Any, wm_table: str) -> None:
    """Idempotently add ``migration_note`` to the watermark table.

    Delta 2.x supports ``ADD COLUMN IF NOT EXISTS``. On older runtimes
    the command fails with a parse error; fall back to plain
    ``ADD COLUMN`` and swallow the ``already exists`` variant. Any other
    failure surfaces so unrelated schema errors are not hidden.
    """
    try:
        spark.sql(
            f"ALTER TABLE {wm_table} ADD COLUMN IF NOT EXISTS migration_note STRING"
        )
        return
    except Exception as exc:  # noqa: BLE001 — classifying message below
        if "already exists" in str(exc).lower():
            return
        # Fall through to plain ADD COLUMN on parse errors.
    try:
        spark.sql(f"ALTER TABLE {wm_table} ADD COLUMN migration_note STRING")
    except Exception as exc:  # noqa: BLE001 — expected on subsequent runs
        if "already exists" not in str(exc).lower():
            raise


def _find_orphans(spark: Any, wm_table: str) -> List[Any]:
    query = f"""
        SELECT run_id, source_system_id, schema_name, table_name,
               watermark_value, watermark_time, watermark_column_name
        FROM {wm_table}
        WHERE status = 'running'
    """
    return spark.sql(query).collect()


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Best-effort parse of a watermark_value into a tz-aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    # ISO-8601 with 'Z' suffix → replace with +00:00 for fromisoformat.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _decision_from_bronze_max(
    bronze_max: Any,
    watermark_value: Any,
) -> str:
    """Return 'promote' or 'hold' based on the 1-second skew rule."""
    if bronze_max is None or watermark_value is None:
        return "hold"
    # Prefer timestamp comparison when both sides parse; otherwise fall
    # back to numeric comparison for integer watermarks.
    a_ts = _parse_timestamp(bronze_max)
    b_ts = _parse_timestamp(watermark_value)
    if a_ts is not None and b_ts is not None:
        if a_ts.timestamp() >= b_ts.timestamp() - _BRONZE_SKEW_TOLERANCE_SECONDS:
            return "promote"
        return "hold"
    try:
        a_num = Decimal(str(bronze_max))
        b_num = Decimal(str(watermark_value))
    except (ArithmeticError, ValueError):
        return "hold"
    return "promote" if a_num >= b_num else "hold"


def _bronze_max_from_view(
    spark: Any,
    *,
    view: str,
    sys_id: str,
    schema: str,
    table: str,
) -> Any:
    """Query the operator-supplied bronze view for the max watermark.

    Expected view columns: ``source_system_id``, ``schema_name``,
    ``table_name``, ``bronze_max_watermark``. The view is supplied as a
    fully-qualified identifier; callers validate it via
    ``SQLInputValidator.identifier`` before invoking.
    """
    query = f"""
        SELECT bronze_max_watermark
        FROM {view}
        WHERE source_system_id = {sql_literal(sys_id)}
          AND schema_name = {sql_literal(schema)}
          AND table_name = {sql_literal(table)}
    """
    row = spark.sql(query).first()
    if row is None:
        return None
    try:
        return row["bronze_max_watermark"]
    except (KeyError, IndexError, TypeError):
        return None


def _bronze_max_from_direct_scan(
    spark: Any,
    *,
    bronze_catalog: str,
    bronze_schema: str,
    table: str,
    watermark_column: str,
) -> Any:
    """Fallback: ``SELECT MAX(<col>) FROM <cat>.<schema>.<table>``.

    Slower than the view path; intended for small environments where no
    pre-computed view exists. Column + table names are re-validated to
    guard against caller error.
    """
    SQLInputValidator.string(watermark_column)
    SQLInputValidator.string(table)
    table_ref = f"{bronze_catalog}.{bronze_schema}.{table}"
    # Use backtick-quoted column to support Spark column naming rules.
    query = f"SELECT MAX(`{watermark_column}`) AS bronze_max_watermark FROM {table_ref}"
    try:
        row = spark.sql(query).first()
    except Exception as exc:  # noqa: BLE001 — missing table treated as unverifiable
        logger.warning(
            "bronze direct scan failed for %s: %s; treating as held",
            table_ref,
            exc,
        )
        return None
    if row is None:
        return None
    try:
        return row["bronze_max_watermark"]
    except (KeyError, IndexError, TypeError):
        return None


def _promote_row(
    spark: Any,
    wm_table: str,
    *,
    run_id: str,
    migration_note: str,
) -> None:
    SQLInputValidator.uuid_or_job_run_id(run_id)
    SQLInputValidator.string(migration_note)
    ts_literal = sql_timestamp_literal(datetime.now(tz=timezone.utc))
    sql = f"""
        UPDATE {wm_table}
        SET status = 'completed',
            completed_at = {ts_literal},
            migration_note = {sql_literal(migration_note)}
        WHERE run_id = {sql_literal(run_id)}
          AND status = 'running'
    """
    spark.sql(sql)


def run_migration(
    spark: Any,
    *,
    wm_catalog: str,
    wm_schema: str,
    bronze_catalog: Optional[str] = None,
    bronze_schema: Optional[str] = None,
    bronze_max_view: Optional[str] = None,
    assume_empty: bool = False,
    dry_run: bool = False,
    today: Optional[datetime] = None,
) -> MigrationResult:
    """Promote verifiable orphan ``running`` rows to ``completed``.

    ``today`` is exposed for deterministic testing of the migration_note
    suffix; production callers leave it ``None``.
    """
    SQLInputValidator.identifier(wm_catalog)
    SQLInputValidator.identifier(wm_schema)
    if bronze_catalog is not None:
        SQLInputValidator.identifier(bronze_catalog)
    if bronze_schema is not None:
        SQLInputValidator.identifier(bronze_schema)
    if bronze_max_view is not None:
        SQLInputValidator.identifier(bronze_max_view)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    wm_table = f"{wm_catalog}.{wm_schema}.watermarks"

    _ensure_migration_note_column(spark, wm_table)

    orphans = _find_orphans(spark, wm_table)
    result = MigrationResult()

    if not orphans:
        logger.info("no orphans found; migration already applied")
        return result

    note = _migration_note(today)

    for row in orphans:
        key = (
            row["source_system_id"],
            row["schema_name"],
            row["table_name"],
            row["run_id"],
        )
        watermark_value = row["watermark_value"]
        watermark_column = (
            row["watermark_column_name"]
            if "watermark_column_name" in row and row["watermark_column_name"]
            else None
        )

        if assume_empty:
            decision = "promote"
        else:
            bronze_max = _lookup_bronze_max(
                spark,
                sys_id=key[0],
                schema=key[1],
                table=key[2],
                watermark_column=watermark_column,
                bronze_catalog=bronze_catalog,
                bronze_schema=bronze_schema,
                bronze_max_view=bronze_max_view,
            )
            decision = _decision_from_bronze_max(bronze_max, watermark_value)

        if decision == "promote":
            if not dry_run:
                _promote_row(spark, wm_table, run_id=key[3], migration_note=note)
            logger.info("promoted %s for %s.%s.%s", key[3], key[0], key[1], key[2])
            result.promoted += 1
            result.promoted_keys.append(key)
        else:
            logger.warning(
                "orphan %s not promoted: bronze verification failed; next "
                "run will full-reload (key=%s.%s.%s)",
                key[3],
                key[0],
                key[1],
                key[2],
            )
            result.held += 1
            result.held_keys.append(key)

    return result


def _lookup_bronze_max(
    spark: Any,
    *,
    sys_id: str,
    schema: str,
    table: str,
    watermark_column: Optional[str],
    bronze_catalog: Optional[str],
    bronze_schema: Optional[str],
    bronze_max_view: Optional[str],
) -> Any:
    """Route to the view path or direct-scan path based on operator flags."""
    if bronze_max_view is not None:
        return _bronze_max_from_view(
            spark,
            view=bronze_max_view,
            sys_id=sys_id,
            schema=schema,
            table=table,
        )
    if bronze_catalog and bronze_schema and watermark_column:
        return _bronze_max_from_direct_scan(
            spark,
            bronze_catalog=bronze_catalog,
            bronze_schema=bronze_schema,
            table=table,
            watermark_column=watermark_column,
        )
    # No verification path configured → treat as unverifiable.
    return None


# -------------------------- CLI ------------------------------------


_DESCRIPTION = (
    "Promote Slice A orphan 'running' watermark rows to 'completed' "
    "when bronze data is verified."
)
_EPILOG = (
    "Exit codes:\n"
    "  0  success (including no-op second runs)\n"
    "  1  unrecoverable error (connection failure, permission error)\n"
    "  2  bronze verification failed for one or more keys (soft fail;\n"
    "     warnings logged, no update issued for those rows)\n"
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slice_a_backfill",
        description=_DESCRIPTION,
        epilog=_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--wm-catalog",
        required=True,
        help="Unity Catalog catalog containing the watermarks table.",
    )
    parser.add_argument(
        "--wm-schema",
        required=True,
        help="Schema (namespace) within --wm-catalog holding the watermarks table.",
    )
    parser.add_argument(
        "--bronze-catalog",
        default=None,
        help="Catalog for direct bronze scans when no --bronze-max-view is supplied.",
    )
    parser.add_argument(
        "--bronze-schema",
        default=None,
        help="Schema for direct bronze scans when no --bronze-max-view is supplied.",
    )
    parser.add_argument(
        "--bronze-max-view",
        default=None,
        help=(
            "Fully-qualified pre-computed view exposing "
            "(source_system_id, schema_name, table_name, bronze_max_watermark). "
            "Preferred over direct scans on large environments."
        ),
    )
    parser.add_argument(
        "--assume-empty",
        action="store_true",
        help=(
            "Skip bronze verification and short-circuit-promote all orphans. "
            "Operator override for fresh dev environments where no prior "
            "data exists."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log decisions without issuing UPDATEs against the watermarks table.",
    )
    return parser


def _get_spark() -> Any:
    """Return an active SparkSession. Patched by unit tests."""
    # Imported lazily so the module can be imported in environments
    # without pyspark (e.g. CLI --help generation, unit tests).
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        spark = _get_spark()
        result = run_migration(
            spark,
            wm_catalog=args.wm_catalog,
            wm_schema=args.wm_schema,
            bronze_catalog=args.bronze_catalog,
            bronze_schema=args.bronze_schema,
            bronze_max_view=args.bronze_max_view,
            assume_empty=args.assume_empty,
            dry_run=args.dry_run,
        )
    except Exception:  # noqa: BLE001 — top-level safety net
        logger.exception("slice_a_backfill: unrecoverable error")
        return EXIT_ERROR

    logger.info(
        "slice_a_backfill summary: promoted=%d held=%d",
        result.promoted,
        result.held,
    )
    if result.held > 0:
        return EXIT_SOFT_FAIL
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
