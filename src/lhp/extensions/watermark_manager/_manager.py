"""
Watermark manager for incremental data ingestion.

Manages high-water marks for tracking incremental data loads using
Delta Lake tables with liquid clustering for concurrent writes.

Simplified port of data_platform WatermarkManager for Lakehouse Plumber.
"""

import logging
import random
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Union

from pyspark.sql import SparkSession

from lhp.extensions.watermark_manager.exceptions import (
    DuplicateRunError,
    TerminalStateGuardError,
    WatermarkConcurrencyError,
)
from lhp.extensions.watermark_manager.sql_safety import (
    SQLInputValidator,
    sql_literal,
    sql_numeric_literal,
    sql_timestamp_literal,
)

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Delta concurrent-commit exception class names. Detection is by string
# (not isinstance) because Delta is not installed in unit-test environments
# and the exception may arrive Py4J-wrapped on Databricks.
_CONCURRENT_COMMIT_NAMES = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
)

# Retry budget for FR-L-05 / NFR-L-02. Reducing shard size is the right
# response to genuine over-contention; raising the budget hides the cause.
_INSERT_NEW_RETRY_BUDGET = 5
_INSERT_NEW_BACKOFF_BASE_SECS = 0.1
_INSERT_NEW_BACKOFF_FACTOR = 2.0
_INSERT_NEW_BACKOFF_JITTER = 0.5  # ±50 % of the base delay


def _is_concurrent_commit_exception(exc: BaseException) -> bool:
    """Match Delta concurrent-commit exceptions by class name + message text.

    Direct class match handles plain Python raises in tests; message match
    catches the Py4J-wrapped form Databricks surfaces in production.
    """
    if type(exc).__name__ in _CONCURRENT_COMMIT_NAMES:
        return True
    msg = str(exc)
    return any(name in msg for name in _CONCURRENT_COMMIT_NAMES)


def _render_watermark_literal(value: Optional[Union[str, int, Decimal]]) -> str:
    """Render a watermark scalar as a SQL literal.

    Strings are validated + emitted as quoted literals; ints/Decimals as
    bare numeric. ``None`` becomes ``NULL``. Float is rejected by
    ``SQLInputValidator.numeric``.
    """
    if value is None:
        return "NULL"
    if isinstance(value, str):
        SQLInputValidator.string(value)
        return sql_literal(value)
    SQLInputValidator.numeric(value)
    return sql_numeric_literal(value)


class WatermarkManager:
    """
    Manages watermarks for incremental data ingestion.

    Uses Delta Lake MERGE operations to prevent duplicate run_ids and
    liquid clustering for high-concurrency writes.

    Examples:
        >>> manager = WatermarkManager(spark, catalog="metadata")
        >>> manager.insert_new(
        ...     run_id="run_001",
        ...     source_system_id="oracle_prod",
        ...     schema_name="HR",
        ...     table_name="EMPLOYEES",
        ...     watermark_column_name="UPDATED_AT",
        ...     watermark_value="2025-01-01 00:00:00",
        ...     row_count=1000,
        ...     extraction_type="incremental",
        ... )
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "metadata",
        schema: str = "orchestration",
    ) -> None:
        """
        Initialize watermark manager.

        Args:
            spark: Active Spark session
            catalog: Unity Catalog name containing watermarks table
            schema: Schema containing watermarks table
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.table_name = f"{catalog}.{schema}.watermarks"

        self._ensure_table_exists()

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> None:
        """
        Validate that a string is safe for use in SQL identifiers/values.

        Allows alphanumeric characters, underscores, dots, and hyphens.

        Args:
            value: The string to validate.
            field_name: Name of the field (for error messages).

        Raises:
            ValueError: If the value contains disallowed characters.
        """
        if not value or not _IDENTIFIER_PATTERN.match(value):
            raise ValueError(
                f"Invalid {field_name}: '{value}'. "
                "Only alphanumeric characters, underscores, dots, and hyphens are allowed."
            )

    def _ensure_utc_session(self) -> None:
        """
        Set ``spark.sql.session.timeZone`` to UTC on the active Spark session.

        Watermark TIMESTAMP literals are normalised to UTC by
        ``sql_timestamp_literal``; the readback path must interpret them in
        the same timezone or round-trips drift by the local UTC offset.
        Idempotent: repeated calls with the same value are no-ops in Spark.
        See FR-L-07 / AC-SA-28.
        """
        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def _ensure_table_exists(self) -> None:
        """
        Ensure watermarks table exists with proper schema.

        Creates the Delta table with liquid clustering if it does not exist.
        Unity Catalog DDL only.
        """
        self._ensure_utc_session()
        start_time = time.time()

        namespace = f"{self.catalog}.{self.schema}"
        tables = self.spark.sql(f"SHOW TABLES IN {namespace}").collect()

        table_exists = any(row["tableName"] == "watermarks" for row in tables)

        if not table_exists:
            logger.info("Creating watermarks table: %s", self.table_name)

            ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    run_id STRING NOT NULL,
                    watermark_time TIMESTAMP NOT NULL,
                    source_system_id STRING NOT NULL,
                    schema_name STRING NOT NULL,
                    table_name STRING NOT NULL,
                    watermark_column_name STRING,
                    watermark_value STRING,
                    previous_watermark_value STRING,
                    row_count BIGINT,
                    extraction_type STRING NOT NULL,
                    bronze_stage_complete BOOLEAN NOT NULL,
                    silver_stage_complete BOOLEAN NOT NULL,
                    status STRING NOT NULL,
                    error_class STRING,
                    error_message STRING,
                    created_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    CONSTRAINT pk_watermarks PRIMARY KEY (run_id)
                )
                USING DELTA
                CLUSTER BY (source_system_id, schema_name, table_name)
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """
            self.spark.sql(ddl)

            duration_ms = (time.time() - start_time) * 1000
            logger.info("Watermarks table created successfully (%.1f ms)", duration_ms)

    def get_latest_watermark(
        self,
        source_system_id: str,
        schema_name: str,
        table_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the most recent terminal-success watermark, or ``None``.

        Filters strictly by ``status='completed'`` so non-terminal rows
        (``running``, ``failed``, ``timed_out``, future ``landed_not_committed``)
        cannot be picked up as the "latest" — that was the pre-Slice-A bug
        FR-L-04 fixes. Same-second collisions tie-break deterministically by
        ``run_id`` lexicographic descending.

        Pre-migration tables that contain only orphan ``running`` rows
        return ``None`` and emit a WARNING so the operator sees the
        impending full-reload (FR-L-04 migration hazard / FR-L-M1).

        Args:
            source_system_id: Source system identifier.
            schema_name: Schema name.
            table_name: Table name.

        Returns:
            Dict with watermark details or ``None``.
        """
        self._ensure_utc_session()
        start_time = time.time()

        # source_system_id / schema_name / table_name form the watermark
        # composite key; they are identifier-shaped data values stored in
        # the table. Use the stricter identifier validator (which rejects
        # ';', whitespace, control chars, and embedded quotes) so a
        # malformed key cannot reach SQL even by accident.
        SQLInputValidator.identifier(source_system_id)
        SQLInputValidator.identifier(schema_name)
        SQLInputValidator.identifier(table_name)

        query = f"""
            SELECT
                run_id,
                watermark_value,
                watermark_time,
                row_count,
                status,
                bronze_stage_complete,
                silver_stage_complete
            FROM {self.table_name}
            WHERE source_system_id = {sql_literal(source_system_id)}
              AND schema_name = {sql_literal(schema_name)}
              AND table_name = {sql_literal(table_name)}
              AND status = 'completed'
            ORDER BY watermark_time DESC, run_id DESC
            LIMIT 1
        """
        result = self.spark.sql(query).collect()

        duration_ms = (time.time() - start_time) * 1000

        if not result:
            logger.warning(
                "No completed watermark for %s.%s.%s; next run will perform "
                "a full load (pre-migration orphan or first run) (%.1f ms)",
                source_system_id,
                schema_name,
                table_name,
                duration_ms,
            )
            return None

        row = result[0]
        watermark = {
            "run_id": row["run_id"],
            "watermark_value": row["watermark_value"],
            "watermark_time": row["watermark_time"],
            "row_count": row["row_count"],
            "status": row["status"],
            "bronze_stage_complete": row["bronze_stage_complete"],
            "silver_stage_complete": row["silver_stage_complete"],
        }

        logger.info(
            "Latest watermark retrieved for %s.%s.%s: %s (%.1f ms)",
            source_system_id,
            schema_name,
            table_name,
            watermark["watermark_value"],
            duration_ms,
        )
        return watermark

    def insert_new(
        self,
        run_id: str,
        source_system_id: str,
        schema_name: str,
        table_name: str,
        watermark_column_name: Optional[str],
        watermark_value: Optional[Union[str, int, Decimal]],
        row_count: int = 0,
        extraction_type: str = "incremental",
        previous_watermark_value: Optional[Union[str, int, Decimal]] = None,
    ) -> None:
        """
        Atomically insert a new watermark row in ``status='running'``.

        Implementation: a single ``MERGE INTO ... WHEN NOT MATCHED THEN INSERT``
        statement. ``num_affected_rows`` is read from the result; zero means
        the ``run_id`` already exists, which raises ``DuplicateRunError``
        (FR-L-05). Delta concurrent-commit exceptions are absorbed by an
        internal retry loop with jittered exponential backoff
        (budget 5, base 100 ms, factor 2, ±50 % jitter); budget exhaustion
        raises ``WatermarkConcurrencyError`` chaining the final cause.

        All interpolated values pass through ``SQLInputValidator`` first;
        the SQL string itself is composed exclusively from emitter output
        (``sql_literal``, ``sql_numeric_literal``, ``sql_timestamp_literal``).

        Args:
            run_id: Unique identifier for this run (UUID, ``local-<uuid>``,
                or ``job-N-task-N-attempt-N``).
            source_system_id: Source system identifier.
            schema_name: Schema name.
            table_name: Table name.
            watermark_column_name: Column used for watermarking.
            watermark_value: Current watermark value (str / int / Decimal).
                ``float`` is rejected.
            row_count: Number of rows processed.
            extraction_type: One of 'incremental', 'full', 'snapshot', 'merge'.
            previous_watermark_value: Previous watermark value for reference.

        Raises:
            WatermarkValidationError: An input failed type/format validation.
            DuplicateRunError: ``run_id`` already exists.
            WatermarkConcurrencyError: Delta retry budget exhausted.
        """
        self._ensure_utc_session()
        start_time = time.time()

        # Validate every input before any SQL composition (FR-L-03 / NFR-L-05).
        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.string(source_system_id)
        SQLInputValidator.string(schema_name)
        SQLInputValidator.string(table_name)
        SQLInputValidator.string(extraction_type)
        SQLInputValidator.numeric(row_count)
        if watermark_column_name is not None:
            SQLInputValidator.string(watermark_column_name)

        wm_val_sql = _render_watermark_literal(watermark_value)
        prev_wm_val_sql = _render_watermark_literal(previous_watermark_value)
        wm_col_sql = (
            sql_literal(watermark_column_name) if watermark_column_name else "NULL"
        )
        ts_literal = sql_timestamp_literal(datetime.now(tz=timezone.utc))

        merge_sql = f"""
            MERGE INTO {self.table_name} AS t
            USING (
                SELECT
                    {sql_literal(run_id)} AS run_id,
                    {ts_literal} AS watermark_time,
                    {sql_literal(source_system_id)} AS source_system_id,
                    {sql_literal(schema_name)} AS schema_name,
                    {sql_literal(table_name)} AS table_name,
                    {wm_col_sql} AS watermark_column_name,
                    {wm_val_sql} AS watermark_value,
                    {prev_wm_val_sql} AS previous_watermark_value,
                    {sql_numeric_literal(row_count)} AS row_count,
                    {sql_literal(extraction_type)} AS extraction_type,
                    false AS bronze_stage_complete,
                    false AS silver_stage_complete,
                    'running' AS status,
                    {ts_literal} AS created_at
            ) AS s
            ON t.run_id = s.run_id
            WHEN NOT MATCHED THEN INSERT (
                run_id, watermark_time, source_system_id, schema_name,
                table_name, watermark_column_name, watermark_value,
                previous_watermark_value, row_count, extraction_type,
                bronze_stage_complete, silver_stage_complete, status,
                created_at
            )
            VALUES (
                s.run_id, s.watermark_time, s.source_system_id, s.schema_name,
                s.table_name, s.watermark_column_name, s.watermark_value,
                s.previous_watermark_value, s.row_count, s.extraction_type,
                s.bronze_stage_complete, s.silver_stage_complete, s.status,
                s.created_at
            )
        """

        affected = self._merge_with_retry(merge_sql, run_id=run_id)
        if affected == 0:
            raise DuplicateRunError(run_id=run_id)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Watermark inserted: run_id=%s, value=%s, rows=%d (%.1f ms)",
            run_id,
            watermark_value,
            row_count,
            duration_ms,
        )

    def _merge_with_retry(self, merge_sql: str, *, run_id: str) -> int:
        """Execute a MERGE with Delta concurrent-commit retry (FR-L-05).

        Returns ``num_affected_rows`` from the MERGE result. Re-raises any
        non-concurrent-commit exception unchanged. Raises
        ``WatermarkConcurrencyError`` when the retry budget is exhausted.
        """
        last_exc: Optional[BaseException] = None
        for attempt in range(_INSERT_NEW_RETRY_BUDGET):
            try:
                result = self.spark.sql(merge_sql)
                row = result.first() if result is not None else None
                if row is None:
                    # Older Spark versions / mocks may not surface the metric.
                    # Treat as success — DuplicateRunError surfaces only when
                    # the metric is explicitly zero.
                    return 1
                try:
                    return int(row["num_affected_rows"])
                except (KeyError, IndexError, TypeError):
                    return 1
            except BaseException as exc:  # noqa: BLE001 — re-raise unrelated below
                if not _is_concurrent_commit_exception(exc):
                    raise
                last_exc = exc
                if attempt + 1 < _INSERT_NEW_RETRY_BUDGET:
                    base_delay = _INSERT_NEW_BACKOFF_BASE_SECS * (
                        _INSERT_NEW_BACKOFF_FACTOR**attempt
                    )
                    jitter = (
                        base_delay
                        * _INSERT_NEW_BACKOFF_JITTER
                        * (2 * random.random() - 1)
                    )
                    time.sleep(base_delay + jitter)
        raise WatermarkConcurrencyError(
            run_id=run_id, attempts=_INSERT_NEW_RETRY_BUDGET
        ) from last_exc

    def mark_failed(
        self,
        run_id: str,
        error_class: str,
        error_message: str,
    ) -> None:
        """
        Transition a run to ``status='failed'`` with truncated error fields.

        Idempotent on ``failed`` → ``failed`` (latest-wins on
        ``error_class`` / ``error_message`` / ``completed_at``). Refuses to
        overwrite a row already in ``status='completed'``: zero affected
        rows triggers a read-back and ``TerminalStateGuardError``
        (FR-L-06a / LHP-WM-002).

        Args:
            run_id: Run identifier.
            error_class: Exception class name (e.g. ``type(e).__name__``);
                stored verbatim, length-validated.
            error_message: Error description; truncated at 4096 characters.

        Raises:
            WatermarkValidationError: Inputs failed validation.
            TerminalStateGuardError: Row is in status='completed'.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.string(error_class, max_len=512)
        # error_message can legitimately contain quotes and longer text; the
        # 4096-char cap (FR-L-02) is the only validation. Normalise control
        # chars defensively by replacing them with spaces so the validator
        # accepts the truncated form.
        truncated = error_message[:4096]
        sanitised = "".join(
            (ch if (0x20 <= ord(ch) < 0x7F or ord(ch) > 0x7F) else " ")
            for ch in truncated
        )
        SQLInputValidator.string(sanitised, max_len=4096)

        ts_literal = sql_timestamp_literal(datetime.now(tz=timezone.utc))
        run_id_lit = sql_literal(run_id)

        fail_sql = f"""
            UPDATE {self.table_name}
            SET status = 'failed',
                error_class = {sql_literal(error_class)},
                error_message = {sql_literal(sanitised)},
                completed_at = {ts_literal}
            WHERE run_id = {run_id_lit}
              AND status IN ('running', 'failed', 'timed_out')
        """
        affected = self._update_with_affected_rows(fail_sql)

        if affected == 0:
            current_status = self._read_back_status(run_id_lit)
            raise TerminalStateGuardError(run_id=run_id, current_status=current_status)

        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            "Run marked as failed: run_id=%s, class=%s, error=%s (%.1f ms)",
            run_id,
            error_class,
            sanitised,
            duration_ms,
        )

    def mark_bronze_complete(self, run_id: str) -> None:
        """
        Mark bronze stage as complete for a run.

        Sets bronze_stage_complete=true and auto-completes the run
        (status='completed') if silver stage is already done.

        Args:
            run_id: Run identifier.
        """
        self._ensure_utc_session()
        start_time = time.time()
        self._validate_identifier(run_id, "run_id")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        update_sql = f"""
            UPDATE {self.table_name}
            SET bronze_stage_complete = true,
                status = CASE WHEN silver_stage_complete = true THEN 'completed' ELSE status END,
                completed_at = CASE WHEN silver_stage_complete = true THEN TIMESTAMP'{current_time}' ELSE completed_at END
            WHERE run_id = '{run_id}'
        """
        self.spark.sql(update_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Bronze stage marked complete: run_id=%s (%.1f ms)",
            run_id,
            duration_ms,
        )

    def mark_silver_complete(self, run_id: str) -> None:
        """
        Mark silver stage as complete for a run.

        Sets silver_stage_complete=true and auto-completes the run
        (status='completed') if bronze stage is already done.

        Args:
            run_id: Run identifier.
        """
        self._ensure_utc_session()
        start_time = time.time()
        self._validate_identifier(run_id, "run_id")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        update_sql = f"""
            UPDATE {self.table_name}
            SET silver_stage_complete = true,
                status = CASE WHEN bronze_stage_complete = true THEN 'completed' ELSE status END,
                completed_at = CASE WHEN bronze_stage_complete = true THEN TIMESTAMP'{current_time}' ELSE completed_at END
            WHERE run_id = '{run_id}'
        """
        self.spark.sql(update_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Silver stage marked complete: run_id=%s (%.1f ms)",
            run_id,
            duration_ms,
        )

    def mark_complete(
        self,
        run_id: str,
        watermark_value: Union[str, int, Decimal],
        row_count: int,
    ) -> None:
        """
        Transition a run from ``status='running'`` to ``status='completed'``.

        Wired into the extraction notebook outside the try/except per L2
        §5.3, so a failure here does not re-enter the failure path. The
        SQL ``WHERE`` clause carries the FR-L-06 terminal-failure guard:
        the UPDATE refuses to overwrite ``failed`` / ``timed_out`` /
        ``landed_not_committed`` rows. Zero affected rows triggers a
        read-back of the current status and raises
        ``TerminalStateGuardError`` (LHP-WM-002).

        Args:
            run_id: Run identifier.
            watermark_value: Final watermark value from the landed batch
                (str / int / Decimal). ``float`` is rejected.
            row_count: Number of rows extracted.

        Raises:
            WatermarkValidationError: An input failed validation.
            TerminalStateGuardError: Row is in a terminal-failure state.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.numeric(row_count)

        wm_val_sql = _render_watermark_literal(watermark_value)
        ts_literal = sql_timestamp_literal(datetime.now(tz=timezone.utc))
        run_id_lit = sql_literal(run_id)

        complete_sql = f"""
            UPDATE {self.table_name}
            SET status = 'completed',
                watermark_value = {wm_val_sql},
                row_count = {sql_numeric_literal(row_count)},
                completed_at = {ts_literal}
            WHERE run_id = {run_id_lit}
              AND status NOT IN ('failed', 'timed_out', 'landed_not_committed')
        """
        affected = self._update_with_affected_rows(complete_sql)

        if affected == 0:
            current_status = self._read_back_status(run_id_lit)
            raise TerminalStateGuardError(run_id=run_id, current_status=current_status)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Run marked as completed: run_id=%s, value=%s, rows=%d (%.1f ms)",
            run_id,
            watermark_value,
            row_count,
            duration_ms,
        )

    def _update_with_affected_rows(self, sql: str) -> int:
        """Execute an UPDATE / MERGE and return ``num_affected_rows``.

        Mocks and older Spark versions may omit the metric; treat that as
        success (1) so behaviour-only tests do not need to fabricate a
        result row. The terminal-guard semantics depend on an explicit
        zero, which Databricks does emit for UPDATE.
        """
        result = self.spark.sql(sql)
        row = result.first() if result is not None else None
        if row is None:
            return 1
        try:
            return int(row["num_affected_rows"])
        except (KeyError, IndexError, TypeError):
            return 1

    def _read_back_status(self, run_id_lit: str) -> Optional[str]:
        """Best-effort read of the current status for a TerminalStateGuardError.

        ``run_id_lit`` must already be SQL-quoted via ``sql_literal``.
        Returns ``None`` if the row has been deleted between the UPDATE
        and the read-back.
        """
        result = self.spark.sql(
            f"SELECT status FROM {self.table_name} WHERE run_id = {run_id_lit} LIMIT 1"
        )
        row = result.first() if result is not None else None
        if row is None:
            return None
        try:
            return row["status"]
        except (KeyError, IndexError, TypeError):
            return None

    @staticmethod
    def _validate_date(value: str, field_name: str) -> None:
        """
        Validate that a string matches YYYY-MM-DD date format.

        Args:
            value: The date string to validate.
            field_name: Name of the field (for error messages).

        Raises:
            ValueError: If the value does not match YYYY-MM-DD format.
        """
        if not _DATE_PATTERN.match(value):
            raise ValueError(
                f"Invalid {field_name}: '{value}'. " "Expected format: YYYY-MM-DD."
            )

    def cleanup_stale_runs(self, stale_timeout_hours: float = 4.0) -> int:
        """
        Mark runs stuck in 'running' past a timeout as 'timed_out'.

        Args:
            stale_timeout_hours: Hours after which a running job is stale.

        Returns:
            Number of runs marked as timed_out.
        """
        self._ensure_utc_session()
        start_time = time.time()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timeout_hours = int(stale_timeout_hours)

        count_sql = f"""
            SELECT COUNT(*) AS cnt
            FROM {self.table_name}
            WHERE status = 'running'
              AND created_at < TIMESTAMP'{current_time}' - INTERVAL {timeout_hours} HOURS
        """
        count_result = self.spark.sql(count_sql).collect()
        stale_count = int(count_result[0]["cnt"])

        if stale_count > 0:
            update_sql = f"""
                UPDATE {self.table_name}
                SET status = 'timed_out',
                    error_message = 'Run exceeded stale timeout ({stale_timeout_hours} hours)',
                    completed_at = TIMESTAMP'{current_time}'
                WHERE status = 'running'
                  AND created_at < TIMESTAMP'{current_time}' - INTERVAL {timeout_hours} HOURS
            """
            self.spark.sql(update_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Stale run cleanup: %d runs marked as timed_out (%.1f ms)",
            stale_count,
            duration_ms,
        )
        return stale_count

    def _build_history_conditions(
        self,
        source_system_id: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list:
        """
        Build WHERE conditions for history/monitoring queries.

        Validates non-None identifiers and dates before inclusion.

        Returns:
            List of SQL condition strings.
        """
        conditions: list = []

        if source_system_id is not None:
            self._validate_identifier(source_system_id, "source_system_id")
            conditions.append(f"source_system_id = '{source_system_id}'")
        if schema_name is not None:
            self._validate_identifier(schema_name, "schema_name")
            conditions.append(f"schema_name = '{schema_name}'")
        if table_name is not None:
            self._validate_identifier(table_name, "table_name")
            conditions.append(f"table_name = '{table_name}'")

        if start_date is not None:
            self._validate_date(start_date, "start_date")
            conditions.append(f"created_at >= '{start_date}'")
        elif end_date is None:
            conditions.append("created_at >= current_date() - INTERVAL 7 DAYS")

        if end_date is not None:
            self._validate_date(end_date, "end_date")
            conditions.append(f"created_at <= '{end_date}'")

        return conditions

    def get_run_history(
        self,
        source_system_id: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Any:
        """
        Query watermark run history with optional filters.

        Returns a Spark DataFrame (not collected) for downstream processing.

        Args:
            source_system_id: Filter by source system.
            schema_name: Filter by schema.
            table_name: Filter by table.
            start_date: Filter start (YYYY-MM-DD). Defaults to 7 days ago.
            end_date: Filter end (YYYY-MM-DD).

        Returns:
            Spark DataFrame of matching runs.
        """
        start_time = time.time()

        conditions = self._build_history_conditions(
            source_system_id,
            schema_name,
            table_name,
            start_date,
            end_date,
        )
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        sql = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY created_at DESC
        """
        result = self.spark.sql(sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info("Run history query executed (%.1f ms)", duration_ms)
        return result

    def get_failed_runs(
        self,
        source_system_id: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Any:
        """
        Query failed watermark runs with optional filters.

        Args:
            source_system_id: Filter by source system.
            schema_name: Filter by schema.
            table_name: Filter by table.
            start_date: Filter start (YYYY-MM-DD). Defaults to 7 days ago.
            end_date: Filter end (YYYY-MM-DD).

        Returns:
            Spark DataFrame of failed runs.
        """
        start_time = time.time()

        conditions = self._build_history_conditions(
            source_system_id,
            schema_name,
            table_name,
            start_date,
            end_date,
        )
        conditions.append("status = 'failed'")
        where_clause = " AND ".join(conditions)

        sql = f"""
            SELECT *
            FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY created_at DESC
        """
        result = self.spark.sql(sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info("Failed runs query executed (%.1f ms)", duration_ms)
        return result

    def get_stale_runs(self, stale_timeout_hours: float = 4.0) -> Any:
        """
        Query runs currently stuck in 'running' past the timeout.

        Args:
            stale_timeout_hours: Hours after which a running job is stale.

        Returns:
            Spark DataFrame of stale runs.
        """
        start_time = time.time()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timeout_hours = int(stale_timeout_hours)

        sql = f"""
            SELECT *
            FROM {self.table_name}
            WHERE status = 'running'
              AND created_at < TIMESTAMP'{current_time}' - INTERVAL {timeout_hours} HOURS
            ORDER BY created_at ASC
        """
        result = self.spark.sql(sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info("Stale runs query executed (%.1f ms)", duration_ms)
        return result

    def get_table_summary(self, source_system_id: Optional[str] = None) -> Any:
        """
        Get aggregated stats per table from watermark history.

        Args:
            source_system_id: Optional filter by source system.

        Returns:
            Spark DataFrame with per-table aggregated statistics.
        """
        start_time = time.time()

        where_clause = ""
        if source_system_id is not None:
            self._validate_identifier(source_system_id, "source_system_id")
            where_clause = f"WHERE source_system_id = '{source_system_id}'"

        sql = f"""
            SELECT source_system_id, schema_name, table_name,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_runs,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
                   MAX(watermark_value) AS latest_watermark_value,
                   MAX(completed_at) AS last_completed_at,
                   SUM(row_count) AS total_rows
            FROM {self.table_name}
            {where_clause}
            GROUP BY source_system_id, schema_name, table_name
            ORDER BY source_system_id, schema_name, table_name
        """
        result = self.spark.sql(sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info("Table summary query executed (%.1f ms)", duration_ms)
        return result
