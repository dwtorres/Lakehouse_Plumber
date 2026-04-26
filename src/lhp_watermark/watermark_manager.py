"""
Watermark manager for incremental data ingestion.

Manages high-water marks for tracking incremental data loads using
Delta Lake tables with liquid clustering for concurrent writes.

Simplified port of data_platform WatermarkManager for Lakehouse Plumber.
"""

import logging
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Union

from pyspark.sql import SparkSession

from lhp_watermark._merge_helpers import (
    CONCURRENT_COMMIT_NAMES as _CONCURRENT_COMMIT_NAMES,
    DEFAULT_BACKOFF_BASE_SECS as _INSERT_NEW_BACKOFF_BASE_SECS,
    DEFAULT_BACKOFF_FACTOR as _INSERT_NEW_BACKOFF_FACTOR,
    DEFAULT_BACKOFF_JITTER as _INSERT_NEW_BACKOFF_JITTER,
    DEFAULT_RETRY_BUDGET as _INSERT_NEW_RETRY_BUDGET,
    execute_with_concurrent_commit_retry,
    is_concurrent_commit_exception as _is_concurrent_commit_exception,
)
from lhp_watermark.exceptions import (
    DuplicateRunError,
    TerminalStateGuardError,
    WatermarkConcurrencyError,
)
from lhp_watermark.sql_safety import (
    SQLInputValidator,
    sql_literal,
    sql_numeric_literal,
    sql_timestamp_literal,
)

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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


def _render_load_group_literal(load_group: Optional[str]) -> str:
    """Render a ``load_group`` value as a SQL literal or ``NULL``.

    ``load_group`` is data-shaped (e.g. ``'pipe_a::fg_a'``) — validate via
    ``SQLInputValidator.string`` (the identifier validator rejects ``::``).
    ``None`` becomes SQL ``NULL`` so legacy callers and pre-Tier-2 rows
    keep matching the right arm of the three-way filter.
    """
    if load_group is None:
        return "NULL"
    SQLInputValidator.string(load_group)
    return sql_literal(load_group)


def _compose_load_group_clause(load_group: Optional[str]) -> str:
    """Compose the three-way ``load_group`` WHERE arm (R4).

    Always emits both arms so legacy callers (``load_group=None``) and B2
    callers (explicit composite) coexist during migration. Runtime collapses
    based on ``load_group``:

    - ``load_group is None`` → both literal substitutions are SQL ``NULL``,
      the left arm evaluates to ``NULL = NULL`` (false in three-valued
      logic), the right arm collapses to ``IS NULL`` and matches legacy +
      pre-Tier-2 NULL-load_group rows.
    - ``load_group`` is a composite (e.g. ``'pipe_a::fg_a'``) → left arm
      matches the row's ``load_group``; right arm degenerates to false.

    Composes through ``sql_literal`` (never f-string concatenation of user
    data); ``load_group`` is validated by ``_render_load_group_literal``.
    """
    lg_sql = _render_load_group_literal(load_group)
    return f"AND ( load_group = {lg_sql} OR ({lg_sql} IS NULL AND load_group IS NULL) )"


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

    # Tier 2 target shape for the watermarks Delta table (R1, R2):
    #   - column ``load_group STRING`` (nullable; back-compat for legacy callers)
    #   - liquid clustering on (source_system_id, load_group, schema_name, table_name)
    # _TARGET_CLUSTERING is the ordered list against which DESCRIBE DETAIL output
    # is compared on every init; mismatch triggers a single ALTER … CLUSTER BY.
    _TARGET_CLUSTERING = (
        "source_system_id",
        "load_group",
        "schema_name",
        "table_name",
    )

    def _ensure_table_exists(self) -> None:
        """
        Ensure watermarks table exists with the Tier 2 schema + clustering.

        - Brand-new tables: ``CREATE TABLE`` with ``load_group STRING`` and
          liquid clustering on ``(source_system_id, load_group, schema_name,
          table_name)`` (R1, R2).
        - Pre-existing tables: probe column list via ``DESCRIBE TABLE`` and
          add ``load_group`` only if missing; probe clustering via
          ``DESCRIBE DETAIL`` and re-cluster only if the current key
          differs from the target. Both probes short-circuit when the
          target shape is already in place — zero ALTER SQL on subsequent
          inits (idempotent, no metadata churn).

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
                    load_group STRING,
                    CONSTRAINT pk_watermarks PRIMARY KEY (run_id)
                )
                USING DELTA
                CLUSTER BY (source_system_id, load_group, schema_name, table_name)
                TBLPROPERTIES (
                    'delta.enableChangeDataFeed' = 'true',
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """
            self.spark.sql(ddl)

            duration_ms = (time.time() - start_time) * 1000
            logger.info("Watermarks table created successfully (%.1f ms)", duration_ms)
            return

        # Pre-existing table path: conditional ALTERs gated on probes.
        if not self._has_load_group_column():
            self._add_load_group_column()

        if not self._clustering_matches_target():
            self._alter_clustering_to_target()

        duration_ms = (time.time() - start_time) * 1000
        logger.debug(
            "Watermarks table existence/shape check complete (%.1f ms)",
            duration_ms,
        )

    def _has_load_group_column(self) -> bool:
        """Probe column list via ``DESCRIBE TABLE``; return True if ``load_group`` present.

        ``DESCRIBE TABLE`` returns one row per column with a ``col_name``
        field; partition-info trailing rows have empty/sentinel names which
        the membership check tolerates.
        """
        rows = self.spark.sql(f"DESCRIBE TABLE {self.table_name}").collect()
        for row in rows:
            try:
                col_name = row["col_name"]
            except (KeyError, IndexError, TypeError):
                continue
            if col_name == "load_group":
                return True
        return False

    def _clustering_matches_target(self) -> bool:
        """Probe Delta clustering via ``DESCRIBE DETAIL``; True iff matches target.

        Target order is ``_TARGET_CLUSTERING``. Any mismatch — different
        columns, different order, or absence of clustering — returns False
        and triggers a re-cluster.
        """
        rows = self.spark.sql(f"DESCRIBE DETAIL {self.table_name}").collect()
        if not rows:
            return False
        first = rows[0]
        try:
            current = first["clusteringColumns"]
        except (KeyError, IndexError, TypeError):
            return False
        if current is None:
            return False
        return tuple(current) == self._TARGET_CLUSTERING

    def _add_load_group_column(self) -> None:
        """Emit ``ALTER TABLE … ADD COLUMNS (load_group STRING)``; surface concurrency errors typed."""
        logger.info(
            "Adding load_group column to watermarks table: %s", self.table_name
        )
        sql = f"ALTER TABLE {self.table_name} ADD COLUMNS (load_group STRING)"
        try:
            self.spark.sql(sql)
        except BaseException as exc:  # noqa: BLE001 — typed surface below
            logger.error(
                "ALTER TABLE ADD COLUMNS failed on %s: %s", self.table_name, exc
            )
            if _is_concurrent_commit_exception(exc):
                raise WatermarkConcurrencyError(
                    run_id=f"ddl:{self.table_name}",
                    attempts=1,
                ) from exc
            raise

    def _alter_clustering_to_target(self) -> None:
        """Emit ``ALTER TABLE … CLUSTER BY (...)``; surface concurrency errors typed."""
        target = ", ".join(self._TARGET_CLUSTERING)
        logger.info(
            "Switching clustering on watermarks table %s to (%s)",
            self.table_name,
            target,
        )
        sql = f"ALTER TABLE {self.table_name} CLUSTER BY ({target})"
        try:
            self.spark.sql(sql)
        except BaseException as exc:  # noqa: BLE001 — typed surface below
            logger.error(
                "ALTER TABLE CLUSTER BY failed on %s: %s", self.table_name, exc
            )
            if _is_concurrent_commit_exception(exc):
                raise WatermarkConcurrencyError(
                    run_id=f"ddl:{self.table_name}",
                    attempts=1,
                ) from exc
            raise

    def get_latest_watermark(
        self,
        source_system_id: str,
        schema_name: str,
        table_name: str,
        load_group: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Return the most recent terminal-success watermark, or ``None``.

        Filters strictly by ``status='completed'`` so non-terminal rows
        (``running``, ``failed``, ``timed_out``, ``landed_not_committed``)
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
            load_group: Optional Tier 2 axis value
                (e.g. ``f"{{pipeline}}::{{flowgroup}}"``). ``None`` (default,
                legacy callers) matches NULL-load_group rows via the right
                arm of the three-way filter. Non-None matches only rows
                with that exact ``load_group`` value (R3, R4).

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

        load_group_clause = _compose_load_group_clause(load_group)

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
              {load_group_clause}
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

    def get_recoverable_landed_run(
        self,
        source_system_id: str,
        schema_name: str,
        table_name: str,
        load_group: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the latest landed-but-not-finalized run for a table key.

        This is the recovery hook for jdbc_watermark_v2 extraction notebooks.
        If a durable landed batch exists but finalization failed, the next run
        can finalize it before opening JDBC again.

        Args:
            source_system_id: Source system identifier.
            schema_name: Schema name.
            table_name: Table name.
            load_group: Optional Tier 2 axis value. Three-way filter
                semantics match ``get_latest_watermark`` (R3, R4): ``None``
                matches NULL-load_group rows; non-None matches only the
                exact composite.
        """
        self._ensure_utc_session()

        SQLInputValidator.identifier(source_system_id)
        SQLInputValidator.identifier(schema_name)
        SQLInputValidator.identifier(table_name)

        load_group_clause = _compose_load_group_clause(load_group)

        query = f"""
            SELECT
                run_id,
                watermark_value,
                row_count,
                status,
                created_at
            FROM {self.table_name}
            WHERE source_system_id = {sql_literal(source_system_id)}
              AND schema_name = {sql_literal(schema_name)}
              AND table_name = {sql_literal(table_name)}
              AND status = 'landed_not_committed'
              {load_group_clause}
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
        """
        result = self.spark.sql(query).collect()
        if not result:
            return None

        row = result[0]
        recoverable = {
            "run_id": row["run_id"],
            "watermark_value": row["watermark_value"],
            "row_count": row["row_count"],
            "status": row["status"],
            "created_at": row["created_at"],
        }
        logger.warning(
            "Recoverable landed run found for %s.%s.%s: run_id=%s",
            source_system_id,
            schema_name,
            table_name,
            recoverable["run_id"],
        )
        return recoverable

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
        load_group: Optional[str] = None,
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
            load_group: Optional Tier 2 axis value
                (e.g. ``f"{{pipeline}}::{{flowgroup}}"``). ``None`` (legacy
                callers) writes SQL ``NULL`` into the row. Non-None values
                are validated via ``SQLInputValidator.string`` and persisted
                so subsequent reads filtered by the same ``load_group``
                isolate this run from cross-flowgroup collisions (R3).

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
        load_group_sql = _render_load_group_literal(load_group)
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
                    {ts_literal} AS created_at,
                    {load_group_sql} AS load_group
            ) AS s
            ON t.run_id = s.run_id
            WHEN NOT MATCHED THEN INSERT (
                run_id, watermark_time, source_system_id, schema_name,
                table_name, watermark_column_name, watermark_value,
                previous_watermark_value, row_count, extraction_type,
                bronze_stage_complete, silver_stage_complete, status,
                created_at, load_group
            )
            VALUES (
                s.run_id, s.watermark_time, s.source_system_id, s.schema_name,
                s.table_name, s.watermark_column_name, s.watermark_value,
                s.previous_watermark_value, s.row_count, s.extraction_type,
                s.bronze_stage_complete, s.silver_stage_complete, s.status,
                s.created_at, s.load_group
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

        Thin wrapper over ``execute_with_concurrent_commit_retry`` that
        raises ``WatermarkConcurrencyError`` (LHP-WM-004) on budget
        exhaustion. The retry loop, backoff, and concurrent-commit
        detection live in ``lhp_watermark._merge_helpers``.
        """

        def _on_exhausted(last_exc: Optional[BaseException]) -> None:
            raise WatermarkConcurrencyError(
                run_id=run_id, attempts=_INSERT_NEW_RETRY_BUDGET
            ) from last_exc

        return execute_with_concurrent_commit_retry(
            self.spark,
            merge_sql,
            on_exhausted=_on_exhausted,
            retry_budget=_INSERT_NEW_RETRY_BUDGET,
            backoff_base_secs=_INSERT_NEW_BACKOFF_BASE_SECS,
            backoff_factor=_INSERT_NEW_BACKOFF_FACTOR,
            backoff_jitter=_INSERT_NEW_BACKOFF_JITTER,
        )

    def mark_failed(
        self,
        run_id: str,
        error_class: str,
        error_message: str,
        load_group: Optional[str] = None,
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
            load_group: Tier 2 axis value (R3). Store-only here — the
                UPDATE filters by ``run_id`` exclusively, so the kwarg
                neither alters the WHERE shape nor the SET clause; it is
                accepted to keep call-site signatures uniform across the
                state machine. Validated when non-None so a malformed
                value raises before any SQL is emitted.

        Raises:
            WatermarkValidationError: Inputs failed validation.
            TerminalStateGuardError: Row is in status='completed'.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.string(error_class, max_len=512)
        if load_group is not None:
            SQLInputValidator.string(load_group)
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

    def mark_landed(
        self,
        run_id: str,
        watermark_value: Union[str, int, Decimal],
        row_count: int,
        load_group: Optional[str] = None,
    ) -> None:
        """Persist that files were durably landed but not yet finalized.

        Args:
            run_id: Run identifier.
            watermark_value: Final watermark value from the landed batch.
            row_count: Number of rows landed.
            load_group: Tier 2 axis value (R3). Store-only — the UPDATE
                filters by ``run_id`` exclusively. Validated when non-None.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.numeric(row_count)
        if load_group is not None:
            SQLInputValidator.string(load_group)

        wm_val_sql = _render_watermark_literal(watermark_value)
        run_id_lit = sql_literal(run_id)

        landed_sql = f"""
            UPDATE {self.table_name}
            SET status = 'landed_not_committed',
                watermark_value = {wm_val_sql},
                row_count = {sql_numeric_literal(row_count)}
            WHERE run_id = {run_id_lit}
              AND status = 'running'
        """
        affected = self._update_with_affected_rows(landed_sql)

        if affected == 0:
            current_status = self._read_back_status(run_id_lit)
            raise TerminalStateGuardError(run_id=run_id, current_status=current_status)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Run marked as landed_not_committed: run_id=%s, value=%s, rows=%d (%.1f ms)",
            run_id,
            watermark_value,
            row_count,
            duration_ms,
        )

    def mark_bronze_complete(
        self,
        run_id: str,
        load_group: Optional[str] = None,
    ) -> None:
        """
        Mark bronze stage complete; auto-promote status if silver is already done.

        Adds the FR-L-06 terminal-failure guard: refuses to overwrite a row
        in ``failed`` / ``timed_out`` / ``landed_not_committed`` and raises
        ``TerminalStateGuardError`` (LHP-WM-002) when the UPDATE matches
        zero rows.

        Args:
            run_id: Run identifier.
            load_group: Tier 2 axis value (R3). Store-only — propagated to
                ``_stage_complete``; the UPDATE filters by ``run_id``
                exclusively.

        Raises:
            WatermarkValidationError: ``run_id`` failed validation.
            TerminalStateGuardError: Row is in a terminal-failure state.
        """
        self._stage_complete(
            run_id=run_id,
            stage_flag="bronze_stage_complete",
            other_flag="silver_stage_complete",
            log_msg="Bronze stage marked complete",
            load_group=load_group,
        )

    def mark_silver_complete(
        self,
        run_id: str,
        load_group: Optional[str] = None,
    ) -> None:
        """
        Mark silver stage complete; auto-promote status if bronze is already done.

        See ``mark_bronze_complete`` for the shared semantics + guards.
        """
        self._stage_complete(
            run_id=run_id,
            stage_flag="silver_stage_complete",
            other_flag="bronze_stage_complete",
            log_msg="Silver stage marked complete",
            load_group=load_group,
        )

    def _stage_complete(
        self,
        *,
        run_id: str,
        stage_flag: str,
        other_flag: str,
        log_msg: str,
        load_group: Optional[str] = None,
    ) -> None:
        """Shared implementation for bronze/silver stage-complete UPDATE.

        ``stage_flag`` is the column to set TRUE; ``other_flag`` is the
        sibling whose value triggers status auto-promotion to 'completed'.
        Both are internal column names — never user input — so they are
        not interpolated through SQLInputValidator.

        ``load_group`` is store-only — accepted for signature uniformity
        across the state machine; the UPDATE filters by ``run_id`` only.
        Validated when non-None so a malformed value raises before SQL
        composition.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        if load_group is not None:
            SQLInputValidator.string(load_group)
        run_id_lit = sql_literal(run_id)
        ts_literal = sql_timestamp_literal(datetime.now(tz=timezone.utc))

        update_sql = f"""
            UPDATE {self.table_name}
            SET {stage_flag} = true,
                status = CASE WHEN {other_flag} = true THEN 'completed' ELSE status END,
                completed_at = CASE WHEN {other_flag} = true THEN {ts_literal} ELSE completed_at END
            WHERE run_id = {run_id_lit}
              AND status NOT IN ('failed', 'timed_out', 'landed_not_committed')
        """
        affected = self._update_with_affected_rows(update_sql)

        if affected == 0:
            current_status = self._read_back_status(run_id_lit)
            raise TerminalStateGuardError(run_id=run_id, current_status=current_status)

        duration_ms = (time.time() - start_time) * 1000
        logger.info("%s: run_id=%s (%.1f ms)", log_msg, run_id, duration_ms)

    def mark_complete(
        self,
        run_id: str,
        watermark_value: Union[str, int, Decimal],
        row_count: int,
        load_group: Optional[str] = None,
    ) -> None:
        """
        Transition a run from ``status='running'`` to ``status='completed'``.

        Wired into the extraction notebook outside the try/except per L2
        §5.3, so a failure here does not re-enter the failure path. The
        SQL ``WHERE`` clause carries the FR-L-06 terminal-failure guard:
        the UPDATE only accepts ``running`` or ``landed_not_committed`` rows.
        Zero affected rows triggers a
        read-back of the current status and raises
        ``TerminalStateGuardError`` (LHP-WM-002).

        Args:
            run_id: Run identifier.
            watermark_value: Final watermark value from the landed batch
                (str / int / Decimal). ``float`` is rejected.
            row_count: Number of rows extracted.
            load_group: Tier 2 axis value (R3). Store-only — the UPDATE
                filters by ``run_id`` exclusively. Validated when non-None.

        Raises:
            WatermarkValidationError: An input failed validation.
            TerminalStateGuardError: Row is in a terminal-failure state.
        """
        self._ensure_utc_session()
        start_time = time.time()

        SQLInputValidator.uuid_or_job_run_id(run_id)
        SQLInputValidator.numeric(row_count)
        if load_group is not None:
            SQLInputValidator.string(load_group)

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
              AND status IN ('running', 'landed_not_committed')
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
