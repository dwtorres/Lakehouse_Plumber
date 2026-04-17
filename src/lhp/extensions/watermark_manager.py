"""
Watermark manager for incremental data ingestion.

Manages high-water marks for tracking incremental data loads using
Delta Lake tables with liquid clustering for concurrent writes.

Simplified port of data_platform WatermarkManager for Lakehouse Plumber.
"""

import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.\-]+$")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


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

    def _ensure_table_exists(self) -> None:
        """
        Ensure watermarks table exists with proper schema.

        Creates the Delta table with liquid clustering if it does not exist.
        Unity Catalog DDL only.
        """
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
        Get latest watermark for a table.

        Args:
            source_system_id: Source system identifier.
            schema_name: Schema name.
            table_name: Table name.

        Returns:
            Dictionary with watermark details or None if no watermark exists.
        """
        start_time = time.time()

        self._validate_identifier(source_system_id, "source_system_id")
        self._validate_identifier(schema_name, "schema_name")
        self._validate_identifier(table_name, "table_name")

        result = self.spark.sql(
            f"""
            SELECT
                run_id,
                watermark_value,
                watermark_time,
                row_count,
                status,
                bronze_stage_complete,
                silver_stage_complete
            FROM {self.table_name}
            WHERE source_system_id = '{source_system_id}'
              AND schema_name = '{schema_name}'
              AND table_name = '{table_name}'
              AND watermark_time = (
                SELECT MAX(watermark_time)
                FROM {self.table_name}
                WHERE source_system_id = '{source_system_id}'
                  AND schema_name = '{schema_name}'
                  AND table_name = '{table_name}'
              )
            LIMIT 1
        """
        ).collect()

        duration_ms = (time.time() - start_time) * 1000

        if not result:
            logger.info(
                "No watermark found for %s.%s.%s (%.1f ms)",
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
        watermark_value: Optional[str],
        row_count: int = 0,
        extraction_type: str = "incremental",
        previous_watermark_value: Optional[str] = None,
    ) -> None:
        """
        Insert new watermark entry using MERGE to prevent duplicate run_ids.

        Args:
            run_id: Unique identifier for this run.
            source_system_id: Source system identifier.
            schema_name: Schema name.
            table_name: Table name.
            watermark_column_name: Column used for watermarking.
            watermark_value: Current watermark value.
            row_count: Number of rows processed.
            extraction_type: One of 'incremental', 'full', 'snapshot', 'merge'.
            previous_watermark_value: Previous watermark value for reference.

        Raises:
            ValueError: If run_id already exists or identifiers are invalid.
        """
        start_time = time.time()

        self._validate_identifier(run_id, "run_id")
        self._validate_identifier(source_system_id, "source_system_id")
        self._validate_identifier(schema_name, "schema_name")
        self._validate_identifier(table_name, "table_name")

        existing = self.spark.sql(
            f"SELECT run_id FROM {self.table_name} WHERE run_id = '{run_id}'"
        ).collect()
        if existing:
            raise ValueError(f"Run ID '{run_id}' already exists in watermarks table")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        watermark_col_sql = (
            f"'{watermark_column_name}'" if watermark_column_name else "NULL"
        )
        watermark_val_sql = f"'{watermark_value}'" if watermark_value else "NULL"
        prev_watermark_sql = (
            f"'{previous_watermark_value}'" if previous_watermark_value else "NULL"
        )

        merge_sql = f"""
            MERGE INTO {self.table_name} AS t
            USING (
                SELECT
                    '{run_id}' AS run_id,
                    TIMESTAMP'{current_time}' AS watermark_time,
                    '{source_system_id}' AS source_system_id,
                    '{schema_name}' AS schema_name,
                    '{table_name}' AS table_name,
                    {watermark_col_sql} AS watermark_column_name,
                    {watermark_val_sql} AS watermark_value,
                    {prev_watermark_sql} AS previous_watermark_value,
                    {row_count} AS row_count,
                    '{extraction_type}' AS extraction_type,
                    false AS bronze_stage_complete,
                    false AS silver_stage_complete,
                    'running' AS status,
                    TIMESTAMP'{current_time}' AS created_at
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

        self.spark.sql(merge_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Watermark inserted: run_id=%s, value=%s, rows=%d (%.1f ms)",
            run_id,
            watermark_value,
            row_count,
            duration_ms,
        )

    def mark_failed(
        self,
        run_id: str,
        error_message: str,
    ) -> None:
        """
        Mark a run as failed.

        Args:
            run_id: Run identifier.
            error_message: Error description.
        """
        start_time = time.time()

        self._validate_identifier(run_id, "run_id")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_message_escaped = error_message.replace("'", "''")

        fail_sql = f"""
            UPDATE {self.table_name}
            SET status = 'failed',
                error_message = '{error_message_escaped}',
                completed_at = TIMESTAMP'{current_time}'
            WHERE run_id = '{run_id}'
        """
        self.spark.sql(fail_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.error(
            "Run marked as failed: run_id=%s, error=%s (%.1f ms)",
            run_id,
            error_message,
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
        watermark_value: Optional[str],
        row_count: int,
    ) -> None:
        """
        Mark a run as completed with the final watermark value.

        Only succeeds when both bronze_stage_complete and silver_stage_complete
        are true. Acts as a force-complete guard.

        Args:
            run_id: Run identifier.
            watermark_value: Final watermark value from landed data.
            row_count: Number of rows extracted.
        """
        start_time = time.time()
        self._validate_identifier(run_id, "run_id")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        watermark_val_sql = f"'{watermark_value}'" if watermark_value else "NULL"

        complete_sql = f"""
            UPDATE {self.table_name}
            SET status = 'completed',
                watermark_value = {watermark_val_sql},
                row_count = {row_count},
                completed_at = TIMESTAMP'{current_time}'
            WHERE run_id = '{run_id}'
              AND bronze_stage_complete = true
              AND silver_stage_complete = true
        """
        self.spark.sql(complete_sql)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Run marked as completed: run_id=%s, value=%s, rows=%d (%.1f ms)",
            run_id,
            watermark_value,
            row_count,
            duration_ms,
        )

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
