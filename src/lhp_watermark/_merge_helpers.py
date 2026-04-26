"""Shared MERGE-with-retry helper for Delta optimistic-concurrency writes.

Extracted from ``WatermarkManager._merge_with_retry`` so B2 ``b2_manifests``
writes (LHP plan 2026-04-25-002 / R1) share the same retry semantics as the
``watermarks`` registry without re-implementing the loop.

Design:
- Pure function — no class state, no I/O beyond ``spark.sql(...)`` + ``time.sleep``.
- Caller supplies the exhaustion callback so each call site raises an
  appropriately-typed error (``WatermarkConcurrencyError`` for watermarks;
  a manifest-specific error for B2).
- Concurrent-commit detection logic shared via ``is_concurrent_commit_exception``.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, NoReturn, Optional

logger = logging.getLogger(__name__)

# Delta concurrent-commit exception class names. Detection is by string
# (not isinstance) because Delta is not installed in unit-test environments
# and the exception may arrive Py4J-wrapped on Databricks.
CONCURRENT_COMMIT_NAMES = (
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
)

# Default retry budget tuned for FR-L-05 / NFR-L-02. Reducing shard size is
# the right response to genuine over-contention; raising the budget hides
# the cause.
DEFAULT_RETRY_BUDGET = 5
DEFAULT_BACKOFF_BASE_SECS = 0.1
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_BACKOFF_JITTER = 0.5  # ±50 % of the base delay


def is_concurrent_commit_exception(exc: BaseException) -> bool:
    """Match Delta concurrent-commit exceptions by class name + message text.

    Direct class match handles plain Python raises in tests; message match
    catches the Py4J-wrapped form Databricks surfaces in production.
    """
    if type(exc).__name__ in CONCURRENT_COMMIT_NAMES:
        return True
    msg = str(exc)
    return any(name in msg for name in CONCURRENT_COMMIT_NAMES)


def execute_with_concurrent_commit_retry(
    spark: Any,
    merge_sql: str,
    *,
    on_exhausted: Callable[[Optional[BaseException]], NoReturn],
    retry_budget: int = DEFAULT_RETRY_BUDGET,
    backoff_base_secs: float = DEFAULT_BACKOFF_BASE_SECS,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    backoff_jitter: float = DEFAULT_BACKOFF_JITTER,
) -> int:
    """Execute a MERGE statement with Delta concurrent-commit retry.

    Returns ``num_affected_rows`` from the MERGE result. When the metric
    is missing (older Spark / mocks), returns ``1`` so callers do not
    spuriously raise on the absence of metadata.

    Re-raises any non-concurrent-commit exception unchanged. On retry-budget
    exhaustion calls ``on_exhausted(last_exc)`` which must raise; the
    helper does not raise on its own past the user-supplied callback.

    Args:
        spark: SparkSession (or duck-typed equivalent with ``.sql(stmt)``).
        merge_sql: Pre-composed MERGE statement (caller is responsible for
            SQL safety via ``lhp_watermark.sql_safety``).
        on_exhausted: Callback invoked with the final concurrent-commit
            exception when the retry budget is exhausted. Must raise
            (return type ``NoReturn``).
        retry_budget: Maximum number of attempts. Default 5.
        backoff_base_secs: Base delay in seconds. Default 0.1.
        backoff_factor: Exponential factor between attempts. Default 2.0.
        backoff_jitter: Jitter as fraction of base delay (±). Default 0.5.

    Returns:
        ``num_affected_rows`` from the MERGE result.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(retry_budget):
        try:
            result = spark.sql(merge_sql)
            row = result.first() if result is not None else None
            if row is None:
                # Older Spark versions / mocks may not surface the metric.
                # Treat as success — the caller decides what zero means.
                return 1
            try:
                return int(row["num_affected_rows"])
            except (KeyError, IndexError, TypeError):
                return 1
        except BaseException as exc:  # noqa: BLE001 — re-raise unrelated below
            if not is_concurrent_commit_exception(exc):
                raise
            last_exc = exc
            if attempt + 1 < retry_budget:
                base_delay = backoff_base_secs * (backoff_factor**attempt)
                jitter = base_delay * backoff_jitter * (2 * random.random() - 1)
                time.sleep(base_delay + jitter)
    on_exhausted(last_exc)
    # on_exhausted is typed NoReturn; this line is unreachable but
    # satisfies static type checkers that cannot prove the callback raises.
    raise AssertionError("on_exhausted callback returned without raising")
