#!/usr/bin/env python3
"""Python-side overhead benchmark for the Slice A watermark lifecycle (AC-SA-40).

Measures the in-process cost of the four DML methods that Slice A
hardened on ``WatermarkManager``:

- ``insert_new``  (FR-L-05: MERGE + validator + emitter composition)
- ``mark_complete`` (FR-L-01/06: terminal-guard + watermark_value arg)
- ``mark_failed`` (FR-L-02/06a: terminal-guard + latest-wins)
- ``get_latest_watermark`` (FR-L-04: completion filter + tie-break)

The harness substitutes a no-op Spark mock, so every SQL statement
returns instantly and the timing reflects *only* the Python-side
additions: validator calls, emitter composition, try/except, and the
retry-loop branch that is not taken on the happy path. Real Databricks
extraction time is dominated by JDBC read + Parquet write + Delta
commit (seconds to minutes); Python overhead below that layer is the
component Slice A actually added.

The verdict compares summed per-lifecycle overhead to a caller-supplied
extraction budget (default 1 second). If the overhead exceeds 5 % of
the budget, FR-L-10's exit criterion is not met on code-side; escalate
before shipping. End-to-end cluster timing is operator-gated and
reported separately in ``BENCHMARK.md``.

Usage::

    python scripts/benchmark_slice_a.py [--iterations N] [--json]
                                         [--extraction-budget-us USEC]
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from typing import Any, Callable, Dict, List, Optional

from lhp.extensions.watermark_manager import WatermarkManager

# The manager emits INFO/ERROR per DML call (context-useful in production,
# noise in a benchmark that calls each method thousands of times). Silence
# the extensions package for the duration of the process; callers who need
# the logs can restore the level after importing.
logging.getLogger("lhp.extensions.watermark_manager").setLevel(logging.CRITICAL)
logging.getLogger("lhp.extensions").setLevel(logging.CRITICAL)


# ------------------------- spark stub --------------------------------


class _NoopResult:
    """Mimic ``spark.sql(...)`` return; .first() / .collect() are cheap."""

    def first(self) -> Optional[Dict[str, Any]]:
        return {"num_affected_rows": 1, "status": "running"}

    def collect(self) -> List[Dict[str, Any]]:
        return []


class _NoopConf:
    def set(self, _k: str, _v: str) -> None:
        return None


class NoopSpark:
    """Minimal SparkSession substitute.

    Pretends the watermarks table already exists so
    ``WatermarkManager.__init__`` does not try to CREATE it. Returns a
    single-row result for every ``sql()`` call so the mgr's
    ``num_affected_rows`` read path exercises the same branches as on
    real Databricks.
    """

    def __init__(self) -> None:
        self.conf = _NoopConf()

    def sql(self, _query: str) -> _NoopResult:
        return _NoopResult()


def _build_manager() -> WatermarkManager:
    """Construct a WatermarkManager against NoopSpark.

    The factory ``SHOW TABLES`` call returns an empty list from
    ``NoopResult.collect``, so ``_ensure_table_exists`` creates a
    (no-op) table. That path runs exactly once per manager construction
    and is not inside the measured hot loop.
    """
    spark = NoopSpark()
    mgr = WatermarkManager(spark, catalog="metadata", schema="orchestration")
    return mgr


# ------------------------- benchmark runners ------------------------


def _time_us(fn: Callable[[], None], iterations: int) -> List[float]:
    """Run ``fn`` ``iterations`` times; return per-call durations in microseconds."""
    durations: List[float] = []
    # Prime any first-run caches (Decimal import, re compilations, etc.).
    fn()
    for _ in range(iterations):
        start = time.perf_counter_ns()
        fn()
        end = time.perf_counter_ns()
        durations.append((end - start) / 1000.0)
    return durations


def _summary(durations: List[float]) -> Dict[str, float]:
    if not durations:
        return {"mean_us": 0.0, "p50_us": 0.0, "p95_us": 0.0, "p99_us": 0.0}
    ordered = sorted(durations)
    n = len(ordered)
    p50 = ordered[n // 2]
    p95 = ordered[min(n - 1, int(n * 0.95))]
    p99 = ordered[min(n - 1, int(n * 0.99))]
    return {
        "mean_us": statistics.fmean(ordered),
        "p50_us": p50,
        "p95_us": p95,
        "p99_us": p99,
    }


def run_benchmark(
    iterations: int = 1000,
    extraction_budget_us: float = 1_000_000.0,
) -> Dict[str, Any]:
    """Exercise each lifecycle method N times; return a summary dict.

    ``extraction_budget_us`` is the caller's assumed end-to-end
    extraction duration (default 1 000 000 us = 1 second). The verdict
    asserts that per-lifecycle summed mean overhead stays under 5 % of
    this budget.
    """
    mgr = _build_manager()

    run_id = "job-1-task-1-attempt-1"
    sys_id = "pg_crm"
    schema = "production"
    table = "product"

    def _insert() -> None:
        mgr.insert_new(
            run_id=run_id,
            source_system_id=sys_id,
            schema_name=schema,
            table_name=table,
            watermark_column_name="ModifiedDate",
            watermark_value="2025-06-01T00:00:00+00:00",
            row_count=0,
            extraction_type="incremental",
            previous_watermark_value=None,
        )

    def _complete() -> None:
        mgr.mark_complete(
            run_id=run_id,
            watermark_value="2025-06-01T00:00:00+00:00",
            row_count=100,
        )

    def _fail() -> None:
        mgr.mark_failed(
            run_id=run_id,
            error_class="IOError",
            error_message="connection reset",
        )

    def _latest() -> None:
        mgr.get_latest_watermark(
            source_system_id=sys_id,
            schema_name=schema,
            table_name=table,
        )

    methods: Dict[str, Dict[str, float]] = {
        "insert_new": _summary(_time_us(_insert, iterations)),
        "mark_complete": _summary(_time_us(_complete, iterations)),
        "mark_failed": _summary(_time_us(_fail, iterations)),
        "get_latest_watermark": _summary(_time_us(_latest, iterations)),
    }

    # A single extraction burns: one insert_new + one get_latest_watermark +
    # exactly one terminal call (complete OR failed). Sum the worst-case
    # (complete path) for the overhead figure.
    total_overhead_us = (
        methods["insert_new"]["mean_us"]
        + methods["get_latest_watermark"]["mean_us"]
        + methods["mark_complete"]["mean_us"]
    )
    overhead_pct = (
        (total_overhead_us / extraction_budget_us) * 100.0
        if extraction_budget_us > 0
        else 0.0
    )
    verdict = "PASS" if overhead_pct <= 5.0 else "FAIL"

    return {
        "iterations": iterations,
        "extraction_budget_us": extraction_budget_us,
        "methods": methods,
        "total_overhead_us": total_overhead_us,
        "overhead_pct": overhead_pct,
        "verdict": verdict,
    }


# ------------------------- formatting -------------------------------


def _format_markdown(result: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(
        f"# Slice A Python-side Overhead Benchmark"
    )
    lines.append("")
    lines.append(f"- iterations: {result['iterations']}")
    lines.append(
        f"- extraction budget: {result['extraction_budget_us']:.0f} us "
        f"({result['extraction_budget_us'] / 1_000_000:.2f} s)"
    )
    lines.append(
        f"- total happy-path overhead (insert_new + get_latest + mark_complete): "
        f"{result['total_overhead_us']:.1f} us"
    )
    lines.append(f"- overhead %: {result['overhead_pct']:.4f} %")
    lines.append(f"- verdict: **{result['verdict']}**")
    lines.append("")
    lines.append("| method | mean (us) | p50 (us) | p95 (us) | p99 (us) |")
    lines.append("|---|---|---|---|---|")
    for name, stats in result["methods"].items():
        lines.append(
            f"| {name} | {stats['mean_us']:.2f} | {stats['p50_us']:.2f} "
            f"| {stats['p95_us']:.2f} | {stats['p99_us']:.2f} |"
        )
    return "\n".join(lines) + "\n"


# ------------------------- CLI --------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Measure Python-side overhead of the Slice A watermark lifecycle."
        )
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1000,
        help="Per-method iteration count (default: 1000).",
    )
    parser.add_argument(
        "--extraction-budget-us",
        type=float,
        default=1_000_000.0,
        help="Assumed end-to-end extraction time in microseconds (default: 1_000_000 = 1 s).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of Markdown.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    result = run_benchmark(
        iterations=args.iterations,
        extraction_budget_us=args.extraction_budget_us,
    )

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(_format_markdown(result), end="")

    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
