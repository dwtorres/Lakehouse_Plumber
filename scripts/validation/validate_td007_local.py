#!/usr/bin/env python3
"""TD-007 Phase A local validation harness.

Validates the V2 parquet-post-write-stats invariants that can be exercised
on a laptop-scale PySpark session without a Databricks cluster:

  V1 — Run-scoped subdir + ``mode("overwrite")`` creates and reads back Parquet.
  V2 — Post-write ``count()`` / ``max(col)`` match the input DataFrame exactly.
  V3 — Post-write aggregate latency budget (≤ 1 s for 1M rows via Parquet stats).
  V4 — Empty-batch write produces a readable Parquet directory.

Runs against a local SparkSession using temp directories. No Databricks profile
required. Usage::

    python scripts/validation/validate_td007_local.py
    python scripts/validation/validate_td007_local.py --rows-v3 1000000 --json

Exit code 0 iff every invariant reports PASS. Results emit a markdown table
by default; ``--json`` produces a machine-readable record suitable for embedding
in the ADR-001 evidence section.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


@dataclass
class InvariantResult:
    id: str
    name: str
    verdict: str
    duration_s: float
    details: Dict[str, Any]
    error: Optional[str] = None


def _build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("td007-local-validation")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def v1_run_scoped_write_and_read_back(spark, tmp_root: Path) -> Dict[str, Any]:
    """V1: write mode=overwrite to {root}/_lhp_runs/{run_id}/; read back."""
    from pyspark.sql import functions as F

    run_id = "job-123-task-456-attempt-0"
    landing_root = str(tmp_root / "landing_v1")
    run_landing_path = f"{landing_root}/_lhp_runs/{run_id}"

    df = spark.range(100).withColumn("ts", F.current_timestamp())
    df.write.mode("overwrite").format("parquet").save(run_landing_path)

    subdir = Path(run_landing_path)
    files = sorted(p.name for p in subdir.iterdir() if p.name.endswith(".parquet"))
    readback = spark.read.format("parquet").load(run_landing_path)
    readback_count = readback.count()

    return {
        "landing_root": landing_root,
        "run_landing_path": run_landing_path,
        "parquet_files_written": files,
        "expected_count": 100,
        "readback_count": readback_count,
        "ok": readback_count == 100 and len(files) >= 1,
    }


def v2_post_write_stats_match_input(spark, tmp_root: Path) -> Dict[str, Any]:
    """V2: landed Parquet agg(count, max) equals the pre-write computed values."""
    from pyspark.sql import functions as F

    run_id = "job-200-task-1-attempt-0"
    run_landing_path = str(tmp_root / "landing_v2" / "_lhp_runs" / run_id)

    source = (
        spark.range(10_000)
        .withColumn(
            "modified_date",
            F.expr("timestamp_millis(1700000000000 + id * 1000)"),
        )
        .select("id", "modified_date")
    )
    expected = source.agg(
        F.count("*").alias("row_count"),
        F.max("modified_date").alias("max_hwm"),
    ).first()

    source.write.mode("overwrite").format("parquet").save(run_landing_path)
    landed = spark.read.format("parquet").load(run_landing_path)
    landed_stats = landed.agg(
        F.count("*").alias("row_count"),
        F.max("modified_date").alias("max_hwm"),
    ).first()

    row_count_match = landed_stats["row_count"] == expected["row_count"]
    hwm_match = landed_stats["max_hwm"] == expected["max_hwm"]

    return {
        "run_landing_path": run_landing_path,
        "expected_row_count": expected["row_count"],
        "landed_row_count": landed_stats["row_count"],
        "expected_max_hwm": str(expected["max_hwm"]),
        "landed_max_hwm": str(landed_stats["max_hwm"]),
        "row_count_match": row_count_match,
        "hwm_match": hwm_match,
        "ok": row_count_match and hwm_match,
    }


def v3_post_write_latency_budget(
    spark, tmp_root: Path, rows: int, budget_s: float
) -> Dict[str, Any]:
    """V3: Parquet-footer-backed post-write agg fits the latency budget."""
    from pyspark.sql import functions as F

    run_id = "job-300-task-1-attempt-0"
    run_landing_path = str(tmp_root / "landing_v3" / "_lhp_runs" / run_id)

    source = spark.range(rows).withColumn(
        "modified_date",
        F.expr("timestamp_millis(1700000000000 + id * 1000)"),
    )
    source.repartition(1).write.mode("overwrite").format("parquet").save(run_landing_path)

    landed = spark.read.format("parquet").load(run_landing_path)

    start = time.perf_counter()
    stats = landed.agg(
        F.count("*").alias("row_count"),
        F.max("modified_date").alias("max_hwm"),
    ).first()
    elapsed = time.perf_counter() - start

    return {
        "rows": rows,
        "budget_s": budget_s,
        "measured_s": elapsed,
        "observed_row_count": stats["row_count"],
        "observed_max_hwm": str(stats["max_hwm"]),
        "ok": elapsed <= budget_s and stats["row_count"] == rows,
    }


def v4_empty_batch_write(spark, tmp_root: Path) -> Dict[str, Any]:
    """V4: empty DataFrame writes a readable Parquet dir with count=0."""
    from pyspark.sql.types import LongType, StructField, StructType, TimestampType

    run_id = "job-400-task-1-attempt-0"
    run_landing_path = str(tmp_root / "landing_v4" / "_lhp_runs" / run_id)

    empty_schema = StructType(
        [
            StructField("id", LongType(), nullable=True),
            StructField("modified_date", TimestampType(), nullable=True),
        ]
    )
    empty_df = spark.createDataFrame([], empty_schema)
    empty_df.write.mode("overwrite").format("parquet").save(run_landing_path)

    landed = spark.read.format("parquet").load(run_landing_path)
    try:
        landed_count = landed.count()
        count_ok = True
        error = None
    except Exception as exc:  # noqa: BLE001 — the invariant is that read does not throw
        landed_count = None
        count_ok = False
        error = f"{type(exc).__name__}: {exc}"

    return {
        "run_landing_path": run_landing_path,
        "landed_count": landed_count,
        "count_matches_zero": landed_count == 0,
        "read_succeeded_without_throw": count_ok,
        "error": error,
        "ok": count_ok and landed_count == 0,
    }


INVARIANTS: List[Dict[str, Any]] = [
    {
        "id": "V1",
        "name": "Run-scoped write + readback",
        "fn": v1_run_scoped_write_and_read_back,
    },
    {
        "id": "V2",
        "name": "Post-write stats match input",
        "fn": v2_post_write_stats_match_input,
    },
    {
        "id": "V3",
        "name": "Post-write latency within budget",
        "fn": None,  # bound below with args
    },
    {
        "id": "V4",
        "name": "Empty-batch write is readable",
        "fn": v4_empty_batch_write,
    },
]


def _run_invariant(
    invariant_id: str,
    name: str,
    fn: Callable[..., Dict[str, Any]],
    *args: Any,
) -> InvariantResult:
    start = time.perf_counter()
    try:
        details = fn(*args)
        elapsed = time.perf_counter() - start
        verdict = "PASS" if details.get("ok") else "FAIL"
        return InvariantResult(
            id=invariant_id,
            name=name,
            verdict=verdict,
            duration_s=elapsed,
            details=details,
        )
    except Exception as exc:  # noqa: BLE001 — we want to surface any failure
        elapsed = time.perf_counter() - start
        return InvariantResult(
            id=invariant_id,
            name=name,
            verdict="ERROR",
            duration_s=elapsed,
            details={},
            error=f"{type(exc).__name__}: {exc}",
        )


def _render_markdown(results: List[InvariantResult], meta: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# TD-007 Phase A Local Validation",
        "",
        f"- Timestamp (UTC): {meta['timestamp']}",
        f"- PySpark: {meta['pyspark_version']}",
        f"- Python: {meta['python_version']}",
        "",
        "| ID | Invariant | Verdict | Duration (s) | Notes |",
        "|---|---|---|---|---|",
    ]
    for result in results:
        notes = result.error if result.error else ""
        if not notes and result.verdict != "PASS":
            notes = json.dumps(result.details, default=str)[:120]
        lines.append(
            f"| {result.id} | {result.name} | {result.verdict} | "
            f"{result.duration_s:.3f} | {notes} |"
        )
    lines.append("")
    for result in results:
        lines.append(f"## {result.id} — {result.name}")
        lines.append("")
        lines.append(f"- Verdict: **{result.verdict}**")
        lines.append(f"- Duration: {result.duration_s:.3f} s")
        if result.error:
            lines.append(f"- Error: `{result.error}`")
        if result.details:
            lines.append("- Details:")
            for key, value in result.details.items():
                lines.append(f"  - `{key}`: `{value}`")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows-v3",
        type=int,
        default=1_000_000,
        help="Row count for V3 latency-budget invariant (default: 1,000,000).",
    )
    parser.add_argument(
        "--budget-v3-s",
        type=float,
        default=1.0,
        help="Latency budget in seconds for V3 (default: 1.0).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of markdown.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    spark = _build_spark()
    tmp_root = Path(tempfile.mkdtemp(prefix="td007-validation-"))

    try:
        results: List[InvariantResult] = []
        results.append(
            _run_invariant("V1", "Run-scoped write + readback", v1_run_scoped_write_and_read_back, spark, tmp_root)
        )
        results.append(
            _run_invariant("V2", "Post-write stats match input", v2_post_write_stats_match_input, spark, tmp_root)
        )
        results.append(
            _run_invariant(
                "V3",
                f"Post-write latency within {args.budget_v3_s}s for {args.rows_v3} rows",
                v3_post_write_latency_budget,
                spark,
                tmp_root,
                args.rows_v3,
                args.budget_v3_s,
            )
        )
        results.append(
            _run_invariant("V4", "Empty-batch write is readable", v4_empty_batch_write, spark, tmp_root)
        )
    finally:
        spark.stop()
        shutil.rmtree(tmp_root, ignore_errors=True)

    import pyspark
    import platform

    meta = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pyspark_version": pyspark.__version__,
        "python_version": platform.python_version(),
    }

    if args.json:
        sys.stdout.write(
            json.dumps(
                {"meta": meta, "results": [asdict(r) for r in results]},
                indent=2,
                default=str,
            )
        )
        sys.stdout.write("\n")
    else:
        sys.stdout.write(_render_markdown(results, meta))

    failed = [r for r in results if r.verdict != "PASS"]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
