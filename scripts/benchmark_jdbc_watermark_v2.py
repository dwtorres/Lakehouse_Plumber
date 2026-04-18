#!/usr/bin/env python3
"""Generation-time benchmark for jdbc_watermark_v2 artifacts."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from typing import Any, Dict, List

from lhp.generators.bundle.workflow_resource import WorkflowResourceGenerator
from lhp.generators.load.jdbc_watermark_job import JDBCWatermarkJobGenerator
from lhp.models.config import Action, FlowGroup, WriteTarget


def _build_flowgroup() -> FlowGroup:
    load_action = Action(
        name="load_product_jdbc",
        type="load",
        source={
            "type": "jdbc_watermark_v2",
            "url": "jdbc:postgresql://host:5432/db",
            "user": "test_user",
            "password": "test_pass",
            "driver": "org.postgresql.Driver",
            "table": '"Production"."Product"',
            "schema_name": "Production",
            "table_name": "Product",
            "options": {
                "cloudFiles.schemaEvolutionMode": "addNewColumns",
                "cloudFiles.rescueDataColumn": "_rescued_data",
            },
        },
        target="v_product_raw",
        landing_path="/Volumes/catalog/schema/landing/product",
        watermark={
            "column": "ModifiedDate",
            "type": "timestamp",
            "source_system_id": "pg_prod",
        },
    )
    write_action = Action(
        name="write_product_bronze",
        type="write",
        source="v_product_raw",
        write_target=WriteTarget(
            type="streaming_table",
            catalog="bronze_catalog",
            schema="bronze_schema",
            table="product",
            table_properties={"delta.enableChangeDataFeed": "true"},
        ),
    )
    return FlowGroup(
        pipeline="benchmark_pipeline",
        flowgroup="benchmark_flowgroup",
        actions=[load_action, write_action],
    )


def _summary(samples_us: List[float]) -> Dict[str, float]:
    ordered = sorted(samples_us)
    n = len(ordered)
    return {
        "mean_us": statistics.fmean(ordered),
        "p50_us": ordered[n // 2],
        "p95_us": ordered[min(n - 1, int(n * 0.95))],
    }


def run_benchmark(iterations: int = 200) -> Dict[str, Any]:
    flowgroup = _build_flowgroup()
    load_action = flowgroup.actions[0]
    load_generator = JDBCWatermarkJobGenerator()
    workflow_generator = WorkflowResourceGenerator()

    load_samples: List[float] = []
    workflow_samples: List[float] = []
    combined_samples: List[float] = []

    for _ in range(iterations):
        flowgroup._auxiliary_files.clear()

        start = time.perf_counter_ns()
        load_generator.generate(load_action, {"flowgroup": flowgroup})
        load_samples.append((time.perf_counter_ns() - start) / 1000.0)

        start = time.perf_counter_ns()
        workflow_generator.generate(flowgroup, {})
        workflow_samples.append((time.perf_counter_ns() - start) / 1000.0)

        flowgroup._auxiliary_files.clear()
        start = time.perf_counter_ns()
        load_generator.generate(load_action, {"flowgroup": flowgroup})
        workflow_generator.generate(flowgroup, {})
        combined_samples.append((time.perf_counter_ns() - start) / 1000.0)

    return {
        "iterations": iterations,
        "artifacts": {
            "load_generator": _summary(load_samples),
            "workflow_generator": _summary(workflow_samples),
            "combined_generation": _summary(combined_samples),
        },
    }


def _format_markdown(result: Dict[str, Any]) -> str:
    lines = [
        "# jdbc_watermark_v2 Generation Benchmark",
        "",
        f"- iterations: {result['iterations']}",
        "",
        "| artifact | mean (us) | p50 (us) | p95 (us) |",
        "|---|---|---|---|",
    ]
    for name, stats in result["artifacts"].items():
        lines.append(
            f"| {name} | {stats['mean_us']:.2f} | {stats['p50_us']:.2f} | {stats['p95_us']:.2f} |"
        )
    return "\n".join(lines) + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark local jdbc_watermark_v2 generation overhead."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=200,
        help="Iteration count per measured artifact (default: 200).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of markdown.",
    )
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    result = run_benchmark(iterations=args.iterations)
    if args.json:
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True))
    else:
        sys.stdout.write(_format_markdown(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
