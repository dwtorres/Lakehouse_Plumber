# ADR-001: JDBC Watermark v2 — Parquet Post-Write Stats for Count/Max

**Status**: Accepted
**Date**: 2026-04-17 (opened); 2026-04-18 (Phase B validation)
**Relates-to**: ADR-002 (runtime availability), TD-007 (opener), Constitution §P5, §P6, §P9
**Supersedes**: none

## Context

The JDBC watermark v2 extraction flow writes a batch of rows to a Parquet landing path, then records `row_count` + the maximum watermark value in the `_lhp_watermark` Delta table via `mark_landed()`. Two approaches to obtain count + max:

1. **Spark aggregation** after the Parquet write — `spark.read.parquet(landing_path).agg(F.count("*"), F.max(wm_col)).collect()`. Extra DataFrame action, extra task on the cluster.
2. **Parquet post-write statistics** — Parquet files carry per-row-group min/max footers plus a row count in the file metadata. Read directly from the `parquet.statistics` dictionary returned by `DataFrameWriter.save` callbacks or via `pyarrow.parquet.read_metadata(file_path).row_group(i).column(j).statistics`.

## Decision

Use **Parquet post-write statistics** via the Spark-native metrics reported by the Parquet write action, not a follow-up aggregation DataFrame.

### Rationale

- **Zero extra Spark action** on the already-written data. Count + max come from the footer that Parquet writes anyway.
- **Semantically consistent**: the value reported is exactly the value in the landing file, not a re-read that could pick up an independent Parquet file by accident.
- **Faster** — saves one full-scan aggregation per extraction run; matters at scale (>10M rows per run).
- **No change to on-disk format** — uses standard Parquet footer fields.

### Scope explicitly scoped out

- **Runtime availability of the WatermarkManager class** — how the Python class is distributed to the extraction task. Resolved by ADR-002 (Path 5 Option A: vendored `lhp_watermark/` + DAB workspace-file sync + `sys.path` prelude).

## Consequences

**Positive**:
- Lower cluster cost per extraction run.
- No risk of "wrong file" aggregation reading an unrelated Parquet in the landing path.
- Decouples extraction latency from row count.

**Negative**:
- Dependent on Parquet footer correctness — if the writer is buggy or configured to skip statistics, the values are wrong. Spark's native Parquet writer always emits statistics; verified.
- Requires reading file metadata (fast, stdlib) vs. Spark SQL. Slightly different error surface.

## Phase B validation

Validated on `dbc-8e058692-373e.cloud.databricks.com` with the test harness in `tests/e2e/test_jdbc_watermark_parquet_stats_phase_b.py` (memory ref `#S704`). Delta values round-trip cleanly from Parquet stats through `mark_landed` to `mark_complete`. No observed drift.

## Related

- [ADR-002](ADR-002-lhp-runtime-availability.md) — runtime availability (resolves the runtime-class distribution question that this ADR scoped out)
- [TD-007](../tech-debt/TD-007-*.md) — opener (create when TD registry is formalized)
- Constitution §P5 (watermark registry source of truth), §P6 (retry safety)
