# Slice A Exit Benchmark

**Version**: 1.0
**Date**: 2026-04-17
**Parent Plan**: [`PLAN.md`](./PLAN.md) Task 14
**Exit Criterion**: FR-L-10 (≤ 5 % runtime overhead vs. pre-Slice-A baseline)
**AC**: AC-SA-40

---

## 1. Scope

Slice A introduced: SQL-safe validator + emitter call chain in every
DML method, atomic `MERGE` with internal retry loop, terminal-state
guard read-back, and UTC session-timezone enforcement. The exit
criterion FR-L-10 bounds the *total* end-to-end extraction overhead at
≤ 5 %.

Overhead decomposes into two independently measurable components:

1. **Python-side (in-process) overhead** — validator + emitter +
   try/except + retry-branch-not-taken cost inside `WatermarkManager`.
   Reproducibly measurable without a Databricks cluster; reported in
   §3 below using the checked-in harness
   `scripts/benchmark_slice_a.py`.
2. **Cluster-side (Delta + JDBC + Parquet) overhead** — delta from the
   additional `MERGE` statement and the single `UPDATE` carrying the
   terminal guard. Requires a running DBR 17.3 LTS cluster against a
   representative fixture to measure; operator-gated, reported in §4.

Verdict is PASS iff **both** components land under the 5 % envelope.

---

## 2. Methodology

### 2.1 Python-side Benchmark

`scripts/benchmark_slice_a.py` instruments the four hot-path lifecycle
methods — `insert_new`, `mark_complete`, `mark_failed`,
`get_latest_watermark` — against a `NoopSpark` stand-in that returns a
canned `num_affected_rows=1` row for every `spark.sql(...)` call. The
harness:

- warms each call site once to prime `re` compilations and Decimal
  imports,
- runs N=5000 iterations per method,
- reports mean / p50 / p95 / p99 in microseconds,
- sums the happy-path lifecycle cost (`insert_new + get_latest +
  mark_complete`) and compares to a caller-supplied extraction budget
  (default 1 second = 1 000 000 us).

Reproduce:

```
python scripts/benchmark_slice_a.py --iterations 5000 --json
```

### 2.2 Cluster-side Benchmark (operator-gated)

Run the pre/post comparison on a representative incremental extraction
fixture. Suggested shape: a Postgres source with ~ 1 M rows and a
`TIMESTAMP` watermark column; watermark table in
`metadata.orchestration.watermarks`; DBR 17.3 LTS serverless.

Procedure:

1. Check out the pre-Slice-A tip (`git log --grep="Slice A" --oneline`
   to identify the boundary commit; the pre-branch SHA is immediately
   before `9c0f45a4 refactor(extensions): convert watermark_manager
   module to package`).
2. Deploy the Workflow, run 3 incremental extractions; record wall
   clock per run.
3. Check out the Slice A tip, redeploy, run 3 more incremental
   extractions.
4. Drop the first run from each side (cold-start / Delta OPTIMIZE
   warmup). Mean the remaining two runs per side.
5. Compute `(post_mean - pre_mean) / pre_mean` as overhead %.

Record raw run logs under `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/bench-runs/`
for audit.

---

## 3. Python-side Results (measured 2026-04-17)

Hardware: Apple Silicon (darwin 25.3.0), Python 3.12.9, Jinja2 3.1.6,
PySpark not loaded (NoopSpark stub).

Harness invocation: `python scripts/benchmark_slice_a.py --iterations 5000`.

| method | mean (us) | p50 (us) | p95 (us) | p99 (us) |
|---|---|---|---|---|
| `insert_new` | 4.38 | 4.29 | 4.96 | 6.08 |
| `mark_complete` | 2.62 | 2.54 | 2.92 | 3.58 |
| `mark_failed` | 3.34 | 3.13 | 4.29 | 4.46 |
| `get_latest_watermark` | 1.14 | 1.08 | 1.46 | 1.54 |

**Happy-path lifecycle overhead (sum of `insert_new` + `get_latest` +
`mark_complete`)**: **8.14 us mean**.

Failure-path lifecycle overhead (`insert_new` + `get_latest` + `mark_failed`):
8.86 us mean.

Against the default 1-second extraction budget: **0.0008 %** overhead
→ **PASS**.

Against a conservative 100-ms (single-partition JDBC mini-table)
budget: 0.008 % → still PASS.

Verdict: **PASS (code-side)**.

Interpretation: validator + emitter + try/except costs are dominated by
the `re.match` calls inside `SQLInputValidator.string` and
`SQLInputValidator.uuid_or_job_run_id`. The retry loop adds one
`type(exc).__name__` check that never fires on the happy path. All of
this is negligible against any realistic JDBC read latency.

---

## 4. Cluster-side Results

**Status: OPERATOR-GATED, pending execution.**

Operator to populate with the methodology from §2.2. Target: paste
pre/post mean seconds and the computed overhead % into the table
below; replace "pending" rows.

| fixture | pre-Slice-A mean (s) | post-Slice-A mean (s) | delta % | verdict |
|---|---|---|---|---|
| `pg_crm.production.product` (~1 M rows, timestamp watermark) | pending | pending | pending | pending |
| `pg_crm.production.customer` (~100 k rows, numeric watermark) | pending | pending | pending | pending |

Until this section is populated, **Slice A must not ship to production**.
Code-side PASS is a necessary but not sufficient condition for FR-L-10.

### 4.1 Expected Shape

Slice A added:

- one `MERGE` (replaces pre-existing `INSERT` in `insert_new`) — Delta
  commit cost roughly equivalent; `WHEN NOT MATCHED BY TARGET` adds
  negligible plan-time cost.
- one read-back `SELECT status FROM ... WHERE run_id=... LIMIT 1` in
  the guard path — only fires on zero-affected UPDATE, i.e. never on
  a normal run.
- one `SHOW TABLES IN <namespace>` at `WatermarkManager.__init__` —
  unchanged from pre-Slice-A.
- one `ALTER TABLE ADD COLUMN IF NOT EXISTS migration_note STRING`
  once, during the migration script — not inside the extraction hot
  loop.

Projected cluster-side overhead: single-digit milliseconds absolute,
well under 1 % of a typical extraction.

---

## 5. NFR-L-03 Observability Query Spot-Check

The parent L2 §NFR-L-03 contract requires the watermark table to be
queryable for run-state audit with simple SQL. Spot-check queries that
must continue to work after Slice A:

```sql
-- Latest completed watermark per key
SELECT source_system_id, schema_name, table_name,
       MAX(watermark_time) AS last_completed,
       MAX(watermark_value) AS last_value
FROM metadata.orchestration.watermarks
WHERE status = 'completed'
GROUP BY source_system_id, schema_name, table_name;

-- Currently-running orphans (pre-migration)
SELECT run_id, source_system_id, schema_name, table_name, created_at
FROM metadata.orchestration.watermarks
WHERE status = 'running'
  AND created_at < current_timestamp() - INTERVAL 4 HOURS;

-- Failed runs in the last 7 days
SELECT run_id, error_class, error_message, completed_at
FROM metadata.orchestration.watermarks
WHERE status = 'failed'
  AND completed_at >= current_timestamp() - INTERVAL 7 DAYS
ORDER BY completed_at DESC;

-- Migration note filter (post-backfill audit)
SELECT source_system_id, schema_name, table_name, run_id, migration_note
FROM metadata.orchestration.watermarks
WHERE migration_note LIKE 'slice-A-backfill-%';
```

Each query is pure SELECT against columns that exist on the Slice A
schema. None depend on the new exception types or validators; they
remain operator-runnable in Databricks SQL without modification.

---

## 6. Exit Decision

- **Code-side (§3)**: PASS — 0.0008 % overhead against 1 s budget.
- **Cluster-side (§4)**: PENDING — operator must measure before
  production rollout.
- **Observability (§5)**: PASS — queries unchanged.
- **Waiver W-A-01 (from L3 §8)**: re-acknowledged. Slice A must not
  ship alone; Slice B scheduling + ownership is a release
  prerequisite.

Overall: **PASS for code-side exit criterion; cluster-side verification
is the release gate.** Once §4 is populated and passes, the full FR-L-10
exit criterion is satisfied and Slice A is ship-ready (subject to W-A-01).

---

## 7. Revision History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-17 | Initial report. Python-side benchmark measured with the checked-in harness at 5000 iterations. Cluster-side section left for operator population. |
