# L3 — Feature Specification: JDBC Watermark v2 — Slice A (Lifecycle Hardening)

**Feature**: Runtime hardening of the v2 watermark lifecycle — terminal-state wiring, SQL safety, completion-filtered lookup, atomic insert with Delta retry, terminal-state guards, UTC-explicit timestamps, deterministic `run_id` provenance, and a one-shot orphan-run migration.
**Version**: 1.0
**Date**: 2026-04-17
**Status**: Draft
**Branch**: watermark
**Parent L2**: [`L2-srs.md`](./L2-srs.md) rev 1.1
**Parent L3 (v2 baseline)**: [`../../L3-feature-spec.md`](../../L3-feature-spec.md)
**Governing Constitution**: [`../../../constitution.md`](../../../constitution.md)

---

## 1. Feature Overview

Slice A hardens the runtime of the v2 `jdbc_watermark` load action without changing its YAML contract. The generated extraction notebook now wraps JDBC read, Parquet write, and high-water-mark computation in an explicit `try`/`except`, transitioning the watermark row from `status='running'` to `status='completed'` on success (via `WatermarkManager.mark_complete`) or to `status='failed'` on exception (via `WatermarkManager.mark_failed`) before re-raising for Workflow retry. Every SQL statement composed by `WatermarkManager` or the template routes untrusted inputs through typed validators (`SQLInputValidator.string|numeric|timestamp|identifier|uuid_or_job_run_id`) and emitters (`sql_literal`, `sql_numeric_literal`, `sql_timestamp_literal`, `sql_identifier`); raw f-string interpolation of YAML, JDBC, or task-parameter values into SQL is no longer permitted and is enforced by a white-box Jinja AST lint. `get_latest_watermark` now filters to `status='completed'` with a deterministic `(watermark_time DESC, run_id DESC) LIMIT 1` tie-breaker. `insert_new` becomes atomic via a single `MERGE … WHEN NOT MATCHED` statement that inspects `num_affected_rows` and retries internally on Delta `ConcurrentAppendException` / `ConcurrentDeleteReadException` with jittered exponential backoff (5 attempts, base 100 ms, factor 2, ±50 % jitter); a zero-affected `MERGE` surfaces as `DuplicateRunError` (`LHP-WM-001`). `mark_bronze_complete`, `mark_silver_complete`, `mark_complete`, and `mark_failed` carry SQL-side `WHERE` clauses that refuse to overwrite terminal states of the opposite polarity and surface `TerminalStateGuardError` (`LHP-WM-002`) when the row would be re-classified. All timestamps are written as `TIMESTAMP 'ISO-8601-UTC'` literals after `spark.sql.session.timeZone='UTC'` is set on the active session. `run_id` is derived from Databricks job/task/attempt identifiers via a new `derive_run_id(dbutils)` helper (fallback: `local-<uuid4>` with warning). Before Slice A activates against any non-empty environment, a standalone migration script (`src/lhp/extensions/migrations/slice_a_backfill.py`) promotes verifiable orphan `running` rows to `completed` and logs the rest. No new YAML keys, no new source types, no schema migration; all changes are behavioural.

---

## 2. Behavioral Scenarios

### 2.1 Successful Extraction — Terminal Transition To `'completed'`

```
Given a flowgroup with source.type: jdbc_watermark
  And WatermarkManager.get_latest_watermark() returns a prior completed row
  And the JDBC read returns N>0 rows and the Parquet write succeeds
When the generated extraction notebook runs
Then the sequence on the watermark table is:
    1. insert_new(run_id, ...) commits a row with status='running'
    2. df.write.parquet(landing_path) completes
    3. max_hwm = df.agg(max(watermark.column)).first()[0]
    4. row_count = df.count()                 # FR-L-10 second scan, acknowledged
    5. mark_complete(run_id, row_count, watermark_value=max_hwm) is called OUTSIDE the try block
  And the row for run_id now has:
    - status = 'completed'
    - watermark_value = max_hwm
    - row_count = N
    - completed_at = UTC ISO-8601 microsecond-precision timestamp
  And the extraction notebook exits 0
  And the downstream DLT pipeline executes as Task 2 of the Workflow
```

### 2.2 JDBC Read Failure — Audit Trail And Re-Raise

```
Given insert_new has already committed status='running' for run_id
  And spark.read.format("jdbc").load() raises py4j.protocol.Py4JJavaError
When the except block runs
Then mark_failed(run_id, error_class='Py4JJavaError', error_message=str(e)[:4096]) is called
  And the watermark row transitions to status='failed'
  And error_class, error_message, failed_at are recorded
  And the original exception is re-raised
  And Databricks Workflow marks Task 1 failed and applies its configured retry policy
  And mark_complete is NEVER called
```

### 2.3 `mark_complete` Failure After Successful Extraction

```
Given the extraction try block completed successfully (extraction_succeeded = True)
  And mark_complete raises TerminalStateGuardError (e.g. an operator manually
      marked the row 'failed' between insert_new and mark_complete)
When the template's post-try mark_complete call executes
Then the TerminalStateGuardError propagates to the notebook exit
  And mark_failed is NEVER called on the already-terminal row
  And the audit trail preserves the operator-applied 'failed' status
  And the Workflow task fails with the TerminalStateGuardError
```

### 2.4 SQL Injection Attempt In Watermark Value

```
Given watermark.type == 'timestamp'
  And the source database returns a crafted MAX() value (e.g. a string column
      masquerading as a timestamp) such that the Python-side value is
      "2025-06-01' OR 1=1 --"
When WatermarkManager.insert_new() attempts to compose the MERGE statement
Then SQLInputValidator.timestamp(value) raises WatermarkValidationError
  And the class is LHP-WM-003
  And no Spark SQL call is executed (enforced by the NFR-L-05 unit test mock)
  And the exception propagates to the except block
  And mark_failed records error_class='WatermarkValidationError'
```

### 2.5 SQL Injection Attempt In `run_id`

```
Given run_id = "job-1 DROP TABLE x --"
When WatermarkManager.insert_new(run_id=run_id, ...) is called
Then SQLInputValidator.uuid_or_job_run_id(run_id) raises WatermarkValidationError
  And the regex ^(uuid-v4|job-\d+-task-\d+-attempt-\d+)$ does not match
  And no MERGE is issued
```

### 2.6 Identifier Validation On `jdbc_table`

```
Given source.table == "valid_schema.valid_table"
When SQLInputValidator.identifier(source.table) runs
Then it passes (matches ^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$)

Given source.table == '"Production"."Product"'
When SQLInputValidator.identifier(source.table) runs
Then it passes (matches the pre-quoted alternate ^"[^"]+"(\."[^"]+")*$)

Given source.table == "users; DROP TABLE x"
When SQLInputValidator.identifier(source.table) runs
Then it raises WatermarkValidationError
```

### 2.7 Completion-Filtered Lookup With Same-Second Tie-Break

```
Given the watermark table contains three rows for (sys_id, schema, table):
    - run_id='job-1', status='completed', watermark_time=T
    - run_id='job-2', status='completed', watermark_time=T   # identical
    - run_id='job-3', status='running',   watermark_time=T+1 # later, non-terminal
When get_latest_watermark(sys_id, schema, table) is called
Then the returned row is the one with run_id='job-2'
    (status='completed' filter excludes job-3; tie-break on run_id DESC picks job-2)
```

### 2.8 Completion-Filtered Lookup Returns `None` For Migration-Pending Key

```
Given the watermark table contains one row for (sys_id, schema, table):
    - run_id='job-old', status='running'   # pre-Slice-A orphan, not migrated
When get_latest_watermark(sys_id, schema, table) is called
Then the return value is None
  And the next extraction performs a FULL LOAD
  And a WARNING "no terminal-success row for <key>; performing full load" is logged
  And FR-L-M1 migration documents this as the expected pre-migration behaviour
```

### 2.9 Atomic `insert_new` Under Duplicate `run_id`

```
Given two extraction notebooks invoked with identical run_id='job-1-task-1-attempt-1'
  And both call insert_new concurrently
When both MERGE statements commit
Then exactly one observes num_affected_rows == 1 and returns
  And exactly one observes num_affected_rows == 0 and raises DuplicateRunError (LHP-WM-001)
  And the watermark table contains exactly one row for run_id='job-1-task-1-attempt-1'
```

### 2.10 Atomic `insert_new` Under Delta Concurrent-Commit Contention

```
Given N=200 extraction notebooks inserting distinct run_ids into the same Delta table
When the Delta commit service surfaces ConcurrentAppendException to some writers
Then insert_new's internal retry loop absorbs the exception
  And retries with jittered backoff (base 100 ms, factor 2, ±50 % jitter, budget 5)
  And all 200 distinct run_ids end up persisted exactly once
  And P99 insert latency including retry budget is under 10 seconds (NFR-L-02)

Given the retry budget is exhausted (5 attempts all failed)
When insert_new surfaces the final exception
Then it raises WatermarkConcurrencyError(run_id, attempts=5) as LHP-WM-004
  And the original ConcurrentAppendException is set as __cause__
```

### 2.11 Terminal-State Guard — `mark_complete` On Failed Row

```
Given the watermark row for run_id is status='failed'
When mark_complete(run_id, row_count) is called
Then the UPDATE's WHERE clause excludes status IN ('failed','timed_out','landed_not_committed')
  And num_affected_rows = 0
  And mark_complete raises TerminalStateGuardError(run_id, current_status='failed')
  And the failed row is unchanged (error_class, error_message, failed_at preserved)
```

### 2.12 Terminal-State Guard — `mark_failed` On Completed Row

```
Given the watermark row for run_id is status='completed'
When mark_failed(run_id, error_class, error_message) is called
Then the UPDATE's WHERE clause restricts status IN ('running','failed','timed_out')
  And num_affected_rows = 0
  And mark_failed raises TerminalStateGuardError(run_id, current_status='completed')
  And the completed row is unchanged (watermark_value, row_count, completed_at preserved)
```

### 2.13 `mark_failed` Idempotency — Latest Failure Wins

```
Given the watermark row for run_id is status='failed'
  with error_class='IOError', error_message='connection reset'
When mark_failed(run_id, error_class='TimeoutError', error_message='read timed out')
    is called on the same run_id
Then the UPDATE succeeds (num_affected_rows = 1)
  And the row's error_class is now 'TimeoutError'
  And the row's error_message is 'read timed out'
  And the row's failed_at is updated to the later timestamp
  And status remains 'failed'
```

### 2.14 UTC Timestamp Round-Trip From Non-UTC Session

```
Given spark.sql.session.timeZone is 'America/Chicago' when a writer calls insert_new
  And the writer submits a Python datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
When WatermarkManager issues the MERGE
Then WatermarkManager first sets spark.sql.session.timeZone='UTC' on the active session
  And the emitted literal is TIMESTAMP '2025-06-01T12:00:00.000000+00:00'
When a reader with spark.sql.session.timeZone='UTC' issues get_latest_watermark
Then the returned watermark_value corresponds to the same UTC instant 2025-06-01T12:00:00Z
```

### 2.15 `run_id` Provenance — Job Context Available

```
Given the notebook runs inside a Databricks Jobs task
  And dbutils.notebook.entry_point.getDbutils().notebook().getContext() exposes
    jobRunId='12345', taskRunId='67890', currentRunAttempt='2'
When derive_run_id(dbutils) is called
Then it returns 'job-12345-task-67890-attempt-2'
  And no warning is logged
```

### 2.16 `run_id` Provenance — Interactive Fallback

```
Given the notebook runs attached to an all-purpose cluster (no Jobs context)
  And the context accessors raise or return None
When derive_run_id(dbutils) is called
Then it returns f'local-{uuid.uuid4()}'
  And logger.warning("derive_run_id: no Jobs context; fell back to local-<uuid>; NON-PRODUCTION") is emitted
  And the return value matches ^local-[0-9a-f-]{36}$
```

### 2.17 `run_id` Provenance — Operator Override Widget

```
Given the notebook widget 'lhp_run_id_override' is set to a UUID
  (test/backfill scenario)
When derive_run_id(dbutils) is called
Then the widget value takes precedence over Jobs context
  And logger.warning("derive_run_id: override widget supplied; intended for backfill/test") is emitted
  And the returned run_id is validated via SQLInputValidator.uuid_or_job_run_id
  And a concurrent rerun with the same override triggers DuplicateRunError (FR-L-05)
```

### 2.18 Migration Script — Promote Verifiable Orphan

```
Given an orphan row for (sys_id, schema, table) with status='running',
    watermark_value='2025-06-01T00:00:00Z'
  And the bronze streaming table has max(_ingestion_time) >= '2025-06-01T00:00:00Z'
    AND max(watermark.column) == '2025-06-01T00:00:00Z' (within 1-second skew)
When slice_a_backfill.py runs
Then the orphan row is UPDATEd to status='completed'
  And migration_note = 'slice-A-backfill-YYYYMMDD'
  And completed_at is set to NOW() UTC
  And the original watermark_time is preserved
  And the script logs "promoted 1 row for <key>"
```

### 2.19 Migration Script — Leave Unverifiable Orphan

```
Given an orphan row for (sys_id, schema, table) with status='running'
  And the bronze streaming table does not exist OR max(_ingestion_time) is older
    than the orphan's watermark_value by more than 1 second
When slice_a_backfill.py runs
Then the orphan row is NOT modified
  And logger.warning("orphan <key> not promoted: bronze verification failed; next run will full-reload") is emitted
  And the script emits a summary line listing the key
```

### 2.20 Migration Script — Idempotency

```
Given slice_a_backfill.py has already run once and promoted N rows
When slice_a_backfill.py runs a second time with no data changes
Then zero additional rows are UPDATEd
  And the script logs "no orphans found; migration already applied"
  And exit code is 0

Given the operator passes --assume-empty on a fresh dev environment
When slice_a_backfill.py runs
Then the bronze-verification check is skipped
  And any non-'completed' rows are short-circuit-promoted
  (explicit operator override; default behaviour still conservative)
```

### 2.21 White-Box Template Lint — Bare SQL Interpolation

```
Given the template jdbc_watermark_job.py.j2 contains a Jinja expression
    {{ run_id }} inside a lexical context matching (INSERT|MERGE|UPDATE|SELECT|DELETE|WHERE)
When the NFR-L-05 lint test walks the parsed template AST
Then the test fails with:
    "Bare Jinja expression '{{ run_id }}' in SQL context; wrap in sql_literal/sql_identifier/..."
  And CI blocks the merge
```

### 2.22 Parent L2 Amendment — Workflow Retry Still Fires

```
Given a run whose except block called mark_failed and re-raised
When Databricks Workflow's task-level retry policy fires
Then a new attempt starts with attempt_number = prior+1
  And derive_run_id returns 'job-<jobRunId>-task-<taskRunId>-attempt-N+1'
  And insert_new commits a new row with status='running' under the new run_id
  And the prior 'failed' row is preserved as audit history
```

---

## 3. Acceptance Criteria

| ID | Criterion | Pass Condition | Traces To |
|----|-----------|---------------|-----------|
| AC-SA-01 | Generated extraction template calls `mark_complete(run_id, row_count, watermark_value=...)` **outside** the `try` block | Template-rendering AST test: `mark_complete` call is a top-level statement after the `try/except`, not nested inside it | FR-L-01 |
| AC-SA-02 | On successful completion, `watermark_value` recorded equals the `MAX(watermark.column)` of the landed batch | Integration test: insert a run, inject a known max, assert the stored value | FR-L-01 |
| AC-SA-03 | `completed_at` column is written in UTC ISO-8601 with microsecond precision | Unit test on `WatermarkManager`: assert the emitted literal matches `TIMESTAMP '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00'` | FR-L-01, FR-L-07 |
| AC-SA-04 | Generated extraction template wraps JDBC read + Parquet write + HWM aggregation in `try`/`except Exception` | AST test on template output: exactly one `try` block contains the three operations | FR-L-02 |
| AC-SA-05 | On any exception, `mark_failed(run_id, error_class=type(e).__name__, error_message=str(e)[:4096])` is called before re-raising | Integration test: raise during `df.write`, assert the watermark row has matching error fields and the exception still propagates | FR-L-02 |
| AC-SA-06 | `error_message` is truncated at 4096 characters | Unit test: pass a 10_000-char message, assert stored length == 4096 | FR-L-02 |
| AC-SA-07 | `SQLInputValidator.string(value, max_len)` rejects control chars, `\0`, length > max_len | Parametric unit test over adversarial inputs | FR-L-03 |
| AC-SA-08 | `SQLInputValidator.numeric(value)` accepts `int` and `Decimal`; rejects `float`, `Decimal('NaN')`, `Decimal('Infinity')` | Parametric unit test | FR-L-03 |
| AC-SA-09 | `SQLInputValidator.timestamp(value)` requires a timezone-aware `datetime`; rejects naive, string, int | Parametric unit test | FR-L-03, FR-L-07 |
| AC-SA-10 | `SQLInputValidator.identifier(value)` enforces the two documented regexes | Parametric unit test covering passing + failing strings, including injection attempts | FR-L-03 |
| AC-SA-11 | `SQLInputValidator.uuid_or_job_run_id(value)` enforces UUID-v4 OR `^job-\d+-task-\d+-attempt-\d+$` | Parametric unit test | FR-L-03, FR-L-09 |
| AC-SA-12 | Emitters `sql_literal`, `sql_numeric_literal`, `sql_timestamp_literal`, `sql_identifier` exist and are exported from `lhp.extensions.watermark_manager.sql_safety` | Import test + signature assertions | FR-L-03 |
| AC-SA-13 | No code under `src/lhp/extensions/watermark_manager/` contains `.replace("'", "")` | Static grep test | FR-L-03 |
| AC-SA-14 | No Jinja expression in `jdbc_watermark_job.py.j2` appears inside a SQL lexical context without wrapping in one of the four emitters | White-box Jinja AST lint test | FR-L-03, NFR-L-05 |
| AC-SA-15 | `get_latest_watermark` SQL includes `WHERE status = 'completed'` | Statement-capture unit test | FR-L-04 |
| AC-SA-16 | `get_latest_watermark` SQL orders by `watermark_time DESC, run_id DESC LIMIT 1` | Statement-capture unit test | FR-L-04 |
| AC-SA-17 | `get_latest_watermark` returns `None` when no `completed` row exists | Integration test against a table with only `running`/`failed` rows | FR-L-04 |
| AC-SA-18 | Same-second tie-break is deterministic across 100 repetitions | Property test: seed with N runs at identical `watermark_time`, assert identical winner each call | FR-L-04 |
| AC-SA-19 | `insert_new` uses exactly one `MERGE INTO ... USING (SELECT ...) ON t.run_id = s.run_id WHEN NOT MATCHED BY TARGET THEN INSERT ...` | Statement-capture unit test | FR-L-05 |
| AC-SA-20 | `insert_new` reads `num_affected_rows` from the Spark Delta command result; raises `DuplicateRunError(run_id)` when zero | Integration test with two parallel threads, identical `run_id` | FR-L-05 |
| AC-SA-21 | `insert_new` retries on `ConcurrentAppendException` or `ConcurrentDeleteReadException` with jittered exponential backoff (base 100 ms, factor 2, jitter ±50 %, budget 5) | Unit test injecting mock exceptions; assert attempt count + sleep durations | FR-L-05 |
| AC-SA-22 | On retry-budget exhaustion, `insert_new` raises `WatermarkConcurrencyError(run_id, attempts=5)` (`LHP-WM-004`) with `__cause__` set | Unit test | FR-L-05 |
| AC-SA-23 | `mark_bronze_complete`, `mark_silver_complete`, `mark_complete` SQL `WHERE` clause includes `AND status NOT IN ('failed','timed_out','landed_not_committed')` | Statement-capture unit test per method | FR-L-06 |
| AC-SA-24 | When those methods update zero rows, they raise `TerminalStateGuardError(run_id, current_status)` (`LHP-WM-002`); `current_status` is read back from the table | Integration test | FR-L-06 |
| AC-SA-25 | `mark_failed` SQL `WHERE` clause includes `AND status IN ('running','failed','timed_out')` | Statement-capture unit test | FR-L-06a |
| AC-SA-26 | `mark_failed` on an existing `failed` row updates `error_class`, `error_message`, `failed_at`; row remains `status='failed'` | Integration test: call `mark_failed` twice, assert latest-wins | FR-L-06a |
| AC-SA-27 | `mark_failed` on a `completed` row raises `TerminalStateGuardError` | Integration test | FR-L-06a |
| AC-SA-28 | `WatermarkManager` sets `spark.conf.set("spark.sql.session.timeZone", "UTC")` before issuing any DML against the watermark table | Spy test on `spark.conf.set` | FR-L-07 |
| AC-SA-29 | A `datetime` written from a session with `spark.sql.session.timeZone='America/Chicago'` round-trips to the same UTC instant when read from a session with `timeZone='UTC'` | Integration test using two Spark sessions (or session-timezone toggling) | FR-L-07 |
| AC-SA-30 | No code path uses `datetime.utcnow()` or `str(datetime.now())` (naive) in `src/lhp/extensions/watermark_manager/` or the extraction template | Static grep test | FR-L-07 |
| AC-SA-31 | No new YAML keys are introduced; all parent L3 v2 fixtures still pass `lhp validate` | Fixture replay test | FR-L-08 |
| AC-SA-32 | `derive_run_id(dbutils)` returns `job-{jobRunId}-task-{taskRunId}-attempt-{N}` when context accessors succeed | Unit test with mocked `dbutils` | FR-L-09 |
| AC-SA-33 | `derive_run_id(dbutils)` returns `local-<uuid4>` and logs a `WARNING` when context accessors fail | Unit test capturing log output | FR-L-09 |
| AC-SA-34 | `derive_run_id(dbutils)` honours an `lhp_run_id_override` widget value (validated via `uuid_or_job_run_id`) and logs a `WARNING` for override use | Unit test | FR-L-09, §7 OQ-4 |
| AC-SA-35 | Generated extraction template imports `derive_run_id` from `lhp.extensions.watermark_manager.runtime` and does not call `uuid.uuid4()` as its primary source | AST test on rendered template | FR-L-09 |
| AC-SA-36 | `src/lhp/extensions/migrations/slice_a_backfill.py` exists, is invokable as `python -m lhp.extensions.migrations.slice_a_backfill`, and is idempotent | Integration test: run twice, assert second run changes nothing | FR-L-M1 |
| AC-SA-37 | The migration script promotes a `running` orphan to `completed` when the bronze verification query passes | Integration test with a mocked bronze table | FR-L-M1 |
| AC-SA-38 | The migration script leaves an orphan untouched and emits a WARNING when bronze verification fails | Integration test | FR-L-M1 |
| AC-SA-39 | `--assume-empty` short-circuits bronze verification and is documented in `--help` | CLI test | FR-L-M1 |
| AC-SA-40 | Slice A exit benchmark: lifecycle wiring adds ≤ 5 % runtime overhead vs. the pre-Slice-A baseline on a representative incremental extraction | Benchmark report committed to `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/BENCHMARK.md` before merge | FR-L-10 |
| AC-SA-41 | Every public `WatermarkManager` method is covered by a `monkeypatch` test that replaces `spark.sql` with a mock failing the test if invoked before any adversarial input is rejected by `SQLInputValidator` | Per-method unit test | NFR-L-05 |
| AC-SA-42 | All four new exception classes expose `error_code` as a class attribute (`LHP-WM-001`..`LHP-WM-004`) and inherit from `LHPError` | Import + attribute test | §5.2 of L2 |

---

## 4. API / Config Contract

### 4.1 YAML Contract (Unchanged)

Slice A introduces **no** new YAML keys. The parent L3 §4.1 field reference remains authoritative. See [`../../L3-feature-spec.md`](../../L3-feature-spec.md) §4.1.

### 4.2 New / Amended Python API

#### 4.2.1 `WatermarkManager` (amended)

See L2 §5.1 for the canonical signatures. Summary of Slice-A-specific changes:

| Method | Change |
|---|---|
| `get_latest_watermark` | Adds `WHERE status='completed'`; adds `ORDER BY watermark_time DESC, run_id DESC LIMIT 1`. |
| `insert_new` | Replaces pre-check+insert with a single `MERGE`; checks `num_affected_rows`; retries on Delta concurrent-commit exceptions. `watermark_value` accepts `Union[str, int, Decimal]` (no `float`). |
| `mark_complete` | Signature gains `watermark_value` parameter (required). Adds terminal-failure guard in `WHERE`. |
| `mark_bronze_complete` / `mark_silver_complete` | Add terminal-failure guard in `WHERE`. (Signatures unchanged.) |
| `mark_failed` | Adds terminal-success guard in `WHERE`. Idempotent on `failed` → `failed` (latest-wins). |

#### 4.2.2 `SQLInputValidator` (new) — `lhp.extensions.watermark_manager.sql_safety`

```python
class SQLInputValidator:
    @staticmethod
    def string(value: str, max_len: int = 512) -> str: ...
    @staticmethod
    def numeric(value: Union[int, Decimal]) -> Union[int, Decimal]: ...
    @staticmethod
    def timestamp(value: datetime) -> datetime: ...  # must be tz-aware UTC
    @staticmethod
    def identifier(value: str) -> str: ...
    @staticmethod
    def uuid_or_job_run_id(value: str) -> str: ...
```

Each method returns the validated value or raises `WatermarkValidationError(field, value, reason)` (`LHP-WM-003`).

#### 4.2.3 SQL Emitters (new) — `lhp.extensions.watermark_manager.sql_safety`

```python
def sql_literal(value: str) -> str:
    """Return '<escaped>' with embedded single quotes doubled."""

def sql_numeric_literal(value: Union[int, Decimal]) -> str:
    """Return bare numeric form; caller must have passed SQLInputValidator.numeric first."""

def sql_timestamp_literal(value: datetime) -> str:
    """Return TIMESTAMP '<ISO-8601-UTC>' ; requires tz-aware UTC datetime."""

def sql_identifier(value: str) -> str:
    """Return the validated identifier verbatim when pre-quoted, else dialect-quoted."""
```

These emitters are the **only** sanctioned way to compose SQL strings in the extensions package. The white-box lint (AC-SA-14) enforces this in templates; `grep` assertions (AC-SA-13) enforce it in Python.

#### 4.2.4 `derive_run_id` (new) — `lhp.extensions.watermark_manager.runtime`

```python
def derive_run_id(dbutils: Any) -> str:
    """Return a deterministic run_id.

    Resolution order:
      1. Widget 'lhp_run_id_override' (if set) — validated, warning logged.
      2. dbutils Jobs context (jobRunId, taskRunId, currentRunAttempt).
      3. f"local-{uuid.uuid4()}" — warning logged.
    """
```

Return values are guaranteed to satisfy `SQLInputValidator.uuid_or_job_run_id` except for the `local-<uuid>` fallback, whose form is covered by a dedicated UUID branch in the validator.

#### 4.2.5 New Exception Classes

Per L2 §5.2:

```python
class DuplicateRunError(LHPError):            error_code = "LHP-WM-001"
class TerminalStateGuardError(LHPError):      error_code = "LHP-WM-002"
class WatermarkValidationError(LHPError):     error_code = "LHP-WM-003"
class WatermarkConcurrencyError(LHPError):    error_code = "LHP-WM-004"
```

All four are importable from `lhp.extensions.watermark_manager`.

### 4.3 Template Control-Flow Contract

The generated `extract_{table_slug}.py` notebook **must** follow the structure defined in L2 §5.3 verbatim. The template-rendering tests (AC-SA-01, AC-SA-04, AC-SA-35) assert this.

### 4.4 Migration Script CLI

```
$ python -m lhp.extensions.migrations.slice_a_backfill \
      --wm-catalog metadata --wm-schema orchestration \
      --bronze-catalog <cat> --bronze-schema <schema> \
      [--assume-empty] [--dry-run]

Exit codes:
  0  success (including no-op second runs)
  2  bronze verification failed for one or more keys (soft fail — logs warnings, does not modify those rows)
  1  unrecoverable error (connection failure, permission error)
```

Bronze verification query (concretising L2 OQ-3):

```sql
WITH candidate AS (
  SELECT w.run_id, w.watermark_value, w.watermark_time,
         w.source_system_id, w.schema_name, w.table_name
  FROM <wm>.watermarks w
  WHERE w.status = 'running'
)
SELECT c.run_id,
       -- 'promote' when bronze max(watermark.column) >= candidate's value
       -- within 1-second skew tolerance; else 'hold'
       CASE
         WHEN b.bronze_max_watermark IS NULL THEN 'hold'
         WHEN b.bronze_max_watermark >= c.watermark_value - INTERVAL 1 SECOND
           THEN 'promote'
         ELSE 'hold'
       END AS decision
FROM candidate c
LEFT JOIN <bronze_max_watermark_view> b
  ON (c.source_system_id, c.schema_name, c.table_name)
   = (b.source_system_id, b.schema_name, b.table_name)
```

The operator supplies `<bronze_max_watermark_view>` via `--bronze-max-view <view>` or the script falls back to scanning the bronze streaming table directly (slower, intended for small environments).

---

## 5. Edge Cases

### 5.1 Clock Skew Between Writers And Delta Commit Service

Two writers on different clusters may produce identical `watermark_time` values to sub-millisecond precision when both use `datetime.now(tz=utc)`. FR-L-04's secondary `ORDER BY run_id DESC` makes the tie-break deterministic and cluster-independent; tests (AC-SA-18) exercise this.

### 5.2 `datetime.now(tz=utc)` Monotonicity Across Retries

A Workflow task retry may produce a `completed_at` older than the prior `running` row's `started_at` if system clocks drift. The schema does not constrain ordering; monotonicity is not a Slice A guarantee. Documented here so operators do not misread it as a correctness bug.

### 5.3 `MERGE` With `WHEN NOT MATCHED BY TARGET` — Delta Version Requirement

The `MERGE … WHEN NOT MATCHED BY TARGET THEN INSERT` clause requires Databricks Runtime 13.3 LTS or later. All in-scope environments (DBR 17.3 LTS per `CLAUDE.md`) satisfy this. The Slice A installation check **must** fail fast with a clear message if the runtime is older; implemented via a `WatermarkManager.__init__`-time Spark version assertion. (Added to AC list as implicit under AC-SA-19; not a separate AC to avoid overcounting.)

### 5.4 `ConcurrentAppendException` On Checkpointing Metastore

Delta's concurrent-commit exceptions surface as Py4J-wrapped `io.delta.exceptions.ConcurrentAppendException` on DBR; the retry loop must match on both the wrapped and unwrapped class name. Test `AC-SA-21` covers the Py4J-wrapped form explicitly.

### 5.5 Retry Loop Starvation Under Persistent Contention

If contention truly exceeds the retry budget, `WatermarkConcurrencyError` is correct: the caller should reduce shard size (NFR-L-02). Increasing the retry budget is not a valid response because it hides the root cause and widens the window during which the calling notebook holds JDBC resources.

### 5.6 Interactive Notebook Runs With `local-<uuid>` Fallback

Two interactive runs in quick succession cannot collide on `run_id` because `uuid4` has 122 bits of entropy. They also cannot exercise `DuplicateRunError` by accident — that is a feature (interactive runs are non-production per FR-L-09).

### 5.7 Migration Script Run After Slice A Is Active

The script is idempotent: re-running after Slice A writes its own `running`/`completed` rows finds no unverifiable orphans (all post-Slice-A rows have the correct terminal-state wiring). AC-SA-36 exercises the second-run zero-change contract.

### 5.8 Terminal-State Guard Race With Operator Intervention

An operator running `UPDATE watermarks SET status='failed' WHERE run_id=X` between `insert_new` and `mark_complete` causes `mark_complete` to raise `TerminalStateGuardError`. This is by design (FR-L-06, FR-L-06a): the operator's intent wins, the extraction caller is informed. Tests AC-SA-24 + 2.3 cover the path.

### 5.9 `error_message` Containing UTF-8 Surrogate Pairs

Python `str[:4096]` is character-based; it will not split surrogate pairs (Python 3 strings are code-point sequences). No additional handling required.

### 5.10 `mark_failed` Called Without A Prior `insert_new`

Caller passes a `run_id` that has no watermark row. `WHERE run_id=? AND status IN (...)` matches zero rows. `mark_failed` raises `TerminalStateGuardError(run_id, current_status=None)`. The template structure (L2 §5.3) prevents this — `mark_failed` only runs inside `except` after `insert_new` committed — but the DB-layer guard still applies if someone calls the API directly.

---

## 6. Non-Goals

Slice A explicitly does **not** cover:

- **Run-scoped landing paths and `_lhp_*` metadata columns.** Slice B.
- **DataFrame persistence between `agg` and `write`.** Slice B (single-scan).
- **`Decimal`-safe numeric watermark handling with arbitrary precision.** Slice B extends the numeric validator to cover precision; Slice A accepts `int` / `Decimal` without precision checks.
- **Cross-flowgroup task/notebook uniqueness.** Slice C.
- **Workflow notebook `.py` suffix mismatch.** Slice C.
- **`job_name` sharding.** Slice C.
- **JDBC tuning passthrough (`fetchSize`, `numPartitions`, `partitionColumn`, bounds, `customSchema`).** Slice C.
- **Workspace-files-only runtime deploy.** Slice C.
- **Stage-specific `mark_bronze_complete` / `mark_silver_complete` wiring from the DLT pipeline.** Deferred (L2 OQ-1); Slice A wires `mark_complete`/`mark_failed` from the extraction notebook only.
- **Failed-row retention policy / `VACUUM` cadence.** Operational documentation only (L2 OQ-2); no code change.
- **Schema rewrite to nested `connection`/`relation`/`extraction` blocks.** Deferred.
- **Standalone watermark registry product.** Deferred.
- **Shadow-mode parity harness against `edp-data-engineering`.** Post-slice operational step.

---

## 7. Resolved Open Questions (From L2 §7.2)

| L2 OQ | Resolution | Rationale |
|---|---|---|
| OQ-1 (`mark_bronze_complete` / `mark_silver_complete` wiring) | **Defer.** Extraction notebook wires only `mark_complete` + `mark_failed`; stage-specific calls belong to the DLT pipeline, a separate artifact. Slice A still implements the DB-side terminal guard on those methods (AC-SA-23/24) so whenever they are wired they are safe. | Matches L2 §2.3 amendment scope; avoids entangling extraction notebook with DLT callback lifecycle. |
| OQ-2 (failed-row retention) | **Out of code.** Documented in a future ops runbook (`.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/OPS.md`, tracked but not a Slice A blocker). | Retention policy is environmental and changes frequently; hard-coding it is wrong granularity. |
| OQ-3 (migration consistency check) | **Specified in §4.4**: bronze verification via `max(watermark.column) >= candidate_value - INTERVAL 1 SECOND`. Operator supplies bronze view; 1-second skew is the default tolerance (rationale: Databricks Delta commit timestamps have second-level granularity in some historical versions; sub-second precision is preserved but comparisons relax to 1-second to absorb clock drift). | Concrete enough for operators; tolerance documented so it is a deliberate choice. |
| OQ-4 (`derive_run_id` on interactive runs) | **`local-<uuid4>` with warning + optional `lhp_run_id_override` widget** (FR-L-09, AC-SA-33/34). Ad-hoc backfills supply the widget; interactive exploration gets the warning and a fresh UUID. | Supports both use cases without requiring Jobs context; makes override explicit and auditable. |

---

## 8. Constitution Waivers

Per L2 §6.2 Waiver **W-A-01** (P6 — Retry safety by construction): Slice A leaves the shared landing-path retry hazard unaddressed. Retries can duplicate Parquet files; Silver CDC MERGE deduplicates. Waiver expires when Slice B merges (Tasks #2, #3, #9). **Slice A must not ship to production without Slice B also scheduled and owned.**

No additional waivers introduced by L3.

---

## 9. Traceability Summary

| L2 Requirement | L3 Scenarios | L3 Acceptance Criteria |
|---|---|---|
| FR-L-01 — Terminal success transition | 2.1, 2.3 | AC-SA-01, AC-SA-02, AC-SA-03 |
| FR-L-02 — Failure path audit | 2.2, 2.22 | AC-SA-04, AC-SA-05, AC-SA-06 |
| FR-L-03 — SQL-safe interpolation | 2.4, 2.5, 2.6, 2.21 | AC-SA-07..AC-SA-14 |
| FR-L-04 — Completion-filtered lookup | 2.7, 2.8 | AC-SA-15, AC-SA-16, AC-SA-17, AC-SA-18 |
| FR-L-05 — Atomic `insert_new` | 2.9, 2.10 | AC-SA-19, AC-SA-20, AC-SA-21, AC-SA-22 |
| FR-L-06 — Terminal-state guards (success-polarity) | 2.11 | AC-SA-23, AC-SA-24 |
| FR-L-06a — `mark_failed` guard + idempotency | 2.12, 2.13 | AC-SA-25, AC-SA-26, AC-SA-27 |
| FR-L-07 — UTC timestamps | 2.14 | AC-SA-28, AC-SA-29, AC-SA-30, AC-SA-03 |
| FR-L-08 — YAML contract stability | (no scenario — invariant) | AC-SA-31 |
| FR-L-09 — Deterministic `run_id` | 2.15, 2.16, 2.17 | AC-SA-32, AC-SA-33, AC-SA-34, AC-SA-35 |
| FR-L-10 — Perf gap acknowledgement | (FR-L-10 exit criterion) | AC-SA-40 |
| FR-L-M1 — One-shot migration | 2.18, 2.19, 2.20 | AC-SA-36, AC-SA-37, AC-SA-38, AC-SA-39 |
| NFR-L-01 — Perf neutrality | (FR-L-10 exit criterion) | AC-SA-40 |
| NFR-L-02 — Concurrency safety | 2.10 | AC-SA-20, AC-SA-21, AC-SA-22 |
| NFR-L-03 — Observability queries | (out-of-band SQL check) | — (validated in BENCHMARK.md) |
| NFR-L-04 — Testability | — | AC-SA-01..AC-SA-42 (all covered) |
| NFR-L-05 — Security boundary | 2.21 | AC-SA-14, AC-SA-41 |

---

## 10. Approval

- [x] Author (Claude): rev 1.0 drafted 2026-04-17 against L2 rev 1.1.
- [ ] Human reviewer: pending.
- [ ] L4 design **must not** begin until this L3 is approved (Constitution P10).

---

## 11. Revision History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-17 | Initial draft. Covers L2 rev 1.1 FR-L-01..10, FR-L-M1, NFR-L-01..05. Resolves L2 OQ-1..4. Adopts W-A-01 waiver unchanged. |
