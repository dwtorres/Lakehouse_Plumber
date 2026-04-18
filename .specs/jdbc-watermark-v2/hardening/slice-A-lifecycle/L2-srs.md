# L2 — Software Requirements Specification: JDBC Watermark v2 — Slice A (Lifecycle Hardening)

**Feature**: Post-review hardening of the v2 watermark lifecycle, SQL safety, and concurrency contract.
**Version**: 1.1
**Date**: 2026-04-17
**Status**: Draft (rev 1 — review feedback applied)
**Parent**: [`.specs/jdbc-watermark-v2/L2-srs.md`](../../L2-srs.md) (v2 baseline, 2026-04-14)
**Governing Constitution**: [`.specs/constitution.md`](../../../constitution.md)
**Traces to**: Branch reviews (Review #1 TD-001/002/003/005/006/012/013; Review #2 P0 latest-watermark, P1 lifecycle, P1 atomic insert, P1 mark_* guards)
**Maps to Tasks**: #1, #4, #5, #10, #11, #12

---

## 1. Purpose

Bring the lifecycle wiring, SQL safety, and concurrency semantics of `jdbc_watermark_v2` to production-safe behavior without changing the public YAML contract.

This SRS does **not** introduce new YAML fields, new source types, or new artifacts. It tightens the runtime behavior of existing artifacts so retries, concurrent runs, and partial failures are explicit and deterministic.

---

## 2. Scope

### 2.1 In Scope

- Status lifecycle wiring in the generated extraction notebook: `insert_new` → terminal state transition on both success and failure paths.
- SQL-injection-safe interpolation of watermark values, run identifiers, and table identifiers.
- Completion-filtered watermark lookups with deterministic tie-breakers.
- Atomic `insert_new` semantics under concurrent writers, including Delta concurrent-commit retry.
- Terminal-state guards on `mark_bronze_complete`, `mark_silver_complete`, `mark_complete`, **and** `mark_failed`.
- Timezone-explicit (UTC) timestamp formatting across `WatermarkManager` and the extraction template.
- One-shot migration of pre-existing `status='running'` orphan rows before Slice A activates.
- Explicit `run_id` provenance derived from Databricks job/task identifiers.
- Test coverage for concurrent inserts, same-second runs, failure-path audit trail, and SQL injection defense.

### 2.2 Out Of Scope

- Run-scoped landing paths and `_lhp_*` metadata columns. (Slice B.)
- DataFrame persistence between aggregate and write. (Slice B.)
- `Decimal`-safe numeric watermark handling. (Slice B.)
- Cross-flowgroup task/notebook uniqueness. (Slice C.)
- Workflow notebook `.py` suffix mismatch. (Slice C.)
- `job_name` sharding. (Slice C.)
- JDBC tuning passthrough (`fetchSize`, `numPartitions`, `partitionColumn`, bounds, `customSchema`). (Slice C.)
- Workspace-files-only runtime deploy (no wheel). (Slice C.)
- Schema rewrite to nested `connection`/`relation`/`extraction` blocks. (Deferred; violates Constitution P1.)
- Standalone watermark registry product exposed as a separate feature. (Deferred.)
- Shadow-mode parity harness against `edp-data-engineering`. (Post-slice operational step.)

### 2.3 Parent SRS Amendments

This slice amends the parent SRS [`L2-srs.md`](../../L2-srs.md) Section 2.2 ("Out of Scope") as follows:

- "Error retry logic inside the extraction notebook — Workflow task-level retries handle this" — **superseded**. Workflow retries are necessary but insufficient because the generated notebook never transitions the watermark row out of `running`, leaving the ledger inconsistent even on success. This slice wires explicit `mark_complete` / `mark_failed` semantics inside the notebook while keeping Workflow-level retries as the retry driver.
- "`mark_bronze_complete()`, `mark_silver_complete()`, `mark_failed()` — not wired in v2" — **superseded**. This slice wires `mark_complete` (or an equivalent terminal-success API) and `mark_failed` from the generated extraction notebook. Stage-specific `mark_bronze_complete` / `mark_silver_complete` wiring remains deferred.

---

## 3. Functional Requirements

### FR-L-01 — Terminal Status Transition On Success

The generated extraction notebook **must** call `WatermarkManager.mark_complete(run_id, row_count)` after the landing write is durably complete. The operation **must** update the row inserted by `insert_new` from `status='running'` to `status='completed'`.

The terminal-success status name is fixed as `'completed'` (decision: matches existing observability queries; renaming would break `NFR-L-03` dashboards).

On successful completion:
- The row in the watermark table **must** record `status='completed'`.
- The recorded `watermark_value` **must** equal the maximum watermark observed in the landed data for the current run.
- The `completed_at` timestamp **must** be set in UTC ISO-8601 format per FR-L-07.
- The call **must** occur *outside* the `try/except` block that wraps extraction so a failure in `mark_complete` itself cannot trigger `mark_failed` against an already-completed row. See Section 5.3.

### FR-L-02 — Failure Path Audit Trail

The generated extraction notebook **must** wrap its JDBC read, landing write, and watermark computation in a `try`/`except Exception` block. On any exception:

- `WatermarkManager.mark_failed(run_id, error_class, error_message)` **must** be called before the exception propagates.
- The row **must** transition from `status='running'` to a terminal failure status.
- `error_class` and `error_message` **must** be recorded with the exception type name and the truncated message (≤ 4096 chars).
- The exception **must** re-raise after the `mark_failed` call so Workflow task-level retries fire.

### FR-L-03 — SQL-Safe Watermark And Identifier Interpolation

All values interpolated into any SQL statement generated by the extraction template or executed by `WatermarkManager` **must** pass through a type-appropriate validator and emitter.

Validator taxonomy:

| Input category | Validator | Emitter |
|---|---|---|
| String literal (error messages, `source_system_id`, `schema_name`, `table_name`) | `SQLInputValidator.string(value, max_len)` — reject control chars, length cap 512 | `sql_literal(value)` — wrap in single quotes, double embedded quotes (`'` → `''`) |
| Numeric literal (`Decimal`, `int`, `float` watermark values) | `SQLInputValidator.numeric(value)` — accept `int`/`Decimal`; reject `float`; reject NaN/Inf | `str(value)` bare, no quotes |
| Timestamp literal (`datetime` watermark values) | `SQLInputValidator.timestamp(value)` — require timezone-aware UTC `datetime` | `TIMESTAMP '<ISO-8601-UTC>'` |
| Identifier (`jdbc_table`, column names) | `SQLInputValidator.identifier(value)` — allowlist `^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`, optional pre-quoted form `^"[^"]+"(\."[^"]+")*$` | emit verbatim (already quoted) or wrap in dialect-appropriate quotes |
| Run identifier (`run_id`) | `SQLInputValidator.uuid_or_job_run_id(value)` — accept UUID v4 or `job-<digits>-task-<digits>-attempt-<digits>` | `sql_literal(value)` |

Additional requirements:

- The current `.replace("'", "")` pattern that strips quotes **must** be removed. Stripping quotes does not prevent injection and silently corrupts legitimate input.
- `watermark_value` handling **must** branch on `watermark_type`: `'timestamp'` → `timestamp()` validator, `'numeric'` → `numeric()` validator. Type mismatches raise `WatermarkValidationError`.
- No raw f-string concatenation of untrusted values into SQL is permitted in template or runtime code.
- Parameterized queries (via Spark SQL `%()s` or PySpark DataFrame API filters) are preferred where the execution path allows; direct SQL string composition is allowed only for the `MERGE` / `UPDATE` statements in `WatermarkManager` and only through the validator/emitter pair above.

### FR-L-04 — Completion-Filtered Watermark Lookup

`WatermarkManager.get_latest_watermark(source_system_id, schema_name, table_name)` **must** return only the most recent row whose `status = 'completed'`. Non-terminal statuses (`running`, `failed`, `timed_out`, any future `landed_not_committed`) **must** be excluded from the lookup.

When multiple rows share the same `watermark_time`, the lookup **must** apply a deterministic tie-breaker: ORDER BY `watermark_time` DESC, then `run_id` DESC (lexicographic). `LIMIT 1` after this ordering is required.

When no terminal-success row exists, the method **must** return `None`. The extraction notebook already treats `None` as "no prior run" and performs a full load — this contract is preserved.

**Migration hazard**: Any existing rows stuck in `status='running'` (prior-branch bug, Task #5 root cause) are silently excluded by the new filter. Without migration, the first run after deploy performs a full reload on every affected table. See FR-L-M1 for the one-shot backfill required before Slice A activates in any non-dev environment.

### FR-L-05 — Atomic Duplicate-Run Protection

`WatermarkManager.insert_new(run_id, ...)` **must** be atomic under concurrent callers operating on the same `run_id`.

- The pre-check query followed by `MERGE` pattern **must** be replaced with a single `MERGE INTO ... USING (SELECT ...) ON target.run_id = source.run_id WHEN NOT MATCHED BY TARGET THEN INSERT ...` statement.
- The `MERGE` **must** capture `num_affected_rows`. When zero, the `run_id` already existed; `insert_new` **must** raise `DuplicateRunError(run_id)`.
- Databricks informational primary keys **must not** be relied upon for uniqueness. Uniqueness is enforced by `MERGE` semantics and the `num_affected_rows` check.
- The `MERGE` **must** handle Delta optimistic concurrency: on `ConcurrentAppendException` or `ConcurrentDeleteReadException` from the underlying Delta transaction, `insert_new` **must** retry with jittered exponential backoff (base 100 ms, factor 2, jitter ±50 %) up to 5 attempts before surfacing the exception as `LHPError(LHP-WM-004)`. This retry is internal to `insert_new` and not visible to the caller.

### FR-L-06 — Terminal-State Guards On Late Updates

`mark_bronze_complete`, `mark_silver_complete`, and `mark_complete` **must** refuse to overwrite terminal failure states.

- The `UPDATE` statement **must** include `WHERE run_id = ? AND status NOT IN ('failed', 'timed_out', 'landed_not_committed')`.
- When the `UPDATE` affects zero rows, the method **must** raise `TerminalStateGuardError(run_id, current_status)`.
- Callers **may** inspect the current status before calling these methods, but the database-side guard is the authoritative defense.

### FR-L-06a — `mark_failed` Guard Against Overwriting Success

`mark_failed` **must** refuse to overwrite terminal success states.

- The `UPDATE` statement **must** include `WHERE run_id = ? AND status IN ('running', 'failed', 'timed_out')`.
- When the `UPDATE` affects zero rows, the method **must** raise `TerminalStateGuardError(run_id, current_status)`. Callers wrapping `mark_failed` in a generic exception handler **must** not suppress this error.
- `mark_failed` on a row already in `status='failed'` **must** succeed (idempotent) and **must** update `error_class`, `error_message`, and `failed_at` to reflect the most recent failure. Latest failure wins.
- Rationale: a failure inside `mark_complete` can propagate to the extraction template's `except` block, which would call `mark_failed` on an already-completed row. Without this guard, a transient network error after success corrupts the audit trail.

### FR-L-07 — UTC-Explicit Timestamp Formatting

All timestamps written by `WatermarkManager` and by the extraction template **must** use UTC with explicit timezone awareness on the Python side.

- Python-side formatting **must** use `datetime.now(tz=datetime.timezone.utc).isoformat(timespec='microseconds')`.
- No use of `str(datetime.datetime.now())` (naive local time) or `datetime.utcnow()` (also naive) is permitted.
- On the SQL side, emit as `TIMESTAMP '<ISO-8601-UTC>'` literal per FR-L-03 timestamp emitter.
- **Delta column semantics**: `TIMESTAMP` columns in Databricks store UTC microseconds internally but interpret string literals and display values using the session timezone (`spark.sql.session.timeZone`). To keep round-trips deterministic:
  - All DML against the watermark tables executed by `WatermarkManager` **must** set `spark.sql.session.timeZone='UTC'` on the active Spark session before issuing any statement.
  - Tests **must** verify that values written from a non-UTC session timezone round-trip as the same UTC instant when read from a UTC session.
- `TIMESTAMP_NTZ` is not used in this slice; switching is a schema change, out of scope.

### FR-L-08 — Backward-Compatible YAML Contract

This slice **must not** introduce any new top-level YAML keys, new `source.*` keys, or new `watermark.*` keys. Existing flowgroups that pass validation against the v2 baseline SRS **must** continue to pass validation after this slice lands.

Behavior changes for existing configs are limited to runtime semantics: status transitions, SQL escaping, lookup filtering, atomicity, terminal guards, and timestamp formatting.

### FR-L-09 — Deterministic `run_id` Provenance

The generated extraction notebook **must** derive `run_id` from Databricks job/task runtime identifiers, not from `uuid.uuid4()`, so that task retries reuse the prior `run_id` when appropriate and `DuplicateRunError` is a reachable condition under concurrent manual reruns.

- Format: `"job-{job_run_id}-task-{task_run_id}-attempt-{attempt_number}"`.
- `job_run_id` is sourced from `dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().get()` or via the `job_run_id` widget/task parameter.
- `task_run_id` is sourced from the equivalent `taskRunId()` context accessor or task parameter.
- `attempt_number` is sourced from the Databricks Jobs `currentRunAttempt` context (or equivalent).
- Fallback (local dev, interactive): when any of the above are unavailable, fall back to `f"local-{uuid.uuid4()}"`. This fallback **must** emit a `logger.warning` identifying the code path as non-production.
- Databricks Jobs task-level retry semantics: a retried attempt increments `attempt_number` and therefore produces a distinct `run_id`. Manual rerun from the Jobs UI produces a new `job_run_id`. Neither produces a duplicate `run_id` under normal operation.
- `DuplicateRunError` coverage: concurrent manual reruns using the same externally-supplied `run_id` override (test/backfill scenario) **must** trigger the error. This is the contract tested by NFR-L-04 FR-L-05 case.

### FR-L-10 — Acknowledged Performance Gap (Slice A Exit Criterion)

Slice A does **not** resolve the double-JDBC-scan issue (Task #3) or the shared-landing-path retry hazard (Task #2). Those fixes belong to Slice B.

- The template **may** call `df.count()` inside the `try` block as the source of `row_count` for `mark_complete`. This is a second JDBC scan and **must** be removed in Slice B by persisting the DataFrame and counting from the cached plan.
- Slice A **must not** attempt a partial perf fix (e.g. `cache()` before `agg`) because serverless compute restrictions motivated earlier removal; a proper fix requires Slice B's full landing-path redesign.
- Exit criterion: Slice A is complete when FR-L-01 through FR-L-09 pass tests AND a benchmark shows the extra round-trips from lifecycle wiring add ≤ 5 % to a representative incremental extraction runtime. The double-scan regression is counted against the Slice B baseline, not Slice A.

### FR-L-M1 — One-Shot Migration Of Pre-Existing Orphan Runs

Before Slice A activates against any watermark table that already contains rows (i.e. any non-empty deployment), a migration step **must** run exactly once per environment.

Responsibilities:

- Identify every `(source_system_id, schema_name, table_name)` key that has rows but no `status='completed'` row (i.e. only `running`, `failed`, or legacy statuses).
- For each such key where a `running` row exists whose `watermark_value` is demonstrably consistent with the downstream bronze table (verifiable via a bronze-side row count or `max(_ingestion_time)` comparison — procedure defined in L3), **promote** that row to `status='completed'` with a `migration_note='slice-A-backfill-YYYYMMDD'` annotation and preserve the original `watermark_time`.
- For each such key where no such correspondence can be established, **leave the rows alone** and log a warning listing the key so that the next extraction performs a full reload as a deliberate choice, not an accident.
- The migration **must** be idempotent: running it twice on the same environment produces no additional changes.
- The migration **must** be delivered as a standalone script under `src/lhp/extensions/migrations/slice_a_backfill.py` (not an auto-run on `WatermarkManager` startup) and **must** require explicit invocation by an operator.
- Dev/test environments without historical data **may** skip the migration; a flag `--assume-empty` short-circuits the consistency check.

Rationale: without this migration, enabling FR-L-04 (completion filter) causes every table with an orphaned `running` row to full-reload on its next extraction.

---

## 4. Non-Functional Requirements

### NFR-L-01 — Performance Neutrality

The lifecycle wiring **must not** add more than one additional round-trip to the watermark Delta table per extraction run beyond the `insert_new` already present (one `insert_new` + one terminal update = two committed writes per run).

The completion-filtered lookup in `get_latest_watermark` **must** return in sub-second P99 latency on watermark tables of up to 1M rows, achieved via Z-ORDER on `(source_system_id, schema_name, table_name, watermark_time)` and a predicate-pushdown-friendly `WHERE status='completed'` clause.

A benchmark **must** be published as part of Slice A's exit criteria (see FR-L-10) using a representative incremental extraction; the benchmark serves as the authoritative target rather than a theoretical ms number.

### NFR-L-02 — Concurrency Safety

The `MERGE`-based `insert_new` **must** be safe under at least N concurrent writers on the same Delta table where `N = max_shard_size` — the upper bound of the Slice C sharding policy (currently 200 tasks per job shard).

- Distinct `run_id`s: no row lost, no row duplicated, no `ConcurrentAppendException` visible to the caller (absorbed by the FR-L-05 retry loop).
- Identical `run_id` (manual rerun / backfill override): exactly one winner commits; all other callers raise `DuplicateRunError`.
- P99 insert latency under N=200 concurrent writers **must** be under 10 seconds including the retry budget. If this cannot be achieved, the shard size is the variable to reduce, not the retry budget.

### NFR-L-03 — Observability

After this slice lands, the following queries **must** return non-empty, correct results against the watermark table:

- Count of runs in `status='completed'` per (`source_system_id`, `schema_name`, `table_name`) in the last 24h.
- Count of runs in `status='failed'` with `error_class` grouped for the last 24h.
- Most recent `watermark_value` per table, used by `get_latest_watermark`.

### NFR-L-04 — Testability

Every functional requirement in Section 3 **must** have at least one automated test. Tests **must** cover:

- FR-L-01: successful run transitions status to `'completed'`, records watermark; `mark_complete` failure does not trigger `mark_failed` overwrite.
- FR-L-02: raised exception in extraction path triggers `mark_failed`, re-raises.
- FR-L-03: adversarial inputs (`' OR 1=1 --`, `"; DROP TABLE x; --`, Unicode quotes, null bytes, `Decimal('NaN')`, timezone-naive `datetime`) produce validation errors of the correct typed subclass before reaching SQL.
- FR-L-04: non-terminal rows are ignored by `get_latest_watermark`; same-second rows tie-break deterministically; zero terminal rows returns `None`.
- FR-L-05: concurrent `insert_new` with identical `run_id` produces exactly one success and one `DuplicateRunError`; simulated `ConcurrentAppendException` is absorbed by the internal retry loop.
- FR-L-06: `mark_complete` on a `failed` row raises `TerminalStateGuardError` and does not mutate the row.
- FR-L-06a: `mark_failed` on a `completed` row raises `TerminalStateGuardError`; `mark_failed` on an existing `failed` row updates latest error fields (idempotent with latest-wins).
- FR-L-07: timestamps written from a session with `spark.sql.session.timeZone='America/Chicago'` round-trip as the same UTC instant when read from a session with `timeZone='UTC'`.
- FR-L-09: `run_id` is derived from job/task/attempt identifiers when present; fallback emits a warning; retried attempt produces distinct `run_id`.
- FR-L-M1: migration script is idempotent; correctly promotes verifiable orphans; leaves unverifiable keys untouched and logs them.

### NFR-L-05 — Security Boundary

No value originating from external sources (JDBC query results, YAML configs, task parameters, user-provided `run_id`) may be interpolated into SQL without passing through the FR-L-03 validator/emitter layer.

Enforcement via white-box lint, not black-box output scanning:

- Jinja templates **must** use the named helpers `sql_literal(x)`, `sql_identifier(x)`, `sql_timestamp_literal(x)`, or `sql_numeric_literal(x)` when interpolating into SQL string contexts.
- A unit test **must** parse the `jdbc_watermark_job.py.j2` AST (Jinja2's parser) and walk every `{{ Name }}` and `{{ Getattr }}` expression. For each, determine lexical context; if the surrounding template source matches `SQL_CONTEXTS = {r'\bINSERT\b', r'\bMERGE\b', r'\bUPDATE\b', r'\bSELECT\b', r'\bDELETE\b', r'\bWHERE\b'}` (simple heuristic acceptable since templates are small), the expression **must** be wrapped in one of the named helpers. Bare `{{ x }}` inside a SQL context fails the test.
- `WatermarkManager` Python code **must** have unit tests injecting each adversarial input from FR-L-03 through every public method and asserting that `WatermarkValidationError` is raised before any Spark SQL call executes. A pytest `monkeypatch` replaces `spark.sql` with a mock that fails the test if called.

---

## 5. External Interfaces

### 5.1 WatermarkManager Public API (Amended)

```python
class WatermarkManager:
    def get_latest_watermark(
        self,
        source_system_id: str,
        schema_name: str,
        table_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Return most recent terminal-success row, or None.
        Filters by status IN ('completed',) and orders by watermark_time DESC,
        run_id DESC, LIMIT 1."""

    def insert_new(
        self,
        run_id: str,
        source_system_id: str,
        schema_name: str,
        table_name: str,
        watermark_value: Union[str, int, Decimal],
        watermark_type: Literal['timestamp', 'numeric'],
        previous_watermark_value: Optional[Union[str, int, Decimal]] = None,
    ) -> None:
        """Atomic insert. Raises DuplicateRunError on run_id conflict.
        All values pass through SQLInputValidator before SQL composition."""

    def mark_complete(
        self,
        run_id: str,
        row_count: int,
    ) -> None:
        """Transition run_id from 'running' to 'completed'.
        Raises TerminalStateGuardError if current status is terminal failure."""

    def mark_failed(
        self,
        run_id: str,
        error_class: str,
        error_message: str,
    ) -> None:
        """Transition run_id to 'failed'. Idempotent on repeated terminal failure.
        error_message truncated at 4096 chars."""
```

### 5.2 Exception Types (New)

| Error code | Class | Raised by | Condition |
|---|---|---|---|
| `LHP-WM-001` | `DuplicateRunError(run_id)` | `insert_new` | `run_id` already exists in the watermark table. |
| `LHP-WM-002` | `TerminalStateGuardError(run_id, current_status)` | `mark_complete`, `mark_bronze_complete`, `mark_silver_complete`, `mark_failed` | Target row is in a status that the caller is not allowed to overwrite (failure → success via `mark_complete`, or success → failure via `mark_failed`). |
| `LHP-WM-003` | `WatermarkValidationError(field, value, reason)` | `SQLInputValidator` | Input rejected by per-type validator (injection attempt, wrong type, out-of-range value). |
| `LHP-WM-004` | `WatermarkConcurrencyError(run_id, attempts)` | `insert_new` internal retry loop | Delta concurrent-commit retry budget exhausted. |

All four inherit from `LHPError` (per existing LHP error hierarchy) and expose `error_code` as a class attribute. Each class **must** implement `__str__` producing a single-line message suitable for `mark_failed(error_class, error_message)` propagation.

### 5.3 Template Contract

The generated extraction notebook **must** structurally follow this control flow. Note the explicit separation of `insert_new`, the extraction `try` block, and `mark_complete`:

```python
from lhp.extensions.watermark_manager import (
    WatermarkManager,
    DuplicateRunError,
    TerminalStateGuardError,
)
from lhp.extensions.watermark_manager.runtime import derive_run_id  # FR-L-09

spark.conf.set("spark.sql.session.timeZone", "UTC")  # FR-L-07

run_id = derive_run_id(dbutils)  # job/task/attempt-derived, fallback to uuid with warning
wm = WatermarkManager(catalog=..., schema=...)

# insert_new is OUTSIDE the try block: a DuplicateRunError here means no
# watermark row was created for this invocation, so mark_failed has nothing
# to transition and must not be called.
wm.insert_new(run_id=run_id, ...)

extraction_succeeded = False
try:
    df = spark.read.format("jdbc")...load()
    df.write.mode("append").format("parquet").save(landing_path)
    max_hwm = df.agg(F.max(watermark_column)).first()[0]
    row_count = df.count()  # FR-L-10 acknowledged second scan; Slice B removes
    extraction_succeeded = True
except Exception as e:
    # FR-L-02: audit the failure, then re-raise for Workflow retry.
    wm.mark_failed(
        run_id=run_id,
        error_class=type(e).__name__,
        error_message=str(e)[:4096],
    )
    raise

# mark_complete is OUTSIDE and AFTER the try block: a failure here must
# NOT re-enter the except branch and call mark_failed on a row whose
# extraction actually succeeded. FR-L-06a enforces this at the DB layer
# as defence-in-depth, but the template structure is the first line of
# defence.
if extraction_succeeded:
    try:
        wm.mark_complete(run_id=run_id, row_count=row_count, watermark_value=max_hwm)
    except TerminalStateGuardError:
        # Row was already moved to a terminal failure state (e.g. by a
        # concurrent operator intervention). Log and re-raise without
        # overwriting anything.
        raise
```

The template **must not**:
- Call `mark_failed` inside the `mark_complete` error handler.
- Use `uuid.uuid4()` as the primary `run_id` source (fallback only, via `derive_run_id`).
- Interpolate any Python value into a SQL string without using the FR-L-03 validated emitters.
- Set session timezone to anything other than UTC for DML against watermark tables.

*Note: DataFrame persistence, run-scoped paths, and single-scan semantics are addressed in Slice B (Task #2, #3) and intentionally absent from this structural contract. Slice A accepts the double-scan perf regression per FR-L-10.*

---

## 6. Constitution Conformance

### 6.1 Principles Satisfied By This Slice

- **P1 (Additive)**: No new types, no schema changes, no `_v3`.
- **P3 (Thin notebook)**: Template delegates all state transitions to `WatermarkManager`; control flow is the only template-owned concern.
- **P5 (Registry SoT)**: Completion-filtered lookup, terminal transitions, and atomic insert with retry enforce registry authority.
- **P9 (SQL safety)**: Per-type validator and emitter required on every interpolation path; enforced by white-box lint (NFR-L-05).
- **P10 (Human validation)**: User must approve this L2 before L3 work begins.

### 6.2 Waivers (Explicit, Scoped, Expiring)

| ID | Principle | Scope | Reason | Expiry |
|---|---|---|---|---|
| W-A-01 | **P6 (Retry safety by construction)** | Slice A only. Extraction landing remains shared append-only; retries can duplicate files. | Slice A is scoped to lifecycle + SQL safety + concurrency. Landing retry safety is a materially larger change (run-scoped paths, metadata columns, DLT-side dedupe verification) and ships in Slice B. | Expires when Slice B (Tasks #2, #3, #9) merges. Slice A **must not** ship to any production environment without Slice B also scheduled and owned. |

The waiver is explicit so that reviewers and operators know retry-safety is partially delivered and must not rely on Slice A alone in production. No other principle is waived.

---

## 7. Decisions And Remaining Open Questions

### 7.1 Decisions Made In This Revision

- **Terminal-success status name**: `'completed'` (FR-L-01). Matches existing observability queries per Review #1 Doc Review; renaming would be a breaking observability change.
- **`mark_failed` idempotency**: latest failure wins (FR-L-06a). `mark_failed` on an already-`failed` row updates `error_class`, `error_message`, `failed_at`.
- **`mark_failed` guard**: blocks overwriting `'completed'` (FR-L-06a). Template control flow is first defense; DB guard is authoritative.
- **`run_id` source**: job/task/attempt-derived (FR-L-09); `uuid4` is fallback with warning.
- **Concurrent-commit retries**: absorbed inside `insert_new` via jittered exponential backoff, budget 5, surfaced as `LHP-WM-004` (FR-L-05).
- **TIMESTAMP handling**: `spark.sql.session.timeZone='UTC'` set inside `WatermarkManager` DML (FR-L-07). No `TIMESTAMP_NTZ` migration.
- **Known Slice A perf regression**: `df.count()` is a second JDBC scan (FR-L-10). Slice B fixes.

### 7.2 Remaining Open Questions

1. **Stage-specific `mark_bronze_complete` / `mark_silver_complete`**: wire in Slice A or defer? Recommendation: defer. The generated extraction notebook is extraction-only; bronze/silver completion belongs to a callback in the DLT pipeline, which is a distinct artifact. Confirm with reviewer.
2. **Failed-row retention**: how long do `failed` rows stay in the watermark table? Recommendation: leave retention policy out of code; document operational `VACUUM` cadence in the ops runbook. Confirm.
3. **Migration consistency check mechanics (FR-L-M1)**: how exactly does the migration script verify that an orphan `running` row corresponds to data already landed in bronze? Candidate: compare `watermark_value` against `max(<watermark_column>)` observed in the bronze streaming table. Needs L3 to specify the query and the tolerance for clock skew. Risk: if the check is too permissive we promote bad rows; if too strict we force unnecessary full reloads.
4. **`derive_run_id` behavior on manual notebook runs**: interactive notebooks attached to a cluster do not expose `jobRunId`. Fallback is `uuid4` per FR-L-09. Is that acceptable for ad-hoc backfills, or do we require an explicit widget override? Decision deferred to L3 after product input.

---

## 8. Traceability Matrix

| Requirement | Review Finding | Task ID | Constitution Principle |
|---|---|---|---|
| FR-L-01 | R1 TD-006; R2 P1 "never transitions from running" | #5 | P5 |
| FR-L-02 | R1 TD-005 "no try/except, mark_failed never called" | #5 | P5, P6 |
| FR-L-03 | R1 TD-001/002/003 "SQL injection"; R1 TD-010 Decimal; R2 P2 numeric precision | #1, #9 | P9 |
| FR-L-04 | R1 TD-012; R2 P0 "latest watermark ignores completion state" | #4 | P5 |
| FR-L-05 | R2 P1 "duplicate run protection not atomic" | #10 | P5 |
| FR-L-06 | R2 P1 "late stage updates overwrite terminal failures" | #11 | P5 |
| FR-L-06a | Review of L2 rev 0 — mark_failed can overwrite success on mark_complete failure | #11 | P5 |
| FR-L-07 | R1 TD-013 "timestamp format no tz control" | #12 | P9 (data integrity) |
| FR-L-08 | N/A (additive invariant) | all Slice A | P1 |
| FR-L-09 | Review of L2 rev 0 — `DuplicateRunError` unreachable with `uuid4()` | #20 (new) | P5, P10 |
| FR-L-10 | Slice A/Slice B boundary explicit ack | tracked under #3 (Slice B) | P6 (waiver W-A-01) |
| FR-L-M1 | Review of L2 rev 0 — mass full-reload on deploy | #21 (new) | P5 |

---

## 9. Revision History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-17 | Initial draft. |
| 1.1 | 2026-04-17 | Review feedback: B-1 (FR-L-M1 migration), B-2 (FR-L-06a + Section 5.3 template restructure), B-3 (FR-L-05 Delta retry loop), B-4 (Section 6 waiver formalized). F-1 per-type validators. F-2 Databricks TIMESTAMP semantics. F-3 `derive_run_id` + FR-L-09. F-4 NFR-L-01 dropped ms number. F-5 NFR-L-02 shard-sized. F-6 white-box lint. P2 nits: status name committed, error codes assigned, Open Q #4 decided, `row_count` perf regression documented. |

---

## 10. Approval

- [x] Author (Claude): rev 1.1 drafted 2026-04-17, incorporating 4 blockers + 6 flags from self-review.
- [x] Human reviewer: approved 2026-04-17 (dwtorres@gmail.com).
- [x] L3 work unblocked (Constitution P10).

---

## 11. Appendix: New Tasks Added By This Revision

The following tasks are added to the 19 already tracked to cover requirements introduced in revision 1.1:

- **#20**: Implement `derive_run_id` helper in `src/lhp/extensions/watermark_manager/runtime.py`; update template to use it. (FR-L-09)
- **#21**: Implement + ship one-shot migration script `src/lhp/extensions/migrations/slice_a_backfill.py`; document operator runbook. (FR-L-M1)

Existing tasks that must be updated in scope per this revision:

- **#1** (SQL injection fix) — extend to per-type validators and white-box template lint.
- **#4** (status filter) — include deterministic tie-break on `(watermark_time DESC, run_id DESC)`.
- **#5** (lifecycle wiring) — enforce `mark_complete` outside `try`; set `spark.sql.session.timeZone='UTC'` before DML.
- **#10** (atomic insert_new) — add Delta `ConcurrentAppendException` retry loop with jittered backoff; surface as `LHP-WM-004`.
- **#11** (terminal-state guards) — extend to `mark_failed` (FR-L-06a), not only `mark_*_complete`.
- **#12** (tz-explicit timestamps) — add session timezone assertion + round-trip test from non-UTC session.
