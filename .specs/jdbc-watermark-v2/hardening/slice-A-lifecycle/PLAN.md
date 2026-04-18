# Implementation Plan — JDBC Watermark v2 Slice A (Lifecycle Hardening)

**Version**: 1.0
**Date**: 2026-04-17
**Parent L3**: [`L3-feature-spec.md`](./L3-feature-spec.md) v1.0
**Parent L2**: [`L2-srs.md`](./L2-srs.md) rev 1.1 (approved)

---

## 1. Overview

Slice A hardens the runtime of `jdbc_watermark` without changing YAML. Work splits into four phases: (1) package + foundation primitives (exceptions, SQL safety, UTC session), (2) `WatermarkManager` method hardening (atomic insert, completion filter, terminal guards), (3) template + `derive_run_id` + AST lint, (4) migration script + exit benchmark.

Current state:
- `src/lhp/extensions/watermark_manager.py` — monolithic 709-line module. Must be refactored to package `watermark_manager/` to host `sql_safety`, `runtime`, `migrations` submodules per L3 §4.2.
- Template `src/lhp/templates/load/jdbc_watermark_job.py.j2` — 117 lines. Needs `try/except` + terminal wiring + emitter-safe interpolation.
- Tests: one integration file `tests/test_jdbc_watermark_v2_integration.py`. Gets extended per-phase.

---

## 2. Architecture Decisions

- **Package refactor first.** Single monolithic module cannot absorb `sql_safety` + `runtime` + `migrations` submodules cleanly; defer rename risk to task #1 so later tasks stay import-stable.
- **Exceptions + SQL safety are pure foundation.** No Spark, no I/O; unlocks parallel method-level work after Phase 1.
- **One method per task in Phase 2.** Keeps diffs small and test surface focused; lets reviewers read each change against its FR.
- **Template restructure is one task.** The L2 §5.3 control-flow contract is indivisible — splitting would leave the file in a half-wired state.
- **Migration script is operator-facing, not auto-run.** Ships as `python -m lhp.extensions.migrations.slice_a_backfill` per L3 §4.4; not invoked by `WatermarkManager.__init__`.
- **White-box AST lint is a unit test, not a pre-commit hook.** CI failure is enough; adding a hook is Slice-C ergonomics work.
- **Retry loop is internal to `insert_new`.** Not a decorator, not shared infra — keeps Delta-specific retry logic out of generic utility surface (NFR-L-02 scoped to one method).
- **Benchmark = plaintext markdown report, not a dashboard.** Sufficient for exit criterion (FR-L-10); observability dashboards are ops work (NFR-L-03).

---

## 3. Task List

### Phase 1 — Foundation

#### Task 1: Refactor `watermark_manager.py` → `watermark_manager/` package

**Description**: Convert monolithic module to package. Preserve every public import (`WatermarkManager`, `DuplicateRunError` if present, etc.) via re-exports in `__init__.py`. No behaviour change.

**Acceptance criteria**:
- [ ] `src/lhp/extensions/watermark_manager/__init__.py` re-exports current public API unchanged
- [ ] `src/lhp/extensions/watermark_manager/_manager.py` holds the class body
- [ ] All existing `from lhp.extensions.watermark_manager import ...` statements still resolve
- [ ] Existing integration test `tests/test_jdbc_watermark_v2_integration.py` passes without modification

**Verification**:
- [ ] `pytest tests/test_jdbc_watermark_v2_integration.py -v`
- [ ] `python -c "from lhp.extensions.watermark_manager import WatermarkManager; print(WatermarkManager)"`
- [ ] `grep -r "from lhp.extensions.watermark_manager" src/ tests/` — all imports still valid

**Dependencies**: None
**Files touched**: `src/lhp/extensions/watermark_manager.py` (deleted), `src/lhp/extensions/watermark_manager/__init__.py` (new), `src/lhp/extensions/watermark_manager/_manager.py` (new)
**Scope**: S
**Traces to**: (structural prerequisite for L3 §4.2)

---

#### Task 2: New exception classes (`LHP-WM-001`..`LHP-WM-004`)

**Description**: Add `DuplicateRunError`, `TerminalStateGuardError`, `WatermarkValidationError`, `WatermarkConcurrencyError` in `watermark_manager/exceptions.py`. All inherit `LHPError`, expose `error_code` class attribute, single-line `__str__`.

**Acceptance criteria**:
- [ ] Four classes exist with the codes listed in L2 §5.2
- [ ] All four importable from `lhp.extensions.watermark_manager`
- [ ] `__str__` returns a one-line message suitable for `mark_failed(error_class, error_message)` propagation
- [ ] Each inherits `LHPError`

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_exceptions.py -v` (new file)
- [ ] Covers AC-SA-42

**Dependencies**: #1
**Files touched**: `src/lhp/extensions/watermark_manager/exceptions.py` (new), `src/lhp/extensions/watermark_manager/__init__.py` (re-export), `tests/extensions/watermark_manager/test_exceptions.py` (new)
**Scope**: XS
**Traces to**: L2 §5.2, AC-SA-42

---

#### Task 3: `SQLInputValidator` + emitters (`sql_safety` module)

**Description**: Implement five static validators + four emitters per L3 §4.2.2, §4.2.3. Reject `.replace("'", "")`-style shortcuts. Raise `WatermarkValidationError` on failure. No Spark dependency.

**Acceptance criteria**:
- [ ] `SQLInputValidator.string / numeric / timestamp / identifier / uuid_or_job_run_id` match L2 FR-L-03 taxonomy
- [ ] Emitters `sql_literal / sql_numeric_literal / sql_timestamp_literal / sql_identifier` exist with documented contracts
- [ ] `numeric` rejects `float`, `Decimal('NaN')`, `Decimal('Infinity')`
- [ ] `timestamp` requires tz-aware UTC `datetime`
- [ ] `identifier` honours both regex forms (bare + pre-quoted)
- [ ] `uuid_or_job_run_id` accepts both UUID-v4 and `job-\d+-task-\d+-attempt-\d+`, and `local-<uuid>` fallback form
- [ ] `sql_literal` doubles embedded single quotes

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_sql_safety.py -v`
- [ ] Parametric adversarial inputs from FR-L-03 all raise `WatermarkValidationError`
- [ ] Covers AC-SA-07..13

**Dependencies**: #2
**Files touched**: `src/lhp/extensions/watermark_manager/sql_safety.py` (new), `tests/extensions/watermark_manager/test_sql_safety.py` (new)
**Scope**: M
**Traces to**: FR-L-03, AC-SA-07..14

---

#### Task 4: UTC session-timezone helper

**Description**: Add private `_ensure_utc_session(spark)` helper that sets `spark.sql.session.timeZone='UTC'` idempotently. Called as first statement of every DML method on `WatermarkManager`. Store prior value is NOT required (writes go through a single session; FR-L-07 does not mandate restore).

**Acceptance criteria**:
- [ ] `_ensure_utc_session(spark)` exists in `watermark_manager/_manager.py`
- [ ] Calling it twice is a no-op (idempotent)
- [ ] Every `WatermarkManager` method that issues DML calls it before the first `spark.sql(...)`

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_utc_session.py -v`
- [ ] Spy on `spark.conf.set` → asserted called with `("spark.sql.session.timeZone", "UTC")` before any `spark.sql`

**Dependencies**: #1
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_utc_session.py` (new)
**Scope**: XS
**Traces to**: FR-L-07, AC-SA-28

---

### Checkpoint A — Foundation

- [ ] `pytest tests/extensions/watermark_manager/` all green
- [ ] `pytest tests/test_jdbc_watermark_v2_integration.py` still passes (no regressions)
- [ ] `python -c "from lhp.extensions.watermark_manager import (WatermarkManager, DuplicateRunError, TerminalStateGuardError, WatermarkValidationError, WatermarkConcurrencyError, SQLInputValidator, sql_literal, sql_numeric_literal, sql_timestamp_literal, sql_identifier)"` succeeds
- [ ] No Phase 2 task started yet

---

### Phase 2 — `WatermarkManager` Method Hardening

Tasks 5–9 touch different methods on one class. Task 5 lands first (changes class state most); tasks 6–9 can ship in parallel PRs or a single sequenced batch.

#### Task 5: `insert_new` — atomic MERGE + Delta retry

**Description**: Replace pre-check-then-insert with `MERGE INTO … USING (SELECT …) ON t.run_id = s.run_id WHEN NOT MATCHED BY TARGET THEN INSERT`. Read `num_affected_rows`; raise `DuplicateRunError` on zero. Wrap in retry loop: 5 attempts, jittered exponential backoff (base 100 ms, factor 2, jitter ±50 %) on `ConcurrentAppendException` / `ConcurrentDeleteReadException` (and their Py4J-wrapped forms). Budget exhaustion → `WatermarkConcurrencyError` with `__cause__`. All inputs pass through `SQLInputValidator` + emitters. DBR version check at `__init__` (≥ 13.3 LTS) — fail fast with clear message.

**Acceptance criteria**:
- [ ] Single `MERGE` statement covers the happy path (no pre-`SELECT`)
- [ ] Zero-affected MERGE → `DuplicateRunError(run_id)`
- [ ] Retry loop absorbs `ConcurrentAppendException` (both wrapped + unwrapped class names)
- [ ] Budget exhaustion → `WatermarkConcurrencyError(run_id, attempts=5)` with original exception as `__cause__`
- [ ] Every interpolated value uses an emitter; no f-string concatenation
- [ ] `watermark_value: Union[str, int, Decimal]` — `float` rejected with `WatermarkValidationError`

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_insert_new.py -v`
- [ ] Mock Spark surfaces `ConcurrentAppendException` for first N attempts, success on N+1 → asserted call count + sleep durations
- [ ] Two-thread integration test with identical `run_id` → exactly one `DuplicateRunError`
- [ ] Covers AC-SA-19, AC-SA-20, AC-SA-21, AC-SA-22

**Dependencies**: #2, #3, #4
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_insert_new.py` (new)
**Scope**: M
**Traces to**: FR-L-05, NFR-L-02, AC-SA-19..22

---

#### Task 6: `get_latest_watermark` — completion filter + tie-break

**Description**: Add `WHERE status = 'completed'`; add `ORDER BY watermark_time DESC, run_id DESC LIMIT 1`. Return `None` when no terminal-success row exists. Log `WARNING` with key when `None` is returned so operators see pre-migration full-reload intent.

**Acceptance criteria**:
- [ ] SQL contains `WHERE status = 'completed'`
- [ ] Tie-break `ORDER BY watermark_time DESC, run_id DESC LIMIT 1`
- [ ] Returns `None` when no completed row exists
- [ ] `WARNING` log emitted on `None` path with `(sys_id, schema, table)` key

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_get_latest.py -v`
- [ ] Property test: 100 repetitions with identical `watermark_time` → identical winner
- [ ] Covers AC-SA-15, AC-SA-16, AC-SA-17, AC-SA-18

**Dependencies**: #2, #3, #4
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_get_latest.py` (new)
**Scope**: S
**Traces to**: FR-L-04, AC-SA-15..18

---

#### Task 7: `mark_complete` — terminal guard + required `watermark_value`

**Description**: Add `watermark_value` to signature (required). `UPDATE` statement adds `AND status NOT IN ('failed','timed_out','landed_not_committed')`. Zero-row update → `TerminalStateGuardError(run_id, current_status)` where `current_status` is read back from the table. Emit `completed_at` as UTC ISO-8601 microsecond literal.

**Acceptance criteria**:
- [ ] Signature: `mark_complete(run_id, row_count, watermark_value)`
- [ ] `WHERE` clause has the three-status guard
- [ ] Zero-row → `TerminalStateGuardError` with `current_status` populated
- [ ] `completed_at` literal matches the AC-SA-03 regex

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_mark_complete.py -v`
- [ ] Integration test: set row to `'failed'`, call `mark_complete` → `TerminalStateGuardError`; row unchanged
- [ ] Covers AC-SA-01 (partial — template side in #11), AC-SA-02, AC-SA-03, AC-SA-23, AC-SA-24

**Dependencies**: #2, #3, #4
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_mark_complete.py` (new)
**Scope**: S
**Traces to**: FR-L-01, FR-L-06, FR-L-07, AC-SA-02/03/23/24

---

#### Task 8: `mark_failed` — guard + latest-wins idempotency

**Description**: `UPDATE` adds `AND status IN ('running','failed','timed_out')`. Zero-row → `TerminalStateGuardError(run_id, current_status='completed')`. `mark_failed` on `'failed'` updates `error_class`, `error_message`, `failed_at` (latest wins). Truncate `error_message` at 4096 chars.

**Acceptance criteria**:
- [ ] `WHERE` has the three-status allow-set
- [ ] Zero-row on `'completed'` → `TerminalStateGuardError`
- [ ] Idempotent on `'failed'` → `'failed'`; error fields overwritten
- [ ] `error_message` truncated to 4096 characters

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_mark_failed.py -v`
- [ ] Two-call latest-wins test asserts field values after second call
- [ ] Covers AC-SA-05, AC-SA-06, AC-SA-25, AC-SA-26, AC-SA-27

**Dependencies**: #2, #3, #4
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_mark_failed.py` (new)
**Scope**: S
**Traces to**: FR-L-02, FR-L-06a, AC-SA-05/06/25/26/27

---

#### Task 9: `mark_bronze_complete` + `mark_silver_complete` guards

**Description**: Both methods get the same `AND status NOT IN ('failed','timed_out','landed_not_committed')` guard as `mark_complete`. Zero-row → `TerminalStateGuardError`. Signatures unchanged.

**Acceptance criteria**:
- [ ] Both methods carry the guard
- [ ] Zero-row → `TerminalStateGuardError`

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_mark_stage.py -v`
- [ ] Covers AC-SA-23, AC-SA-24 (stage-method branches)

**Dependencies**: #2, #3, #4
**Files touched**: `src/lhp/extensions/watermark_manager/_manager.py`, `tests/extensions/watermark_manager/test_mark_stage.py` (new)
**Scope**: S
**Traces to**: FR-L-06, AC-SA-23/24

---

### Checkpoint B — WatermarkManager Hardened

- [ ] `pytest tests/extensions/watermark_manager/` all green
- [ ] `pytest tests/test_jdbc_watermark_v2_integration.py` still passes
- [ ] NFR-L-05 mock assertion in place on every public method: `spark.sql` mock fails the test if called before `SQLInputValidator` raises on adversarial input (AC-SA-41)
- [ ] Human review: SQL strings in every method inspected for emitter coverage (AC-SA-13 grep check passes)

---

### Phase 3 — Template, `run_id` Provenance, Lint

#### Task 10: `derive_run_id` helper + widget override

**Description**: Implement `lhp.extensions.watermark_manager.runtime.derive_run_id(dbutils)` per L3 §4.2.4. Resolution order: widget `lhp_run_id_override` → Jobs context → `local-<uuid4>` + warning. Validate widget value via `SQLInputValidator.uuid_or_job_run_id`.

**Acceptance criteria**:
- [ ] Widget override path wins when set; emits warning
- [ ] Jobs context path returns `job-{jobRunId}-task-{taskRunId}-attempt-{N}`
- [ ] Fallback returns `local-<uuid4>`; emits warning
- [ ] Validator extended to accept `local-<uuid>` if not already

**Verification**:
- [ ] `pytest tests/extensions/watermark_manager/test_runtime.py -v`
- [ ] Three-branch tests with mocked `dbutils`; capture log output
- [ ] Covers AC-SA-32, AC-SA-33, AC-SA-34

**Dependencies**: #3
**Files touched**: `src/lhp/extensions/watermark_manager/runtime.py` (new), `src/lhp/extensions/watermark_manager/__init__.py` (re-export), `tests/extensions/watermark_manager/test_runtime.py` (new); possibly extend `sql_safety.uuid_or_job_run_id` to accept `local-<uuid>`
**Scope**: S
**Traces to**: FR-L-09, AC-SA-32/33/34

---

#### Task 11: Extraction template restructure (L2 §5.3 control flow)

**Description**: Rewrite `src/lhp/templates/load/jdbc_watermark_job.py.j2` to match L2 §5.3 verbatim: set UTC session timezone, call `derive_run_id`, `insert_new` OUTSIDE `try`, extraction inside `try`, `mark_failed` + re-raise in `except`, `mark_complete` in separate post-try block with `TerminalStateGuardError` propagation. Every Jinja expression inside a SQL lexical context wraps with an emitter.

**Acceptance criteria**:
- [ ] Rendered output has `mark_complete` as top-level statement after the `try/except`
- [ ] Rendered output wraps JDBC read + Parquet write + `agg` + `count` in one `try/except Exception`
- [ ] `except` calls `mark_failed(type(e).__name__, str(e)[:4096])` then `raise`
- [ ] Template imports `derive_run_id`; no `uuid.uuid4()` at top level
- [ ] Every SQL interpolation uses `sql_literal / sql_numeric_literal / sql_timestamp_literal / sql_identifier`
- [ ] Rendered output passes Black

**Verification**:
- [ ] `pytest tests/templates/test_jdbc_watermark_template.py -v`
- [ ] Render against a representative fixture; AST-walk the output
- [ ] `black --check` on rendered output
- [ ] Covers AC-SA-01, AC-SA-04, AC-SA-35; supports AC-SA-29/30 via generated code shape

**Dependencies**: #5, #7, #8, #10
**Files touched**: `src/lhp/templates/load/jdbc_watermark_job.py.j2`, `tests/templates/test_jdbc_watermark_template.py` (new)
**Scope**: M
**Traces to**: FR-L-01, FR-L-02, FR-L-09, FR-L-10, AC-SA-01/04/35

---

#### Task 12: White-box Jinja AST lint

**Description**: New test parses `jdbc_watermark_job.py.j2` with Jinja2's AST parser. Walks every `{{ Name }}` / `{{ Getattr }}`. Determines lexical SQL context via regex over surrounding template source (`\bINSERT\b | \bMERGE\b | \bUPDATE\b | \bSELECT\b | \bDELETE\b | \bWHERE\b`). Expression in SQL context must be wrapped in one of the four emitters. Bare expression → test fails with actionable message.

**Acceptance criteria**:
- [ ] Lint implemented in `tests/templates/test_template_sql_lint.py`
- [ ] Current post-Task-11 template passes
- [ ] Seeded failure (manually unwrap a single interpolation) is caught and reported with source location + actionable message

**Verification**:
- [ ] `pytest tests/templates/test_template_sql_lint.py -v`
- [ ] Run against a deliberately broken fixture copy → expect failure
- [ ] Covers AC-SA-14, NFR-L-05

**Dependencies**: #11
**Files touched**: `tests/templates/test_template_sql_lint.py` (new), possibly `tests/templates/fixtures/broken_template.j2` (new)
**Scope**: S
**Traces to**: NFR-L-05, AC-SA-14

---

### Checkpoint C — Template + Lint

- [ ] `pytest tests/templates/` all green
- [ ] `pytest tests/extensions/watermark_manager/` all green
- [ ] Integration test exercises end-to-end: render → Black → execute locally with DBR-compatible stub → asserted lifecycle
- [ ] Human review: read the rendered extraction notebook side-by-side with L2 §5.3 control-flow contract

---

### Phase 4 — Migration Script + Exit Benchmark

#### Task 13: `slice_a_backfill.py` migration script

**Description**: Standalone CLI at `src/lhp/extensions/migrations/slice_a_backfill.py`. Invokable via `python -m lhp.extensions.migrations.slice_a_backfill`. Implements bronze verification SQL from L3 §4.4 (1-second skew tolerance). Idempotent. `--assume-empty` short-circuits bronze check. `--dry-run` reports decisions without writing. `--bronze-max-view` optional override. Exit codes per L3 §4.4.

**Acceptance criteria**:
- [ ] Script exists and is importable as a module
- [ ] `--help` lists all flags + exit codes
- [ ] Idempotent: second run reports "no orphans found"
- [ ] Promotes verifiable orphans with `migration_note='slice-A-backfill-YYYYMMDD'`
- [ ] Leaves unverifiable orphans alone; logs warning with key
- [ ] `--assume-empty` bypasses bronze query
- [ ] Exit codes: 0 / 1 / 2 per L3 §4.4

**Verification**:
- [ ] `pytest tests/extensions/migrations/test_slice_a_backfill.py -v`
- [ ] Three-branch test matrix: verifiable orphan, unverifiable orphan, empty-table
- [ ] Covers AC-SA-36, AC-SA-37, AC-SA-38, AC-SA-39

**Dependencies**: #2, #3
**Files touched**: `src/lhp/extensions/migrations/__init__.py` (new), `src/lhp/extensions/migrations/slice_a_backfill.py` (new), `tests/extensions/migrations/__init__.py` (new), `tests/extensions/migrations/test_slice_a_backfill.py` (new)
**Scope**: M
**Traces to**: FR-L-M1, AC-SA-36..39

---

#### Task 14: Exit benchmark + report

**Description**: Run representative incremental extraction on a known-shape fixture table before and after Slice A. Publish `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/BENCHMARK.md` with pre/post timings, ≤ 5 % overhead verdict, and raw run logs path. Required by FR-L-10 exit criterion.

**Acceptance criteria**:
- [ ] `BENCHMARK.md` exists in slice-A directory
- [ ] Contains pre-Slice-A baseline, post-Slice-A timing, delta %, verdict
- [ ] Verdict ≤ 5 % overhead (if > 5 %, treat as blocker — escalate; do NOT ship)
- [ ] Includes NFR-L-03 observability-query spot-check output

**Verification**:
- [ ] Manual review of `BENCHMARK.md` against FR-L-10 exit criterion
- [ ] Covers AC-SA-40

**Dependencies**: #5, #6, #7, #8, #9, #11
**Files touched**: `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/BENCHMARK.md` (new), possibly `scripts/benchmark_slice_a.py` if reproducibility wanted
**Scope**: S
**Traces to**: FR-L-10, NFR-L-01, NFR-L-03, AC-SA-40

---

### Checkpoint D — Slice A Complete

- [ ] All 42 ACs from L3 §3 traced to at least one passing test or committed artefact
- [ ] `BENCHMARK.md` shows ≤ 5 % overhead
- [ ] Migration runbook drafted in `OPS.md` (or inline in PR description)
- [ ] Human review: end-to-end dry run against a `devtest` watermark table with orphan rows
- [ ] Waiver W-A-01 re-acknowledged: Slice A **does not** ship to production alone — Slice B must be scheduled + owned

---

## 4. Parallelization Map

| Task | Can start after | Parallel-safe with |
|---|---|---|
| #1 Package | — | — |
| #2 Exceptions | #1 | #4 |
| #3 SQL safety | #2 | #4 |
| #4 UTC helper | #1 | #2, #3 |
| #5 insert_new | #2, #3, #4 | #6, #7, #8, #9, #10 |
| #6 get_latest | #2, #3, #4 | #5, #7, #8, #9, #10 |
| #7 mark_complete | #2, #3, #4 | #5, #6, #8, #9, #10 |
| #8 mark_failed | #2, #3, #4 | #5, #6, #7, #9, #10 |
| #9 mark_stage | #2, #3, #4 | #5, #6, #7, #8, #10 |
| #10 derive_run_id | #3 | #5-#9 |
| #11 Template | #5, #7, #8, #10 | #13 |
| #12 AST lint | #11 | — |
| #13 Migration | #2, #3 | #11, #12 |
| #14 Benchmark | #5-#9, #11 | — |

**Coordination note**: tasks #5-#9 all edit `_manager.py`. Serialise merges (one PR at a time) to avoid conflicts, OR split method bodies into sub-files under `watermark_manager/methods/` in #1 (not proposed — adds indirection without clear win). Recommend sequential merge.

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| `MERGE WHEN NOT MATCHED BY TARGET` unsupported on local test Spark | High | Gate `__init__` on DBR ≥ 13.3 LTS; use DBR-matching test runner in CI; provide a no-op mock path for local unit tests |
| Two-thread concurrency test is flaky | Med | Use `concurrent.futures.ThreadPoolExecutor` + a barrier; loop 20× in CI; mark `@pytest.mark.flaky(reruns=2)` if residual flake |
| Template renders differently under Black version drift | Med | Pin Black version; assert on AST shape not string equality where possible |
| Migration script `bronze_max_watermark` query is slow on large tables | Med | Require operator to supply a pre-computed view (`--bronze-max-view`); fall back to direct scan with warning |
| `datetime` round-trip fails under session-timezone toggles in CI (single Spark session) | Med | Write a dedicated test that sets/restores `spark.sql.session.timeZone` around assertions; skip-if-no-Delta on local Spark |
| Retry loop sleeps make unit tests slow | Low | Monkeypatch `time.sleep` with a spy; assert sleep durations without actually sleeping |
| White-box AST lint misses a SQL context (false negative) | High (silent) | Seed a deliberate failure in a fixture file as part of Task 12; assert the lint catches it |

---

## 6. Open Questions

All L2 open questions resolved in L3 §7. No new open questions at plan level.

---

## 7. Verification Checklist (Pre-Implementation)

- [x] Every task has acceptance criteria
- [x] Every task has verification step
- [x] Dependencies identified + ordered
- [x] No task touches > 5 files (all in S/M range)
- [x] Checkpoints exist between phases
- [ ] Human reviewer approves this plan before Task #1 begins

---

## 8. Approval

- [x] Author (Claude): rev 1.0 drafted 2026-04-17
- [ ] Human reviewer: pending
- [ ] Task #1 **must not** begin until this plan is approved

---

## 9. Revision History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-17 | Initial draft. 14 tasks across 4 phases. Serial merge order for Phase 2 due to shared `_manager.py` surface. |
