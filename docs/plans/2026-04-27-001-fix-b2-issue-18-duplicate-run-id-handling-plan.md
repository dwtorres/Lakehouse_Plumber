---
title: "fix(b2): explicit DuplicateRunError handling in worker (issue #18)"
type: fix
status: active
date: 2026-04-27
origin: https://github.com/dwtorres/Lakehouse_Plumber/issues/18
---

# fix(b2): explicit DuplicateRunError handling in worker (issue #18)

## Overview

Resolve issue #18 P2 (ADV-008): when the B2 for_each worker raises `DuplicateRunError` from `wm.insert_new`, the operator-visible exception is unhelpful and a stale watermarks `completed` row can mask the abort, producing a silent false pass in `validate`.

Two-part fix:

1. **Worker** (`jdbc_watermark_job.py.j2`) — add a dedicated `except DuplicateRunError` block around `wm.insert_new` that performs a clean abort: it does not call `wm.mark_failed` (no row to transition → would itself raise `TerminalStateGuardError`), does not write `'failed'` to the manifest (the issue is run-id provenance, not extraction failure), logs the duplicate-run condition explicitly, and re-raises the original `DuplicateRunError` with chain preservation so the operator sees the true root cause.
2. **Validate** (`validate.py.j2`) — tighten the `final_status` projection so a stale watermarks row (matched by `worker_run_id`) cannot override a manifest-side non-completed state. Defense-in-depth against the silent-divergence pattern called out in the issue, independent of how a particular worker chose to update the manifest.

---

## Problem Frame

Issue #18 (ADV-008) describes two coupled defects in the B2 for_each worker error path:

1. **Operator visibility loss.** When `wm.insert_new` raises `DuplicateRunError` (re-run after partial commit, `__lhp_run_id_override` collision per #23, or a DAB redeploy that resets task counters such that `derive_run_id` reproduces a prior `run_id` already in `lhp_watermarks`), the exception escapes the worker. Per the issue's reproduction path, downstream handling can call `wm.mark_failed` on a `run_id` that has no `'running'` row, raising `TerminalStateGuardError` that overwrites `DuplicateRunError` on the call stack. Operator sees `LHP-WM-002` with no breadcrumb to the real cause (`LHP-WM-001`).
2. **Silent data divergence.** Today's worker leaves the manifest row in `'running'` state when `insert_new` raises (the FR-L-05 contract places `insert_new` outside the `try` block precisely so `mark_failed` is not called when no row was created). However, `validate.py.j2:143` does `coalesce(worker_status, manifest_status) AS final_status` over a `LEFT JOIN ... ON w.run_id = m.worker_run_id`. If the watermarks table retains a stale `'completed'` row from a prior run with the same `run_id`, the join hits it and `coalesce` picks `'completed'` from watermarks — masking the manifest's `'running'` state. Validate reports pass for an iteration that never actually ran extraction this batch.

The first defect is operator UX. The second is correctness.

The issue's first recommendation ("move `insert_new` inside the `try` block") would violate the FR-L-05 contract documented at `jdbc_watermark_job.py.j2:225-227` ("a DuplicateRunError here means no watermark row was created for this invocation, so mark_failed has nothing to transition and must not be called") and at the file header `:8-9`. The contract is load-bearing for the entire mark_failed → mark_complete state machine. This plan adopts the issue's second recommendation: a dedicated `except DuplicateRunError` clause around `insert_new` that handles the duplicate condition explicitly without changing FR-L-05's structural separation.

---

## Requirements Trace

- **R1.** When `wm.insert_new` raises `DuplicateRunError`, the worker must NOT call `wm.mark_failed` (preserves FR-L-05 contract) and must NOT mirror `'failed'` to the manifest (per issue recommendation: "the issue is run-id provenance, not extraction failure"). (Issue #18)
- **R2.** When `wm.insert_new` raises `DuplicateRunError`, the worker must log a structured phase event with the original exception class, error code (`LHP-WM-001`), `run_id`, `batch_id`, and `action_name` BEFORE re-raising, so the operator has a clean breadcrumb. (Issue #18)
- **R3.** The original `DuplicateRunError` must propagate out of the worker as the visible exception (no replacement by `TerminalStateGuardError` or any other class). If any wrapping is required by surrounding code, `raise ... from original_exc` must be used to preserve the cause chain. (Issue #18)
- **R4.** `validate.py.j2`'s `final_status` projection must NOT report `'completed'` for an iteration whose manifest row is in any non-`completed` state, even when a stale watermarks row matched on `worker_run_id` shows `'completed'`. (Issue #18 silent-data-divergence)
- **R5.** The fix must preserve the existing legacy (non-`for_each`) worker code path byte-for-byte; the dedicated `DuplicateRunError` handler is gated by `{% if execution_mode == "for_each" %}` only when the manifest interaction matters, but the bare `insert_new` → `try`/`except` shape (FR-L-05) stays identical across both modes. (Regression guard against `test_r4_raise_on_failure_block_verbatim_between_modes`-style invariants.)
- **R6.** Tests must enforce R1-R4 directly: a render-time test that the new `except DuplicateRunError` block is present in for_each mode worker output, a behavior-style test (mock or fake `WatermarkManager`) that verifies `mark_failed` is not called and the manifest is not mutated to `'failed'` when `insert_new` raises `DuplicateRunError`, and a validate-template test that demonstrates the silent-divergence scenario produces `final_status != 'completed'`.
- **R7.** `docs/errors_reference.rst` must have an entry for `LHP-WM-001 DuplicateRunError` covering the duplicate-run-id abort path: causes (run_id collision via override widget, redeploy reset, replay), validate-side behavior under R4 tightening, and operator resolution. The existing entry (if present) must be reviewed and extended; if absent it is added.

---

## Scope Boundaries

- Issue #23 P3 (`__lhp_run_id_override` widget collision with live `batch_id`) is the upstream root cause for one of the run_id collision paths. **Out of scope** — tracked separately. This plan handles the worker's response when a collision occurs, not the override widget's collision detection.
- Issue #20 P3 (validate 24h window) is a different validate.py.j2 tightening axis. **Out of scope.**
- Issues #19, #21, #22 P3 are unrelated B2 follow-ups. **Out of scope.**
- Migrating `LHP-WM-NNN` exceptions from `lhp_watermark` package's local exception hierarchy to LHP CLI-side `LHPError` is **out of scope.** The watermark package is vendored into the bundle and runs inside generated Databricks notebooks; its exception classes are deliberately decoupled from `src/lhp/utils/error_formatter.py`. Issue #18 does not require migration.
- Reworking the `coalesce(worker_status, manifest_status)` semantics across the entire validate query (e.g., adding a batch-time-window predicate to the watermarks join) is **out of scope.** R4 takes the minimal-tightening path: the projection becomes "manifest must be `completed` AND (worker is `completed` or unknown) for `final_status = completed`." Larger validate-side overhauls belong in a separate plan.
- Adding a new manifest `execution_status` value (e.g., `'aborted_duplicate_run_id'`) is **out of scope.** Per the issue recommendation ("a clean exit ... is more appropriate"), the manifest row is left in its claim-time `'running'` state on `DuplicateRunError`. Validate's R4 tightening then naturally surfaces this as a non-`completed` action — operator investigates via the explicit log breadcrumb from R2. Adding a new status value would expand the manifest schema and the manifest_status enum across validate, runbook, dashboards.

### Deferred to Follow-Up Work

- Issue #23 P3 (`__lhp_run_id_override` collision detection): separate issue, separate plan.
- Manifest schema extension with a dedicated abort status: requires new plan if operator UX with the R2 log breadcrumb proves insufficient in practice.

---

## Context & Research

### Relevant Code and Patterns

- **Worker `insert_new` site:** `src/lhp/templates/load/jdbc_watermark_job.py.j2:273-284`. Outside the `try` block at `:299` per FR-L-05 contract documented at `:225-227` and file header `:8-9`.
- **Worker `try`/`except` block:** `src/lhp/templates/load/jdbc_watermark_job.py.j2:299-386`. The Anomaly B failure-mirror (`:354-378`, gated by `{% if execution_mode == "for_each" %}`) and `wm.mark_failed` (`:380-385`) are inside the `except Exception as e` handler. `DuplicateRunError` raised at `:273` does NOT enter this block under current control flow — but the existence of the path matters for R3 (operator clarity) and R4 (silent divergence is real even without the path being entered).
- **`DuplicateRunError` definition:** `src/lhp_watermark/exceptions.py:117-132`. Error code `LHP-WM-001`, raised by `WatermarkManager.insert_new` at `src/lhp_watermark/watermark_manager.py:611` when MERGE matches an existing target row (zero `num_affected_rows`).
- **`TerminalStateGuardError` definition:** `src/lhp_watermark/exceptions.py:135-152`. Error code `LHP-WM-002`. The class the issue identifies as masking the root cause.
- **Validate query — silent-divergence site:** `src/lhp/templates/bundle/validate.py.j2:122-153` (count query) and `:184-215` (failure enumeration query). Both use the same `coalesce(w.worker_status, m.manifest_status) AS final_status` pattern with `LEFT JOIN ... ON w.run_id = m.worker_run_id`. R4 tightening must apply to both call sites to keep them in sync.
- **`_log_phase` helper:** `src/lhp/templates/load/jdbc_watermark_job.py.j2` (search `_log_phase`). Structured phase log for operator visibility. Existing call sites: `jdbc_read_start`, `jdbc_read_complete`, `landing_write_start`, `landing_write_complete`, `landing_empty_schema_fallback`, `finalization_prepare`, `finalization_commit_start`, `finalization_commit_complete`. New phase event for R2: `duplicate_run_id_abort`.
- **R4 mark_failed→raise byte-identical invariant:** `tests/test_jdbc_watermark_v2_for_each_worker.py` and the `test_r4_raise_on_failure_block_verbatim_between_modes` discipline established in PR #13 / PR #24. R5 must be enforceable via the same render-equivalence pattern.

### Institutional Learnings

- PR #25 (issues #14-#17) demonstrated the working pattern for B2 worker template fixes: structural code change + render test + behavior test + errors_reference.rst entry + parity-guard regression test. Reuse the same shape.
- Memory `feedback_lhp_b2_iteration_contract`: every per-action attribute the worker reads must be in `B2_ITERATION_KEYS`. Not directly relevant here (no new iteration keys), but the contract test pattern (`tests/test_b2_iteration_contract.py`) is a precedent for AST-walk regression guards.
- Memory `feedback_lhp_secret_regex_apostrophe`: stray apostrophes in template comments swallow `__SECRET_*__` placeholders in `src/lhp/templates/**/*.j2`. The new comment block in U1 must be apostrophe-free.

### External References

External research skipped per Phase 1.2 — issue is well-bounded, three direct local patterns (existing `_log_phase` instrumentation, FR-L-05 contract structure, validate.py.j2 coalesce projection) cover the implementation surface.

---

## Key Technical Decisions

- **Adopt issue recommendation #2, reject recommendation #1.** Recommendation #1 ("move `insert_new` inside the try block") would violate FR-L-05 and re-couple `mark_failed` to a non-existent row. Recommendation #2 (dedicated `except DuplicateRunError`) cleanly preserves FR-L-05 while delivering R1-R3.
- **Clean exit, not new manifest status.** On `DuplicateRunError` the manifest row stays in its claim-time `'running'` state. Validate's R4 tightening surfaces this as `final_status = 'running'` → non-completed → batch fails. Operator resolves via the R2 log breadcrumb. Avoids manifest schema growth.
- **R4 form: tighten projection, do not add a JOIN predicate.** A natural alternative for R4 would be filtering the watermarks `LEFT JOIN` to rows from this batch (e.g., `AND w.created_at >= batch_start`). That requires plumbing a batch-start timestamp through `prepare_manifest` taskValues into validate. Heavy. Instead, change `final_status` projection from `coalesce(w.worker_status, m.manifest_status)` to a CASE that requires `m.manifest_status = 'completed' AND (w.worker_status IS NULL OR w.worker_status = 'completed')` for the `'completed'` bucket. Stale watermarks `'completed'` row no longer overrides manifest `'running'`. Self-contained inside the SQL string.
- **Re-raise with bare `raise`, not `raise DuplicateRunError(...) from original_exc`.** The original exception is already `DuplicateRunError`; wrapping it adds noise. Preserve the original frame.

---

## Open Questions

### Resolved During Planning

- **Should U1 also touch the manifest claim row on `DuplicateRunError`?** No. The claim UPDATE at `:248-251` already set `worker_run_id` and `execution_status='running'`. Reverting it on `DuplicateRunError` would race other workers and add complexity. Leave at `'running'`; let R4 + the R2 log breadcrumb cover operator visibility.
- **Should the `except DuplicateRunError` block also exist in legacy (non-`for_each`) mode?** Yes for R1-R3 (the operator visibility concern is mode-agnostic), but legacy mode has no manifest interaction so the manifest-mirror guard is a no-op. The block can be unconditional; only the manifest-related comment text changes per mode. R5 (byte-identical mark_failed→raise) is unaffected because the new block is BEFORE the `try`, not inside the `except` slice that R5 protects.

### Deferred to Implementation

- Exact `_log_phase` event name: tentatively `duplicate_run_id_abort`. Final name reconciled at implementation time against any logging conventions surfaced during the test pass.
- Whether the validate test for R4 uses an in-memory Spark fixture or an SQL string assertion. Either is acceptable; pick whichever matches the existing test style in `tests/templates/test_jdbc_watermark_template.py` and `tests/test_jdbc_watermark_aux_for_each.py`.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

**Worker control flow change (jdbc_watermark_job.py.j2 around line 273):**

```
# Today:
wm.insert_new(...)            # raises DuplicateRunError → propagates uncaught
extraction_succeeded = False
...
try:                          # JDBC read + landing write
    ...
except Exception as e:
    [for_each] Anomaly B fail-mirror UPDATE → manifest 'failed'
    wm.mark_failed(...)       # raises TerminalStateGuardError if no 'running' row
    raise

# Proposed:
try:
    wm.insert_new(...)
except DuplicateRunError:
    _log_phase("duplicate_run_id_abort", error_code="LHP-WM-001",
               run_id=run_id, batch_id=batch_id, action_name=action_name)
    # Do NOT call wm.mark_failed (FR-L-05: no row exists)
    # Do NOT mirror manifest to 'failed' (run-id provenance, not extraction failure)
    # Manifest row remains in claim-time 'running' state — validate R4 surfaces it
    raise

extraction_succeeded = False
...
try:                          # unchanged
    ...
except Exception as e:        # unchanged — DuplicateRunError no longer reaches here
    ...
```

**Validate projection change (validate.py.j2 lines 143 and 206):**

```
# Today:
coalesce(w.worker_status, m.manifest_status) AS final_status

# Proposed:
CASE
  WHEN m.manifest_status = 'completed'
       AND (w.worker_status IS NULL OR w.worker_status = 'completed')
    THEN 'completed'
  WHEN m.manifest_status = 'failed' THEN 'failed'
  ELSE coalesce(w.worker_status, m.manifest_status)
END AS final_status
```

The CASE preserves existing semantics for the common path (manifest+worker both completed → completed; manifest failed → failed) and tightens the silent-divergence corner: stale watermarks `'completed'` over manifest `'running'` now reports `'running'`, which the existing failure enumeration block at `:183` flags.

---

## Implementation Units

- [ ] U1. **Worker `except DuplicateRunError` clause around `wm.insert_new`**

**Goal:** Wrap `wm.insert_new` in a dedicated `except DuplicateRunError` block that emits a structured log event, performs no manifest mutation and no `mark_failed` call, and re-raises the original exception. Preserve FR-L-05 (`insert_new` outside the JDBC-read `try` block).

**Requirements:** R1, R2, R3, R5

**Dependencies:** None.

**Files:**
- Modify: `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- Modify: `tests/templates/test_jdbc_watermark_template.py` (render assertions)
- Test: new behavior test (see U3)

**Approach:**
- Wrap the `wm.insert_new(...)` call at `:273-284` in a `try` / `except DuplicateRunError` shape. Only `DuplicateRunError` is caught; any other exception (including `WatermarkValidationError`, `WatermarkConcurrencyError`, generic `Exception`) propagates unchanged.
- The `except` body calls `_log_phase("duplicate_run_id_abort", error_code="LHP-WM-001", run_id=run_id, batch_id=batch_id, action_name=action_name)`. In legacy (non-for_each) mode `batch_id` and `action_name` are undefined; the log call must be gated by `{% if execution_mode == "for_each" %}` for those keys, falling back to `_log_phase("duplicate_run_id_abort", error_code="LHP-WM-001", run_id=run_id)` in legacy mode.
- The `except` body ends with bare `raise` (no `from`) to preserve the original `DuplicateRunError` frame.
- Update the FR-L-05 comment block at `:225-227` to document the new explicit handler ("`DuplicateRunError` from `insert_new` is now caught explicitly to log a clean breadcrumb; the contract that `mark_failed` is not called still holds because the handler does not call it").
- The new code MUST sit BEFORE the `try` at `:299`. R5 (byte-identical mark_failed→raise slice) is unchanged because the `except Exception as e` block at `:353-386` is untouched.
- Apostrophe-free comments (memory `feedback_lhp_secret_regex_apostrophe`).

**Patterns to follow:**
- Existing `_log_phase` call sites in `jdbc_watermark_job.py.j2` for keyword argument shape.
- FR-L-05 contract comment style at `:225-227`.

**Test scenarios:**
- Happy path: Render the for_each worker template; assert the rendered notebook source contains `except DuplicateRunError:` exactly once and that it appears between the `insert_new(` call site and the `try:` at the JDBC-read block. Prefix: **Happy path**.
- Happy path: Render the legacy (non-for_each) worker template; assert the same `except DuplicateRunError:` block is present. The legacy log line must NOT reference `batch_id` or `action_name` (those are for_each-only iteration keys). Prefix: **Happy path**.
- Edge case: Assert the rendered for_each notebook still contains the FR-L-05 marker comment (`FR-L-05` substring) and the `wm.mark_failed(` call site at the existing `except Exception as e` block. Prefix: **Edge case** (R5 byte-identical guard regression).
- Integration: After rendering both modes, do a `difflib`-style comparison of the `except Exception as e:` slice (line containing `except Exception as e:` through the line containing the bare `raise` at `:386`) and assert byte-identical equality between for_each and legacy renders for that slice. Prefix: **Integration** (mirrors `test_r4_raise_on_failure_block_verbatim_between_modes`).

**Verification:**
- For_each render contains the new `except DuplicateRunError` block at the correct position.
- Legacy render contains the new `except DuplicateRunError` block, without iteration-keyed log fields.
- `except Exception as e` slice (R4 mark_failed→raise) is byte-identical across modes.
- No new pytest failures.

---

- [ ] U2. **Validate `final_status` projection tightening**

**Goal:** Replace `coalesce(w.worker_status, m.manifest_status)` in both validate query call sites with a CASE expression that requires manifest-side `'completed'` for the `'completed'` bucket. Stale watermarks rows can no longer mask manifest non-progress.

**Requirements:** R4

**Dependencies:** None (independent of U1 — even if U1 is reverted, R4 still hardens validate against the silent-divergence class).

**Files:**
- Modify: `src/lhp/templates/bundle/validate.py.j2`
- Test: new validate-template test (see U3)

**Approach:**
- Change `:143` and `:206` to the CASE form sketched in High-Level Technical Design.
- The CASE preserves these existing buckets: manifest `completed` AND worker `completed`/null → `completed`; manifest `failed` → `failed`; everything else → existing coalesce fallback (covers worker-side intermediate statuses like `running`, `landed_not_committed`, `abandoned` that never get written to b2_manifests but are valid worker_status values per validate.py.j2:179-181).
- Update the comment block at `:117-121` to document the tightened semantics and reference issue #18.

**Patterns to follow:**
- Existing CTE structure in validate.py.j2; do not restructure the CTEs, only the `final_status` projection.
- Comment style (CTE-block-level comments with R-IDs).

**Test scenarios:**
- Happy path: manifest `completed` + watermarks `completed` → `final_status = 'completed'`. Both queries pass. Prefix: **Happy path**.
- Edge case: manifest `running` + watermarks `completed` (the silent-divergence scenario) → `final_status = 'running'` (NOT `'completed'`). Failure enumeration query lists the action. Prefix: **Edge case** (R4 silent-divergence guard).
- Edge case: manifest `failed` + watermarks NULL (no worker run) → `final_status = 'failed'`. Prefix: **Edge case**.
- Edge case: manifest `running` + watermarks NULL (claim-only, no worker output) → `final_status = 'running'`. Batch fails. Prefix: **Edge case**.
- Edge case: manifest `completed` + watermarks `running` (worker started post-completion mirror, unusual race) → existing fallback path; assert behavior matches today (this is not the bug being fixed; just guard it). Prefix: **Edge case**.

**Verification:**
- Both validate-query call sites use the CASE form; comment references issue #18 and R4.
- Silent-divergence test fails on the pre-fix template and passes on the post-fix template.

---

- [ ] U3. **Behavior + render tests for U1 and U2**

**Goal:** Test files for the new behavior; ensures R6 is durable.

**Requirements:** R6

**Dependencies:** U1, U2

**Files:**
- Modify: `tests/templates/test_jdbc_watermark_template.py` (add render assertions for U1)
- Modify or Create: `tests/test_jdbc_watermark_v2_for_each_worker.py` (add a behavior test that simulates `DuplicateRunError` from a fake `WatermarkManager.insert_new` and asserts no `mark_failed` call, no manifest-side `UPDATE ... SET execution_status = 'failed'`, and the original `DuplicateRunError` propagates)
- Modify: `tests/templates/test_template_sql_lint.py` if it lints validate.py.j2 SQL — extend to accept the CASE form
- Create: a new validate-template test file or extend an existing one for the U2 silent-divergence scenarios — pattern after `tests/test_jdbc_watermark_aux_for_each.py` if it covers validate, otherwise the closest existing validate test

**Approach:**
- The behavior test for U1 uses the existing fake/mocked `WatermarkManager` pattern from `test_jdbc_watermark_v2_for_each_worker.py` (or its closest sibling). Drive `insert_new` to raise `DuplicateRunError(run_id="job-1-task-2-attempt-3")`, catch the propagated exception, assert `type(caught) is DuplicateRunError`, assert the fake recorded zero `mark_failed` calls, and assert no SQL UPDATE matching `execution_status = 'failed'` was executed against the manifest table fake.
- Render tests for U1: see U1 test scenarios above.
- Validate tests for U2: see U2 test scenarios above. Use either an in-memory Spark session (matching whatever `tests/test_jdbc_watermark_aux_for_each.py` does for validate-side coverage) or a string-extraction approach that pulls the rendered SQL and asserts the CASE substring. Prefer behavior-style if the existing test infra supports it.

**Patterns to follow:**
- `tests/test_jdbc_watermark_v2_for_each_worker.py` for the fake-WatermarkManager pattern.
- `tests/templates/test_jdbc_watermark_template.py` for render assertions.

**Test scenarios:**
- Happy path: `insert_new` raises `DuplicateRunError` → no `mark_failed` invocation; original exception propagates; phase log includes `duplicate_run_id_abort` and `error_code="LHP-WM-001"`. Prefix: **Happy path**.
- Edge case: `insert_new` raises `WatermarkValidationError` (NOT `DuplicateRunError`) → falls through the new handler unchanged; existing FR-L-05 propagation behavior preserved. Prefix: **Edge case** (specificity of `except DuplicateRunError`).
- Edge case: `insert_new` succeeds (no exception) → JDBC-read `try` runs as today; no impact. Prefix: **Edge case** (no-op for happy path).
- Integration: U2 silent-divergence scenarios from U2 test list — covered there, not duplicated here.

**Verification:**
- All new tests pass on the post-fix branch.
- All new tests fail on the pre-fix branch (each test asserts the specific bug it guards against).
- `pytest tests/templates/ tests/test_jdbc_watermark_v2_for_each_worker.py` green.

---

- [ ] U4. **Errors reference + B2 anomaly registry update**

**Goal:** Document `LHP-WM-001 DuplicateRunError` in `docs/errors_reference.rst` covering the duplicate-run-id abort path, validate behavior under R4, and operator resolution. Note the U1+U2 fix in any standing B2 anomaly registry / runbook.

**Requirements:** R7

**Dependencies:** U1, U2

**Files:**
- Modify: `docs/errors_reference.rst`
- Modify: B2 runbook(s) — locate via `rg -l "Anomaly B" docs/`. Likely candidates: `docs/runbooks/b2_for_each_rollout.md` or similar.

**Approach:**
- `errors_reference.rst`: if `LHP-WM-001` already has an entry from earlier work, extend the "Common conditions" and "Operator resolution" sections to reference issue #18, the duplicate-run-id abort breadcrumb (`_log_phase` event `duplicate_run_id_abort`), and the validate-side surfacing as `final_status = 'running'`. If absent, add the full entry following the existing `LHP-MAN-*` and `LHP-VAL-*` entry shape from PR #25.
- Runbook: append an "Anomaly B addendum" sub-section noting the U1 explicit handler change and the U2 validate tightening, with a one-paragraph operator runbook ("if a batch reports `unfinished_n > 0` with `final_status = 'running'`, search worker logs for `duplicate_run_id_abort` — the iteration's `run_id` collided; investigate `__lhp_run_id_override` widget state and watermarks-table replay history").

**Test scenarios:**
- Test expectation: none — pure documentation. The U1+U2 implementation tests cover the behavior; this unit only updates docs.

**Verification:**
- `docs/errors_reference.rst` builds without warnings (`make -C docs html` or equivalent if the project has Sphinx wired up — otherwise manual visual check).
- Runbook addendum lands in a plausible location with a one-sentence link from the existing Anomaly B section.

---

## System-Wide Impact

- **Interaction graph:** The U1 change is local to `jdbc_watermark_job.py.j2`; no callbacks or middleware touched. The U2 change is local to the validate.py.j2 SQL; the bundle generator that renders both files is unchanged. The `prepare_manifest.py.j2` ↔ `validate.py.j2` taskValue contract (`batch_id`) is untouched.
- **Error propagation:** `DuplicateRunError` now propagates as the raw `LHP-WM-001` it always was, instead of potentially being replaced by `LHP-WM-002` (`TerminalStateGuardError`). DAB task-level retries still fire on the `DuplicateRunError` because the worker still re-raises — issue #14's status-based reclaim already handles the per-iteration retry case correctly.
- **State lifecycle risks:** When `DuplicateRunError` aborts the worker, the manifest row stays in `'running'`. This is intentional. Validate's R4 tightening surfaces this as a non-completed action so the batch fails loudly. Operator must intervene; manifest cleanup of orphan `'running'` rows is the existing operational pattern (already documented in B2 runbook for cancelled-batch ghost rows). No new cleanup mechanism needed.
- **API surface parity:** `LHP-WM-001` is a runtime exception inside generated notebooks; no public API change. Validate's SQL projection changes its CASE form but the columns and semantics for the operator-visible `_summary` dict (`expected`, `completed_n`, `failed_n`, `unfinished_n`) are unchanged for the common path. Only the silent-divergence corner shifts from false-pass to correct-fail.
- **Integration coverage:** U3's behavior test covers the `insert_new` → `except DuplicateRunError` → re-raise integration. U3's validate-template test covers the `manifest 'running' + watermarks 'completed'` join+projection integration that unit-level coalesce tests would miss.
- **Unchanged invariants:** FR-L-05 (`insert_new` outside the JDBC-read `try` block; `mark_failed` never called when no row was created) is explicitly preserved by U1's design. The R4 mark_failed→raise byte-identical-between-modes invariant is preserved by U1's structural placement (new handler is BEFORE the JDBC-read `try`, not inside the existing `except Exception as e`). The `B2_ITERATION_KEYS` 10-key contract is untouched. The `prepare_manifest` taskValue 48 KB ceiling guards from PR #25 are untouched.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| U1's new `except DuplicateRunError` block accidentally extends the byte-identical mark_failed→raise slice and breaks `test_r4_raise_on_failure_block_verbatim_between_modes`. | U1 places the new block strictly BEFORE the JDBC-read `try`. The R5 invariant test asserts byte-identical equality of the `except Exception as e` slice — U1 does not touch that slice. Verified by U3's regression test (last U1 test scenario). |
| U2's CASE form changes the semantics for an unanticipated worker_status value. | U2 explicitly preserves the `coalesce` fallback in the `ELSE` branch, so any worker_status value not in `{NULL, 'completed'}` falls through to today's behavior. The only new behavior is for `manifest='running' + worker='completed'` which is exactly the silent-divergence case being fixed. |
| U2 breaks an existing validate test that depends on the old coalesce string literal in the rendered SQL. | Existing tests assert behavior (counts, statuses) not literal SQL string. If any string-match tests exist, U3 updates them. Mitigated by the standard test pass before merge. |
| The `_log_phase` event name `duplicate_run_id_abort` collides with an existing event name. | Searched the template at planning time; the existing event names are listed in Context section and do not include this. Final name reconciled at U1 implementation. |

---

## Documentation / Operational Notes

- U4 covers the `errors_reference.rst` and B2 runbook updates. No additional operational changes required.
- Devtest validation: after U1+U2 land, devtest replay of a known-duplicate `run_id` scenario (e.g., set `__lhp_run_id_override` to a value already in `lhp_watermarks`) should produce: (1) `DuplicateRunError` in the worker task log with `_log_phase` `duplicate_run_id_abort` breadcrumb, (2) validate batch exits non-zero with `final_status = 'running'` for the affected action, (3) no spurious `'failed'` row in `b2_manifests`. Tracked as a manual operator-verification step in the PR test plan; not a code-change task in this plan.

---

## Sources & References

- **Origin issue:** https://github.com/dwtorres/Lakehouse_Plumber/issues/18 (P2, ADV-008, anchor 75)
- **Related issues:** #14 (DAB per-iteration retries — fixed in PR #25), #15-#17 (PR #25), #23 (`__lhp_run_id_override` widget collision — separate plan), #20 (validate 24h window — separate plan)
- **Related PR:** #25 (`fix(b2): P0/P1 deferred follow-ups`) — established the worker-template fix pattern and errors_reference.rst conventions reused here
- Related code: `src/lhp/templates/load/jdbc_watermark_job.py.j2`, `src/lhp/templates/bundle/validate.py.j2`, `src/lhp_watermark/exceptions.py`, `src/lhp_watermark/watermark_manager.py`
- Related tests: `tests/templates/test_jdbc_watermark_template.py`, `tests/test_jdbc_watermark_v2_for_each_worker.py`, `tests/lhp_watermark/test_mark_failed.py`, `tests/lhp_watermark/test_insert_new.py`
