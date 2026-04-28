---
title: "fix(b2): P3 backlog from 2026-04-26 review run (issues #19-#23)"
type: fix
status: active
date: 2026-04-27
deepened: 2026-04-27
origin: null
---

# fix(b2): P3 backlog from 2026-04-26 review run (issues #19-#23)

## Overview

Resolve the five P3 issues filed from the same `ce-code-review` run
(`20260426-115040-b1c3456d`) that produced #18 (P2, already merged). All
five items target the B2 `execution_mode: for_each` topology and the
shared aux templates (`prepare_manifest.py.j2`, `validate.py.j2`,
`watermark_manager.py`). They are author-triage hardening items: small
defensive fixes, observability gaps, and documented-design notes — not
behavior changes that affect a current production pipeline.

- **#19** — `b2_manifests` DDL omits three `B2_ITERATION_KEYS` columns; observability gap, undocumented
- **#20** — `validate` 24h window concern; audit post-#19 (`batch_id` via taskValue) for any remaining paths
- **#21** — Empty `actions` list in `prepare_manifest` produces malformed `MERGE` SQL; defensive hardening
- **#22** — Concurrent `ALTER TABLE CLUSTER BY` in `watermark_manager` unprotected; fleet-deploy race
- **#23** — `__lhp_run_id_override` widget can collide with live `batch_id`; silent extraction skip

This plan mirrors the bundling pattern from PR #25 (P0/P1 followups
#14-17): one cohesive PR closing all five issues, sequenced for review.

---

## Problem Frame

Five issues filed by the adversarial + api-contract reviewers on the
2026-04-26 walk-through of the watermark scale-out. Each was triaged P3
because the trigger is narrow (deliberate operator action, fleet-deploy
race, contributor refactor exposure) but the *consequence* is high
(silent data skip, opaque error, observability gap). PR #24 / PR #25 /
PR #26 / PR #27 closed all P0–P2 items. The remaining five form this
plan's scope.

The shared theme: **safety margins around inputs that the runtime trusts
without verification** — an empty action list, an operator-supplied
`run_id`, a partial deploy mid-flight, an unbounded validate scan
window, and an audit-time join that requires off-table joins. None are
broken today; each one is one refactor / one operator-error / one
deploy-race away from a silent failure mode.

---

## Requirements Trace

- **R1.** `b2_manifests` DDL omission of `jdbc_table`, `watermark_column`, `landing_path` is captured as an explicit design note in template + runbook + `errors_reference.rst` so post-run audits know to join `watermarks`. (Issue #19)
- **R2.** No 24-hour-window code path remains in `validate.py.j2` after the `batch_id`-via-`taskValue` fix from PR #25 (#19 in that plan's numbering). (Issue #20 audit step)
- **R3.** If any `INTERVAL 24 HOURS` window code path is found surviving in `validate.py.j2`, it is removed and replaced by the existing `WHERE batch_id = …` filter, **or** made configurable via codegen context with documented rationale. (Issue #20 conditional)
- **R4.** `prepare_manifest.py.j2` rendered output raises a clear error (not a Spark `ParseException`) when `actions` is empty. (Issue #21)
- **R5.** `watermark_manager._alter_clustering_to_target` and `_add_load_group_column` survive a fleet-deploy race against `ConcurrentAppendException` by retrying with backoff. (Issue #22)
- **R6.** `prepare_manifest.py.j2` rejects an operator-supplied `__lhp_run_id_override` whose derived `batch_id` collides with a non-`pending` row in `b2_manifests`, before any MERGE state mutation. (Issue #23)
- **R7.** Operator-supplied override widget values are validated against a strict pattern (`local-<uuid4>` or `job-{int}-task-{int}-attempt-{int}`); malformed values are rejected early with a clear error. (Issue #23 secondary — already enforced by `SQLInputValidator.uuid_or_job_run_id`; this plan adds a regression test, not new validator code.)
- **R8.** B2 rollout runbook (`docs/runbooks/b2-for-each-rollout.md`) and `docs/errors_reference.rst` reflect every new error code introduced by R3-R7 with cause / common conditions / resolution / example. (Issues #19, #22, #23 docs)

---

## Scope Boundaries

- The MERGE `WHEN MATCHED` semantics in `prepare_manifest.py.j2` (#23 Option B) are **not** changed. R6 implements Option A (proactive `SELECT COUNT(1)` rejection at the top of `prepare_manifest`) — the issue body presents both options as alternatives, not additive. Option A surfaces the collision before any DML and produces a clear, single-source error message; Option B would still require Option A's `SELECT` to know whether to fail, so layering both adds no defense in depth. If a future operator workflow requires Option B (e.g., a rerun policy that distinguishes "completed" from "in-flight" collisions), it is filed as a follow-up.
- Adding the three omitted columns to the `b2_manifests` DDL (#19 Option B) is **not** in scope. R1 implements Option A (document the omission). The values flow through `taskValues` and are stored in `watermarks` already; auditors join the two tables. Adding the columns means an `ALTER TABLE … ADD COLUMNS` migration on every existing devtest/qa/prod manifest table — non-trivial blast radius for an observability gap.
- New B2 features (per-flowgroup workflow YAML for multi-`for_each`, parity check implementation, manifest cleanup for ghost rows from cancelled batches) remain out of scope per PR #25's plan.
- Devtest e2e replay of any rendered template change is gated on the PR test plan checklist, not duplicated here.

### Deferred to Follow-Up Work

- **#19 Option B** (DDL columns + MERGE projection): deferred unless an explicit auditor request lands. File a new issue if so.
- **#23 Option B** (`MERGE WHEN MATCHED` rejection): deferred unless Option A's prepare-time check proves insufficient (e.g., a non-DAB caller bypasses `prepare_manifest`).

---

## Context & Research

### Relevant Code and Patterns

- **#19 — DDL block:** `src/lhp/templates/bundle/prepare_manifest.py.j2:87-112` (10-column CREATE TABLE)
- **#19 — Iteration contract:** `src/lhp/models/b2_iteration.py:34-52` (10-key `B2_ITERATION_KEYS`)
- **#20 — `validate` query:** `src/lhp/templates/bundle/validate.py.j2:127-174` and `:206-249`. Both queries currently use `WHERE batch_id = {sql_literal(batch_id)}` only — *no* `INTERVAL 24 HOURS` window. **Audit performed during planning (2026-04-27): `rg -n "INTERVAL.*HOURS|created_at.*>" src/lhp/ tests/` returned zero matches in `validate.py.j2` (the only hit is `prepare_manifest.py.j2:174-177`, the scope-distinct retention DELETE). Audit is complete; U2 reduces to a regression-prevention template comment.**
- **#20 — `prepare_manifest` retention DELETE:** `src/lhp/templates/bundle/prepare_manifest.py.j2:174-177` uses `INTERVAL 30 DAYS` — this is the manifest **retention** path, intentional, scope-distinct from the `validate` window. R2 is bounded to `validate.py.j2`.
- **#21 — MERGE block:** `src/lhp/templates/bundle/prepare_manifest.py.j2:131-222`. Loop driver: `_manifest_rows = [...]` for-loop at `:131-141`; `_values_fragment` join at `:144-154`; MERGE at `:199-222`. Empty `actions` produces an empty `_values_fragment` and the rendered `FROM VALUES\n    AS s(...)` triggers Spark `ParseException`.
- **#21 — Reachability gates:** `_emit_b2_aux_files` is called at `src/lhp/generators/load/jdbc_watermark_job.py:372`, gated on at least one `jdbc_watermark_v2` LOAD action. Validator `LHP-CFG-033` in `src/lhp/core/validator.py:362-363` rejects `action_count == 0`. Path is unreachable today; this is a defensive belt-and-braces.
- **#22 — ALTER paths:** `src/lhp_watermark/watermark_manager.py:309-329` (`_alter_clustering_to_target`) and `:290-307` (`_add_load_group_column`). Both wrap a single `spark.sql(ALTER ...)` and surface `_is_concurrent_commit_exception` as `WatermarkConcurrencyError`. The retry helper `execute_with_concurrent_commit_retry` is already imported and used for `MERGE` paths.
- **#23 — Override + derive_run_id:** `src/lhp_watermark/runtime.py:31` (widget name is `lhp_run_id_override`, *not* `__lhp_run_id_override` as written in the issue body — the double-underscore form is the DAB internal task widget convention but the actual widget name in `runtime.py` has no leading underscores; verify before referencing in error messages and runbook).
- **#23 — Pattern for prepare-time check:** `src/lhp/templates/bundle/prepare_manifest.py.j2:120` — `batch_id = derive_run_id(dbutils)` runs *after* DDL but *before* MERGE. The collision-rejection `SELECT COUNT(1)` slots between `batch_id` derivation (`:120`) and the MERGE block (`:199`).

### Institutional Learnings

- **PR #25 plan, U6 pattern (regression test for parity):** every error code raised in B2 paths must have an `errors_reference.rst` entry; AST parity test enforces it. New codes (R3 conditional, R4, R5, R6, R7) inherit this contract — `tests/test_b2_iteration_contract.py`-style guard already covers it via `tests/test_b2_error_code_catalog_parity.py` (PR #25 U6).
- **Devtest-replay rule (memory `feedback_lhp_b2_iteration_contract`):** every per-action attribute the worker reads must be in `B2_ITERATION_KEYS` and emitted by `prepare_manifest`. R1 explicitly does *not* widen the contract; it documents that three keys flow via taskValues only.
- **Secret regex apostrophe rule (memory `feedback_lhp_secret_regex_apostrophe`):** any error message embedded in `*.j2` templates must avoid possessives (e.g., write `the operator must` not `operator's must`). Applies to R4 and R6 raised messages.

### External References

- Delta `ALTER TABLE … CLUSTER BY` retry semantics: same `ConcurrentAppendException` class as DML; the helper at `src/lhp_watermark/watermark_manager.py` `execute_with_concurrent_commit_retry` already handles this exception class. No new external research needed.

---

## Key Technical Decisions

- **#19 implements Option A (document) over Option B (add columns):** Option A is the deliberate-design path. The values are already stored in `watermarks` (joinable on `worker_run_id`) and audit tools that need them already join. Option B's blast radius (ALTER on every existing manifest table across devtest/qa/prod) outweighs its forensic value for what is *already* an explicit design. Documentation closes the issue cleanly. *Rationale: smallest change that makes the design intentional and discoverable.*
- **#20 implementation is audit-first:** Phase 1 grep-and-render verification confirms whether any 24h window code path survives in the rendered `validate.py.j2`. Expected result: zero remaining paths (PR #25 U7 replaced the window with `WHERE batch_id = …`). If confirmed, U2 closes #20 with a one-line comment in the template + runbook note. If a path is found, U2 escalates to remove it (preferred) or convert it to a configurable `validate_window_hours` codegen-context value with a default of `168` (7 days). *Rationale: do not invent a configuration knob when the audit may show none is needed.*
- **#21 implements the Jinja `{% if actions %}` guard:** Template-level guard surfaces the empty-list case as a Python `RuntimeError` with a clear LHP error code (`LHP-MAN-006: prepare_manifest rendered with empty actions list`) instead of a Spark `ParseException`. The guard wraps the `_manifest_rows` list comprehension and the MERGE block. *Rationale: catches both the contributor-bypass case (a future generator calling `_emit_b2_aux_files` directly) and the per-action-filter refactor case (a future `include_tests=False` filter shrinking the list to zero) at the lowest layer possible.*
- **#22 implements Option A (retry with backoff) on both `_alter_clustering_to_target` and `_add_load_group_column`:** The retry wrapper `execute_with_concurrent_commit_retry` already handles `ConcurrentAppendException`. DDL is slower than DML, so the budget is tuned wider (`budget=10`, `base_secs=0.5`). Both ALTER paths share the race; both get the wrap. *Rationale: TOCTOU between the `_clustering_matches_target` probe and the `ALTER` cannot be made atomic in Delta, so retry-with-backoff is the practical mitigation. Same for the `_has_load_group_column` probe + `ADD COLUMNS` pair.*
- **#23 implements Option A (prepare-time collision rejection) over Option B (MERGE WHEN MATCHED rejection):** Option A surfaces the collision before any DML state mutation, with a clean error message and operator instructions. Option B would still need Option A's `SELECT` (the MERGE WHEN MATCHED clause cannot itself raise a clear error, only update or no-op), so layering adds no defense in depth. *Rationale: single-source rejection at the earliest visible point.*
- **#23 also covers widget-value validation (R7):** `derive_run_id` returns either `local-<uuid>` or `job-{int}-task-{int}-attempt-{int}`. The override path validates against `SQLInputValidator.uuid_or_job_run_id` already (`runtime.py:50`). **Verified during planning (2026-04-27): `src/lhp_watermark/sql_safety.py:39` already enforces `_JOB_RUN_ID = re.compile(r"^job-\d+-task-\d+-attempt-\d+$")` and `:35-37` enforces `_UUID_V4` and `_LOCAL_UUID` strictly.** R7 is therefore *verify-only* — no validator code change needed. U5 adds a regression test confirming malformed forms (e.g., `job-foo-task-bar-attempt-baz`, `not-a-valid-form`, `''`) are rejected, but no production code in `sql_safety.py` or `runtime.py` is modified. *Rationale: regex is already strict; the work is documenting + locking the contract via test, not tightening a permissive validator.*

---

## Open Questions

### Resolved During Planning

- **Should #19 add the columns to DDL?** No — Option A (document). See Key Technical Decisions.
- **Should #20 invent a `validate_window_hours` knob preemptively?** No — audit first; only add if a path survives.
- **Should #23 do Option A, B, or both?** Option A only. See Key Technical Decisions.
- **Does `_emit_b2_aux_files` ever get called with empty actions today?** No — gated on a `jdbc_watermark_v2` LOAD action existing, which forces `len(actions) >= 1`. #21 is forward-defensive.
- **What is the actual override widget name?** `lhp_run_id_override` per `src/lhp_watermark/runtime.py:31`. The issue body's `__lhp_run_id_override` is incorrect; the runbook entry must use the real name to be operator-actionable.

### Deferred to Implementation

- **Exact retry budget tuning for #22:** start at `budget=10`, `base_secs=0.5`. May need a one-line bump after devtest replay if log spam is excessive; do not over-engineer at plan time.
- **Exact wording of the LHP-MAN-006 (#21) and LHP-MAN-007 (#23) error messages:** must follow the existing `LHP-MAN-NNN: <human title> — <details> [<remediation hint>]` form from PR #25 U2/U3. Final wording in code review.

---

## Implementation Units

- [ ] U1. **Document #19 omission in template + runbook + errors_reference**

**Goal:** Close issue #19 by making the `b2_manifests` DDL design choice explicit and discoverable. No behavior change.

**Requirements:** R1, R8

**Dependencies:** None

**Files:**
- Modify: `src/lhp/templates/bundle/prepare_manifest.py.j2` (DDL block comment at `:87-112`)
- Modify: `docs/runbooks/b2-for-each-rollout.md` (new addendum under "Internals: iteration contract" or similar — three-key audit join pattern)
- Modify: `docs/errors_reference.rst` (no new error code; cross-reference under existing `b2_manifests` audit notes if any, or new short prose section)

**Approach:**
- Add a `{# ... #}` template comment above the DDL stating that `jdbc_table`, `watermark_column`, `landing_path` flow via `taskValues` only, and pointing to the runbook section that documents the audit join pattern (`b2_manifests JOIN watermarks ON worker_run_id = run_id`).
- Add a runbook subsection with a sample audit query showing the join.
- Update `errors_reference.rst` if there is an existing B2 audit-pattern section; otherwise omit.

**Patterns to follow:**
- `prepare_manifest.py.j2:117-119` — existing `{# R1a: ... #}` template comment style.
- `docs/runbooks/b2-for-each-rollout.md:380-404` — "Internals: iteration contract" addendum pattern.

**Test scenarios:**
- *Test expectation: none — pure documentation/comment change with no code path. Verified by review.*

**Verification:**
- `git diff` shows comment-only change in `prepare_manifest.py.j2`.
- Runbook addendum renders cleanly when previewed.
- Issue #19 can be closed with a link to the runbook section.

---

- [ ] U2. **Audit + close #20 (validate 24h window)**

**Goal:** Confirm no `INTERVAL 24 HOURS` code path survives in rendered `validate.py.j2` after the PR #25 U7 batch_id-via-taskValue fix; close #20 with the audit result.

**Requirements:** R2 (and R3 only if audit finds a surviving path)

**Dependencies:** None

**Files:**
- Read-only audit: `src/lhp/templates/bundle/validate.py.j2`
- Read-only audit: rendered output from `tests/test_validate_template.py` fixtures
- Modify (small): `src/lhp/templates/bundle/validate.py.j2` (add a 1-line template comment above the WHERE clause documenting that the window scan was removed in PR #25)
- Modify: `docs/runbooks/b2-for-each-rollout.md` (one-line note in troubleshooting)

**Approach:**
- **Audit already performed during planning (2026-04-27).** `rg -n "INTERVAL.*HOURS|created_at.*>" src/lhp/ tests/` returned zero matches in `validate.py.j2`. The single match in `prepare_manifest.py.j2:174-177` is the scope-distinct 30-day retention DELETE.
- Add a 1-line `{# ... #}` template comment above the manifest WITH clause (`validate.py.j2:127`) stating that the previous `INTERVAL 24 HOURS` window was removed in the PR #25 (issue #19) batch_id-via-taskValue fix and must not be reintroduced — the `WHERE batch_id = …` filter is now the only correct form.
- Add a 1-line note to the runbook troubleshooting section stating that `validate` no longer time-bounds its scan, so long-running fleets (>24 h) are no longer at risk of `noop_pass` masking.
- Audit-found-path escalation logic from the original plan is *not* needed; remove it from this unit.

**Patterns to follow:**
- `validate.py.j2:33-39` — existing "NOTE — batch_id via taskValue" template comment.

**Test scenarios:**
- *Test expectation: none — comment-only change. Audit already complete; no behavioral change.*

**Verification:**
- Audit `rg` command returns zero matches in `validate.py.j2`.
- Render of `validate.py.j2` shows no time-window filter on the manifest WITH clause.
- Issue #20 can be closed with a link to the audit grep result + the new template comment.

---

- [ ] U3. **Add `{% if actions %}` guard around `prepare_manifest` MERGE block**

**Goal:** Defensive hardening: empty `actions` list raises a clear `LHP-MAN-006` `RuntimeError` instead of a Spark `ParseException`.

**Requirements:** R4

**Dependencies:** None

**Files:**
- Modify: `src/lhp/templates/bundle/prepare_manifest.py.j2` (lines `:131-285` — wrap `_manifest_rows`, `_values_fragment`, MERGE block, AND iteration emission in `{% if actions %}`; `{% else %}` raises early)
- Modify: `tests/test_prepare_manifest_template.py` (new test covering the empty-actions render → executable-Python-with-clear-error path)

**Approach:**
- Add `{% if actions %}` immediately after the DDL block (`:113`) and `{% else %}raise RuntimeError("LHP-MAN-006: …"){% endif %}` before the closing of the file.
- The guard wraps both the MERGE construction (`_manifest_rows`, `_values_fragment`, `_merge_sql`, retention DELETE, `execute_with_concurrent_commit_retry`) AND the iterations payload (`_iterations`, `taskValues.set`) — empty actions means there is nothing to MERGE *and* nothing to emit.
- Error message: `"LHP-MAN-006: prepare_manifest rendered with empty actions list — generator contract violation. _emit_b2_aux_files must be called with at least one jdbc_watermark_v2 LOAD action; LHP-CFG-033 enforces this at codegen time."`

**Patterns to follow:**
- `prepare_manifest.py.j2:225-230` — `_on_merge_exhausted` raise pattern for `LHP-MAN-001`.
- Existing template-side `{% if ... %}` blocks (e.g., `validate.py.j2:259-272` parity-check guard).

**Test scenarios:**
- Happy path — `actions=[<one entry>]` renders the same as today (regression check on existing fixture).
- Edge case — `actions=[]` renders to a notebook that, when executed, raises `RuntimeError` with `"LHP-MAN-006"` substring before any `spark.sql` call.
- Edge case — `actions=[]` rendered output contains *no* `MERGE INTO`, no `taskValues.set`, no `dbutils.jobs.taskValues.set` (verify by string search on rendered template).

**Verification:**
- Rendered template with `actions=[]` parses as valid Python.
- Executing rendered template with `actions=[]` against a mock `spark`/`dbutils` raises `RuntimeError("LHP-MAN-006: …")` and `spark.sql` is never called.
- Existing `tests/test_prepare_manifest_template.py` tests pass unchanged.

---

- [ ] U4. **Wrap `_alter_clustering_to_target` + `_add_load_group_column` in retry helper**

**Goal:** Survive fleet-deploy `ConcurrentAppendException` race on `ALTER TABLE` paths in `WatermarkManager.__init__`.

**Requirements:** R5, R8

**Dependencies:** None

**Files:**
- Modify: `src/lhp_watermark/watermark_manager.py:290-329` (both `_add_load_group_column` and `_alter_clustering_to_target`)
- Modify: `tests/lhp_watermark/test_watermark_manager.py` (add concurrent-commit retry coverage for both methods)
- Modify: `docs/runbooks/b2-for-each-rollout.md` (note expected behavior on first concurrent deploy)

**Approach:**
- Replace the bare `self.spark.sql(sql)` in each method with `execute_with_concurrent_commit_retry(self.spark, sql, on_exhausted=..., retry_budget=10, base_secs=0.5)`.
- Keep the existing `WatermarkConcurrencyError` translation in `on_exhausted` so the public exception contract is unchanged: only the retry budget widens.
- Update logger.info messages to note retry attempts when they happen (`logger.warning("ALTER TABLE … retry %d/%d after ConcurrentAppendException", attempt, budget)`).

**Patterns to follow:**
- Existing `execute_with_concurrent_commit_retry` call sites in `src/lhp_watermark/watermark_manager.py` (search for `execute_with_concurrent_commit_retry` — already imported at module top).
- Error class translation pattern at `_alter_clustering_to_target:320-329`.

**Test scenarios:**
- Happy path — first ALTER succeeds, no retry; existing test fixtures continue to pass.
- Edge case — first ALTER raises `ConcurrentAppendException`, second succeeds; retry helper is invoked with `budget=10`; method returns cleanly with one warning log.
- Error path — all `budget` attempts raise `ConcurrentAppendException`; method raises `WatermarkConcurrencyError` with `attempts=10` (not 1 as today).
- Error path — first ALTER raises a non-concurrent-commit `Exception` (e.g., `AnalysisException`); retry helper does **not** retry; method re-raises as today.
- Edge case — `_add_load_group_column` retry path mirrors `_alter_clustering_to_target` retry path; both paths covered.

**Verification:**
- `tests/lhp_watermark/test_watermark_manager.py` ALTER tests pass; new retry tests pass.
- `WatermarkConcurrencyError.attempts` reflects retry count, not constant 1.
- Devtest replay (manual, gated on PR test plan): two pipelines deployed concurrently against a fresh watermarks table both succeed.

---

- [ ] U5. **Add prepare-time collision rejection + override-pattern validation for #23**

**Goal:** Reject operator-supplied `lhp_run_id_override` whose derived `batch_id` collides with a non-`pending` row in `b2_manifests`, before any MERGE state mutation. Also tighten override-widget pattern validation.

**Requirements:** R6, R7, R8

**Dependencies:** None

**Files:**
- Modify: `src/lhp/templates/bundle/prepare_manifest.py.j2` (insert new COMMAND between `:120` `batch_id = derive_run_id(...)` and the retention-DELETE block at `:158`)
- Modify: `tests/test_prepare_manifest_template.py` (collision-rejection coverage)
- Modify: `tests/lhp_watermark/test_runtime.py` (extend with override-pattern regression tests; file already exists)
- Modify: `docs/runbooks/b2-for-each-rollout.md` (override widget operator guidance + collision recovery)
- Modify: `docs/errors_reference.rst` (`LHP-MAN-007` entry)
- *No changes to `src/lhp_watermark/runtime.py` or `src/lhp_watermark/sql_safety.py` — the override regex `_JOB_RUN_ID = r"^job-\d+-task-\d+-attempt-\d+$"` already enforces R7 strictly. Verified during planning.*

**Approach:**
- New COMMAND block in `prepare_manifest.py.j2`, slotted *after* `batch_id` derivation and *before* retention DELETE:
  ```python
  # R6 (issue #23): reject operator-supplied run_id overrides that collide
  # with a non-pending row in b2_manifests. Catches batch_id reuse from a
  # prior live or completed batch — the MERGE WHEN MATCHED only updates
  # updated_at and would leave non-pending rows in their stale state,
  # producing silent extraction skip + false validate pass.
  _collision_sql = (
      f"SELECT count(1) AS n FROM {manifest_table} "
      f"WHERE batch_id = {sql_literal(batch_id)} "
      "AND execution_status != 'pending'"
  )
  _collision_n = int(spark.sql(_collision_sql).collect()[0]["n"])
  if _collision_n > 0:
      raise RuntimeError(
          f"LHP-MAN-007: batch_id {batch_id!r} collides with "
          f"{_collision_n} non-pending row(s) in {manifest_table}. "
          "Operator must supply a distinct lhp_run_id_override value, "
          "or clear the override and let derive_run_id resolve from the "
          "Jobs context. See docs/runbooks/b2-for-each-rollout.md "
          "section on lhp_run_id_override."
      )
  ```
- Lock the override-validator contract via regression tests in `tests/lhp_watermark/test_runtime.py` (file exists). Cover happy + malformed forms. The validator regex itself (`sql_safety.py:39`) is already strict — the test exists to fail loudly if a future contributor weakens it.
- Runbook addendum: under existing "Anomaly B addendum" or a new "lhp_run_id_override widget" section, document valid forms, collision risk, and recovery procedure (clear the widget OR pick a distinct value).

**Patterns to follow:**
- Existing prepare-time SQL pattern: `prepare_manifest.py.j2:158-191` (retention DELETE — same `spark.sql(...).collect()` shape).
- Error code raise pattern: `prepare_manifest.py.j2:225-230` (`LHP-MAN-001` shape).
- Runbook addendum pattern: `docs/runbooks/b2-for-each-rollout.md:278-329` (Anomaly B / issue #18 addendum format).

**Test scenarios:**
- Happy path — fresh `batch_id` (no rows in `b2_manifests`): collision check passes (`_collision_n == 0`), MERGE proceeds.
- Happy path — `batch_id` exists but only as `execution_status = 'pending'` (idempotent re-execute): collision check passes (predicate filters `!= 'pending'`), MERGE proceeds with `WHEN MATCHED` updating `updated_at` only (today's behavior preserved).
- Error path — `batch_id` exists with `execution_status = 'completed'` from a prior batch: collision check raises `RuntimeError("LHP-MAN-007: …")`; no MERGE executes; `dbutils.jobs.taskValues.set` is never called.
- Error path — `batch_id` exists with `execution_status = 'failed'` from a prior batch: same as above.
- Error path — `batch_id` exists with `execution_status = 'running'` (live batch concurrent collision): same as above.
- Override pattern — operator supplies `job-123-task-456-attempt-2`: `derive_run_id` returns it; collision check applies normally.
- Override pattern — operator supplies `not-a-valid-form`: `SQLInputValidator.uuid_or_job_run_id` raises early at `runtime.py:50` before `derive_run_id` returns; `prepare_manifest` never sees the malformed value.
- Override pattern — operator supplies `''` (empty): `_read_override_widget` returns `None`; falls through to Jobs-context path (today's behavior preserved).

**Verification:**
- New `tests/test_prepare_manifest_template.py` tests pass for all three collision states (`completed` / `failed` / `running`) and the two non-collision states (`pending` / no-row).
- `tests/lhp_watermark/test_runtime.py` rejects malformed override values with the expected error class.
- Devtest replay (manual, gated on PR test plan): operator deliberately re-uses a recent `run_id` via override widget; pipeline fails fast with `LHP-MAN-007`.
- Runbook addendum renders cleanly; operator can find the recovery procedure.

---

- [ ] U6. **Tests + errors_reference parity for new error codes**

**Goal:** Wire the new error codes (`LHP-MAN-006` from U3, `LHP-MAN-007` from U5, optional `LHP-VAL-051` from U2 only if a window survived) into the existing parity test (`tests/test_b2_error_code_catalog_parity.py` from PR #25 U6) and `docs/errors_reference.rst`.

**Requirements:** R8

**Dependencies:** U2 (only if U2 added a new code), U3, U5

**Files:**
- Modify: `docs/errors_reference.rst` (new entries for `LHP-MAN-006`, `LHP-MAN-007`, and conditionally `LHP-VAL-051`)
- Modify: `tests/test_b2_error_code_catalog_parity.py` (extend code-catalog list if hardcoded)
- *(no production code changes — purely docs + test wiring)*

**Approach:**
- Each new entry in `errors_reference.rst` follows the existing 4-section form: cause / common conditions / resolution / example. Mirror the `LHP-MAN-001..005` entries already added by PR #25.
- If `tests/test_b2_error_code_catalog_parity.py` walks the templates AST/string for raised codes (rather than a hardcoded list), no test change is needed — the parity test self-extends. Verify which form during implementation.

**Patterns to follow:**
- `docs/errors_reference.rst` — existing `LHP-MAN-001..005` entry forms (added in PR #25 U5).
- `tests/test_b2_error_code_catalog_parity.py` — PR #25 U6.

**Test scenarios:**
- Parity test passes: every code raised in any B2 template (`LHP-MAN-001..007`) has a matching `errors_reference.rst` section.
- Parity test fails (negative case): if a developer adds `LHP-MAN-008` later without an `errors_reference.rst` entry, the test fails with a clear message naming the missing code.

**Verification:**
- `pytest tests/test_b2_error_code_catalog_parity.py` passes.
- `make docs` (or equivalent Sphinx build) renders the new entries cleanly.

---

## System-Wide Impact

- **Interaction graph:**
  - U3 / U5 changes are scoped to `prepare_manifest.py.j2` rendering; no caller signature changes.
  - U4 changes are scoped to `WatermarkManager.__init__` internals; no public API changes.
  - U2 changes are template-comment-only in the no-surviving-path case; the conditional escalation path widens `_emit_b2_aux_files`'s codegen context with one new key (`validate_window_hours`) — covered if triggered.
  - U1 / U6 are pure docs/test wiring.
- **Error propagation:**
  - U3's `LHP-MAN-006` raises at notebook-execution time, before any `spark.sql` call. Caught by DAB task error surface; reported as a clean failure.
  - U4's `WatermarkConcurrencyError` contract is unchanged — only the retry budget changes; downstream callers (`mark_completed`, `mark_failed`, `claim`) see the same error class.
  - U5's `LHP-MAN-007` raises before MERGE; manifest stays in pre-batch state; no partial rows.
- **State lifecycle risks:**
  - U5 specifically *prevents* a state-divergence class (silent skip + false validate pass). No new lifecycle risk introduced.
  - U4 reduces the `pending`-row-leak risk on first concurrent deploy (constructor failure mid-`__init__` previously left a worker dead before manifest claim).
- **API surface parity:** None — every change is internal or documentation.
- **Integration coverage:** Devtest replay of a smoke fixture (`b2_smoke.yaml` from PR #25 / PR #26 plan) is the operator-validation step gating PR-merge for U3/U4/U5. Captured in PR test plan, not duplicated here.
- **Unchanged invariants:**
  - `B2_ITERATION_KEYS` shape (10 keys, frozenset). U1 explicitly does not widen this.
  - `derive_run_id` return contract (`local-<uuid4>` OR `job-{int}-task-{int}-attempt-{int}`). U5 R7 only tightens the validator regex; the contract is unchanged.
  - `MERGE WHEN MATCHED` semantics (`UPDATE SET t.updated_at = current_timestamp()` only). U5 rejects collisions *before* MERGE, leaving WHEN MATCHED behavior unchanged for the legitimate idempotent-re-execute case.
  - `b2_manifests` DDL column list. U1 documents the omission, does not change DDL.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| U2 audit finds a surviving 24h window code path that the plan assumes is gone — scope creep mid-implementation | Plan U2 as audit-first with a documented escalation path (remove or make configurable); time-box the audit to 30 min before deciding. |
| U3's `{% if actions %}` guard breaks an existing test that constructs an empty-actions render fixture for negative testing | Run the full `tests/test_prepare_manifest_template.py` suite before committing U3; if a test exists, update it to expect `LHP-MAN-006` instead of `ParseException`. |
| U4's wider retry budget (`budget=10`) masks a real Delta DDL failure (e.g., AnalysisException for invalid clustering column) by retrying it 10 times before surfacing | Verify the existing `_is_concurrent_commit_exception` predicate filters on `ConcurrentAppendException` only — non-concurrent exceptions raise on first attempt today; retry helper preserves this. |
| U5's collision-check `SELECT COUNT(1)` adds one round-trip to `prepare_manifest`. At fleet scale (300 actions × hourly batches) the predicate is selective on `batch_id` (PK first column, liquid-clustered) so cost is sub-second | Confirm `EXPLAIN` shows index/clustering pruning during devtest replay. If unexpectedly slow, narrow predicate or add a `LIMIT 1` short-circuit. |
| U5's runbook addendum and U1's runbook addendum may overlap in the override-widget operator guidance section | Coordinate during U1/U5 implementation: place U1's audit-join section and U5's override-widget section in distinct subsections of `b2-for-each-rollout.md`. |
| Issue #23 body refers to widget name `__lhp_run_id_override` (double-underscore prefix); actual widget name in `runtime.py:31` is `lhp_run_id_override` | Plan + error messages + runbook use the actual widget name. Cross-reference the issue body's discrepancy in the PR description. |

---

## Documentation / Operational Notes

- **Runbook updates** land with U1 (audit-join section), U2 (window-removal note), U4 (concurrent-deploy expected behavior), and U5 (override widget operator guidance + collision recovery). Coordinate via a single docs commit at the end of the PR rather than one per unit if review prefers.
- **`errors_reference.rst` updates** land with U6, gated by the existing parity test (PR #25 U6).
- **PR description** must explicitly note the widget-name discrepancy between issue #23 body and `runtime.py:31`, and document that the plan / fix uses the actual widget name `lhp_run_id_override`.
- **Devtest replay** of U3 / U4 / U5 changes happens against the existing `b2_smoke.yaml` fixture before PR-merge per the standing PR test-plan checklist (memory `project_devtest_validation`).

---

## Sources & References

- **Origin issues:** [#19](https://github.com/dwtorres/Lakehouse_Plumber/issues/19), [#20](https://github.com/dwtorres/Lakehouse_Plumber/issues/20), [#21](https://github.com/dwtorres/Lakehouse_Plumber/issues/21), [#22](https://github.com/dwtorres/Lakehouse_Plumber/issues/22), [#23](https://github.com/dwtorres/Lakehouse_Plumber/issues/23)
- **Predecessor plan:** `docs/plans/2026-04-26-001-fix-b2-p0-p1-deferred-followups-plan.md` (P0/P1 issues #14-17 — same review run, merged via PR #25)
- **Predecessor plan:** `docs/plans/2026-04-27-001-fix-b2-issue-18-duplicate-run-id-handling-plan.md` (P2 issue #18 — same review run, merged via PR #26 / PR #27)
- **Review run:** `20260426-115040-b1c3456d` (adversarial + api-contract reviewers)
- **Related code:**
  - `src/lhp/templates/bundle/prepare_manifest.py.j2`
  - `src/lhp/templates/bundle/validate.py.j2`
  - `src/lhp_watermark/watermark_manager.py`
  - `src/lhp_watermark/runtime.py`
  - `src/lhp/models/b2_iteration.py`
- **Runbook:** `docs/runbooks/b2-for-each-rollout.md`
- **Error catalog:** `docs/errors_reference.rst`
