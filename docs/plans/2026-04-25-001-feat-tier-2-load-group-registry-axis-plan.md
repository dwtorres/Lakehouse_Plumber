---
title: "feat: Tier 2 — load_group Registry Axis"
type: feat
status: active
date: 2026-04-25
origin: docs/planning/tier-2-hwm-load-group-fix.md
---

# feat: Tier 2 — load_group Registry Axis

## Overview

Add a `load_group STRING` axis to the `<env>_orchestration.watermarks` Delta registry and extend `WatermarkManager` to accept `load_group: Optional[str] = None` on every state-machine method. Switch Delta liquid clustering to `(source_system_id, load_group, schema_name, table_name)` so HWM lookups under per-pipeline batching scale. Ship a CLI helper that emits per-table SELECT-preview + INSERT-seed SQL for fleet adoption, and a Databricks-side V1–V4 deep-clone validation notebook. Tier 1 (commit `63bcfdb`) closed cross-source HWM poisoning; Tier 2 closes same-source same-`(schema, table)` cross-flowgroup poisoning, which is a hard prerequisite for B2.

---

## Problem Frame

Today every `WatermarkManager` call writes and reads watermark rows scoped only by `(source_system_id, schema_name, table_name)`. Two LHP flowgroups loading overlapping `(schema, table)` pairs from the same `source_system_id` collide: whichever runs first advances the HWM; the other reads that HWM as its starting point and silently returns zero new rows on a static-data slice. B2's `execution_mode: for_each` topology depends on per-`(pipeline, flowgroup)` HWM identity — without a `load_group` axis, it cannot ship correctly.

The fix is a four-part, three-environment, zero-downtime migration:

1. Add nullable `load_group STRING` column via auto-DDL inside `_ensure_table_exists`, plus conditional `ALTER TABLE … CLUSTER BY` for the new key.
2. Extend `WatermarkManager` method signatures with `load_group: Optional[str] = None` (back-compat with `None` callers via three-way `WHERE` clause).
3. Thread the composite `load_group = f"{flowgroup.pipeline}::{flowgroup.flowgroup}"` from generator → template → manager calls.
4. Ship a CLI helper for per-flowgroup legacy-HWM seed (Step 4a) and a V1–V4 deep-clone validation notebook.

(see origin: `docs/planning/tier-2-hwm-load-group-fix.md`)

---

## Requirements Trace

- R1. Auto-DDL adds nullable `load_group STRING` column to `watermarks` on first write against a pre-Tier-2 table; idempotent on every subsequent `WatermarkManager` init (no metadata churn).
- R2. Auto-DDL switches Delta liquid clustering to `(source_system_id, load_group, schema_name, table_name)` via conditional `ALTER TABLE … CLUSTER BY` (skip-if-already-matching, evaluated via `DESCRIBE DETAIL`).
- R3. `WatermarkManager.insert_new`, `mark_landed`, `mark_complete`, `mark_failed`, `mark_bronze_complete`, `mark_silver_complete`, `get_latest_watermark`, `get_recoverable_landed_run` all accept `load_group: Optional[str] = None` with full back-compat for `None` callers.
- R4. `get_latest_watermark` and `get_recoverable_landed_run` use the three-way filter `(load_group = :load_group OR (:load_group IS NULL AND load_group IS NULL))` so legacy callers (`load_group=None`) and B2 callers (explicit composite) coexist safely during migration.
- R5. The JDBC template (`jdbc_watermark_job.py.j2`) and generator (`jdbc_watermark_job.py`) thread `load_group` into every `WatermarkManager` call so an existing `jdbc_watermark_v2` flowgroup, on next deploy, writes its `(pipeline, flowgroup)` composite to the registry.
- R6. Operator can run an idempotent `UPDATE … SET load_group = 'legacy' WHERE load_group IS NULL` per environment to backfill pre-Tier-2 rows.
- R7. Operator can run `OPTIMIZE metadata.<env>_orchestration.watermarks` once after backfill so HWM lookups against newly-seeded namespaces see clustered storage from the first B2 run.
- R8. Operator can run a CLI helper `lhp seed-load-group` that consumes a flowgroup YAML + env substitutions and emits per-table SELECT-preview + INSERT-seed SQL ready to paste into a Databricks SQL session.
- R9. V1–V4 verification probes pass against a deep-clone (`watermarks_v1_probe`): V1 isolation smoke (per-`load_group` HWM separation), V2 Tier-1 cross-source regression, V3 legacy fallback for `load_group=None` callers, V4 legacy-seed-then-incremental smoke.
- R10. Operator runbook documents Step 3-before-Step-4a precondition, the per-env pre-flight `DESCRIBE DETAIL` capture, and the `extraction_type='seeded'` enum extension for the seed insert.

---

## Scope Boundaries

- New `LHP-CFG-019` (separator collision), `LHP-CFG-026` (composite uniqueness) error codes — those are codegen-time validators that fire ONLY when `execution_mode: for_each` is set, and the `execution_mode` field itself ships in the B2 plan. Move both error codes to `2026-04-25-002-feat-b2-watermark-scale-out-emission-plan.md` (U2). Tier 2 ships zero new error codes.
- Big-bang fleet migration of all flowgroups to per-`(pipeline, flowgroup)` `load_group` values. Per-flowgroup adoption (Step 4a / `lhp seed-load-group`) is the rollout pattern; Tier 2 ships only the registry, manager, threading, and tooling.
- Flipping `load_group` to `NOT NULL WITH DEFAULT`. One-way Delta protocol bump (`delta.columnMapping.mode = 'name'`); deferred ≥30 days post-fleet adoption, gated on Delta protocol review.
- Tier 3 per-integration registry tables.
- ADR-004 registry placement changes.
- HIPAA hashing implementation (separate milestone).
- B2 emission pattern (`execution_mode: for_each`, `b2_manifests`, `prepare_manifest.py.j2`, `validate.py.j2`, three-task DAB topology) — covered in Plan 2.

### Deferred to Follow-Up Work

- Per-flowgroup seed execution across the fleet: requires Step 3 (`'legacy'` backfill) per env, then operator-coordinated `lhp seed-load-group` runs per `(flowgroup, source_table)`. Not in this plan beyond shipping the helper + runbook.
- Recurring `OPTIMIZE` (weekly) + `VACUUM RETAIN 168 HOURS` (daily) policy — covered in Plan 2 OPS unit, not Tier 2 directly.
- Capture migration evidence in a follow-up ADR (ADR-005 candidate per ADR-004 §"Open questions deferred"). Author after Tier 2 ships in qa.

---

## Context & Research

### Relevant Code and Patterns

- `src/lhp_watermark/watermark_manager.py:155-205` — `_ensure_table_exists` is currently `CREATE TABLE IF NOT EXISTS` only. **No `ALTER` path today.** This unit must extend it with a conditional `ALTER TABLE … ADD COLUMNS` + `ALTER TABLE … CLUSTER BY` based on a `DESCRIBE DETAIL` probe.
- `src/lhp_watermark/watermark_manager.py:207-297` (`get_latest_watermark`) — strict `status='completed'` filter precedent; SQL composition uses `sql_literal()` + `SQLInputValidator.identifier()`.
- `src/lhp_watermark/watermark_manager.py:353-465` (`insert_new`) — MERGE composition; column list at lines 438–451 is the verified DDL ground truth.
- `src/lhp_watermark/watermark_manager.py:506-572` (`mark_failed`), `:574-611` (`mark_landed`), `:688-747` (`mark_complete`), `:649-686` (`_stage_complete`) — every state-machine entry point that needs the new kwarg.
- `src/lhp_watermark/sql_safety.py` — `SQLInputValidator.string` / `.identifier`; `sql_literal()` / `sql_identifier()`. The new three-way `load_group` filter must compose through these, never f-string concatenation.
- `src/lhp_watermark/runtime.py:97` — `derive_run_id` returns `f"job-{job_run_id}-task-{task_run_id}-attempt-{attempt}"`. Verified empirically; B2 depends on this format.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2:73-77, :179-189, :283-297` — every `wm.<method>(...)` call site that must add `load_group=load_group` kwarg.
- `src/lhp/generators/load/jdbc_watermark_job.py:233-250` — `template_context` dict where the composite `load_group = f"{flowgroup.pipeline}::{flowgroup.flowgroup}"` lands.
- `src/lhp/cli/main.py:443-461` — `sync-runtime` decorator pattern; `lhp seed-load-group` mirrors this shape.
- `src/lhp/cli/commands/base_command.py:12` — `BaseCommand` class with `setup_from_context()` + `ensure_project_root()`; the new `seed_load_group_command.py` inherits this.
- `scripts/validation/validate_td007_databricks.py` — Databricks-notebook validation pattern (`# COMMAND ----------` markers + widgets + JSON exit). Template for `validate_tier2_load_group.py`.
- `Example_Projects/edp_lhp_starter/substitutions/{devtest,qa,prod}.yaml` — `watermark_catalog: metadata` + `watermark_schema: <env>_orchestration` substitutions; the CLI helper reads both to assemble `metadata.<env>_orchestration.watermarks`.
- `tests/lhp_watermark/test_get_latest.py:145`, `test_insert_new.py:259` — `@pytest.mark.parametrize` precedent across `_RecordingSpark` / `_ScriptedSpark` mocks.
- `tests/test_jdbc_watermark_v2_integration.py` — class-based integration test, `@pytest.mark.integration`, `tmp_path` project + full orchestrator run; pattern for the per-env `load_group` literal assertion.

### Institutional Learnings

- ADR-004 §"Open questions (deferred)" anticipates Tier 2: "Capture migration evidence in a future ADR if/when a destructive schema change is needed." Tier 2 is exactly that case (column add + clustering switch).
- ADR-002 §"Path 5" allows new submodules in `src/lhp_watermark/`; Tier 2 keeps to method-signature additions inside `watermark_manager.py` plus `_ensure_table_exists` extension. **No new public exports** in `src/lhp_watermark/__init__.py`.
- Tier 1 V1 probe (`docs/planning/tier-1-hwm-fix.md`) uses synthetic `'fake_federation'` `source_system_id` rows at `2099-01-01` and cleanup by isolated marker `run_id`s (not table-scoped DELETE). Tier 2 V1 mirrors this with synthetic `load_group='lg_a'` vs `'lg_b'`.
- `docs/runbooks/devtest-validation-adr-003-phase-a-c.md` — the deploy/run/SQL-verify shape for Databricks-side validation. Tier 2 V1–V4 runbook follows this format.
- `extraction_type` column is `STRING NOT NULL` with no enum constraint at DDL (`watermark_manager.py:184`). Adding the new value `'seeded'` for Step 4a inserts is structurally safe; the audit `rg "extraction_type" --type py` confirms no consumer enumerates explicit values today.

### External References

- Delta Lake `ALTER TABLE … CLUSTER BY` semantics (Delta 3.x+) — clustering change reshuffles only future writes; existing files require `OPTIMIZE`. Documented in plan via Step 3a `OPTIMIZE`.
- Delta `ALTER TABLE … ADD COLUMNS IF NOT EXISTS` — supported since Delta 3.x; the conditional probe pattern (`DESCRIBE TABLE` + check column list) is the safest portable form.

---

## Key Technical Decisions

- **Auto-DDL extends to ALTER, not just CREATE.** Current `_ensure_table_exists` only creates on absence. Tier 2 adds a conditional `ALTER TABLE … ADD COLUMNS (load_group STRING)` + `ALTER TABLE … CLUSTER BY (...)` path gated by a `DESCRIBE TABLE` / `DESCRIBE DETAIL` probe. Rationale: alternative (operator-run DDL ahead of code deploy) creates a dependency that's hard to enforce in dev/qa/prod ordering and introduces "works in dev, missing in prod" risk.
- **Three-way SQL filter for `get_latest_watermark`.** Compose via Python conditional fragments (no f-string of user data), then emit through `sql_literal()`. The clause `(load_group = :lg OR (:lg IS NULL AND load_group IS NULL))` is the migration safety valve: legacy callers passing `None` see legacy + NULL rows; B2 callers passing the composite see only their own.
- **`load_group` is kwarg, not positional, on every method.** Default `None`. All existing test callers and notebook callers continue to compile and pass without code changes until they're explicitly updated.
- **`extraction_type='seeded'` for Step 4a inserts.** The seed copies the `'legacy'` row's HWM into the new namespace with a UUID-based `run_id` and `extraction_type='seeded'` so an operator can audit the migration trail. The column is `STRING NOT NULL`; the new value flows through with no enum-extension code change required (audited).
- **CLI helper as `lhp seed-load-group` Click subcommand**, not standalone `scripts/`. Matches the `sync-runtime` integration pattern; flows through `cli_error_boundary`; reads project root via `ensure_project_root()`.
- **V1–V4 ship as a Databricks notebook under `scripts/validation/`, not pytest.** The `_ScriptedSpark` mock pattern in `tests/lhp_watermark/` cannot exercise deep-clone semantics. Mirror `validate_td007_databricks.py` shape.
- **Backfill SQL and Step 4a SQL are runbook content, not code.** Operator runs them once per env via Databricks SQL or a notebook; Tier 2 ships the SQL templates + the precondition documentation + the helper that materializes the per-flowgroup INSERT statements.
- **No new public exports in `src/lhp_watermark/__init__.py`.** Method-signature additions only. `__all__` unchanged.

---

## Open Questions

### Resolved During Planning

- **Where do the LHP-CFG-019 (separator) and uniqueness checks live?** Resolved: in Plan 2 (B2). They operate on `execution_mode: for_each`, which is a B2 field. Tier 2 ships zero new error codes.
- **Auto-DDL ALTER path vs operator-run DDL?** Resolved: extend `_ensure_table_exists` with conditional ALTER. The plan author explicitly flagged the gap (current code is CREATE-only); the alternative creates an operator-ordering hazard.
- **CLI helper as standalone script or Click subcommand?** Resolved: Click subcommand `lhp seed-load-group`. Matches existing `sync-runtime` pattern, flows through `cli_error_boundary`, integrates with project-root resolution.

### Deferred to Implementation

- **Exact `DESCRIBE TABLE` shape for column-presence probe.** May need to use `DESCRIBE DETAIL` for both column list and clustering check; the conditional logic is straightforward but the SQL command choice depends on the simplest portable form. Implementer picks during U1.
- **Whether `mark_bronze_complete` / `mark_silver_complete` need `load_group` propagation.** They go through `_stage_complete`; the kwarg threading is mechanical but needs verification that the called SQL filters by `run_id` only (not by `load_group`) so the kwarg is store-only there. Implementer confirms during U2.
- **Per-env clustering-switch cost.** Pre-flight `DESCRIBE DETAIL metadata.<env>_orchestration.watermarks` per env captures `partitionColumns` / `clusteringColumns` / `numFiles` / Delta protocol. Devtest already liquid-clustered (per ADR-004 §Implementation Status); qa/prod state must be confirmed at deploy time. If partitioned-not-clustered, plan a maintenance window before merging Step 1.

---

## High-Level Technical Design

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

### Auto-DDL extension shape

```
_ensure_table_exists():
    SHOW TABLES IN namespace
    if not exists:
        CREATE TABLE IF NOT EXISTS watermarks ( ... load_group STRING, ... )
            CLUSTER BY (source_system_id, load_group, schema_name, table_name)
        return
    DESCRIBE TABLE watermarks → column list
    if 'load_group' not in columns:
        ALTER TABLE watermarks ADD COLUMNS (load_group STRING)
    DESCRIBE DETAIL watermarks → clusteringColumns
    if clusteringColumns != [source_system_id, load_group, schema_name, table_name]:
        ALTER TABLE watermarks CLUSTER BY (source_system_id, load_group, schema_name, table_name)
```

Idempotent on every init: the column probe and clustering probe both short-circuit when the target shape is already in place.

### Three-way `WHERE` clause shape

```
WHERE source_system_id = :src
  AND schema_name      = :sch
  AND table_name       = :tbl
  AND status           = 'completed'
  AND ( load_group = :lg OR (:lg IS NULL AND load_group IS NULL) )
```

When `load_group is None`: the Python composer skips the `load_group = :lg` arm and emits only the `IS NULL` arm; the resulting SQL filters to legacy + NULL-load_group rows.
When `load_group is not None`: the composer emits both arms; SQL evaluates as `load_group = :lg` (the right-arm `IS NULL` short-circuits to false).

### Migration ordering across envs

```
dev  → Step 1 deploy (auto-DDL fires) → Step 3 backfill → Step 3a OPTIMIZE → V1–V4 deep-clone → per-flowgroup Step 4a (helper) over time
qa   → Step 1 deploy → Step 3 → Step 3a → V1–V4 deep-clone → per-flowgroup Step 4a
prod → Step 1 deploy → Step 3 → Step 3a → V1–V4 deep-clone → per-flowgroup Step 4a
```

Each env's first B2 run depends on all five gates clearing for that env.

---

## Implementation Units

- [ ] U1. **Extend `_ensure_table_exists` for conditional ALTER (column add + clustering switch)**

**Goal:** `WatermarkManager._ensure_table_exists` adds `load_group STRING` column and switches Delta liquid clustering to the new key — both conditional, both idempotent.

**Requirements:** R1, R2

**Dependencies:** None (first unit in the plan)

**Files:**
- Modify: `src/lhp_watermark/watermark_manager.py` (extend `_ensure_table_exists` lines 155–205)
- Test: `tests/lhp_watermark/test_ensure_table_exists.py` (new file)

**Approach:**
- For brand-new tables: keep CREATE path; just add `load_group STRING` to the column list and update `CLUSTER BY` to `(source_system_id, load_group, schema_name, table_name)`.
- For pre-existing tables: probe column list via `DESCRIBE TABLE {table_name}`. If `load_group` not present, emit `ALTER TABLE {table_name} ADD COLUMNS (load_group STRING)`.
- Probe clustering via `DESCRIBE DETAIL {table_name}` reading `clusteringColumns`. If not equal to target list, emit `ALTER TABLE {table_name} CLUSTER BY (source_system_id, load_group, schema_name, table_name)`.
- Both probes short-circuit when target shape already in place (no SQL emitted on subsequent inits).
- Wrap each ALTER in `try/except` logging; surface as `WatermarkConcurrencyError` if Delta optimistic-concurrency rejects (unlikely for DDL, but the existing retry helper covers DML).

**Patterns to follow:**
- `_ensure_utc_session()` pre-call (line 162) — same pattern.
- `time.time()` start + duration logging (line 163, 204) — same pattern.
- SQL composition via f-string but only with `self.table_name` and `self.catalog`/`self.schema` (already validated identifiers in `WatermarkManager.__init__`); never with user data.

**Test scenarios:**
- Happy path: new namespace, no `watermarks` table → CREATE path runs → table exists with `load_group` column and target clustering. `_RecordingSpark` asserts SQL emitted contains `load_group STRING` and `CLUSTER BY (source_system_id, load_group, ...)`.
- Edge case: pre-existing table missing `load_group` column → `DESCRIBE TABLE` returns column list without `load_group` → exactly one `ALTER TABLE … ADD COLUMNS (load_group STRING)` SQL emitted.
- Edge case: pre-existing table with `load_group` column but old clustering `(source_system_id, schema_name, table_name)` → `DESCRIBE DETAIL` returns old `clusteringColumns` → exactly one `ALTER TABLE … CLUSTER BY` SQL emitted.
- Edge case: pre-existing table already in target shape → `DESCRIBE TABLE` returns full column list with `load_group`, `DESCRIBE DETAIL` returns target `clusteringColumns` → zero ALTER SQL emitted (no metadata churn).
- Idempotency: call `_ensure_table_exists()` twice in a row → second call emits zero SQL beyond the two probes.

**Verification:**
- All test scenarios pass.
- `WatermarkManager` instantiated against a `_RecordingSpark` with no preexisting table runs CREATE once and only emits the two probes on the second init.

---

- [ ] U2. **Extend `WatermarkManager` method signatures with `load_group: Optional[str] = None`**

**Goal:** Every state-machine and read method on `WatermarkManager` accepts `load_group: Optional[str] = None` kwarg with full back-compat for legacy callers.

**Requirements:** R3, R4

**Dependencies:** U1

**Files:**
- Modify: `src/lhp_watermark/watermark_manager.py`
  - `insert_new` (lines 353–465): add kwarg, write into MERGE source row.
  - `mark_landed` (lines 574–611): add kwarg, store-only (filter still by `run_id`).
  - `mark_complete` (lines 688–747): add kwarg, store-only.
  - `mark_failed` (lines 506–572): add kwarg, store-only.
  - `_stage_complete` (lines 649–686): add kwarg; propagate from `mark_bronze_complete` / `mark_silver_complete`.
  - `get_latest_watermark` (lines 207–297): add kwarg, three-way `WHERE` clause.
  - `get_recoverable_landed_run` (lines 299–351): add kwarg, three-way `WHERE` clause.
- Modify: `src/lhp_watermark/sql_safety.py` (only if a new validator helper is needed for the optional kwarg — otherwise reuse `SQLInputValidator.string`).
- Test: `tests/lhp_watermark/test_get_latest.py` (extend), `tests/lhp_watermark/test_insert_new.py` (extend), `tests/lhp_watermark/test_landed_recovery.py` (extend), `tests/lhp_watermark/test_mark_complete.py` (extend), `tests/lhp_watermark/test_mark_failed.py` (extend), `tests/lhp_watermark/test_mark_stage.py` (extend).

**Approach:**
- Insert the new kwarg as the **last** kwarg on every method, default `None`. No positional-argument churn.
- For `insert_new`: validate via `SQLInputValidator.string(load_group)` if not None; emit via `sql_literal(load_group)`. Add to MERGE source row column list.
- For `get_latest_watermark` / `get_recoverable_landed_run`: build the `load_group` clause in Python — when `None`, emit `AND load_group IS NULL`; when non-None, emit `AND ( load_group = :lg OR (:lg IS NULL AND load_group IS NULL) )`. The two-arm form is required when the caller passes the composite explicitly because legacy rows still have `IS NULL` and the migration safety valve expects to match them too.

Wait — re-read origin SQL exactly. Origin (`tier-2-hwm-load-group-fix.md` Step 2):

```
AND ( load_group = :load_group OR (:load_group IS NULL AND load_group IS NULL) )
```

Both arms render at SQL composition time even when caller passes a non-null composite. The right arm (`IS NULL AND IS NULL`) evaluates to false at runtime when `:load_group` is non-null. This is correct for legacy callers (`load_group=None` → both literal `:load_group` substitutions are `NULL`, right arm fires, returns NULL-load_group rows). For B2 callers (`load_group='pipe::fg'` → left arm fires only).

The Python composer always emits both arms. Three-way filter is one canonical SQL fragment, not two.

**Patterns to follow:**
- `SQLInputValidator.identifier()` precedent at `watermark_manager.py:242-244` — same shape for `load_group` validation, but use `.string()` since `load_group` is data-shaped not identifier-shaped (contains `::`).
- `sql_literal()` emission pattern; never f-string-concat user data.
- UTC-session pre-call + start-time + duration-logging shape; do not change.

**Test scenarios:**
- Happy path: `insert_new(..., load_group='pipe_a::fg_a')` → MERGE source row contains `load_group = 'pipe_a::fg_a'`.
- Happy path: `get_latest_watermark(..., load_group='pipe_a::fg_a')` against probe rows in `('pipe_a::fg_a', 'pipe_b::fg_b', 'legacy', NULL)` → returns only the `pipe_a::fg_a` row.
- Edge case: `get_latest_watermark(..., load_group=None)` against the same probe set → returns the `legacy` and `NULL` rows (both arms fire because both `:load_group` substitutions are SQL `NULL`).
- Edge case: `insert_new(..., load_group=None)` (legacy caller) → MERGE source row contains `load_group = NULL`.
- Error path: `insert_new(..., load_group='bad;value')` → `SQLInputValidator.string` rejects → raises `WatermarkValidationError` (LHP-WM-003).
- Edge case: `mark_complete(..., load_group='pipe::fg')` (kwarg present but filter is by `run_id` only) → does not change `WHERE` shape; the kwarg is store-only.
- Parametrize across `load_group ∈ {None, 'legacy', 'pipe_a::fg_a', 'pipe_a::fg_b'}` for full coverage matrix.

**Verification:**
- All test scenarios pass.
- `tests/lhp_watermark/test_package_structure.py` still passes (no new exports).

---

- [ ] U3. **Thread composite `load_group` through JDBC template + generator**

**Goal:** `jdbc_watermark_v2` flowgroups on next deploy write `load_group = f"{flowgroup.pipeline}::{flowgroup.flowgroup}"` to the registry, and reads filter by it.

**Requirements:** R5

**Dependencies:** U2

**Files:**
- Modify: `src/lhp/templates/load/jdbc_watermark_job.py.j2` (lines 73–77, 179–189, 283–297) — add `load_group=load_group` kwarg to every `wm.<method>(...)` call site.
- Modify: `src/lhp/generators/load/jdbc_watermark_job.py` (line 248 area, inside the `template_context` dict) — populate `"load_group": f"{flowgroup.pipeline}::{flowgroup.flowgroup}"`.
- Modify: `src/lhp/templates/load/jdbc_watermark_job.py.j2` (top of template, near line 67–71) — add `load_group = {{ load_group|tojson }}` literal injection (mirrors existing `logged_action_name` precedent at line 80).
- Test: `tests/test_jdbc_watermark_v2_integration.py` (extend `_generate` helper assertions).
- Test: `tests/e2e/test_edp_lhp_starter.py` (extend `test_per_env_catalog_substitution`).

**Approach:**
- Add `"load_group": f"{flowgroup.pipeline}::{flowgroup.flowgroup}"` to `template_context` near line 248 in the generator.
- In the .j2, inject the literal at the top of the script-body section: `load_group = {{ load_group|tojson }}`.
- At every `wm.insert_new(...)`, `wm.mark_*(...)`, `wm.get_latest_watermark(...)`, `wm.get_recoverable_landed_run(...)` call, append `load_group=load_group`.
- The composite is a regular Python string in the rendered notebook; `tojson` ensures correct escaping for unusual flowgroup or pipeline names. Note: this template rendering does NOT yet enforce the `LHP-CFG-019` separator-collision check — that fires only in B2 (when `execution_mode: for_each` is set). For Tier 2, a Tier-2-only flowgroup (no B2) writes `load_group` even without that guard; if its `pipeline` or `flowgroup` field happens to contain `::`, no harm is done because Tier 2 doesn't enforce uniqueness yet either. **The B2 plan adds those guards as part of `_validate_for_each_invariants`.**

**Patterns to follow:**
- `logged_action_name = {{ action_name|tojson }}` literal injection at template line 80.
- Existing `template_context` dict shape lines 233–250.
- Existing `tojson` filter usage for safe Python-literal injection.

**Test scenarios:**
- Integration: generate a project with one `jdbc_watermark_v2` flowgroup `pipeline='bronze', flowgroup='customers_daily'` → assert generated extraction notebook contains `load_group = "bronze::customers_daily"`.
- Integration: assert generated notebook's `wm.insert_new(...)` call line contains `load_group=load_group`.
- Integration: assert generated notebook's `wm.get_latest_watermark(...)` call line contains `load_group=load_group`.
- Integration: per-env `test_per_env_catalog_substitution` extension — assert `load_group` literal lands identically across devtest/qa/prod (the composite is env-independent).

**Verification:**
- All integration tests pass.
- A diff of an existing extraction notebook before/after this unit shows only `load_group =` literal addition + `load_group=load_group` kwarg additions (no other behavioral changes).

---

- [ ] U4. **V1–V4 deep-clone validation Databricks notebook**

**Goal:** Operator can run `validate_tier2_load_group.py` against a deep-clone of `metadata.<env>_orchestration.watermarks` and have V1, V2, V3, V4 all pass before promoting Tier 2 to that env.

**Requirements:** R9

**Dependencies:** U2

**Files:**
- Create: `scripts/validation/validate_tier2_load_group.py` (Databricks notebook, mirrors `validate_td007_databricks.py`)
- Test: `tests/scripts/test_validate_tier2_load_group.py` (mock-Spark unit test for the notebook's helper functions; precedent: `tests/scripts/test_verify_databricks_bundle.py`)

**Approach:**
- Notebook header: standard Databricks notebook conventions (`# Databricks notebook source`), `# COMMAND ----------` cell separators, widgets for `env`, `probe_table_name` (default `watermarks_v1_probe`).
- Cell 1: deep-clone source `watermarks` table to probe table; abort if probe table already exists (operator must drop manually).
- Cell 2 (V1): insert two probe rows with same `(source_system_id, schema, table)` but different `load_group` values (`'lg_a'`, `'lg_b'`); assert `WatermarkManager(..., spark).get_latest_watermark(..., load_group='lg_a')` returns `lg_a`'s row, and `load_group='lg_b'` returns `lg_b`'s row.
- Cell 3 (V2): existing Tier-1 probe — insert a `'fake_federation'`-source row at `2099-01-01`; assert `get_latest_watermark(source_system_id='real_source', ...)` does not pick it up.
- Cell 4 (V3): assert `get_latest_watermark(..., load_group=None)` returns the `'legacy'`-backfilled row when one exists.
- Cell 5 (V4): apply Step 4a INSERT against the probe table for a synthetic `(source, schema, table, target_load_group)`; run a worker iteration in dry-run mode that calls `get_latest_watermark(..., load_group=target_load_group)` and assert (a) returned HWM equals the seeded `watermark_value`, (b) extraction_type would be `'incremental'`, (c) JDBC WHERE clause includes the seeded HWM.
- Cell 6: cleanup probe rows by isolated marker `run_id`s (not table-scoped DELETE); leave probe table for operator inspection.
- Final cell: emit `dbutils.notebook.exit(json.dumps({'V1': ..., 'V2': ..., 'V3': ..., 'V4': ...}))`.

**Patterns to follow:**
- `scripts/validation/validate_td007_databricks.py` — full notebook shape.
- `tests/scripts/test_verify_databricks_bundle.py` — mock-Spark unit-test pattern for notebook helpers.
- Tier 1 V1 probe shape from `docs/planning/tier-1-hwm-fix.md` — synthetic-row + isolated-run_id cleanup.

**Test scenarios:**
- Unit: V1 helper builds correct probe row dicts; assert SQL composition through `SQLInputValidator` + `sql_literal` (no string-concat).
- Unit: V4 helper Step 4a INSERT SQL matches the origin doc shape exactly (column list + ORDER BY + LIMIT 1).
- Unit: cleanup helper deletes only by isolated `run_id` markers, not by `WHERE source_system_id = ...` (table-scoped).

**Verification:**
- Notebook runs end-to-end against a dev deep-clone; final exit JSON shows all four checks pass.
- Unit tests for helpers pass.

---

- [ ] U5. **CLI helper `lhp seed-load-group`**

**Goal:** Operator runs `lhp seed-load-group --env devtest --flowgroup customers_daily.yaml` and gets per-table SELECT-preview + INSERT-seed SQL printed to stdout (or `--apply` to execute via Databricks SQL).

**Requirements:** R8

**Dependencies:** U2 (manager API for the kwarg) — but CLI helper does not import `WatermarkManager`; it composes SQL directly.

**Files:**
- Create: `src/lhp/cli/commands/seed_load_group_command.py` (new `BaseCommand` subclass)
- Modify: `src/lhp/cli/main.py` (register subcommand near line 443, mirror `sync-runtime` decorator pattern)
- Test: `tests/test_cli_seed_load_group.py`

**Approach:**
- Click signature: `lhp seed-load-group [--env <env>] [--flowgroup <yaml-path>] [--apply] [--dry-run] [--catalog <override>] [--schema <override>]`.
- Read flowgroup YAML via existing project-config helpers (mirror `validate_command.py`).
- Read env substitutions from project's `substitutions/<env>.yaml` to resolve `${watermark_catalog}` and `${watermark_schema}`; default to `metadata.<env>_orchestration` per ADR-004.
- For each `(source_system_id, schema, table)` in the flowgroup's `jdbc_watermark_v2` actions, emit:
  - SELECT preview: count, max watermark_time, max watermark_value for `load_group = 'legacy'`.
  - INSERT seed: the full SQL from origin doc Step 4a (UUID-based `run_id`, `extraction_type = 'seeded'`, ORDER BY tiebreakers, LIMIT 1).
- Composite `load_group` value: `f"{flowgroup.pipeline}::{flowgroup.flowgroup}"`.
- `--apply` executes via Databricks SQL Statement Execution API (use existing `databricks-sdk` if already imported by another command; otherwise emit SQL only and require operator to paste — pick during implementation).
- `--dry-run` is the default; explicit `--apply` is required to mutate.
- All composed values flow through `lhp_watermark.sql_safety.SQLInputValidator` before emission.
- Wrap in `cli_error_boundary`.

**Execution note:** Test-first. Write `tests/test_cli_seed_load_group.py` with `CliRunner().invoke(...)` against a fixture flowgroup YAML before implementing the command body.

**Patterns to follow:**
- `src/lhp/cli/commands/sync_runtime_command.py` — full `BaseCommand` subclass shape.
- `tests/test_cli_sync_runtime.py` — `CliRunner().invoke(cli, ["sync-runtime", "--check"])` test pattern.

**Test scenarios:**
- Happy path: fixture flowgroup with two `jdbc_watermark_v2` actions → command emits exactly two SELECT-preview blocks + two INSERT statements; SQL contains the composite `load_group` value.
- Edge case: flowgroup YAML with no `jdbc_watermark_v2` actions → command exits with informative message (no SQL emitted).
- Edge case: env not in `substitutions/` → command exits with `LHPConfigError` pointing to the missing file.
- Error path: malformed flowgroup YAML → command exits via `cli_error_boundary` with formatted error.
- Error path: `pipeline` or `flowgroup` field contains `::` literal → command warns (not fatal in Tier 2 — that guard ships in B2).
- Output: `--dry-run` (default) emits SQL only, never executes; `--apply` would execute (skip in unit test).
- Output: SQL is paste-ready against Databricks SQL — column list matches origin doc Step 4a verbatim.

**Verification:**
- `lhp seed-load-group --help` shows the command in main help.
- Fixture run against `Example_Projects/edp_lhp_starter/` produces expected SQL for known flowgroups.
- All test scenarios pass.

---

- [ ] U6. **Backfill ops runbook + pre-flight checks**

**Goal:** Operator has a single-document runbook for promoting Tier 2 through dev/qa/prod, with pre-flight `DESCRIBE DETAIL` capture, Step 3 backfill SQL, Step 3a OPTIMIZE SQL, and the Step 3-before-Step-4a precondition called out.

**Requirements:** R6, R7, R10

**Dependencies:** None for the doc itself; runs against U1+U2+U3 deployed code.

**Files:**
- Create: `docs/runbooks/tier-2-load-group-rollout.md` (new runbook)
- Modify: `docs/errors_reference.rst` (only if any new error codes ship — none in Tier 2; the file gets updates in B2 plan)
- Modify: `docs/planning/tier-2-hwm-load-group-fix.md` (add cross-reference to the new runbook, mark as scheduled-then-shipped)

**Approach:**
- Runbook structure: pre-flight checks → per-env Step 1 deploy → per-env Step 3 backfill → per-env Step 3a OPTIMIZE → per-env V1–V4 deep-clone → per-flowgroup Step 4a (helper) → post-flight verification.
- Pre-flight `DESCRIBE DETAIL metadata.<env>_orchestration.watermarks` capture per env: `partitionColumns` / `clusteringColumns` / `numFiles` / `minReaderVersion` / `minWriterVersion`. Document expected before-state (clustered on `(source_system_id, schema_name, table_name)` per ADR-004 §Implementation Status) and after-state (clustered on `(source_system_id, load_group, schema_name, table_name)` per Tier 2).
- Pre-flight `rg "extraction_type" --type py` audit. Document the new `'seeded'` enum value as additive only.
- Step 3 SQL: `UPDATE metadata.<env>_orchestration.watermarks SET load_group = 'legacy' WHERE load_group IS NULL;` — idempotent.
- Step 3a SQL: `OPTIMIZE metadata.<env>_orchestration.watermarks;` — once per env, after Step 3.
- Step 4a precondition: callout box stating "Step 3 must complete in this env before Step 4a runs for any flowgroup. Running Step 4a first silently inserts zero rows because the source `WHERE load_group = 'legacy'` matches nothing."
- `::` cosmetic note for log parsers.
- Operator-ordering footgun guard: explicit warning that Step 1 must run via `WatermarkManager.__init__` (any deploy that loads `lhp_watermark`); a deploy that just runs `lhp generate` without exercising the manager doesn't trigger the auto-DDL.

**Patterns to follow:**
- `docs/runbooks/devtest-validation-adr-003-phase-a-c.md` — full runbook shape, including provisioning matrix table.
- `docs/runbooks/lhp-runtime-deploy.md` — step-numbered deploy runbook.

**Test scenarios:**
Test expectation: none — pure documentation, no behavior change.

**Verification:**
- Runbook reviewed by code-quality agent or peer.
- Linked from `docs/planning/tier-2-hwm-load-group-fix.md` and from `docs/adr/ADR-004-watermark-registry-placement.md` (as the destructive-schema-change migration evidence).

---

- [ ] U7. **Test parameterization across `load_group` axis**

**Goal:** Existing `tests/lhp_watermark/` unit tests and `tests/test_jdbc_watermark_v2_integration.py` cover the four-cell load_group matrix without regressing Tier 1 cross-source isolation.

**Requirements:** R3, R4 (test coverage for both)

**Dependencies:** U2, U3

**Files:**
- Modify: `tests/lhp_watermark/test_get_latest.py` — parametrize `@pytest.mark.parametrize("load_group", [None, "legacy", "pipe_a::fg_a", "pipe_a::fg_b"])`.
- Modify: `tests/lhp_watermark/test_insert_new.py` — same parametrization.
- Modify: `tests/lhp_watermark/test_landed_recovery.py` — assert `get_recoverable_landed_run` honors `load_group` filter.
- Modify: `tests/lhp_watermark/test_mark_complete.py`, `test_mark_failed.py`, `test_mark_stage.py` — assert kwarg is store-only (no `WHERE` shape change for state-machine writes).
- Modify: `tests/test_jdbc_watermark_v2_integration.py` — add `_assert_load_group_threaded` helper; assert generated extraction notebook contains the composite literal.

**Approach:**
- Use existing `_RecordingSpark` / `_ScriptedSpark` mocks; do not introduce real-Spark dependency.
- For `get_latest_watermark`: probe rows in all four `load_group` values; assert isolation per `(load_group)` query.
- For state-machine writes: assert MERGE source row carries `load_group` value, but `WHERE` clause is unchanged (still `run_id` filter only).
- Tier 1 regression: `tests/lhp_watermark/test_get_latest.py` existing test cases (cross-source isolation) must still pass without modification.

**Patterns to follow:**
- `@pytest.mark.parametrize` precedent at `test_get_latest.py:145`, `test_insert_new.py:259`.
- `_RecordingSpark` / `_ScriptedSpark` helpers (duplicated per-file, do not centralize in this plan).

**Test scenarios:**
- Happy path: parametrized `load_group ∈ {None, 'legacy', 'pipe_a::fg_a', 'pipe_a::fg_b'}` × every method → all assertions pass.
- Edge case: `get_latest_watermark(load_group=None)` against probe rows mixing `NULL`, `'legacy'`, `'pipe_a::fg_a'` → returns NULL+legacy rows only.
- Edge case: `get_latest_watermark(load_group='pipe_a::fg_a')` against same set → returns only `'pipe_a::fg_a'` row.
- Regression: existing Tier 1 test cases continue to pass without modification.

**Verification:**
- `pytest tests/lhp_watermark/ -v` — all parametrized cases pass.
- `pytest tests/test_jdbc_watermark_v2_integration.py -v -m integration` — extension passes.
- Coverage report shows `load_group` lines covered in all `WatermarkManager` methods.

---

## System-Wide Impact

- **Interaction graph:** `WatermarkManager` is consumed by (a) `jdbc_watermark_job.py.j2` extraction notebook (per-flowgroup, every load_group threaded), (b) Tier 1 spike `prepare_manifest.py` notebooks (legacy callers, `load_group=None`, must continue to work), (c) any future B2 `prepare_manifest.py.j2` (B2 plan), (d) any future ManifestManager (B2 plan). Tier 2 keeps all four working via the back-compat kwarg + three-way SQL filter.
- **Error propagation:** No new exception types. `SQLInputValidator.string` rejects malformed `load_group` values via existing `WatermarkValidationError` (LHP-WM-003). DDL ALTER failures bubble up via the existing UTC-session + start-time-logging shape; no new error codes.
- **State lifecycle risks:** None. Tier 2 doesn't change the L2 §5.3 control flow (`insert_new` outside try; `mark_failed`-then-raise; `mark_complete` after). The `load_group` kwarg is store-only on state-machine writes; the state machine reads/writes by `run_id` exclusively.
- **API surface parity:** `lhp_watermark.__init__.py` exports unchanged. Method signatures gain one optional kwarg each — back-compat preserved for every existing call site.
- **Integration coverage:** Test parametrization (U7) covers the four-cell matrix; V1–V4 deep-clone notebook (U4) covers same-source-different-load_group isolation, Tier-1 cross-source regression, legacy fallback, and seed-then-incremental smoke. CLI helper (U5) covers per-flowgroup adoption.
- **Unchanged invariants:** ADR-002 namespace (`lhp_watermark` reused verbatim, no new exports). ADR-003 landing shape (Tier 2 doesn't touch landing paths). ADR-004 registry placement (still `metadata.<env>_orchestration.watermarks`). L2 §5.3 control flow. Terminal-state guard (`TerminalStateGuardError` propagation). SQL safety via `SQLInputValidator` pre-composition. UTC session on every DML.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| `_ensure_table_exists` ALTER fires unconditionally and causes metadata churn on every init | U1 conditional probe via `DESCRIBE TABLE` + `DESCRIBE DETAIL`; short-circuit when target shape already in place. Test scenario explicitly asserts zero ALTER SQL on already-target tables. |
| Per-env clustering switch is heavy rewrite + Delta protocol bump (qa/prod state unknown) | Pre-flight `DESCRIBE DETAIL` per env (U6 runbook); plan a maintenance window if partitioned-not-clustered. Devtest already liquid-clustered (ADR-004 §Implementation Status). |
| `get_latest_watermark(load_group=None)` returns wrong row set for mixed-NULL-and-`'legacy'` data after Step 3 backfill | The three-way clause emits both arms always; when `:load_group` is `NULL`, both arms collapse to `IS NULL` matches. Test scenario (U7) parametrizes against this exact set. |
| Operator runs Step 4a (per-flowgroup seed) before Step 3 (legacy backfill) → silent zero-row insert | U6 runbook callout. CLI helper (U5) surfaces a warning if it detects no `'legacy'` rows for the target `(source, schema, table)`. |
| `extraction_type='seeded'` value breaks an unknown consumer | Pre-flight `rg "extraction_type" --type py` audit (U6); confirmed audit passes today. New consumers added in the future must accept `'seeded'` as a valid value. |
| `WatermarkConcurrencyError` fires on ALTER under concurrent first-init from multiple notebooks | Acknowledged. Delta DDL is generally not retry-resilient under concurrent contention; in practice `WatermarkManager.__init__` is called once per cluster boot. If contention surfaces in prod, file as a follow-up. |
| Plan 2 (B2) cannot ship before this plan | Hard sequencing: B2 worker calls `get_latest_watermark(..., load_group)` — that signature lands here. B2 plan explicitly depends on this plan. |

---

## Documentation / Operational Notes

- Update `docs/planning/tier-2-hwm-load-group-fix.md` to mark scheduled-then-shipped and link the runbook.
- Capture migration evidence in a follow-up ADR (candidate ADR-005) per ADR-004 §"Open questions deferred". Defer until Tier 2 ships in qa.
- Recurring `OPTIMIZE` (weekly) + `VACUUM RETAIN 168 HOURS` (daily) policy on the watermarks registry is documented in Plan 2 OPS unit, not here.

---

## Sources & References

- **Origin document:** [docs/planning/tier-2-hwm-load-group-fix.md](../planning/tier-2-hwm-load-group-fix.md)
- Cross-reference: [docs/planning/tier-1-hwm-fix.md](../planning/tier-1-hwm-fix.md) (Tier 1, V1-proven 2026-04-24, commit `63bcfdb`)
- Cross-reference: [docs/planning/b2-watermark-scale-out-design.md](../planning/b2-watermark-scale-out-design.md) (B2 design baseline; this plan's downstream dependency)
- Cross-reference: [docs/adr/ADR-002-lhp-runtime-availability.md](../adr/ADR-002-lhp-runtime-availability.md), [docs/adr/ADR-003-landing-zone-shape.md](../adr/ADR-003-landing-zone-shape.md), [docs/adr/ADR-004-watermark-registry-placement.md](../adr/ADR-004-watermark-registry-placement.md)
- Related code: `src/lhp_watermark/watermark_manager.py:155-205` (`_ensure_table_exists` extension target), `:207-297` (`get_latest_watermark` filter target), `:299-351` (`get_recoverable_landed_run` filter target)
- Related code: `src/lhp/templates/load/jdbc_watermark_job.py.j2:73-77, :179-189, :283-297` (manager call sites in template)
- Related code: `src/lhp/cli/commands/sync_runtime_command.py` (CLI subcommand pattern)
- Related code: `scripts/validation/validate_td007_databricks.py` (V1–V4 notebook pattern)
