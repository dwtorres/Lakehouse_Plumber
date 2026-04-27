---
title: "fix(b2): P0/P1 deferred follow-ups (issues #14-#17)"
type: fix
status: active
date: 2026-04-26
deepened: 2026-04-27
origin: docs/plans/2026-04-25-002-feat-b2-watermark-scale-out-emission-plan.md
---

# fix(b2): P0/P1 deferred follow-ups (issues #14-#17)

## Overview

Resolve the four P0/P1 issues deferred from PR #24 (`feat/b2-watermark-scale-out` ce-code-review walk-through):

- **#14 P0** — DAB per-iteration retries broken at manifest claim WHERE clause
- **#15 P0** — `prepare_manifest` taskValue payload exceeds DAB 48 KB ceiling before LHP-CFG-033 300-action cap fires
- **#16 P1** — Pipelines with 2+ `execution_mode: for_each` flowgroups silently drop all but the first
- **#17 P1** — Error codes `LHP-MAN-*` and `LHP-VAL-04A/04B` violate project naming conventions

PR #24 already merged; these were filed as separate GitHub issues for author-decision triage. This plan executes the four agreed fixes.

---

## Problem Frame

The B2 watermark scale-out shipped (PR #13 merged, then ce-code-review walk-through PR #24 merged) with four documented defects/violations that require author decisions before fix:

1. **#14**: `derive_run_id` returns `job-{jobRunId}-task-{taskRunId}-attempt-{N}`. DAB per-iteration retry increments `attempt`. The manifest claim UPDATE WHERE clause `(worker_run_id IS NULL OR worker_run_id = {current})` cannot match a row claimed by attempt-0 when attempt-1 fires. Every DAB retry of the same iteration fails immediately at LHP-MAN-002. Runbook §4.3 advertises `max_retries`; implementation contradicts it.

2. **#15**: `prepare_manifest` serializes 10-key iteration entries (`source_system_id`, `schema_name`, `table_name`, `action_name`, `load_group`, `batch_id`, `manifest_table`, `jdbc_table`, `watermark_column`, `landing_path`) at ~350-550 bytes/entry. DAB 48 KB taskValue ceiling breaches at ~89-140 actions, well below the LHP-CFG-033 structural cap of 300. Existing 100-entry size-log test asserts ≤ 48 KB but the 300-entry test only checks render success, not payload size. The runtime byte log at line 277 fires AFTER `dbutils.jobs.taskValues.set()` so the operator never sees the size when `set()` itself throws.

3. **#16**: `_generate_workflow_resources` (`src/lhp/core/orchestrator.py:1830-1928`) keys `pipeline_name_map` by pipeline name only; the first for_each flowgroup encountered wins. When a second for_each flowgroup is added to the same pipeline (a routine refactor — split by source system), the orchestrator emits a single workflow YAML referencing only the first flowgroup's `__lhp_prepare_manifest_<fg>.py` and `__lhp_validate_<fg>.py`. The second flowgroup's aux files exist on disk (per-flowgroup emission is correct) but are never referenced. No validator catches this. Validate sees N expected / N completed → green. The second flowgroup's actions silently never execute.

4. **#17**: Per `CLAUDE.md` § Error Handling: error codes formatted as `LHP-{category}-{number}` where category is in `ErrorCategory` enum (`{CLOUDFILES, VALIDATION, IO, CONFIG, DEPENDENCY, ACTION, GENERAL, WATERMARK}`) and number is 3 digits. Two violations:
   - `LHP-MAN-001..004` use undeclared category `MAN`. Codes emit as runtime `RuntimeError("LHP-MAN-NNN: ...")` strings inside generated notebooks (not via `LHPError`).
   - `LHP-VAL-04A/04B` use alphanumeric suffix.
   - None of `LHP-MAN-001..004`, `LHP-VAL-04A/04B`, `LHP-VAL-048/049` appear in `docs/errors_reference.rst`. Operator searching the reference finds nothing.

---

## Requirements Trace

- **R1.** DAB per-iteration retries (`max_retries` advertised in runbook §4.3) must work end-to-end for the same B2 worker iteration after a failure on a prior attempt. (Issue #14)
- **R2.** Iteration #14 fix must preserve same-task-different-attempt **safety**: a failed attempt's row must be reclaimable by the next attempt of the same `(jobRunId, taskRunId)`, but a competing concurrent claim by a different worker must still be rejected. No silent ownership theft.
- **R3.** `prepare_manifest` must hard-fail at codegen time when projected per-action payload size × action count exceeds DAB 48 KB taskValue ceiling. (Issue #15 codegen guard)
- **R4.** `prepare_manifest` notebook must hard-fail at runtime, before `dbutils.jobs.taskValues.set()`, when the actual serialized payload exceeds 48 KB. (Issue #15 runtime guard — defense in depth)
- **R5.** `lhp validate` and bundle generation must reject any pipeline containing 2+ `execution_mode: for_each` flowgroups with a clear, actionable error code at codegen time. (Issue #16)
- **R6.** All error codes raised in B2 paths must conform to `LHP-{category}-{NNN}` format with category drawn from `ErrorCategory` enum. (Issue #17)
- **R7.** Every error code raised in B2 paths must have a corresponding entry in `docs/errors_reference.rst` with cause / common conditions / resolution / example. (Issue #17 docs)
- **R8.** A regression test must enforce R7 across future code additions — automated parity check between raise sites and reference catalog. (Preventative; structural fix to keep R7 satisfied long-term.)

---

## Scope Boundaries

- The remaining 6 P2/P3 deferred issues (#18-#23) are out of scope. They are author-triage architectural items; this plan executes only the P0/P1 set.
- The 3 plan deviations called out in PR #24 body (CFG-031..034 vs plan's 019/026/028 numbering, 10-key vs 7-key `B2_ITERATION_KEYS`, `MultiFlowGroupArray.flowgroups[]` `Workflow` `$ref` treatment) are **not** addressed here. The 10-key payload is now load-bearing for B2 worker correctness (Anomaly A regression fix); shrinking it would re-open the silent-data-corruption class. The CFG-031..034 numbering is stable and shipped.
- Devtest e2e validation of the existing fixture (`b2_smoke.yaml` manual run, R12 strict-`>` second-run duplicate check, `enableRowTracking` `DESCRIBE DETAIL`) is the operator-validation step gating PR-merge, not a code-change task. Tracked in PR #24 test plan checklist; not duplicated here.
- LHP CLI-level migration of `LHP-MAN-001..004` from `RuntimeError` strings to `LHPError` instances is **not** in scope. The codes emit inside generated Databricks notebooks where importing `LHPError` adds bootstrap weight. The MANIFEST enum addition makes the codes catalog-conformant; CLI-side codegen errors (e.g., new LHP-MAN-005 codegen-time size estimate from #15) use full `LHPError`, runtime template errors stay as `RuntimeError("LHP-MAN-NNN: ...")` prefix strings.
- Adding new B2 features (e.g., per-flowgroup workflow YAML for multi-for_each, parity check implementation behind LHP-VAL-049, manifest cleanup for ghost rows from cancelled batches) is out of scope. **Note on R5:** R5 adds a *validator* that rejects 2+ for_each flowgroups per pipeline at codegen time — this is a guard preventing the broken topology, NOT a feature implementing per-flowgroup workflow YAML. The two are distinct: this plan rejects the topology cleanly; a future plan could add Option B per-flowgroup workflow generation if real demand emerges.

---

## Context & Research

### Relevant Code and Patterns

- **#14 — Manifest claim WHERE clause sites:** `src/lhp/templates/load/jdbc_watermark_job.py.j2`
  - Line 232-240: claim UPDATE
  - Line 255-265: readback `SELECT worker_run_id`
  - Line 266-270: ownership guard raising LHP-MAN-002
  - Line 366-377: failure-mirror UPDATE (ownership predicate at `:369`)
  - Line 435-444: completion-mirror UPDATE (ownership predicate at `:438`)
  - All three SQL UPDATEs share the predicate `worker_run_id = {sql_literal(worker_run_id)}` which is the broken contract.
- **#14 — `derive_run_id` definition:** `src/lhp_watermark/runtime.py:97`. Returns `f"job-{job_run_id}-task-{task_run_id}-attempt-{attempt}"`. Must NOT change — `prepare_manifest.py.j2:120` uses it for `batch_id`, and changing the format breaks the documented batch_id contract.
- **#15 — Payload composition:** `src/lhp/templates/bundle/prepare_manifest.py.j2:248-263` (10-key iteration dict) + `:271` (`taskValues.set("iterations", json.dumps(_iterations))`) + `:277` (size log AFTER set, useless when set throws).
- **#15 — Iteration key contract:** `src/lhp/models/b2_iteration.py` — `B2_ITERATION_KEYS` is a 10-element `frozenset`. `tests/test_b2_iteration_contract.py` enforces symmetry via AST walk.
- **#15 — Codegen actions list:** `src/lhp/generators/load/jdbc_watermark_job.py:425-522` (`_emit_b2_aux_files`) builds the same 10-key actions list the template renders. Per-entry size estimation feasible at codegen because all field values are concrete strings at that point.
- **#15 — Existing size-aware test:** `tests/test_prepare_manifest_template.py:407-444` (`test_payload_size_logging_and_ceiling_headroom`) — 100-entry asserts ≤ 48 KB; 300-entry asserts render does not raise. Does **not** assert 300-entry payload fits.
- **#15 — LHP-CFG-033 structural cap:** `src/lhp/core/validator.py:383-403` — count-only, 300 actions per for_each flowgroup.
- **#16 — Orchestrator merge logic:** `src/lhp/core/orchestrator.py:1830-1928` (`_generate_workflow_resources`).
  - `:1865-1866` — `pipeline_name_map[pipeline_name] = flowgroup` first-wins keying.
  - `:1907-1913` — `merged_fg = FG(pipeline=pipeline_name, flowgroup=ref_fg.flowgroup, workflow=ref_fg.workflow, actions=v2_actions)` — first flowgroup metadata + all flowgroups' actions.
- **#16 — Project-scope validator pattern:** `src/lhp/core/validator.py:568-662` (`validate_project_invariants`) hosts CFG-032 (composite uniqueness) and the per-flowgroup CFG-033 facets. **CFG-034 mixed-mode rejection lives at `src/lhp/core/orchestrator.py:1887`** inside `_generate_workflow_resources`, NOT in `validator.py`. Both surfaces matter: `validate_project_invariants` is reachable from `lhp validate` (orchestrator.py:2088 inside `validate_pipeline_by_field`); `_generate_workflow_resources` is reachable from `lhp generate` (orchestrator.py:802 `generate_pipeline_by_field`). `lhp generate` does NOT call `validate_project_invariants` — it calls only `discover_all_flowgroups` and `validate_duplicate_pipeline_flowgroup_combinations`. Therefore a validator-only addition to `validate_project_invariants` would catch the multi-for_each topology at `lhp validate` time but bypass at `lhp generate` time, which is the path that ships pipelines.
- **#16 — Existing CFG-034 mixed-mode pattern:** Dual placement is the precedent. CFG-034 sits at `orchestrator.py:1887` (generate-time guard); CFG-032/033 sit at `validator.py:validate_project_invariants` (validate-time guard). The new CFG-036 must follow the same dual placement (orchestrator + validator) to cover both `lhp generate` and `lhp validate` entry points.
- **#17 — `ErrorCategory` enum:** `src/lhp/utils/error_formatter.py:10-21`. Current values: `CLOUDFILES`, `VALIDATION`, `IO`, `CONFIG`, `DEPENDENCY`, `ACTION`, `GENERAL`, `WATERMARK`. `WATERMARK = "WM"` precedent for adding `MANIFEST = "MAN"`.
- **#17 — Code emission sites (verified by ce-doc-review feasibility pass):**
  - `prepare_manifest.py.j2:226-230` (LHP-MAN-001 MERGE retry exhaustion, raised via `_on_merge_exhausted`)
  - `jdbc_watermark_job.py.j2:241-245` (LHP-MAN-001 claim retry exhaustion)
  - `jdbc_watermark_job.py.j2:262-265` (LHP-MAN-003 manifest row missing)
  - `jdbc_watermark_job.py.j2:266-270` (LHP-MAN-002 competing owner)
  - `jdbc_watermark_job.py.j2:440-444` (LHP-MAN-004 completion mirror exhaustion)
  - `validate.py.j2:108-111` (LHP-VAL-048 batch_id taskValue absent — already 3-digit, no rename)
  - `validate.py.j2:222` (LHP-VAL-04A validate failed — **needs rename**; this is the only remaining 04* alphanumeric raise site)
  - `validate.py.j2:227-236` (LHP-VAL-049 parity not implemented — already 3-digit, no rename)
  - **LHP-VAL-04B — confirmed DEAD in template.** The original parity-mismatch raise was superseded by LHP-VAL-049 (parity check not implemented). `tests/test_validate_template.py:528-532` explicitly asserts `"LHP-VAL-04B" not in rendered`. The runbook at `docs/runbooks/b2-for-each-rollout.md:134, 264` still references 04B as the future parity-check code — runbook entry stays but no template raise-site rename is needed.
- **#17 — VAL/CFG numbering availability (verified):**
  - `LHP-VAL-040..047` reserved by `src/lhp/core/validators/load_validator.py:185-238` comments for jdbc_watermark_v2 field validators (see `# LHP-VAL-040: landing_path is required`, `# LHP-VAL-041: watermark config required`, etc.).
  - `LHP-VAL-041` actively raised by `src/lhp/core/action_registry.py:159` and `src/lhp/core/config_field_validator.py:237` (`LHPValidationError` for "Removed load source type"). Re-using 041 for B2 would collide.
  - `LHP-VAL-048/049` used in `validate.py.j2`.
  - **Next free VAL slot for B2 rename: `LHP-VAL-050`.**
  - `LHP-CFG-035` used at `src/lhp/core/validator.py:450` (strict-comparison check).
  - **Next free CFG slot for U4: `LHP-CFG-036`.**
- **#17 — Test substring assertions:** `tests/test_validate_template.py:319-501`, `tests/test_jdbc_watermark_v2_for_each_worker.py:840`, `tests/test_prepare_manifest_template.py:481-488`.
- **#17 — Errors reference structure:** `docs/errors_reference.rst:625-924` — CFG-031..034 are precedent shape (cause / common conditions / resolution / example with `:caption: Before/After` YAML blocks). Categories overview table at lines 61-83.
- **#17 — Runbook troubleshooting table:** `docs/runbooks/b2-for-each-rollout.md:240-330`.

### Institutional Learnings

- `~/.claude/projects/-Users-dwtorres-src-Lakehouse-Plumber/memory/feedback_lhp_b2_iteration_contract.md` — B2 iteration-contract three-edit rule (`B2_ITERATION_KEYS`, prepare_manifest, jdbc_watermark_job in lockstep). Honored by leaving the 10-key payload intact (out of scope per Scope Boundaries) and routing #15 through size-bound guards instead of trimming keys.
- `docs/planning/b2-watermark-scale-out-design.md` (R1, R1a, R2) — `derive_run_id` contract; codegen-time count-bounds vs runtime byte-bounds split; `_merge_with_retry` / `execute_with_concurrent_commit_retry` is the canonical Delta DML retry helper.
- `docs/runbooks/b2-for-each-rollout.md` §"Internals: iteration contract" — operator-visible contract mirror; the LHP-MAN/LHP-VAL codes referenced here must match the rename.
- ce-code-review run `20260426-115040-b1c3456d` — `correctness.json:findings[0]` (P0 dup of #14), `adversarial.json:ADV-001` (P1 dup of #16), `adversarial.json:ADV-002` (P0 dup of #14), `reliability.json:RR-001` (residual on #15 byte-cap), `project-standards.json:PS-001/PS-002` (#17 root findings). All confirm the issue diagnosis and recommended fix shape.

### External References

External research not pursued. Codebase has strong direct precedent for every fix:

- #14: existing optimistic-concurrency claim pattern in same file — change is to the WHERE predicate only.
- #15: existing `LHP-CFG-033` 300-cap pattern in validator; `print(...payload bytes)` already rendered; defense-in-depth is the documented design intent.
- #16: existing LHP-CFG-034 mixed-mode rejection in same `validate_project_invariants` function — exact sibling.
- #17: existing `WATERMARK = "WM"` enum addition is the precedent for `MANIFEST = "MAN"`; existing CFG-031..034 entries are the precedent shape for new reference doc entries.

### Slack context

Not gathered (not requested).

---

## Key Technical Decisions

- **#14 — Status-based reclaim** (loosen claim WHERE to accept reclaim when row's `execution_status IN ('pending','failed')`).
  *Rationale:* Smallest semantic change. Keeps `worker_run_id` ownership for `running` rows (a row in flight cannot be stolen by another attempt). Allows the next DAB attempt to reclaim a row whose previous attempt left it in `failed` status. Matches the issue's option #1. Doesn't change `derive_run_id` semantics (which `batch_id` depends on). Adversarial-reviewer-recommended.
- **#15 — Defense in depth: codegen-time + runtime guards.**
  *Rationale:* Codegen-time check (R3) catches misconfigurations at `lhp generate`/`lhp validate` time so the operator sees the failure pre-deploy with a clear error. Runtime check (R4) is the last-line defense for any payload growth between codegen and run (e.g., manifest_table identifier change at deploy time). RR-001 (reliability review) explicitly recommends runtime hard guard. Both are cheap.
- **#16 — Option A: project-scope validator rejects 2+ for_each per pipeline.**
  *Rationale:* Aligns with LHP grain (validators reject ambiguity, codegen stays boring). Sibling of LHP-CFG-034 mixed-mode rejection. ~30 LOC + 1 test vs ~200 LOC orchestrator restructure for option B (per-flowgroup workflows). CLAUDE.md §2 "Min code solve problem"; §3 "Touch only what must"; "No flexibility not requested" — multi-for_each-per-pipeline is not a documented feature. Reversible: if real demand emerges, option B is additive.
- **#17 — Add `ErrorCategory.MANIFEST = "MAN"` + renumber `VAL-04A → VAL-050`.**
  *Rationale:* Mirrors `WATERMARK = "WM"` precedent. Smallest net change. Keeps existing `LHP-MAN-001..004` codes (no operator-visible token churn for already-shipped runtime errors). **VAL-040..047 are reserved-by-comment** for jdbc_watermark_v2 field validators in `load_validator.py:185-238`, and **VAL-041 is actively raised** by `action_registry.py:159` and `config_field_validator.py:237` for "Removed load source type" errors — using VAL-040 or VAL-041 for B2 would collide. **VAL-048/049 used.** Skip the entire 040-049 range and use **VAL-050** (next free slot) for the B2 validate-failed rename. LHP-VAL-04B is dead in the template (test asserts not-present); its runbook reference stays as future-parity placeholder, no template rename needed.
- **#16 dual placement.** CFG-036 must live at BOTH `orchestrator.py:1887` (generate-time guard, sibling of CFG-034) AND `validator.py:validate_project_invariants` (validate-time guard, sibling of CFG-032). Single-site placement at validator only would bypass `lhp generate`. See Context & Research §"#16 Project-scope validator pattern" for the call-graph rationale.
- **Runtime template emissions stay as `RuntimeError("LHP-XXX-NNN: ...")` prefix strings, not `LHPError` instances.**
  *Rationale:* Templates render into Databricks notebook source. Importing `LHPError` adds bootstrap weight (the `_lhp_watermark_bootstrap_syspath` shim already exists for the watermark module; spreading it further is friction). The MANIFEST enum addition gives the codes catalog-conformant backing for any CLI-side raise (e.g., codegen-time LHP-MAN-005 from R3). Operator-visible behavior (the `LHP-MAN-NNN: ...` prefix in stderr) is unchanged.

---

## Open Questions

### Resolved During Planning

- **#14 fix selection**: status-based reclaim chosen over clear-NULL-on-fail-mirror or jobRunId+taskRunId-prefix-match (user decision).
- **#14 reclaim status terms**: only `'failed'` added as new disjunct, NOT `'pending'`. Delta UPDATE is atomic — the claim UPDATE sets `worker_run_id` AND `execution_status='running'` in the same statement, so a row with `execution_status='pending'` necessarily has `worker_run_id IS NULL` (already covered by existing NULL arm). Adding 'pending' would be redundant; worse, if any future code path violated atomicity it could allow a competing worker on a different job_run to claim a `pending` row whose worker_run_id was set by a concurrent in-flight claim. Restrict to `'failed'` only — the actual bug being fixed.
- **#15 guard placement**: dual-layer (codegen + runtime), not single-layer (per RR-001 recommendation + general LHP codegen-vs-runtime split).
- **#16 strategy**: Option A reject (user decision).
- **#16 placement**: dual placement (orchestrator.py:1887 sibling of CFG-034 + validator.py:validate_project_invariants sibling of CFG-032). Validator-only would bypass `lhp generate` (verified: generate_pipeline_by_field at orchestrator.py:802 does not call validate_project_invariants).
- **#17 strategy**: Add MANIFEST enum + rename VAL-04A → VAL-050 (user decision; numbering revised after collision check).
- **#17 numbering**: VAL-040..047 reserved by load_validator comments; VAL-041 actively raised by action_registry/config_field_validator (collision); VAL-048/049 used. **Skip 040-049, use VAL-050** for B2 validate-failed rename. LHP-VAL-04B is dead in template (`tests/test_validate_template.py:528-532` asserts not-present); no template rename needed. Runbook reference stays as future-parity placeholder.
- **#17 max_retries:0 for prepare_manifest task**: U3 must verify and enforce in `workflow_resource.yml.j2` so a payload-too-large LHP-MAN-005 runtime failure does NOT consume DAB retry budget repeating the same deterministic failure. Promoted from deferred-implementation to required U3 file modification.
- **New error code numbering**: LHP-MAN-005 (R3 codegen-time size guard, U3); LHP-CFG-036 (R5 multi-for_each rejection, U4); LHP-VAL-050 (R6 rename of VAL-04A, U2).
- **Runtime LHP-MAN-* migration to LHPError**: not in scope (see Scope Boundaries rationale).

### Deferred to Implementation

- **Per-entry size estimation method (U3 codegen-time):** measure actual `json.dumps({...})` bytes per concrete action. Use a representative `batch_id` placeholder string of **64 bytes** (covers `derive_run_id` worst-case `job-{N}-task-{N}-attempt-{N}` with 10-digit run IDs) and the concrete `manifest_table` 3-part identifier resolved from `wm_catalog`/`wm_schema`/`b2_manifests` available in `prepare_context`. The 8 statically-injected per-action keys come from `_emit_b2_aux_files`'s actions_list (per `src/lhp/generators/load/jdbc_watermark_job.py:474-487`). Multiply summed payload by ~1.10 fudge (5% JSON array overhead + 5% identifier-substitution drift cushion).
- **Validator entry-point concreteness:** `_validate_for_each_invariants` runs after `FlowGroupTemplateExpander` per the existing CFG-033 (300-action) check. Confirm at U3 implementation start by tracing the call stack from `validate_flowgroup` → `_validate_for_each_invariants` and printing one action's `landing_path`/`jdbc_table` to verify they are concrete strings, not `${var}` placeholders.
- **Test fixture realism for #15 boundary tests:** "realistic production field lengths" per the issue body. Implementation will use `source_system_id`, `schema_name`, `table_name` lengths drawn from existing `b2_smoke.yaml` fixture or representative production-shape strings. Specifics chosen at test-write time.
- **Concurrent-claim contention test for U1:** add a test simulating two concurrent attempts both observing `execution_status='failed'`, both routed through `execute_with_concurrent_commit_retry`. Assert exactly one wins and the other raises LHP-MAN-002. (Promoted from residual risk.)

---

## Implementation Units

- [ ] **U1. Fix #14: status-based reclaim in B2 worker manifest UPDATEs**

**Goal:** Enable DAB per-iteration retries to reclaim a manifest row when the prior attempt left it in `pending` or `failed` state. Preserve ownership rejection for rows still `running`.

**Requirements:** R1, R2

**Dependencies:** None.

**Files:**
- Modify: `src/lhp/templates/load/jdbc_watermark_job.py.j2` (claim WHERE at ~:239; readback ownership guard at ~:266-270; failure-mirror WHERE at ~:369; completion-mirror WHERE at ~:438)
- Test: `tests/test_jdbc_watermark_v2_for_each_worker.py` (add same-task-attempt-N+1 reclaim scenario)

**Approach:**
- Claim WHERE clause: change `(worker_run_id IS NULL OR worker_run_id = {current})` to `(worker_run_id IS NULL OR worker_run_id = {current} OR execution_status = 'failed')`. The new `'failed'` term covers retry-after-error (the actual #14 bug); existing NULL arm covers initial seed + retention-stranded; existing equality arm covers idempotent re-claim within the same attempt. **`'pending'` is intentionally NOT added** — Delta UPDATE atomicity guarantees a `pending` row has `worker_run_id IS NULL` (already covered by NULL arm); adding `'pending'` as a standalone status term would be redundant and risk admitting cross-job-run claims under hypothetical atomicity violations.
- Readback ownership guard: keep existing `if readback[0]["worker_run_id"] not in (None, worker_run_id)` raising LHP-MAN-002. Status-based reclaim happens via the UPDATE; if the UPDATE didn't take ownership, the readback still rejects — correct and unchanged.
- Failure-mirror WHERE (line 369): keep `worker_run_id = {worker_run_id}` ownership predicate. After the claim UPDATE in U1, the row's `worker_run_id` is the current attempt's run_id, so the failure mirror correctly addresses this worker's claimed row. **Verification at implementation:** trace the SQL trail of attempt-1 reclaiming an attempt-0-failed row: (1) claim WHERE matches via `'failed'` term; (2) claim UPDATE sets `worker_run_id=run_id_1, status='running'` atomically; (3) JDBC fails; (4) fail-mirror WHERE matches via `worker_run_id=run_id_1`; (5) row transitions to `status='failed'` owned by run_id_1. All 5 steps must be asserted in the new test scenario.
- Completion-mirror WHERE (line 438): same reasoning. Trace the success path: claim → JDBC succeeds → mark_complete → completion-mirror WHERE matches via `worker_run_id=run_id_current`. Assert in the same integration test.

**Execution note:** Test-first. Add the failing same-task-attempt-N+1 reclaim test before changing the WHERE clause.

**Patterns to follow:**
- Existing `_FakeDbutils` harness in `tests/test_jdbc_watermark_v2_for_each_worker.py:140-157` accepts `run_id` and parses `attempt` from the suffix — same-task-different-attempt scenarios are testable without changing the harness.
- Existing `test_b2_runtime_competing_owner_raises` (`tests/test_jdbc_watermark_v2_for_each_worker.py:809`) is the template for ownership-rejection assertions.

**Test scenarios:**
- *Happy path — initial claim:* manifest row exists with `worker_run_id=NULL, execution_status='pending'` (just MERGE'd). Worker attempt-0 claim WHERE matches via `worker_run_id IS NULL`. Row updates to `worker_run_id=run_id_0, execution_status='running'`. Readback owner = `run_id_0`. No raise.
- *Happy path — same-attempt idempotent re-claim:* row already `worker_run_id=run_id_0, execution_status='running'` (e.g., spark retry mid-task). Claim WHERE matches via `worker_run_id = run_id_0`. UPDATE is a no-op set. Readback owner = `run_id_0`. No raise.
- *Edge case — DAB attempt-1 reclaims attempt-0-failed row (R1, the bug being fixed).* Manifest row: `worker_run_id=run_id_0, execution_status='failed'` (attempt-0 failed and fail-mirror ran). Worker attempt-1 fires with `run_id_1` (different attempt suffix). Claim WHERE matches via `execution_status = 'failed'` term. Row updates to `worker_run_id=run_id_1, execution_status='running'`. Readback owner = `run_id_1`. No raise. **Without the fix, this scenario raises LHP-MAN-002.**
- *Edge case — DAB attempt-1 reclaims attempt-0-pending row.* Manifest row: `worker_run_id=NULL, execution_status='pending'` because attempt-0 crashed before claim UPDATE. Attempt-1 claim WHERE matches via `worker_run_id IS NULL`. Same as initial claim path; no raise.
- *Error path — competing owner on running row:* row already `worker_run_id=run_id_OTHER, execution_status='running'` (concurrent worker holds claim). Current worker's claim WHERE matches **none** of the OR terms (worker_run_id not NULL, not equal to current, status='running' is not 'failed'). UPDATE matches 0 rows. Readback owner = `run_id_OTHER`, not in `(None, run_id_current)`. Raises LHP-MAN-002. **Critical: this case must NOT regress with the fix.**
- *Error path — competing owner on already-completed row:* row `worker_run_id=run_id_OTHER, execution_status='completed'`. Claim WHERE matches none. UPDATE matches 0 rows. Readback raises LHP-MAN-002. Status-based reclaim must NOT admit `'completed'` rows — the `'failed'` term is exact, not `IN`.
- *Error path — manifest row missing:* claim UPDATE matches 0 rows. Readback returns empty list. Raises LHP-MAN-003. Unchanged.
- *Concurrent-claim contention (R2 safety regression guard):* simulate two attempts both observing `execution_status='failed'` on the same row. Both submit claim UPDATEs through `execute_with_concurrent_commit_retry`. Delta optimistic concurrency: one commits first (status→running, owner=winner_id); the other's commit conflicts and retries; second pass observes `status='running'` (no longer `'failed'`) and `worker_run_id != current` — claim WHERE matches zero rows. Readback returns winner_id. Loser raises LHP-MAN-002. Assert exactly one winner.
- *Integration scenario — full attempt-0-fails, attempt-1-succeeds flow (5-step SQL trace):* (1) attempt-0 claims via NULL arm (`worker_run_id=run_id_0, status=running`). (2) JDBC fails. (3) Fail-mirror UPDATE matches `worker_run_id=run_id_0` → `status=failed`. (4) Attempt-1 claim WHERE matches via `status='failed'` term, UPDATE sets `worker_run_id=run_id_1, status=running`. (5) JDBC succeeds, completion-mirror UPDATE matches `worker_run_id=run_id_1` → `status=completed`. Final manifest row owned by attempt-1, terminal-completed.
- *Integration scenario — attempt-0-fails, attempt-1-also-fails:* both attempts go through claim → fail-mirror correctly. Attempt-1's fail-mirror matches its own `worker_run_id=run_id_1`. Final state: `worker_run_id=run_id_1, execution_status='failed'`. Validate task sees row terminal-failed.

**Verification:**
- New same-task-attempt-N+1 reclaim test passes.
- `test_b2_runtime_competing_owner_raises` still passes (no regression in ownership-rejection guard).
- Full B2-scoped suite passes (all 193 from PR #24).

---

- [ ] **U2. Fix #17a: ErrorCategory.MANIFEST + rename VAL-04A → VAL-050**

**Goal:** Bring B2 error codes into compliance with the `LHP-{category}-{NNN}` convention. Add the missing `MANIFEST` category to the enum so existing LHP-MAN-* codes are catalog-conformant. Rename alphanumeric `LHP-VAL-04A` to `LHP-VAL-050` (next free numeric slot — 040..049 all taken/reserved). LHP-VAL-04B is dead in the template; runbook reference stays as future-parity placeholder, no template rename needed.

**Requirements:** R6

**Dependencies:** None. (U2 must precede U3 because U3 introduces a new MAN-005 code that depends on the MANIFEST enum value.)

**Files:**
- Modify: `src/lhp/utils/error_formatter.py` (add `MANIFEST = "MAN"` to `ErrorCategory` enum)
- Modify: `src/lhp/templates/bundle/validate.py.j2` (rename `LHP-VAL-04A` → `LHP-VAL-050` at raise site line 222 AND at module-level header comment line 29 — both occurrences)
- Modify: `tests/test_validate_template.py` (update `\bLHP-VAL-04A\b` substring assertions to `LHP-VAL-050`; lines 8, 320, 336, 354, 370, 401, 503; the `tests/test_validate_template.py:528-532` regression test asserting `"LHP-VAL-04B" not in rendered` stays as-is — that's the canonical proof 04B is dead)
- Test: `tests/test_validate_template.py` (add a one-line guard test asserting the rendered notebook contains `LHP-VAL-050` and not `LHP-VAL-04A`)
- Modify: `docs/runbooks/b2-for-each-rollout.md` (rename the `LHP-VAL-04A` row at line 263 in the troubleshooting table to `LHP-VAL-050`; **leave the `LHP-VAL-04B` row at line 264 unchanged** — it documents the future parity-check code, currently superseded by LHP-VAL-049)

**Approach:**
- Add `MANIFEST = "MAN"` immediately after `WATERMARK = "WM"` in the enum (preserve declaration order; the precedent for adding a category is positional-trailing).
- Rename in `validate.py.j2`: replace `LHP-VAL-04A` with `LHP-VAL-050` everywhere in the file (raise site at line 222 AND module-level header docstring at line 29). Use anchored regex `\bLHP-VAL-04A\b` for sweeps to avoid false matches against unrelated tokens (e.g., hex literals).
- Test substring updates: anchor regex sweep `rg '\bLHP-VAL-04A\b' tests/ src/ docs/` to find all sites; replace with `LHP-VAL-050`. Final state: zero `\bLHP-VAL-04A\b` matches; the `"04B"` regression test (`test_validate_template.py:528-532`) intentionally remains.
- LHP-MAN-001..004 codes themselves are **unchanged** — they were already valid 3-digit suffixes; they were just missing the enum-backed category. Now backed by `ErrorCategory.MANIFEST`.
- LHP-VAL-048 / LHP-VAL-049 codes are also **unchanged** — already 3-digit. They get reference doc entries in U5 but no rename here.

**Execution note:** Order: (a) enum addition, (b) test addition asserting rendered notebook contains `LHP-VAL-050`, (c) template rename (line 222 + line 29 comment), (d) test substring updates with anchored regex, (e) runbook line 263 rename. The "anchored regex" guidance prevents the substring-sweep corruption hazard.

**Patterns to follow:**
- `WATERMARK = "WM"` enum addition is the direct precedent for `MANIFEST = "MAN"`.
- `errors_reference.rst` Categories overview table at lines 61-83 — U5 will add MANIFEST row here.

**Test scenarios:**
- *Happy path — enum addition:* `ErrorCategory.MANIFEST.value == "MAN"`. `LHPError(category=ErrorCategory.MANIFEST, code_number="005", ...).code == "LHP-MAN-005"`.
- *Happy path — rendered notebook code rename:* render `validate.py.j2` with a representative for_each context. Assert rendered text contains `LHP-VAL-050` and does NOT contain `LHP-VAL-04A` (anchored). The existing test `test_validate_template.py:528-532` continues to assert `LHP-VAL-04B` not present.
- *Edge case — existing LHP-MAN-001..004 strings still appear in rendered notebooks:* render `prepare_manifest.py.j2` and `jdbc_watermark_job.py.j2`. Assert `LHP-MAN-001`, `LHP-MAN-002`, `LHP-MAN-003`, `LHP-MAN-004` still appear at their respective sites (unchanged codes, guards against accidental rename).
- *Test maintenance — substring assertions sweep:* anchored regex `rg '\bLHP-VAL-04A\b' tests/ src/ docs/` finds every site; rewrite to `LHP-VAL-050`. Final state: zero `\bLHP-VAL-04A\b` matches; `LHP-VAL-04B` references intentionally retained at runbook:134, runbook:264, and `test_validate_template.py:528-532`.

**Verification:**
- `ErrorCategory.MANIFEST` exists and has value `"MAN"`.
- Rendered notebook artifacts contain `LHP-VAL-050` and not `LHP-VAL-04A`.
- `rg '\bLHP-VAL-04A\b' src/ tests/ docs/runbooks/` returns zero matches.
- `LHP-VAL-04B` references at runbook:134, runbook:264, and `test_validate_template.py:528-532` remain (intentional — future-parity placeholder + regression guard).
- Full repo test suite passes (currently 3942/3942 per PR #24).

---

- [ ] **U3. Fix #15: dual-layer taskValue payload size guards**

**Goal:** Reject for_each flowgroups whose projected `prepare_manifest` taskValue payload would exceed the DAB 48 KB ceiling, both at codegen time (clear pre-deploy error) and at runtime (defense in depth).

**Requirements:** R3, R4

**Dependencies:** U2 (uses `ErrorCategory.MANIFEST` for codegen-time error; runtime guard uses RuntimeError prefix string per Key Technical Decisions).

**Files:**
- Modify: `src/lhp/templates/bundle/prepare_manifest.py.j2` (insert hard runtime guard immediately before `dbutils.jobs.taskValues.set(key="iterations", ...)` at line 271; new code `LHP-MAN-005`. The `taskValues.set(key="batch_id", ...)` at line 272 stays as-is — `batch_id` is a scalar string, well under any ceiling)
- Modify: `src/lhp/core/validator.py` (add codegen-time per-action size estimation in `_validate_for_each_invariants` near LHP-CFG-033 at lines 383-403; new code `LHP-MAN-005` raised via `LHPError(category=ErrorCategory.MANIFEST, code_number="005", ...)`)
- Modify: `src/lhp/templates/bundle/workflow_resource.yml.j2` (set `max_retries: 0` for the prepare_manifest task. Promoted from deferred-implementation per coh-004 / ADV-002: a payload-too-large LHP-MAN-005 failure is deterministic — every retry computes the same batch_id, MERGEs no-op rows, hits the same size limit, fails again. DAB retry of this specific failure mode wastes budget without progress. Verify the prepare_manifest task in the rendered DAB workflow YAML declares `max_retries: 0`)
- Modify: `tests/test_prepare_manifest_template.py:407-444` (extend `test_payload_size_logging_and_ceiling_headroom` to assert hard raise on 300-entry render with realistic field lengths exceeding 48 KB)
- Test: `tests/test_prepare_manifest_template.py` (add new test for runtime guard: render 300-entry notebook, run, assert `RuntimeError` containing `LHP-MAN-005` and the actual byte count; assert `dbutils.jobs.taskValues.set` for key="iterations" was NOT called)
- Test: `tests/test_config_validator_for_each.py` (add new tests at boundaries N=80, N=100, N=140 with realistic per-action field lengths; assert validator raises LHP-MAN-005 when projected payload exceeds 48 KB; assert no raise when projected payload fits)
- Test: `tests/` (add a test rendering `workflow_resource.yml.j2` and asserting the prepare_manifest task block declares `max_retries: 0`)

**Approach:**
- **Codegen-time guard** in `_validate_for_each_invariants` (per `validator.py:383-403` neighborhood):
  - For each post-expansion concrete action in the for_each flowgroup, build the same 10-key dict the template will render (`{"source_system_id": ..., ..., "landing_path": ...}`).
  - The 8 statically-injected per-action keys come directly from `_emit_b2_aux_files`'s `actions_list` at `src/lhp/generators/load/jdbc_watermark_job.py:474-487`. The 2 runtime-derived keys are `batch_id` and `manifest_table`:
    - `batch_id`: at codegen this is unknown (`derive_run_id(dbutils)` runtime). Use a **64-byte placeholder** to model worst-case `f"job-{N}-task-{N}-attempt-{N}"` with 10-digit run IDs.
    - `manifest_table`: resolve from `prepare_context`'s `wm_catalog`/`wm_schema` plus literal `b2_manifests` (e.g., `devtest_edp_watermarks.public.b2_manifests`).
  - Compute `projected_payload_bytes = len(json.dumps([per_action_dict, ...])) * 1.10` (5% JSON-array overhead + 5% identifier-substitution drift cushion). Strict 5% headroom is insufficient — the dual-cushion 10% factor accounts for batch_id placeholder uncertainty (see ADV-001 finding).
  - Compare against `48 * 1024`. If exceeds, raise `LHPError(category=MANIFEST, code_number="005", title=..., details=f"projected taskValue payload {projected_payload_bytes} bytes exceeds DAB 48 KB ceiling for {len(actions)} actions; reduce action count or shorten field values", suggestions=["Trim action count", "Shorten landing_path / jdbc_table identifiers"])`.
- **Runtime guard** in `prepare_manifest.py.j2` immediately before line 271 (the `taskValues.set("iterations", ...)` call):
  - `_payload = json.dumps(_iterations)` (replaces both the implicit set-time serialization and the line 277 log-time serialization — pull up).
  - `if len(_payload) > 48 * 1024: raise RuntimeError(f"LHP-MAN-005: taskValue payload {len(_payload)} bytes exceeds DAB 48 KB ceiling for {len(_iterations)} actions. Reduce action count or trim per-entry fields.")`
  - Move the `print(f"manifest entries: ..., taskvalue payload bytes: ...")` log BEFORE the size guard so operators see the size on both pass and fail paths.
  - Then `dbutils.jobs.taskValues.set("iterations", _payload)` (reuse the already-computed `_payload` string).
  - The line 272 `dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)` is unchanged — `batch_id` is small.
- **DAB retry suppression**: `workflow_resource.yml.j2` sets `max_retries: 0` for the prepare_manifest task. LHP-MAN-005 is deterministic and not retry-recoverable (same input → same overflow → same failure). DAB task-level retry would burn budget without progress and pollute alerting. Other tasks in the same workflow keep their existing retry policy.

**Execution note:** Test-first for both layers. Write the failing codegen-time test (validator raises LHP-MAN-005) and the failing runtime test (`runpy.run_path` of rendered notebook raises with `LHP-MAN-005` prefix and asserts no `taskValues.set` for "iterations") before implementing the guards. Verify the actions-list concreteness assumption (post-`FlowGroupTemplateExpander`, no `${var}` placeholders) at U3 implementation start by tracing the call stack from `validate_flowgroup` and printing one action's `landing_path`.

**Patterns to follow:**
- `LHP-CFG-033` 300-action structural cap in `validator.py:383-403` is the placement and shape model for the codegen-time guard.
- `_on_merge_exhausted` callback shape at `prepare_manifest.py.j2:225-230` is the model for the runtime `RuntimeError("LHP-MAN-NNN: ...")` raise.
- `tests/test_prepare_manifest_template.py:407-444` (`test_payload_size_logging_and_ceiling_headroom`) is the model for both the codegen-time fixture and the runtime size-assertion test.

**Test scenarios:**
- *Happy path — codegen guard, fits:* validator runs against a flowgroup with 50 actions at moderate field lengths; projected payload < 48 KB; no raise.
- *Happy path — runtime guard, fits:* render notebook with 50 actions at moderate field lengths; run via `runpy.run_path`; `dbutils.jobs.taskValues.set` called once with `key="iterations"` and a payload < 48 KB; no raise.
- *Edge case — codegen guard, exact boundary:* validator runs against a flowgroup whose projected payload is 48 * 1024 - 1 bytes; no raise. At 48 * 1024 + 1 bytes; raises LHP-MAN-005 with `projected payload N bytes` in details.
- *Error path — codegen guard, 300 actions × realistic 415 bytes/entry:* validator rejects with LHP-MAN-005 even though LHP-CFG-033 (count cap) would pass. Asserts the size guard fires before the count guard, and that the error message names the projected byte count.
- *Error path — codegen guard, N=140 with realistic 350-byte entries:* boundary regression test — projected payload ~49 KB, just over ceiling. Raises LHP-MAN-005.
- *Error path — runtime guard, 300 actions × actual 415 bytes/entry:* render notebook (codegen guard would have fired in real `lhp generate`; but in this test we render directly, bypassing validator). Run via `runpy`. Assert `RuntimeError` with `LHP-MAN-005:` prefix + byte count in message + `manifest entries: 300, taskvalue payload bytes: ...` log line emitted on stderr/stdout. **`dbutils.jobs.taskValues.set` is NOT called.**
- *Edge case — runtime guard happens AFTER MERGE:* runtime guard fires after the manifest MERGE has already committed rows. Operator-visible state: rows exist in `b2_manifests` for this `batch_id`; downstream worker tasks never receive `iterations` taskValue and fail at validate task (LHP-VAL-048 batch_id taskValue absent — already-existing code). Test asserts the manifest table has the rows but no `iterations` taskValue is set. With `max_retries: 0` on prepare_manifest, the task fails once, not 4-5 times.
- *Integration scenario — codegen + runtime cooperate (deploy-time substitution divergence):* if `manifest_table` or `landing_path` contains `${var}` substitution syntax, the codegen estimate uses the unresolved-token length. Substitution typically happens upstream of `_validate_for_each_invariants` (post-FlowGroupTemplateExpander), so this case should not occur in practice. The 10% drift cushion plus the runtime guard catch any rare overshoot. Synthesize a fixture where codegen estimate is within 90% of ceiling but runtime renders bytes over it (uses a deliberately late-bound identifier in the test fixture; demonstrates the value of the second layer).
- *DAB retry suppression:* render `workflow_resource.yml.j2` for a representative for_each pipeline. Assert the prepare_manifest task block declares `max_retries: 0`. Other tasks (worker, validate) retain their existing retry policy.

**Verification:**
- `lhp validate` rejects a 300-action for_each flowgroup at realistic field lengths with LHP-MAN-005, before bundle generation.
- Generated `prepare_manifest.py.j2` rendered notebook hard-raises LHP-MAN-005 if payload exceeds 48 KB at runtime.
- Existing `test_payload_size_logging_and_ceiling_headroom` 100-entry sub-case still passes.
- 300-entry sub-case is updated to assert the runtime raise path (or split into two tests: render-ok vs runtime-fails).

---

- [ ] **U4. Fix #16: project-scope validator rejects 2+ for_each flowgroups per pipeline**

**Goal:** At `lhp validate` / `lhp generate` time, reject any project containing a pipeline with two or more `execution_mode: for_each` flowgroups, with a clear LHP-CFG-036 error pointing the operator to either consolidate the flowgroups or split into separate pipelines.

**Requirements:** R5

**Dependencies:** None. (Independent of U1-U3, U5.)

**Files:**
- Modify: `src/lhp/core/validator.py` (add new project-scope check `_validate_for_each_per_pipeline_uniqueness` invoked from `validate_project_invariants` at lines 568-662; new code `LHP-CFG-036`. Validate-time placement, sibling of CFG-032/033)
- Modify: `src/lhp/core/orchestrator.py` (add the same `LHP-CFG-036` check inside `_generate_workflow_resources` near line 1887, sibling to the existing CFG-034 mixed-mode raise. Generate-time placement, ensures `lhp generate` cannot bypass the guard. **Verified gap** via ce-doc-review feasibility pass: `validate_project_invariants` is called only from `validate_pipeline_by_field` (orchestrator.py:2088), NOT from `generate_pipeline_by_field` (orchestrator.py:802). Without the orchestrator-side raise, an operator running `lhp generate -e devtest` without first running `lhp validate` would still hit the silent-drop bug)
- Test: `tests/test_config_validator_for_each.py` (new test methods for the validator-time guard)
- Test: `tests/test_jdbc_watermark_aux_for_each.py` or new orchestrator-focused test (new test for the orchestrator-time guard via `lhp generate` path; assert raise even when `validate_project_invariants` is not called)

**Approach:**
- **Validator-time placement** (`validator.py`): new helper `_validate_for_each_per_pipeline_uniqueness(self, flowgroups: list[FlowGroup]) -> list[LHPError]`:
  - Group flowgroups by `flowgroup.pipeline`.
  - For each pipeline, count flowgroups with `flowgroup.workflow.execution_mode == "for_each"`.
  - If count > 1, raise `LHPError(category=ErrorCategory.CONFIG, code_number="036", title=..., details=f"pipeline '{pipeline}' contains {count} for_each flowgroups: {names}. Multi-for_each-per-pipeline is not supported by the current DAB workflow generator (would silently drop all but the first flowgroup's iterations).", suggestions=[...])`.
  - Invoke from `validate_project_invariants` immediately after the existing CFG-032 composite-uniqueness check — same call chain, same shape.
- **Generate-time placement** (`orchestrator.py`): inside `_generate_workflow_resources`, immediately after the existing CFG-034 mixed-mode block at line 1887:
  - When iterating `pipeline_v2_flowgroups`, count for_each flowgroups per pipeline. If >1 for the same pipeline, raise the same `LHP-CFG-036` LHPError with the same message.
  - This is the load-bearing guard — even if `lhp validate` is skipped, `lhp generate` cannot ship a broken bundle.
- Error message text: `"Pipeline '{pipeline}' has {count} flowgroups with execution_mode: for_each ({fg_a}, {fg_b}, ...). LHP currently supports at most one for_each flowgroup per pipeline (the DAB workflow generator emits one workflow YAML per pipeline, keyed on the first flowgroup; remaining flowgroups would not execute). To fix, either: (a) consolidate the for_each actions into a single flowgroup if they share orchestration concerns; or (b) split each flowgroup into its own pipeline if they are organizationally distinct (e.g., separate source systems). If you have a use case requiring multiple for_each flowgroups per pipeline, please file an issue describing the scenario."`
- Error code `LHP-CFG-036` (CFG-035 already used at `validator.py:450`).

**Execution note:** Test-first. Add the failing test for the multi-for_each-per-pipeline rejection before implementing the validator. Existing `test_multiple_b2_flowgroups_get_separate_aux_files` (`tests/test_jdbc_watermark_aux_for_each.py:295-328`) covers two flowgroups in **different** pipelines — that test must continue to pass.

**Patterns to follow:**
- LHP-CFG-034 mixed-mode rejection at `src/lhp/core/orchestrator.py:1887` is the direct precedent for the **generate-time** placement (sibling raise within `_generate_workflow_resources`).
- LHP-CFG-032 composite-uniqueness check inside `validate_project_invariants` is the direct precedent for the **validate-time** placement.
- LHPError construction pattern from `validator.py:383-403` (the per-flowgroup CFG-033 raise) for code, title, details, suggestions.
- `tests/test_config_validator_for_each.py:1-100` test class shape for adding new validation tests.

**Test scenarios:**
- *Happy path — single for_each per pipeline:* project with pipeline P1 containing one for_each flowgroup `fg_a` (5 actions). Validator passes; no LHP-CFG-036 raised.
- *Happy path — two for_each flowgroups in different pipelines:* project with pipeline P1 containing for_each `fg_a`, pipeline P2 containing for_each `fg_b`. Validator passes; no LHP-CFG-036 raised. **Critical: this case must NOT regress** — `test_multiple_b2_flowgroups_get_separate_aux_files` must continue to pass.
- *Happy path — non-for_each flowgroup co-existing with for_each in same pipeline:* this is already rejected by LHP-CFG-034 mixed-mode. New CFG-036 check is purely about ≥2 for_each, not about for_each + non-for_each. Confirm CFG-034 still fires, CFG-036 does not double-raise on the same project.
- *Error path — two for_each flowgroups in same pipeline (the bug):* project with pipeline P1 containing for_each `fg_a` (3 actions) AND for_each `fg_b` (2 actions). Validator raises LHPConfigError with code `LHP-CFG-036`, message containing `pipeline=P1`, `fg_a`, and `fg_b`. **Without the fix, `_generate_workflow_resources` silently merges and emits a workflow YAML referencing only `fg_a`'s aux files — the bug.**
- *Error path — three for_each flowgroups in same pipeline:* validator raises LHP-CFG-036 with all three flowgroup names listed.
- *Edge case — flowgroup with `execution_mode` absent (default legacy):* not counted as for_each. Validator passes if only one for_each + N legacy flowgroups (CFG-034 would have already rejected the mixed case).
- *Edge case — flowgroup with `execution_mode: for_each` but zero actions:* per-flowgroup CFG-033 zero-action check fires first in the validator loop (per-flowgroup invariant runs before project-scope check). Operator sees CFG-033 "flowgroup has no actions after expansion", not CFG-036. Acceptable: empty-actions is a per-flowgroup defect that should be reported as such. Add a comment to U4's validator that documents this ordering: empty-action flowgroups raise CFG-033 first; CFG-036 fires only when both for_each flowgroups have ≥1 action.
- *Generate-path bypass guard (R5 critical):* construct a project with 2 for_each flowgroups in the same pipeline. Invoke `generate_pipeline_by_field` directly (bypassing `validate_pipeline_by_field`). Assert LHP-CFG-036 raises from the orchestrator-side check, not just the validator-side. **Without the orchestrator-side raise, this test would silently emit a workflow YAML referencing only the first flowgroup — the original #16 bug.**

**Verification:**
- New test for 2-for_each-same-pipeline asserts LHP-CFG-036 raised with both flowgroup names in message.
- All existing for_each-related tests continue to pass (no regressions in `test_config_validator_for_each.py`, `test_jdbc_watermark_aux_for_each.py`, `test_b2_iteration_contract.py`).
- Manual smoke: construct a `lhp_project_root/` with 2 for_each flowgroups in same pipeline; run `lhp validate`; observe LHP-CFG-036 in stderr with actionable suggestion.

---

- [ ] **U5. Fix #17b: errors_reference.rst entries for B2 codes + runbook table sync**

**Goal:** Ensure every B2-emitted error code has a complete `docs/errors_reference.rst` entry (cause / common conditions / resolution / example) so an operator who sees `LHP-MAN-NNN` or `LHP-VAL-NNN` in Databricks logs and searches the reference finds it. Also add the new `MANIFEST` category to the categories overview table and sync the runbook troubleshooting table.

**Requirements:** R7

**Dependencies:** U1 (no new codes from U1, but its scenarios update troubleshooting examples), U2 (renamed VAL-040/041), U3 (new LHP-MAN-005), U4 (new LHP-CFG-036). Must run last in the sequence.

**Files:**
- Modify: `docs/errors_reference.rst`
  - Add `Manifest` section with entries for `LHP-MAN-001`, `LHP-MAN-002`, `LHP-MAN-003`, `LHP-MAN-004`, `LHP-MAN-005`
  - Add `LHP-VAL-050` (validate failed: `completed_n != expected` or `failed_n > 0`; renamed from VAL-04A in U2), `LHP-VAL-048`, `LHP-VAL-049` to existing Validation section
  - Add `LHP-CFG-035` (already shipped at validator.py:450 strict-comparison; was missing) and `LHP-CFG-036` (new from U4) to existing Configuration section, sorted into monotonic order with CFG-031..034
  - Add `MANIFEST (MAN)` row to the Categories overview table at lines 61-83
- Modify: `docs/runbooks/b2-for-each-rollout.md`
  - Update troubleshooting table cross-references to point at `errors_reference.rst#lhp-man-NNN` and `#lhp-val-NNN` rather than duplicating cause/resolution
  - Confirm `LHP-VAL-04A → LHP-VAL-050` rename at line 263 was applied in U2
  - Leave `LHP-VAL-04B` row at line 264 unchanged — placeholder for future parity check (currently superseded by LHP-VAL-049)
  - Add LHP-MAN-005 row (new code from U3) with cross-reference to errors_reference.rst
  - Add LHP-CFG-036 row (new code from U4) with cross-reference to errors_reference.rst

**Approach:**
- For each new entry, follow the precedent shape established by CFG-031..034 at `errors_reference.rst:625-924`:
  - `**When it occurs:**` — single sentence describing trigger condition
  - `**Why it is an error:**` — operational consequence
  - `**Common causes:**` — bullet list of typical scenarios
  - `**Resolution:**` — actionable steps
  - Optional `.. code-block:: yaml` with `:caption: Before/After` for codegen-time errors
  - `.. seealso::` cross-refs where related codes exist
- For `LHP-MAN-001..004`, source content from existing runbook descriptions at `b2-for-each-rollout.md:240-330` (cross-reviewer confirmed accurate).
- For `LHP-MAN-005`, write fresh from U3 implementation context.
- For `LHP-CFG-036`, write fresh from U4 implementation context. Cross-reference `LHP-CFG-034` mixed-mode (sibling pattern).

**Execution note:** Reference content is documentation — no test scenarios. Verify by manual rendering/reading.

**Patterns to follow:**
- CFG-031..034 entries at `errors_reference.rst:625-924` for entry shape.
- `WATERMARK (WM)` row in the Categories overview table for the `MANIFEST (MAN)` row format.
- Existing runbook troubleshooting table at `b2-for-each-rollout.md:240-330` for the cross-reference style.

**Test scenarios:**
- Test expectation: none — pure documentation. Verification is via human review (reviewer reads each new entry and confirms cause/resolution match the actual raise-site behavior).

**Verification:**
- For each newly added entry, manually verify it contains all required sections: `**When it occurs:**`, `**Why it is an error:**`, `**Common causes:**`, `**Resolution:**`, optional code-block example. Entries missing any required section fail R7.
- `rg -E 'LHP-(MAN|VAL|CFG)-' docs/errors_reference.rst` returns entries for every code raised by B2 paths in `src/lhp/templates/{bundle,load}/*.j2` and `src/lhp/core/validator.py`.
- `rg '\bLHP-VAL-04A\b' docs/` returns nothing in errors_reference.rst (rename complete); intentionally retained at runbook line 263 if not yet swept by U2 (sweep verified there).
- Runbook troubleshooting table contains rows for LHP-MAN-001..005, LHP-VAL-050/048/049, LHP-CFG-035/036. LHP-VAL-04B row stays as future-parity placeholder.
- Categories overview table contains `MANIFEST (MAN)` row.
- **U6 parity test passes** (see U6 below) — automated regression guard against future #17-class drift.

---

- [ ] **U6. Catalog parity regression guard**

**Goal:** Prevent future regressions of #17 by adding an automated test that introspects every error-code raise site in the LHP codebase and asserts each has a corresponding entry in `docs/errors_reference.rst`. The very gap the plan corrects today (codes raised but not documented) recurs every time someone adds a new code without updating the reference. A small parity test ensures CI catches it.

**Requirements:** R7 (preventatively — keeps R7 satisfied across future code additions, not just the current set).

**Dependencies:** U1, U2, U3, U4, U5 (must run last; references the populated `errors_reference.rst`).

**Files:**
- Test: `tests/test_error_code_catalog_parity.py` (new file)

**Approach:**
- Discovery: walk `src/lhp/templates/**/*.j2`, `src/lhp/core/validator.py`, `src/lhp/core/validators/*.py`, `src/lhp/core/orchestrator.py`, `src/lhp/utils/error_formatter.py`, and any other directory that has been observed to raise codes (broaden if grep finds more sites). Use a regex: `LHP-(?P<cat>[A-Z]+)-(?P<num>\d{3})` to enumerate all code tokens. Anchored 3-digit suffix matches the project convention; alphanumeric suffixes are not allowed (after U2 there should be none — but the test fails if any reappear, doubling as a convention guard).
- Filter: deduplicate, exclude codes that appear only in comments or docstrings (look for raise-context: `LHPError(...code_number="NNN"...)` or `RuntimeError(f"LHP-{cat}-{num}: ..."`). A simple substring-presence test is acceptable for v1: every raised code token appears in `docs/errors_reference.rst` somewhere.
- Read `docs/errors_reference.rst` once; for each discovered code token, assert `code in reference_text`. Failures list the missing codes with their raise-site file paths.
- Optional: add a category-membership check — every category prefix discovered (`MAN`, `VAL`, `CFG`, `WM`, `CF`, `IO`, `ACT`, `DEP`, `GEN`) must exist as a value in `ErrorCategory` enum. Catches a future `LHP-FOO-001` raise that bypasses the enum entirely.
- Allowed-exclusions list: a small frozenset for codes that are intentionally documented-only (e.g., `LHP-VAL-04B` runbook reference, kept as future-parity placeholder). Document each exclusion with a one-line comment explaining why.

**Execution note:** Test-first. The test should fail initially against an incomplete `errors_reference.rst` (proving it works), then pass after U5 is complete. If U5 missed any entries, U6 is the canary that surfaces the gap before merge.

**Patterns to follow:**
- `tests/test_b2_iteration_contract.py` is the precedent for this shape: a meta-test that asserts a structural invariant of the codebase (B2_ITERATION_KEYS lockstep), runs in `unit` marker, sub-100 LOC.
- AST-based parsing optional but not required — substring/regex over file text is sufficient for the parity check.

**Test scenarios:**
- *Happy path — all codes present:* run after U5 complete; every raised LHP-XXX-NNN token appears in errors_reference.rst; test passes.
- *Failure mode — missing entry:* synthetic test fixture (or test-only modification) that adds a new raise like `RuntimeError("LHP-MAN-099: ...")` to a temporary file; parity test fails listing `LHP-MAN-099` as undocumented. (Use a tmp-file fixture, not actual src modification.)
- *Failure mode — undeclared category:* synthetic raise like `LHPError(category=fake_category, code_number="001", ...)` where `fake_category` has prefix `XYZ`. Parity test fails listing `XYZ` as not in `ErrorCategory` enum.
- *Edge case — alphanumeric suffix sneaks back:* synthetic raise like `RuntimeError("LHP-VAL-04C: ...")`. The regex `LHP-(?P<cat>[A-Z]+)-(?P<num>\d{3})` does not match alphanumeric suffix → code not discovered → parity check passes (false negative). Add a complementary regex `LHP-[A-Z]+-\w+` that catches all token shapes; assert every match satisfies the strict `\d{3}` form. Convention violation surfaces as a test failure.
- *Edge case — code in a comment only:* `# LHP-VAL-040: landing_path is required` in `load_validator.py:185` is a reservation comment, not a raise. Test should NOT require these to have ref entries (they're not raised yet). Document the heuristic: comment-only codes are allowed-exclusions; raise-context codes are required.
- *Edge case — allowed exclusion:* `LHP-VAL-04B` appears in runbook + dead test assertion (`test_validate_template.py:528-532` asserts not-rendered). It is NOT raised anywhere. Test allowed-exclusions list includes it with the comment "future parity-check placeholder, superseded by LHP-VAL-049".

**Verification:**
- `pytest tests/test_error_code_catalog_parity.py -v` passes locally.
- Adding a synthetic raise without a ref entry causes the test to fail (manual smoke).
- Test is in `unit` marker, runs in default CI.
- Total LOC under 100 lines, mirrors `test_b2_iteration_contract.py` shape.

---

## System-Wide Impact

- **Interaction graph:**
  - U1 (#14): manifest claim/fail-mirror/complete-mirror UPDATE chain — three SQL statements share the ownership predicate. Status-based reclaim term loosens claim WHERE only; mirror UPDATEs unchanged because by the time they run, the current attempt has already overwritten `worker_run_id`. Confirm in implementation.
  - U3 (#15): codegen-time guard runs in `validate_project_invariants` chain (after CFG-031..035, before generation). Runtime guard fires inside generated notebook between manifest MERGE and `taskValues.set` — manifest rows already committed, downstream worker tasks fail at validate-task with LHP-VAL-048 (existing code, no change).
  - U4 (#16): new project-scope validator runs in `validate_project_invariants` chain — short-circuits orchestrator silent-merge bug at codegen.
  - U2 (#17): `ErrorCategory.MANIFEST` enum addition is referenced by U3 codegen-time `LHPError`. Order: U2 must precede U3.
- **Error propagation:**
  - U1: failure at JDBC read → `wm.mark_failed` (via `wm.insert_new` outside try block + mark_failed inside except) AND fail-mirror UPDATE on `b2_manifests`. Both legs unchanged. New: status-based reclaim by next attempt is the recovery path.
  - U3: codegen-time `LHPError` propagates to `lhp validate` / `lhp generate` exit code. Runtime `RuntimeError("LHP-MAN-005: ...")` propagates to Databricks task failure. **U3 sets `max_retries: 0` on the prepare_manifest task** in `workflow_resource.yml.j2` so this deterministic failure does not retry. Other tasks (worker, validate) keep their existing retry policy.
  - U4: codegen-time `LHPError` propagates to both `lhp validate` (validator path) AND `lhp generate` (orchestrator path) — dual placement. No runtime impact.
- **State lifecycle risks:**
  - U1: status-based reclaim is the **enabler** of correct retry semantics. Risk reduced, not increased.
  - U3 runtime guard: manifest table has rows for the failing batch_id but no taskValue emitted. Operator must clean up manually OR the next batch's retention DELETE (30-day window) reclaims them. Acceptable per existing retention policy.
  - U4: no runtime state risk; codegen-time only.
- **API surface parity:** All new error codes follow `LHP-{CATEGORY}-{NNN}` convention. Operator-visible CLI errors and notebook log strings stay consistent.
- **Integration coverage:** U1 has integration scenarios (full attempt-0-fails / attempt-1-succeeds flow). U3 has integration scenarios (codegen + runtime cooperation). U4 is purely codegen.
- **Unchanged invariants:**
  - `derive_run_id` format `f"job-{j}-task-{t}-attempt-{N}"` is unchanged (load-bearing for `batch_id` contract).
  - `B2_ITERATION_KEYS` (10 keys) is unchanged (load-bearing for B2 worker correctness post-Anomaly A regression fix).
  - `LHP-MAN-001..004` code values are unchanged (operator-visible token churn avoided).
  - LHP-CFG-031..035 numbering is unchanged.
  - LHP-VAL-048/049 numbering is unchanged.

---

## Risks & Dependencies

| Risk | Mitigation |
|------|------------|
| U1 status-based reclaim could allow a worker that's slow-failing (still `running`) to be reclaimed if a peer worker mistakenly thinks it's stuck. | The new status term `'failed'` deliberately excludes `running` and `completed`. Only the existing `worker_run_id IS NULL OR worker_run_id = current` terms can match a `running` row. Net effect: a `running` row owned by a different attempt is still rejected (LHP-MAN-002). Verified by `test_b2_runtime_competing_owner_raises` regression and the new concurrent-claim contention test. |
| U3 codegen-time size estimate could under/over-shoot actual runtime payload due to batch_id (runtime-derived) and identifier substitution. | Two-layer defense + 10% headroom: codegen-time uses 64-byte batch_id placeholder + concrete manifest_table from prepare_context, multiplied by 1.10 (5% JSON-array overhead + 5% drift cushion). Runtime guard catches any rare overshoot. |
| U3 runtime guard fires after manifest MERGE — leaves orphan rows in `b2_manifests`. | Existing 30-day retention DELETE in `prepare_manifest.py.j2:170-190` reclaims them. Operator-visible note in U5 errors_reference.rst entry includes manual cleanup query for impatient operators. |
| U3 LHP-MAN-005 deterministic retry storm if `max_retries > 0` on prepare_manifest task. | U3 explicitly sets `max_retries: 0` for the prepare_manifest task in `workflow_resource.yml.j2`. Test asserts the rendered DAB workflow YAML declares this. |
| U4 `LHP-CFG-036` may break a fixture in `tests/` that uses 2+ for_each flowgroups in the same pipeline. | Audit during implementation: `rg execution_mode tests/` to enumerate fixture flowgroups, confirm no test fixture currently constructs the rejected topology. If found, fixture is wrong (the silent-drop bug means it never actually exercised both flowgroups); update fixture to split pipelines. |
| U4 single-site placement at validator-only would bypass `lhp generate`. | Plan now requires dual placement (validator.py:validate_project_invariants AND orchestrator.py:_generate_workflow_resources near CFG-034 at line 1887). Generate-path bypass guard test asserts both paths raise. |
| U2 rename of `LHP-VAL-04A → LHP-VAL-050` may collide with future code numbering. | Verified VAL-050 is the next free numeric slot above the load_validator 040..047 reservation and the active VAL-048/049 raises. Plan lists the verification grep in U2. |
| U2 substring sweep could corrupt unrelated tokens (hex literals, base32 IDs, etc.). | U2 uses anchored regex `\bLHP-VAL-04A\b` for both sweep and verification. |
| Sequencing: U3 depends on U2 (MANIFEST enum); U5 depends on U1-U4 (entries for new codes); U6 depends on U5 (populated reference). | Plan sequence enforces order: U1, U2, U3, U4, U5, U6. U1 and U4 are independent and could run parallel if needed. |
| U6 parity test could be over-strict (allowed-exclusions list grows over time). | Allowed-exclusions list is per-test, comment-documented, reviewable in PR. Acceptable maintenance cost vs the cost of #17-class regressions. |

---

## Documentation / Operational Notes

- **Runbook updates:** `docs/runbooks/b2-for-each-rollout.md` troubleshooting table updated in U2 (renames) and U5 (new codes + cross-references). Cross-reference style preferred over duplication.
- **Errors reference:** `docs/errors_reference.rst` gains a new `Manifest` section + categories overview row + monotonic CFG-036 insertion in U5.
- **No bundle YAML changes.** No DAB workflow file naming changes. No deploy migration needed for in-flight bundles. (This is a key Option A advantage over Option B for #16.)
- **Devtest validation gating:** the 3 unchecked items in PR #24 test plan (`b2_smoke.yaml` manual run, R12 strict-`>` second-run, `enableRowTracking` `DESCRIBE DETAIL`) are independent of this plan. They should ideally run AFTER this plan's PR merges to validate that U1's retry fix doesn't introduce a duplicate-row regression on second-run. Coordinate with operator validation in devtest workspace (`devtest_edp_*` catalogs ready per project memory).
- **MEMORY.md update:** consider adding a feedback memory documenting the LHP grain reasoning that led to Option A for #16 (validators-reject-ambiguity vs orchestrator-restructure) so future similar decisions reach the same conclusion without re-derivation.

---

## Sources & References

- **Origin plan:** [docs/plans/2026-04-25-002-feat-b2-watermark-scale-out-emission-plan.md](2026-04-25-002-feat-b2-watermark-scale-out-emission-plan.md)
- **Related PRs:** #13 (B2 feature), #24 (ce-code-review walk-through, merged)
- **Related issues:** #14, #15, #16, #17 (this plan); #18-#23 (P2/P3 deferred, out of scope)
- **ce-code-review run artifact:** `.context/compound-engineering/ce-code-review/20260426-115040-b1c3456d/` — `correctness.json`, `adversarial.json` (ADV-001, ADV-002), `reliability.json` (REL-001, RR-001), `project-standards.json` (PS-001, PS-002), `performance.json`, `testing.json`
- **ce-doc-review pass (2026-04-27):** 5 reviewers (coherence, feasibility, product-lens, scope-guardian, adversarial). Material corrections applied: VAL-040/041 collision → VAL-050 rename target; CFG-034 location attribution; LHP-VAL-04B confirmed dead; U1 'pending' arm dropped (kept 'failed' only); U3 batch_id placeholder length + 10% drift cushion; U3 max_retries:0 promoted from deferred to required; U4 dual placement (validator + orchestrator) closes lhp-generate-bypass gap; U2 anchored substring regex guidance.
- **Design doc:** `docs/planning/b2-watermark-scale-out-design.md` (R1, R1a, R2, R12)
- **Operator runbook:** `docs/runbooks/b2-for-each-rollout.md` §"Internals: iteration contract", §"Operational caveats", §4.3 max_retries
- **Iteration contract memory:** `~/.claude/projects/-Users-dwtorres-src-Lakehouse-Plumber/memory/feedback_lhp_b2_iteration_contract.md`
- **Conventions:** `CLAUDE.md` § Error Handling, `AGENTS.md:17` "Existing LHP Compatibility"
