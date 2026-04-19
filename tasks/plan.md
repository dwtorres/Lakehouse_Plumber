# Plan — ADR-002 LHP Runtime Availability (Path 5: Namespace Extraction + DAB Workspace-File Sync)

**Status**: Phases 0–4 complete; Phase 5 in progress. On branch `feature/adr-002-runtime-availability`.
**Source**: `docs/adr/inputs/adr-002-runtime-availability.md` → formalized in `docs/adr/ADR-002-lhp-runtime-availability.md` (amended 2026-04-19 to replace Git Folder deploy with DAB workspace-file sync — see §Amendment below).
**Skill**: `agent-skills:planning-and-task-breakdown`.
**Governing**: `.specs/constitution.md` v1.1 (P1, P2, P3, P5, P6, P9, P10 load-bearing).
**Live status**: see [`tasks/todo.md`](todo.md) for checkbox state.

## Amendment summary (2026-04-19)

The 2026-04-18 plan scoped Phase 2 around a Databricks Git Folder deploy mechanism (`databricks repos update` + ephemeral deploy branches). During Phase 4 reconnaissance it became clear that:

1. `databricks bundle deploy` already syncs all bundle-root files to `${workspace.file_path}/`, including `src/lhp_watermark/` — no second deploy mechanism needed.
2. User bundles (distinct from the LHP repo itself) cannot rely on `sync.paths` pointing at a sibling LHP checkout, because DAB rebases sync to the common parent and breaks include-pattern portability.
3. The simplest, watermark-branch-compatible shape: drop the `environments.dependencies` wheel block from `workflow_resource.yml.j2`, keep the existing `${workspace.file_path}/generated/${bundle.target}/…` anchor, add `source: WORKSPACE` on each notebook task, and **vendor** `lhp_watermark/` into each user bundle.

ADR-002 was amended accordingly. Open Qs 2, 3, 4 became moot; Q1 (sys.path anchor) was resolved empirically by the T4.1 probe — prelude required. The original phase structure (P0–P5) and vertical slicing are preserved; only the Phase 2 deploy shape and the Phase 5 runbook body reflect the amendment.

## Phase completion summary

| Phase | State | Key commits |
|---|---|---|
| 0 — Decision docs | ✅ | `0bc88d28` |
| 1 — Namespace extraction | ✅ | `bdaeeaa3` + `717d4e25` (fixup) |
| 2 — Bundle refactor | ✅ (Option A) | `33d1b0b6` |
| 2.5 — Runtime-lib decoupling (discovered during Phase 4) | ✅ | `0e7c1126` |
| 3 — Packaging | ✅ (no-op; already auto-discovered) | — |
| 4 — Databricks smoke | ✅ (Wumbo E2E green) | `a1c12852` (template bootstrap) |
| 5 — Rollout docs | partial | (this commit — ADR-002 evidence + runbook body + plan/todo updates) |

Evidence: ADR-002 §Phase 4 Evidence + commit `517674443949620` job run on `dbc-8e058692-373e.cloud.databricks.com`.

---

Original planning narrative preserved below for audit.

---

## 0. Blockers & Pre-Conditions (must resolve before Phase 1)

### 0.1 Worktree placement

**Current worktree**: `upbeat-mayer-c929f7`, branch `claude/upbeat-mayer-c929f7`, off `origin/main`. Does **not** contain the fork's watermark runtime (`src/lhp/extensions/watermark_manager/`) nor the bundle template/generator. Planning docs can be authored here, but **execution must happen in the sibling worktree** `adr-002-runtime-availability` (branch `feature/adr-002-runtime-availability`, off the fork's `watermark` branch).

Decision required from user:
- **(A)** Author plan + ADR-002 here, execute in sibling worktree. Recommended.
- **(B)** Cherry-pick / merge fork watermark feature branch into current branch, execute here. Heavy; risk of diverging from the watermark branch.
- **(C)** Abandon current worktree, move into `adr-002-runtime-availability` for both planning and execution.

### 0.2 Missing deliverable

The file referenced in the user's invocation — `docs/adr/ADR-002-lhp-runtime-availability.md` — does not exist. It is a **deliverable of Phase 0**, not a pre-condition. Plan proceeds treating the ADR input as the authoritative spec surrogate.

### 0.3 Codebase facts (verified 2026-04-18)

| Artifact | Location | Status |
|---|---|---|
| `src/lhp/extensions/watermark_manager/` | sibling worktree `adr-002-runtime-availability` | 5 files, 1563 LoC (matches ADR input) |
| `src/lhp/templates/load/jdbc_watermark_job.py.j2` | same | Imports `from lhp.extensions.watermark_manager import …` (lines 19–24) |
| `src/lhp/templates/bundle/workflow_resource.yml.j2` | same | Has `environments` block with `{{ lhp_whl_path }}` (lines 7–12); notebook_path currently `${workspace.file_path}/generated/${bundle.target}/…` |
| `src/lhp/generators/bundle/workflow_resource.py` | same | Builds `lhp_whl_path` context; emits `${workspace.file_path}` prefix |
| `tests/extensions/watermark_manager/` | same | 11 test modules + `__init__.py` |
| `.specs/constitution.md` | repo root (all worktrees) | P2 wording conflicts with `lhp_watermark.*` import path per ADR input's "⚠️ literal text conflict" note |

### 0.4 Open questions from ADR input (require decisions before Phase 2/4 start)

1. **`sys.path` anchor point** — Databricks auto-adds `/Workspace/${env}/Lakehouse_Plumber` vs `/Workspace/${env}/Lakehouse_Plumber/src`. Determines whether `import lhp_watermark` works directly or needs `sys.path` prelude. **Blocks Phase 4**.
2. **Git folder path convention** — `/Workspace/${env}/Lakehouse_Plumber` vs `/Workspace/Shared/lhp/${env}/…`. **Blocks Phase 2** (template literal) unless parameterized via DAB variable.
3. **Generated artifacts branching** — commit `generated/${env}/**` to persistent `release/${env}` branch vs ephemeral `deploy/${env}/${COMMIT_SHA}` branch. **Blocks Phase 4 / runbook** (Phase 5).
4. **Notebook_path convention** — ADR MVP says `/Workspace/${var.env}/Lakehouse_Plumber/generated/${var.env}/…`. Current generator emits `${workspace.file_path}/generated/${bundle.target}/…`. Confirm the shift from `bundle.target` to `var.env` is intended. **Blocks Phase 2**.

**Recommendation**: resolve 1–4 during Phase 0 ADR authoring. Phase 4 probe notebook can empirically settle Q1.

---

## 1. Overview

### Goal

Realign LHP fork with upstream intent (pure code generator, zero runtime overhead) while preserving six production-grade safety features of the watermark runtime, by (a) extracting the runtime into a distinct top-level package `lhp_watermark`, and (b) shifting deployment from wheel-in-bundle to git-folder-sync.

### Scope

Single PR on branch `feature/adr-002-runtime-availability` targeting `watermark`. All ten MVP scope items from ADR input §MVP Scope, plus formal ADR-002 authoring and constitution P2 amendment.

### Non-goals (per ADR input §Not Doing + Out)

- Mode A simple-inline-SQL variant for upstream contribution.
- `lhp_watermark` as independent PyPI package / separate repo.
- Auto-provisioning Git folders per workspace.
- Retention / cleanup of `generated/${env}/` trees.
- Migration tooling beyond documented mechanical update.

---

## 2. Dependency Graph

```
                       ┌─────────────────────────────┐
                       │ Phase 0 — Decision docs     │
                       │ (ADR-002, Constitution P2,  │
                       │  Open Qs 1–4 resolved)      │
                       └──────────────┬──────────────┘
                                      │ Checkpoint C0 (human)
                                      ▼
                       ┌─────────────────────────────┐
                       │ Phase 1 — Namespace extract │
                       │ (move + rename + tests)     │
                       └──────────────┬──────────────┘
                                      │ Checkpoint C1 (all tests green)
                                      ▼
                       ┌─────────────────────────────┐
                       │ Phase 2 — Bundle refactor   │
                       │ (template + generator +     │
                       │  deprecation warning)       │
                       └──────────────┬──────────────┘
                                      │ Checkpoint C2 (render tests green)
                                      ▼
                       ┌─────────────────────────────┐
                       │ Phase 3 — Packaging config  │
                       │ (pyproject includes both    │
                       │  packages; build works)     │
                       └──────────────┬──────────────┘
                                      │ Checkpoint C3 (wheel + editable install)
                                      ▼
                       ┌─────────────────────────────┐
                       │ Phase 4 — Databricks smoke  │
                       │ (probe + E2E in dev)        │
                       └──────────────┬──────────────┘
                                      │ Checkpoint C4 (real workspace green)
                                      ▼
                       ┌─────────────────────────────┐
                       │ Phase 5 — Rollout docs      │
                       │ (parallel: runbook, TD-008, │
                       │  ADR-001 xref, release note)│
                       └─────────────────────────────┘
                                      │ Checkpoint C5 (PR ready)
                                      ▼
                                    MERGE
```

**Serialization rationale**: P1 → P2 disjoint on files but both touch template-render test harness; sequential avoids concurrent harness edits. P2 → P3 strictly sequential because pyproject references renamed package. P4 is worst cancelable (needs real workspace); do after local tests green.

---

## 3. Phases (vertical slices)

Each phase is a **complete vertical slice**: code change → unit test coverage → verification. No phase produces a half-done horizontal layer.

### Phase 0 — Decision docs & governance

**Why first**: Constitution P2 literal text conflicts with Path 5's `lhp_watermark.*` imports. Amending before code change avoids self-inflicted audit failure during PR review (P10).

#### T0.1 Author formal ADR-002

- **File**: `docs/adr/ADR-002-lhp-runtime-availability.md`
- **Inputs**: `docs/adr/inputs/adr-002-runtime-availability.md` (copy exists in current worktree)
- **Content**: Problem, Context, Decision (Path 5), Alternatives (Paths 1–4), Consequences (positive + negative + risks), Compliance (constitution mapping), Open Questions (4 from §0.4), Links.
- **Resolves**: Open Qs 1–4 in §0.4 (explicit decisions, not deferrals).
- **Acceptance**:
  - [ ] File exists at specified path.
  - [ ] All §MVP Scope items have explicit inclusion criteria.
  - [ ] All §Not Doing items have documented rationale.
  - [ ] Constitution P1–P10 mapped: which principles govern, any waivers declared per `.specs/constitution.md` §Waiver Procedure.
  - [ ] Cross-references to TD-008 opener, ADR-001, upstream issue #65, upstream README.
- **Verify**: human reads end-to-end; no `TODO` / placeholder markers remain.

#### T0.2 Amend constitution P2 wording

- **File**: `.specs/constitution.md`
- **Change**: P2 bullet 1 rewritten to: *"Generated notebooks import runtime libraries (e.g., `lhp_watermark.*`) from PYTHONPATH, not from an installed wheel."* (Current wording restricts to `lhp.extensions.*` / `lhp.runtime.*`.)
- **Bump**: Version 1.0 → 1.1. Date to execution date.
- **Acceptance**:
  - [ ] P2 wording permits `lhp_watermark.*` imports from generated notebooks.
  - [ ] Version + date incremented.
  - [ ] Rationale paragraph unchanged (still "Fewer moving parts. Faster iteration").
- **Verify**: `grep -n "lhp_watermark" .specs/constitution.md` returns P2 match.

#### T0.3 [P] Open a runbook stub

- **File**: `docs/runbooks/lhp-runtime-deploy.md`
- **Content**: Section skeleton only (Purpose, Prerequisites, Deploy steps, Rollback, Troubleshooting). Details filled in Phase 5.
- **Acceptance**:
  - [ ] File exists with sections.
  - [ ] Phase 5 can fill without structural change.

#### Checkpoint C0 — Human review

Gate: human reviewer approves ADR-002 + constitution amendment. Per P10, cross-boundary without review = rejection.

---

### Phase 1 — Namespace extraction (single vertical slice)

**Why vertical**: one cohesive move preserving git history + test parity. Splitting (e.g., move package now, rename module later) would leave the repo in an incoherent state.

#### T1.1 Package move via `git mv`

- **Source**: `src/lhp/extensions/watermark_manager/`
- **Target**: `src/lhp_watermark/`
- **Files**: 5 files (`__init__.py`, `_manager.py`, `exceptions.py`, `runtime.py`, `sql_safety.py`).
- **Command**: `git mv src/lhp/extensions/watermark_manager src/lhp_watermark`
- **Acceptance**: files moved, `git log --follow` preserves history.

#### T1.2 Rename private manager module

- **Source**: `src/lhp_watermark/_manager.py` → `src/lhp_watermark/watermark_manager.py`
- **Rationale**: module is now the package's primary public surface; leading underscore misleads.
- **Also update**: `src/lhp_watermark/__init__.py` re-exports.
- **Acceptance**: `from lhp_watermark.watermark_manager import WatermarkManager` resolves; old import path fails.

#### T1.3 Update internal package self-references

- **Scope**: any `from lhp.extensions.watermark_manager.*` inside the moved package itself (there should be none, but verify).
- **Also**: `__init__.py` re-exports use new module name.
- **Acceptance**: `rg "lhp.extensions.watermark_manager" src/lhp_watermark/` returns zero matches.

#### T1.4 Update Jinja template imports

- **File**: `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- **Before** (lines 19–24):
  ```python
  from lhp.extensions.watermark_manager import (
      TerminalStateGuardError,
      WatermarkManager,
  )
  from lhp.extensions.watermark_manager.runtime import derive_run_id
  from lhp.extensions.watermark_manager.sql_safety import SQLInputValidator
  ```
- **After**:
  ```python
  from lhp_watermark.watermark_manager import (
      TerminalStateGuardError,
      WatermarkManager,
  )
  from lhp_watermark.runtime import derive_run_id
  from lhp_watermark.sql_safety import SQLInputValidator
  ```
- **Acceptance**: template renders successfully; rendered output contains new import paths.

#### T1.5 Relocate tests

- **Source**: `tests/extensions/watermark_manager/`
- **Target**: `tests/lhp_watermark/`
- **Update**: all `from lhp.extensions.watermark_manager.*` → `from lhp_watermark.*`
- **Command**: `git mv tests/extensions/watermark_manager tests/lhp_watermark && rg -l "lhp\.extensions\.watermark_manager" tests/ | xargs sed -i '' 's|lhp\.extensions\.watermark_manager|lhp_watermark|g'` (verify each file after sed).
- **Acceptance**: `pytest tests/lhp_watermark -q` passes at same coverage as before.

#### T1.6 Update package structure test

- **File**: `tests/lhp_watermark/test_package_structure.py` (previously in `tests/extensions/watermark_manager/`).
- **Change**: module-presence assertions reference new `lhp_watermark.*` import paths.
- **Acceptance**: test green; asserts `_manager` absent and `watermark_manager` present.

#### Phase 1 verification

- [ ] `pytest tests/ -q` passes (full suite).
- [ ] `rg "lhp\.extensions\.watermark_manager" src/ tests/` returns zero matches.
- [ ] `rg "from lhp_watermark" src/ tests/` returns expected matches (template + tests).
- [ ] `python -c "import lhp_watermark; from lhp_watermark.watermark_manager import WatermarkManager"` succeeds.
- [ ] Rendered `jdbc_watermark_job.py.j2` output for a representative action contains new imports, no `lhp.extensions.*` strings.
- [ ] `git log --follow src/lhp_watermark/watermark_manager.py` shows pre-rename history.

#### Checkpoint C1

Gate: full pytest green + mechanical-only diff (no behavior change).

---

### Phase 2 — Bundle template + generator refactor (single vertical slice)

**Why vertical**: template change without generator change = KeyError on missing context; generator change without template change = silent wheel emission. Must flip together.

**Blocks on**: Open Qs 2 and 4 (§0.4) resolved in Phase 0.

#### T2.1 Update `workflow_resource.yml.j2`

- **File**: `src/lhp/templates/bundle/workflow_resource.yml.j2`
- **Remove** (lines 7–12): entire `environments:` block.
- **Add** to each task's `notebook_task`: `source: WORKSPACE`.
- **`notebook_path`**: shape per ADR Open Q 4 decision. Default: `/Workspace/${var.env}/Lakehouse_Plumber/generated/${var.env}/{{ pipeline_name }}/__lhp_extract_{{ action_name }}`.
- **Also remove**: `environment_key: lhp_env` from each task.
- **Acceptance**: template renders without `lhp_whl_path` in context; rendered YAML has no `environments:` key and no `environment_key:` references.

#### T2.2 Update `workflow_resource.py` generator

- **File**: `src/lhp/generators/bundle/workflow_resource.py`
- **Remove**: `"lhp_whl_path": "${var.lhp_whl_path}"` from `template_context` (line 77).
- **Change**: `notebook_path` construction (lines 48–51) from `${workspace.file_path}/generated/${bundle.target}/…` to the Open-Q-4-resolved shape.
- **Acceptance**: `extraction_tasks[*]["notebook_path"]` uses new path convention; no `lhp_whl_path` key in context dict.

#### T2.3 Deprecation warning for `${var.lhp_whl_path}`

- **Behavior**: generator detects user bundle still declares `var.lhp_whl_path` (via `databricks.yml` or variables file) and emits a `DeprecationWarning` pointing at ADR-002.
- **Acceptance**: test case: bundle with `lhp_whl_path` variable → warning captured via `pytest.warns`.
- **Removal schedule**: one release grace window per Constitution P1 (migration path).

#### T2.4 Render test

- **File**: `tests/unit/generators/bundle/test_workflow_resource.py` (existing).
- **Update**: existing test assertions for `lhp_whl_path` / `environments` replaced by asserts-absence; add assertions for `source: WORKSPACE` + new `notebook_path` shape.
- **Acceptance**: `pytest tests/unit/generators/bundle/test_workflow_resource.py -v` passes.

#### T2.5 Bundle integration tests

- **Files**: `tests/test_bundle_jinja2.py`, `tests/test_orchestrator_bundle_integration.py`, `tests/e2e/test_bundle_manager_e2e.py`, `tests/scripts/test_verify_databricks_bundle.py`.
- **Update**: any fixture or assertion expecting wheel block / `${workspace.file_path}/generated/…` path.
- **Acceptance**: `pytest -k bundle -q` passes.

#### Phase 2 verification

- [ ] All bundle-scoped tests green.
- [ ] `rg "lhp_whl_path" src/` returns zero matches (plus deprecation-warning path).
- [ ] `rg "environments:" src/lhp/templates/bundle/` returns zero matches.
- [ ] End-to-end generator run on a sample pipeline produces YAML validating under `databricks bundle validate` (dry, no deploy).

#### Checkpoint C2

Gate: bundle tests green + `databricks bundle validate` dry-run green on sample.

---

### Phase 3 — Packaging config (single vertical slice)

#### T3.1 Update `pyproject.toml`

- Include both `lhp` and `lhp_watermark` as top-level packages.
- Setuptools config: `packages = ["lhp", "lhp_watermark"]` (or equivalent find-config).
- `lhp_watermark` **not** a subpackage of `lhp`.
- **Acceptance**: `pip install -e .` then `python -c "import lhp, lhp_watermark"` succeeds.

#### T3.2 Update MANIFEST.in / setup.cfg if present

- Ensure `lhp_watermark` module data files ship correctly.
- **Acceptance**: `python -m build` produces wheel containing both packages (inspect with `unzip -l dist/*.whl | grep lhp_watermark`).

#### T3.3 CI build verification

- Extend existing CI wheel-build job (`.github/workflows/*.yml`) with an import-smoke step after install: `python -c "import lhp_watermark.watermark_manager; import lhp.cli"`.
- **Acceptance**: CI green on a PR branch probe.

#### Phase 3 verification

- [ ] `pip install -e .` from clean venv succeeds.
- [ ] Both imports resolve post-install.
- [ ] Wheel ships both top-level packages.
- [ ] CI probe green.

#### Checkpoint C3

Gate: fresh-venv install + both imports + CI probe green.

---

### Phase 4 — Databricks smoke validation (single vertical slice)

**Blocks on**: Open Qs 1–4 resolved + dev workspace + `dbc-8e058692-373e` CLI profile.

#### T4.1 Probe notebook for `sys.path` anchor

- **Artifact**: one-cell notebook at `Example_Projects/acmi/notebooks/_probes/sys_path_probe.py` (or similar).
- **Content**: prints `sys.path`, attempts `import lhp_watermark`, reports anchor (`/Workspace/${env}/Lakehouse_Plumber` vs `.../src`).
- **Deploy**: `databricks repos update … --branch deploy/probe` then run as one-time job.
- **Decision output**: prelude needed (`sys.path.insert(0, "…/src")`) or not.
- **Acceptance**: probe runs to completion; anchor resolution documented in ADR-002 addendum.

#### T4.2 Git folder deploy recipe verification

- **Command sequence**:
  ```bash
  databricks --profile dbc-8e058692-373e repos update \
    /Workspace/dev/Lakehouse_Plumber --branch release/dev
  databricks --profile dbc-8e058692-373e bundle deploy -t dev
  ```
- **First-run concern**: `repos create` vs `repos update` — document fallback.
- **Acceptance**: sequence green on dev target; workspace files match local tree.

#### T4.3 E2E smoke test

- **Base harness**: TD-007 Phase B harness (referenced in ADR input).
- **Change**: swap import path in the validation notebook to `lhp_watermark.*`.
- **Run**: one jdbc_watermark_v2 extraction against a test source; verify watermark advance, exception taxonomy, terminal-state guards.
- **Acceptance**:
  - [ ] Extraction succeeds.
  - [ ] Watermark row advances only on terminal success.
  - [ ] `mark_failed` path still raises correct typed exception.
  - [ ] Concurrent-commit retry loop path exercised via forced second attempt.

#### Phase 4 verification

- [ ] Probe resolved `sys.path` anchor.
- [ ] Deploy recipe green.
- [ ] E2E smoke green with all six safety features exercised.
- [ ] Evidence captured in `docs/adr/evidence/adr-002-smoke-test.md` or similar.

#### Checkpoint C4

Gate: real-workspace smoke green + evidence artifact committed.

---

### Phase 5 — Rollout docs (parallel)

Four tasks, all independent, all `[P]`.

#### T5.1 [P] Runbook body

- **File**: `docs/runbooks/lhp-runtime-deploy.md`
- **Content**: Azure DevOps recipe + GitHub Actions recipe mirroring edp's `update_git_folder.yml` + `deploy_bundle.yml`. First-deploy bootstrap (`repos create` vs `repos update`). Rollback: revert release branch + redeploy. Troubleshooting: `sys.path` resolution, missing module, stale git folder.
- **Acceptance**: runbook executable by a platform engineer without access to ADR-002.

#### T5.2 [P] TD-008 closure

- **File**: `docs/tech-debt/TD-008-runtime-availability-pattern.md`
- **Change**: status → Resolved; link to ADR-002 + smoke-test evidence.
- **Acceptance**: `TD-008` shows Resolved + ADR-002 cross-ref.

#### T5.3 [P] ADR-001 cross-reference

- **File**: `docs/adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md`
- **Change**: replace "runtime-availability scoped out" pointer with direct link to resolved ADR-002.
- **Acceptance**: ADR-001 xref points at `ADR-002-lhp-runtime-availability.md`.

#### T5.4 [P] Release notes migration note

- **File**: `CHANGELOG.md` or `docs/releases/vNEXT.md`.
- **Content**: Users with existing `${var.lhp_whl_path}` bundles must drop the DAB variable and update notebook path within one release window. Deprecation warning emitted until then.
- **Acceptance**: note exists, references ADR-002, documents grace window per P1.

#### Checkpoint C5

Gate: docs reviewed, PR drafted, CI green, human final approval → merge.

---

## 4. Constitution compliance map

| Principle | Phase | Compliance lever |
|---|---|---|
| P1 Additive Over Rewrite | P2.3, P5.4 | Deprecation warning + grace window, not hard-break |
| P2 No Wheel Until Justified | P0.2, P2.1, P2.2 | Constitution amendment + wheel removal from template + generator context |
| P3 Thin Notebook, Fat Runtime | P1 (preserves existing structure) | Runtime still in importable package; notebook stays thin |
| P5 Registry source of truth | P4.3 (verified via smoke) | Watermark advance logic unchanged; verified under retry |
| P6 Retry Safety By Construction | P4.3 (verified via smoke) | Concurrent-commit retry loop exercised in smoke |
| P9 Security-Exact SQL Interpolation | P1 (preserves `SQLInputValidator` intact) | Import path change only; validator semantics unchanged |
| P10 Human Validation At Level Boundaries | C0, C1, C2, C3, C4, C5 | All six checkpoints require explicit human approval |

**Waivers declared**: none.

---

## 5. Risk register

| Risk | Likelihood | Impact | Mitigation | Phase |
|---|---|---|---|---|
| Databricks auto-`sys.path` does not include `src/` | M | H | Probe notebook (T4.1) empirically resolves; add prelude if needed | P4 |
| `databricks repos update` rejects branch names with slashes | L | M | ADR Open Q; verify in T4.2; fall back to branch without slash | P4 |
| Existing bundle tests depend on `${workspace.file_path}` notebook_path shape | M | M | T2.5 updates all bundle integration tests | P2 |
| Fork divergence during Phase 1 mechanical rename (long-lived branch) | M | M | Short-lived branch; Phase 1 ≤ 1 day; rebase on `watermark` frequently | P1 |
| Constitution amendment rejected in review | L | H | Pre-circulate amendment wording with reviewer before Phase 0 close | P0 |
| TD-007 Phase B harness unavailable or changed | L | M | Inspect harness state before Phase 4; adapt to current shape | P4 |
| `lhp_watermark` name collides with future upstream naming | L | L | Coordinate with upstream maintainer via issue #65 thread | P5 |

---

## 6. Execution location

Per §0.1: plan + ADR-002 authored in current worktree; **code changes execute in sibling worktree `adr-002-runtime-availability`** after user confirms option (A).

Phase 0 deliverables (ADR-002, constitution amendment, runbook stub) **could** live in either worktree; recommendation = sibling, because they're part of the same PR as code changes and must merge together.

If user picks (A), operational sequence:
1. Finalize this plan in current worktree.
2. Copy / re-create plan + ADR-002 draft in sibling worktree.
3. Sibling worktree does Phases 0–5.
4. Current worktree: close, or retain for planning only.

---

## 7. Review questions for user before Phase 1 start

1. **Confirm worktree placement** — option A, B, or C from §0.1?
2. **Open Q 1 resolution** — if known from prior work, skip probe; else accept probe in T4.1.
3. **Open Q 2 resolution** — git folder path convention (`/Workspace/${env}/Lakehouse_Plumber` vs `/Workspace/Shared/lhp/${env}/…` vs parameterized)?
4. **Open Q 3 resolution** — `release/${env}` persistent vs `deploy/${env}/${SHA}` ephemeral branch?
5. **Open Q 4 resolution** — new notebook_path shape: `var.env` vs `bundle.target`?
6. **Constitution amendment scope** — narrow (just P2 bullet 1) or broader (document Path 5 as a named pattern)?
7. **Phase 4 workspace target** — which dev workspace for smoke test? Confirm `dbc-8e058692-373e` profile.
8. **Parallel execution** — P1/P2 could theoretically parallel (disjoint files). Serialize (simpler) or parallelize (faster)?

---

## 8. Appendix — Files modified by phase

| Phase | Created | Modified | Moved / Renamed |
|---|---|---|---|
| P0 | `docs/adr/ADR-002-lhp-runtime-availability.md`, `docs/runbooks/lhp-runtime-deploy.md` | `.specs/constitution.md` | — |
| P1 | — | `src/lhp/templates/load/jdbc_watermark_job.py.j2` | `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/`; `_manager.py` → `watermark_manager.py`; `tests/extensions/watermark_manager/` → `tests/lhp_watermark/` |
| P2 | — | `src/lhp/templates/bundle/workflow_resource.yml.j2`, `src/lhp/generators/bundle/workflow_resource.py`, `tests/unit/generators/bundle/test_workflow_resource.py`, bundle integration tests | — |
| P3 | — | `pyproject.toml` (+ `setup.cfg`, `MANIFEST.in` if present), one CI workflow file | — |
| P4 | `Example_Projects/.../sys_path_probe.py`, `docs/adr/evidence/adr-002-smoke-test.md` | — | — |
| P5 | — | `docs/runbooks/lhp-runtime-deploy.md` (body), `docs/tech-debt/TD-008-runtime-availability-pattern.md`, `docs/adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md`, `CHANGELOG.md` | — |

---

## 9. Out of scope / intentionally deferred

Copied verbatim from ADR input §Out, re-affirmed here to prevent scope creep in any phase:

- Mode A (simple inline SQL) upstream contribution — separate ticket post-merge.
- `lhp_watermark` split to independent PyPI package / repo — operational signal required first.
- Auto-provisioning Git folders per workspace — admin concern.
- Retention / cleanup of `generated/${env}/` subtrees over time.
- Migration tooling beyond documented mechanical update.
