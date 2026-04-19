# ADR Input — ADR-002: LHP Runtime Availability via Namespace Extraction + Git Folder

**Status**: ADR input artifact (pre-draft). Feeds a forthcoming `docs/adr/ADR-002-lhp-runtime-availability.md`.

**Origin**: TD-008 opened during TD-007 closure (ADR-001). Fork's generated extraction notebook imports `from lhp.extensions.watermark_manager import ...`, and the generated DAB workflow resource emits `environments.dependencies: [${var.lhp_whl_path}]` — a Python wheel as runtime vehicle. External design review + upstream evidence confirmed a deeper misalignment: **LHP upstream is "zero runtime overhead — pure code generation, not a runtime framework"** (upstream `README.md`), and maintainer intent for watermark support per issue [#65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65) is "LHP will create and maintain a table of high watermarks for tables which gets loaded by external integration tools so they can use the value as a high watermark for next ingestions" — a narrower scope than our fork's runtime `WatermarkManager` class. An alignment audit (three parallel research agents + sequential-thinking synthesis, 2026-04-18) produced a revised direction.

**Consumer**: ADR author. Use this document's Problem, Evidence, Option Space, Recommended Direction, Assumptions, MVP Scope, and Not-Doing sections as the skeleton of ADR-002.

---

## Problem Statement

**How might we preserve the production-grade safety features of our watermark runtime (`WatermarkManager` + `SQLInputValidator` + exception taxonomy + concurrent-commit retry + terminal-state guards) while aligning LHP's role with upstream intent as a pure code generator with zero `lhp.*` runtime dependencies in generated code, and deploying the runtime via a stable CI/CD-driven mechanism?**

## Evidence

### Upstream LHP design intent (authoritative)

- **`README.md` (upstream/main)**: "Zero runtime overhead — pure code generation, not a runtime framework." Anti-goal: "it should NOT… Introduce runtime overhead (no compiling configurations at runtime to generate pipelines)."
- **Generated code audit** (all upstream Jinja templates under `src/lhp/templates/load/`, `transform/`, `write/`): zero `from lhp.*` imports. Generated notebooks use only `pyspark`, `pyspark.pipelines` as `dp`, standard library, and user-provided modules.
- **Upstream has no watermark template**: `src/lhp/templates/load/jdbc_watermark_job.py.j2` does not exist in `upstream/main`. Feature is fork-only.

### Maintainer intent on watermark support

- **Issue [#65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65)** (Mmodarre, 2025-12-17, OPEN, labeled enhancement): *"New feature to create and update High watermark table. This feature will help systems which use external tools (Azure Data Factory, etc.) for ingestion. LHP will create and maintain a table of high watermarks for tables which gets loaded by external integration tools so they can use the value as a high watermark for next ingestions."*
- **Scope**: LHP as producer/maintainer of a watermark table that external orchestrators consume. Not a Python runtime class instantiated inside the pipeline.

### Fork's current state

- `src/lhp/extensions/watermark_manager/` (1563 LoC across 5 files) holds the runtime framework.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` imports `from lhp.extensions.watermark_manager import WatermarkManager, TerminalStateGuardError` etc.
- `src/lhp/templates/bundle/workflow_resource.yml.j2` emits `environments.dependencies: [${var.lhp_whl_path}]` (wheel dep).
- Six critical safety features live in `WatermarkManager` that cannot be dropped without regressions: `SQLInputValidator` per-type validators, Delta concurrent-commit retry loop (jittered backoff, 5 attempts), terminal-state guards (`mark_complete` and `mark_failed` both), UTC session enforcement, atomic MERGE with `num_affected_rows` check, structured LHP-WM-001..004 exception taxonomy.

### Reference pattern (edp-data-engineering, proven in production)

- Two-step CI/CD per environment: `databricks repos update <path> --branch <branch>` then `databricks bundle deploy -t <target>`.
- DAB job YAML references absolute workspace paths with `source: WORKSPACE`: e.g., `notebook_path: /Workspace/${var.env}/notebooks/orchestration/bronze/source_to_bronze`.
- Runtime library `dlaccel/` lives at the git folder root, sibling to notebooks. Generated/hand-written notebooks `from dlaccel.orchestration.watermarks import AccelWatermark` — resolves via Databricks Runtime's auto-`sys.path` for notebook-task execution.
- `dlaccel` is **a separate package from the orchestration framework**, not a submodule of it. That separation is what makes the imports legitimate per upstream conventions.

## Option Space

### Family I — Generated notebook imports a runtime library (wheel OR workspace files)

- A1–A4 — Wheel via PyPI / UC Volume / workspace_file / git URL. Wheel lifecycle in all cases.
- B1–B4 — Admin Git folder or DAB-synced workspace files, with either `sys.path` prelude or editable install.
- **Path 3 (prior Option P)** — Host `lhp.extensions.*` via git folder per edp pattern.

### Family II — Generated notebook self-contained

- E1 — Inline `WatermarkManager` source into every generated notebook via Jinja (~1000 LoC per notebook).
- E2 — SQL-only inline runtime (big refactor, loses Python class ergonomics).
- E3 — Compile-time partial eval.
- E4 — UC Functions / stored procedures.

### Family III — Strict upstream alignment

- **Path 1 (external review)** — Replace class with plain inline `spark.sql` calls for watermark read/write; orchestrator handles retries. Loses six critical safety features.

### Emergent synthesis

- **Path 5 — Namespace extraction + git folder deploy**: Rename `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` in the fork (sibling to `src/lhp/`). Package published as **`lhp_watermark`** — technically a distinct Python package, prefix-named for ecosystem discoverability but **not a submodule of `lhp`**. LHP reverts to pure code generator. Generated extraction notebook imports from the external library: `from lhp_watermark.watermark_manager import WatermarkManager`. Deployment via Databricks Git folder per edp pattern (Option P mechanism). Wheel block removed from workflow template.

## Recommended Direction: Path 5

### Why Path 5 wins

| Criterion | Path 1 (strict) | Path 2 (E1 inline) | Path 3 (Option P as-is) | **Path 5 (namespace extract)** |
|---|---|---|---|---|
| Generated code has no `lhp.*` imports | ✅ | ✅ | ❌ | ✅ (`lhp_watermark.*` ≠ `lhp.*`) |
| LHP stays pure code generator | ✅ | ✅ | ❌ | ✅ |
| All six safety features preserved | ❌ rebuilt in orchestrator | ✅ | ✅ | ✅ |
| Upstream issue #65 scope compatible | ✅ | ✅ | ⚠️ | ✅ (table contract preserved; library is opt-in add-on) |
| User sunk code preserved | ❌ rewrite | ✅ | ✅ | ✅ (mechanical rename) |
| edp-style git folder deploy | N/A | ✅ | ✅ | ✅ |
| "One runtime instance per workspace" | N/A | ❌ (per bundle) | ✅ | ✅ |
| Future upstream contribution path | Direct | Hard | Unlikely | Mode A additive contribution possible |
| Constitution P2 spirit | ✅ | ✅ | ⚠️ literal text conflict | ✅ (amended wording) |
| Migration cost | Complete rewrite | Big refactor | Small CI change | **Mechanical rename + CI change** |

### Deployment shape (Path 5, concrete)

Repo layout:

```
Lakehouse_Plumber/                        ← Databricks Git Folder root
├── src/
│   ├── lhp/                               ← pure code generator (no runtime role)
│   └── lhp_watermark/                     ← runtime library (co-located; distinct package)
│       ├── __init__.py
│       ├── watermark_manager.py           ← was _manager.py
│       ├── sql_safety.py
│       ├── exceptions.py
│       └── runtime.py                     ← derive_run_id
├── generated/${env}/                      ← committed per env by CI
│   └── {pipeline}/__lhp_extract_{action}.py
└── resources/lhp/                         ← DAB resource YAMLs
```

CI pipeline (per env, unchanged from prior Option P):

```bash
lhp generate --env ${env}
git add generated/${env}/ resources/lhp/${env}/
git commit -m "deploy(${env}): regen for ${COMMIT_SHA}"
git push origin release/${env}

databricks --profile ${PROFILE} repos update \
  /Workspace/${env}/Lakehouse_Plumber --branch release/${env}

databricks --profile ${PROFILE} bundle deploy -t ${env}
```

Generated workflow resource YAML (same as prior Option P):

```yaml
resources:
  jobs:
    {{ pipeline_name }}_workflow:
      name: {{ pipeline_name }}_workflow
      tasks:
        - task_key: extract_{{ action_name }}
          notebook_task:
            notebook_path: /Workspace/${var.env}/Lakehouse_Plumber/generated/${var.env}/{{ pipeline_name }}/__lhp_extract_{{ action_name }}
            source: WORKSPACE
```

Generated extraction notebook (only import line changes from current template):

```python
# was: from lhp.extensions.watermark_manager import (...)
from lhp_watermark.watermark_manager import (
    TerminalStateGuardError,
    WatermarkManager,
)
from lhp_watermark.runtime import derive_run_id
from lhp_watermark.sql_safety import SQLInputValidator
```

Databricks Runtime auto-adds git folder root to `sys.path` for notebook-task execution; `src/lhp_watermark/` is reachable as a top-level package.

### Why `lhp_watermark` (not `watermark_runtime` or `cnh_watermark`)

- `lhp_` prefix: signals ecosystem association with Lakehouse Plumber (discoverability for LHP users).
- Underscore separator (not dot): makes it a **distinct top-level package**, not a submodule of `lhp`. Satisfies "generated code has no `lhp.*` imports" (strict read: `from lhp_watermark.*` is NOT an import from `lhp.*`).
- Single-word domain: `watermark` maps cleanly to the feature's scope.
- Potential future siblings: `lhp_rest_api`, `lhp_kafka_runtime`, etc., if additional runtime libraries emerge.
- Simpler than renaming to something fully independent (e.g., `cnh_watermark`) while still technically separate from `lhp` in Python's namespace resolution.

## Key Assumptions to Validate

- [ ] **Databricks Runtime auto-adds the Git Folder root to `sys.path`** for notebook-task execution on serverless env v2. Proven empirically by edp's `from dlaccel.*` imports working in production. Our TD-007 Phase B evidence also validated this (notebook imported `from lhp.extensions.watermark_manager`; same mechanism applies regardless of package name). Spot-check with a `lhp_watermark` probe before shipping.
- [ ] **`src/lhp_watermark/` is reachable at the git folder root** after `databricks repos update`. Path structure: `/Workspace/${env}/Lakehouse_Plumber/src/lhp_watermark/...`. Verify `sys.path` contains `/Workspace/${env}/Lakehouse_Plumber/src` OR `/Workspace/${env}/Lakehouse_Plumber` (depending on Databricks's auto-path behavior).
- [ ] **`databricks repos update` handles branch names with slashes** (e.g., `release/${env}`). Docs say yes; verify.
- [ ] **Mechanical rename preserves all tests.** After moving `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` and updating imports in template + tests, the existing test suite passes unchanged.
- [ ] **DAB bundle validate resolves `${var.env}` in notebook paths** correctly per target (dev/qa/prod).
- [ ] **No race between `repos update` and `bundle deploy`** when the bundle references a notebook path that must exist. Order is deterministic.

## MVP Scope

**In** (single atomic PR):

1. **Move + rename**: `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` (top-level package). Rename `_manager.py` → `watermark_manager.py` (drop leading underscore; module is now public since it's the package's primary surface).
2. **Update imports** in `src/lhp/templates/load/jdbc_watermark_job.py.j2`:
   - `from lhp_watermark.watermark_manager import WatermarkManager, TerminalStateGuardError`
   - `from lhp_watermark.runtime import derive_run_id`
   - `from lhp_watermark.sql_safety import SQLInputValidator`
3. **Update workflow template** `src/lhp/templates/bundle/workflow_resource.yml.j2`:
   - Remove `environments.*` block entirely
   - Change `notebook_task.notebook_path` to `/Workspace/${var.env}/Lakehouse_Plumber/generated/${var.env}/{{ pipeline_name }}/__lhp_extract_{{ action_name }}`
   - Add `source: WORKSPACE`
4. **Update generator** `src/lhp/generators/bundle/workflow_resource.py`:
   - Drop `lhp_whl_path` from template context
   - Emit env-aware notebook_path context
5. **Test relocations**: `tests/extensions/watermark_manager/` → `tests/lhp_watermark/`; update any imports referencing the old path.
6. **Deprecate `${var.lhp_whl_path}`** with a one-release grace window (Constitution P1 migration path); emit a generator warning when the variable is still set in user bundles.
7. **Constitution amendment**: amend `.specs/constitution.md` P2 wording to reflect that generated notebooks may import runtime libraries deployed via workspace files or git folder (not just `lhp.*`). Draft language:
   > "Runtime deploys as workspace files or production Git folder synced by Databricks Asset Bundles or an admin-managed service principal. A Python wheel is introduced only when reuse, versioning, or rollback operationally requires it. Generated notebooks import runtime libraries (e.g., `lhp_watermark.*`) from PYTHONPATH, not from an installed wheel."
8. **ADR-002** at `docs/adr/ADR-002-lhp-runtime-availability.md` documenting the decision with Path 5 rationale + alignment evidence.
9. **Runbook** `docs/runbooks/lhp-runtime-deploy.md` with Azure DevOps + GitHub Actions recipes modeled on edp's `update_git_folder.yml` + `deploy_bundle.yml`.
10. **Validation**: template render test asserting new shape; end-to-end smoke test reusing TD-007 Phase B harness (swap import path in the validation notebook).

**Out (separate concerns, tracked as follow-ups)**:

- **Mode A (simple inline SQL) contribution to upstream**: matches issue #65's narrower scope; optional future work for upstream merge consideration. Does not block this ADR.
- **`lhp_watermark` as an independent PyPI package**: keep co-located for now; split later if operational need justifies.
- **Auto-provisioning Git folders per workspace**: admin operational concern.
- **Retention / cleanup** of `generated/${env}/` subtrees in the git folder over time.
- **Migration tooling** for users with existing `${var.lhp_whl_path}` bundles: documented in ADR-002; mechanical update.

## Not Doing (and Why)

- **Path 1 strict alignment (plain inline `spark.sql`)** — discards six production-grade safety features that protect against data-loss race conditions, SQL injection, and concurrent-commit races. External reviewer's proposal works only if an equivalent-complexity orchestrator-side library rebuilds those features; net complexity unchanged.
- **Path 2 E1 inline (~1000 LoC per notebook)** — preserves safety but forces multi-hundred-LoC copies per bundle, violates user's "one instance per workspace" preference, complicates upgrade propagation.
- **Path 3 Option P as-is (keep `lhp.extensions.*` namespace)** — preserves wheel-free deploy but still imports `lhp.*` from generated code, which is upstream's explicit non-goal. Path 5 is structurally identical with a clean namespace.
- **A-series wheel variants** — wheel barrier is lifted, but wheel lifecycle (build, host, install) adds surface area that Path 5's git-folder deploy eliminates.
- **E4 UC Functions** — promising long-term but requires Python → SQL function translation for each method; minimal benefit over Path 5 for the current threat model.
- **Independent repo for `lhp_watermark`** — co-located in fork is simpler now; split later if justified by operational signal.

## Open Questions

1. **Commit `generated/${env}/**` to a persistent deploy branch, or regenerate in CI and push to an ephemeral branch per run?** Upstream LHP docs prefer regenerate-in-CI (no artifacts in source control). edp commits notebooks (but edp doesn't regenerate — its notebooks are hand-authored). For LHP fork: regenerate + push to ephemeral `deploy/${env}/${COMMIT_SHA}` branch per CI run is cleanest — no branch accumulation, deterministic per commit. Decide in ADR-002.
2. **Git folder path convention**: `/Workspace/${env}/Lakehouse_Plumber` (matches edp shape) vs. `/Workspace/Shared/lhp/${env}/...` vs. `/Workspace/Repos/<sp>/Lakehouse_Plumber_${env}`. Pick one; allow override via DAB variable.
3. **First-deploy bootstrap**: first deploy per env needs `databricks repos create`; subsequent deploys use `repos update`. CI treats "folder already exists" as success.
4. **`sys.path` anchor point**: does Databricks auto-add `/Workspace/${env}/Lakehouse_Plumber` or `/Workspace/${env}/Lakehouse_Plumber/src` to `sys.path`? If the former, `src/lhp_watermark/` is NOT directly importable as `import lhp_watermark` — would need `src.lhp_watermark` or explicit `sys.path.insert(0, "/Workspace/${env}/Lakehouse_Plumber/src")` prelude. Validate via probe notebook.
5. **Constitution P2 amendment**: exact wording. Draft in MVP scope #7 is a starting point; ADR-002 author refines.

## Cross-References

- Constitution (to be amended): `.specs/constitution.md` — P2 (runtime availability)
- TD-008 opener: `docs/tech-debt/TD-008-runtime-availability-pattern.md`
- ADR-001: `docs/adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md` — explicitly scoped runtime-availability out; cross-refs ADR-002
- Upstream LHP `README.md`: "Zero runtime overhead — pure code generation, not a runtime framework."
- Upstream issue [#65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65): maintainer's stated scope for watermark feature
- Upstream LHP docs: https://lakehouse-plumber.readthedocs.io/databricks_bundles.html
- edp-data-engineering reference pattern:
  - `devops/templates/update_git_folder.yml` — `databricks repos update` step
  - `devops/templates/deploy_bundle.yml` — `databricks bundle deploy` step
  - `bundles/data_engineering/resources/jobs/batch_source_to_bronze_bundle.yml:13` — `notebook_path` + `source: WORKSPACE` shape
  - `notebooks/orchestration/bronze/source_to_bronze.py:1-5` — `from dlaccel.*` imports via sys.path auto-resolution
- Affected template: `src/lhp/templates/bundle/workflow_resource.yml.j2`
- Affected generator: `src/lhp/generators/bundle/workflow_resource.py`
- Refactor target: `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` (1563 LoC mechanical move)

## Follow-ups After ADR-002 Lands

1. **Implement Path 5**: mechanical refactor + template changes + generator change (MVP scope #1–#5). Single atomic PR on branch `feature/adr-002-runtime-availability` targeting `watermark`.
2. **Author formal ADR-002** at `docs/adr/ADR-002-lhp-runtime-availability.md` using this input as skeleton.
3. **Runbook** at `docs/runbooks/lhp-runtime-deploy.md` with CI/CD recipes.
4. **Constitution P2 amendment** in same PR (MVP scope #7).
5. **Update ADR-001 cross-reference** to point at resolved ADR-002.
6. **TD-008 closure**: mark resolved by ADR-002, link evidence.
7. **Validation**: render test + Databricks smoke test.
8. **Release notes migration note**: users with existing `${var.lhp_whl_path}` bundles must update `notebook_path` and drop the DAB var within one release window.
9. **Future (separate ticket)**: Mode A simple-inline-SQL variant of jdbc_watermark for upstream contribution consideration.
