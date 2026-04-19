# ADR-002: LHP Runtime Availability via Namespace Extraction + Git Folder Deploy

**Status**: Proposed
**Date**: 2026-04-18
**Deciders**: Fork maintainer (David Torres), to be reviewed
**Supersedes**: none
**Superseded-by**: none
**Relates-to**: ADR-001 (JDBC Watermark Parquet Post-Write Stats), TD-008 (Runtime Availability Pattern), Constitution §P2
**Input artifact**: [`docs/adr/inputs/adr-002-runtime-availability.md`](inputs/adr-002-runtime-availability.md)

---

## Context

The LHP fork ships a production-grade watermark runtime (`src/lhp/extensions/watermark_manager/`, 1563 LoC across 5 files) that provides six safety features critical to correct operation under retry, concurrency, and adversarial input:

1. `SQLInputValidator` per-type validators (strings, identifiers, numerics)
2. Delta concurrent-commit retry loop (jittered backoff, 5 attempts)
3. Terminal-state guards on both `mark_complete` and `mark_failed`
4. UTC session-timezone enforcement
5. Atomic MERGE with `num_affected_rows` validation
6. Structured LHP-WM-001..004 exception taxonomy

The runtime is imported from generated notebooks:

```python
# src/lhp/templates/load/jdbc_watermark_job.py.j2 (lines 19–24)
from lhp.extensions.watermark_manager import (
    TerminalStateGuardError,
    WatermarkManager,
)
from lhp.extensions.watermark_manager.runtime import derive_run_id
from lhp.extensions.watermark_manager.sql_safety import SQLInputValidator
```

…and the generated DAB workflow resource attaches a Python wheel as runtime vehicle:

```yaml
# src/lhp/templates/bundle/workflow_resource.yml.j2 (lines 7–12)
environments:
  - environment_key: lhp_env
    spec:
      environment_version: "2"
      dependencies:
        - {{ lhp_whl_path }}
```

### Upstream design intent (authoritative)

Upstream LHP `README.md`: **"Zero runtime overhead — pure code generation, not a runtime framework."**
Upstream explicit anti-goal: *"it should NOT… Introduce runtime overhead (no compiling configurations at runtime to generate pipelines)."*

Audit of all upstream Jinja templates under `src/lhp/templates/load/`, `transform/`, `write/`: **zero** `from lhp.*` imports. Generated notebooks use only `pyspark`, `pyspark.pipelines as dp`, standard library, and user-provided modules. Upstream has no watermark template at all (`src/lhp/templates/load/jdbc_watermark_job.py.j2` does not exist upstream).

### Maintainer intent for watermark support

Upstream issue [#65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65) (Mmodarre, 2025-12-17, OPEN, enhancement):

> *"New feature to create and update High watermark table. This feature will help systems which use external tools (Azure Data Factory, etc.) for ingestion. LHP will create and maintain a table of high watermarks for tables which gets loaded by external integration tools so they can use the value as a high watermark for next ingestions."*

Scope: LHP as **producer/maintainer of a watermark table** that external orchestrators consume. **Not** a Python runtime class instantiated inside the pipeline.

### Gap

The fork's `WatermarkManager` Python class plus wheel-dependency deployment violates upstream intent. Constitution P2 already codifies the wheel-avoidance principle but its literal wording restricts imports to `lhp.extensions.*` / `lhp.runtime.*`, conflicting with any namespace-extraction remedy.

---

## Decision

**Adopt Path 5: Namespace extraction + Git folder deploy.**

1. Rename `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` — a **distinct top-level Python package**, sibling to `src/lhp/`, *not* a submodule of it.
2. LHP (`src/lhp/`) reverts to pure code generator. Zero runtime role.
3. Generated extraction notebooks import the runtime from the external library: `from lhp_watermark.watermark_manager import WatermarkManager`.
4. Runtime deploys via Databricks Git Folder synced by CI/CD (`databricks repos update … --branch <deploy-branch>`), not via a Python wheel attached to the workflow.
5. The generated DAB workflow resource drops its `environments` / `dependencies` block and references notebook paths resolvable inside the synced git folder.

### Concrete deployment shape

Repo layout (post-Path-5):

```
Lakehouse_Plumber/                     ← Databricks Git Folder root
├── src/
│   ├── lhp/                            ← pure code generator (no runtime role)
│   └── lhp_watermark/                  ← runtime library (distinct package)
│       ├── __init__.py
│       ├── watermark_manager.py        ← was _manager.py (leading underscore dropped)
│       ├── sql_safety.py
│       ├── exceptions.py
│       └── runtime.py
├── generated/${env}/                   ← pushed per CI run (ephemeral branch)
│   └── {pipeline}/__lhp_extract_{action}.py
└── resources/lhp/                      ← DAB resource YAMLs
```

CI pipeline per environment:

```bash
lhp generate --env ${env}
git add generated/${env}/ resources/lhp/${env}/
git commit -m "deploy(${env}): regen for ${COMMIT_SHA}"
git push origin deploy/${env}/${COMMIT_SHA}

databricks --profile ${PROFILE} repos update \
  /Workspace/${env}/Lakehouse_Plumber --branch deploy/${env}/${COMMIT_SHA}

databricks --profile ${PROFILE} bundle deploy -t ${env}
```

Generated workflow resource YAML (post-change):

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

Generated extraction notebook (only the import lines change from current template):

```python
from lhp_watermark.watermark_manager import (
    TerminalStateGuardError,
    WatermarkManager,
)
from lhp_watermark.runtime import derive_run_id
from lhp_watermark.sql_safety import SQLInputValidator
```

Databricks Runtime adds the synced Git Folder root to `sys.path` for notebook-task execution; `src/lhp_watermark/` is reachable as the top-level `lhp_watermark` package *if the anchor is `…/Lakehouse_Plumber/src/`*. If the anchor is `…/Lakehouse_Plumber/` only, a one-line `sys.path.insert(0, "/Workspace/${env}/Lakehouse_Plumber/src")` prelude is emitted by the template. Resolved empirically in Phase 4 probe (see Open Questions Q1).

### Why `lhp_watermark` (not `watermark_runtime`, not `cnh_watermark`)

- **`lhp_` prefix**: ecosystem discoverability for LHP users.
- **Underscore separator (not dot)**: Python namespace semantics — `lhp_watermark` is a distinct top-level package, not a submodule of `lhp`. Satisfies the strict reading of upstream intent: `from lhp_watermark.*` is NOT an import from `lhp.*`.
- **Single-word domain** (`watermark`): matches the feature scope precisely.
- **Room to grow**: future siblings `lhp_rest_api`, `lhp_kafka_runtime` follow the same convention if other runtime libraries emerge.

---

## Alternatives considered

| Path | Summary | Why rejected |
|---|---|---|
| **1** — Strict alignment, plain inline `spark.sql` | Replace class with inline SQL; orchestrator handles retries/safety | Discards all six safety features. Rebuilding them in an orchestrator-side library is equivalent complexity, zero net gain. Data-loss risk under retry. |
| **2 (E1)** — Inline `WatermarkManager` source into every generated notebook | Preserves safety; ~1000 LoC copied per notebook | Violates "one runtime instance per workspace" preference; multi-hundred-LoC duplicates bloat every bundle; upgrade propagation is manual across bundles. |
| **3 (prior Option P)** — Keep `lhp.extensions.*` namespace; deploy via git folder only | Preserves code; drops wheel only | Generated code still imports `lhp.*`, which is upstream's explicit anti-pattern. Structurally identical to Path 5 otherwise, but loses the clean-namespace benefit. |
| **4 (A-series)** — Wheel variants (PyPI, UC Volume, workspace_file, git URL) | Various wheel hosting mechanisms | Any wheel lifecycle (build, host, install) adds surface area Path 5's git-folder sync eliminates. |
| **E4** — UC Functions / stored procedures | SQL-only runtime | Requires Python→SQL translation per method; loses Python class ergonomics; minimal safety-feature gain vs Path 5. |

### Trade-off table

| Criterion | P1 (strict) | P2 (inline) | P3 (Option P as-is) | **P5 (this ADR)** |
|---|---|---|---|---|
| No `lhp.*` imports in generated code | ✅ | ✅ | ❌ | ✅ (`lhp_watermark.*` ≠ `lhp.*`) |
| LHP stays pure code generator | ✅ | ✅ | ❌ | ✅ |
| All six safety features preserved | ❌ rebuilt in orchestrator | ✅ | ✅ | ✅ |
| Upstream issue #65 scope compatible | ✅ | ✅ | ⚠️ | ✅ |
| User-sunk runtime code preserved | ❌ rewrite | ✅ | ✅ | ✅ (mechanical rename) |
| Git folder deploy (no wheel) | N/A | ✅ | ✅ | ✅ |
| One runtime instance per workspace | N/A | ❌ (per bundle) | ✅ | ✅ |
| Future upstream contribution path | Direct | Hard | Unlikely | Mode-A additive contribution possible |
| Constitution P2 literal text | ✅ | ✅ | ⚠️ conflict | ✅ (with amendment §Compliance) |
| Migration cost | Complete rewrite | Big refactor | CI change only | **Mechanical rename + CI change** |

---

## Consequences

### Positive

- **Upstream alignment**: generated code contains no `lhp.*` imports; LHP reverts to upstream's stated role of pure code generator.
- **Safety features preserved**: all six `WatermarkManager` safety features remain intact through a mechanical rename; no behavior change.
- **One runtime per workspace**: upgrades propagate via `databricks repos update`, not via rebuild-and-redeploy of every bundle's wheel.
- **Wheel lifecycle eliminated**: no wheel build step, no wheel hosting (UC Volume, PyPI, workspace_file), no `environments.dependencies` block. Surface area reduced.
- **Upstream contribution path opens**: Mode-A (simple inline SQL implementation of issue #65's narrower scope) becomes additive upstream contribution, independent of this fork's `lhp_watermark` package.

### Negative

- **Deployment topology change**: CI/CD must orchestrate `databricks repos update` alongside `databricks bundle deploy`. Platform engineers need runbook access.
- **Constitution amendment required**: P2 bullet 1 wording must change from `lhp.extensions.*` / `lhp.runtime.*` to a broader "runtime libraries (e.g., `lhp_watermark.*`)" formulation. See §Compliance.
- **User bundle migration**: existing bundles declaring `var.lhp_whl_path` must drop the variable and update `notebook_path`. One-release grace window enforced via deprecation warning (Constitution P1).
- **First-deploy bootstrap**: first deployment per environment needs `databricks repos create`; subsequent deploys use `repos update`. CI treats "folder already exists" as success. Documented in runbook.

### Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Databricks Runtime `sys.path` anchor is repo-root, not `…/src/` | M | H | Probe notebook in Phase 4 resolves empirically; template emits `sys.path.insert` prelude conditionally |
| `databricks repos update` rejects branch names with slashes (`deploy/${env}/${SHA}`) | L | M | Verify in Phase 4; fallback to flat branch naming (`deploy-${env}-${SHA}`) if needed |
| Race between `repos update` and `bundle deploy` | L | M | Sequential execution in CI; fail-fast on first error |
| Constitution amendment rejected in review | L | H | Pre-circulate amendment wording; this ADR documents the exact change |
| `lhp_watermark` name collides with future upstream naming | L | L | Coordinate via issue #65 thread; rename feasible pre-merge |

---

## Open Questions (with recommendations)

1. **`sys.path` anchor point** — Does Databricks Runtime auto-add `/Workspace/${env}/Lakehouse_Plumber` or `/Workspace/${env}/Lakehouse_Plumber/src` to `sys.path` for notebook-task execution?
   **Recommendation**: Resolve empirically via probe notebook (Phase 4 T4.1). Plan emits `sys.path.insert(0, "/Workspace/${var.env}/Lakehouse_Plumber/src")` prelude conditionally based on probe result.

2. **Git folder path convention** — `/Workspace/${env}/Lakehouse_Plumber` (edp pattern) vs `/Workspace/Shared/lhp/${env}/…` vs `/Workspace/Repos/<sp>/…`?
   **Recommendation**: Default `/Workspace/${var.env}/Lakehouse_Plumber` (matches edp, maximizes pattern reuse). Parameterize via DAB variable `lhp_git_folder_root` so users can override without template changes.

3. **Generated artifacts branching** — Commit `generated/${env}/**` to persistent `release/${env}` branch vs ephemeral `deploy/${env}/${COMMIT_SHA}` per CI run?
   **Recommendation**: **Ephemeral** `deploy/${env}/${COMMIT_SHA}`. Rationale: upstream LHP convention is regenerate-in-CI (no artifacts in source control); ephemeral branches are deterministic per commit, avoid branch accumulation, and survive parallel CI runs.

4. **Notebook path shape** — `${var.env}` vs `${bundle.target}` in the emitted `notebook_path`?
   **Recommendation**: **`${var.env}`**. `bundle.target` is the DAB target name, which may or may not equal the environment name. Using `var.env` keeps the notebook path consistent with the git folder path (also `var.env`-anchored).

5. **Constitution P2 amendment wording** — Narrow edit to bullet 1, or broader pattern documentation?
   **Recommendation**: **Narrow**. Amend only P2 bullet 1 (see §Compliance for exact wording). ADR-002 itself serves as the pattern documentation; constitution churn stays minimal.

---

## Compliance

### Constitution mapping

| Principle | Path 5 compliance |
|---|---|
| P1 Additive Over Rewrite | Deprecation warning for `${var.lhp_whl_path}` with documented grace window; no hard-break |
| P2 No Wheel Until Justified | Satisfied in spirit; **literal text requires amendment** (see below) |
| P3 Thin Notebook, Fat Runtime | Preserved — runtime still in importable package; notebook still thin |
| P4 FlowGroup = Entity | Unchanged |
| P5 Registry source of truth | Preserved (watermark advance semantics unchanged) |
| P6 Retry Safety By Construction | Preserved (Delta concurrent-commit retry, terminal-state guards intact) |
| P7 Declarative Tuning Passthrough | Unchanged |
| P8 Unity Catalog + Secret Scopes | Unchanged |
| P9 Security-Exact SQL Interpolation | Preserved (`SQLInputValidator` moves with the package) |
| P10 Human Validation At Level Boundaries | Enforced via six checkpoints in `tasks/plan.md` |

### Required Constitution amendment

**File**: `.specs/constitution.md`
**Section**: P2 — No Wheel Until Justified
**Version bump**: 1.0 → 1.1
**Change**: bullet 1 only.

**Before**:
> Generated notebooks import `lhp.extensions.*` and `lhp.runtime.*` from PYTHONPATH, not from an installed wheel.

**After**:
> Generated notebooks import runtime libraries (e.g., `lhp_watermark.*`, `lhp.extensions.*`, `lhp.runtime.*`) from PYTHONPATH, not from an installed wheel. Runtime libraries live as distinct top-level Python packages sibling to `lhp/`; submodules of `lhp/` (e.g., `lhp.extensions.*`) remain permitted for backward compatibility during migration windows.

Rationale paragraph ("Fewer moving parts. Faster iteration. Packaging is an operational concern, not an architectural one.") unchanged.

### Waivers declared

None. All compliance achieved via the P2 amendment above.

---

## Implementation

See [`tasks/plan.md`](../../tasks/plan.md) and [`tasks/todo.md`](../../tasks/todo.md) for full phase plan. Summary:

- **Phase 0** — this ADR + constitution amendment + runbook stub
- **Phase 1** — namespace extraction (`git mv` + test relocation)
- **Phase 2** — bundle template + generator refactor (drop wheel block, update notebook_path)
- **Phase 3** — packaging config (pyproject includes both top-level packages)
- **Phase 4** — Databricks smoke validation (probe for Q1; E2E smoke with all six safety features exercised)
- **Phase 5** — rollout docs (runbook body, TD-008 closure, ADR-001 cross-ref update, release notes)

Six checkpoints (C0–C5), each requiring explicit human approval per P10.

Target branch: `feature/adr-002-runtime-availability` (current branch). Single atomic PR targeting `watermark`.

---

## Cross-references

- Input artifact: [`docs/adr/inputs/adr-002-runtime-availability.md`](inputs/adr-002-runtime-availability.md)
- Phase plan: [`tasks/plan.md`](../../tasks/plan.md)
- Runbook: [`docs/runbooks/lhp-runtime-deploy.md`](../runbooks/lhp-runtime-deploy.md)
- TD-008 opener: [`docs/tech-debt/TD-008-runtime-availability-pattern.md`](../tech-debt/TD-008-runtime-availability-pattern.md)
- ADR-001: [`docs/adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md`](ADR-001-jdbc-watermark-parquet-post-write-stats.md) (cross-ref updated in Phase 5)
- Constitution: [`.specs/constitution.md`](../../.specs/constitution.md) (P2 amendment in Phase 0)
- Upstream README: "Zero runtime overhead — pure code generation, not a runtime framework"
- Upstream issue [#65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65): maintainer's stated scope for watermark feature
- edp-data-engineering reference pattern: `devops/templates/update_git_folder.yml`, `devops/templates/deploy_bundle.yml`
- Affected template: `src/lhp/templates/bundle/workflow_resource.yml.j2`
- Affected generator: `src/lhp/generators/bundle/workflow_resource.py`
- Affected template: `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- Refactor target: `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` (1563 LoC mechanical move)
