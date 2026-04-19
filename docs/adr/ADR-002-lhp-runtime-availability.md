# ADR-002: LHP Runtime Availability via Namespace Extraction + DAB Workspace File Sync

**Status**: Proposed
**Date**: 2026-04-18
**Amended**: 2026-04-19 — deploy mechanism simplified from Databricks Git Folder (`databricks repos update`) to DAB workspace-file sync (`databricks bundle deploy` alone). Rationale: `databricks bundle deploy` already syncs repo contents including `src/lhp_watermark/` to `${workspace.file_path}/src/…`; no reason to layer a second deploy mechanism. Single-mechanism deploy keeps watermark-branch parity and drops ~40 test/fixture changes that a git-folder anchor would have required across `pipeline_resource.yml.j2`. Title, Decision, Deployment shape, Consequences, Risks, Open Questions below reflect the amendment. Alternatives table and Constitution compliance unchanged.
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

**Adopt Path 5: Namespace extraction + DAB workspace-file sync.**

1. Rename `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` — a **distinct top-level Python package**, sibling to `src/lhp/`, *not* a submodule of it.
2. LHP (`src/lhp/`) reverts to pure code generator. Zero runtime role.
3. Generated extraction notebooks import the runtime from the external library: `from lhp_watermark import WatermarkManager` (package-level re-export; sub-module form also valid).
4. Runtime deploys via the existing DAB workspace-file sync (`databricks bundle deploy`). `src/lhp_watermark/` ships as a regular workspace file alongside generated notebooks; no Python wheel, no separate `databricks repos update` step.
5. The generated DAB workflow resource drops its `environments` / `dependencies` block (wheel attachment) but keeps the existing `${workspace.file_path}/generated/${bundle.target}/…` notebook_path anchor. Only the wheel block and `environment_key` are removed; `source: WORKSPACE` is added.

### Concrete deployment shape

Repo layout (post-Path-5):

```
Lakehouse_Plumber/                     ← DAB bundle root (also git repo)
├── src/
│   ├── lhp/                            ← pure code generator (no runtime role)
│   └── lhp_watermark/                  ← runtime library (distinct package)
│       ├── __init__.py
│       ├── watermark_manager.py        ← was _manager.py (leading underscore dropped)
│       ├── sql_safety.py
│       ├── exceptions.py
│       └── runtime.py
├── generated/${bundle.target}/         ← written by `lhp generate`; synced by `databricks bundle deploy`
│   └── {pipeline}_extract/__lhp_extract_{action}.py
└── resources/lhp/                      ← DAB resource YAMLs
```

CI pipeline per environment:

```bash
lhp generate --env ${env}
databricks --profile ${PROFILE} bundle deploy -t ${env}
```

Single command. DAB syncs the repo (including `src/lhp_watermark/` and `generated/${bundle.target}/`) to the workspace under `${workspace.file_path}/…` and registers the workflow + DLT resources. No separate git-folder step, no ephemeral-branch git push.

Generated workflow resource YAML (post-change — minimal diff from watermark branch):

```yaml
resources:
  jobs:
    {{ pipeline_name }}_workflow:
      name: {{ pipeline_name }}_workflow
      tasks:
        - task_key: extract_{{ action_name }}
          notebook_task:
            notebook_path: ${workspace.file_path}/generated/${bundle.target}/{{ pipeline_name }}_extract/__lhp_extract_{{ action_name }}
            source: WORKSPACE
```

Only three diffs from watermark branch template:
1. The entire `environments:` block (wheel attachment) is removed.
2. `environment_key: lhp_env` is removed from each extraction task.
3. `source: WORKSPACE` is added under each extraction task's `notebook_task`.

The `notebook_path` anchor (`${workspace.file_path}/generated/${bundle.target}/…`) is **unchanged** — matching the `pipeline_resource.yml.j2` convention app-wide.

Generated extraction notebook (only the import lines change from current template):

```python
from lhp_watermark import (
    TerminalStateGuardError,
    WatermarkManager,
)
from lhp_watermark.runtime import derive_run_id
from lhp_watermark.sql_safety import SQLInputValidator
```

`src/lhp_watermark/` lands at `${workspace.file_path}/src/lhp_watermark/` via DAB workspace-file sync. Databricks Runtime's `sys.path` for notebook-task execution may or may not include `${workspace.file_path}/src/` automatically. Resolved empirically in Phase 4 probe (see Open Questions Q1). If the anchor is `${workspace.file_path}` only, a one-line `sys.path.insert(0, "${workspace.file_path}/src")` prelude is emitted in the generated notebook template.

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
| Deploy without wheel | N/A | ✅ | ✅ | ✅ (via DAB workspace-file sync; amended 2026-04-19) |
| One runtime instance per bundle/workspace | N/A | ❌ (per bundle) | one per workspace (git folder) | **one per bundle** (workspace files; simpler, matches watermark branch) |
| Future upstream contribution path | Direct | Hard | Unlikely | Mode-A additive contribution possible |
| Constitution P2 literal text | ✅ | ✅ | ⚠️ conflict | ✅ (with amendment §Compliance) |
| Migration cost | Complete rewrite | Big refactor | CI change only | **Mechanical rename; minimal template/generator diff** |

---

## Consequences

### Positive

- **Upstream alignment**: generated code contains no `lhp.*` imports; LHP reverts to upstream's stated role of pure code generator.
- **Safety features preserved**: all six `WatermarkManager` safety features remain intact through a mechanical rename; no behavior change.
- **Single-mechanism deploy**: `databricks bundle deploy` handles both runtime library and generated notebooks. No separate `databricks repos update` step, no ephemeral deploy branches, no bootstrap `repos create`. Matches watermark-branch operational model.
- **Wheel lifecycle eliminated**: no wheel build step, no wheel hosting (UC Volume, PyPI, workspace_file), no `environments.dependencies` block. Surface area reduced.
- **Upstream contribution path opens**: Mode-A (simple inline SQL implementation of issue #65's narrower scope) becomes additive upstream contribution, independent of this fork's `lhp_watermark` package.
- **Minimal test/fixture churn**: keeping the `${workspace.file_path}/generated/${bundle.target}` anchor avoids touching `pipeline_resource.yml.j2` and ~40 bundle tests/fixtures/docs that assert that shape app-wide.

### Negative

- **Constitution amendment required**: P2 bullet 1 wording must change from `lhp.extensions.*` / `lhp.runtime.*` to a broader "runtime libraries (e.g., `lhp_watermark.*`)" formulation. See §Compliance.
- **User bundle migration**: existing bundles declaring `var.lhp_whl_path` must drop the variable. One-release grace window enforced via deprecation warning (Constitution P1).
- **Runtime lib lives per-bundle, not per-workspace**: each bundle ships its own `src/lhp_watermark/` under `${workspace.file_path}`. Concurrent bundles using different `lhp_watermark` versions coexist without contention but also without cross-bundle sharing. Acceptable: upgrade propagation is a normal bundle redeploy.
- **`sys.path` anchor uncertainty**: Databricks Runtime may or may not auto-add `${workspace.file_path}/src/` to `sys.path` for notebook-task execution. Phase 4 probe resolves empirically; template emits a conditional one-line prelude if needed.

### Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Databricks Runtime `sys.path` does not include `${workspace.file_path}/src/` for notebook-task execution | M | H | Phase 4 probe resolves empirically; generated template emits `sys.path.insert(0, "${workspace.file_path}/src")` prelude conditionally |
| DAB workspace-file sync omits `src/` (e.g., due to `sync.exclude` in bundle config) | L | H | Audit `databricks.yml` of representative bundles in Phase 4; document exclude/include patterns required for `src/` inclusion in runbook |
| Constitution amendment rejected in review | L | H | Pre-circulated; already committed in Phase 0 |
| `lhp_watermark` name collides with future upstream naming | L | L | Coordinate via issue #65 thread; rename feasible pre-merge |

---

## Open Questions (with recommendations)

1. ~~**`sys.path` anchor point**~~ — **RESOLVED 2026-04-19 via Phase 4 T4.1 probe** (Databricks Runtime `client.5.1`, serverless). Probe job `sys_path_probe_job` in the `scooty_puff_junior` Wumbo bundle showed:
   - Plain `import lhp_watermark` FAILS (`${workspace.file_path}` not on `sys.path`; only the notebook's own directory is)
   - `sys.path.insert(0, "<workspace_file_root>")` + import SUCCEEDS
   - `<workspace_file_root>` derived from the running notebook path by splitting on `/files/` — robust against bundle-specific root prefixes
   **Implementation (commit `a1c12852`)**: generated `jdbc_watermark_job.py.j2` template emits a `_lhp_watermark_bootstrap_syspath()` helper (wrapped in a `def` + module-level call, not a top-level `try:`, so template-contract tests that locate the extraction try block continue to resolve correctly). Helper is a no-op when `dbutils` is absent (static template-render tests); in Databricks it prepends the bundle files root to `sys.path` before the `lhp_watermark.*` imports.

2. ~~**Git folder path convention**~~ — **Resolved by 2026-04-19 amendment**: no git folder in deploy path. DAB workspace-file sync handles delivery.

3. ~~**Generated artifacts branching**~~ — **Resolved by 2026-04-19 amendment**: no deploy branches. `lhp generate` + `databricks bundle deploy` per environment.

4. ~~**Notebook path shape `${var.env}` vs `${bundle.target}`**~~ — **Resolved by 2026-04-19 amendment**: keep `${bundle.target}` (existing app-wide convention in `pipeline_resource.yml.j2`); no new anchor introduced.

5. **Constitution P2 amendment wording** — Narrow edit to bullet 1, or broader pattern documentation?
   **Recommendation (applied in Phase 0 commit `0bc88d28`)**: **Narrow**. Amended only P2 bullet 1 — see §Compliance for exact wording. ADR-002 itself serves as the pattern documentation; constitution churn stays minimal.

6. **Should a user `sync.exclude` override ever hide `src/` from the DAB bundle sync?**
   **Recommendation**: No. Phase 5 runbook must call this out. If a bundle excludes `src/`, the runtime library won't land in the workspace and imports fail. Detectable at `databricks bundle validate` time via a check the generator can suggest in its migration notes.

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

## Phase 4 Evidence (2026-04-19)

End-to-end smoke proved Path 5 Option A operational on Databricks serverless runtime `client.5.1`.

**Test bundle**: `scooty_puff_junior` at [`github.com/dwtorres/Wumbo`](https://github.com/dwtorres/Wumbo). AdventureWorks source data on Supabase Postgres via JDBC.

**Workspace**: `https://dbc-8e058692-373e.cloud.databricks.com`, profile `lhp-deploy-sp` (service principal).

**T4.1 — `sys.path` probe** (run `135873449992972`, `561719421793073`):
- Plain `import lhp_watermark` fails — `${workspace.file_path}` not on `sys.path` for notebook-task execution.
- `sys.path.insert(0, "<workspace_file_root>")` then import succeeds; `<workspace_file_root>` derived by splitting `notebookPath()` on `/files/`.
- Verdict: prelude required. Implemented in template commit `a1c12852`.

**T4.2 — bundle deploy sequence**:
```
databricks --profile lhp-deploy-sp bundle validate -t dev  # → Validation OK!
databricks --profile lhp-deploy-sp bundle deploy   -t dev  # → Deployment complete!
```
Single-mechanism deploy confirmed — no `databricks repos update` step; DAB workspace-file sync delivers both `lhp_watermark/` (vendored at bundle root) and the generated extraction notebooks under `${workspace.file_path}/`.

**T4.3 — end-to-end extraction workflow** (run `517674443949620`, duration 4m 19s):
```
databricks --profile lhp-deploy-sp bundle run -t dev jdbc_ingestion_workflow
```
Result: `TERMINATED SUCCESS`. Four extraction tasks executed (`load_sales_order_header_jdbc`, `load_product_jdbc`, `load_transaction_history_jdbc`, `load_sales_order_detail_jdbc`) followed by the DLT pipeline task. Each extraction task:
- Imported `lhp_watermark.*` via the `sys.path` prelude
- Exercised `WatermarkManager` safety features: `insert_new`, `mark_landed`, `mark_complete` terminal-state guards, `SQLInputValidator`, UTC session enforcement, JDBC→Parquet write
- Advanced the watermark row to a terminal success state

**User-bundle vendoring** (for Option A under user-bundle semantics, where the bundle repo is separate from the LHP repo):
- Runtime delivery: copy `src/lhp_watermark/` from the LHP repo into the user-bundle repo root as `lhp_watermark/`. DAB sync uploads it to `${workspace.file_path}/lhp_watermark/` alongside `generated/` notebooks.
- Requires the user bundle to be a git repo or have explicit `sync.include` patterns so DAB knows which files to upload (Wumbo case: `git init` + populated `.gitignore` resolved the "no files to sync" warning).
- Follow-up: `lhp sync-runtime` CLI subcommand to automate vendoring from an installed `lhp_watermark` distribution (Phase 5 backlog).

**Constitution compliance verified**: P1 (additive — wheel still permitted via `environments.dependencies` in bundles that opt in), P2 (wheel not required), P3 (runtime in importable package; notebook thin), P5/P6/P9 (safety features preserved through mechanical rename).

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
