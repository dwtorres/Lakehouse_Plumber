# Runbook: B2 for_each Rollout — Watermark Scale-Out

**Status**: Active (B2 codegen merged, devtest sign-off 2026-04-25 V1-V5 PASS).
**Owner**: dwtorres@gmail.com.
**Scope**: Adopt `execution_mode: for_each` on a `jdbc_watermark_v2` flowgroup to
replace N static extract tasks with the B2 three-task DAB topology
(`prepare_manifest → for_each_ingest → validate`).
**Profile**: `dbc-8e058692-373e` (per project memory).
**Related**: [b2-watermark-scale-out-design.md](../planning/b2-watermark-scale-out-design.md),
[devtest-validation-adr-003-phase-a-c.md](./devtest-validation-adr-003-phase-a-c.md),
[tier-2-load-group-rollout.md](./tier-2-load-group-rollout.md).

---

## Overview

B2 is LHP's emission pattern for `jdbc_watermark_v2` flowgroups that declare
`workflow.execution_mode: for_each`. Instead of emitting one static Databricks
job task per table, LHP generates a three-task DAB workflow:

1. **`prepare_manifest`** — collects the action list at runtime, writes one row
   per action to the `b2_manifests` Delta table, and emits an `iterations`
   taskValue consumed by step 2.
2. **`for_each_ingest`** — fans out over the manifest entries in parallel,
   running the same `jdbc_watermark_job.py` worker template for each action.
3. **`validate`** — reads the manifest batch, asserts all iterations completed,
   and optionally runs JDBC-rows-read vs landed-parquet-rows parity checks.

This runbook is for platform engineers and data engineers who own or operate
pipelines with 50 or more tables per flowgroup. Read it before enabling
`for_each` on any flowgroup in devtest, qa, or prod.

Enable `execution_mode: for_each` when a single flowgroup manages roughly 50 or
more tables and the team needs per-table retry granularity and per-iteration
visibility in the Databricks Jobs UI. At that scale, the static N-task topology
creates job graphs too large for DAB to manage efficiently and makes failed-task
triage tedious. The B2 topology keeps job graph size constant (three tasks)
regardless of action count.

The trade-off is one extra ~10-second `prepare_manifest` task per job run.
Against a typical B2 wall-clock of 20-30 minutes at N=50-300, this overhead is
negligible. Flowgroups with fewer than ~20 tables are typically better served by
the legacy static emission, which requires no manifest infrastructure.

---

## Prerequisites

### Tier 2 (load_group axis) deployed

Tier 2 load_group registry-axis support must be deployed per environment before
B2 can run. The devtest sign-off for Tier 2 completed 2026-04-25 (V1-V5 PASS).
Follow [`docs/runbooks/devtest-validation-adr-003-phase-a-c.md`](./devtest-validation-adr-003-phase-a-c.md)
and [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md)
to deploy and sign off Tier 2 per environment before proceeding here.

### B2 codegen merged

The B2 codegen must be present on the LHP fork integration branch (`watermark`).
Verify:

```bash
git log --oneline | grep -i "b2\|for_each\|watermark scale"
```

### Workspace and metadata schema

- Databricks workspace at devtest, qa, or prod with `metadata.<env>_orchestration`
  schema present (pre-provisioned by platform admin per the provisioning matrix
  in `devtest-validation-adr-003-phase-a-c.md` §Prerequisites).
- The `metadata.<env>_orchestration.watermarks` table auto-created by
  `WatermarkManager.__init__` on first extraction task run. No manual DDL required.

### `b2_manifests` table

The `b2_manifests` table is auto-created on the first `prepare_manifest` run via
`CREATE TABLE IF NOT EXISTS`. No manual DDL is required. The table lands at
`<wm_catalog>.<wm_schema>.b2_manifests`.

---

## Flowgroup adoption walkthrough

### 4.1 — Declare `execution_mode: for_each`

Add `workflow.execution_mode: for_each` to the flowgroup YAML:

```yaml
pipeline: crm_bronze
flowgroup: product_ingestion
workflow:
  execution_mode: for_each
actions:
  # ... your jdbc_watermark_v2 actions
```

Flowgroups that do not declare this field keep the legacy static emission
unchanged. Mixed-mode within one pipeline is blocked by LHP-CFG-033 / LHP-CFG-034.

### 4.2 — Set concurrency

```yaml
workflow:
  execution_mode: for_each
  concurrency: 10   # integer 1–100; default: min(action_count, 10)
```

Higher concurrency increases parallelism but also Delta concurrent-commit
pressure on `b2_manifests`. Start at 10 and increase only if MERGE retries
appear in `prepare_manifest` logs.

### 4.3 — Set max_retries (optional)

```yaml
workflow:
  execution_mode: for_each
  concurrency: 10
  max_retries: 1   # default: 1; set to 0 to disable DAB-level retry per iteration
```

DAB retries failed iterations up to `max_retries` times. At default 1, a
transient JDBC timeout is retried once before the iteration is marked failed.

### 4.4 — Enable parity check (optional)

```yaml
workflow:
  execution_mode: for_each
  parity_check: true
```

When `true`, the `validate` task compares JDBC-rows-read (from the manifest)
against landed-parquet-rows for each action. Mismatches are reported in
`parity_mismatches` in the validate summary and raise `LHP-VAL-04B`.
Recommended for initial rollout; can be disabled once the pipeline is stable.

### 4.5 — Seed load_group registry

If Tier 2 has not yet been seeded for this flowgroup's source system, run:

```bash
lhp seed-load-group \
  --env <env> \
  --pipeline <pipeline-name> \
  --flowgroup <flowgroup-name>
```

Refer to the Tier 2 plan §Seeding for full usage. Skip if the load_group
rows are already present (verify with `SELECT COUNT(*) FROM
metadata.<env>_orchestration.load_groups WHERE pipeline = '<pipeline>'`).

### 4.6 — Generate

```bash
lhp generate -e <env>
```

Inspect the generated output under `generated/<env>/<pipeline>/` — you should
see `prepare_manifest.py`, `validate.py`, and a `worker/` directory containing
the per-action worker notebook(s).

### 4.7 — Deploy

```bash
export DATABRICKS_TF_EXEC_PATH=$(which terraform)
export DATABRICKS_TF_VERSION=$(terraform --version | head -1 | awk '{print $2}' | sed 's/^v//')

databricks bundle deploy --target <env> -p dbc-8e058692-373e
```

See [lhp-runtime-deploy.md](./lhp-runtime-deploy.md) for the full deploy
sequence including runtime vendoring and the bundled-Terraform GPG workaround.

### 4.8 — Run the workflow

```bash
databricks bundle run <pipeline>_workflow --target <env> -p dbc-8e058692-373e
```

Observe task progression in the Databricks Jobs UI:
`prepare_manifest` → `for_each_ingest` (with iteration sub-tasks) → `validate`.

---

## Validation (V-checklist for B2)

After the workflow completes, verify the following pass criteria. These mirror
the V1-V5 criteria used in the 2026-04-25 devtest sign-off.

| # | Criterion | How to verify |
|---|-----------|---------------|
| V1 | Workflow ran with exactly three top-level tasks: `prepare_manifest`, `for_each_ingest`, `validate`. | Databricks Jobs UI — task graph shows three nodes. |
| V2 | `for_each_task` spawned the expected iteration count (one per action). | Click `for_each_ingest` in the Jobs UI — iteration list shows N entries matching action count. |
| V3 | All iterations completed; `b2_manifests` row count for `batch_id` matches expected. | `SELECT COUNT(*) FROM <wm_catalog>.<wm_schema>.b2_manifests WHERE batch_id = '<batch_id>' AND status = 'completed'` equals action count. |
| V4 | `watermarks` rows emitted with correct `load_group` composite key (`<pipeline>::<flowgroup>`). | `SELECT load_group, COUNT(*) FROM metadata.<env>_orchestration.watermarks WHERE load_group LIKE '<pipeline>::%' GROUP BY load_group` — one group per table. |
| V5 | `validate` task exited with `status: pass` in summary JSON. | Task output in Jobs UI or cluster log: look for `"status": "pass"` in the validate summary block. |

---

## R12 strict-`>` operator awareness

The B2 worker inherits the same strict `WHERE wm_col > max_wm` predicate used
by the existing `jdbc_watermark_v2` extraction path. The following items are
hard-gate checks before enabling `for_each` on a production flowgroup.

**Sub-second precision required.** Source watermark columns should carry
millisecond or microsecond precision. Second-only sources risk losing rows on
every re-run if writes share the same second as `max_wm`. Verify timestamp
precision in the source schema before adoption.

**Late-arriving records at exact `max_wm` timestamp.** Strict `>` skips them.
Operators may opt into a delta-buffer subtraction in the source query (e.g.
`WHERE wm > max_wm - INTERVAL 5 MINUTE`) for slow-emit sources. Document the
buffer choice in flowgroup YAML comments.

**Same-second collisions on high-volume second-precision sources.** Composite
tiebreaker `(wm_col, pk) > (last_wm, last_pk)` is a follow-up if operators
report row loss. Track the issue in the operator hand-off document.

**UTC normalization.** `WatermarkManager._ensure_utc_session()` enforces
`spark.sql.session.timeZone='UTC'` before any DML. The B2 worker path inherits
this via the same prelude. Confirm source timestamps are stored in UTC or are
explicitly cast before the watermark column comparison.

---

## Manifest retention cadence

The `b2_manifests` table stores one row per action per batch. Unbounded growth
is prevented by an automatic DELETE at every batch start.

| Operation | Who runs it | Frequency | Notes |
|-----------|-------------|-----------|-------|
| **30-day DELETE** | `prepare_manifest.py` automatically | Every batch start | Deletes rows older than 30 days; logs deleted-row count. No operator action required. |
| **VACUUM** | Operator-scheduled (out-of-band) | Weekly recommended | `VACUUM <wm_catalog>.<wm_schema>.b2_manifests RETAIN 168 HOURS` |
| **OPTIMIZE** | Operator-scheduled (out-of-band) | Weekly recommended, after VACUUM | `OPTIMIZE <wm_catalog>.<wm_schema>.b2_manifests` |

Steady-state table size at fleet scale: approximately 216,000 rows
(300 actions × 24 hourly batches/day × 30 days).

---

## Watermarks registry maintenance

Apply the same recurring maintenance policy to the watermarks registry:

- **Daily**: `VACUUM metadata.<env>_orchestration.watermarks RETAIN 168 HOURS`
- **Weekly**: `OPTIMIZE metadata.<env>_orchestration.watermarks`

Schedule these as separate Databricks SQL tasks or as a maintenance workflow
running off-peak. They do not block extraction jobs.

---

## Troubleshooting

| Error code | Cause | Resolution |
|-----------|-------|------------|
| LHP-CFG-031 | `::` in pipeline or flowgroup name | Rename to remove `::` — use `-` or `_` as separator instead. See [errors_reference.rst](../errors_reference.rst#lhp-cfg-031-separator-collision-in-for-each-pipeline-or-flowgroup-name) |
| LHP-CFG-032 | Two `for_each` flowgroups produce the same `<pipeline>::<flowgroup>` composite | Rename one flowgroup or pipeline so the composite is unique. See [errors_reference.rst](../errors_reference.rst#lhp-cfg-032-composite-load-group-not-unique-within-project) |
| LHP-CFG-033 | Action count, shared-key disagreement, concurrency out of bounds, or mixed execution_mode in same pipeline | See error message detail and [errors_reference.rst](../errors_reference.rst#lhp-cfg-033-for-each-post-expansion-structural-violation) for sub-check resolutions |
| LHP-CFG-034 | Orchestrator-time mixed-mode guard fired (validator missed the condition) | Move `for_each` flowgroups into a separate pipeline; file an issue if LHP-CFG-033 did not fire first. See [errors_reference.rst](../errors_reference.rst#lhp-cfg-034-mixed-execution-mode-flowgroups-in-same-pipeline-orchestrator) |
| LHP-CFG-035 | Non-strict watermark operator (`>=` or `<=`) on a `for_each` action | Set `watermark.operator: ">"`. See [errors_reference.rst](../errors_reference.rst#lhp-cfg-035-non-strict-watermark-operator-for-execution-mode-for-each) |
| LHP-CFG-036 | Pipeline contains two or more `for_each` flowgroups | Consolidate into one flowgroup or split each into its own pipeline. See [errors_reference.rst](../errors_reference.rst#lhp-cfg-036-pipeline-contains-multiple-for-each-flowgroups) |
| LHP-VAL-048 | `batch_id` taskValue absent in validate task | Confirm `prepare_manifest` succeeded and is a dependency of `validate`. See [errors_reference.rst](../errors_reference.rst#lhp-val-048-b2-validate-batch-id-taskvalue-absent) |
| LHP-VAL-049 | Parity check enabled but not yet implemented | Set `workflow.parity_check_enabled: false`. See [errors_reference.rst](../errors_reference.rst#lhp-val-049-parity-check-not-yet-implemented) |
| LHP-VAL-050 | Validate failed: `completed_n != expected` or `failed_n > 0` | Inspect `unfinished_actions` in validate summary JSON; check per-iteration logs in Databricks Jobs UI. See [errors_reference.rst](../errors_reference.rst#lhp-val-050-b2-validate-failed) |
| LHP-VAL-04B | Parity check failed: JDBC row count does not match landed parquet row count _(future — not yet implemented; superseded by LHP-VAL-049)_ | Inspect `parity_mismatches` in validate summary; check source for late-arriving writes or truncation during the batch window |
| LHP-MAN-001 | Manifest MERGE retry budget exhausted in `prepare_manifest` | Reduce `concurrency`; inspect Delta concurrent-commit metrics. See [errors_reference.rst](../errors_reference.rst#lhp-man-001-manifest-merge-retry-budget-exhausted) |
| LHP-MAN-002 | Manifest claim ownership conflict — competing worker owns the row | Usually transient; re-run failed iterations. If persistent, reset the manifest row. See [errors_reference.rst](../errors_reference.rst#lhp-man-002-manifest-claim-ownership-conflict) |
| LHP-MAN-003 | Manifest row missing for action after claim UPDATE | Re-run full workflow from beginning; check that `prepare_manifest` succeeded. See [errors_reference.rst](../errors_reference.rst#lhp-man-003-manifest-row-missing-for-action) |
| LHP-MAN-004 | Completion mirror MERGE retry exhausted; manifest row stuck in `running` | Reduce `concurrency`; manually correct manifest row if watermark shows completed. See [errors_reference.rst](../errors_reference.rst#lhp-man-004-completion-mirror-merge-retry-budget-exhausted) |
| LHP-MAN-005 | Projected or actual `iterations` taskValue payload exceeds DAB 48 KB ceiling | Reduce action count or shorten field identifiers. At runtime: DELETE orphaned manifest rows and redeploy. See [errors_reference.rst](../errors_reference.rst#lhp-man-005-manifest-taskvalue-payload-exceeds-dab-48-kb-ceiling) |
| LHP-WM-001 | `DuplicateRunError` raised by `wm.insert_new` — `run_id` already present in watermarks. Often a `__lhp_run_id_override` collision or a redeploy that resets DAB task counters. | Search worker task log for `duplicate_run_id_abort`. Clear the override widget or redeploy to refresh task identifiers. The manifest row stays in `running` and `validate` surfaces it as `final_status='running'` — manually reset via the LHP-MAN-002 procedure after fixing run_id provenance. See [errors_reference.rst](../errors_reference.rst#lhp-wm-001-duplicate-run-id-duplicaterunerror) |

---

### Anomaly B addendum (issue #18 — explicit DuplicateRunError handling)

The Anomaly B failure-mirror was added in PR #13 / PR #24 to make
`b2_manifests` an authoritative state log instead of a join-only surface
coalesced against watermarks. Issue #18 (PR for fix follows) tightened
the worker error-path in two places:

1. **Worker**: `wm.insert_new` is now wrapped in a dedicated
   `try / except DuplicateRunError` block in
   `src/lhp/templates/load/jdbc_watermark_job.py.j2`. The handler logs a
   structured `_log_phase("duplicate_run_id_abort", error_code="LHP-WM-001",
   batch_id=...)` breadcrumb and re-raises the original exception. It does
   **not** call `wm.mark_failed` (no row was created to transition) and does
   **not** mirror the manifest to `'failed'` (the issue is run-id
   provenance, not extraction failure). The manifest row stays in its
   claim-time `'running'` state.

2. **Validate**: `validate.py.j2`'s `final_status` projection switched from
   `coalesce(worker_status, manifest_status)` to a CASE that requires
   manifest-side `'completed'` AND (worker-side either `'completed'` or
   NULL) before treating an action as completed. A stale watermarks row
   matched on `worker_run_id` can no longer mask a fresh manifest
   `'running'` state — that produced a silent false pass under the
   pre-issue-#18 form.

**Operator runbook for `final_status='running'` after a `DuplicateRunError`:**

1. In the Databricks Jobs UI, locate the failed `for_each_ingest` iteration.
   The worker task log contains the `duplicate_run_id_abort` breadcrumb
   with `run_id`, `batch_id`, and `action_name`.
2. Investigate the run-id provenance:
   - Was `__lhp_run_id_override` set on the workflow or a parent context?
     Clear it.
   - Was the bundle redeployed in a way that reset DAB task counters? A
     fresh deploy with new task identifiers resolves the collision.
3. Reset the manifest row using the LHP-MAN-002 procedure (manual
   `UPDATE ... SET execution_status = 'failed'` then re-run the failed
   iteration from the Jobs UI).

- The iteration list for `for_each_ingest` is shown in the Databricks Jobs UI
  under the task. Click any iteration to drill into its logs and Spark metrics.
- Failed iterations are highlighted in red. Individual failed iterations can be
  re-run from the UI without re-running the entire batch (`Re-run failed
  iterations` button in DAB ≥ 0.295).
- The `inputs:` taskValue resolution for `for_each_ingest` is visible in the
  `prepare_manifest` task output. Look for the line:
  `manifest entries: N, taskvalue payload bytes: M`
- If `prepare_manifest` succeeds but `for_each_ingest` shows zero iterations,
  check that the `iterations` taskValue was emitted — search `prepare_manifest`
  stdout for `taskvalue` or `b2_manifests INSERT`.

---

## Cosmetic notes

- The `::` separator in the `load_group` composite key (`<pipeline>::<flowgroup>`)
  may appear in Databricks cluster log lines. Some log parsers treat `:` as a
  key-value separator, which can produce unexpected splits. When searching
  Databricks logs, use the full composite string (e.g.
  `crm_bronze::product_ingestion`) as the search term to avoid false partial
  matches.
- DAB task names are sanitised by replacing non-alphanumeric characters, so
  `::` does not propagate into DAB resource identifiers — only into log output
  and the `load_group` column in the watermarks and b2_manifests tables.

---

## E2E Smoke Validation (U12)

A read-write devtest smoke notebook validates the B2 codegen against a live
Databricks workspace before promoting to qa/prod. Notebook lives at
`scripts/validation/validate_b2_for_each_e2e.py`.

**Smoke fixture**: `Example_Projects/edp_lhp_starter/pipelines/02_bronze/b2_smoke.yaml`
loads three small AdventureWorks HumanResources tables (Department, Shift,
JobCandidate) under `execution_mode: for_each, concurrency: 3`.

**Procedure**:

1. `lhp generate -e devtest` from `Example_Projects/edp_lhp_starter/`
2. `databricks bundle deploy --target devtest --profile dbc-8e058692-373e`
3. `databricks bundle run edp_b2_smoke_jdbc_workflow --target devtest --profile dbc-8e058692-373e`
4. Wait for completion (typical: 3-5 minutes for 3-table smoke).
5. Open the smoke validation notebook in the workspace, set widgets, run all.
6. Read final JSON exit; expect `status: pass` with V1-V5 + R12 second-run check all green.
7. On success the notebook cleans up bronze tables + manifest + watermark rows
   keyed on the test source_system_id `pg_supabase_aw_b2`.

**Cleanup commands** (manual, on FAIL when notebook does not auto-clean):

```sql
DROP TABLE IF EXISTS devtest_edp_bronze.bronze.b2_smoke_department;
DROP TABLE IF EXISTS devtest_edp_bronze.bronze.b2_smoke_shift;
DROP TABLE IF EXISTS devtest_edp_bronze.bronze.b2_smoke_jobcandidate;
DELETE FROM metadata.devtest_orchestration.watermarks
  WHERE source_system_id = 'pg_supabase_aw_b2';
DELETE FROM metadata.devtest_orchestration.b2_manifests
  WHERE load_group = 'edp_b2_smoke_jdbc::b2_hr_smoke';
```

---

## Internals: iteration contract

Every per-action attribute that the B2 worker reads at iteration time must satisfy
two requirements simultaneously:

1. It must be a member of `B2_ITERATION_KEYS` — the canonical frozenset defined in
   `src/lhp/models/b2_iteration.py`.
2. It must be emitted by `prepare_manifest` into the iteration payload that flows
   to each worker via DAB `taskValues.set("iterations", ...)`.

The 10 keys split into two persistence categories — both flow through the
iteration payload, but only six are also persisted in `b2_manifests` rows:

| Key | In `b2_manifests` row? | In iteration payload? |
|---|---|---|
| `batch_id` | yes (PK) | yes |
| `action_name` | yes (PK) | yes |
| `source_system_id` | yes | yes |
| `schema_name` | yes | yes |
| `table_name` | yes | yes |
| `load_group` | yes | yes |
| `manifest_table` | no (FQN, not data) | yes |
| `jdbc_table` | **no** (issue #19) | yes |
| `watermark_column` | **no** (issue #19) | yes |
| `landing_path` | **no** (issue #19) | yes |

The three "no" rows are a deliberate design choice: `jdbc_table`,
`watermark_column`, and `landing_path` flow via `taskValues` only because the
`watermarks` table already records all three on every run and is joinable on
`worker_run_id`. Adding them to the manifest DDL would require an `ALTER TABLE`
migration on every existing devtest/qa/prod manifest table for an
observability gap already covered by the watermarks join.

### Audit join pattern (for the three taskValue-only keys)

To reconstruct full per-action source coordinates from a completed batch:

```sql
SELECT
    m.batch_id,
    m.action_name,
    m.execution_status                AS manifest_status,
    m.worker_run_id,
    w.source_system_id,
    w.schema_name,
    w.table_name,
    w.watermark_column_name,
    w.watermark_value,
    w.row_count,
    w.status                          AS worker_status,
    w.completed_at
FROM metadata.<env>_orchestration.b2_manifests m
LEFT JOIN metadata.<env>_orchestration.watermarks w
    ON w.run_id = m.worker_run_id
WHERE m.batch_id = '<batch_id>'
ORDER BY m.action_name;
```

`jdbc_table` and `landing_path` are not stored in either table; recover them
from the rendered pipeline YAML if needed (each LOAD action declares both as
inline literals, which is how `prepare_manifest` populates them at codegen).

**To extend the contract** (add a new per-action attribute):

1. Add the attribute name to `B2_ITERATION_KEYS` in `src/lhp/models/b2_iteration.py`.
2. Emit the attribute from the `prepare_manifest` template
   (`src/lhp/templates/b2/prepare_manifest.py.j2`) into the iteration payload.
3. Consume the attribute in the worker template
   (`src/lhp/templates/b2/worker/jdbc_watermark_job.py.j2`).
4. Re-run `tests/test_b2_iteration_contract.py` — it asserts that every key in
   `B2_ITERATION_KEYS` is present in both the iteration payload and the worker
   read path.

---

## Operational caveats

**`b2_manifests` TBLPROPERTIES are set once at CREATE TABLE time.** The auto-DDL
uses `CREATE TABLE IF NOT EXISTS` with the following properties:
`delta.enableChangeDataFeed`, `delta.autoOptimize.optimizeWrite`,
`delta.autoOptimize.autoCompact`, and `delta.enableRowTracking`.

Re-running the auto-DDL (e.g., after a `lhp generate` / deploy cycle) does **not**
update an existing table's properties — `IF NOT EXISTS` skips the CREATE when the
table already exists.

To change TBLPROPERTIES on an existing table, run an explicit ALTER:

```sql
ALTER TABLE metadata.<env>_orchestration.b2_manifests
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
  -- add or modify properties as needed
);
```

---

## Cross-references

| Document | Role |
|----------|------|
| [`docs/planning/b2-watermark-scale-out-design.md`](../planning/b2-watermark-scale-out-design.md) | Origin design + binding requirements |
| [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md) | Hard prerequisite — Tier 2 must be deployed first |
| [`docs/runbooks/devtest-validation-adr-003-phase-a-c.md`](./devtest-validation-adr-003-phase-a-c.md) | Tier 2 V1-V5 devtest sign-off (completed 2026-04-25) |
| [`docs/runbooks/lhp-runtime-deploy.md`](./lhp-runtime-deploy.md) | Full DAB deploy sequence + Terraform GPG workaround |
| [`docs/runbooks/tier-2-load-group-rollout.md`](./tier-2-load-group-rollout.md) | Tier 2 operator rollout runbook |
| [`docs/adr/ADR-002-lhp-runtime-availability.md`](../adr/ADR-002-lhp-runtime-availability.md) | Runtime vendoring decision |
| [`docs/adr/ADR-003-landing-zone-shape.md`](../adr/ADR-003-landing-zone-shape.md) | Landing zone and volume shape |
| [`docs/adr/ADR-004-watermark-registry-placement.md`](../adr/ADR-004-watermark-registry-placement.md) | Watermark registry placement |
| [`docs/errors_reference.rst`](../errors_reference.rst) | LHP-CFG-031, LHP-CFG-032, LHP-CFG-033, LHP-CFG-034 |
