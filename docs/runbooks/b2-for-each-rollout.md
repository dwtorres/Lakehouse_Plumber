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
| LHP-CFG-031 | `::` in pipeline or flowgroup name | Rename to remove `::` — use `-` or `_` as separator instead |
| LHP-CFG-032 | Two `for_each` flowgroups produce the same `<pipeline>::<flowgroup>` composite | Rename one flowgroup or pipeline so the composite is unique across the project |
| LHP-CFG-033 | Action count, shared-key disagreement, concurrency out of bounds, or mixed execution_mode in same pipeline | See error message detail; common fixes: split flowgroup if >300 actions, set `concurrency` in 1-100, or move non-`for_each` flowgroups to a separate pipeline |
| LHP-CFG-034 | Orchestrator-time mixed-mode guard fired (validator missed the condition) | Move `for_each` flowgroups into a separate pipeline; also file an issue if LHP-CFG-033 did not fire first |
| LHP-VAL-04A | Validate failed: `completed_n != expected` or `failed_n > 0` | Inspect `failed_actions` array in validate summary JSON; check per-iteration logs in Databricks Jobs UI for the failing iteration |
| LHP-VAL-04B | Parity check failed: JDBC row count does not match landed parquet row count | Inspect `parity_mismatches` in validate summary; check source for late-arriving writes or truncation during the batch window |
| LHP-MAN-001 | Manifest MERGE retry budget exhausted in `prepare_manifest` | Reduce `concurrency` (lower concurrent Delta commits); inspect Delta concurrent-commit metrics in the cluster Spark UI |
| LHP-MAN-002 | Manifest UPDATE matches zero rows (race condition) | Check DAB retry behavior; this is usually a transient race — the job typically self-resolves on next attempt; if persistent, check `b2_manifests` for orphaned rows with mismatched `batch_id` |

---

## Common DAB UI patterns for `for_each_task`

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
