# Runbook: Tier 2 `load_group` Rollout (dev → qa → prod)

**Status**: Active (Tier 2 implementation Plan: [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md)).
**Owner**: Platform engineering.
**Scope**: Promote the Tier 2 `load_group` axis through `dev` → `qa` → `prod` for the shared registry `metadata.<env>_orchestration.watermarks`. Captures the pre-flight `DESCRIBE DETAIL` baseline, runs the Step 3 backfill + Step 3a `OPTIMIZE`, executes the V1-V4 deep-clone validation, and walks per-flowgroup Step 4a seeding via `lhp seed-load-group`.
**Profile**: `dbc-8e058692-373e` — every Databricks CLI invocation in this runbook uses `--profile dbc-8e058692-373e` per project memory. Substitute your own profile if running against a different workspace.
**Reference plan**: [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md)
**Origin doc**: [`docs/planning/tier-2-hwm-load-group-fix.md`](../planning/tier-2-hwm-load-group-fix.md)
**Related**: [ADR-004](../adr/ADR-004-watermark-registry-placement.md) (registry placement; Tier 2 is the destructive-schema-change migration evidence ADR-004 §"Open questions deferred" anticipates), [`docs/runbooks/devtest-validation-adr-003-phase-a-c.md`](./devtest-validation-adr-003-phase-a-c.md) (provisioning matrix + deploy shape), [`docs/runbooks/lhp-runtime-deploy.md`](./lhp-runtime-deploy.md) (DAB sync mechanics).

---

## Purpose

Tier 2 adds a `load_group STRING` axis to `metadata.<env>_orchestration.watermarks` and switches Delta liquid clustering to `(source_system_id, load_group, schema_name, table_name)`. The auto-DDL fires on first `WatermarkManager.__init__` after the Tier 2 code deploys; the data-side migration is then operator-driven SQL (Step 3 backfill + Step 3a `OPTIMIZE`), V1-V4 validation against a deep-clone, and per-flowgroup `lhp seed-load-group` runs (Step 4a) over time.

This runbook is the single source of truth for the operator. Each environment's first B2 run depends on all five gates clearing for that env — pre-flight, Step 1 deploy verification, Step 3 backfill, Step 3a OPTIMIZE, V1-V4 deep-clone validation. Per-flowgroup Step 4a is per-flowgroup, not per-env, and ships incrementally as flowgroups onboard to B2.

---

## Migration ordering across envs

```
dev  → Step 1 deploy (auto-DDL fires) → Step 3 backfill → Step 3a OPTIMIZE → V1-V4 deep-clone → per-flowgroup Step 4a (helper) over time
qa   → Step 1 deploy → Step 3 → Step 3a → V1-V4 deep-clone → per-flowgroup Step 4a
prod → Step 1 deploy → Step 3 → Step 3a → V1-V4 deep-clone → per-flowgroup Step 4a
```

Promote `dev` end-to-end before starting `qa`. Promote `qa` end-to-end before starting `prod`. Per-flowgroup Step 4a runs continuously per env as flowgroups onboard to B2.

---

## Pre-flight checks (run once per env before merging Step 1)

### A. Capture `DESCRIBE DETAIL` baseline

For each target env (`dev`, `qa`, `prod`), capture the registry table's current physical shape so you can detect drift after the auto-DDL ALTER fires. Run once per env, before deploying Tier 2 code.

```bash
# Replace <env> with dev, qa, or prod for each pass.
# Replace <warehouse-id> with any running SQL warehouse id; list with:
#   databricks warehouses list --profile dbc-8e058692-373e
databricks api post /api/2.0/sql/statements --profile dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "DESCRIBE DETAIL metadata.<env>_orchestration.watermarks",
  "wait_timeout": "30s"
}'
```

Capture and save to your runbook log:

| Field | Expected before Tier 2 (per ADR-004 §Implementation Status) | Expected after Tier 2 (Step 1 + Step 3a) |
|---|---|---|
| `partitionColumns` | `[]` (liquid-clustered, not partitioned) | `[]` |
| `clusteringColumns` | `[source_system_id, schema_name, table_name]` | `[source_system_id, load_group, schema_name, table_name]` |
| `numFiles` | env-dependent baseline | reduced after Step 3a `OPTIMIZE` |
| `minReaderVersion` | unchanged | unchanged |
| `minWriterVersion` | unchanged | unchanged |

> **WARNING:** If `DESCRIBE DETAIL` shows the table is **partitioned** (non-empty `partitionColumns`), liquid clustering is not in effect and a Step 3a `OPTIMIZE` will rewrite every file in the table. Plan a maintenance window before merging Step 1 in that env. `dev` is known liquid-clustered (per ADR-004 §Implementation Status); `qa` and `prod` baselines must be confirmed at deploy time.

### B. `extraction_type` consumer audit

Tier 2 introduces a new `extraction_type` value `'seeded'` (used by Step 4a inserts). The column is `STRING NOT NULL` with no enum constraint at the DDL level. Confirm no Python consumer enumerates the legal values:

```bash
cd /Users/dwtorres/src/Lakehouse_Plumber
rg "extraction_type" --type py
```

Expected: zero hits where consumers compare `extraction_type` against an explicit allowlist (e.g. `if x in ('full', 'incremental'): ...`). If a hit appears, extend that consumer to accept `'seeded'` **before** merging Tier 2 code.

### C. Inclusive-naming separator pre-flight (cosmetic)

The Tier 2 composite uses `::` as the `pipeline::flowgroup` separator. The fatal `LHP-CFG-019` separator-collision validator ships in B2 (when `execution_mode: for_each` is set), so Tier 2 emits a **warning, not an error** when `::` appears literally inside a flowgroup or pipeline name. Operators should still pre-scan their YAML to avoid log-parser noise:

```bash
cd /Users/dwtorres/src/Lakehouse_Plumber
rg -l '::' Example_Projects/*/pipelines/**/*.yaml || true
# Repeat for any external user-bundle repos where Tier 2 is being adopted.
```

> **Note:** A non-empty hit list is **not a Tier 2 blocker**. Cosmetic only — the registry still writes the literal composite, and Tier 2 reads honour it. The hard guard fires in B2 when those flowgroups declare `execution_mode: for_each`.

### D. Deploy principal grants

Confirm the deploy principal (or interactive user, in dev mode) has `MODIFY` on `metadata.<env>_orchestration` so the auto-DDL `ALTER TABLE` can run:

```sql
SHOW GRANTS ON SCHEMA metadata.<env>_orchestration;
-- Required for the deploy principal: USE SCHEMA, MODIFY, SELECT
-- If MODIFY is absent:
--   GRANT MODIFY ON SCHEMA metadata.<env>_orchestration TO `<deploy-principal>`;
```

If `MODIFY` is missing, the first Step 1 init fails with `Permission denied: ALTER TABLE`. Fix grants before deploying.

---

## Per-env Step 1 — Deploy Tier 2 code (auto-DDL fires)

Tier 2 ships extension to `WatermarkManager._ensure_table_exists` that adds the `load_group STRING` column and switches clustering — both gated by `DESCRIBE TABLE` / `DESCRIBE DETAIL` probes, both idempotent on every subsequent init.

### 1.1 — Deploy

Follow your normal deploy shape. For an LHP-managed bundle this is the standard sequence:

```bash
cd <user-bundle-repo>

# Refresh vendored runtime (per ADR-002 amendment / lhp-runtime-deploy.md).
lhp sync-runtime

# Regenerate notebooks + resource files for the target env.
lhp generate -e <env> --pipeline-config config/pipeline_config.yaml --force

databricks bundle validate -t <env> --profile dbc-8e058692-373e
databricks bundle deploy -t <env> --profile dbc-8e058692-373e
```

> **WARNING — operator-ordering footgun:** Step 1's auto-DDL fires from `WatermarkManager.__init__` (`src/lhp_watermark/watermark_manager.py:121` → `_ensure_table_exists`). A deploy that uploads bundle files but never executes a task that constructs `WatermarkManager` will **not** trigger the ALTER. To exercise it, you must run a workflow whose extraction notebook loads `lhp_watermark` and instantiates `WatermarkManager` (i.e. any `jdbc_watermark_v2` extract task). `lhp generate` alone does **not** fire the auto-DDL.

### 1.2 — Trigger the auto-DDL

Run any task that loads `lhp_watermark` and constructs the manager. The recommended path is the same JDBC extract task you run for ADR-003 §C2 validation:

```bash
databricks bundle run \
  -t <env> --profile dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_workflow
```

The extract task may succeed or fail at the JDBC read — either is fine for Step 1. The auto-DDL runs in `WatermarkManager.__init__` *before* the JDBC read, so even a downstream secret-resolution failure (`Secret does not exist with scope: …`) leaves the registry shape correctly migrated. See `docs/runbooks/devtest-validation-adr-003-phase-a-c.md` §2.3 outcome matrix for analogous bootstrap semantics.

### 1.3 — Verify Step 1 fired

Re-capture `DESCRIBE DETAIL` and compare to your pre-flight baseline:

```bash
databricks api post /api/2.0/sql/statements --profile dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "DESCRIBE DETAIL metadata.<env>_orchestration.watermarks",
  "wait_timeout": "30s"
}'
```

**Pass criteria** (all must hold):

- `clusteringColumns` is now `[source_system_id, load_group, schema_name, table_name]`.
- The `load_group` column is present in `DESCRIBE TABLE`:

```sql
DESCRIBE TABLE metadata.<env>_orchestration.watermarks;
-- Expect: load_group STRING column appears in the column list
-- (nullable; existing rows have load_group IS NULL pending Step 3 backfill).
```

If either probe shows the pre-Tier-2 shape, the auto-DDL did not fire. Re-check §1.2 — confirm a task that constructs `WatermarkManager` actually ran. Workflow event logs (`databricks bundle run --no-wait` then `databricks jobs get-run` against the run id) show whether the extraction notebook reached `_ensure_table_exists`.

### 1.4 — Outcome

- **PASS** → Step 1 verified for this env. Proceed to Step 3.
- **FAIL** (`Permission denied: ALTER TABLE`) → deploy principal lacks `MODIFY` on `metadata.<env>_orchestration`. Re-run pre-flight §D and re-grant.
- **FAIL** (no shape change) → no task constructed `WatermarkManager`. Re-run §1.2; check workflow logs.

---

## Per-env Step 3 — Backfill `'legacy'`

Run once per env after Step 1 verifies. Idempotent — re-running is a no-op once all rows are populated.

```sql
UPDATE metadata.<env>_orchestration.watermarks
SET load_group = 'legacy'
WHERE load_group IS NULL;
```

Execute via Databricks SQL editor against any running warehouse, or via the API:

```bash
databricks api post /api/2.0/sql/statements --profile dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "UPDATE metadata.<env>_orchestration.watermarks SET load_group = '\''legacy'\'' WHERE load_group IS NULL",
  "wait_timeout": "30s"
}'
```

**Pass criterion**: post-update count of `load_group IS NULL` rows is zero:

```sql
SELECT
  COUNT(*) AS null_remaining,
  COUNT_IF(load_group = 'legacy') AS legacy_count,
  COUNT(*) AS total_rows
FROM metadata.<env>_orchestration.watermarks;
-- Expect: null_remaining = 0; legacy_count + non-legacy_count = total_rows
```

Pre-Tier-2 every row has `load_group IS NULL`, so post-Step-3 every row should have `load_group = 'legacy'`. Total row count is unchanged (UPDATE, not INSERT or DELETE).

---

## Per-env Step 3a — `OPTIMIZE` after backfill

Liquid clustering attached at column-add does not reshuffle existing files until `OPTIMIZE` runs. Run once per env, after Step 3, before per-flowgroup Step 4a begins.

```sql
OPTIMIZE metadata.<env>_orchestration.watermarks;
```

Or via API:

```bash
databricks api post /api/2.0/sql/statements --profile dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "OPTIMIZE metadata.<env>_orchestration.watermarks",
  "wait_timeout": "120s"
}'
```

**Pass criterion**: `numFiles` from `DESCRIBE DETAIL` decreases (typical) or stays the same (small tables already well-packed). `OPTIMIZE` rewrites under the new clustering key; subsequent HWM lookups under per-pipeline batching read clustered storage.

```bash
databricks api post /api/2.0/sql/statements --profile dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "DESCRIBE DETAIL metadata.<env>_orchestration.watermarks",
  "wait_timeout": "30s"
}'
# Compare numFiles to pre-Step-3a value; expect stable or reduced.
```

A second `OPTIMIZE` after fleet-wide seeding (or as part of the recurring weekly OPTIMIZE policy from B2 OPS unit) bounds long-term file count. Recurring policy is **not** in scope for this runbook.

---

## Per-env V1-V4 deep-clone validation

Before promoting Tier 2 to the next env (or starting per-flowgroup Step 4a in this env), run V1-V4 against a deep-clone of the live `watermarks` table. The validation notebook lives at `scripts/validation/validate_tier2_load_group.py` (Tier 2 plan U4 deliverable).

### V1-V4 checks (summary)

| Check | What it asserts |
|---|---|
| **V1 — Isolation smoke** | Two probe rows with same `(source_system_id, schema, table)` but different `load_group` values (`'lg_a'`, `'lg_b'`). `get_latest_watermark(load_group='lg_a')` returns `lg_a`'s row; `load_group='lg_b'` returns `lg_b`'s row. Neither leaks. |
| **V2 — Tier-1 cross-source regression** | Insert a synthetic `'fake_federation'`-source row at `2099-01-01`. `get_latest_watermark(source_system_id='real_source', ...)` does not pick it up. Tier 1 cross-source isolation preserved. |
| **V3 — Legacy fallback** | `get_latest_watermark(load_group=None)` returns the `'legacy'`-backfilled row. Migration safety valve for un-migrated callers. |
| **V4 — Legacy-seed-then-incremental smoke** | Apply Step 4a INSERT against the probe table for a synthetic `(source, schema, table, target_load_group)`. Run a worker iteration in dry-run mode that calls `get_latest_watermark(..., load_group=target_load_group)` and assert (a) returned HWM equals the seeded `watermark_value`, (b) extraction_type would be `'incremental'`, (c) JDBC WHERE clause includes the seeded HWM. |

### Procedure

1. **Deep-clone the live registry to a probe table.** The notebook does this in its first cell, but operators must confirm the probe table does not already exist (the cell aborts to avoid silently overwriting):

   ```sql
   -- One-off cleanup if a stale probe table is present:
   DROP TABLE IF EXISTS metadata.<env>_orchestration.watermarks_v1_probe;
   ```

2. **Upload + run the validation notebook.** The notebook is a Databricks-format script (`# Databricks notebook source` header, `# COMMAND ----------` cell separators, widgets for `env` and `probe_table_name`). Run it via the workspace UI, or as a one-off job:

   ```bash
   # Expected invocation shape (the helper lives in the LHP repo, not the user bundle).
   # Copy scripts/validation/validate_tier2_load_group.py to a workspace path,
   # then run it via the Databricks Jobs API or interactively in the workspace.
   databricks workspace import \
     --source-path scripts/validation/validate_tier2_load_group.py \
     --target-path /Users/<you>/lhp_validation/validate_tier2_load_group \
     --language PYTHON --format SOURCE \
     --profile dbc-8e058692-373e
   ```

3. **Confirm exit JSON.** The notebook's final cell calls `dbutils.notebook.exit(json.dumps({'V1': ..., 'V2': ..., 'V3': ..., 'V4': ...}))`. All four entries must be `'PASS'` (or equivalent truthy success marker per the U4 implementation).

4. **Cleanup.** The notebook removes its probe rows by isolated marker `run_id`s (not table-scoped DELETE). The probe table itself is left in place for operator inspection — drop it manually when done:

   ```sql
   DROP TABLE metadata.<env>_orchestration.watermarks_v1_probe;
   ```

### Outcome

- **PASS** (V1, V2, V3, V4 all green) → env cleared for per-flowgroup Step 4a, and cleared for B2 first-run.
- **FAIL on any check** → do **not** proceed to per-flowgroup Step 4a in this env. Capture the notebook output, file an issue against the Tier 2 plan, fix root cause, re-run V1-V4.

---

## Per-flowgroup Step 4a — `lhp seed-load-group` helper

Per-flowgroup Step 4a copies the legacy HWM ceiling into a flowgroup's new `load_group` namespace **before** that flowgroup's first B2 run. Without this seed, the flowgroup's first B2 run cold-starts with a full source scan — material against PgBouncer-pooled or rate-limited sources.

> **WARNING — Step 3-before-Step-4a precondition:** Step 3 (`'legacy'` backfill) must complete in this env before Step 4a runs for **any** flowgroup. Running Step 4a first silently inserts zero rows because the source `WHERE load_group = 'legacy'` matches nothing. The CLI helper warns when it detects no `'legacy'` rows for the target `(source, schema, table)`, but the operator runbook is the authoritative gate.

### Helper invocation

`lhp seed-load-group` (Tier 2 plan U5 deliverable) consumes a flowgroup YAML + env substitutions and emits per-table SELECT-preview + INSERT-seed SQL. Expected shape:

```bash
# Dry-run (default) — emits SELECT preview + INSERT SQL to stdout, executes nothing.
lhp seed-load-group \
  --env <env> \
  --flowgroup pipelines/<pipeline-dir>/<flowgroup>.yaml \
  --dry-run

# Apply — executes the INSERT against the live registry via Databricks SQL.
lhp seed-load-group \
  --env <env> \
  --flowgroup pipelines/<pipeline-dir>/<flowgroup>.yaml \
  --apply
```

`--dry-run` is the **default**. Explicit `--apply` is required to mutate. Always run `--dry-run` first, copy the SELECT preview into a Databricks SQL session, confirm row counts and `max(watermark_value)` look sensible, then re-run with `--apply`.

### Per-table SELECT preview

For each `(source_system_id, schema, table)` the flowgroup will load, the helper emits a preview block. Confirm before applying:

```sql
-- Preview block emitted by `lhp seed-load-group --dry-run`. Confirms what the seed
-- INSERT would copy. Mandatory dry-run gate before fleet-scale seeding.
SELECT count(*), max(watermark_time), max(watermark_value)
FROM   metadata.<env>_orchestration.watermarks
WHERE  load_group       = 'legacy'
  AND  source_system_id = :src
  AND  schema_name      = :schema
  AND  table_name       = :table
  AND  status           = 'completed';
```

If `count(*)` is 0, the legacy backfill (Step 3) hit no rows for that `(source, schema, table)` — the table was either never loaded under Tier 1, or its prior runs all failed. The helper still emits the INSERT, but it will insert zero rows on apply. The flowgroup's first B2 run will cold-start with a full scan — correct behaviour for a never-loaded source. Operators should not interpret a zero-insert as a seed failure.

### Per-table INSERT seed

Pinned to the verified DDL in `src/lhp_watermark/watermark_manager.py::_ensure_table_exists` (17 base columns + `load_group` from Tier 2; `error_class` / `error_message` are nullable and default NULL):

```sql
INSERT INTO metadata.<env>_orchestration.watermarks
    (run_id, watermark_time, source_system_id, schema_name, table_name,
     watermark_column_name, watermark_value, previous_watermark_value,
     row_count, extraction_type,
     bronze_stage_complete, silver_stage_complete, status,
     created_at, completed_at, load_group)
SELECT
    concat('seed-', uuid()),                -- UUID-based; no '::' inside run_id
    watermark_time, source_system_id, schema_name, table_name,
    watermark_column_name, watermark_value, previous_watermark_value,
    row_count,
    'seeded',                               -- new extraction_type value (additive only)
    bronze_stage_complete, silver_stage_complete, status,
    current_timestamp(), completed_at,
    :load_group_target                      -- f"{flowgroup.pipeline}::{flowgroup.flowgroup}"
FROM metadata.<env>_orchestration.watermarks
WHERE load_group       = 'legacy'
  AND source_system_id = :src
  AND schema_name      = :schema
  AND table_name       = :table
  AND status           = 'completed'
ORDER BY watermark_time DESC, completed_at DESC, run_id DESC  -- timestamp tiebreakers; lex run_id last
LIMIT 1;
```

### Idempotency

Re-running the seed inserts an additional `seed-<uuid>` row with the same `watermark_value`. `get_latest_watermark` still returns the highest-watermark row, so observable behaviour is unchanged. Operators may dedupe by `(load_group, source_system_id, schema, table, watermark_value)` if registry hygiene matters.

### Cosmetic note: `::` separator collision

The `LHP-CFG-019` separator-collision validator and the `LHP-CFG-026` composite-uniqueness validator are **codegen-time validators that fire only when `execution_mode: for_each` is set**. `execution_mode` ships in the B2 plan, not Tier 2. Tier 2 ships zero new error codes.

For Tier 2, a `lhp seed-load-group` run against a flowgroup whose `pipeline` or `flowgroup` field happens to contain `::` produces a **warning, not a fatal abort**. The composite still serialises cleanly; the registry still writes the literal value. Operators should still avoid `::` in pipeline/flowgroup names because B2 will reject them.

---

## Post-flight verification

After Step 1 + Step 3 + Step 3a + V1-V4 all PASS for an env, capture verification evidence:

### A. Confirm clustering switched

```sql
DESCRIBE DETAIL metadata.<env>_orchestration.watermarks;
-- clusteringColumns = [source_system_id, load_group, schema_name, table_name]
```

### B. Confirm row counts unchanged from Step 3

```sql
SELECT COUNT(*) AS total_rows,
       COUNT_IF(load_group = 'legacy') AS legacy_rows,
       COUNT_IF(load_group IS NULL) AS null_rows
FROM metadata.<env>_orchestration.watermarks;
-- total_rows: equal to pre-Step-3 baseline (UPDATE is not DELETE)
-- legacy_rows: equal to pre-Step-3 NULL count
-- null_rows: 0
```

### C. Confirm a seeded HWM is readable

For any flowgroup that has run Step 4a, confirm `get_latest_watermark(..., load_group='<composite>')` returns the seeded row. Smoke-check via SQL (the live runtime path goes through `WatermarkManager`, which composes the same filter):

```sql
SELECT load_group, watermark_value, extraction_type, run_id
FROM   metadata.<env>_orchestration.watermarks
WHERE  source_system_id = :src
  AND  schema_name      = :schema
  AND  table_name       = :table
  AND  load_group       = :load_group_target  -- e.g. 'bronze::customers_daily'
  AND  status           = 'completed'
ORDER BY watermark_time DESC, completed_at DESC, run_id DESC
LIMIT 1;
-- Expect one row with extraction_type = 'seeded' (or 'incremental' / 'full' if
-- the flowgroup has since run a real B2 extract).
```

### D. Capture validation log

For each env, append a row to your runbook log:

| Env | Step 1 verified | Step 3 backfilled | Step 3a OPTIMIZED | V1-V4 PASS | Date | Run ids | Notes |
|---|---|---|---|---|---|---|---|
| devtest | yes | yes (no-op; empty registry) | yes (no-op; numFiles 0→0) | deferred | 2026-04-25 | preflight stmt 01f1410e-a704; init via `lhp init-registry`; backfill+optimize via `lhp tier2-rollout`. Principal: `verbena1@gmail.com`. Warehouse: `9f30611e0b932a47`. | Empty registry (numFiles=0, sizeInBytes=0). M14 idempotency confirmed (re-run init issued zero ALTERs). V1-V5 (Phase 5) deferred — no interactive cluster + lhp_watermark wheel in workspace. Phase 6 dry-run validated for `customer_bronze` (zero-row preview = correct cold-start). Bug fixed mid-flight: SDK 0.105 needs enum `ExecuteStatementRequestOnWaitTimeout.CONTINUE` not string. |
| qa  | … | … | … | … | … | … | … |
| prod | … | … | … | … | … | … | … |

---

## Rollback

Tier 2's auto-DDL is **forward-compatible by design** — adding a nullable column and switching clustering does not break legacy callers passing `load_group=None` (the three-way SQL filter handles them). Rollback paths:

- **Step 1 rollback (auto-DDL ALTER undesired)**: there is no clean SQL-level "undo" for `ALTER TABLE … ADD COLUMNS` and `ALTER TABLE … CLUSTER BY`. The column can be dropped only by re-creating the table (destructive). In practice: revert the Tier 2 code commit, redeploy, and the manager continues to write `load_group=NULL` against the (now-extended) registry. The added column is benign with `NULL` values.
- **Step 3 rollback**: trivial — `UPDATE metadata.<env>_orchestration.watermarks SET load_group = NULL WHERE load_group = 'legacy';`. No data loss.
- **Step 3a rollback**: not applicable — `OPTIMIZE` is a physical-storage operation; there's nothing to roll back. File count may be different; read semantics are unchanged.
- **Step 4a rollback (per-flowgroup seed)**: delete by `run_id LIKE 'seed-%'`:
  ```sql
  DELETE FROM metadata.<env>_orchestration.watermarks
  WHERE load_group       = :load_group_target
    AND source_system_id = :src
    AND schema_name      = :schema
    AND table_name       = :table
    AND extraction_type  = 'seeded'
    AND run_id LIKE 'seed-%';
  ```

A full env-level rollback to pre-Tier-2 state requires a Delta time-travel restore (`RESTORE TABLE … TO VERSION AS OF …`) — coordinate with the Delta protocol owner before attempting.

---

## Follow-ups after Tier 2 ships in qa

Per [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md) §"Documentation / Operational Notes":

- Capture migration evidence in a follow-up ADR (candidate ADR-005) per [ADR-004](../adr/ADR-004-watermark-registry-placement.md) §"Open questions deferred". Defer until Tier 2 ships in qa.
- Recurring `OPTIMIZE` (weekly) + `VACUUM RETAIN 168 HOURS` (daily) policy on the watermarks registry is documented in the B2 OPS unit, not here.
- The B2 plan ships the `LHP-CFG-019` separator-collision and `LHP-CFG-026` composite-uniqueness fatal validators; until then, operators rely on the Tier 2 warning + this runbook's pre-flight scan (§Pre-flight check C).

---

## References

- [`docs/plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md`](../plans/2026-04-25-001-feat-tier-2-load-group-registry-axis-plan.md) — Tier 2 implementation plan; this runbook is U6.
- [`docs/planning/tier-2-hwm-load-group-fix.md`](../planning/tier-2-hwm-load-group-fix.md) — origin doc; Step 3 backfill SQL, Step 3a OPTIMIZE SQL, Step 4a per-flowgroup seed.
- [ADR-004](../adr/ADR-004-watermark-registry-placement.md) — registry placement (`metadata.<env>_orchestration.watermarks`); §"Open questions deferred" frames Tier 2 as the migration-evidence ADR-005 candidate.
- [`docs/runbooks/devtest-validation-adr-003-phase-a-c.md`](./devtest-validation-adr-003-phase-a-c.md) — provisioning matrix, deploy shape, watermark-table bootstrap semantics.
- [`docs/runbooks/lhp-runtime-deploy.md`](./lhp-runtime-deploy.md) — DAB sync mechanics; vendoring `lhp_watermark/`.
- [`docs/planning/tier-1-hwm-fix.md`](../planning/tier-1-hwm-fix.md) — Tier 1 (predecessor; V1 probe shape mirrored in V2).
- [`docs/planning/b2-watermark-scale-out-design.md`](../planning/b2-watermark-scale-out-design.md) — B2 design baseline; downstream consumer of Tier 2.
