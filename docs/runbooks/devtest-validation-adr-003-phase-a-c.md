# Runbook: Devtest Validation — ADR-003 Phase A/C

**Status**: Active (post-ADR-004 close, 2026-04-19).
**Owner**: dwtorres@gmail.com.
**Scope**: Validate the four code/doc artefacts that closed (or partially closed) ADR-003 §Q3 / §Q5 against the live `devtest` Databricks workspace + `devtest_edp_*` catalogs.
**Profile**: `dbc-8e058692-373e` (per project memory).
**Reference project**: [`Example_Projects/edp_lhp_starter/`](../../Example_Projects/edp_lhp_starter/) — canonical layout used by every step below.
**Related**: [ADR-003](../adr/ADR-003-landing-zone-shape.md), [ADR-004](../adr/ADR-004-watermark-registry-placement.md), [`docs/planning/adr-003-followups.md`](../planning/adr-003-followups.md), [`docs/runbooks/lhp-runtime-deploy.md`](./lhp-runtime-deploy.md).

---

## Purpose

Discharge the four outstanding manual validation items from PRs #6 / #7 / #8 / #9. Each step takes a code-shipped change end-to-end through `lhp generate` → `databricks bundle deploy -t devtest` → `databricks bundle run -t devtest` → SQL verification, and either flips an ADR-003 success criterion to `[x]` or leaves it as a documented failure.

| # | Validates | PR | ADR success criterion impacted |
|---|-----------|----|-------------------------------|
| 1 | A2 — empty-batch schema-bearing fallback | [#6](https://github.com/dwtorres/Lakehouse_Plumber/pull/6) | ADR-003 §Q3 `[~]` → `[x]` |
| 2 | A3 — generator-side landing-schema overlap guard | [#7](https://github.com/dwtorres/Lakehouse_Plumber/pull/7) | ADR-003 §Q5 (already closed; this is a regression-guard smoke) |
| 3 | C2 — per-env catalog convention end-to-end | [#8](https://github.com/dwtorres/Lakehouse_Plumber/pull/8) | ADR-003 §Q5 (already closed; this is the production-shape evidence) |
| 4 | ADR-004 — per-env watermark registry write | [#9](https://github.com/dwtorres/Lakehouse_Plumber/pull/9) | confirms ADR-004 §Implementation Status |

Run them in numerical order. Steps 3 + 4 share the same deploy and can be observed in one bundle run.

---

## Prerequisites

Before any step:

1. **Repo state**: branch `watermark` checked out, up to date with `origin/watermark` (post #6/#7/#8/#9 merges).
   ```bash
   cd /Users/dwtorres/src/Lakehouse_Plumber
   git checkout watermark
   git pull --ff-only origin watermark
   ```

2. **LHP CLI installed from source** (V0.8.2 + watermark plugin):
   ```bash
   pip install -e . --quiet
   lhp --version   # expect: lhp, version 0.8.2
   ```

3. **Databricks CLI authenticated** as profile `dbc-8e058692-373e`:
   ```bash
   databricks auth profiles | grep dbc-8e058692-373e
   databricks current-user me -p dbc-8e058692-373e   # expect: 200 OK + user JSON
   ```

4. **Devtest workspace + catalogs provisioned** (already done by platform team):
   ```sql
   -- Run in devtest workspace SQL editor:
   SHOW CATALOGS LIKE 'devtest_edp_*';
   -- expect: devtest_edp_bronze, devtest_edp_silver, devtest_edp_gold,
   --         devtest_edp_landing, devtest_edp_orchestration
   ```

5. **External landing volume available**:
   ```sql
   SHOW VOLUMES IN devtest_edp_landing.landing;
   -- expect at least: landing  (EXTERNAL, with abfss:// location)
   ```

6. **Edit `databricks.yml` placeholders** in the EDP starter to your real workspace + service principal (do this once; commit to a private branch if your env values are sensitive):
   ```bash
   cd Example_Projects/edp_lhp_starter
   # Replace <your-devtest-workspace> with actual hostname,
   # <qa-deploy-sp-uuid> / <prod-deploy-sp-uuid> with real SP UUIDs.
   $EDITOR databricks.yml
   ```

7. **Source secrets present** in `devtest_secrets` Databricks scope (per `substitutions/devtest.yaml`):
   ```bash
   databricks secrets list-scopes -p dbc-8e058692-373e | grep devtest
   databricks secrets list-secrets devtest_db_secrets -p dbc-8e058692-373e
   # expect at least: pg_host, pg_user, pg_password
   ```

   If the JDBC source you want to test against is not Postgres, edit
   `Example_Projects/edp_lhp_starter/pipelines/02_bronze/customer_bronze.yaml` to
   match your source's driver + URL + table identifier and re-run `lhp generate`.

---

## Step 1 — A2: empty-batch schema-bearing fallback

**Goal**: prove that an incremental JDBC extract that returns zero rows still leaves a schema-bearing parquet at `{landing_volume_root}/customer/_lhp_runs/<uuid>/`, so the bronze AutoLoader does not fail with `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` on the next run.

### 1.1 — Force the customer source to return zero rows

Two options:

**Option a (recommended)** — temporarily override `WHERE` clause via watermark setup:

```sql
-- Run in devtest_edp_orchestration.watermarks SQL editor:
-- Pre-seed the watermark to a future timestamp so the extractor reads zero rows.
INSERT INTO devtest_edp_orchestration.watermarks.watermarks (
  source_system_id, schema_name, table_name,
  watermark_column_name, watermark_value, row_count,
  status, created_at, updated_at, run_id, extraction_type
) VALUES (
  'pg_edp', 'public', 'customer',
  'ModifiedDate', '2099-12-31T00:00:00.000000+00:00', 0,
  'COMPLETED', current_timestamp(), current_timestamp(),
  'sentinel-empty-batch-test', 'incremental'
);
```

**Option b** — edit `customer_bronze.yaml` to point at an empty source table.

### 1.2 — Vendor runtime + generate + deploy + run

```bash
cd Example_Projects/edp_lhp_starter

# Vendor the runtime library (per ADR-002 / lhp-runtime-deploy.md)
lhp sync-runtime

lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
databricks bundle validate -t devtest -p dbc-8e058692-373e
databricks bundle deploy   -t devtest -p dbc-8e058692-373e

# Run only the JDBC extract task, not the bronze DLT yet
databricks bundle run \
  -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_workflow
```

### 1.3 — Observe the landing path

```bash
databricks fs ls \
  dbfs:/Volumes/devtest_edp_landing/landing/landing/customer/_lhp_runs/ \
  -p dbc-8e058692-373e

# Expect: at least one <uuid>/ subdirectory containing one *.parquet file
```

Inspect the parquet — it must have schema but zero rows:

```bash
# In a Databricks notebook attached to a serverless cluster:
landing = "/Volumes/devtest_edp_landing/landing/landing/customer/_lhp_runs"
import os
latest = max(
    [f.path for f in dbutils.fs.ls(landing)],
    key=lambda p: dbutils.fs.ls(p)[0].modificationTime,
)
df = spark.read.parquet(latest)
assert df.count() == 0, f"expected zero rows, got {df.count()}"
print("Schema:", df.schema.simpleString())
# Schema must contain CustomerID, FirstName, LastName, EmailAddress, ModifiedDate (or whatever your source has)
```

### 1.4 — Run the bronze DLT

```bash
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_pipeline
```

**Pass criterion**: bronze DLT update finishes `COMPLETED` (not `FAILED`); event log shows `INFO` entries, no `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` exception. Bronze table `devtest_edp_bronze.bronze.customer` exists and is empty (or unchanged from prior runs).

```sql
SELECT COUNT(*) FROM devtest_edp_bronze.bronze.customer;
-- expect: 0 (first run) or whatever the table held before
```

### 1.5 — Cleanup the sentinel watermark row

```sql
DELETE FROM devtest_edp_orchestration.watermarks.watermarks
WHERE run_id = 'sentinel-empty-batch-test';
```

### 1.6 — Outcome

- **PASS** → flip ADR-003 §Q3 from `[~]` to `[x]` in a follow-up commit.
- **FAIL** → capture the DLT event-log JSON for the failed update, file an issue against the watermark branch, do NOT flip §Q3.

---

## Step 2 — A3: generator-side landing-schema overlap guard

**Goal**: confirm `LHPConfigError LHP-CFG-018` raises at `lhp generate` time when `landing_path` resolves into the same UC catalog/schema as the bronze `write_target`. Pure local check; no devtest deploy required.

### 2.1 — Construct a misconfigured pipeline YAML

```bash
cd Example_Projects/edp_lhp_starter

# Save current config first
cp pipelines/02_bronze/customer_bronze.yaml /tmp/customer_bronze.yaml.bak

# Edit landing_path to overlap bronze write_target
# (devtest_edp_bronze.bronze is bronze write target;
#  set landing_path under same catalog+schema)
$EDITOR pipelines/02_bronze/customer_bronze.yaml
# Replace:
#   landing_path: "${landing_volume_root}/customer"
# With:
#   landing_path: /Volumes/devtest_edp_bronze/bronze/landing/customer
```

### 2.2 — Generate

```bash
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
```

**Pass criterion**: generation aborts with:

```
❌ Error [LHP-CFG-018]
```

…and an explanation that `landing_path` schema overlaps `write_target` schema. Generation must NOT produce a `generated/devtest/edp_bronze_jdbc_ingestion/` directory for the misconfigured flow.

### 2.3 — Restore

```bash
mv /tmp/customer_bronze.yaml.bak pipelines/02_bronze/customer_bronze.yaml
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
# expect: clean generation, no LHP-CFG-018
```

### 2.4 — Outcome

- **PASS** → A3 regression guard verified end-to-end. No ADR change needed (§Q5 already `[x]`).
- **FAIL** → A3 generator logic is broken; file an issue against the watermark branch. Investigate `src/lhp/generators/load/jdbc_watermark_job.py::_check_landing_schema_overlap`.

---

## Step 3 — C2: per-env catalog convention end-to-end (devtest)

**Goal**: deploy the full EDP starter to devtest and confirm all three pipelines write to their expected `devtest_edp_*` catalogs and the cross-catalog reads resolve correctly at runtime (not just at generate time).

### 3.1 — Generate + deploy + run

```bash
cd Example_Projects/edp_lhp_starter

# Source data must exist for the bronze pipeline to land non-empty rows.
# If you ran Step 1 (sentinel watermark) ensure that row is gone:
# (SQL: DELETE FROM devtest_edp_orchestration.watermarks.watermarks WHERE run_id='sentinel-empty-batch-test';)

lhp sync-runtime
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
databricks bundle validate -t devtest -p dbc-8e058692-373e
databricks bundle deploy   -t devtest -p dbc-8e058692-373e

# Bronze workflow → bronze DLT → silver DLT → gold DLT (sequential)
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_workflow
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_pipeline
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_silver_curation_pipeline
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_gold_marts_pipeline
```

### 3.2 — Verify per-env writes

```sql
-- All three queries must return rows.
SELECT COUNT(*) AS bronze_rows FROM devtest_edp_bronze.bronze.customer;
SELECT COUNT(*) AS silver_rows FROM devtest_edp_silver.silver.customer;
SELECT COUNT(*) AS gold_rows
  FROM devtest_edp_gold.gold.customer_orders_summary_monthly;
```

### 3.3 — Verify cross-catalog source binding

Pull the silver DLT pipeline event log entry for the `v_customer_bronze` view and confirm the source resolved to `devtest_edp_bronze.bronze.customer` (not some default catalog):

```sql
SELECT details:flow_definition.dataset_name, details:flow_definition.spec
  FROM event_log(TABLE(devtest_edp_silver.silver.event_log_<pipeline_id>))
  WHERE event_type = 'create_view'
    AND details:flow_definition.dataset_name = 'v_customer_bronze'
  ORDER BY timestamp DESC LIMIT 1;
```

The `spec` field must reference `devtest_edp_bronze.bronze.customer`.

### 3.4 — Outcome

- **PASS** → C2 production-shape evidence captured. Drop a screenshot or log excerpt into a follow-up commit referenced from ADR-003 §Q5.
- **FAIL** → identify which pipeline failed, capture event log, file issue. Do not consider §Q5 production-validated.

---

## Step 4 — ADR-004: per-env watermark registry write

**Goal**: confirm that the JDBC extract from Step 3 wrote a row into `devtest_edp_orchestration.watermarks.watermarks` with the expected key shape and `COMPLETED` status. This is the live-environment counterpart to ADR-004's design rationale.

### 4.1 — Prerequisite

Step 3 was run successfully (bronze workflow at minimum).

### 4.2 — Verify

```sql
-- Confirm the registry table exists in the per-env catalog.
DESCRIBE TABLE devtest_edp_orchestration.watermarks.watermarks;

-- Confirm at least one row from this run exists with COMPLETED status.
SELECT
  source_system_id, schema_name, table_name,
  watermark_column_name, watermark_value, row_count,
  status, created_at, updated_at, run_id, extraction_type
FROM devtest_edp_orchestration.watermarks.watermarks
WHERE source_system_id = 'pg_edp'
  AND schema_name      = 'public'
  AND table_name       = 'customer'
ORDER BY updated_at DESC
LIMIT 5;
```

**Pass criteria** (all must hold):

- Table exists in `devtest_edp_orchestration.watermarks` (per-env catalog, NOT in `metadata.orchestration` default).
- At least one row with `status = 'COMPLETED'`.
- `watermark_value` is a parseable timestamp (`ModifiedDate` MAX from the source).
- `row_count` matches what landed in `devtest_edp_bronze.bronze.customer`.
- `run_id` matches a `_lhp_runs/<uuid>/` directory under
  `/Volumes/devtest_edp_landing/landing/landing/customer/`.

Spot-check the cross-env isolation:

```sql
-- These tables should NOT exist if Option B is correctly enforced.
SHOW TABLES IN qa_edp_orchestration.watermarks;
SHOW TABLES IN prod_edp_orchestration.watermarks;
-- (Will succeed if the catalog/schema is provisioned but empty;
--  rows must NOT mention 'pg_edp / public / customer' from the devtest run.)
```

### 4.3 — Outcome

- **PASS** → ADR-004 §Implementation Status row gains a "validated against devtest workspace 2026-04-XX" note in a follow-up commit.
- **FAIL** → ADR-004 §Decision needs revisiting; capture event log + watermark table contents, file issue.

---

## Cleanup (after all four steps pass)

```sql
-- Optional: clear the test rows from devtest tables if you do not want them
-- accumulating. Skip if devtest is also your dev-loop environment.

DELETE FROM devtest_edp_bronze.bronze.customer;
DELETE FROM devtest_edp_silver.silver.customer;
DELETE FROM devtest_edp_gold.gold.customer_orders_summary_monthly;
DELETE FROM devtest_edp_orchestration.watermarks.watermarks
  WHERE source_system_id = 'pg_edp'
    AND schema_name      = 'public'
    AND table_name       = 'customer';
```

```bash
# Remove the run-scoped landing parquet too if disk pressure matters.
databricks fs rm -r \
  dbfs:/Volumes/devtest_edp_landing/landing/landing/customer/_lhp_runs \
  -p dbc-8e058692-373e
```

---

## Follow-up commits to author after PASS

| Step | Commit |
|------|--------|
| 1 (A2 PASS) | Flip [`docs/adr/ADR-003-landing-zone-shape.md`](../adr/ADR-003-landing-zone-shape.md) §Q3 from `[~]` to `[x]`; cite this runbook + the run id. |
| 2 (A3 PASS) | None required (regression guard, not a new closure). |
| 3 (C2 PASS) | Append "validated against devtest workspace 2026-04-XX (run id …)" note to ADR-003 §Q5. |
| 4 (ADR-004 PASS) | Append the same note to ADR-004 §Implementation Status. |

If any step fails, leave the ADR success criteria as they are, file an issue, and stop here — do not run later steps until the failing one is fixed.
