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
| 1 | A3 — generator-side landing-schema overlap guard | [#7](https://github.com/dwtorres/Lakehouse_Plumber/pull/7) | ADR-003 §Q5 (already closed; this is a regression-guard smoke) |
| 2 | C2 — per-env catalog convention end-to-end | [#8](https://github.com/dwtorres/Lakehouse_Plumber/pull/8) | ADR-003 §Q5 (already closed; this is the production-shape evidence). **Bootstraps the watermark registry table** as a side effect (auto-CREATE on first `WatermarkManager()` call). |
| 3 | ADR-004 — per-env watermark registry write | [#9](https://github.com/dwtorres/Lakehouse_Plumber/pull/9) | confirms ADR-004 §Implementation Status |
| 4 | A2 — empty-batch schema-bearing fallback | [#6](https://github.com/dwtorres/Lakehouse_Plumber/pull/6) | ADR-003 §Q3 `[~]` → `[x]`. **Requires Step 2 to have run first** (Step 4's sentinel `INSERT INTO metadata.<env>_orchestration.watermarks` only works after Step 2 auto-created the table). |

Run in numerical order. Step 1 (A3) is local-only and can run any time. Steps 2 + 3 share the same deploy and can be observed in one bundle run. Step 4 (A2) **must** follow Step 2 because the Delta watermark table is created on first `WatermarkManager()` call — INSERT against a non-existent table fails with `[TABLE_OR_VIEW_NOT_FOUND]`.

---

## Prerequisites

### Provisioning matrix (pre-provision vs auto-create)

What must exist BEFORE you run the runbook, vs what LHP / WatermarkManager / DAB will create on first use. Captured during the 2026-04-20 devtest validation pass; update if the platform shape changes.

| Object | Pre-provision (admin)? | Auto-created by? | Notes |
|---|---|---|---|
| Catalog `metadata` | **YES** (one-time, metastore admin) | — | Shared platform catalog (ADR-004 Option C). |
| Schema `metadata.<env>_orchestration` | **YES** (one-time per env, catalog admin) | — | Required for the watermark table to land in the right schema. |
| Table `metadata.<env>_orchestration.watermarks` | NO | `WatermarkManager.__init__` (`src/lhp_watermark/watermark_manager.py:121` → `_ensure_table_exists` line 155) on first extract task run | Delta table with liquid clustering on `(source_system_id, schema_name, table_name)`. |
| Catalog `<env>_edp_bronze` | **YES** | — | Bronze medallion. |
| Catalog `<env>_edp_silver` | **YES** | — | Silver medallion. |
| Catalog `<env>_edp_gold` | **YES** | — | Gold medallion. |
| Catalog `<env>_edp_landing` | **YES** | — | Landing zone catalog. |
| Schema `<env>_edp_landing.landing` | **YES** | — | Container for the landing volume. |
| External UC volume `<env>_edp_landing.landing.landing` | **YES** (platform team; external ADLS location) | — | EXTERNAL volume backed by ADLS Gen2. Sidesteps `LOCATION_OVERLAP` (ADR-003 §Q5). A MANAGED volume works for runbook validation but is not the production shape. |
| Bronze `streaming_table` `<env>_edp_bronze.bronze.<table>` | NO | DLT runtime on first pipeline update | One per LHP write action with `type: streaming_table`. |
| Silver `streaming_table` `<env>_edp_silver.silver.<table>` | NO | DLT runtime | Same. |
| Gold `materialized_view` `<env>_edp_gold.gold.<table>` | NO | DLT runtime | Same. |
| Landing parquet `/Volumes/<env>_edp_landing/landing/landing/<source>/_lhp_runs/<uuid>/` | NO | JDBC extraction notebook write | Run-scoped per ADR-001. |
| DAB resource files `resources/lhp/*.pipeline.yml` | NO | `lhp generate -e <env>` | Regenerated per env (literal catalog values baked from substitutions). |
| Generated `.py` files `generated/<env>/...` | NO | `lhp generate -e <env>` | Per env. |
| Vendored runtime `lhp_watermark/` directory in bundle root | NO | `lhp sync-runtime` | Per ADR-002 amendment 2026-04-19; required before `databricks bundle deploy`. |
| Secret scope (e.g. `dev-secrets`) and JDBC keys | **YES** (workspace admin) | — | Production: typically Azure Key Vault-backed; dev: a Databricks-backend scope is fine. |

### Steps to run before this runbook

1. **Repo state**: branch `watermark` checked out, up to date with `origin/watermark` (post #6/#7/#8/#9/#10 merges).
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

3. **Databricks CLI + Terraform**:
   ```bash
   databricks --version            # expect: Databricks CLI v0.295.0+
   databricks auth profiles | grep dbc-8e058692-373e   # expect: VALID
   databricks current-user me -p dbc-8e058692-373e

   # `databricks bundle deploy` ships its own Terraform. As of 2026-04-20 the
   # signed-checksum verification on the bundled binary fails with
   # `openpgp: key expired`. Workaround: pin to a system-installed terraform
   # and override the version check.
   terraform --version             # require any 1.x; e.g. v1.14.8
   export DATABRICKS_TF_EXEC_PATH=$(which terraform)
   export DATABRICKS_TF_VERSION=$(terraform --version | head -1 | awk '{print $2}' | sed 's/^v//')
   ```

4. **Catalogs + schemas + landing volume pre-provisioned** per the matrix above. Verify:
   ```bash
   databricks catalogs list -p dbc-8e058692-373e | \
     grep -E 'devtest_edp_(bronze|silver|gold|landing)|^metadata'
   databricks schemas list metadata -p dbc-8e058692-373e | \
     grep devtest_orchestration
   databricks schemas list devtest_edp_landing -p dbc-8e058692-373e | \
     grep '^devtest_edp_landing\.landing '
   databricks volumes list devtest_edp_landing landing -p dbc-8e058692-373e | \
     grep '"name": "landing"'
   ```

5. **Edit `databricks.yml` placeholders** in the EDP starter to your real workspace + service principal (one-time; commit to a private branch if values are sensitive). For the devtest workspace `dbc-8e058692-373e.cloud.databricks.com` the substitution is mechanical:
   ```bash
   cd Example_Projects/edp_lhp_starter
   sed -i '' \
     's|https://<your-devtest-workspace>.cloud.databricks.com|https://dbc-8e058692-373e.cloud.databricks.com|' \
     databricks.yml
   # If qa/prod targets are also being tested, replace the SP UUIDs too.
   ```

6. **Source secrets present** in a Databricks scope. The starter's `substitutions/devtest.yaml` aliases the secret scope as `database`. Out of the box this maps to `devtest_db_secrets`; in the devtest workspace tested 2026-04-20 the only existing scope was `dev-secrets`, which has only `jdbc_user` + `jdbc_password` (no `pg_host`). For a full Step 2 run you must EITHER:
   - Add `pg_host`, `pg_user`, `pg_password` keys to a scope and update `substitutions/devtest.yaml::secrets.scopes.database` to point at it, OR
   - Edit `pipelines/02_bronze/customer_bronze.yaml` to point at a JDBC source whose credentials are already available.

   Without working source secrets, the Step 2 extraction task fails with `IllegalArgumentException: Secret does not exist with scope: <scope> and key: pg_host`. The watermark table still bootstraps (WatermarkManager init runs before the JDBC read), so Steps 3 and 4 can still be partially validated against the empty bootstrapped table. See §Step 2 for the failure mode.

   **Production note**: in real deployments these scopes are typically Databricks-secret-scopes backed by Azure Key Vault (`databricks secrets create-scope --scope-backend-type AZURE_KEYVAULT ...`). The starter is scope-name agnostic — only the substitution alias matters.

---

## Step 1 — A3: generator-side landing-schema overlap guard

**Goal**: confirm `LHPConfigError LHP-CFG-018` raises at `lhp generate` time when `landing_path` resolves into the same UC catalog/schema as the bronze `write_target`. Pure local check; no devtest deploy required.

This step runs first because it has zero deploy dependencies and zero side effects. It can also be re-run any time without affecting later steps.

### 1.1 — Construct a misconfigured pipeline YAML

The A3 guard compares `Action.write_target.catalog` and `Action.write_target.schema` literally — substitution tokens are NOT resolved before comparison. If the production starter uses `catalog: "${bronze_catalog}"` and your `landing_path` uses a literal catalog name, the catalog values do not match (string `${bronze_catalog}` ≠ string `devtest_edp_bronze`) so only the cross-catalog WARNING fires, not the LHP-CFG-018 ERROR. The misconfigured YAML below sets BOTH sides to literal devtest values so the guard's same-catalog branch is reached.

```bash
cd Example_Projects/edp_lhp_starter

# Save current config first
cp pipelines/02_bronze/customer_bronze.yaml /tmp/customer_bronze.yaml.bak

# Two edits in one pass:
# 1. landing_path must be UC volume path under devtest_edp_bronze.bronze
# 2. write_target.catalog must be literal devtest_edp_bronze (not substitution token)
sed -i '' 's|landing_path: "${landing_volume_root}/customer"|landing_path: /Volumes/devtest_edp_bronze/bronze/landing/customer|' \
  pipelines/02_bronze/customer_bronze.yaml
sed -i '' 's|catalog: "${bronze_catalog}"|catalog: "devtest_edp_bronze"|' \
  pipelines/02_bronze/customer_bronze.yaml
```

### 1.2 — Generate

```bash
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
```

**Pass criterion**: among the per-pipeline output, the bronze pipeline emits:

```
❌ Error [LHP-CFG-018]: landing_path overlaps bronze write schema
```

with body text identifying the action name and pointing at remediation (move landing to a dedicated schema, or use abfss://). The other pipelines (silver, gold) generate normally.

**Important**: `lhp generate` exit code is **0** even when a per-pipeline LHP-CFG-018 fires. The error is rendered to stdout and the affected pipeline's output dir contains stale prior content. Automation that gates on exit code will NOT catch this — grep stdout for `LHP-CFG-018` instead. Confirmed against generator commit `<see follow-up A3 dict-fix commit>`.

The bronze pipeline directory still appears under `generated/devtest/edp_bronze_jdbc_ingestion/` because prior `--force` runs left it there; LHP does not delete it on per-pipeline failure. Inspect `git status` to confirm the bronze pipeline did not regenerate fresh content.

### 1.3 — Restore

```bash
cp /tmp/customer_bronze.yaml.bak pipelines/02_bronze/customer_bronze.yaml
rm -rf generated/  # wipe stale bronze artifacts
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
# expect: 3 pipelines generated, no LHP-CFG-018 in stdout
```

### 1.4 — Outcome

- **PASS** → A3 regression guard verified end-to-end. No ADR change needed (§Q5 already `[x]`).
- **FAIL — only WARNING fires, no ERROR** → A3's substitution-blindness is hitting your scenario. Confirm both `landing_path` and `write_target.catalog` are LITERAL strings (no `${...}` tokens). If still failing, the `isinstance(write_target, dict)` branch in `_check_landing_schema_overlap` may be bypassed — check generator implementation.
- **FAIL — generator crashes** → A3 logic broken; file an issue. Investigate `src/lhp/generators/load/jdbc_watermark_job.py::_check_landing_schema_overlap`.

**Known limitation**: the A3 check operates on raw YAML strings, not post-substitution values. A production scenario where `landing_path` and `write_target.catalog` BOTH come from substitutions resolving to the same final catalog is NOT caught at generate time — the check fires only when the literal strings happen to match. Long-term fix: resolve substitutions before comparing. Tracked separately from this runbook.

---

## Step 2 — C2: per-env catalog convention end-to-end (devtest)

**Goal**: deploy the full EDP starter to devtest and confirm all three pipelines write to their expected `devtest_edp_*` catalogs and the cross-catalog reads resolve correctly at runtime (not just at generate time).

**Bootstrap side effect**: this step is the first thing that calls `WatermarkManager()` against `metadata.devtest_orchestration` and therefore auto-creates the `metadata.devtest_orchestration.watermarks` Delta table via `CREATE TABLE IF NOT EXISTS` (`src/lhp_watermark/watermark_manager.py::_ensure_table_exists`). Steps 3 and 4 depend on this table existing.

### 2.1 — Pre-flight: SP grants on the metadata schema

The deploy SP (or interactive user, in dev mode) must have `MODIFY` on the per-env schema for `CREATE TABLE IF NOT EXISTS` to succeed during WatermarkManager bootstrap. If absent, the first run fails with `Permission denied: CREATE TABLE`.

```bash
# Effective grants endpoint:
databricks api get \
  /api/2.1/unity-catalog/effective-permissions/SCHEMA/metadata.devtest_orchestration \
  -p dbc-8e058692-373e
```

```sql
-- Or via SQL:
SHOW GRANTS ON SCHEMA metadata.devtest_orchestration;
-- Required for the deploy principal: USE SCHEMA, MODIFY, SELECT
-- If MODIFY is absent:
--   GRANT MODIFY ON SCHEMA metadata.devtest_orchestration TO `<deploy-principal>`;
```

In `mode: development` DAB targets the deploy principal IS the interactive user (`workspace.User` in `databricks bundle summary`), so personal-token auth without separate SP grants generally works for devtest.

### 2.2 — Generate + sync runtime + validate + deploy

```bash
cd Example_Projects/edp_lhp_starter

# Required env exports captured in §Prerequisites step 3:
#   DATABRICKS_TF_EXEC_PATH=$(which terraform)
#   DATABRICKS_TF_VERSION=<your-installed-tf-version>

lhp sync-runtime
# Creates ./lhp_watermark/ — DAB will sync this to the workspace alongside
# generated notebooks. Re-run after every LHP upgrade.

lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
databricks bundle validate -t devtest -p dbc-8e058692-373e
# expect: "Validation OK!"
# If it errors with "the host in the profile (...) doesn't match the host
# configured in the bundle (https://<your-...>...)", you skipped Prereq §5.

databricks bundle deploy -t devtest -p dbc-8e058692-373e
# expect: "Deployment complete!"
# If it errors with "error downloading Terraform: unable to verify checksums
# signature: openpgp: key expired", the DATABRICKS_TF_EXEC_PATH +
# DATABRICKS_TF_VERSION env exports from Prereq §3 are not set in this shell.

# Inspect what got created:
databricks bundle summary -t devtest -p dbc-8e058692-373e
# expect: 1 Job (edp_bronze_jdbc_ingestion_workflow)
#         3 Pipelines (edp_bronze_jdbc_ingestion_pipeline,
#                      edp_silver_curation_pipeline,
#                      edp_gold_marts_pipeline)
# All names prefixed [dev <user>] when mode: development.
```

### 2.3 — Run the JDBC extract task (bootstraps registry)

```bash
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_workflow
```

**Two outcomes are POSSIBLE and BOTH advance Step 3**:

| Outcome | What happened | Bootstrap status | Step 3 / 4 unblocked? |
|---|---|---|---|
| Job task `extract_*` finishes COMPLETED | WatermarkManager init AND JDBC read both succeeded. Watermark row written. | YES | YES, with row to verify in Step 3 |
| Job task `extract_*` fails with `IllegalArgumentException: Secret does not exist with scope: <scope> and key: pg_host` (or analogous secret-resolution error) | WatermarkManager init succeeded. JDBC read failed at `dbutils.secrets.get` BEFORE the JDBC connection. | YES (verified in §2.4) | YES for Step 3 schema check; Step 4 sentinel works because table exists |
| Job task fails with `Permission denied: CREATE TABLE` | WatermarkManager init failed at `_ensure_table_exists`. SP missing MODIFY on `metadata.<env>_orchestration`. | NO | NO — go fix grants per §2.1 |
| Job task fails with `[CATALOG_OR_SCHEMA_NOT_FOUND]` on `metadata.<env>_orchestration` | Pre-provisioning incomplete. | NO | NO — provision schema per §Prerequisites matrix |

The first two outcomes BOTH satisfy this runbook's Step 2 bootstrap goal. The third and fourth are misconfiguration; fix and retry.

### 2.4 — Verify the watermark table was auto-created

```bash
# Use any running SQL warehouse; list to find one:
databricks warehouses list -p dbc-8e058692-373e

# Then query (replace warehouse_id):
databricks api post /api/2.0/sql/statements -p dbc-8e058692-373e --json '{
  "warehouse_id": "<warehouse-id>",
  "statement": "DESCRIBE TABLE metadata.devtest_orchestration.watermarks",
  "wait_timeout": "30s"
}' | python3 -c "import sys,json;d=json.load(sys.stdin);[print(r[0],r[1]) for r in d['result']['data_array']]"
```

Expected schema (verified 2026-04-20 against `_ensure_table_exists`):

```
run_id                    string
watermark_time            timestamp
source_system_id          string
schema_name               string
table_name                string
watermark_column_name     string
watermark_value           string
previous_watermark_value  string
row_count                 bigint
extraction_type           string
bronze_stage_complete     boolean
silver_stage_complete     boolean
status                    string
error_class               string
error_message             string
created_at                timestamp
completed_at              timestamp
# Clustering columns (liquid clustering):
source_system_id, schema_name, table_name
```

If `DESCRIBE TABLE` returns `[TABLE_OR_VIEW_NOT_FOUND]`, the bootstrap did not run — re-check §2.3 outcome and fix the underlying error before continuing to Steps 3 and 4.

### 2.5 — Run the bronze + silver + gold DLT pipelines (only if §2.3 reached COMPLETED)

If the JDBC extract failed at secret resolution but the watermark table bootstrapped, skip this section — the bronze DLT has nothing to consume from `_lhp_runs/`.

```bash
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_pipeline
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_silver_curation_pipeline
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_gold_marts_pipeline
```

### 2.6 — Verify per-env writes (only if §2.5 ran)

```sql
-- All three queries must return rows when §2.5 succeeded.
SELECT COUNT(*) AS bronze_rows FROM devtest_edp_bronze.bronze.customer;
SELECT COUNT(*) AS silver_rows FROM devtest_edp_silver.silver.customer;
SELECT COUNT(*) AS gold_rows
  FROM devtest_edp_gold.gold.customer_orders_summary_monthly;
```

### 2.7 — Verify cross-catalog source binding (only if §2.5 ran)

Pull the silver DLT pipeline event log entry for the `v_customer_bronze` view and confirm the source resolved to `devtest_edp_bronze.bronze.customer` (not some default catalog):

```sql
SELECT details:flow_definition.dataset_name, details:flow_definition.spec
  FROM event_log(TABLE(devtest_edp_silver.silver.event_log_<pipeline_id>))
  WHERE event_type = 'create_view'
    AND details:flow_definition.dataset_name = 'v_customer_bronze'
  ORDER BY timestamp DESC LIMIT 1;
```

The `spec` field must reference `devtest_edp_bronze.bronze.customer`.

### 2.8 — Outcome

- **FULL PASS** (extract + DLT all green) → C2 production-shape evidence captured AND watermark registry table bootstrapped (now exists for Steps 3 + 4). Drop log excerpts into a follow-up commit referenced from ADR-003 §Q5.
- **PARTIAL PASS** (extract failed at secret resolution; bootstrap confirmed via §2.4) → Steps 3 and 4 still partially runnable; do NOT cite as ADR-003 §Q5 production-shape evidence (silver + gold never executed). Capture failure mode in your runbook log and provision real source secrets before re-running.
- **FAIL** (`Permission denied: CREATE TABLE`) → SP missing `MODIFY` on `metadata.<env>_orchestration`. Re-run §2.1 and re-grant. Bootstrap did NOT happen.
- **FAIL** (DLT failure after extract succeeded) → identify which pipeline failed, capture event log, file issue.

---

## Step 3 — ADR-004: per-env watermark registry write

**Goal**: confirm that the JDBC extract from Step 2 wrote a row into `metadata.devtest_orchestration.watermarks` with the expected key shape and `COMPLETED` status. This is the live-environment counterpart to ADR-004's design rationale.

### 3.1 — Prerequisite

Step 2 was run successfully (bronze workflow at minimum). The watermark table now exists at `metadata.devtest_orchestration.watermarks`.

### 3.2 — Verify the row

```sql
-- Confirm at least one row from this run exists with COMPLETED status.
SELECT
  source_system_id, schema_name, table_name,
  watermark_column_name, watermark_value, row_count,
  status, watermark_time, run_id, extraction_type
FROM metadata.devtest_orchestration.watermarks
WHERE source_system_id = 'pg_edp'
  AND schema_name      = 'public'
  AND table_name       = 'customer'
ORDER BY watermark_time DESC
LIMIT 5;
```

**Pass criteria** (all must hold):

- At least one row with `status = 'COMPLETED'`.
- `watermark_value` is a parseable timestamp (`ModifiedDate` MAX from the source).
- `row_count` matches what landed in `devtest_edp_bronze.bronze.customer`.
- `run_id` matches a `_lhp_runs/<uuid>/` directory under
  `/Volumes/devtest_edp_landing/landing/landing/customer/`.

Spot-check the cross-env schema isolation under ADR-004 Option C:

```sql
-- All three schemas live in ONE catalog (metadata).
SHOW SCHEMAS IN metadata LIKE '*_orchestration';
-- expect (after qa/prod bootstrap): devtest_orchestration, qa_orchestration, prod_orchestration

-- The qa/prod schemas may be empty (no qa/prod run executed yet) but
-- when populated, their watermarks tables must NOT contain the devtest
-- run rows — schema-level isolation is the runtime blast-radius bound.
SHOW TABLES IN metadata.qa_orchestration;
SHOW TABLES IN metadata.prod_orchestration;
-- If either schema's `watermarks` table contains rows for source_system_id='pg_edp'
-- with a run_id from this devtest run, the per-env schema isolation has been
-- compromised — investigate WatermarkConfig.schema substitution immediately.
```

### 3.3 — Outcome

- **PASS** → ADR-004 §Implementation Status row gains a "validated against devtest workspace 2026-04-XX" note in a follow-up commit.
- **FAIL** → ADR-004 §Decision needs revisiting; capture event log + watermark table contents, file issue.

---

## Step 4 — A2: empty-batch schema-bearing fallback

**Goal**: prove that an incremental JDBC extract that returns zero rows still leaves a schema-bearing parquet at `${landing_volume_root}/customer/_lhp_runs/<uuid>/`, so the bronze AutoLoader does not fail with `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` on the next run.

### 4.1 — Prerequisite: registry table exists

The sentinel-watermark trick used below `INSERT INTO metadata.devtest_orchestration.watermarks (...)`. The table is auto-created on first `WatermarkManager()` call (Step 2). On a brand-new env this step fails with `[TABLE_OR_VIEW_NOT_FOUND]` if Step 2 has not run.

```sql
-- Sanity check before proceeding:
DESCRIBE TABLE metadata.devtest_orchestration.watermarks;
-- expect: column list. If [TABLE_OR_VIEW_NOT_FOUND], go run Step 2 first.
```

### 4.2 — Force the customer source to return zero rows

Two options:

**Option a (recommended)** — pre-seed a sentinel watermark row at a future timestamp so the extractor's `WHERE ModifiedDate >= '2099-…'` returns zero rows:

```sql
-- Run in `metadata` catalog SQL editor (ADR-004 Option C — shared catalog, per-env schema):
INSERT INTO metadata.devtest_orchestration.watermarks (
  run_id, watermark_time, source_system_id, schema_name, table_name,
  watermark_column_name, watermark_value, previous_watermark_value, row_count,
  extraction_type, status
) VALUES (
  'sentinel-empty-batch-test',
  current_timestamp(),
  'pg_edp', 'public', 'customer',
  'ModifiedDate', '2099-12-31T00:00:00.000000+00:00', NULL, 0,
  'incremental', 'COMPLETED'
);
```

(Adjust the column list and types if the runtime DDL drifts — re-check with `DESCRIBE TABLE metadata.devtest_orchestration.watermarks`.)

**Option b** — edit `customer_bronze.yaml` to point at an empty source table.

### 4.3 — Generate + deploy + run only the extract task

```bash
cd Example_Projects/edp_lhp_starter

lhp sync-runtime
lhp generate -e devtest --pipeline-config config/pipeline_config.yaml --force
databricks bundle deploy -t devtest -p dbc-8e058692-373e

# Run only the JDBC extract task, not the bronze DLT yet
databricks bundle run \
  -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_workflow
```

### 4.4 — Observe the landing path

```bash
databricks fs ls \
  dbfs:/Volumes/devtest_edp_landing/landing/landing/customer/_lhp_runs/ \
  -p dbc-8e058692-373e

# Expect: at least one <uuid>/ subdirectory containing one *.parquet file
```

Inspect the parquet — it must have schema but zero rows:

```python
# In a Databricks notebook attached to a serverless cluster:
landing = "/Volumes/devtest_edp_landing/landing/landing/customer/_lhp_runs"
latest = max(
    [f.path for f in dbutils.fs.ls(landing)],
    key=lambda p: dbutils.fs.ls(p)[0].modificationTime,
)
df = spark.read.parquet(latest)
assert df.count() == 0, f"expected zero rows, got {df.count()}"
print("Schema:", df.schema.simpleString())
# Schema must contain CustomerID, FirstName, LastName, EmailAddress, ModifiedDate
# (or whatever your source has)
```

### 4.5 — Run the bronze DLT

```bash
databricks bundle run -t devtest -p dbc-8e058692-373e \
  edp_bronze_jdbc_ingestion_pipeline
```

**Pass criterion**: bronze DLT update finishes `COMPLETED` (not `FAILED`); event log shows `INFO` entries, no `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` exception. Bronze table `devtest_edp_bronze.bronze.customer` exists and is unchanged from Step 2 (no new rows from the empty-batch run).

```sql
SELECT COUNT(*) FROM devtest_edp_bronze.bronze.customer;
-- expect: same count as after Step 2 (no new rows)
```

### 4.6 — Cleanup the sentinel watermark row

```sql
DELETE FROM metadata.devtest_orchestration.watermarks
WHERE run_id = 'sentinel-empty-batch-test';
```

### 4.7 — Outcome

- **PASS** → flip ADR-003 §Q3 from `[~]` to `[x]` in a follow-up commit.
- **FAIL** → capture the DLT event-log JSON for the failed update, file an issue against the watermark branch, do NOT flip §Q3.

---

## Cleanup (after all four steps pass)

```sql
-- Optional: clear the test rows from devtest tables if you do not want them
-- accumulating. Skip if devtest is also your dev-loop environment.

DELETE FROM devtest_edp_bronze.bronze.customer;
DELETE FROM devtest_edp_silver.silver.customer;
DELETE FROM devtest_edp_gold.gold.customer_orders_summary_monthly;
DELETE FROM metadata.devtest_orchestration.watermarks
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
