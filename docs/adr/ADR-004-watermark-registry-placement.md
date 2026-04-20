# ADR-004 — Watermark Registry Placement: Shared `metadata` Catalog, Per-Env Schema (Option C)

- **Status**: Accepted (revised 2026-04-19 from Option B → Option C; see §Revision below).
- **Date**: 2026-04-19
- **Deciders**: dwtorres@gmail.com
- **Supersedes**: —
- **Superseded by**: —
- **Related**: [ADR-003](ADR-003-landing-zone-shape.md) §Q5 + Phase C3 of [`docs/planning/adr-003-followups.md`](../planning/adr-003-followups.md); [ADR-001](ADR-001-jdbc-watermark-parquet-post-write-stats.md) (run-scoped landing); [`Example_Projects/edp_lhp_starter/`](../../Example_Projects/edp_lhp_starter/) (canonical evidence).

## Revision (2026-04-19)

Initial draft Accepted Option B (per-env catalog: `<env>_edp_orchestration.watermarks.watermarks`). Skeptical re-review on the same day flipped the decision to Option C (shared `metadata` catalog, per-env schema: `metadata.<env>_orchestration.watermarks`). Same blast-radius semantics at the SCHEMA + TABLE level (the only levels that bound runtime damage), one catalog to provision instead of four, cleaner conceptual model (orchestration is platform infra, not env data), easier cross-env analytics. Catalog-level isolation under Option B was over-weighting a UC primitive that does not bound runtime damage in this workload — `WatermarkManager` writes are scoped per-row by `(source_system_id, schema_name, table_name, run_id)`, and `OPTIMIZE`/`VACUUM`/`GRANT` all work at the schema level on UC. Original Option B rationale below preserved with strikethrough markings.

## Context

The fork's `WatermarkManager` (in `lhp_watermark/_manager.py`) persists run-lifecycle rows to a Delta table referenced by a Unity Catalog `catalog.schema.watermarks` triplet. The JDBC watermark v2 extraction template (`src/lhp/templates/load/jdbc_watermark_job.py.j2:57-61`) instantiates the manager from `wm_catalog` and `wm_schema` template variables, which are populated by the generator from `WatermarkConfig.catalog` and `WatermarkConfig.schema` (`src/lhp/models/pipeline_config.py:WatermarkConfig`). When `WatermarkConfig.catalog` and `WatermarkConfig.schema` are unset, the generator currently defaults to `metadata` and `orchestration`.

The fork is migrating from a single-catalog dev-tier shape (`main._landing.landing` + single `main` catalog) to a per-env, per-medallion shape (`<env>_edp_<medallion>` catalog naming, ADR-003 §Q5 / Phase C2). That migration forces a placement decision for the watermark registry table:

- **Option A — Platform-shared registry**: one Delta table in a dedicated platform catalog (e.g. `_platform.orchestration.watermarks`) carries every run row across every env. Read-side is one query for cross-env analytics.
- **Option B — Per-env registry**: each env owns its own `<env>_edp_orchestration.watermarks` Delta table. No env shares the table with another. Cross-env analytics require explicit catalog references.

The choice has direct operational consequences: governance, blast radius of accidental writes/deletes, and the friction of running concurrent or partially-promoted experiments across envs. ADR-003 §Q5 left this open as a Phase C3 follow-up; Wumbo PR #2 evidence already cleared the broader landing-volume placement question. This ADR closes the registry-placement subset.

### Why this is load-bearing

- The watermark table participates in every JDBC-watermark-v2 run's lifecycle (`insert_new` → `mark_landed` → `mark_complete` and the recovery path via `get_recoverable_landed_run`). A misplaced or misgranted registry stalls every incremental extract.
- Registry rows reference run_ids that point at landing parquet under a SPECIFIC env's external ADLS volume. A cross-env registry lookup that returns the wrong env's run_id would advance the wrong env's watermark on a misrouted call.
- Per-env catalog isolation is the data-platform fork's existing convention. Diverging on the registry alone would create a single platform-shared exception that future maintainers must remember.

## Alternatives

| ID | Alternative | Catalogs to provision | Cross-env query | DROP TABLE blast | DROP SCHEMA blast | DROP CATALOG blast |
|----|-------------|----------------------|-----------------|-------------------|---------------------|----------------------|
| A  | Shared `metadata.orchestration.watermarks` (current Wumbo) | 1 | trivial (single table) | platform-wide | platform-wide | platform-wide |
| B  | Per-env catalog (`<env>_edp_orchestration.watermarks.watermarks`) | 4 (metadata + 3 envs) | UNION ALL across 3 catalogs | per env | per env | per env |
| **C**  | **Shared `metadata` catalog, per-env schema (`metadata.<env>_orchestration.watermarks`)** | **1** | **UNION ALL across 3 schemas (1 catalog)** | **per env** | **per env** | platform-wide (rare admin op) |
| D  | Shared `metadata.orchestration.<env>_watermarks` (env in table name) | 1 | UNION ALL across 3 tables (1 schema) | per env | platform-wide | platform-wide |

## Decision

Adopt **Alternative C (Shared `metadata` catalog, per-env schema)**. One Delta watermark table per env at `metadata.<env>_orchestration.watermarks`, where `<env>` ∈ {`devtest`, `qa`, `prod`}.

Concrete contract codified in [`Example_Projects/edp_lhp_starter/substitutions/devtest.yaml`](../../Example_Projects/edp_lhp_starter/substitutions/devtest.yaml):

```yaml
devtest:
  watermark_catalog: metadata
  watermark_schema:  devtest_orchestration
```

…and consumed in [`Example_Projects/edp_lhp_starter/pipelines/02_bronze/customer_bronze.yaml`](../../Example_Projects/edp_lhp_starter/pipelines/02_bronze/customer_bronze.yaml):

```yaml
watermark:
  column: ModifiedDate
  type: timestamp
  operator: ">="
  source_system_id: pg_edp
  catalog: "${watermark_catalog}"  # → metadata
  schema:  "${watermark_schema}"   # → devtest_orchestration
```

Identical shape repeats in `qa.yaml` (`metadata.qa_orchestration.watermarks`) and `prod.yaml` (`metadata.prod_orchestration.watermarks`). The starter's parametrized e2e test (`tests/e2e/test_edp_lhp_starter.py::TestEdpLhpStarterGeneration::test_per_env_catalog_substitution`) asserts the per-env schema-qualified literal lands in the generated extraction notebook for all three envs.

### Rationale

1. **Operational metadata is platform infrastructure, not env data.** Bronze / silver / gold catalogs are env-scoped because data is env-specific. The watermark registry is operational state about *where extracts are* — same conceptual category as event logs and lineage tables, neither of which warrants a per-env catalog.
2. **Schema-level GRANT bounds runtime damage equivalently to catalog-level.** Unity Catalog supports schema-level `MODIFY` grants. Each env's deploy service principal gets `MODIFY` on its own `<env>_orchestration` schema only — no cross-env writes possible. Catalog-level grants under Option B add no extra blast-radius reduction at the schema or table level (the only levels that matter for runtime damage in this workload).
3. **`OPTIMIZE` and `VACUUM` are table-level commands.** Per-env service principal with `MODIFY` on its schema can run maintenance on its own table without cross-env interference. Option B's catalog-level grant offers no advantage here.
4. **Concurrent run isolation works identically.** `WatermarkManager` keys by `(source_system_id, schema_name, table_name, run_id)`. Per-env schema bounds the catalog/schema portion of the key namespace; cross-env collision impossible.
5. **Cross-env analytics easier than under Option B.** Analyst issues `USE CATALOG metadata` once, then `UNION ALL` across `<env>_orchestration.watermarks` schemas. Under Option B the analyst needs `USE CATALOG` on three separate catalogs.
6. **One catalog to provision instead of four.** New env = create one schema (admin schema-level ticket), not a new catalog (admin catalog-level ticket). Lower friction for env bootstrap.
7. **Cleaner conceptual model.** "Where do I look for a stuck watermark?" → always `metadata.<env>_orchestration`. Mental model is one location with env disambiguator, not "find which `<env>_edp_orchestration` catalog this run touched".

### Why not Option A (shared `metadata.orchestration.watermarks` — current Wumbo)

- Single shared Delta table mixes dev experiments and prod runs. `SHOW HISTORY` requires row filtering for env audits.
- `DROP TABLE` is platform-wide outage. Misconfigured `source_system_id` in dev (e.g. `pg_prod`) writes to a row prod will then read — composite key prevents collision but not correctness.
- Maintenance grant (`MODIFY` on the shared table) is conflated across envs.

### Why not Option B (per-env catalog `<env>_edp_orchestration.watermarks.watermarks` — initial draft of this ADR)

- 4 catalogs to provision instead of 1. New env requires a catalog-level admin ticket instead of a schema-level one.
- Catalog-level isolation buys nothing the schema-level isolation in Option C doesn't already buy at runtime. The only thing catalog-level isolation adds is `DROP CATALOG` blast radius — but `DROP CATALOG` is a rare admin-authority op, not a runtime-reachable command.
- Cross-env reporting queries require `USE CATALOG` on three catalogs instead of one.
- Diverges from the natural conceptual model ("orchestration is platform infra"). Forces engineers to remember a `_platform`-style exception inside a per-env naming scheme.

### Why not Option D (env in table name `metadata.orchestration.<env>_watermarks`)

- Shared schema = shared `OPTIMIZE` / `VACUUM` permission boundary. Slight regression vs C.
- Naming friction: queries must remember the `<env>_` prefix on the table name; less greppable than a schema-qualified path.
- No advantage over C; strictly worse on the maintenance dimension.

## Consequences

### Positive

- **One catalog to provision (`metadata`).** Already exists in most Databricks accounts; reuse simplifies platform team work.
- **Per-env schema bounds runtime blast radius** at the schema + table level identically to Option B.
- **Per-env service principals own per-env schemas** via UC schema-level `MODIFY` grant. No cross-grants required.
- **Cross-env analytics**: one `USE CATALOG metadata` then `UNION ALL` across `<env>_orchestration.watermarks` schemas — fewer ceremony than Option B.
- **Conceptual clarity**: orchestration is platform infra; mental model "always look in `metadata.<env>_orchestration`" is uniform.

### Negative

- **`DROP CATALOG metadata`** would wipe all envs' orchestration in one stroke. Mitigation: `DROP CATALOG` is admin-only, requires explicit acknowledgement in UC, and rarely runs outside disaster recovery — it is not reachable from any pipeline-level grant.
- **Cross-env reporting still requires three queries / one `UNION ALL`.** Same cost as Option B.
- **Per-env schema provisioning** (3 schemas instead of 1 table). Trivial; one-time per env bootstrap.

### Neutral

- **Schema naming `<env>_orchestration`**: explicit purpose suffix avoids collision with future schemas in the `metadata` catalog (e.g. `lineage`, `audit`). The Delta table inside is `watermarks` so the fully-qualified name is `metadata.<env>_orchestration.watermarks`.
- **`metadata` catalog stewardship**: typically owned by platform team. EDP deploy service principals get `USE CATALOG` on `metadata` (low privilege) plus `MODIFY` on their own `<env>_orchestration` schema only.

## Implementation Status

Codified by [`Example_Projects/edp_lhp_starter/`](../../Example_Projects/edp_lhp_starter/) (PR #8 + this PR). Three substitution files (`devtest.yaml`, `qa.yaml`, `prod.yaml`) declare `watermark_catalog: metadata` and `watermark_schema: <env>_orchestration`. The bronze JDBC pipeline (`pipelines/02_bronze/customer_bronze.yaml`) consumes these as `WatermarkConfig.catalog/schema`. The parametrized e2e test asserts the per-env, schema-qualified literal lands in the generated extraction notebook for all three envs.

No LHP code change required — `WatermarkConfig.catalog` and `WatermarkConfig.schema` already exist in `src/lhp/models/pipeline_config.py` and the generator already plumbs them through the JDBC watermark template (`wm_catalog`, `wm_schema`).

### Provisioning runbook (one-time per workspace + per env)

```sql
-- 1. Run ONCE per workspace, as a metastore admin.
CREATE CATALOG IF NOT EXISTS metadata
  COMMENT 'Platform-shared catalog for operational metadata (orchestration, lineage, audit).';

-- 2. Run PER env (devtest, qa, prod), as a metastore or catalog admin.
CREATE SCHEMA IF NOT EXISTS metadata.<env>_orchestration
  COMMENT 'Watermark registry — populated by lhp_watermark.WatermarkManager.';

-- The watermarks table itself is created on first use by WatermarkManager
-- (DDL in lhp_watermark/_manager.py::_ensure_watermark_table). No manual
-- CREATE TABLE required.

-- 3. Grant per env. Use the SAME service principal declared in
-- databricks.yml per target.
GRANT USE CATALOG ON CATALOG metadata TO `<env>-deploy-sp`;
GRANT USE SCHEMA  ON SCHEMA  metadata.<env>_orchestration TO `<env>-deploy-sp`;
GRANT MODIFY      ON SCHEMA  metadata.<env>_orchestration TO `<env>-deploy-sp`;
GRANT SELECT      ON SCHEMA  metadata.<env>_orchestration TO `<env>-deploy-sp`;
```

`USE CATALOG` on `metadata` is a low-privilege grant; the SP cannot read or modify other envs' schemas without their own `USE SCHEMA` + `MODIFY` grants. Catalog-level metadata visibility (`SHOW SCHEMAS IN metadata`) is acceptable cross-env leakage — schema names are not sensitive.

## ADR-003 §Q5 closure

This ADR satisfies the C3 success criterion of [`docs/planning/adr-003-followups.md`](../planning/adr-003-followups.md) and removes the last open Q5 sub-item. ADR-003 §Q5 is therefore ready to flip from `[~]` partially-closed to `[x]` closed once this ADR + the C2 starter PR (#8) merge.

## Open questions (deferred)

- **Cross-env reporting view.** When the first cross-env "watermark advancement" report request lands, design a Databricks SQL view that unions the three per-env schemas. Out of scope.
- **Schema migration.** When the watermark table schema changes (a new column on `lhp_watermark/_manager.py::_ensure_watermark_table`), every env's table must migrate. The runtime currently uses `ALTER TABLE … ADD COLUMNS IF NOT EXISTS`. Capture migration evidence in a future ADR if/when a destructive schema change is needed.
- **`metadata` catalog co-tenancy.** Future operational metadata (lineage, audit) will share the `metadata` catalog with `<env>_orchestration` schemas. ADR is silent on naming for those — likely `metadata.<env>_lineage`, `metadata.<env>_audit`. Capture in a follow-up ADR when those workloads materialize.
