# ADR-004 — Watermark Registry Placement: Per-Env Catalog (Option B)

- **Status**: Accepted
- **Date**: 2026-04-19
- **Deciders**: dwtorres@gmail.com
- **Supersedes**: —
- **Superseded by**: —
- **Related**: [ADR-003](ADR-003-landing-zone-shape.md) §Q5 + Phase C3 of [`docs/planning/adr-003-followups.md`](../planning/adr-003-followups.md); [ADR-001](ADR-001-jdbc-watermark-parquet-post-write-stats.md) (run-scoped landing); [`Example_Projects/edp_lhp_starter/`](../../Example_Projects/edp_lhp_starter/) (canonical evidence).

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

| ID | Alternative | Storage shape | Cross-env query | Blast radius |
|----|-------------|---------------|-----------------|--------------|
| A  | Platform-shared (`_platform.orchestration.watermarks`) | One Delta table | One query, no joins | Whole platform on accidental DROP/DELETE |
| B  | Per-env (`<env>_edp_orchestration.watermarks`) | One Delta table per env | Explicit catalog refs in cross-env query | Single env per accidental DROP/DELETE |
| C  | Per-env-per-medallion (`<env>_edp_orchestration.<medallion>_watermarks`) | One Delta table per env × medallion | Multi-table joins per env | Single (env, medallion) per drop |

## Decision

Adopt **Alternative B (Per-env registry)** at the granularity of one `watermarks` Delta table per environment, in the `<env>_edp_orchestration` catalog. The schema name is `watermarks` (singular catalog, plural table — consistent with Databricks UC conventions and the fork's existing data_platform layout).

Concrete contract codified in [`Example_Projects/edp_lhp_starter/substitutions/devtest.yaml`](../../Example_Projects/edp_lhp_starter/substitutions/devtest.yaml):

```yaml
devtest:
  watermark_catalog: devtest_edp_orchestration
  watermark_schema:  watermarks
```

…and consumed in [`Example_Projects/edp_lhp_starter/pipelines/02_bronze/customer_bronze.yaml`](../../Example_Projects/edp_lhp_starter/pipelines/02_bronze/customer_bronze.yaml):

```yaml
watermark:
  column: ModifiedDate
  type: timestamp
  operator: ">="
  source_system_id: pg_edp
  catalog: "${watermark_catalog}"  # → devtest_edp_orchestration in devtest
  schema:  "${watermark_schema}"   # → watermarks
```

Identical shape repeats in `qa.yaml` (`qa_edp_orchestration.watermarks`) and `prod.yaml` (`prod_edp_orchestration.watermarks`). The starter's parametrized e2e test (`tests/e2e/test_edp_lhp_starter.py::TestEdpLhpStarterGeneration::test_per_env_catalog_substitution`) asserts the per-env catalog literal lands in the generated extraction notebook for all three envs.

### Rationale

1. **Catalogs are the primary unit of data isolation in Unity Catalog** (Databricks docs, "Unity Catalog best practices"). Splitting at the catalog boundary aligns the registry's blast radius with the rest of the EDP data plane.
2. **Deletion blast radius bounded per env.** A `DROP TABLE devtest_edp_orchestration.watermarks.watermarks` cannot stall qa or prod extracts. Under Option A, an analogous mistake stalls the entire platform.
3. **Permission model is per-env already.** The deploy service principals are per env (`run_as: <devtest-sp>`, `<qa-sp>`, `<prod-sp>` in `databricks.yml`). A platform-shared registry forces a single SP with `MANAGE` on the platform catalog or every per-env SP cross-granted on the platform catalog — both are governance regressions.
4. **Concurrent run isolation already correct under Option B.** `WatermarkManager` keys by `(source_system_id, schema_name, table_name, run_id)`. Per-env registries cannot collide because no two envs share a `(catalog, schema)` ancestor for the manager to read from.
5. **Cross-env analytical queries are post-hoc, not runtime-critical.** The use case for cross-env reads is "did the same source advance the same way across dev/qa/prod over the last week?" That is a reporting query, run by an analyst with explicit `USE CATALOG` privileges, not a runtime path. Cost: one `UNION ALL` across `<env>_edp_orchestration.watermarks.watermarks` per env.
6. **Convention matches the existing data_platform fork.** Engineers moving between projects see one `<env>_edp_<medallion>` shape, not a special-cased `_platform` exception for the registry.

### Why not Option A (platform-shared)

- Single point of governance failure. A `MANAGE` grant scoped to one platform catalog couples every env's deploy SP to that catalog's permissions tree.
- Cross-env writes from a single table conflate audit trails. `SHOW HISTORY` on the platform table mixes dev experiments and prod runs; auditors must filter by row.
- Diverges from the fork's per-env convention everywhere else.
- Cross-env queries are easy under Option B with `UNION ALL`. The "single query" advantage is real but small.

### Why not Option C (per-env-per-medallion)

- The watermark table is structurally one table — its row keys (`source_system_id`, `schema_name`, `table_name`, `run_id`) already namespace by source. Splitting per medallion (bronze, silver, gold) adds two more tables per env without changing isolation semantics.
- Recovery (`get_recoverable_landed_run`) already keys per source; cross-medallion recovery never happens (silver does not produce watermark-bearing runs).
- More schema files, more migrations, no operational benefit.

## Consequences

### Positive

- **Blast radius matches catalog boundary.** A schema-level mistake (DROP, ALTER, GRANT REVOKE) cannot cross envs.
- **Per-env service principals can own their own registry** without cross-grants. Platform team's IAM/permission story is consistent across all `<env>_edp_*` catalogs.
- **No special-case in the data plane.** Engineers debugging a watermark issue look in `<env>_edp_orchestration` — same shape as bronze/silver/gold lookup.
- **Generator default removed friction.** `WatermarkConfig.catalog/schema` are explicitly required in the EDP starter (no falling back to `metadata.orchestration`); a missing value at generate time becomes immediately visible.

### Negative

- **Cross-env reporting queries require an explicit `UNION ALL` per env.** Acceptable; one-time per report.
- **Three-table provisioning per fresh environment** (one watermark table per env). Trivial; happens at env bootstrap.
- **No single audit row for "every watermark advancement across the platform".** If/when the platform team needs that view, build a downstream materialized view that unions the three sources. Out of scope for this ADR.

### Neutral

- **Schema name `watermarks`**: singular catalog, plural table. Matches Databricks UC convention (`information_schema.tables` etc.) and the data_platform fork's existing pattern. The Delta table inside is also called `watermarks` so the fully-qualified name is `<env>_edp_orchestration.watermarks.watermarks`. The repetition is intentional — schema and table sit at different UC namespace levels.

## Implementation Status

Codified by [`Example_Projects/edp_lhp_starter/`](../../Example_Projects/edp_lhp_starter/) (PR #8). Three substitution files (`devtest.yaml`, `qa.yaml`, `prod.yaml`) declare `watermark_catalog: <env>_edp_orchestration` and `watermark_schema: watermarks`. The bronze JDBC pipeline (`pipelines/02_bronze/customer_bronze.yaml`) consumes these as `WatermarkConfig.catalog/schema`. The parametrized e2e test asserts the per-env catalog literal lands in the generated extraction notebook for all three envs.

No LHP code change required — `WatermarkConfig.catalog` and `WatermarkConfig.schema` already exist in `src/lhp/models/pipeline_config.py` and the generator already plumbs them through the JDBC watermark template (`wm_catalog`, `wm_schema`).

### Provisioning runbook (per env, one-time)

```sql
-- Run as a workspace admin in the target env's catalog.
CREATE CATALOG IF NOT EXISTS <env>_edp_orchestration
  COMMENT 'Per-env orchestration metadata: watermark registry.';

CREATE SCHEMA IF NOT EXISTS <env>_edp_orchestration.watermarks
  COMMENT 'Watermark registry — populated by lhp_watermark.WatermarkManager.';

-- The watermarks table itself is created on first use by WatermarkManager
-- (DDL in lhp_watermark/_manager.py::_ensure_watermark_table). No manual
-- CREATE TABLE required.

GRANT USE CATALOG ON CATALOG <env>_edp_orchestration TO `<env>-deploy-sp`;
GRANT USE SCHEMA  ON SCHEMA  <env>_edp_orchestration.watermarks TO `<env>-deploy-sp`;
GRANT MODIFY      ON SCHEMA  <env>_edp_orchestration.watermarks TO `<env>-deploy-sp`;
GRANT SELECT      ON SCHEMA  <env>_edp_orchestration.watermarks TO `<env>-deploy-sp`;
```

Repeat per env. The `<env>-deploy-sp` is the same service principal already declared in `databricks.yml` per target.

## ADR-003 §Q5 closure

This ADR satisfies the C3 success criterion of [`docs/planning/adr-003-followups.md`](../planning/adr-003-followups.md) and removes the last open Q5 sub-item. ADR-003 §Q5 is therefore ready to flip from `[~]` partially-closed to `[x]` closed once this ADR + the C2 starter PR (#8) merge.

## Open questions (deferred)

- **Cross-env reporting view.** When the first cross-env "watermark advancement" report request lands, design a Databricks SQL view that unions the three per-env tables. Out of scope.
- **Platform-shared backup registry.** A future ADR can address whether to add a write-through replicator from each per-env table to a read-only platform table for compliance. Out of scope.
- **Schema migration.** When the watermark table schema changes (a new column on `lhp_watermark/_manager.py::_ensure_watermark_table`), every env's table must migrate. The runtime currently uses `ALTER TABLE … ADD COLUMNS IF NOT EXISTS`. Capture migration evidence in a future ADR if/when a destructive schema change is needed.
