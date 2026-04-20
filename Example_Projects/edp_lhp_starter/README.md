# EDP Lakehouse Plumber Starter — per-env catalog convention

Reference Lakehouse Plumber project for the **`<env>_edp_<medallion>`** catalog
convention used by the `Lakehouse_Plumber` fork's `watermark` integration
branch. Mirrors the existing `data_platform` fork's catalog naming so engineers
moving between projects see the same shape.

This starter is the canonical evidence backing ADR-003 §Q5 closure (Phase C2 of
[`docs/planning/adr-003-followups.md`](../../docs/planning/adr-003-followups.md))
and ADR-004 (watermark registry placement = Option B per env).

## Layout

```
edp_lhp_starter/
├── lhp.yaml                    # project name, version, include globs
├── databricks.yml              # 2-workspace bundle: devtest + shared qa/prod
├── substitutions/
│   ├── devtest.yaml            # devtest_edp_* catalogs
│   ├── qa.yaml                 # qa_edp_* catalogs
│   └── prod.yaml               # prod_edp_* catalogs
├── pipelines/
│   ├── 02_bronze/
│   │   └── customer_bronze.yaml    # jdbc_watermark_v2 → bronze catalog
│   ├── 03_silver/
│   │   └── customer_silver.yaml    # cross-catalog: bronze → silver
│   └── 04_gold/
│       └── customer_gold.yaml      # cross-catalog: silver → gold (MV)
└── resources/
    └── lhp/                    # populated by `lhp generate -e <env>`
```

## Catalog convention

| Layer        | Catalog token         | Resolves to (devtest)            | Schema token        |
| ------------ | --------------------- | -------------------------------- | ------------------- |
| Landing      | `{landing_catalog}`   | `devtest_edp_landing`            | `{landing_schema}`  |
| Bronze       | `{bronze_catalog}`    | `devtest_edp_bronze`             | `{bronze_schema}`   |
| Silver       | `{silver_catalog}`    | `devtest_edp_silver`             | `{silver_schema}`   |
| Gold         | `{gold_catalog}`      | `devtest_edp_gold`               | `{gold_schema}`     |
| Watermarks   | `{watermark_catalog}` | `devtest_edp_orchestration`      | `{watermark_schema}`|

Schemas inside each catalog are flat (`bronze`, `silver`, `gold`, `landing`,
`watermarks`) so the same `{X_schema}` token reuses across envs.

## Workspace layout

| Workspace                    | Targets hosted | Isolation              |
| ---------------------------- | -------------- | ---------------------- |
| devtest workspace            | `devtest`      | dedicated, dev mode    |
| shared qa/prod workspace     | `qa`, `prod`   | catalog name + run-as service principal |

The qa and prod targets share a workspace but are isolated by catalog name
(`qa_edp_*` vs `prod_edp_*`) and by separate `run_as` service principals. Adjust
the workspace hostnames and SP UUIDs in `databricks.yml` for your account.

## Cross-catalog reads

LHP supports cross-catalog source reads today via `database: "{X}.{Y}"`
substitution interpolation — no source-side `catalog:` field add was required
on `Action`. See `pipelines/03_silver/customer_silver.yaml`:

```yaml
source:
  type: delta
  database: "{bronze_catalog}.{bronze_schema}"
  table: customer
```

`WriteTarget` already exposes `catalog` and `schema` fields directly; the
modern form is preferred over the deprecated `database:` (REMOVE_AT_V1.0.0):

```yaml
write_target:
  type: streaming_table
  catalog: "{silver_catalog}"
  schema: "{silver_schema}"
  table: customer
```

## Watermark registry placement

The watermark catalog is per-env (`devtest_edp_orchestration`,
`qa_edp_orchestration`, `prod_edp_orchestration`), not platform-shared. This
matches Databricks' "catalogs as primary unit of data isolation" guidance and
bounds the deletion blast radius per env. Tradeoff: cross-env analytical
queries against the watermark table require explicit catalog references. See
ADR-004 for the full rationale.

## External ADLS volumes

The platform team provisions external UC volumes ahead of bundle deploy. This
starter assumes `/Volumes/{landing_catalog}/{landing_schema}/landing` resolves
to an ADLS-Gen2-backed external volume — no Terraform/Bicep is included here
(handled outside the bundle, in the Azure account).

The external-volume shape sidesteps UC `LOCATION_OVERLAP` because external
storage is disjoint from managed catalog roots — the failure mode discovered
in Wumbo PR #2 and validated in ADR-003 §Q5.

## Migration from single-catalog

If migrating from a single-`{catalog}` substitution layout (e.g. ACMI), the
delta is mechanical:

1. Replace `catalog: <single>` in each substitution YAML with the five
   per-medallion `_catalog` variables.
2. Search-and-replace pipeline YAMLs:
   - `database: "{catalog}.{bronze_schema}"` → `database: "{bronze_catalog}.{bronze_schema}"`
   - same for silver/gold/landing/watermarks layers.
3. `WatermarkConfig` blocks: add `catalog: "{watermark_catalog}"` and
   `schema: "{watermark_schema}"` (both default to `metadata` / `orchestration`
   if unset, but explicit is better in multi-env deploys).
4. `databricks.yml` targets: set `variables.default_pipeline_catalog` to the
   per-env bronze catalog.

## Generating

```bash
cd Example_Projects/edp_lhp_starter
lhp generate -e devtest               # writes to generated/devtest/
databricks bundle validate -t devtest # validate the DAB resource files
databricks bundle deploy -t devtest   # deploy to devtest workspace
```

For qa/prod, swap `-e devtest` → `-e qa` or `-e prod` and ensure your CLI is
authenticated against the shared qa/prod workspace.
