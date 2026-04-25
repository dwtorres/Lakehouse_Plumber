# Scheduled Work: Tier 1 HWM Isolation Fix

**Status:** Scheduled, not started
**Priority:** Prerequisite for any LHP integration of Spike A1 or Spike B
**Effort:** 4h
**Blocks:** Options A / B / C in `docs/ideas/spike-a1-vs-b-comparison.md`
**Created:** 2026-04-24

## Problem

Both spike implementations of `get_current_hwm` in `prepare_manifest.py` query the shared `devtest_edp_orchestration.jdbc_spike.watermark_registry` filtered only by `schema_name`, `table_name`, and `status='completed'`. Any pipeline that loaded a table with matching name into the registry advances the HWM for every OTHER pipeline reading that same (schema, table) pair. This caused A1's original scale run to report zero rows — Spike B's earlier run had advanced PG AdventureWorks HWMs to the dataset ceiling, and A1 correctly filtered out all rows on a static historical source. Retest with a cleared registry proved A1 works fine when given fair first-run state; the defect is in the lookup, not the ingestion.

Confirmed by retest `retest_1777031969`: A1 materialized 738,913 rows across 61 tables at exact parity with PG source. The "A1 is broken" narrative was wrong.

## Current get_current_hwm signature

`spikes/jdbc-sdp-a1/tasks/prepare_manifest.py:104-117`:

```python
def get_current_hwm(schema_name: str, table_name: str) -> str:
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name   = :schema_name
          AND table_name    = :table_name
          AND status        = 'completed'
        """,
        args={"schema_name": schema_name, "table_name": table_name},
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO
```

No filter on `source_system_id`, `load_group`, or `run_id`. Registry rows from different federations or pipelines sharing `(schema, table)` names collide.

## Fix (Tier 1)

Add `source_system_id` parameter to both the signature and the WHERE clause. Caller passes the manifest row's `source_catalog` (which is the value stored in registry's `source_system_id` column). No DDL change. No data backfill beyond the single retest cleanup already performed.

### Proposed signature

```python
def get_current_hwm(schema_name: str, table_name: str, source_system_id: str) -> str:
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name      = :schema_name
          AND table_name       = :table_name
          AND source_system_id = :source_system_id
          AND status           = 'completed'
        """,
        args={
            "schema_name": schema_name,
            "table_name": table_name,
            "source_system_id": source_system_id,
        },
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO
```

### Call-site change

Every `get_current_hwm(src_schema, src_table)` in `prepare_manifest.py` becomes `get_current_hwm(src_schema, src_table, src_catalog)` where `src_catalog = row["source_catalog"]` already read from the manifest row in both `fresh` and `failed_only` paths.

## Files to modify

1. `spikes/jdbc-sdp-a1/tasks/prepare_manifest.py`
   - Update function signature (line 104).
   - Update SQL WHERE clause (lines 110–113).
   - Update `args=` dict (line 114).
   - Update three call sites:
     - `fresh` mode (line 139).
     - Any other call in `failed_only` branch if present (read full function to confirm).

2. `spikes/jdbc-sdp-b/tasks/prepare_manifest.py` — same three changes. Spike B's copy mirrors A1's function.

No changes to:
- `watermark_registry_spike.sql` DDL (source_system_id column already exists).
- `reconcile.py` (reads registry by run_id, unaffected by this fix).
- `sdp_pipeline.py` (pipeline reads manifest rows, already carries source_catalog).
- `ingest_one.py` (B per-iteration worker; same pattern, unaffected).

## Tests / verification

This is a spike prototype layer, not production. No unit test harness. Validate via behavioral test:

### V1 — Filter works in isolation (smoke)

Insert a fake HWM row:
```sql
INSERT INTO devtest_edp_orchestration.jdbc_spike.watermark_registry
  (run_id, source_system_id, schema_name, table_name, watermark_column_name,
   watermark_value, previous_watermark_value, row_count, extraction_type,
   status, created_at, updated_at)
VALUES
  ('tier1_fix_probe', 'fake_federation', 'humanresources', 'employee', 'ModifiedDate',
   '2099-01-01T00:00:00', NULL, 0, 'full',
   'completed', current_timestamp(), current_timestamp());
```

Without Tier 1: next A1 or B run against `humanresources.employee` via `pg_supabase` reads HWM `2099-01-01` from the fake row → filter returns 0 source rows → silent poisoning.

With Tier 1: filter adds `AND source_system_id = 'pg_supabase'` → fake row excluded → correct HWM used → data loads normally.

Cleanup:
```sql
DELETE FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
WHERE run_id = 'tier1_fix_probe';
```

### V2 — A1 regression retest

After applying Tier 1, trigger A1 with the registry left populated from Spike B's prior run (no scoped DELETE this time). Expected: A1 completes with 61/61 non-zero bronze tables and exact source parity. Demonstrates the fix obviates the DELETE step entirely.

### V3 — B regression retest

Same pattern as V2 against Spike B's bundle. Expected: B re-runs produce correct incremental behavior regardless of A1's prior HWMs for the same (schema, table) in other federations.

## Rollout plan

1. Apply Tier 1 changes to both `prepare_manifest.py` copies.
2. Run V1 probe.
3. Run V2 retest.
4. Run V3 retest.
5. Commit: `fix(spike-a1,spike-b): Tier 1 HWM isolation — add source_system_id filter to get_current_hwm`.
6. Drop retest backup tables after 7-day retention window:
   - `devtest_edp_orchestration.jdbc_spike.watermark_registry_bak_retest_1777031969`
   - `devtest_edp_orchestration.jdbc_spike.manifest_bak_retest_1777031969`
   - `devtest_edp_orchestration.jdbc_spike.hwm_ceiling_snapshot_retest_1777031969`
   - `devtest_edp_orchestration.jdbc_spike.source_row_counts_retest_1777031969`

## Out of scope

- Tier 2 DDL migration (add `load_group` column). Captured in comparison doc appendix; defer until a case for same-federation-different-pipeline collision emerges.
- Tier 3 per-integration registry tables. Captured in comparison doc appendix; defer unless multi-tenancy isolation becomes a compliance requirement.
- LHP codegen integration. This fix is on the spike prototypes only; LHP template update happens as part of the chosen Option A/B/C rollout.
- Broader `get_current_hwm` generalization (e.g. making it a standalone helper module). Keep inline until the chosen LHP integration path motivates a module.

## How to execute

Treat as a GSD quick task when scheduled:
```
/gsd-quick "Apply Tier 1 HWM isolation: add source_system_id filter to get_current_hwm in both spike prepare_manifest.py copies; validate via V1 probe + V2/V3 retests"
```

Or execute inline: approved by this plan, apply the edits above, run V1–V3, commit.
