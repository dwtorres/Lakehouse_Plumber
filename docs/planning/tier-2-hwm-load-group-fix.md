# Scheduled Work: Tier 2 HWM `load_group` Fix

**Status:** Scheduled, not started
**Priority:** Prerequisite for any LHP integration of Spike B2
**Effort:** see Effort section
**Blocks:** B2 integration (`b2-watermark-scale-out-design.md`)
**Created:** 2026-04-25
**Source plan:** `~/.claude/plans/cozy-scribbling-spring.md` (Revision 4)
**Predecessor fix:** Tier 1 (commit `63bcfdb`, V1-proven)

---

## Problem

Tier 1 filters HWM lookups by `source_system_id`. This closes cross-source poisoning
(e.g., `pg_supabase` vs `freesql_catalog` sharing a schema+table name).

It does **not** close same-source cross-flowgroup poisoning. Two LHP flowgroups both
targeting `source_system_id = 'pg_supabase'` against overlapping `(schema, table)` pairs
still collide: whichever flowgroup runs first advances the HWM; the other reads that HWM
as its starting point and correctly returns zero new rows on a static-data slice.

B2's flowgroup-oriented batching relies on per-pipeline HWM identity. The registry has no
column that distinguishes one LHP flowgroup from another today.

Tier 2 closes this by adding a `load_group` axis to the registry.

---

## `load_group` definition (composite with separator-collision guard)

```
load_group = f"{flowgroup.pipeline}::{flowgroup.flowgroup}"
```

Separator chosen as `::` (visually distinctive; not used in idiomatic LHP names). Both
`pipeline: str` and `flowgroup: str` are required `FlowGroup` fields. Composite key
prevents collision when the same `flowgroup` name is reused across pipelines (LHP allows
this). Same flowgroup running in dev/qa/prod is isolated by env-scoped catalog placement
(ADR-004); no env axis in `load_group`.

---

## Codegen-time uniqueness guard

`LoadActionValidator` runs two checks **scoped to flowgroups declaring
`execution_mode: for_each`** (non-B2 flowgroups using `::` for any unrelated reason are
not blocked — backward-compat preserved):

1. **LHP-CFG-019** — for each `for_each` flowgroup, reject any `flowgroup.pipeline` or
   `flowgroup.flowgroup` value containing the separator literal `::`.
   Error message: "`::` reserved for load_group composite; rename pipeline/flowgroup."

2. **LHP-CFG-020** — build the composite for every `(pipeline, flowgroup)` tuple of
   `for_each` flowgroups in the project; raise `LHPConfigError` if any composite is
   non-unique. Catches distinct tuples that happen to normalise identically. This check is
   project-scoped only; multi-project workspaces sharing one watermark registry can still
   collide at runtime — captured in Known Issues for implementation-phase mitigation
   (e.g., `WatermarkManager` init-time near-collision warning against existing registry
   rows).

**Pre-flight scan:** `rg -l '::' <project>/flowgroups/*.yaml` — run before the Tier 2
deploy to confirm zero existing flowgroup files contain the new separator literal.

Without these guards an `__` separator could collide
(`pipeline='customers' + flowgroup='daily'` ↔ `pipeline='' + flowgroup='customers__daily'`),
or an embedded separator inside a flowgroup name (e.g. `reporting::hourly`) could shadow a
real `(pipeline, flowgroup)` pair.

---

## Rejected alternative — encode in `source_system_id`

Encoding the flowgroup identity directly into `source_system_id`
(e.g. `pg_supabase::customers_daily`) saves a DDL migration but breaks Tier 1's
source-system isolation semantics, conflates two axes, and makes the registry unqueryable
by source alone. Rejected on design grounds; named here for future-reader context.

---

## Fix

Add `load_group STRING` column to `<env>_orchestration.watermarks`. Extend
`WatermarkManager.insert_new` / `mark_*` / `get_latest_watermark` /
`get_recoverable_landed_run` to accept `load_group: Optional[str] = None` (back-compat)
and write/filter by it. Extend template + generator to thread the composite into manager
calls.

---

## Files to modify

- `src/lhp_watermark/watermark_manager.py` — method signatures + SQL filter in every
  affected method.
- `src/lhp_watermark/__init__.py` — no change to exported names.
- Auto-DDL in `_ensure_table_exists` — add the `load_group STRING` column (nullable)
  **and update Delta liquid clustering** to
  `(source_system_id, load_group, schema_name, table_name)` for query performance under
  HWM-lookup load.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` — thread composite `load_group` into
  manager calls.
- `src/lhp/generators/load/jdbc_watermark_job.py` — populate `load_group` in template
  context using composite construction.
- `tests/lhp_watermark/` — parameterise by `load_group` in affected test files.

---

## Migration steps

### Step 1 — Add column nullable

The auto-DDL path (`_ensure_table_exists`) adds `load_group STRING` on next write.
Existing rows have `load_group IS NULL`.

### Step 2 — Deploy WatermarkManager with optional `load_group`

Deploy `WatermarkManager` accepting `load_group: Optional[str] = None` everywhere.
`get_latest_watermark` uses the three-way filter for safe coexistence during migration:

```sql
SELECT MAX(watermark_value) AS hwm
FROM   <wm_catalog>.<wm_schema>.watermarks
WHERE  source_system_id = :source_system_id
  AND  schema_name      = :schema_name
  AND  table_name       = :table_name
  AND  status           = 'completed'
  AND  (
         load_group = :load_group
         OR (:load_group IS NULL AND load_group IS NULL)
       )
```

This preserves pre-Tier-2 behaviour when callers pass `load_group=None` and strictly
filters when callers pass a value — so a B2 flowgroup's HWM lookups are isolated from a
legacy flowgroup's registry rows.

### Step 3 — Backfill `'legacy'`

Run once per environment (dev/qa/prod). Idempotent:

```sql
UPDATE metadata.<env>_orchestration.watermarks
SET    load_group = 'legacy'
WHERE  load_group IS NULL;
```

Manifest table doesn't exist pre-B2, so there is no per-flowgroup remap to attempt.

### Step 3a — OPTIMIZE after backfill

Liquid clustering attached at column-add does not reshuffle existing files until OPTIMIZE
runs. The backfill from step 3 also produces a wide-write spread across many small files.
Run once after step 3 completes and before per-flowgroup seeding (step 4a) begins, so HWM
lookups against the seeded namespace see clustered storage from the first B2 run forward:

```sql
OPTIMIZE metadata.<env>_orchestration.watermarks;
```

A second OPTIMIZE after fleet-wide seeding (or as part of the recurring weekly OPTIMIZE
policy) bounds long-term file count.

### Step 4 — Per-flowgroup adoption

Each flowgroup's first B2 run writes `load_group = '<pipeline>::<flowgroup>'`. The
`'legacy'` row remains the HWM ceiling for that `(source, schema, table, 'legacy')` tuple
— orphaned but harmless; a future cleanup pass can drop legacy rows once their flowgroups
have successfully migrated.

### Step 4a — Legacy seed step

Before a flowgroup's first B2 run, copy the legacy HWM ceiling into the flowgroup's new
`load_group` namespace so its first B2 run does an incremental load, not a wasteful full
scan against the source.

Column list pinned to the verified DDL in
`src/lhp_watermark/watermark_manager.py:_ensure_table_exists` (17 base columns +
`load_group` from Tier 2; `error_class`/`error_message` are nullable and intentionally
omitted from the SELECT clause to default NULL).

For each `(source_system_id, schema, table)` the flowgroup will load:

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
    'seeded',                               -- new extraction_type value; safe (column is STRING NOT NULL
                                            -- with no enum constraint; consumers checked, none enumerate
                                            -- explicitly — see WatermarkManager docstring update)
    bronze_stage_complete, silver_stage_complete, status,
    current_timestamp(), completed_at,
    :load_group_target
FROM metadata.<env>_orchestration.watermarks
WHERE load_group = 'legacy'
  AND source_system_id = :src
  AND schema_name = :schema
  AND table_name = :table
  AND status = 'completed'
ORDER BY watermark_time DESC, completed_at DESC, run_id DESC  -- timestamp tiebreakers; lex run_id last
LIMIT 1;
```

- **Preview before execute.** Before running the INSERT, run the same WHERE/ORDER BY as a
  SELECT to confirm what the seed would copy:
  ```sql
  SELECT count(*), max(watermark_time), max(watermark_value)
  FROM   metadata.<env>_orchestration.watermarks
  WHERE  load_group = 'legacy'
    AND  source_system_id = :src
    AND  schema_name = :schema
    AND  table_name = :table
    AND  status = 'completed';
  ```
  This is a mandatory dry-run gate before fleet-scale seeding.

- **Idempotency.** Re-running the seed inserts an additional `seed-<uuid>` row with the
  same `watermark_value`. `get_latest_watermark` still returns the highest-watermark row,
  so observable behaviour is unchanged; operators cleaning up may dedupe by
  `(load_group, source_system_id, schema, table, watermark_value)` if desired.

- **No-legacy-row case.** If `LIMIT 1` matches nothing (legacy never successfully loaded
  the table), the INSERT inserts zero rows. The flowgroup's first B2 run then cold-starts
  with a full scan — correct behaviour, since there is no prior HWM to inherit. Operators
  should not interpret a zero-insert as a seed failure.

- **Operator ordering footgun guard.** Step 4a requires step 3 (legacy backfill) to have
  completed for the target env. Running 4a before 3 silently inserts zero rows because
  `load_group = 'legacy'` matches nothing. The operator runbook must call out this
  dependency as a one-line precondition.

Without this step a 50-flowgroup migration triggers 50 source-side full scans on first
deploy — material against PgBouncer-pooled or rate-limited sources.

### Step 4b — Run-frequency + CLI helper

Step 4a runs once per `(flowgroup, source_table)` as part of B2 onboarding.

**Required tooling to ship alongside Tier 2:** a CLI helper or `tools/` script that
consumes a flowgroup's YAML + env substitutions and emits the per-table SELECT preview +
INSERT statements. Without the helper, fleet migration is per-table hand-rolled SQL — a
real friction point. The implementation phase delivers this script as part of the Tier 2
commit.

### Step 5 — Delta column-mapping warning (one-way)

Flipping `load_group` to `NOT NULL WITH DEFAULT` requires Delta protocol bump
(`delta.columnMapping.mode = 'name'`). **Any** introduction of column mapping to the
watermarks table is a one-way protocol upgrade affecting every reader (Spark 3.0+ required;
certain external clients break). Treat as a one-way operational decision.

Test on a Delta deep-clone first. Defer the flip until ≥ 30 days of zero-NULL writes from
the active fleet.

---

## Verification (run V1–V4 against a deep-clone)

Run all probes against a deep-clone, never against the production registry:

```sql
CREATE TABLE devtest_edp_orchestration.watermarks_v1_probe
    DEEP CLONE devtest_edp_orchestration.watermarks;
-- Run all V-checks against watermarks_v1_probe.
-- Drop when all pass.
DROP TABLE devtest_edp_orchestration.watermarks_v1_probe;
```

### V1 — Isolation smoke

Insert two probe rows with same `(source_system_id, schema, table)` but different
`load_group`. Assert:

- `get_latest_watermark(load_group='lg_a')` returns `lg_a`'s HWM.
- `get_latest_watermark(load_group='lg_b')` returns `lg_b`'s HWM.
- Neither leaks to the other.

Cleanup uses isolated marker `run_id`s, not table-scoped DELETE.

### V2 — Regression

Tier 1 cross-source filter preserved. Existing `tests/lhp_watermark/test_get_latest.py`
passes unchanged.

### V3 — Legacy fallback

`get_latest_watermark(load_group=None)` returns the `'legacy'` row. Migration safety valve
for un-migrated flowgroups.

### V4 — Legacy-seed-then-incremental smoke

Seed a probe flowgroup's `load_group` from a known legacy row (step 4a SQL applied to the
deep-clone). Run a worker iteration in dry-run / probe mode that calls
`get_latest_watermark(load_group='<seeded>')` and assert:

- (a) Returned HWM equals the seeded row's `watermark_value`.
- (b) The worker's resulting `extraction_type` would be `'incremental'` (not `'full'`).
- (c) The JDBC WHERE clause includes the seeded HWM.

Cleanup uses isolated marker `run_id`s. Closes the seed-step smoke gap.

---

## Out of scope

- Tier 3 per-integration registry tables.
- ADR-004 registry placement changes.
- Big-bang fleet migration.
- Flipping `load_group` to `NOT NULL` (deferred ≥ 30 days, gated on Delta protocol review).
- HIPAA hashing implementation.
- B2 manifest table retention policy.

---

## Effort

1–2 days for the `WatermarkManager` + DDL changes + flat backfill + V1/V2/V3 verification.
Per-flowgroup adoption (step 4a seed) adds roughly 0.5 day per flowgroup × fleet size for
operator coordination + per-flowgroup smoke. With the CLI helper (step 4b), per-flowgroup
work drops to ~5 min each.

Prerequisite for B2 integration commit: the `WatermarkManager` work (steps 1–3), not the
fleet rollout.

---

## How to execute

Treat as a GSD phase when scheduled:

```
/gsd-plan-phase "Tier 2 HWM load_group fix: add column, extend WatermarkManager with load_group kwarg, backfill legacy, OPTIMIZE, V1–V4 against deep-clone"
```

Or inline once reviewed:

```
/gsd-quick "Apply Tier 2 from docs/planning/tier-2-hwm-load-group-fix.md; all six file changes + CLI helper; backfill dev first; V1–V4 on dev deep-clone; then qa/prod"
```

---

## Pre-flight checks before merging Tier 2

- Run `rg "LHP-CFG-(019|020)" src/` — must return zero matches, otherwise reassign error
  codes.
- Run `DESCRIBE DETAIL metadata.<env>_orchestration.watermarks` per env — capture current
  `partitionColumns`, `clusteringColumns`, `numFiles`, Delta protocol versions.
- Audit `extraction_type` consumers: `rg "extraction_type" --type py` to confirm no
  consumer enumerates explicit values; if any do, extend their enum to include `'seeded'`
  before Tier 2 ships.
- Run `rg -l '::' <project>/flowgroups/*.yaml` to confirm no existing flowgroup files
  contain the new separator literal.
