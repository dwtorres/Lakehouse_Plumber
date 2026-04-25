# Spike B2 — Watermark Scale-Out Design

**Status:** Design baseline (not yet implemented)
**Prerequisite:** Tier 2 HWM load_group fix (see [tier-2-hwm-load-group-fix.md](tier-2-hwm-load-group-fix.md))
**Approved:** 2026-04-25
**Source plan:** `~/.claude/plans/cozy-scribbling-spring.md` (Revision 4)

---

## Design intent

B2 is LHP's emission pattern for `jdbc_watermark_v2` flowgroups declaring `execution_mode: for_each`. Codegen replaces the current N-static-tasks emission with a three-task DAB topology:

```
prepare_manifest  →  for_each_ingest  →  validate
```

The per-iteration worker is the existing `jdbc_watermark_job.py.j2` template plus a header that unpacks `taskValues`. Existing public `lhp_watermark` API is reused verbatim.

Flowgroups without `execution_mode: for_each` keep the existing per-action static emission unchanged. YAML authors see no change beyond opting into the new field.

---

## Why dynamic manifest, not static codegen-time manifest

A static manifest baked into YAML at codegen time cannot capture runtime context: `derive_run_id` reads the DAB `job_run_id`, which is only available at job execution. Every iteration needs a run_id that parents to the batch's job run for the registry's terminal-state guard to hold.

Additionally, LHP distinguishes deploy-time substitutions (baked into YAML) from run-time substitutions (secrets, env-scoped catalog prefixes). A static manifest forces all substitutions into deploy-time, breaking the secret-scope integration already in the template. Template expansion that depends on env cannot be materialised at codegen time.

A dynamic `prepare_manifest` notebook decouples LHP codegen from per-batch state and matches the existing template's runtime-resolution model. Cost: one extra ~10-second task per job. Against B2's ~30-minute wall-clock at N=61, this is negligible. The dynamic manifest is the correct boundary.

---

## Why not collapse prepare+validate into a single ThreadPoolExecutor wrapper

Collapsing these into a single wrapper loses DAB's per-iteration UI visibility — the exact win B2 claims over A1. The three-task topology is intentional.

---

## Five binding requirements

### R1. Per-table manifest row, no HWM snapshot

`prepare_manifest` writes one row per concrete action (post-template-expansion) into the **batch manifest table at `metadata.<env>_orchestration.b2_manifests`** (placed alongside the watermark registry per ADR-004). Manifest schema:

```
batch_id            STRING   -- = derive_run_id(dbutils) at prepare_manifest entry
action_name         STRING
source_system_id    STRING
schema_name         STRING
table_name          STRING
load_group          STRING
worker_run_id       STRING NULL  -- written by worker at iteration start
execution_status    STRING       -- 'pending'|'running'|'completed'|'failed'|'abandoned'
created_at          TIMESTAMP
updated_at          TIMESTAMP
PRIMARY KEY (batch_id, action_name)
```

Manifest carries iteration keys only. **HWM is read live by the worker** via `WatermarkManager.get_latest_watermark(source_system_id, schema_name, table_name, load_group)` — the production contract. No HWM snapshot in manifest; the earlier draft's snapshot was decorative and contradicted R3.

### R1a. prepare_manifest idempotency

`MERGE INTO b2_manifests ON (batch_id, action_name) WHEN MATCHED THEN UPDATE updated_at WHEN NOT MATCHED THEN INSERT`.

`batch_id = derive_run_id(dbutils)` evaluated once at task entry — deterministic within a single attempt. A DAB task retry generates a new `attempt` token per `runtime.py:97` (verified empirically: `derive_run_id` returns `job-{jobRunId}-task-{taskRunId}-attempt-{N}`) and therefore a new `batch_id`, which is the desired behaviour. Orphaned manifest rows from a failed attempt are abandoned; a retention policy is deferred (acknowledged slow leak, not correctness-blocking).

### R2. Small task-value key

`prepare_manifest` emits one taskValue with **key = `iterations`** (named explicitly so `for_each` references are `{{ tasks.prepare_manifest.values.iterations }}` with no ambiguity). Value = JSON array of objects:

```json
{
  "source_system_id": "pg_supabase",
  "schema_name":      "humanresources",
  "table_name":       "employee",
  "action_name":      "bronze_humanresources_employee",
  "load_group":       "customers_daily",
  "batch_id":         "job-12345-task-67890-attempt-0",
  "manifest_table":   "metadata.devtest_orchestration.b2_manifests"
}
```

~250 bytes per entry. DAB task-value cap ~48 KB → maximum **300 entries per `for_each` batch**. Codegen validates `len(actions_post_template_expansion) ≤ 300` in `LoadActionValidator` (which runs **after** `FlowGroupTemplateExpander`, so a template fanning out to 50 actions is counted at expanded size). If exceeded, `LHPConfigError` at bundle generation time, not at runtime.

Worker resolves remaining per-table config (jdbc_url, secrets, landing_path) from substitutions + manifest table at iteration start.

### R3. Worker uses production contract verbatim

Per-iteration notebook is `jdbc_watermark_job.py.j2` with a single conditional header block: when `taskValues` are present, unpack iteration key into the same template variables that codegen would otherwise have rendered statically; else fall through to existing static-render behaviour (preserves the legacy emission path). Below the header, the L2 §5.3 body is **unchanged**: `derive_run_id` → `insert_new` outside try → JDBC + landing inside try → `mark_landed` → `mark_complete` outside try.

**Worker writes its run_id back to the manifest** (after `derive_run_id`, before `insert_new`):

```sql
UPDATE b2_manifests
SET worker_run_id    = :worker_run_id,
    execution_status = 'running',
    updated_at       = current_timestamp()
WHERE batch_id   = :batch_id
  AND action_name = :action_name
```

This **closes the validate-race concern** by giving R5 a deterministic join from manifest → watermarks rather than a heuristic time-window match. Two concurrent batches of the same flowgroup are naturally separable: each batch has its own `batch_id`, its own manifest rows, its own `worker_run_id` set, and validate joins exclusively on those.

**Cold-start case (explicit):** when `get_latest_watermark` returns `None`, worker performs a full scan; `extraction_type = 'full'` is stamped in the watermark row.

**HIPAA hook (scope clarified):** the hook insertion point is between the JDBC read and the landing write, inside the try block, before `mark_landed`. This design doc **describes** the hook location; **no template code change ships in this plan-doc phase**. The hook (no-op default) is added in a separate phase coupled with the HIPAA implementation.

### R4. Failed iterations raise

Worker's except block calls `mark_failed(type(e).__name__, str(e)[:4096])` then re-raises — exactly matching `jdbc_watermark_job.py.j2:255-262` today. `for_each_task.task.max_retries` is the authoritative retry knob.

DAB-retried iterations get a fresh `derive_run_id` per attempt (verified). Failed-then-succeeded retry leaves both a `failed` row and a `completed` row in the watermark registry — the worker also re-issues the manifest UPDATE to flip `execution_status` from `'failed'` to `'running'` to `'completed'`/`'failed'` on the new attempt. Validate joins via `worker_run_id` so it always reads the latest attempt's state, never the earlier attempt's failed row.

### R5. Strict validate gate (race-free, retry-aware)

```sql
WITH manifest AS (
  SELECT batch_id, action_name, source_system_id, schema_name, table_name,
         load_group, worker_run_id, execution_status AS manifest_status
  FROM   metadata.<env>_orchestration.b2_manifests
  WHERE  batch_id = :batch_id
),
worker_states AS (
  SELECT m.action_name, w.status AS worker_status
  FROM   manifest m
  LEFT JOIN metadata.<env>_orchestration.watermarks w
    ON  w.run_id = m.worker_run_id           -- deterministic join, not time-window
),
final AS (
  SELECT m.action_name,
         coalesce(w.worker_status, m.manifest_status) AS final_status
  FROM   manifest m
  LEFT JOIN worker_states w USING (action_name)
)
SELECT
  count(*)                                                         AS expected,
  count_if(final_status = 'completed')                             AS completed_n,
  count_if(final_status = 'failed')                                AS failed_n,
  count_if(final_status IN ('pending', 'running', 'landed_not_committed', 'abandoned')) AS unfinished_n
FROM final;
```

**Status-source clarification.** `final_status = coalesce(worker_status, manifest_status)` blends two enums:

- **`worker_status`** comes from `watermarks.status` and uses the registry's L2 §5.3 enum: `running`, `landed_not_committed`, `completed`, `failed`, `timed_out`.
- **`manifest_status`** comes from `b2_manifests.execution_status` and uses the manifest enum, which **mirrors the registry enum verbatim** (R1 declares it as `pending|running|completed|failed|abandoned`; `pending` and `abandoned` are manifest-only states, the rest match registry literals exactly).

The `IN`-list `('pending', 'running', 'landed_not_committed', 'abandoned')` covers both sources' "still in progress" terms. `pending` matches a manifest row whose worker never started (no registry row exists). `running`/`landed_not_committed` match registry-side mid-flight. `abandoned` matches manifest-side cancelled iterations.

**Pass conditions:** `completed_n == expected` AND `failed_n == 0` AND `unfinished_n == 0`. Concurrent batches of the same flowgroup are isolated by `batch_id` and never poison each other.

**Empty-batch case.** A flowgroup that expands to zero actions produces `expected = 0`, all four counts zero, and validate trivially passes as a no-op. Codegen should additionally fail at bundle-gen time if a `for_each` flowgroup expands to zero actions (`LoadActionValidator` adds this check) — a no-op B2 job is almost certainly authoring error, not intent.

**In-scope optional check (config-flagged):** per-table parity `JDBC-rows-read == landed-parquet-rows` using landed parquet metadata. **Out-of-scope:** bronze parity vs landed parquet (bronze is downstream DLT pipeline, separate job, different concern).

---

## Integration surface against existing LHP codegen

Verified against `src/lhp/` as of `spike/jdbc-sdp-a1` HEAD.

- `src/lhp/models/config.py` — extend `FlowGroup.workflow` dict to accept `execution_mode: "for_each"` and `concurrency: int`.
- `src/lhp/core/validators/load_validator.py` — when `execution_mode == "for_each"`: enforce shared `source_system_id`, shared `landing_path` root, shared `wm_catalog.wm_schema`, **and `len(post-expansion actions) ≤ 300`**. Concurrency knob bounds: `1 ≤ concurrency ≤ 100`; default `min(action_count, 10)`. The 300-cap evaluation runs **after** `FlowGroupTemplateExpander` so template fan-out is counted at expanded size. Also enforce non-zero action count and `LHP-CFG-019`/`LHP-CFG-020` load_group separator guards (see [tier-2-hwm-load-group-fix.md](tier-2-hwm-load-group-fix.md)).
- `src/lhp/generators/bundle/workflow_resource.py` — branch on `flowgroup.workflow.execution_mode`. When `for_each`, emit the three-task topology.
- `src/lhp/templates/bundle/workflow_resource.yml.j2` — `{% if execution_mode == "for_each" %}` branch rendering DAB `for_each_task` YAML, including taskValue ref `{{ tasks.prepare_manifest.values.iterations }}`.
- New `src/lhp/templates/bundle/tasks/prepare_manifest.py.j2` and `validate.py.j2` — manifest writer + validate gate notebooks.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` — single conditional header to unpack `taskValues` + manifest UPDATE for `worker_run_id`. Body unchanged (`jdbc_watermark_job.py.j2:255-262` raise-on-failure semantics preserved verbatim).
- `src/lhp/generators/load/jdbc_watermark_job.py` — populate `load_group` in template context using composite construction; thread `execution_mode` so header block emits when `for_each`.
- `src/lhp/core/action_registry.py` — no change.

---

## Public API reuse

`lhp_watermark` names consumed by B2 (no new exports, ≤10):

`WatermarkManager`, `derive_run_id`, `SQLInputValidator`, `DuplicateRunError`, `TerminalStateGuardError`, `WatermarkValidationError`, `WatermarkConcurrencyError`, `sql_literal`, `sql_timestamp_literal`, `sql_identifier`.

Tier 2 extends existing `WatermarkManager` method signatures with `load_group: Optional[str] = None` (back-compat). No new exported names.

---

## Architectural invariants

B2 preserves the following binding invariants. Each is named here so implementation review can verify the design does not erode any of them.

- **ADR-002 namespace** — `lhp_watermark` deployed as top-level package via DAB workspace-file sync. B2 notebooks import from the package; they do not re-implement state-machine logic.
- **ADR-003 landing shape** — `<landing_root>/_lhp_runs/<run_id>/` — B2 reuses the existing template's `df.write.mode("overwrite").format("parquet")` call to that path. Spike B's direct-to-bronze `saveAsTable` is not the pattern B2 uses.
- **ADR-004 registry placement** — `metadata.<env>_orchestration.watermarks` and the co-located `b2_manifests`. No new registries.
- **L2 §5.3 control flow** — `insert_new` outside try; JDBC + landing inside try with `mark_failed`-then-`raise`; `mark_complete` outside/after. Worker body unchanged.
- **Terminal-state guard** — `TerminalStateGuardError` propagates; never swallowed. `prepare_manifest` idempotency (R1a) uses a fresh `batch_id` per attempt so this invariant is not challenged.
- **SQL safety via `SQLInputValidator` pre-composition** — applied before any manifest or registry SQL composition. No string-concat SQL.
- **UTC session on every DML** — manifest writes follow the same convention as the watermark registry.

---

## Deferred to follow-up

- HIPAA hashing implementation (hook location described here; no-op default + column-masking code ship in a separate phase coupled to the HIPAA implementation).
- Bronze parity gate (downstream task; owned by the DLT pipeline that consumes the landing zone).
- Manifest table retention policy (time-based or batch-count-based pruning; defer until operational data exists).
- Per-iteration runtime distribution observability (histograms, p95 duration alerts).
- B2 for non-JDBC load types (kafka, cloudfiles, delta). Start with `jdbc_watermark_v2`; generalise once the pattern is proven in production.

---

## Known issues for implementation-phase review

These items are real but not blockers for committing the design docs. They are in scope for the implementation phase rather than being rediscovered then.

- **Manifest-table write hotspot.** Worker UPDATEs `b2_manifests` at iteration start + state transitions. At concurrency=10 × N=300 actions, Delta optimistic-concurrency conflicts and file-rewrite cost may matter. Mitigations to evaluate: partition by `batch_id`, use Delta deletion vectors, or switch to append-only event-log + `ROW_NUMBER() OVER (PARTITION BY batch_id, action_name ORDER BY ts DESC)` in validate. Decide during implementation based on measured contention.
- **`max_retries` default.** B2 design defers to operator. Recommend setting default `max_retries: 1` in the codegen template with operator override documented in the flowgroup YAML schema. Default 0 makes retry-tolerant R4/R5 dead weight; default 3+ wastes budget on systematic source failures.
- **`b2_manifests` table-create owner.** Auto-DDL inside `prepare_manifest` notebook on first run is the simplest path; pin this in the design doc to avoid "works in dev, missing in prod" surprise. Alternative: Tier 2 migration creates the table alongside the watermarks column add.
- **>300 action guidance.** Codegen rejects with `LHPConfigError`; author needs a hint. Recommended sentence: "Split by source schema prefix or table subset into multiple flowgroups; batch-scoped manifest isolates them from each other."
- **Liquid clustering conversion cost on existing watermarks.** If the production registry is currently partitioned (not liquid-clustered), Tier 2's clustering switch is a full table rewrite + protocol bump. Verify current state in the implementation phase; if already liquid-clustered, ALTER is light. If partitioned, plan a maintenance window.
- **Liquid clustering choice (HWM lookup vs run_id hot-path tradeoff).** Tier 2 clusters on `(source_system_id, load_group, schema_name, table_name)` for HWM lookups. State-machine writes (`mark_landed` / `mark_complete` / `mark_failed`) filter by `run_id`. Clustering does not optimise that path; the existing primary-key uniqueness is what guards it. Document the tradeoff in the Tier 2 doc.
- **Ghost manifest rows from cancelled batches.** Slow leak. Schedule a maintenance task `DELETE FROM b2_manifests WHERE created_at < current_date() - INTERVAL 30 DAYS` as part of the initial B2 deploy, not a "future follow-up."
- **Concurrent-worker race on manifest UPDATE during DAB retry timeout.** Low-probability last-write-wins under aggressive timeout. Add optimistic-concurrency check to the worker's manifest UPDATE: `WHERE worker_run_id IS NULL OR worker_run_id = :previous_worker_run_id`.
- **VACUUM + OPTIMIZE policy on the watermarks registry.** Define as part of Tier 2 rollout (suggest weekly OPTIMIZE, daily VACUUM RETAIN 168 HOURS after fleet adoption). Not deferred to a follow-up.
- **HWM read scaling.** Live read (R1) is correct; clustering helps; long-term VACUUM/OPTIMIZE policy bounds file-count growth. Re-measure once registry exceeds 100K rows.
- **Append-only manifest as v2.** If the write-hotspot mitigations above prove insufficient, the design migrates manifest writes to append-only with latest-per-key in validate. Tradeoff: validate query complexity. Worth noting as a known v2 path.
- **`LHP-CFG-020` multi-project scope.** Codegen-time uniqueness check is project-scoped. Multi-project workspaces sharing one watermark registry can collide at runtime. Mitigation: add a runtime warning in `WatermarkManager.__init__` (or first `insert_new`) that scans the registry for `(load_group)` near-collisions against the caller's value and logs WARN. Decide during implementation; not a code blocker.
- **Error-code registry collision check.** Verify `LHP-CFG-019` and `LHP-CFG-020` are unused by any existing LHP error catalog before claiming them. Update the error-code reference (`docs/errors_reference.rst` or equivalent) as part of the implementation commit.
- **Auto-DDL clustering check.** `_ensure_table_exists` runs on every `WatermarkManager` init. The clustering switch must be a *conditional* ALTER (skip if current clustering already matches the target spec). Naive unconditional ALTER causes metadata churn on every init. Pin in implementation.
- **Pre-flight `DESCRIBE DETAIL`.** Before Tier 2 ships, run `DESCRIBE DETAIL metadata.<env>_orchestration.watermarks` to capture current `partitionColumns`, `clusteringColumns`, `numFiles`, and Delta protocol versions. If currently partitioned (not clustered), the switch is a heavy rewrite + protocol bump and needs a maintenance window. If already clustered on a different key, ALTER is light.
- **`::` in observability surfaces.** `load_group` flows into log lines, dashboards, and error messages. `::` is fine for SQL but trips some log parsers that interpret `:` as a key/value separator. Accept as cosmetic; flag once in the operator runbook.

---

## Cross-references

- [`../ideas/spike-a1-vs-b-comparison.md`](../ideas/spike-a1-vs-b-comparison.md) — strategic context; B2-adopt vs A1-defer framing; four tightening corrections.
- [`tier-2-hwm-load-group-fix.md`](tier-2-hwm-load-group-fix.md) — Tier 2 prerequisite; `load_group` DDL migration, seed step, verification plan.
- [`tier-1-hwm-fix.md`](tier-1-hwm-fix.md) — Tier 1 (shipped 2026-04-24, commit `63bcfdb`); prerequisite closed.
- `docs/adr/ADR-002-lhp-runtime-availability.md` — `lhp_watermark` package deployment model.
- `docs/adr/ADR-003-landing-zone-shape.md` — landing path `<landing_root>/_lhp_runs/<run_id>/`.
- `docs/adr/ADR-004-watermark-registry-placement.md` — registry at `metadata.<env>_orchestration`.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` — reused per-iteration worker (single header addition; L2 §5.3 body unchanged).
- `src/lhp_watermark/watermark_manager.py` — public API consumed verbatim.
- `src/lhp_watermark/runtime.py:97` — `derive_run_id` return format verified empirically.
