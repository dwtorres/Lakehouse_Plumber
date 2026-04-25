# Design Spec: Spike B2 — Hardened `for_each` Watermark Scale-out

**Status:** Design baseline; implementation pending
**Priority:** Adopted production path for `jdbc_watermark_v2` scale-out emission
**Prerequisite:** Tier 2 `load_group` migration (see `docs/planning/tier-2-hwm-load-group-fix.md`)
**Created:** 2026-04-24
**Branch:** `spike/jdbc-sdp-a1` (cherry-pick to `watermark` when implementation begins)

---

## 1. Design intent

B2 is LHP's codegen emission pattern for `jdbc_watermark_v2` flowgroups that declare `execution_mode: for_each` in their `workflow` stanza. It replaces the current N-static-notebook-tasks emission with a three-task Databricks Jobs topology:

```
prepare_manifest  →  for_each_ingest  →  validate
```

The per-iteration worker is the existing `src/lhp/templates/load/jdbc_watermark_job.py.j2` template with one conditional header block added: when `dbutils.jobs.taskValues` carries an iteration key, the worker unpacks it into the same template variables that static codegen would have rendered. Below the header, the L2 §5.3 state-machine body is unchanged.

The public `lhp_watermark` API is reused verbatim — no new exports, no signature changes beyond what Tier 2 already adds.

B2 ships as a codegen branch, not a framework replacement. Flowgroups without `execution_mode: for_each` keep the existing per-action static emission. YAML authors see no change beyond opting into the new field.

---

## 2. Why dynamic manifest, not static codegen-time manifest

An earlier draft considered baking the manifest into the generated YAML at codegen time (static manifest). Rejected because:

- **Runtime context required.** `derive_run_id` reads the DAB `job_run_id`, which is only available at job execution. Every iteration needs a run_id that parents to the batch's job run for the registry's terminal-state guard to hold.
- **Substitution boundary.** LHP distinguishes deploy-time substitutions (baked into YAML) from run-time substitutions (secrets, env-scoped catalog prefixes). A static manifest forces all substitutions into deploy-time, breaking the secret-scope integration already in the jdbc_watermark_v2 template.
- **Template expansion depends on env.** Flowgroup templates that enumerate tables via runtime discovery (future enhancement) cannot be materialised at codegen time.

The cost of a runtime `prepare_manifest` task is one extra ~10-second task per job. Against B2's ~30+ minute wall-clock at N=61, this is negligible. The dynamic manifest is the correct boundary.

---

## 3. Five binding requirements

### R1. Per-table manifest row; HWM read live, not snapshotted

`prepare_manifest` writes one row per concrete action (post-template-expansion) into a manifest Delta table. Manifest rows carry **iteration keys only** — no HWM snapshot, no JDBC URL, no secret refs.

HWM is read live by the worker at iteration start via `WatermarkManager.get_latest_watermark(source_system_id, schema_name, table_name, load_group)`. This matches the production contract in the existing jdbc_watermark_v2 template. An earlier draft proposed snapshotting the HWM into the manifest row; that was dropped because it contradicted R3 (worker uses production contract verbatim) and because per-iteration atomicity does not require it — each iteration operates on an independent `(source_system_id, schema, table, load_group)` registry slice after Tier 2.

**Cold-start behaviour.** When `get_latest_watermark` returns `None`, the worker performs a full scan; the manifest row stamps `extraction_type = 'full'` for downstream observability.

**Manifest schema (bound):**

| Column | Type | Notes |
|--------|------|-------|
| `batch_id` | STRING NOT NULL | `derive_run_id(dbutils)` evaluated at `prepare_manifest` entry |
| `action_name` | STRING NOT NULL | LHP action name; unique within a flowgroup |
| `source_system_id` | STRING NOT NULL | Matches registry filter key |
| `schema_name` | STRING NOT NULL | |
| `table_name` | STRING NOT NULL | |
| `load_group` | STRING NOT NULL | Flowgroup name; Tier 2 prerequisite |
| `extraction_type` | STRING NOT NULL | `'full'` \| `'incremental'` |
| `created_at` | TIMESTAMP NOT NULL | UTC |

Primary key: `(batch_id, action_name)`.

### R1a. `prepare_manifest` idempotency

Manifest write uses `MERGE INTO manifest USING (...) ON (batch_id, action_name) WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. `batch_id = derive_run_id(dbutils)` is deterministic within a single task attempt; a DAB task retry of `prepare_manifest` produces a new `batch_id`, which is the desired behaviour — a re-attempt after a partial failure must produce a clean batch, not accumulate stale rows alongside fresh ones.

The for_each iterations run under the batch_id emitted by the attempt that succeeded; task-value passing (R2) couples iteration identity to that batch_id.

### R2. Small task-value key, with explicit ceiling

The `for_each_task` inputs taskValue is a JSON array of objects:

```json
{
  "source_system_id": "pg_supabase",
  "schema_name": "humanresources",
  "table_name": "employee",
  "action_name": "bronze_humanresources_employee",
  "load_group": "customers_daily",
  "batch_id": "job_run_01H2..."
}
```

~200 bytes per entry. DAB taskValue cap is ~48 KB per task. B2 codegen **validates `len(actions) ≤ 300`** in `LoadActionValidator`; exceeding the cap raises `LHPConfigError` at bundle-generation time, not at runtime.

The worker uses `(batch_id, action_name)` to read its manifest row, then re-resolves environment substitutions at iteration start (secrets, catalog prefixes).

Validate task also consumes the `batch_id` taskValue emitted by `prepare_manifest`; the topology is:

```
prepare_manifest  --taskValues(batch_id, iterations)-->  for_each_ingest  --(implicit batch_id)-->  validate
```

`validate` reads the `batch_id` taskValue directly from `prepare_manifest`'s output to scope its queries (see R5).

### R3. Worker uses production contract verbatim

Per-iteration notebook is `src/lhp/templates/load/jdbc_watermark_job.py.j2` with a single conditional header block added:

```python
if "iteration_key" in dbutils.jobs.taskValues.get(...):  # pseudo-code
    key = dbutils.jobs.taskValues.get(taskKey="prepare_manifest", key="iteration_key")
    source_system_id = key["source_system_id"]
    schema_name      = key["schema_name"]
    table_name       = key["table_name"]
    action_name      = key["action_name"]
    load_group       = key["load_group"]
    batch_id         = key["batch_id"]
# else: static-render path (legacy emission, unchanged)
```

Below the header, the L2 §5.3 body is **unchanged**: `derive_run_id` → `insert_new` outside try → JDBC read + landing write inside try → `mark_landed` → `mark_complete` outside try (existing code at `jdbc_watermark_job.py.j2:255-262` for the raise-on-failure semantics).

**HIPAA hashing hook (scope clarified).** The insertion point is between the JDBC read and the landing write, inside the try block, before `mark_landed`. This design doc **describes** the hook location; it does **not** ship a code change to the template. The hook (no-op default) lands in a separate phase coupled to the HIPAA hashing implementation.

### R4. Failed iterations raise; retry-aware validate

Worker's except block: `mark_failed(run_id, type(e).__name__, str(e)[:4096])` then `raise`. Matches `jdbc_watermark_job.py.j2:255-262` today. `for_each_task.task.max_retries` is the authoritative retry knob.

**Retry semantics and their interaction with R5.** DAB-retried iterations get a fresh `derive_run_id` per attempt. A failed-then-succeeded retry leaves **both** a failed row and a completed row in the watermark registry for the same `(source_system_id, schema_name, table_name, load_group)` key. Naïve row-counting in `validate` would fail the batch even though DAB recovered. R5 resolves this by aggregating on the **latest** row's status per logical key, scoped to this batch's runs (see R5).

### R5. Strict validate gate, batch-scoped

Two distinct parity checks; only the first is in-scope for B2.

**In-scope — within-job landing parity (B2 validate task).**

`validate` reads the `batch_id` taskValue from `prepare_manifest`, then joins the manifest table to the watermark registry on the run_ids produced by this batch's iterations:

```sql
WITH batch_runs AS (
    SELECT r.run_id, r.source_system_id, r.schema_name, r.table_name, r.load_group, r.status, r.updated_at
    FROM <wm_catalog>.<wm_schema>.watermarks r
    JOIN manifest m
      ON m.batch_id = :batch_id
     AND m.source_system_id = r.source_system_id
     AND m.schema_name      = r.schema_name
     AND m.table_name       = r.table_name
     AND m.load_group       = r.load_group
    WHERE r.updated_at >= :batch_started_at
),
latest_per_key AS (
    SELECT source_system_id, schema_name, table_name, load_group,
           status
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY source_system_id, schema_name, table_name, load_group
            ORDER BY updated_at DESC
        ) AS rn
        FROM batch_runs
    )
    WHERE rn = 1
)
SELECT
    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
    COUNT(*) FILTER (WHERE status = 'failed')    AS failed,
    COUNT(*) FILTER (WHERE status IN ('pending', 'running', 'landed_not_committed')) AS in_flight
FROM latest_per_key;
```

Assertions (all must hold, else `validate` fails the job):

- `completed == expected_count_from_manifest`
- `failed == 0`
- `in_flight == 0`

Optional per-table JDBC-read-rows vs landed-parquet-rows parity check is available behind a flowgroup-level config flag. It reads landed parquet metadata directly (no AutoLoader, no bronze involvement).

**Out-of-scope — bronze parity.** Bronze row count vs landed parquet count is a downstream concern owned by the DLT pipeline that consumes the landing zone. A future "bronze parity gate" can be added as a separate downstream task; B2 does not own it.

---

## 4. Integration surface against existing LHP codegen

Verified against `src/lhp/` as of `spike/jdbc-sdp-a1` HEAD.

| File | Change |
|------|--------|
| `src/lhp/models/config.py` | Extend `FlowGroup.workflow` dict schema to accept `execution_mode: "for_each"` and `concurrency: int`. No new top-level model field. |
| `src/lhp/core/validators/load_validator.py` | Extend `_validate_jdbc_watermark_v2_source`: when `execution_mode == "for_each"`, enforce (a) all actions share `source_system_id`; (b) all actions target the same `landing_path` root; (c) all actions share `wm_catalog`.`wm_schema`; (d) `len(actions) ≤ 300`; (e) `1 ≤ concurrency ≤ 100` with default = `min(action_count, 10)`. |
| `src/lhp/generators/bundle/workflow_resource.py` | Branch on `flowgroup.workflow.execution_mode`. When `"for_each"`, emit the three-task topology via new Jinja template; else retain current per-action static emission. |
| `src/lhp/templates/bundle/workflow_resource.yml.j2` | Add `{% if execution_mode == "for_each" %}` branch rendering the DAB for_each_task YAML plus prepare_manifest and validate task entries. |
| `src/lhp/templates/bundle/tasks/prepare_manifest.py.j2` | **New.** Manifest writer notebook. Computes action list from flowgroup, evaluates `batch_id = derive_run_id(dbutils)`, `MERGE`s manifest rows, emits the JSON-array taskValue. |
| `src/lhp/templates/bundle/tasks/validate.py.j2` | **New.** Batch-level validation gate. Reads `batch_id` taskValue, runs the SQL in R5, fails the task on any assertion violation. |
| `src/lhp/templates/load/jdbc_watermark_job.py.j2` | Single conditional header block to unpack `taskValues` when present. Body unchanged. |
| `src/lhp/generators/load/jdbc_watermark_job.py` | Plumb `load_group` (Tier 2) into template context. Thread `execution_mode` through so the template header block is emitted when `for_each`. |
| `src/lhp/core/action_registry.py` | No change. |

---

## 5. Public API reuse

`lhp_watermark` names consumed by B2 (no new exports):

- `WatermarkManager` — state machine
- `derive_run_id` — batch + iteration run_id derivation
- `SQLInputValidator` — sanitisation before any SQL composition
- `DuplicateRunError`, `TerminalStateGuardError`, `WatermarkValidationError`, `WatermarkConcurrencyError` — exception hierarchy (propagated verbatim)
- `sql_literal`, `sql_timestamp_literal`, `sql_identifier` — SQL emission helpers for manifest writes

Tier 2 extends existing `WatermarkManager` method signatures with `load_group: Optional[str] = None` (back-compat). No new exported names.

---

## 6. Architectural invariants preserved

B2 preserves the following binding invariants from existing ADRs and the L2 spec. Each is named here so review can verify the design does not erode any of them.

1. **ADR-002 — lhp_watermark as top-level package.** Deployed via DAB workspace-file sync. B2 notebooks import from the package; they do not re-implement state machine logic.
2. **ADR-003 — landing shape `<landing_root>/_lhp_runs/<run_id>/`.** B2 reuses the existing template's `df.write.mode("overwrite").format("parquet").save(run_landing_path)` call. No change to shape.
3. **ADR-004 — watermark registry placement `metadata.<env>_orchestration.watermarks`.** B2 reads and writes only this table. No new registries.
4. **L2 §5.3 state-machine control flow.** `insert_new` outside try; JDBC + landing inside try with `mark_failed`-then-`raise`; `mark_complete` outside/after. B2 worker body is unchanged from today's template.
5. **Terminal-state guard.** `TerminalStateGuardError` propagates; never swallowed. `prepare_manifest` idempotency (R1a) uses a fresh `batch_id` per attempt so this invariant is not challenged.
6. **SQL safety.** `SQLInputValidator` applied before any manifest or registry SQL composition. No string-concat SQL. No quote-stripping shortcuts.
7. **UTC session.** Every watermark DML sets session UTC; B2 manifest writes follow the same convention.

---

## 7. Concurrency and ceiling bounds

| Knob | Default | Floor | Ceiling | Enforcement |
|------|---------|-------|---------|-------------|
| `concurrency` | `min(action_count, 10)` | 1 | 100 | Codegen-time validation in `load_validator.py`. Hard cap matches DAB `for_each_task` platform limit. |
| `len(actions)` | — | 1 | 300 | Codegen-time validation; emits `LHPConfigError` at bundle generation. 300 chosen to leave headroom against the ~48 KB DAB taskValue ceiling at ~200 bytes/entry. |
| `for_each_task.task.max_retries` | 1 | 0 | — | DAB runtime; operator-tunable per flowgroup. |

**Source connection caveat (operator responsibility).** B2 cannot validate that the JDBC source can sustain `concurrency` parallel readers. Operator is responsible for sizing `concurrency` against source connection ceilings (e.g., PostgreSQL `max_connections`, RDS connection pool limits). This is documented in the flowgroup YAML schema help text.

---

## 8. Deferred to follow-up phases

Out of scope for this design doc; each item is a separate planning artifact when its time comes:

- HIPAA hashing implementation (hook location described here; no-op default hook + column-masking code ship in a separate phase).
- Bronze parity gate (downstream task; consumed by DLT pipeline owners).
- Manifest table retention policy (time-based or batch-count-based pruning; defer until operational data exists).
- Per-iteration runtime distribution observability (histograms, p95 duration alerts).
- B2 for non-JDBC load types (kafka, cloudfiles, delta). Start with jdbc_watermark_v2; generalise once the pattern is proven in production.

---

## 9. Cross-references

- `docs/ideas/spike-a1-vs-b-comparison.md` — strategic context; B2-adopt vs A1-defer framing.
- `docs/planning/tier-1-hwm-fix.md` — Tier 1 (shipped); prerequisite closed.
- `docs/planning/tier-2-hwm-load-group-fix.md` — Tier 2 (prerequisite for B2 integration commit).
- `docs/adr/ADR-002-lhp-runtime-availability.md` — lhp_watermark package deployment.
- `docs/adr/ADR-003-landing-zone-shape.md` — landing path convention.
- `docs/adr/ADR-004-watermark-registry-placement.md` — registry location.
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` — reused per-iteration worker (single header addition).
- `src/lhp_watermark/watermark_manager.py` — public API consumed verbatim.

---

## 10. Verification of this design doc

Cross-check performed before commit:

- [x] Five binding requirements R1–R5 all present.
- [x] R1a idempotency requirement present (addresses review issue #7).
- [x] Validate query batch-scoped via `batch_id` join to manifest (addresses review-of-Rev-2 gap).
- [x] Task-value ceiling bound to `len(actions) ≤ 300` in codegen validator (review issue #5).
- [x] Validate parity explicitly scoped to landing, not bronze (review issue #6).
- [x] HWM read live per R3, not snapshotted per R1 (review issue #2 resolved).
- [x] Concurrency knob bounded (review issue #10).
- [x] HIPAA hook location described without shipping template code (review issue #12).
- [x] Cold-start behaviour explicit (review issue #14).
- [x] Retry semantics explicit; validate tolerates retry-induced duplicate rows via latest-per-key aggregation (review issue #1 resolved).
- [x] Every invariant from ADR-002/003/004 + L2 §5.3 + SQL safety + UTC + terminal-state guard named in §6.
- [x] All file changes scoped to integration surface table §4; no hidden code changes.
- [x] Public API reuse list ≤ 10 names; no new lhp_watermark exports.
- [x] Static-manifest alternative refuted in §2 with three reasons (review issue #9).
