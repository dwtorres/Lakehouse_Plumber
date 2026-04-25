# Spike A1 vs Spike B — Comparison & Integration Recommendation

**Agent:** COMPARE-A1-B / RETEST-MEASURE-COMMIT
**Date:** 2026-04-24 (reframed to B2-adopt / A1-deferred 2026-04-24)
**Branch:** spike/jdbc-sdp-a1
**Source:** PG AdventureWorks via `pg_supabase` (Supabase PgBouncer pooler)

---

## Summary Verdict

**B2 (hardened `for_each` on the production `lhp_watermark` substrate) is the adopted production path. Option A1 (SDP-native dynamic flows) is a deferred future optimisation. Tier 2 (`load_group` registry column) is a B2 prerequisite, not an optional later mitigation.**

Both spikes proved their theses under fair test conditions: at N=61 on PG AdventureWorks, A1 materialised 738,913 rows with exact source parity (retest `retest_1777031969`), and B materialised 780,132 catalog rows across 63 tables. The shared Tier 1 HWM isolation prerequisite is shipped (commit `63bcfdb`, V1 probe verified 2026-04-24).

The earlier "Spike A vs Spike B, preferred path B" framing understated the integration gap. Raw Spike B is not production-ready — its `ingest_one.py` swallows exceptions, passes full manifest rows through DAB task values, uses `saveAsTable` as its idempotency primitive, and is disconnected from the existing production `lhp_watermark` L2 §5.3 contract. B2 is the hardened variant: same `for_each` topology, but rebuilt on the production substrate (see `docs/planning/b2-watermark-scale-out-design.md` for the design spec). A1 remains viable as a future cost optimisation but is deferred behind three concrete trigger conditions (see §A1-deferral triggers).

**Tier 1 scope correction.** Tier 1 closes **cross-source** HWM poisoning only (different `source_system_id` values against the same `(schema, table)` pair). Same-source + same-`(schema, table)` + different flowgroup still poisons. B2 requires Tier 2 (`load_group` column) to close this; Tier 2 is a hard prerequisite for any B2 integration commit, not the "available if future multi-tenancy requires" caveat the original draft described. See `docs/planning/tier-2-hwm-load-group-fix.md` for the migration plan.

Jump to the [TL;DR decision guide](#tldr-decision-guide) for a one-paragraph read, or skip to [Integration Path Options](#integration-path-options) for the full reframed options.

---

## Test Harness

| Parameter | Value |
|-----------|-------|
| Source system | `pg_supabase` — PostgreSQL AdventureWorks (Supabase hosted, PgBouncer pooler) |
| AW base tables | 73 |
| Skip list (binary/XML/credentials) | 5 (`production.document`, `.illustration`, `.productmodel`, `.productphoto`, `person.password`) |
| Target working set | 68 selected |
| Environment | `devtest` — `devtest_edp_bronze`, `devtest_edp_orchestration` |
| A1 run_id (original) | `scale-1777006574` — 2026-04-24 |
| A1 run_id (retest) | `retest_1777031969` — 2026-04-24 |
| B run_id | `b-scale-1777004929` — 2026-04-24 |
| A1 effective flows | 61 (7 filtered by over-broad view-exclusion regex `^v[a-z]`) |
| B effective iterations | 63 (68 selected; 5 already processed in a prior partial run) |

---

## Reality-Check of Claimed Results

### Spike A1 — Original Run

**Claimed:** 61/61 flows COMPLETED, 0 rows written (MV semantics, normal behavior).
**Actual:** 0 rows in all bronze tables. Confirmed by `COUNT(*)` query against
`devtest_edp_bronze.jdbc_spike.humanresources_employee` (source has 290 rows).

**Root cause — second-run watermark stale-HWM trap:**

The A1 scale run `scale-1777006574` was not a first run. The watermark registry
already held completed entries for a prior B run (`b-scale-1777004929`) that loaded
290 rows for `humanresources.employee` with `watermark_value = '2014-12-26 09:17:08'`.
A1's `prepare_manifest.py` correctly reads the latest completed HWM from the registry
and uses it as `watermark_value_at_start`. The SDP flow body then applies:

```python
df = df.filter(F.col("ModifiedDate") > F.lit("2014-12-26 09:17:08.637000"))
```

The AdventureWorks dataset's `MAX(ModifiedDate)` for `humanresources.employee` is
exactly `2014-12-26T09:17:08Z` — the same value. The strict `>` filter returns
**zero rows on every table** because AW is a static historical dataset with no
records newer than the stored HWM.

**Why 0 rows does not surface as a failure:**

SDP Materialized Views report `COMPLETED` even when the query returns 0 rows —
a zero-row result is a valid MV state. The reconcile task then queries
`MAX(ModifiedDate)` from the empty bronze MV, gets `NULL`, and falls back to
`new_hwm = hwm_at_start`. On the next run, `prepare_manifest` reads that same HWM
and the cycle repeats. A1 is permanently trapped with 0 new rows and no error signal.

**This is not an MV-semantics-only issue.** Even if A1 used streaming tables,
the watermark filter would still return 0 rows. The root fault is that
`prepare_manifest` uses a shared watermark registry that was already advanced to
the AW dataset ceiling by Spike B. A1 never performed a first-run full-load because
B ran first and claimed the HWM.

**Other suspects examined and cleared:**

- Decimal-column skip: PG AW uses `decimal(19,4)` not `decimal(38,10)`. No columns
  are skipped. The HANDOFF doc confirms "no overflow risk."
- Column-name case sensitivity: `ModifiedDate` is present verbatim in federation
  DESCRIBE output. No mismatch.
- STRING vs TIMESTAMP comparison: `ModifiedDate > '2014-12-26 09:17:08.637000'`
  works correctly via Spark implicit cast; tested by B successfully.

**Additional structural finding:** The 133 objects in `devtest_edp_bronze.jdbc_spike`
consist of 66 `MATERIALIZED_VIEW` objects (the user-visible targets) and 67 `MANAGED`
tables with names matching `__materialization_mat_{pipeline_uuid}_{table}_1`. These
internal backing tables also contain 0 rows, confirming that SDP materialized all MV
bodies and found zero qualifying rows — not that SDP skipped execution.

### Spike A1 — Retest Verification (2026-04-24)

Retest `retest_1777031969` deleted all `pg_supabase` rows from `watermark_registry`
and `manifest` (Oracle rows preserved), re-deployed, and ran the A1 job from a clean
HWM state. Results:

- **61/61 flows COMPLETED**, 0 failed.
- **738,913 rows materialised** into 61 Delta tables in `devtest_edp_bronze.jdbc_spike`.
- **Per-table parity: 61/61 PASS**, delta_pct = 0.0000 on every table.
- **HWM ceiling: 61/61 PASS** — every registry HWM exactly matches the pre-delete
  B-ceiling snapshot.
- **Wall clock: 1332s (~22 min)** — within 90-min budget.
- No retry, no recovery events.

A1 proves the thesis: SDP dynamic flow generation can ingest a full 61-table dataset
at N=61 scale with zero row loss when given a clean first-run HWM state.

### Spike B — Row Count Verification

**Claimed:** 739,005 rows across 62 completed tables.
**Actual (verified):** 780,132 rows across 63 tables confirmed by `SUM(COUNT(*))` over
all 63 `devtest_edp_bronze.jdbc_spike_b.*` tables via warehouse API.

**Discrepancy explanation:** The metrics doc reports rows written at job completion
time (2026-04-24). Between job completion and this agent's query, no new runs
were triggered, but the `devtest_edp_bronze.jdbc_spike_b.humanresources__department`
table — which failed with `TABLE_OR_VIEW_ALREADY_EXISTS` during the run — exists in
the schema (created by a prior cancelled run) and holds 16 rows not counted in the
739,005 figure. The additional ~41,127 rows above 739,005 suggest at least one partial
prior run deposited data in some tables before being cancelled. The claimed 739,005 is
the rows written *by this run*; the actual catalog total is higher due to append-mode
accumulation from earlier partial runs. Both figures confirm B actually materialized
Delta data.

---

## Dimension-by-Dimension Comparison

Two rows have tightening corrections applied (failure isolation, per-flow retry) reflecting the distinction between raw Spike B and the hardened B2 design. The rest of the table records empirical spike-run numbers and stands unchanged.

| Dimension | Spike A1 (SDP dynamic flows) | Spike B (`for_each_task`) | Verdict |
|-----------|------------------------------|-------------------------|---------|
| **Tables successfully ingested** | 61/61 (738,913 rows; retest-verified) | 62 of 63 (1 idempotency failure) | Equivalent at clean-run |
| **Total rows loaded (verified)** | 738,913 (retest; exact match to PG source) | 780,132 (catalog); 739,005 (this-run metric) | Equivalent on overlapping 61 tables |
| **Total wall-clock (job trigger → validate)** | 1,332 s retest (22 min); 2,462 s original (41 min) | 3,097 s (51.6 min) | A1 faster |
| **Concurrency model** | SDP runtime-managed (opaque) | `for_each concurrency: 10` (explicit, configurable) | B — transparent and tunable |
| **Concurrency observed** | Not directly measurable per-flow | 10 sustained (confirmed via API) | B — observable |
| **Failure isolation** | SDP flow-level (MV body exception → flow FAILED; others continue) | Spike B's `ingest_one.py` swallowed exceptions, defeating DAB iteration failure detection. **B2 returns to the production contract's raise-on-failure semantics** (`jdbc_watermark_job.py.j2:255-262` already does this correctly); DAB then sees iteration failures and fires task-level retries honestly. | Both pass under B2; Spike B raw code is non-production |
| **State machine location** | External (prepare_manifest + reconcile bookend; no state inside flow body) | B2: inline per iteration using the production `WatermarkManager` L2 §5.3 contract. Spike B's direct registry writes from `ingest_one.py` bypass the production state machine. | B2 simpler than A1; A1 introduces reconcile as a required 5th task |
| **Code complexity** | 1,239 total lines (5 tasks + pipeline + 2 yml) | Spike B raw: 874 lines. B2 reuses existing production template + adds prepare_manifest + validate notebooks (est. +200 lines of template, no duplicate state machine). | B2 is lightest |
| **HWM poisoning vulnerability** | Cross-source: Tier 1 closed. Same-source cross-flowgroup: requires Tier 2. | Same as A1 — both share the registry. | Both need Tier 2 before any multi-flowgroup production deployment. |
| **Ops gotchas (scale run)** | (1) Zero-row silent trap on pre-advanced HWMs (Tier 1 fix closes cross-source portion); (2) `^v[a-z]` over-filter loses valid tables; (3) stale-pending-row collision needs pre-cleanup; (4) fixtures_discovery slow (1,401 s via PgBouncer SHOW TABLES) | Spike B: (1) `saveAsTable` idempotency fails on retry of cancelled run — **B2 replaces this with ADR-003's run-scoped landing (`<landing_root>/_lhp_runs/<run_id>/`), which is already idempotent per (run_id, source_table)**; (2) prepare_manifest 403 s for 73 sequential HWM lookups — B2 batches via single `GROUP BY` per Tier 2 registry; (3) `abandoned` row provenance — handled by B2's retry-aware validate (R5). | B2 resolves all three Spike B gotchas in its design |
| **Cost model per iteration** | Single pipeline update runs all N flows on one cluster; marginal cost per table near-zero once cluster is up | Each of 63 iterations starts a serverless notebook (~10-15 s cold start); 63 cold starts at \$0.40/DBU-hour serverless is material | A1 cheaper per-flow at large N on warm cluster; B cold-start cost scales with N (still within budget at N≤100) |
| **DAB asset footprint** | Job (5 tasks) + Pipeline (1 SDP resource) | Job (3 tasks): prepare_manifest → for_each_ingest → validate | B2 simpler (no SDP pipeline resource to manage) |
| **Operator debuggability** | Two places: pipeline UI (flow events) + job run history; reconcile must succeed for watermarks to be correct | One place: job run history; each iteration is a visible task with its own log | B2 wins — single pane |
| **Per-flow retry model** | SDP internal retries inside pipeline update (configurable); failed flows do not rerun without new pipeline update | Spike B: `ingest_one.py` never re-raised, so DAB `for_each_task.task.max_retries` was theoretical. **B2 changes this: iteration re-raises after `mark_failed`, so DAB retry is real.** Validate task tolerates retry-induced duplicate rows via latest-per-key aggregation. | B2 retry is operationally real and observable |
| **Observability surface** | SDP `event_log()` TVF (queryable via SQL); requires knowing pipeline_id; reconcile reads it post-run | Job run history API + manifest table; no external TVF required | B2 simpler operationally |
| **LHP integration delta** | Larger: new SDP pipeline resource, reconcile task, event_log TVF reader — none of these exist in current LHP codegen | B2: one new `execution_mode: for_each` field on `FlowGroup.workflow`; two new task templates (prepare_manifest, validate); one conditional header block in existing jdbc_watermark_v2 template. Reuses production `WatermarkManager` API verbatim. | B2 smaller integration delta |

---

## Integration Path Options

**Shared prerequisite — DONE:** Tier 1 HWM isolation shipped in commit `63bcfdb` on 2026-04-24 (quick task `260424-dwc`, V1 probe PASSED). Both spike `prepare_manifest.py` copies filter the registry by `source_system_id`. See `docs/planning/tier-1-hwm-fix.md` and the [V1 Probe Verification](#v1-probe-verification-tier-1-proof) section below.

**B2 prerequisite — scheduled:** Tier 2 `load_group` migration (`docs/planning/tier-2-hwm-load-group-fix.md`). Must land before the B2 integration commit.

### TL;DR decision guide

**Use B2 (adopt as default).** All current N ≤ 100 workloads ship on B2 regardless of whether they are first-run full-loads or steady-state incremental. B2 is LHP's scale-out emission pattern for `jdbc_watermark_v2` flowgroups declaring `execution_mode: for_each`. Design spec: `docs/planning/b2-watermark-scale-out-design.md`.

**Defer to A1 only when all three hold:** (a) a production B2 flowgroup consistently exceeds 0.5 wall-clock minutes per table at N ≥ 100 measured over a 4-week window; AND (b) total B2 cold-start cost exceeds the team's cost-tracking owner's established DBU/month baseline (placeholder until baseline is captured); AND (c) SDP streaming-table over foreign-catalog incremental support is generally available. Until all three are true, A1 stays deferred.

### V1 Probe Verification (Tier 1 proof)

Empirical test of the Tier 1 filter fix, run 2026-04-24 against `devtest_edp_orchestration.jdbc_spike.watermark_registry`:

1. Baseline HWM for `humanresources.employee` via `pg_supabase` = `2014-12-26 09:17:08.637000`.
2. Inserted synthetic "poisoning" row — `source_system_id = 'fake_federation'`, same `(schema, table)`, `watermark_value = '2099-01-01T00:00:00'`.
3. Ran old query (no `source_system_id` filter): returned **`2099-01-01T00:00:00`** — poisoning confirmed as real attack vector.
4. Ran new query with Tier 1 filter (`AND source_system_id = 'pg_supabase'`): returned **`2014-12-26 09:17:08.637000`** — correct, poisoning excluded.
5. Cleanup: fake row deleted, `SELECT COUNT(*) WHERE source_system_id='fake_federation'` = 0.

The cross-source hazard is no longer theoretical — both the attack and the fix are demonstrated end-to-end. The same-source cross-flowgroup hazard remains open and is closed by Tier 2.

### Option B2 — Hardened `for_each` on the production `lhp_watermark` substrate (ADOPTED)

**What LHP ships.** A new `execution_mode: for_each` field on `FlowGroup.workflow`. LHP codegen emits a three-task DAB job:

```
prepare_manifest  →  for_each_ingest  →  validate
```

The per-iteration worker is the existing `src/lhp/templates/load/jdbc_watermark_job.py.j2` template with one conditional header block to unpack `dbutils.jobs.taskValues`. Below the header the L2 §5.3 state-machine body is unchanged. Public `lhp_watermark` API is reused verbatim.

**Five binding requirements (condensed; see design doc for full spec):**

- **R1.** Per-table manifest row; HWM read live by worker via `WatermarkManager.get_latest_watermark(source_system_id, schema_name, table_name, load_group)`. No HWM snapshot in the manifest row.
- **R1a.** `prepare_manifest` MERGEs on `(batch_id, action_name)` for idempotency; DAB task retries produce fresh `batch_id`.
- **R2.** Task values carry only iteration key (`source_system_id, schema_name, table_name, action_name, load_group, batch_id`, ~200 B/entry). Codegen validates `len(actions) ≤ 300` against DAB's ~48 KB taskValue ceiling.
- **R3.** Worker uses production contract verbatim — `derive_run_id` → `insert_new` outside try → JDBC + landing inside try → `mark_landed` → `mark_complete` outside try. HIPAA hashing hook location is between JDBC read and landing write; hook code ships separately.
- **R4.** Failed iterations call `mark_failed` then `raise`, so DAB `for_each_task.task.max_retries` is honoured. Validate uses latest-per-key aggregation to tolerate retry-induced duplicate rows.
- **R5.** Strict validate gate: landing parity only (bronze parity is a downstream concern). `completed == expected`, `failed == 0`, `in_flight == 0`, scoped to this batch via the manifest-table-join on `batch_id`.

**Four tightening corrections vs raw Spike B:**

- **(a) Tier 1 scope.** Tier 1 closes cross-source poisoning only; Tier 2 is a B2 prerequisite, not optional future mitigation.
- **(b) Retry contract.** Spike B's `ingest_one.py` swallowed exceptions, which prevented DAB from observing iteration failures. B2 returns to the production template's raise-on-failure semantics (`jdbc_watermark_job.py.j2:255-262` already does this correctly). DAB retry becomes operationally real once that divergence is removed.
- **(c) Manifest shape.** Task values carry the small iteration key (~200 B/entry), not full manifest rows. Worker reads its row from the manifest table at iteration start and re-resolves env substitutions at runtime. Cap at `len(actions) ≤ 300` enforced at codegen.
- **(d) Idempotency primitive.** B2 uses run-scoped landing (`<landing_root>/_lhp_runs/<run_id>/`) per ADR-003, which is deterministic overwrite per `(run_id, source_table)`. AutoLoader consumes the landing zone downstream. Spike B's direct-to-bronze `saveAsTable` is not the pattern B2 uses.

**Reusable from spike.** `spikes/jdbc-sdp-b/tasks/prepare_manifest.py` and `tasks/validate.py` — conceptually; both are rewritten against the `lhp_watermark` API and the manifest schema in the design doc. Not a direct port.

**Cost & effort.** ~1.5 sprints once Tier 2 is in. Closer to LHP's existing per-table model. Reuses all of `load/jdbc_watermark_job.py.j2`'s body.

**Where it wins (vs A1).** Per-iteration UI visibility; explicit tunable concurrency; single debug surface; native DAB retry honestly observable once raise-on-failure is in; smallest LHP integration delta; no SDP pipeline resource to manage per env.

**Where it hurts (vs A1).** Per-iteration cold-start cost scales with N. At N ≥ 100 cold-starts become measurable overhead relative to A1's warm-cluster single-pipeline model. Wall-clock is 2.3× A1's at N=61 and the gap widens with N. Both concerns are acceptable at the team's current N ≤ 100 target and are the basis for A1's deferral triggers.

**Serverless quota caveats.** DAB `for_each_task` hard limits: 100 concurrent iterations and 5,000 total per job run. B2 codegen caps concurrency at 100 and actions at 300; larger flowgroups need a split strategy before they can ship.

### Deferred: Option A1 (Lakeflow optimisation path)

Keep A1 spike intact for a future reconsideration. Revisit only when all three triggers hold:

1. Production B2 flowgroups consistently exceed 0.5 wall-clock minutes per table at N ≥ 100 measured over a 4-week window.
2. Total B2 cold-start cost exceeds the team's established DBU/month baseline (placeholder until captured).
3. SDP streaming-table over foreign-catalog incremental support is generally available.

**Preconditions before re-validation (if triggered):**

- Run-isolation proof for dynamic flows across concurrent pipeline updates.
- Append/accumulation semantics proven on MVs over foreign catalogs for incremental (non-full) loads.
- Event-log permission model validated in production-equivalent workspace.
- HIPAA hashing integration path demonstrated (B2 defines the hook; A1 would need the equivalent inside the SDP flow body).

Until all of the above hold, A1 stays parked. The A1 spike artifacts (`spikes/jdbc-sdp-a1/`) remain on this branch as historical reference; do not migrate them to `watermark`.

### Option C — Adopt both under `execution_mode` (NOT ADOPTED)

Option C (ship both A1 and B2 generators, pick per flowgroup) is not adopted now. Maintenance cost of two code paths with two test matrices and two operator runbooks is not justified at the current N ≤ 100 scale. Reconsider only if A1 is later validated against its triggers **and** a fleet of production B2 flowgroups already exists — at that point Option C becomes "add A1 as an opt-in to a proven B2 baseline," which is a different project shape with better-understood constraints.

### Decision criteria matrix

Reframed to reflect the B2-adopt decision. Rows are informational; no live tradeoff to resolve.

| Criterion | Option A1 (deferred) | Option B2 (adopted) | Option C (not adopted) |
|-----------|----------|----------|----------|
| Status | Deferred pending 3 triggers | Ships as default after Tier 2 | Not adopted |
| Team capacity needed | 3 sprints + SDP expertise | 1.5 sprints post-Tier-2 | 4 sprints (A1 + B2 dual) |
| Target N per flowgroup | 100+ favourable | 1–300 (codegen-capped) | Any |
| Operator familiarity with SDP | Required | Not required | Required for A1 slice |
| Migration from existing LHP templates | Full rewrite of execution layer | Extension via `execution_mode: for_each` + two new task templates | Extension + additive SDP layer |
| Cost sensitivity (per-run DBU) | Wins at N ≥ 100 on warm cluster | Within budget at N ≤ 100 | Optimise per-flowgroup |
| Debug surface complexity tolerance | Accepts 2-surface debugging | Prefers single pane | Accepts mixed |
| SDP streaming-table-over-foreign-catalog dependency | Hard dependency when revisited | No SDP dependency | Hedges |
| Future-proofing vs Databricks direction | Aligned (Lakeflow declarative) | Neutral | Hedges |

### What to decide (resolved)

1. **LHP integration path:** B2 (adopted). A1 deferred; C not adopted.
2. **Rollout phasing:** per-flowgroup adoption. New jdbc_watermark_v2 flowgroups default to `execution_mode: for_each` post-Tier-2. Existing static-emission flowgroups migrate on demand, not as a fleet push.
3. **Deprecation policy for the existing static emission path:** kept as fallback indefinitely. `execution_mode` defaults to static when absent so legacy flowgroups remain byte-for-byte unchanged.

---

## Concrete Next Actions

### Completed (2026-04-24)

1. **Tier 1 HWM isolation fix** — DONE (commit `63bcfdb`, quick task `260424-dwc`).
   Added `source_system_id` parameter to `get_current_hwm` in both spike `prepare_manifest.py` copies; filter binds the value via `spark.sql args=`. V1 probe proved the filter excludes a synthetic poisoning row. Plan at `docs/planning/tier-1-hwm-fix.md`.

2. **Comparison doc reframe to B2-adopt / A1-deferred** — DONE (this revision).

3. **B2 design spec** — DONE (`docs/planning/b2-watermark-scale-out-design.md`).

4. **Tier 2 migration plan** — DONE (`docs/planning/tier-2-hwm-load-group-fix.md`).

### B2 prerequisite (do before B2 integration commit)

5. **Tier 2 `load_group` migration** (Owner: `@eng`, Effort: 1–2 days)
   Add `load_group STRING` column to `watermarks`. Extend `WatermarkManager.insert_new / mark_* / get_latest_watermark / get_recoverable_landed_run` with `load_group: Optional[str] = None` kwarg. Backfill existing rows to `'legacy'`. Thread flowgroup name through to every manager call in `jdbc_watermark_job.py.j2`. V1/V2/V3 verification per `docs/planning/tier-2-hwm-load-group-fix.md`. **Blocks B2 integration commit.**

### B2 integration surface (once Tier 2 is in)

6. **Extend `FlowGroup.workflow` schema** (Owner: `@eng`, Effort: 0.5 day) — accept `execution_mode: "for_each"` and `concurrency: int`.

7. **Extend `LoadActionValidator._validate_jdbc_watermark_v2_source`** (Owner: `@eng`, Effort: 1 day) — when `execution_mode == "for_each"`: enforce shared `source_system_id`, shared landing root, shared `wm_catalog.wm_schema`, `len(actions) ≤ 300`, `1 ≤ concurrency ≤ 100`.

8. **Branch `workflow_resource.py` on `execution_mode`** (Owner: `@eng`, Effort: 1 day) — emit three-task topology when `for_each`; keep static emission otherwise.

9. **New task templates** (Owner: `@eng`, Effort: 2 days) — `prepare_manifest.py.j2` (manifest MERGE + taskValue emit) and `validate.py.j2` (batch-scoped validate query).

10. **Conditional header block in `jdbc_watermark_job.py.j2`** (Owner: `@eng`, Effort: 0.5 day) — unpack `taskValues` when present; fall through to static-render otherwise.

11. **Plumb `load_group` and `execution_mode` through `generators/load/jdbc_watermark_job.py`** (Owner: `@eng`, Effort: 0.5 day).

### Deferred A1 backlog (only if A1 triggers fire)

12. **Fix `^v[a-z]` view-regex over-filter** — Replace regex in `spikes/jdbc-sdp-a1/tasks/fixtures_discovery.py` with explicit view-name allowlist.

13. **Parallelise `fixtures_discovery` schema queries** — Thread-pool over schemas to drop 1,401 s → <30 s.

14. **A1 N=100+ validation run** — A 100-table scale test with Tier 1+Tier 2 applied to measure whether A1's cost delta justifies the integration complexity.

### Cleanup

15. **Drop retest backup tables after 7-day retention** (Owner: `@eng`, Effort: 15m, Schedule: 2026-05-01)
    `devtest_edp_orchestration.jdbc_spike.{watermark_registry_bak,manifest_bak,hwm_ceiling_snapshot,source_row_counts}_retest_1777031969`.

---

## A1-deferral trigger metric

Revisit Option A1 only when all three hold:

1. Production B2 flowgroups consistently exceed **0.5 wall-clock minutes per table** at N ≥ 100, measured over a 4-week window.
2. Total B2 cold-start cost exceeds the team's established DBU/month baseline (placeholder pending baseline capture by the cost-tracking owner).
3. SDP streaming-table over foreign-catalog incremental support is generally available (currently uncertain; resolved either by Databricks roadmap announcement or a targeted spike).

Until all three trigger conditions are true, A1 stays deferred. The metric anchors the next reconsideration on observable evidence rather than memory.

---

## Open Questions Deferred

- **SDP streaming tables + foreign catalog:** Can SDP streaming tables read from
  `pg_supabase` (Lakehouse Federation) incrementally? Resolution blocks A1 trigger #3.

- **`abandoned` row provenance (B):** Some manifest rows transition to `abandoned`
  status without explicit notebook code. Likely Databricks Jobs platform behavior
  when a job run is cancelled mid-for_each. B2's retry-aware validate (R5) handles
  this without needing root-cause resolution; capture for future observability work.

- **Concurrency ceiling at N ≥ 100:** B2 codegen caps `concurrency` at 100 (DAB hard limit) and `len(actions)` at 300 (taskValue size ceiling). Flowgroups beyond 300 actions need a split strategy; deferred until a real case surfaces.

- **HIPAA hashing integration:** B2 defines the hook insertion point (between JDBC read and landing write, inside the try block, before `mark_landed`). Implementation is a separate phase; no code change ships in the B2 design doc.

- **Incremental load correctness on static datasets:** AW `ModifiedDate` values are
  static (max 2014). Production watermark validation should use a live or synthetic
  source with advancing timestamps to test true incremental correctness. Both spikes
  used AW solely for scale (N ≈ 63), not for incremental correctness.

---

## Appendix — HWM Poisoning: Historical Context and Tiered Mitigations

This appendix is retained for historical continuity. Tier 1 is shipped; Tier 2 is scheduled as a B2 prerequisite (not the optional future mitigation the original draft described); Tier 3 remains deferred.

### What it is

Both spikes share a single `watermark_registry` table. If two pipelines share registry rows — either via identical `(schema_name, table_name)` across sources (Tier 1 hazard) or across flowgroups within the same source (Tier 2 hazard) — one pipeline's completed HWMs can be read as the starting point for another pipeline's first run. The second pipeline sees a HWM at the dataset ceiling and returns zero rows on every table, silently, with no error.

In the original A1 spike run, Spike B ran first against `pg_supabase` and advanced all AW HWMs. A1's `get_current_hwm` took only `schema_name + table_name + status` — it did not filter by `source_system_id` (Tier 1 gap, since closed). Result: A1 returned zero rows on a static dataset.

### Tiered mitigations

**Tier 1 (code-only, no DDL) — SHIPPED 2026-04-24 (commit `63bcfdb`).**
Filter `get_current_hwm` by `source_system_id`. Closes cross-source poisoning (e.g., `pg_supabase` vs `freesql_catalog` sharing a schema+table name). Does **not** close same-source cross-flowgroup poisoning.

**Tier 2 (DDL + code) — B2 prerequisite; scheduled (`docs/planning/tier-2-hwm-load-group-fix.md`).**
Add `load_group STRING` column to the watermarks table. `load_group` = LHP flowgroup field's value, verbatim. Extend `WatermarkManager` methods with `load_group: Optional[str] = None` kwarg (back-compat). Closes the hazard between different LHP flowgroups ingesting from the same source. **Must land before any B2 integration commit.**

**Tier 3 (structural, heaviest) — deferred.**
Per-integration registry tables (one Delta table per load group). Complete isolation with no filter-key coupling. Higher operational cost: DDL per new pipeline; no cross-pipeline HWM queries. Reconsider only if Tier 2's row-level isolation creates query-performance concerns at N ≥ 1000 flowgroups.

### Backup retention note

The following scratch tables from retest `retest_1777031969` can be dropped 7 days after this document lands unchallenged (schedule: 2026-05-01).

- `devtest_edp_orchestration.jdbc_spike.watermark_registry_bak_retest_1777031969`
- `devtest_edp_orchestration.jdbc_spike.manifest_bak_retest_1777031969`
- `devtest_edp_orchestration.jdbc_spike.hwm_ceiling_snapshot_retest_1777031969`
- `devtest_edp_orchestration.jdbc_spike.source_row_counts_retest_1777031969`
