# ADR-003 Follow-Ups — Sequenced Plan

**Status**: Planning (2026-04-19; revised same day to defer Phase B).
**Owner**: dwtorres@gmail.com.
**Related**: [ADR-003](../adr/ADR-003-landing-zone-shape.md), [ADR-002](../adr/ADR-002-lhp-runtime-availability.md), [ADR-001](../adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md), [B1 blueprint](./b1-api-watermark-blueprint.md) (parked).
**Not a decision.** This document sequences the seven open items surfaced by ADR-003 so they can be worked without re-colliding with each other.

**Revision 2026-04-19**: Phase B (API source + Q4 decision) deferred — fork has no current API ingestion need (only API-sink reference); building `api_watermark_v2` solely to validate Q4 generalization is speculation. Q4 stays on Alternative A (status quo) on JDBC-only evidence; revisit when a real API-ingestion consumer materializes. B1 design research preserved in [`b1-api-watermark-blueprint.md`](./b1-api-watermark-blueprint.md) so it can be picked up cold.

## Context

ADR-003 opened as a research charter after Wumbo E2E proved Path ③ green. Seven items remained open. They span different domains (documentation research, generator code, external infrastructure, new source types) and have non-obvious interdependencies. Left uncoordinated, these would either stall on a single blocker or land in conflicting order. This plan lays out four sequenced phases with explicit decision gates.

Not covered here: Constitution amendments (none needed; P6 is agnostic to subdir naming), and the per-environment catalog refactor's ADR (will be its own ADR-004 once Phase C lands).

## Dependency graph

```
  [Q1 rationale]  ───────────┐
                             │
  [Q3 empty-batch]           ├── [Q4 final decision] ──┐
                             │                         │
  [2nd source type (API)] ───┘                         │
                                                       │
  [Q5 generator validation] ──┐                        │
                              ├── [{env}_edp_{medallion} refactor] ──┐
  [Q5 external ADLS pattern] ─┘                                      │
                                                                     │
  [Q2 retention] ───────────────────────────── any time, independent ┤
                                                                     │
                                                                   SHIP
```

Critical paths:

- **Q4 decision** requires both Q1 (rationale) **and** 2nd-source-type evidence. Cannot close either alone.
- **External ADLS pattern + catalog refactor** are tightly coupled — both target the same production-deploy target.
- **Q2 retention** is orthogonal; can ship whenever load warrants.

## Phase A — Empirical unblockers (parallel, small scope)

Goal: close the research questions that don't require building new components. Gather the evidence that Phase B needs.

### A1 — Q1 `_lhp_runs/` underscore rationale

**What**: ask the ADR-001 author (you) directly: was the underscore (a) protective namespace, (b) HDFS-era convention, or (c) arbitrary?
**How**: single-question GitHub Discussion on the fork (https://github.com/dwtorres/Lakehouse_Plumber/discussions) referencing ADR-003 §Q1. Document answer in-line in ADR-003.
**Deliverable**: ADR-003 §Q1 marked closed with authoritative answer.
**Effort**: 15 min (ask) + 15 min (record). Calendar-blocking on response time.

### A2 — Q3 empty-batch hardening

**What**: eliminate `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` when an incremental JDBC extract returns zero rows.
**Approach** (based on docs research, Agent 1 output):
1. Extractor template always writes a schema-bearing 0-row Parquet on empty result, via `spark.createDataFrame([], jdbc_schema).write.mode("overwrite").format("parquet").save(run_landing_path)`. JDBC schema is already available post-connection.
2. Generator adds `cloudFiles.schemaHints` option defaulted from the JDBC column metadata when the generator can derive it at generation time (optional — primary fix is #1).
**Files**:
- `src/lhp/templates/load/jdbc_watermark_job.py.j2` — empty-result branch before the write.
- `tests/unit/generators/load/test_jdbc_watermark_job.py` — empty-result test.
- `tests/templates/test_jdbc_watermark_template.py` — render test.
**Success**: JDBC extract with `WHERE 1=0` (force-empty) lands in Databricks, DLT AutoLoader reads it, no `CF_EMPTY_DIR`. Evidence: Wumbo workflow run with a mock empty-source extraction.
**Effort**: ~2h code + 1h test + 30min Wumbo validation.
**Status (2026-04-19)**: code + tests landed on `feature/adr-003-a2-empty-batch-hardening`. Template now branches on `_landing_has_parquet(...)`; the empty-batch path emits `_log_phase("landing_empty_schema_fallback", ...)` and writes a coalesced 0-row parquet built from the live JDBC `df.schema`. Approach #2 (generator-side `cloudFiles.schemaHints`) deferred — primary fix #1 closes the failure mode the bronze AutoLoader was hitting; hints become a hardening item once we have a per-source schema-drift signal. Wumbo dev-workspace validation (force-empty `WHERE 1=0` extract → bronze `streaming_table` populates without `CF_EMPTY_DIR`) is the remaining checkbox under Phase A exit criteria.

### A3 — Q5 (partial) — generator-side landing-schema validation

**What**: LHP refuses to generate when `landing_path` resolves into a schema that also hosts the flowgroup's bronze write target. Enforces the "landing must not share schema with bronze managed tables" rule discovered in Wumbo PR #2.
**Approach**:
- At generation time in `src/lhp/generators/load/jdbc_watermark_job.py`, parse `landing_path` for `/Volumes/<cat>/<schema>/...` and compare `<schema>` to the sibling write-action's `write_target.schema`. If equal, raise `LHPConfigError(LHP-CFG-0XX)` with remediation guidance.
- Emit a generator warning (not error) when `landing_path` is under a catalog different from the bronze target (cross-catalog landing is unusual but legitimate for the upcoming `{env}_edp_{medallion}` shape).
**Files**:
- `src/lhp/generators/load/jdbc_watermark_job.py`
- `tests/unit/generators/load/test_jdbc_watermark_job.py`
- `docs/errors_reference.rst` (new error code entry)
**Success**: a flowgroup with `landing_path: /Volumes/c/bronze/landing/t` + `write_target.schema: bronze` fails `lhp generate` with a clear error.
**Effort**: ~3h code + 1h test + doc.

### Phase A exit criteria

- [~] A1: ADR-003 §Q1 has authoritative rationale text. **Deferrable** (2026-04-19): underscore-prefix rationale low-stakes; closing it does not gate any production work. Ask whenever convenient.
- [x] A2: empty-batch extract verified end-to-end on a dev workspace. Code shipped on `feature/adr-003-a2-empty-batch-hardening` (`08fe67a1`); live validation 2026-04-20 against devtest workspace `dbc-8e058692-373e` per [`docs/runbooks/devtest-validation-adr-003-phase-a-c.md`](../runbooks/devtest-validation-adr-003-phase-a-c.md) §Step 4 — sentinel watermark forced empty incremental, A2 fallback wrote 0-row schema-bearing parquet at `_lhp_runs/local-f7816a34-.../`, no `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`.
- [x] A3: generator-side landing-schema validator shipped; Wumbo's existing bundle passes validation. Closed by `feature/adr-003-a3-landing-schema-validator` (`LHP-CFG-018`, 4 new tests, 3406 total pass).

Phase A can run in parallel. Earliest completion blocker is A1 response time.

## Phase B — Second source type + Q4 decision (DEFERRED 2026-04-19)

**Status**: deferred. Re-scope decision recorded above. The original goal (validate `jdbc_watermark_v2 + _lhp_runs/* + AutoLoader` convention via a second source type before deciding Q4) is sound only if a second source type is actually wanted. The fork's current scope is JDBC-only ingestion + an API-sink reference; there is no API-ingestion consumer asking for `api_watermark_v2`. Building it to satisfy Q4's "second data point" success criterion is speculation.

**Q4 disposition**: codebase remains on Alternative A (status quo, `_lhp_runs/{run_id}/` + Path ③ glob). Formal Q4 closure deferred until a real second-source consumer materializes — at that point B1 blueprint (`b1-api-watermark-blueprint.md`) provides a cold-start implementation plan. ADR-003 §Decision continues to read "Deferred" with that rationale.

**Original Phase B subsections preserved below for archival** (do not work them without reopening this decision):

Goal: validate whether the `jdbc_watermark_v2 + _lhp_runs/* + AutoLoader` convention generalizes. Decide ADR-003 §Q4 with scale evidence, not extrapolation.

### B1 — Implement `api_watermark_v2` source type

Per Agent 3's ranking: API source is the strongest candidate. Flat-file drop is already covered by upstream cloudfiles and doesn't exercise WatermarkManager; CDC short-circuits the landing tier (producer writes files, no LHP-side extractor) and doesn't stress the shape under test.

**Scope** (Agent 3's 90-min sketch, expanded):

- **New files**:
  - `src/lhp/generators/load/api_watermark_job.py` (~350 LoC) — mirrors `jdbc_watermark_job.py` structure; dispatches extraction notebook + CloudFiles DLT stub.
  - `src/lhp/templates/load/api_watermark_job.py.j2` (~150 LoC) — extraction template: `derive_run_id`, pagination loop (offset / timestamp-cursor / link-header variants), retry-on-429 backoff, WatermarkManager lifecycle, Parquet write.
  - `tests/unit/generators/load/test_api_watermark_job.py` — cursor types, empty result, network error, schema-hints flow.
  - `tests/templates/test_api_watermark_template.py` — render-contract tests mirrored from the JDBC template.
- **Config model**: extend `lhp.models.config` with an API source type (or extend the existing `jdbc_watermark_v2` dispatch to accept a `connection.type: api` variant — less code churn).
- **Runtime reuse**: `lhp_watermark.WatermarkManager`, `SQLInputValidator` (for source_system_id / endpoint validation), `derive_run_id`. No new runtime code.

**Out of scope for B1**:
- OAuth flows (use a simple bearer-token option; OAuth is a follow-up).
- Schema inference from API responses (rely on explicit schema or `cloudFiles.schemaHints`).
- Parallel pagination. Phase 1 is sequential-cursor only.

**Test fixture**: a minimal Flask or FastAPI app launched in a pytest fixture serving `/users?offset=N&limit=M` and `/empty`. Integration tests run the full generator → render → execute-in-pytest path.

**Effort**: ~2 days of focused work.

### B2 — Validate in Wumbo with a live API

Pick a real paginated API (Supabase REST, AdventureWorks Web API on Databricks samples, or a JSONPlaceholder clone) and add one `api_watermark_v2` flowgroup to `pipelines/05_api_ingestion/bronze/<source>_bronze.yaml`. Deploy + run. Validate:

- Extraction lands `_lhp_runs/<uuid>/*.parquet` at `/Volumes/.../api_<endpoint>/`.
- Bronze streaming_table populates.
- Second run returns zero new rows (HWM advanced); lands an empty-schema parquet per A2.
- Concurrent run handled (if applicable) via WatermarkManager's per-run_id isolation.

**Effort**: ~0.5 day deploy + run + evidence capture.

### B3 — Q4 final decision

With B1 + B2 evidence in hand, decide ADR-003 §Q4:

- **A (status quo)**: `_lhp_runs/{run_id}/` works for both jdbc_watermark_v2 and api_watermark_v2. No rename needed.
- **B (Hive partition)**: rename to `run_id={run_id}/`. Supersedes ADR-001 §Decision. Requires ADR-003 to flip from Proposed to Accepted with Alternative B selected.
- **C (drop landing tier for API)**: direct-write via `dlt.append_flow` if API extractor is Python-native. Partial supersede of ADR-001 (JDBC keeps landing; API doesn't). Divergent per-source shape.
- **D (per-source-type shapes)**: record as-is, don't unify. Lowest drift.

**Decision gate**: ADR-003 §Decision filled in, ADR-003 status flips to Accepted, CHANGELOG migration entry drafted.

**Effort**: ~0.5 day decision + doc. Synchronous review.

### Phase B exit criteria

- [ ] B1 `api_watermark_v2` generator shipped with full test coverage.
- [ ] B2 Wumbo bundle has one `api_watermark_v2` flowgroup running green end-to-end.
- [ ] B3 ADR-003 §Decision filled in, status = Accepted.

## Phase C — External ADLS + per-env catalog refactor

Goal: move from dev-tier (`main._landing.landing` managed volume + single-catalog `main`) to the production-shape that survives scale: external ADLS volumes per environment × medallion, `{env}_edp_{medallion}` catalog layout.

**Revision 2026-04-19 (post-A2/A3)**:
- **C1 SKIPPED** — production env (devtest workspace + shared qa/prod workspace) already has external ADLS-Gen2-backed UC volumes provisioned by the platform team. No Terraform/Bicep required from this fork.
- **C2 SHIPPED** as `Example_Projects/edp_lhp_starter/` (3 envs × 3 layers + JDBC watermark v2 + cross-catalog reads + per-env watermark registry; 5 e2e tests pass). Discovered LHP V0.8.2 already supplies everything needed: `WriteTarget.catalog` field, `WatermarkConfig.catalog/schema` fields, modern `${token}` substitution syntax, `source.catalog/schema` on delta sources. Plan's claim of a `source:` field gap was wrong on inspection.
- **C3 (ADR-004 watermark registry placement = Option B per env)** — codified in starter (each env's `${watermark_catalog}` resolves to `<env>_edp_orchestration`). ADR-004 author still TODO.
- **C4 (Q5 closure)** — A3 generator-side validator + starter-as-evidence covers the rule; ADR-003 §Q5 ready to flip to Closed once this branch merges.

### C1 — ~~External ADLS volume setup~~ (out of scope; provisioned externally)

**Workspace-admin scope**:

1. Create Azure Data Lake Storage Gen2 account(s) — typically one per environment.
2. Grant Databricks Unity Catalog managed identity `Storage Blob Data Contributor` on each container (`Agent 2 note: managed identity strongly recommended over service-principal for ADLS Gen2`).
3. Register UC storage credential pointing to the managed identity.
4. Register UC external locations per environment: `abfss://edp-dev@<account>.dfs.core.windows.net/` etc.
5. `CREATE EXTERNAL VOLUME {env}_edp_landing.landing.landing LOCATION '<adls-path>/landing'` (or per-medallion analogue).
6. Grant `READ, WRITE` on each external volume to the deploy service principal (`lhp-deploy-sp` today; productized SP per env going forward).

**Agent 2 verified**: external volumes sidestep `LOCATION_OVERLAP` because their storage is disjoint from managed-catalog roots. Empirically, this closes the Wumbo-discovered issue at production scale.

**Deliverable**: Terraform or Bicep module (pick one — this repo already has Azure idioms per memory) registering storage credential + external location + external volume per env.

**Effort**: ~2 days (terraform scaffold + validation per env) — environment-admin time.

### C2 — LHP substitution + DAB target model for per-env catalogs

**Expose substitutions** per Agent 2's validated pattern:

```yaml
# substitutions/dev.yaml
dev:
  bronze_catalog: devtest_edp_bronze
  silver_catalog: devtest_edp_silver
  gold_catalog: devtest_edp_gold
  landing_catalog: devtest_edp_landing     # NEW — replaces main._landing
  landing_schema: landing
  watermark_catalog: metadata                     # ADR-004 Option C (revised 2026-04-19; Option B per-env catalog rejected, see ADR-004 §Revision)
  watermark_schema: devtest_orchestration         # per-env schema in shared metadata catalog
```

**DAB target**:

```yaml
# databricks.yml
targets:
  dev:
    workspace:
      host: https://<dev-workspace>.databricks.com
    variables:
      env_catalog_prefix: devtest_edp
  tst: # analogous
  prod: # analogous
```

**LHP generator change**: Agent 2 flagged a feature gap — `source:` fields in LHP YAML don't support an optional `catalog:` override today. For cross-catalog silver→bronze reads under the new layout, this is required. Add to `src/lhp/models/config.py` + template renderer.

**Files**:
- `src/lhp/models/config.py` — `source.catalog: Optional[str]`.
- `src/lhp/generators/write/*` + `src/lhp/templates/write/*.j2` — emit `catalog.schema.table` in generated DLT code.
- `src/lhp/templates/bundle/pipeline_resource.yml.j2` — `catalog:` resolved from target-variable.

**Migration**: Wumbo bundle `databricks.yml` + `substitutions/*.yaml` + pipeline YAMLs updated to use new substitutions. Legacy single-catalog projects work unchanged (default token → current behavior).

**Effort**: ~3 days generator + ~1 day Wumbo migration.

### C3 — Watermark registry placement decision

**Agent 2 Option A vs B**:
- **A (platform-shared)**: `_platform.orchestration.watermarks` — one registry across all envs.
- **B (per-env)**: `{env}_edp_orchestration.watermarks` — isolation but lose cross-env query.

**Recommendation** (to validate): **Option B** (per-env). Reasoning:
- Aligns with Databricks best-practice "catalogs as primary unit of data isolation" (Agent 2).
- Deletion blast radius bounded per env.
- Cross-env comparisons are post-hoc analytical queries, not runtime-critical.

**Deliverable**: ADR-004 (separate ADR) capturing the decision + migration story.

**Effort**: ~1 day decision + ADR.

### C4 — Q5 full closure

With C1 + C2 + C3 shipped:

- Generator-side validation (A3) still applies; just operates on the new external-volume paths.
- LOCATION_OVERLAP concern is fully resolved (external volumes sidestep the check).
- Update ADR-003 §Q5 to Closed with evidence cross-refs to ADR-004 + the Terraform module + a Wumbo-analogue bundle running in the per-env catalog shape.

### Phase C exit criteria

- [~] C1 external volumes — out of scope (provisioned by platform team outside this fork).
- [x] C2 LHP per-env catalog convention + cross-catalog reads — `Example_Projects/edp_lhp_starter/` shipped with 5 e2e tests; LHP V0.8.2 already exposes everything required (no LHP code change needed beyond convention codification).
- [x] C3 ADR-004 (watermark registry placement = Option B per env) — `docs/adr/ADR-004-watermark-registry-placement.md` Accepted 2026-04-19.
- [~] C4 ADR-003 §Q5 closed with production-shape evidence — A3 generator-side validator (`LHP-CFG-018`) + starter project = full closure, ready to flip §Q5 to Closed once this branch merges.

## Phase D — Retention policy (opportunistic, independent)

**Refreshed 2026-04-20** (post Phase A + C closure, post-runbook live validation):

- Q4 closed at Alternative A (status quo) → retention targets remain `_lhp_runs/<run_id>/` directories under each landing volume.
- TWO retention concerns surfaced during live validation, not one:
  1. **Landing parquet directories** under `/Volumes/<env>_edp_landing/landing/landing/<source>/_lhp_runs/<uuid>/`. ~1 directory per source-table per extract run.
  2. **Watermark registry rows** in `metadata.<env>_orchestration.watermarks`. ~1 row per run (insert_new) regardless of result; runs retained indefinitely as audit trail. Devtest accumulated ~10 rows in 1 day during runbook iteration.
- Phase A2 added `landing_empty_schema_fallback` parquet writes — empty incrementals now also produce a `_lhp_runs/<uuid>/` directory with one part file. Retention pressure no longer scales only with non-empty extracts.
- Q4 (`_lhp_runs/` vs Hive partition) is closed at status-quo, so any reaper logic must understand `_lhp_runs/<uuid>/` shape, not `run_id=<uuid>/`.

Goal unchanged: stop `_lhp_runs/<uuid>/` directories from growing without bound. Goal extension: agree a separate operator contract for the `metadata.<env>_orchestration.watermarks` row-retention.

### D0 — Discovery (NEW, do first)

Sample current accumulation to size the problem before building.

- For one production-shape source (e.g. AdventureWorks Department in devtest), measure rate of new `_lhp_runs/<uuid>/` per day at typical schedule.
- Project to full prod load (count of source tables × extract cadence × 30/90/365 days).
- Decide whether the problem is "tens of thousands of subdirs by EOY" or "millions" — sets whether D2 reaper is mandatory or nice-to-have.
- Same for watermark rows. Per-row size is small but query plan cost on `get_latest_watermark` (filtered by composite key + ORDER BY watermark_time DESC LIMIT 1) grows with row count without liquid clustering refresh.

**Effort**: ~30 min query + 30 min napkin math. Output: a one-paragraph projection committed alongside D1.

#### D0 baseline (2026-04-20)

Pre-trigger snapshot taken before any prod-shape source is on cadence.

- **Devtest landing dirs** under `/Volumes/devtest_edp_landing/landing/landing/department/_lhp_runs/`: **0** (cleaned up post-runbook validation; landing dir does not currently exist — `databricks fs ls` returns `no such directory`).
- **Devtest watermark rows** at `metadata.devtest_orchestration.watermarks`: **0** (`SELECT COUNT(*)` returned 0; rows wiped post-validation). Table itself remains, schema intact (17 cols + liquid clustering on `(source_system_id, schema_name, table_name)` per ADR-004 §Implementation Status).
- **Per-extract growth model** (verified by live runbook 2026-04-20): each `databricks bundle run edp_bronze_jdbc_ingestion_workflow` produces exactly:
  - 1 new `_lhp_runs/<uuid>/` directory under the source's landing path (regardless of result — empty incrementals also write a 0-row schema-bearing parquet via the A2 fallback).
  - 1 new row in `metadata.devtest_orchestration.watermarks` (status `completed` on success, `failed` on extraction error, `landed_not_committed` on partial finalize crash).
  - 1 part file + `_SUCCESS` + `_committed_*` + `_started_*` markers per `_lhp_runs/<uuid>/`.
- **Cadence projection (placeholder)**: assuming a hypothetical `prod` shape of 50 source tables × 4 extracts/day each = 200 runs/day = **~73k landing dirs + 73k watermark rows per year per env**. With three envs (devtest + qa + prod), aggregate ~219k of each per year. Above the 10⁴ benchmark threshold in D3 by EOY year-1; reaper (D2) becomes mandatory, not nice-to-have, before that point. Numbers are placeholder — tighten with real source list once defined.

**Trigger to refresh this baseline**: first time a prod-shape source runs in devtest on a recurring schedule (e.g. hourly). Re-run D0 measurements after 7 days, project from observed rate, then move to D1 contract drafting.

### D1 — Operator contract

Ratify two contracts:

**Landing parquet retention**:
- Recommend "keep last 30 days" per source. Aligns with typical incremental-load reprocessing windows; sufficient to recover from a bronze table corruption requiring N days of replay.
- Two implementation tiers (verify DBR support before committing):
  - **Tier 1 (`cloudFiles.cleanSource`)**: set `cloudFiles.cleanSource = DELETE` + `cleanSource.retentionDuration = 30 days` in generator-emitted bronze AutoLoader options. Required DBR version: confirm against current Databricks docs (was DBR 16.4+ at original draft). Devtest pipelines run serverless `channel: CURRENT` — verify whether that resolves to a version supporting `cleanSource`.
  - **Tier 2 (reaper job)**: for any pipeline whose runtime does not support `cleanSource`, a scheduled Databricks job runs daily, queries `cloud_files_state` TVF for `commit_time < NOW() - retention`, and deletes corresponding `_lhp_runs/<uuid>/` subdirs via UC Files API. One reaper job per bundle target.

**Watermark row retention**:
- Recommend "keep last 90 days" of completed rows + indefinite retention of rows with status ∈ {`failed`, `landed_not_committed`} for audit. Failed/landed-not-committed are operator-investigation surface; auto-deleting them hides incidents.
- Implementation: a separate scheduled job (or same reaper) issues `DELETE FROM metadata.<env>_orchestration.watermarks WHERE status='completed' AND completed_at < NOW() - INTERVAL 90 DAYS`. Run weekly.
- Tradeoff: under-90-day deletes wipe rows that the recovery path (`get_recoverable_landed_run`) would otherwise pick up. Recovery only consults rows in `landed_not_committed`, so completed rows are safe to delete; sanity-check this before shipping.

### D2 — Implementation

- **LHP generator**: emit `cleanSource.*` options conditionally behind a bundle-level opt-in flag. Default off so existing bundles do not silently start deleting landing data on first re-deploy. Add `lhp_retention.cloud_files_clean_source: { enabled: bool, retention_days: int }` to project / pipeline config.
- **Reaper job**: new LHP CLI subcommand `lhp generate-reaper --env <target>` emits a Databricks Workflow resource YAML that owns the retention logic (both landing parquet AND watermark rows). Single deploy unit per env. Schedule: nightly at 02:00 UTC by default, configurable.
- **Generator-side guard**: refuse to enable Tier 1 cleanSource for a source that also has a reaper job emitted — they would race. Mutually exclusive at generation.

### D3 — Benchmark at scale

Before declaring Q2 closed, run a synthetic test:

- Generate 10^4 `_lhp_runs/<uuid>/` subdirs under one landing volume.
- Measure AutoLoader directory-listing latency with/without `cloudFiles.useIncrementalListing = true`.
- Measure reaper throughput (subdirs/min via UC Files API).
- For watermark table: insert 10^5 historical rows, measure `get_latest_watermark` p99 with + without recent `OPTIMIZE` runs on the liquid-clustering keys.
- Document results inline in Phase D log + cite from ADR-003 §Q2 closure.

No Databricks-published benchmarks exist for this regime — establish own baseline.

### Phase D exit criteria

- [ ] D0 accumulation rate measured + projected (devtest baseline).
- [ ] D1 landing parquet contract + watermark row contract agreed + documented.
- [ ] D2 LHP generator emits `cleanSource.*` opt-in + `lhp generate-reaper` ships; mutual-exclusion guard in place.
- [ ] D3 scale benchmark published (landing listing + reaper throughput at 10^4 subdirs; `get_latest_watermark` p99 at 10^5 rows).
- [ ] ADR-003 §Q2 closed.

### Sequencing notes

- D0 + D1 can run in parallel (D0 measures, D1 drafts).
- D2 depends on D1 (need contract before implementing).
- D3 depends on D2 (need reaper to benchmark its throughput).
- All of Phase D is independent of B / C work; can ship whenever a real growth signal appears (recommend triggering D0 once a real prod-shape source is on incremental cadence in devtest).

## Summary — sequencing

| Phase | Status | Blocks | Notes |
|---|---|---|---|
| A (parallel unblockers) | **CLOSED** 2026-04-20 — A2 + A3 code shipped + live-validated against devtest workspace; A1 deferrable | nothing | merged via PRs #6, #7, #10 |
| B (API source + Q4) | **DEFERRED** 2026-04-19 — no API-ingestion consumer in scope | nothing (Q4 stays on Alternative A) | blueprint preserved at `b1-api-watermark-blueprint.md` |
| C (external ADLS + refactor) | **CLOSED** 2026-04-20 — C1 platform-provisioned; C2 starter shipped + live-validated; C3 ADR-004 Option C accepted; C4 ADR-003 §Q5 closed | nothing | merged via PRs #8, #9, #10 |
| D (retention) | **NEXT** — refreshed 2026-04-20; awaits real growth signal in devtest | ADR-003 §Q2 closure | ~3-5 days incl. D0 measurement |

**Critical sequence**: Phase A + Phase C closed 2026-04-20. Phase B deferred indefinitely. Phase D awaits growth signal.

**Updated kick-off order (2026-04-20, post Phase A + C closure)**:
1. ~~A1 ask today (async).~~ Deferrable.
2. ~~A2 (empty-batch).~~ **Closed** — `08fe67a1` shipped + live-validated 2026-04-20 (sentinel watermark forced empty incremental, A2 fallback wrote 0-row schema-bearing parquet at `_lhp_runs/local-f7816a34-.../`).
3. ~~B1 (API generator) in parallel with A3.~~ B1 deferred; A3 **closed** — `49fe740a` + dict-fix from PR #10 + live-validated 2026-04-20 (`LHP-CFG-018` fired against misconfigured starter pipeline).
4. ~~B2/B3.~~ Deferred with B1.
5. ~~PR review + merge of A2 then A3.~~ Done (PRs #6, #7, #8, #9, #10 merged).
6. ~~Phase C kickoff.~~ Done — C1 platform-provisioned externally; C2 EDP starter shipped + live-validated against devtest workspace `dbc-8e058692-373e` (16/16/6 row pass); C3 ADR-004 accepted with Option C revision (shared `metadata` catalog, per-env schema); C4 ADR-003 §Q5 closed.
7. **Phase D NEXT** — refresh complete 2026-04-20. Trigger D0 measurement once a real prod-shape source is on incremental cadence in devtest. D2 implementation work (~3-5 days) opportunistic until then.

## Out of scope

- **Upstream contribution**: once the API source type + external-volume pattern settles, a Mode A (simple inline SQL watermark) contribution to upstream (Mmodarre/Lakehouse_Plumber issue #65) becomes a separate sequenced ADR.
- **OAuth / SSO for API source type**: B1 uses bearer tokens only.
- **Schema-evolution hardening for `cloudFiles.schemaHints`**: Phase D once we have per-source scale signal.
- **Parallel pagination for API source**: B1 is sequential-cursor; parallel is a Phase E item.
- **Cross-workspace bundle promotion recipes**: C2 lays the substrate; the promotion runbook is downstream.
