# ADR Input — TD-007: JDBC Watermark Parquet-Post-Write Stats

**Status**: ADR input artifact (pre-draft). Feeds a forthcoming `ADR-001-jdbc-watermark-parquet-post-write-stats.md` under `docs/adr/`.

**Origin**: Stale review finding TD-007 proposed fixing a double-JDBC-read + watermark divergence bug. On re-analysis (2026-04-18), the bug was already resolved on branch `watermark` under Phase 2 of the hardening ROADMAP (plan `02-02: Rework jdbc_watermark_job.py.j2 and generator context for run-scoped landing`). This document retroactively captures the problem, the alternatives considered, and the rationale for the pattern that shipped — so that the architectural decision is recorded rather than implied by commits.

**Consumer**: ADR author. Use this document's Problem, Alternatives, Decision, Rationale, and Consequences sections as the skeleton of ADR-001.

---

## Problem

The first-cut `jdbc_watermark_v2` extraction template issued two independent JDBC reads against the source inside a single notebook:

1. `df.agg(F.count("*"), F.max(col)).first()` — computed `row_count` and `new_hwm`.
2. `df.write.mode("append").format("parquet").save(landing_path)` — landed rows to a shared path.

There was no persistence between reads. If the source table mutated between the two queries, the watermark row committed by `mark_complete(new_hwm, row_count)` did not describe the Parquet bytes that actually landed. The inline comment at the time read "single pass (serverless-compatible, no df.cache)" — a claim based on folklore about serverless Spark rather than a measured constraint. The shared `landing_path` also violated Constitution P6 (retry safety by construction): two task retries could write overlapping files into the same directory.

The original L2 SRS baseline (`.specs/jdbc-watermark-v2/L2-srs.md`, 2026-04-14) encoded this pattern in **FR-05** (append mode) and **FR-06** (pre-write agg). Assumption **A5** explicitly claimed `df.count()` after read would not refetch — incorrect for uncached Spark DataFrames.

Slice A hardening (`.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md`, 2026-04-17) acknowledged the gap in **FR-L-10** and waived Constitution P6 temporarily (**W-A-01**) on the understanding that Slice B would close it.

## Alternatives Considered

Five candidate fixes were evaluated against Constitution P3 (thin notebook), P6 (retry safety), P9 (SQL safety), and the operational reality that LHP targets both Databricks serverless and job compute.

### A. `df.cache()` + `df.count()` kickstart before write

Cache the DataFrame after JDBC read, force materialization with a count, then run agg and write against the cached plan. Single JDBC read.

- **Rejected.** Relies on DataFrame caching fitting in cluster memory. Serverless Spark supports caching but with memory pressure limits that are hard to predict ahead of time. Worse, this alternative keeps the shared `landing_path` — Constitution P6 still violated. Partial fix dressed as a solution.

### B. JDBC `REPEATABLE READ` transaction

Open a single JDBC transaction with `REPEATABLE READ` isolation, span both reads. Source DB guarantees snapshot consistency.

- **Rejected.** Pushes complexity onto source DBAs (per-engine transaction semantics: Postgres differs from Oracle differs from SQL Server). Pins a snapshot on the source DB for the duration of the whole extract — increased source load, longer lock windows. Still leaves two separate JDBC payload transfers. Still doesn't address the retry-safety problem.

### C. Single-pass write-with-accumulator

Compute `row_count` and `max(col)` inside the write by wiring Spark accumulators or `foreachPartition`. One JDBC read.

- **Rejected.** Hand-rolled correctness. Bypasses the Spark optimizer. Fragile under partition restart. Template (or runtime) carries logic that the framework exposes natively elsewhere. Violates Constitution P3 — business logic lands in the template or in an ad-hoc helper.

### D. `df.checkpoint()` to DBFS

Materialize the DataFrame to durable storage after read, then agg + write against the checkpoint. Serverless-compatible because it's disk-backed.

- **Rejected.** Every byte gets written twice — once to the checkpoint, once to the final landing Parquet. The landing Parquet already provides durable materialization; checkpointing is the same idea with extra cost.

### E. Derive stats from Parquet after the write — the chosen direction

Write JDBC to a **run-scoped** Parquet subdirectory. Scan that subdirectory for `count` and `max` after the write succeeds. One JDBC read total. Stats describe landed bytes by construction. Run-scoped subdir satisfies Constitution P6 — a retry writes into a different run's subdir and never mutates prior runs.

- **Accepted.** Zero divergence. One JDBC read. Identical code path on serverless and job compute. Parquet footer statistics make `count(*)` and `max(col)` on primitive watermark types (timestamp, bigint, decimal) near-free to read back.

### F. Bound divergence + alert (defensive overlay)

Keep two reads, compare agg count against a post-write Parquet scan, alert if delta exceeds a threshold. Records both values in the watermark row.

- **Rejected.** Guardrail for a problem alternative E eliminates. Adds observability surface area for a divergence that should not be possible at all.

## Decision (Shipped on `watermark`, Phase 2)

Alternative E, with one addition the original proposal missed: an intermediate `LANDED_NOT_COMMITTED` state backed by `WatermarkManager.mark_landed()`, reconciled on next run by `get_recoverable_landed_run()`.

Concrete artifacts as of `watermark` HEAD (commit `8cb6c847`, 2026-04-18):

- `src/lhp/templates/load/jdbc_watermark_job.py.j2` writes via `df.write.mode("overwrite").format("parquet").save(run_landing_path)` where `run_landing_path = f"{root}/_lhp_runs/{run_id}"` (see the template's `_run_landing_path` helper).
- Post-write, `spark.read.parquet(run_landing_path).agg(F.count("*"), F.max(col)).first()` derives `row_count` and `new_hwm` from the landed bytes.
- `WatermarkManager.mark_landed(run_id, watermark_value, row_count)` commits the intermediate state before `mark_complete` finalizes.
- On a subsequent run, `WatermarkManager.get_recoverable_landed_run(source_system_id, schema_name, table_name)` returns any prior `landed` row; if present, the notebook finalizes it with `mark_complete` and exits via `dbutils.notebook.exit()` without reopening JDBC — recovers a crashed prior run without data re-fetch.
- Tests confirm the contract: `tests/unit/generators/load/test_jdbc_watermark_job.py::test_extraction_notebook_contains_landing_path` asserts `_lhp_runs` appears in generated notebook text; `test_extraction_notebook_contains_recovery_hooks` asserts `get_recoverable_landed_run(`, `mark_landed(`, and `dbutils.notebook.exit(` all appear.

### Deliberate divergences from the original TD-007 proposal

| TD-007 proposal | Shipped impl | Why shipped wins |
|---|---|---|
| `run_id={run_id}/` Hive-style subdir | `_lhp_runs/{run_id}/` flat subdir under `_lhp_runs/` | LHP-owned namespace prefix keeps the landing root shareable with non-LHP Parquet without colliding on `run_id=` partition reads. Downstream cloudFiles reads the landing root with a glob, not with partition-pruning on `run_id`. |
| No intermediate state; single `mark_complete` call after agg | `mark_landed` → `mark_complete` two-phase | Matches the `LANDED_NOT_COMMITTED` recovery protocol Constitution P6 mandates. A crash between landing and finalization is recoverable on next run without re-fetching from JDBC. Proposal missed this. |
| `df.write.mode("overwrite")` motivated by retry idempotency on same run_id | `mode("overwrite")` still used, but primary retry-safety comes from distinct run_ids per attempt | `derive_run_id` (per FR-L-09) increments attempt number, so each Workflow task retry has its own run_id and its own subdir. `overwrite` is belt-and-suspenders for the local-dev fallback case where `run_id` is a stable UUID. |

## Rationale

1. **Correctness by construction.** The watermark row describes bytes that have already been committed to Parquet, not bytes that might later be written. No snapshot drift is possible. Assumption A5 (Spark implicit caching) is replaced by a property — stats come from the storage tier, not the compute tier.
2. **Constitution P3 (thin notebook) upheld.** The notebook contains control flow and a single post-write Parquet scan — no business logic, no conditional branching on source types, no string-built SQL beyond the validated JDBC subquery builder.
3. **Constitution P6 (retry safety by construction) upheld.** Run-scoped subdirs guarantee that retries never mutate prior runs. The waiver W-A-01 expires with this change.
4. **Serverless and job compute identical.** No cache means no cache memory-pressure surprises. Same template, same code path, regardless of compute configuration.
5. **Parquet footer stats make the post-write scan cheap.** For timestamp, bigint, and decimal watermark columns, `count(*)` is metadata-only and `max(col)` uses Parquet column statistics. The extra round-trip is dominated by Delta metadata overhead, not data scan.
6. **Recovery extends the value.** The `mark_landed` + `get_recoverable_landed_run` protocol means a crash after landing but before finalization is recoverable without JDBC re-fetch — a property the original proposal did not enumerate but which the spec (Constitution P6, Phase 2 ROADMAP) demanded.

## Consequences

### Positive

- One JDBC read per extraction. Source-DB load halved vs. original template.
- Watermark row provably describes landed data. Dashboards built on the watermark table no longer require "did the stats actually match the files?" caveats.
- Workflow task retries are idempotent: each attempt's `run_id` owns a distinct subdir, writes never collide.
- Partial-failure recovery without re-opening JDBC. If the landing write succeeds but `mark_complete` fails (network blip, cluster eviction), the next run finalizes the landed batch from the watermark row's `landed` state.

### Negative

- Landing path shape is `{root}/_lhp_runs/{run_id}/` — not a flat directory. Downstream cloudFiles readers must glob the `_lhp_runs/` subtree. (Verified: existing `CloudFilesLoadGenerator` emits a schema location under `{root}/_lhp_schema/...` and a glob that traverses the run subdirs.)
- Accumulating run-scoped subdirs. Retention and vacuum policy is an operational concern not covered by this decision. Flagged as a follow-up for the ops runbook.
- One extra Parquet scan round-trip per run (footer stats only). Benchmarked in Phase 4 (`tests/scripts/test_benchmark_jdbc_watermark_v2.py`).

### Neutral / Observational

- `NFR-02` (repartition(1) below 1M rows, repartition(4) above) no longer has a pre-write `row_count` to branch on. Current template defaults to the simpler repartition decision; dynamic tuning is deferred to v3.
- Assumption A5 from the original L2 SRS is now factually wrong; it should be annotated as superseded rather than silently left in the spec.

## Follow-Ups Required

1. **Write the ADR itself**: `docs/adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md`. Reuse the Problem / Alternatives / Decision / Rationale / Consequences sections above.
2. **Spec deltas**:
   - Amend `.specs/jdbc-watermark-v2/L2-srs.md` §3 **FR-05** to state `df.write.mode("overwrite").format("parquet").save("{landing_path}/_lhp_runs/{run_id}")` and remove the "append to shared path" language.
   - Amend **FR-06** to describe post-write Parquet scan as the source of `row_count` and `new_hwm`, with explicit handoff to `mark_landed` → `mark_complete`.
   - Amend **NFR-02** to drop the pre-write-count-based repartition heuristic.
   - Annotate **A5** as superseded: implicit caching was never a correct assumption; the post-write scan makes the question moot.
3. **Slice A waiver W-A-01** is now dischargeable. Update `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md` §6.2 to mark the waiver as discharged by Phase 2 plan 02-02 landing.
4. **Databricks validation** (path α): run live verification against dev workspace via profile `dbc-8e058692-373e` to confirm behavior matches the structural claim:
   - cloudFiles glob resolves landing subtree including nested `_lhp_runs/{run_id}/`.
   - Workflow task retry produces distinct run_ids and distinct subdirs.
   - Backfill at 10M+ rows does not regress vs. legacy append pattern.
   - `get_recoverable_landed_run` actually recovers a simulated mid-finalization crash.
5. **Close TD-007** with evidence: link this document, link Phase 2 plan 02-02, link commit shipping the template change, link the two generator tests asserting `_lhp_runs` and recovery-hook presence.

## Cross-References

- Constitution: `.specs/constitution.md` — principles P3, P5, P6, P9, P10 are the governing gates.
- Slice A L2: `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md` — FR-L-10 (acknowledged gap), §6.2 W-A-01 (waiver now dischargeable).
- Baseline L2: `.specs/jdbc-watermark-v2/L2-srs.md` — FR-05, FR-06, NFR-02, A5 require amendment per follow-up #2.
- ROADMAP: `.planning/ROADMAP.md` — Phase 2 "Harden Extraction Lifecycle" plan 02-02 landed this change.
- Template: `src/lhp/templates/load/jdbc_watermark_job.py.j2` — current implementation.
- Tests: `tests/unit/generators/load/test_jdbc_watermark_job.py` — `test_extraction_notebook_contains_landing_path`, `test_extraction_notebook_contains_recovery_hooks`.
