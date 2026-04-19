# ADR-001 — JDBC Watermark v2: Parquet-Post-Write Stats + Run-Scoped Landing

- **Status**: Accepted
- **Date**: 2026-04-18
- **Deciders**: dwtorres@gmail.com
- **Supersedes**: N/A (first ADR in this repository)
- **Supersedes by**: —
- **Related**: ADR-002 (pending) — LHP runtime-availability pattern on Databricks; TD-007 (resolved by this decision); TD-008 (opened for runtime-availability follow-up)

## Context

The first-cut `jdbc_watermark_v2` extraction template (L2 baseline SRS, 2026-04-14) issued **two independent JDBC reads** inside one notebook:

1. `df.agg(F.count("*"), F.max(col)).first()` — captured `row_count` and `new_hwm`.
2. `df.write.mode("append").format("parquet").save(landing_path)` — landed rows to a shared path.

No DataFrame persistence sat between the two reads. An inline comment justified the design as "single pass (serverless-compatible, no df.cache)" — folklore, not a measured constraint. Two operational problems followed:

1. **Watermark divergence.** If the source table mutated between the two queries, the watermark row committed by `mark_complete(new_hwm, row_count)` did not describe the bytes actually landed in Parquet. The L2 baseline's Assumption **A5** ("`df.count()` before write does not trigger a second full JDBC fetch") was materially wrong — uncached Spark DataFrames re-evaluate on every action.
2. **Retry non-safety.** Workflow task retries appended to the same shared `landing_path`. Constitution **P6** ("Retry safety by construction. Landing paths are run-scoped: `.../run_id=<uuid>/`") was violated by default.

Slice A hardening (2026-04-17) acknowledged the gap in **FR-L-10** and explicitly waived P6 (**W-A-01**) on the understanding that Slice B would close it.

Five candidate fixes were evaluated. See `docs/adr/inputs/td-007-jdbc-watermark-parquet-stats.md` for the full alternatives table.

## Decision

Adopt **Alternative E — derive stats from Parquet after the write, to a run-scoped subdirectory**, extended with an intermediate `LANDED_NOT_COMMITTED` state to support mid-finalization recovery without reopening JDBC.

Concrete contract of the shipping artifact ([src/lhp/templates/load/jdbc_watermark_job.py.j2](../../src/lhp/templates/load/jdbc_watermark_job.py.j2)):

- Write the JDBC DataFrame with `.mode("overwrite").format("parquet").save(run_landing_path)` where `run_landing_path = f"{landing_root}/_lhp_runs/{run_id}"`.
- Derive `row_count` and `new_hwm` by reading the landed Parquet back: `spark.read.parquet(run_landing_path).agg(F.count("*"), F.max(col)).first()`.
- Commit the run lifecycle as `insert_new` → write → `mark_landed(run_id, watermark_value, row_count)` → `mark_complete(run_id, watermark_value, row_count)`.
- On process-level failure between `mark_landed` and `mark_complete`, a subsequent run calls `WatermarkManager.get_recoverable_landed_run(source_system_id, schema_name, table_name)`, receives the landed-but-not-committed row, and finalizes it via `mark_complete` — no JDBC reopen. The template short-circuits via `dbutils.notebook.exit` after recovery.
- Run-scoped subdirs satisfy Constitution P6: distinct Workflow task attempts produce distinct `run_id`s (via `derive_run_id`, FR-L-09), hence distinct subdirs; `mode("overwrite")` bounds any within-run retry to the single owning subdir.

### Out of scope for this ADR

- **Runtime-availability pattern** (how `lhp.extensions.*` reaches the notebook at import time). Current workflow template emits `environments.dependencies: [${var.lhp_whl_path}]`, which conflicts with Constitution P2 ("Generated notebooks import `lhp.extensions.*` from PYTHONPATH, not from an installed wheel"). This drift is captured as **TD-008** and will be resolved by **ADR-002**. The TD-007 Phase B validation used a per-user Databricks Git folder plus a `sys.path` bootstrap in the validation notebook — a validation convenience, not a production-architecture claim.
- `compute_mode` YAML flag (serverless vs. job compute) — orthogonal runtime tuning concern, not required by this decision.
- Landing-path retention / vacuum policy — operational concern, not a correctness concern.

## Alternatives Considered

| ID | Alternative | Verdict | Reason |
|---|---|---|---|
| A | `df.cache()` + `df.count()` kickstart | Rejected | Cache memory-pressure unpredictable on serverless; still leaves shared landing path, P6 still violated. |
| B | JDBC `REPEATABLE READ` transaction | Rejected | Pushes isolation semantics onto source DBAs (dialect-specific); still two JDBC payloads; doesn't address retry safety. |
| C | Single-pass accumulator write | Rejected | Hand-rolled correctness; bypasses Spark optimizer; fragile under partition restart. |
| D | `df.checkpoint()` to DBFS | Rejected | Every byte written twice; landing Parquet already provides durable materialization. |
| E | **Parquet post-write stats + run-scoped subdir** | **Accepted** | Zero divergence by construction; one JDBC read; identical on serverless and job compute; composes with `mark_landed` recovery. |
| F | Divergence-bound + alert overlay | Rejected | Guardrail for a problem E eliminates. |

## Consequences

### Positive

- One JDBC read per extraction. Source-DB load halved vs. original template.
- Watermark row provably describes landed data (empirically validated — Phase B V6, V7).
- Workflow task retries are idempotent by `run_id` discrimination (V5).
- Partial-failure recovery without JDBC re-fetch (V7).
- Identical code path on serverless and job compute. No `df.cache()` debate.
- Post-write Parquet scan is cheap for primitive watermark types (footer statistics). Phase A V3 measured 0.16s for a 1M-row scan on laptop Spark.

### Negative / Observational

- Landing path shape is `{root}/_lhp_runs/{run_id}/` — downstream cloudFiles readers must traverse the `_lhp_runs/` tier. Phase B V8 confirmed `{root}/_lhp_runs/*` glob reads all runs correctly on Unity Catalog Volumes; `recursiveFileLookup=true` at the bare root did not (observed `UNABLE_TO_INFER_SCHEMA` on the first Phase B attempt). This is an observation about the correct read pattern, not a defect; the existing `CloudFilesLoadGenerator` should be spot-checked to confirm its emitted read path is compatible.
- Run-scoped subdirs accumulate. Retention / vacuum policy is operationally required but out of scope for this ADR.
- One extra Parquet scan round-trip per run. Cost is dominated by Parquet footer metadata, not data scan.
- `NFR-02` (repartition decision based on pre-write `row_count`) no longer has a pre-write count to branch on. Current template defaults to a single repartition choice; dynamic tuning deferred to v3.

## Evidence

### Phase A — local PySpark harness

`scripts/validation/validate_td007_local.py` against a laptop SparkSession, temp directories, no cluster.

| ID | Invariant | Verdict | Duration |
|---|---|---|---|
| V1 | Run-scoped write + readback | PASS | 2.17 s |
| V2 | Post-write stats match input (10k rows) | PASS | 0.56 s |
| V3 | Post-write `agg(count, max)` latency on 1M rows ≤ 1s budget | PASS (measured 0.16 s) | 0.88 s total |
| V4 | Empty-batch Parquet dir is readable | PASS | 0.76 s |

Machine-readable evidence: [scripts/validation/td007_phase_a_evidence.json](../../scripts/validation/td007_phase_a_evidence.json)
Narrative evidence: [scripts/validation/td007_phase_a_evidence.md](../../scripts/validation/td007_phase_a_evidence.md)

### Phase B — Databricks serverless Jobs

`scripts/validation/validate_td007_databricks.py` against `main.orchestration.watermarks` + Volume `/Volumes/main/bronze/landing/td007_validation` on profile `dbc-8e058692-373e`, Environment v2 serverless.

| ID | Invariant | Verdict | Duration |
|---|---|---|---|
| V5 | 3 runs → 3 distinct `_lhp_runs/{run_id}/` subdirs; row counts 100/250/50 | PASS | 28.64 s |
| V6 | `insert_new` → write → `mark_landed` → `mark_complete` commits expected state (500 rows, hwm `2023-11-16 02:08:19`) | PASS | 34.53 s |
| V7 | Recovery: `mark_landed` only → next call's `get_recoverable_landed_run` returns row → `mark_complete` finalizes without JDBC reopen (300 rows) | PASS | 20.36 s |
| V8 | `{root}/_lhp_runs/*` glob read: expected 200, observed 200; per-run 120/80 | PASS | 8.64 s |

Job run: `281471321334282`, task `873933535937682`.
Machine-readable evidence: [scripts/validation/td007_phase_b_evidence.json](../../scripts/validation/td007_phase_b_evidence.json)
Narrative evidence: [scripts/validation/td007_phase_b_evidence.md](../../scripts/validation/td007_phase_b_evidence.md)

### Overall

All eight invariants (V1–V8) **PASS**. Decision E is empirically validated on both laptop Spark 4.1.1 and Databricks serverless Jobs env v2 against a production-shaped watermark Delta table.

## Constitution Conformance

| Principle | Status under this decision |
|---|---|
| P1 (Additive over rewrite) | Conformant. The decision is a template/generator change within an existing source type; no new `_v3` emerges. |
| P2 (No wheel until justified) | **Unchanged by this ADR**. The current wheel-based runtime availability is a separate concern; see TD-008 / ADR-002 (pending). |
| P3 (Thin notebook, fat runtime) | Conformant. Recovery logic lives in `WatermarkManager`; the template contains control flow + a post-write Parquet scan, not business logic. |
| P4 (FlowGroup = Entity) | Unaffected. |
| P5 (Registry is SoT) | Conformant. `mark_landed` + `mark_complete` keep state in the registry; lookup filters on terminal-success. |
| P6 (Retry safety by construction) | **Conformant via this decision.** Run-scoped landing plus `overwrite` on per-run subdir. Discharges Slice A waiver W-A-01 in full. |
| P7 (Declarative tuning passthrough) | Unaffected. |
| P8 (UC + secret scopes) | Unaffected. |
| P9 (SQL safety) | Unaffected. |
| P10 (Human validation at boundaries) | Conformant. This ADR is that human-approved validation record. |

## Follow-Ups

1. **Spec amendments** (same PR as this ADR):
   - `.specs/jdbc-watermark-v2/L2-srs.md` §3 **FR-05**: update to `mode("overwrite")` on `{landing_path}/_lhp_runs/{run_id}` instead of shared `append`.
   - §3 **FR-06**: rewrite to describe post-write Parquet scan as stats source plus `mark_landed` → `mark_complete` handoff.
   - §4 **NFR-02**: drop pre-write-count-based repartition heuristic; acknowledge simpler default.
   - §8 **A5**: annotate as superseded by this ADR.
   - `.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md` §6.2 **W-A-01**: mark waiver discharged by Phase 2 plan 02-02 landing.
2. **TD-008** opened: runtime-availability pattern violates Constitution P2 (current generator emits `lhp_whl_path` wheel dependency). ADR-002 will pick between git-folder-with-sys.path-prelude, DAB-synced-workspace-files, and per-bundle vendoring.
3. **CloudFiles read path**: spot-check `CloudFilesLoadGenerator` output for a `jdbc_watermark_v2` flowgroup to confirm the generated DLT pipeline reads the landing subtree in a way compatible with `{root}/_lhp_runs/*`. Phase B V8 showed bare `recursiveFileLookup=true` at the landing root fails on UC Volumes; the fix is a glob-aware read path.
4. **Landing retention / vacuum policy**: opened as an ops-runbook concern, not a code change. Follow-up issue to be filed.

## Cross-References

- Input analysis: [docs/adr/inputs/td-007-jdbc-watermark-parquet-stats.md](./inputs/td-007-jdbc-watermark-parquet-stats.md)
- Constitution: [.specs/constitution.md](../../.specs/constitution.md) — P2, P3, P6
- Baseline L2: [.specs/jdbc-watermark-v2/L2-srs.md](../../.specs/jdbc-watermark-v2/L2-srs.md) — FR-05, FR-06, NFR-02, A5 (amendment targets)
- Slice A L2: [.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md](../../.specs/jdbc-watermark-v2/hardening/slice-A-lifecycle/L2-srs.md) — FR-L-10 (acknowledged gap), W-A-01 (waiver now dischargeable)
- ROADMAP: [.planning/ROADMAP.md](../../.planning/ROADMAP.md) — Phase 2 plan 02-02 (change landed 2026-04-18)
- Shipping template: [src/lhp/templates/load/jdbc_watermark_job.py.j2](../../src/lhp/templates/load/jdbc_watermark_job.py.j2)
- Phase A harness: [scripts/validation/validate_td007_local.py](../../scripts/validation/validate_td007_local.py)
- Phase B harness: [scripts/validation/validate_td007_databricks.py](../../scripts/validation/validate_td007_databricks.py)
- Phase B runbook: [scripts/validation/README_phase_b.md](../../scripts/validation/README_phase_b.md)
