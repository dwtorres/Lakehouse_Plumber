# LHP Enterprise JDBC Execution — Spike Plan

## Problem Statement

How might we execute 5000+ JDBC table loads through LHP-generated artifacts
without an unmanageable Databricks Jobs UI, while preserving L2 §5.3 per-table
watermark correctness, enabling continue-on-failure, and supporting granular
rerun and alerting?

## Recommended Direction

Spike two execution models in parallel before committing code. Abandon the
original single thread-pool task design — it does not scale to 5k tables and
forfeits native Databricks retry/continue semantics.

**Direction A (SDP-native dynamic flows):** Tables become dynamically-generated
flows inside one Lakeflow SDP pipeline per load group. Workflow shrinks to a
single trigger task. SDP owns concurrency, retry, continue-on-failure. Manifest
becomes a materialized view over the pipeline event log joined with the
existing `jdbc_watermark_registry`.

**Direction B (`for_each_task` native):** One `for_each` iterates over manifest
rows; each iteration runs the existing `jdbc_watermark_job.py.j2` notebook
unchanged. `continue_on_error: true`, concurrency capped per Databricks limits.
L2 §5.3 recovery contract migrates zero code.

Decision at end of week: A wins if viable, B wins if A fails hard, C (bounded
for_each + inner pool) only if both fail.

## Spike Execution

- **Driver:** Claude Code sessions + user validation on Databricks devtest workspace
- **Cadence:** 2 sessions per spike
  - Session 1: scaffold + 1 table end-to-end
  - Session 2: scale to 100 flows + inject failure
- **Decision window:** 1 week total, parallel tracks
- **Tie-break rule:** A wins if both pass; B wins only if A fails hard (e.g.,
  SDP flow ceiling <100, or JDBC side-effect writes to watermark registry
  blocked inside flow body)
- **Proof scale:** 100 flows sufficient signal; full 5000-table validation is
  post-decision operational test

## Key Assumptions to Validate

### Direction A

- [ ] SDP supports 100 dynamically-generated flows in one pipeline
      — measure: init time, run time, resource usage
- [ ] SDP flow body permits JDBC batch read + Delta side-effect write to
      watermark registry (`insert_new` / `mark_landed` / `mark_failed`)
      — test: single flow end-to-end
- [ ] `continue_on_failure` isolates per-flow errors
      — test: inject exception in 1 flow, verify other 99 complete
- [ ] Pipeline-level JDBC driver lib attach works (Oracle `ojdbc8` via maven)
      — test: load 1 Oracle table through SDP pipeline

### Direction B

- [ ] `for_each_task` concurrency ≥100 sufficient for SLA
      — verify Databricks docs + measure actual cap
- [ ] Per-iteration task startup overhead fits run window
      — measure: time 100 iterations, extrapolate to 5000
- [ ] Per-iteration cost acceptable vs shared cluster
      — price comparison
- [ ] No total-iteration cap blocks 5000
      — doc + empirical check

## MVP Scope (per spike)

**In scope:**

- 100-table synthetic JDBC source (AdventureWorks or generated fixture)
- 1 load group, 1 execution profile, 1 compute profile
- Existing `jdbc_watermark_registry` + L2 §5.3 contract preserved
- End-to-end: prepare manifest → execute → validate
- Failure injection: 1 table deliberately fails, verify others continue
- Rerun-failed semantics verified once after failure

**Out of scope (spike phase):**

- Multi-source load groups
- Multi-environment promotion
- HIPAA hashing in copydown
- Alerting wiring (prove feasibility only)
- Dashboard build
- Full 5000-table scale test (100 proves scaling path)

## Not Doing (and Why)

- **V3 single thread-pool task** — does not scale to 5k, loses native retry,
  custom state machine to write and maintain
- **Threshold split (small vs large sources)** — two code paths, cognitive
  tax, explicitly ruled out in key design decisions
- **Manifest-as-queue with long-running daemon** — heavier infra than batch
  cadence needs
- **DLT-META adoption now** — deferred to side-by-side future evaluation
- **Direction C (bounded for_each + inner pool)** — fallback only, two-tier
  failure boundary compounds recovery complexity
- **5000-table scale validation during spike** — 100 flows proves the model;
  5000 is an operational test after direction committed

## Open Questions (resolve before implementation)

- Manifest write pattern: insert-per-table-per-run (append) OR upsert active
  + separate run history? Recommendation: **run-scoped insert**, rerun query
  becomes `WHERE run_id = :target AND status = 'failed'`. Confirm with user.
- Manifest table catalog + schema placement. Proposal:
  `{env}_edp_metadata.jdbc_runtime.manifest`. Owned by Wumbo until upstream.
- Per-source concurrency cap location (execution profile? source config?) —
  source-side backpressure not yet modeled
- Additional manifest columns beyond original spec: `watermark_value_at_start`,
  `retry_count`, `source_connection_id`, `parent_run_id`, `timeout_sec`,
  `schema_fingerprint` — keep all?
- Core-vs-Wumbo: load groups + environment selection → LHP core. Manifest
  executor + schema → Wumbo first, upstream once stable. Confirm.

## Acceptance Criteria (end of spike week)

**Direction A passes if:**

- 100-flow SDP pipeline initializes and runs in bounded time (target <10min)
- Per-flow JDBC read + watermark registry writes succeed atomically
- Failure isolation verified (1 fails, 99 complete)
- Oracle driver attach verified in SDP pipeline context
- Rerun-failed query works against event log + watermark registry

**Direction B passes if:**

- `for_each` over 100 manifest rows runs with `continue_on_error`
- Existing watermark notebook runs unchanged inside iteration
- Per-iteration overhead measured; 5000-table extrapolation within SLA
- Cost model acceptable vs current baseline

**Both fail → escalate to Direction C with revised spec.**

## References

- Original spec: v0.2 (inline in ideation session, 2026-04-23)
- LHP workflow generator: `src/lhp/generators/bundle/workflow_resource.py:29`
- JDBC watermark template: `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- Prior chunked impl: `../work/edp-data-engineering/bundles/data_engineering/resources/jobs/batch_source_to_bronze.yml`
- Databricks `for_each_task`: https://docs.databricks.com/aws/en/jobs/for-each
- LHP best practices: https://lakehouse-plumber.readthedocs.io/best_practices.html
