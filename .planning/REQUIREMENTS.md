# Requirements: Lakehouse Plumber JDBC Watermark Hardening

**Defined:** 2026-04-17
**Core Value:** Any engineer can define a reliable incremental JDBC-to-Databricks ingestion flow in YAML without hand-writing notebooks, jobs, or DLT boilerplate.

## v1 Requirements

### Legacy Removal

- [ ] **LEG-01**: `source.type: jdbc_watermark` is rejected with a clear migration error pointing to `jdbc_watermark_v2`
- [ ] **LEG-02**: No v1 generator, template, registry, validator, or test path remains in the supported code path

### Extraction Runtime

- [ ] **EXT-01**: Each `jdbc_watermark_v2` extraction run performs exactly one JDBC source read
- [ ] **EXT-02**: Each run lands data into a deterministic run-scoped subdirectory under the configured landing root
- [ ] **EXT-03**: A durable landed batch can be finalized on retry without reopening JDBC
- [ ] **EXT-04**: Extraction notebooks emit structured phase logs for recovery, JDBC read, landing write, and finalization

### Watermark Correctness

- [ ] **WM-01**: Watermark state includes an explicit landed-but-not-finalized status
- [ ] **WM-02**: `mark_complete` advances from a durable landed batch only
- [ ] **WM-03**: Read and write failures still transition through `mark_failed`

### Bronze Ingestion

- [ ] **BRZ-01**: Generated Bronze stubs preserve CloudFiles options and defaults instead of dropping them
- [ ] **BRZ-02**: A stable `cloudFiles.schemaLocation` is generated when none is supplied
- [ ] **BRZ-03**: Cleanup behavior such as `cleanSource=delete` remains opt-in only

### Workflow And Ops

- [ ] **OPS-01**: Generated workflows keep the current Databricks Job to DLT orchestration model
- [ ] **OPS-02**: Generated workflows support an optional serial extraction mode for source-constrained environments
- [ ] **OPS-03**: Verification tooling requires explicit `--profile` or `DATABRICKS_CONFIG_PROFILE`

## v2 Requirements

### Stress And Performance

- **PERF-01**: Benchmark harness captures v2 lifecycle overhead and generation regression
- **PERF-02**: Operator-run Databricks stress routines confirm recovery semantics under retries and interruptions

## Out of Scope

| Feature | Reason |
|---------|--------|
| New control-plane service for extraction orchestration | Breaks upstream LHP design intent |
| Profile-specific repo defaults in `databricks.yml` | Environment-specific and unsafe for shared repos |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| LEG-01 | Phase 1 | Pending |
| LEG-02 | Phase 1 | Pending |
| EXT-01 | Phase 2 | Pending |
| EXT-02 | Phase 2 | Pending |
| EXT-03 | Phase 2 | Pending |
| EXT-04 | Phase 2 | Pending |
| WM-01 | Phase 2 | Pending |
| WM-02 | Phase 2 | Pending |
| WM-03 | Phase 2 | Pending |
| BRZ-01 | Phase 3 | Pending |
| BRZ-02 | Phase 3 | Pending |
| BRZ-03 | Phase 3 | Pending |
| OPS-01 | Phase 3 | Pending |
| OPS-02 | Phase 3 | Pending |
| OPS-03 | Phase 4 | Pending |
| PERF-01 | Phase 4 | Pending |
| PERF-02 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 14 total
- Mapped to phases: 14
- Unmapped: 0

---
*Requirements defined: 2026-04-17*
*Last updated: 2026-04-17 after brownfield GSD bootstrap*

