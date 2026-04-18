# Roadmap: Lakehouse Plumber JDBC Watermark Hardening

## Overview

This roadmap hardens the deployed `jdbc_watermark_v2` path into a source-safe, recovery-safe, and operationally verifiable production flow while removing the legacy v1 watermark architecture that no longer matches the intended runtime.

## Phases

- [x] **Phase 1: Remove Legacy Watermark Path** - delete supported v1 behavior and replace it with a direct migration failure
- [x] **Phase 2: Harden Extraction Lifecycle** - make landing and finalization retry-safe with single-read extraction semantics
- [x] **Phase 3: Restore Bronze And Workflow Fidelity** - preserve CloudFiles behavior and add optional serial extraction
- [x] **Phase 4: Verification And Stress** - extend tests, benchmarks, and Databricks verification tooling

## Phase Details

### Phase 1: Remove Legacy Watermark Path
**Goal**: Only `jdbc_watermark_v2` remains as the supported watermark ingestion mode.
**Depends on**: Nothing (first phase)
**Requirements**: [LEG-01, LEG-02]
**Success Criteria** (what must be TRUE):
  1. `jdbc_watermark` is rejected with a migration error.
  2. Registry, validators, templates, and tests no longer treat v1 as supported.
**Plans**: 2 plans

Plans:
- [x] 01-01: Remove v1 source type plumbing from models, validators, generators, and registry
- [x] 01-02: Rewrite affected tests and docs around the v2-only contract

### Phase 2: Harden Extraction Lifecycle
**Goal**: Make extraction runs single-read, run-scoped, and recoverable after durable landing.
**Depends on**: Phase 1
**Requirements**: [EXT-01, EXT-02, EXT-03, EXT-04, WM-01, WM-02, WM-03]
**Success Criteria** (what must be TRUE):
  1. A retry after post-land failure finalizes the prior landed batch without reopening JDBC.
  2. Notebook logs clearly show recovery, extract, land, and finalize phases.
  3. Watermark status transitions remain consistent under failure and retry paths.
**Plans**: 2 plans

Plans:
- [x] 02-01: Extend watermark manager state machine for landed recovery
- [x] 02-02: Rework `jdbc_watermark_job.py.j2` and generator context for run-scoped landing

### Phase 3: Restore Bronze And Workflow Fidelity
**Goal**: Preserve CloudFiles semantics and expose a safe workflow execution control.
**Depends on**: Phase 2
**Requirements**: [BRZ-01, BRZ-02, BRZ-03, OPS-01, OPS-02]
**Success Criteria** (what must be TRUE):
  1. Generated Bronze stubs preserve Auto Loader behavior instead of dropping it.
  2. A stable schema location exists even when users omit one.
  3. Workflows can be generated in serial extraction mode when needed.
**Plans**: 2 plans

Plans:
- [x] 03-01: Pass richer v2 source config through the CloudFiles generator
- [x] 03-02: Add workflow serial-extraction support without breaking current defaults

### Phase 4: Verification And Stress
**Goal**: Make the hardening change measurable and operationally verifiable.
**Depends on**: Phase 3
**Requirements**: [OPS-03, PERF-01, PERF-02]
**Success Criteria** (what must be TRUE):
  1. Targeted tests cover the new runtime and generation paths.
  2. Benchmarks capture lifecycle overhead and generation regressions.
  3. Databricks verification commands are explicit-profile and readable for operators.
**Plans**: 2 plans

Plans:
- [x] 04-01: Extend tests and script-based benchmarks
- [x] 04-02: Add profile-explicit verification tooling and bundle guidance

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Remove Legacy Watermark Path | 2/2 | Completed | 2026-04-18 |
| 2. Harden Extraction Lifecycle | 2/2 | Completed | 2026-04-18 |
| 3. Restore Bronze And Workflow Fidelity | 2/2 | Completed | 2026-04-18 |
| 4. Verification And Stress | 2/2 | Completed | 2026-04-18 |
