# Lakehouse Plumber: JDBC Watermark Hardening

## What This Is

Lakehouse Plumber is a YAML-driven code generator for Databricks data pipelines. This brownfield initiative focuses on making `jdbc_watermark_v2` production-safe for the deployed Wumbo topology while removing the legacy `jdbc_watermark` path that now conflicts with the intended architecture.

## Core Value

Any engineer can define a reliable incremental JDBC-to-Databricks ingestion flow in YAML without hand-writing notebooks, jobs, or DLT boilerplate.

## Requirements

### Validated

- `lhp generate` already produces executable Databricks Python and bundle resources from YAML configs
- `jdbc_watermark_v2` already generates extraction notebooks, Bronze stubs, and workflow YAML for Wumbo-style deployments

### Active

- [x] Remove `jdbc_watermark` from the public generation path and replace it with a clear migration error
- [x] Make `jdbc_watermark_v2` do one JDBC read per extraction run
- [x] Recover correctly when files land but watermark finalization does not
- [x] Preserve CloudFiles reader behavior for the generated Bronze stub
- [x] Add source-safe optional serial extraction for generated workflows
- [x] Add explicit-profile Databricks verification tooling that does not hardcode local credentials

### Out of Scope

- New runtime services or control planes outside generated Databricks artifacts — violates upstream LHP intent
- Hardcoding a specific Databricks profile into `databricks.yml` — local environment detail, not repo behavior

## Context

The current runtime target is four extraction notebooks feeding one serverless `jdbc_ingestion` DLT pipeline through landed Parquet and a generated workflow job. The repo already contains Slice A watermark runtime hardening and a broad test suite, but the final production boundary is still open around recovery semantics, dropped CloudFiles behavior, and legacy v1 coexistence.

## Constraints

- **Compatibility**: Keep working `jdbc_watermark_v2` YAML stable for existing users
- **Source safety**: Do not reintroduce duplicate JDBC scans or replay-driven source reads
- **Architecture**: Stay aligned with readable generated Python and native Databricks workflows
- **Security**: Preserve SQL safety and secret-resolution behavior in generated notebooks
- **Brownfield**: Existing dirty worktree content outside this task must remain untouched

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Remove v1 instead of maintaining two watermark architectures | Dual paths are creating drift in validators, docs, and tests | Implemented |
| Use run-scoped landed subdirectories plus recovery state | Needed to finalize durable landings without reopening JDBC | Implemented |
| Preserve CloudFiles behavior by delegating richer source config into the existing CloudFiles generator | Reuses LHP patterns instead of inventing a second Auto Loader path | Implemented |
| Keep workflow fan-out default compatible but add optional serialization | Avoids breaking current deployments while giving source-constrained users a safer switch | Implemented |

---
*Last updated: 2026-04-18 after implementation and verification*
