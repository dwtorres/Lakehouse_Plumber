# Lakehouse Plumber — Constitution

**Status**: Active
**Version**: 1.0
**Date**: 2026-04-17
**Governs**: All specs under `.specs/` and all code under `src/lhp/`.

These principles are immutable for the duration of the current milestone. Every L1-L5 spec and every generated artifact must conform. Violations require explicit, documented waiver with user sign-off.

---

## P1 — Additive Over Rewrite

Extend existing types, actions, and APIs rather than introducing parallel versions.

- No new `_v3`/`_vN` source types without user approval and a migration path for existing configs.
- Breaking schema changes require a documented compatibility matrix and deprecation window.
- New capabilities must compose with the existing action registry, template engine, and generator pipeline.

**Rationale**: LHP methodology is declarative YAML compiled to pipelines. Parallel versions fragment the contract for users and double maintenance burden.

---

## P2 — No Wheel Until Justified

Runtime deploys as workspace files or production Git folder synced by Databricks Asset Bundles or an admin-managed service principal. A Python wheel is introduced only when reuse, versioning, or rollback operationally requires it.

- Generated notebooks import `lhp.extensions.*` and `lhp.runtime.*` from PYTHONPATH, not from an installed wheel.
- JDBC driver JARs are attached at task/job level in the bundle resource, never bundled in the wheel.
- Personal Git folders are never the production runtime path.

**Rationale**: Fewer moving parts. Faster iteration. Packaging is an operational concern, not an architectural one.

---

## P3 — Thin Notebook, Fat Runtime

Generated notebooks and Jinja templates contain no business logic. They read parameters, instantiate runtime classes from `lhp.extensions.*` / `lhp.runtime.*`, invoke one entrypoint, and exit with structured status.

- No source-type branching in templates.
- No watermark arithmetic in Jinja.
- No SQL string-building in templates beyond parameter interpolation through validated helpers.
- Dialect differences belong in runtime defaults and option normalization, never as separate template branches.

**Rationale**: Templates are hard to test, hard to refactor, and invisible to the Python type system. Logic belongs in testable Python modules.

---

## P4 — FlowGroup = Entity

One FlowGroup maps to one table / entity / stream. One load action per extraction unit. Orchestration concerns (sharding, concurrency, scheduling) live in `job_name` and bundle resources, not in notebook-level loops over metadata arrays.

- No generic "extract many tables" notebooks.
- No chunk manifests in Volumes as the control plane.
- Scale is achieved by generating many narrow tasks, not by making one task do many things.

**Rationale**: The monolithic-notebook anti-pattern is exactly the developer nightmare LHP exists to escape.

---

## P5 — Registry Is Source Of Truth For Watermarks

The watermark state model governs resumption. No "resume from here" logic lives in landed files, run ledgers, or ad-hoc notebooks.

- Current watermark advances only after landing is durable AND status transitions to a terminal success state.
- Lookups of current watermark filter by committed / completed status. Non-terminal rows never advance the effective watermark.
- Registry operations are atomic: reading current state, acquiring a lease/version, and committing success cannot interleave across concurrent runs on the same key.
- External tools (ADF, ad-hoc notebooks) use the same registry API as LHP-generated jobs.

**Rationale**: A watermark table that serves as both run ledger and resume state will eventually advance off a failed row. Retry, partial failure, and concurrent execution are the edge cases that matter most.

---

## P6 — Retry Safety By Construction

Every extraction step must be idempotent under task retry.

- Landing paths are run-scoped: `.../run_id=<uuid>/` (or equivalent). Retries write new directories; they never mutate prior runs.
- Extracted rows carry `_lhp_extract_run_id`, `_lhp_extracted_at`, and source identity metadata.
- State commits happen after landing is durable. If landing succeeds and state commit fails, the run is recoverable because the landing path is run-scoped and the ledger records `LANDED_NOT_COMMITTED` (or equivalent).
- Watermark advance uses optimistic concurrency: version check on update, not read-then-write.

**Rationale**: Shared append-only paths duplicate data on retry. Read-then-write watermark updates race under concurrent runs.

---

## P7 — Declarative Tuning Passthrough

Performance-critical options (`fetchSize`, `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound`, `customSchema`, `sessionInitStatement`, `queryTimeout`, dialect-specific extras) pass through declaratively from YAML to the runtime via a flat options map.

- No source-type branching in templates or runtime based on dialect.
- `numPartitions` is treated as both concurrency and connection-count bound; validation enforces sensible caps per source system.
- Partitioned reads require the full quartet of `partitionColumn` + bounds + `numPartitions`.

**Rationale**: Legacy source-type branching is unmaintainable. Declarative passthrough inherits Spark's JDBC documentation as the specification.

---

## P8 — Unity Catalog And Secret Scopes

All table names are three-part `catalog.schema.table`. All secrets resolve through Databricks secret scopes using the `${scope/key}` syntax. No hardcoded credentials, no two-part table names, no workspace-level Hive metastore dependencies.

**Rationale**: Non-negotiable for production. Governance, RBAC, and HIPAA hashing depend on Unity Catalog.

---

## P9 — Security-Exact SQL Interpolation

Every value interpolated into SQL must pass through `SQLInputValidator` or equivalent. String literals use doubled single quotes (`''`), never stripping. Identifiers are validated against an allowlist pattern and quoted with backticks or double quotes per dialect. No raw string concatenation.

**Rationale**: The `.replace("'", "")` pattern deletes data under adversarial input and does not prevent injection.

---

## P10 — Human Validation At Level Boundaries

Each spec level (L0-L5) requires explicit human approval before advancing. AI-generated artifacts that cross a level boundary without review are rejected. Parallel execution of tasks requires the `[P]` marker and documented independence.

**Rationale**: SDD separates planning from implementation. Skipping review collapses that separation.

---

## Waiver Procedure

A principle may be waived for a specific slice if:

1. The waiver is documented in the slice's L3 spec under a `Constitution Waivers` section.
2. The waiver names the principle, the scope, the reason, and the expiry condition.
3. The user has explicitly approved the waiver.

Unapproved waivers are defects.
