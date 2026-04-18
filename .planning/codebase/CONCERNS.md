# Concerns

## Active Risks

- `jdbc_watermark` and `jdbc_watermark_v2` currently coexist, which keeps legacy architecture alive and complicates validation, registry behavior, docs, and tests
- The v2 extraction template still performs multiple actions against a JDBC dataframe before completion bookkeeping, which is a correctness and performance risk
- The generator currently synthesizes a minimal CloudFiles source and drops richer Auto Loader behavior
- Workflow generation defaults to parallel extraction fan-out without any source-safety control

## Fragile Areas

- `src/lhp/generators/load/jdbc_watermark_job.py` because it bridges extraction runtime and CloudFiles generation
- `src/lhp/extensions/watermark_manager/_manager.py` because terminal state semantics, retry behavior, and status transitions are tightly coupled
- `src/lhp/core/config_field_validator.py` because new config fields often require strict allowlist changes

## Technical Debt

- Legacy self-watermark code path still ships in `src/lhp/generators/load/jdbc_watermark.py`
- Docs and tests still reference removed or transitional behavior
- Bundle verification ergonomics are weak when multiple Databricks profiles match the same workspace host

## Performance Hotspots

- Multiple Spark actions on the same extracted dataframe can double source load
- Small-file pressure can emerge if landed paths are not run-scoped and retry-safe
- Parallel extraction can saturate source OLTP systems faster than Databricks itself

## Immediate Focus

- Remove v1 cleanly
- Close the post-land / pre-finalize recovery hole
- Preserve CloudFiles options instead of silently dropping them
- Add explicit-profile verification tooling instead of relying on local profile coincidence

