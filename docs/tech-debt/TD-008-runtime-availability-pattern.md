# TD-008: LHP Runtime Availability Pattern

**Status**: Resolved
**Opened**: 2026-04-18 (during TD-007 closure review)
**Resolved**: 2026-04-19
**Resolves-via**: [ADR-002](../adr/ADR-002-lhp-runtime-availability.md)

## Problem

The fork's generated extraction notebook (`src/lhp/templates/load/jdbc_watermark_job.py.j2`) imported `from lhp.extensions.watermark_manager import …` and the generated DAB workflow resource attached a Python wheel via `environments.dependencies: [${var.lhp_whl_path}]`.

Two alignment gaps with upstream LHP intent:

1. **Upstream says** (`README.md`): *"Zero runtime overhead — pure code generation, not a runtime framework."* A Python wheel attached per extraction task is a runtime dependency.
2. **Maintainer intent for watermark support** ([issue #65](https://github.com/Mmodarre/Lakehouse_Plumber/issues/65)) scopes the feature to "create and maintain a table of high watermarks for tables which gets loaded by external integration tools" — not a Python class instantiated inside the pipeline.

## Resolution

ADR-002 (amended 2026-04-19) adopted **Path 5: Namespace extraction + DAB workspace-file sync** (Option A):

1. Renamed `src/lhp/extensions/watermark_manager/` → `src/lhp_watermark/` (distinct top-level Python package, sibling to `src/lhp/`).
2. LHP (`src/lhp/`) reverted to pure code generator.
3. Generated extraction notebook imports `lhp_watermark.*` (not `lhp.*`).
4. Runtime library deploys via existing `databricks bundle deploy` workspace-file sync — no wheel attachment, no separate Git Folder step.
5. Template emits `_lhp_watermark_bootstrap_syspath()` helper to prepend `${workspace.file_path}` to `sys.path` before the `lhp_watermark` imports (Databricks Runtime `client.5.1` does not include that path by default).

Constitution P2 amended (v1.0 → v1.1) to permit distinct top-level runtime packages.

**Evidence**: Phase 4 E2E smoke test — job run `517674443949620` on `dbc-8e058692-373e.cloud.databricks.com`, TERMINATED SUCCESS (4 extraction tasks + DLT pipeline, 4m 19s).

## Related

- [ADR-002](../adr/ADR-002-lhp-runtime-availability.md)
- [Runbook](../runbooks/lhp-runtime-deploy.md)
- Wumbo reference bundle: https://github.com/dwtorres/Wumbo
- Constitution v1.1 (`.specs/constitution.md`)
