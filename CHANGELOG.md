# Changelog

All notable changes to LHP (this fork).

## [Unreleased]

### Fixed
- **DLT schema inference on `jdbc_watermark_v2` landing paths** — `CloudFilesLoadGenerator` dispatch in `src/lhp/generators/load/jdbc_watermark_job.py` now emits `.load("{landing_path}/_lhp_runs/*")` instead of the bare `.load("{landing_path}")`. The extraction template writes Parquet at `{root}/_lhp_runs/{run_id}/` (ADR-001 §Decision); the bare-root load caused `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` because AutoLoader's schema-inference listing does not recurse into the `_lhp_runs/` subtree. Implements the CloudFilesLoadGenerator spot-check flagged as outstanding in ADR-001 §Consequences §Negative; validated by Phase B V8 evidence (ADR-001 Evidence table). The generator also now defaults `cloudFiles.useStrictGlobber: "false"` for jdbc_watermark_v2 loads so the `*` wildcard matches across directory separators on Unity Catalog volumes; user-supplied overrides via `action.source.options` still win.
- See [ADR-003](docs/adr/ADR-003-landing-zone-shape.md) for the longer-term landing-shape investigation opened alongside this fix.

### Added
- `lhp sync-runtime` CLI subcommand — vendors the installed `lhp_watermark` package into the current bundle directory. Automates ADR-002 Path 5 Option A vendoring; replaces the manual `cp -r ../Lakehouse_Plumber/src/lhp_watermark ./lhp_watermark` step.
- `lhp_watermark` distinct top-level Python package (sibling to `lhp/`) carrying the watermark runtime (`WatermarkManager`, `SQLInputValidator`, `derive_run_id`, exception taxonomy). See [ADR-002](docs/adr/ADR-002-lhp-runtime-availability.md).
- Template bootstrap: generated extraction notebooks (`jdbc_watermark_v2`) now emit `_lhp_watermark_bootstrap_syspath()` which prepends `${workspace.file_path}` to `sys.path` before the `lhp_watermark` imports. Required on Databricks serverless runtime `client.5.1`.
- Runbook: `docs/runbooks/lhp-runtime-deploy.md` — DAB workspace-file sync deploy recipe with ADO + GitHub Actions examples.

### Changed
- **Generated DAB workflow resource** no longer emits the `environments.dependencies` wheel attachment nor `environment_key: lhp_env`. Each extraction task now has `source: WORKSPACE` on its `notebook_task`. Notebook path anchor (`${workspace.file_path}/generated/${bundle.target}/…`) is unchanged.
- **Constitution** v1.0 → v1.1 (`.specs/constitution.md`). P2 bullet 1 amended to permit runtime libraries as distinct top-level packages (not only `lhp.extensions.*` submodules). See ADR-002 §Compliance.
- Generated extraction notebook imports `from lhp_watermark import (…)` instead of `from lhp.extensions.watermark_manager import (…)`.

### Removed
- Internal dependency of `lhp_watermark/exceptions.py` on `lhp.utils.error_formatter`. A minimal `LHPError` + `ErrorCategory.WATERMARK` are inlined into the runtime package so it imports standalone in Databricks task environments where the `lhp/` generator package is not deployed.

### Migration

Users with existing bundles declaring `${var.lhp_whl_path}` and using LHP-generated JDBC-watermark-v2 workflows:

1. Drop the `lhp_whl_path` variable from `databricks.yml` — the generator no longer emits a task-attached wheel dependency. (Keeping the variable is harmless; DAB ignores unused vars. Listed here for cleanliness.)
2. Vendor `lhp_watermark/` into the bundle root:
   ```bash
   lhp sync-runtime
   ```
   (Equivalent: `cp -r $(python -c 'import lhp_watermark, pathlib; print(pathlib.Path(lhp_watermark.__file__).parent)') ./lhp_watermark`)
3. If the bundle is not already a git repo, `git init -b main` and add a `.gitignore` — `databricks bundle deploy` emits a `no files to sync` warning when there is no git metadata and no explicit `sync.include` patterns.
4. `lhp generate --env <target>` — regenerates extraction notebooks with the new `lhp_watermark.*` imports and the `sys.path` bootstrap.
5. `databricks bundle deploy -t <target>` — single-mechanism deploy (no separate `databricks repos update` step).

No action is needed for bundles that do not use `jdbc_watermark_v2` — those workflows are unaffected.

### Evidence

- Phase 4 E2E smoke: job run `517674443949620` on `dbc-8e058692-373e.cloud.databricks.com` TERMINATED SUCCESS (2026-04-19). Four extraction tasks + DLT pipeline, 4m 19s.
- Reference bundle: https://github.com/dwtorres/Wumbo (`scooty_puff_junior`).
