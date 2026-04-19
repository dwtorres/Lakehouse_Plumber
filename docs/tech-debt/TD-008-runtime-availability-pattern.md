# TD-008: LHP Runtime Availability Pattern Violates Constitution P2

- **Status**: Open
- **Severity**: High (constitutional drift; blocks clean production story)
- **Opened**: 2026-04-18
- **Opened by**: dwtorres@gmail.com
- **Surfaced during**: ADR-001 / TD-007 Phase B validation
- **Owning ADR**: ADR-002 (pending)
- **Related**: [ADR-001](../adr/ADR-001-jdbc-watermark-parquet-post-write-stats.md), Constitution P2, [src/lhp/templates/bundle/workflow_resource.yml.j2](../../src/lhp/templates/bundle/workflow_resource.yml.j2)

## Summary

The generated DAB workflow resource ships a wheel dependency by default:

```yaml
environments:
  - environment_key: lhp_env
    spec:
      environment_version: "2"
      dependencies:
        - ${var.lhp_whl_path}     # ← wheel path, supplied per-bundle
```

([src/lhp/templates/bundle/workflow_resource.yml.j2:11](../../src/lhp/templates/bundle/workflow_resource.yml.j2:11), populated by [src/lhp/generators/bundle/workflow_resource.py:77](../../src/lhp/generators/bundle/workflow_resource.py:77).)

This contradicts Constitution P2 as written:

> "Runtime deploys as workspace files or production Git folder synced by Databricks Asset Bundles or an admin-managed service principal. A Python wheel is introduced only when reuse, versioning, or rollback operationally requires it."
>
> "Generated notebooks import `lhp.extensions.*` and `lhp.runtime.*` from PYTHONPATH, not from an installed wheel."

The current template treats wheel as the default deployment vehicle. Constitution P2 says wheel should be a last resort with documented operational justification.

## Why This Matters

- **Constitutional drift undermines the framework's stated shape.** Users reading the constitution and inspecting generated bundles see a direct mismatch.
- **Operational coupling to wheel build/publish pipeline.** Every bundle deploy implicitly assumes a wheel is reachable at `${var.lhp_whl_path}`. No wheel → broken deploy. Contradicts "no wheel until justified" by making wheel mandatory.
- **Blocks "one LHP runtime per workspace" vision.** User goal (captured in TD-007 ADR input) is: one shared LHP runtime instance per workspace, many ingestion bundles consume it. Wheel-per-bundle duplicates the runtime image and defeats "one instance".
- **Cross-env promotion complexity.** Wheel versioning + DAB variable override + environment v2 dependency list is a lot of moving parts for something Constitution P2 expects to be simpler.

## Impact

- Every `jdbc_watermark_v2` flowgroup deploy requires a wheel-build step.
- Operators cannot bootstrap LHP by simply pulling the repo into a Databricks Git folder.
- Validation environments have to reproduce the wheel pipeline (or fall back to `sys.path` hacks — what TD-007 Phase B did).

## Candidate Resolutions

ADR-002 will pick among (non-exhaustive, expand during ideation):

1. **Git folder + `sys.path` prelude + env var `LHP_RUNTIME_PATH`.** Admin-managed git folder (e.g. `/Workspace/Shared/lhp_platform/Lakehouse_Plumber@release`). Generated extraction template prepends `sys.path.insert(0, os.environ["LHP_RUNTIME_PATH"])`. Workflow template emits `environments.env_vars: {LHP_RUNTIME_PATH: "${var.lhp_runtime_path}"}`. Wheel variable deprecated.
2. **DAB workspace_files sync**: platform bundle deploys `src/lhp/**` to `/Workspace/.bundle/lhp_platform/src/`; per-source bundles reference it via convention. Cross-bundle path coupling.
3. **Wheel via `workspace_file://` reference**: `${var.lhp_whl_path}` points at a workspace-hosted wheel built and synced by DAB. Closer to current shape; still wheel.
4. **Vendored per bundle**: `lhp generate` copies the `lhp.extensions.*` subset into each generated bundle. Self-contained; duplicates runtime.
5. **Hybrid**: vendored for single-user/dev workspaces; git folder for team workspaces. Flag on generator.

## Open Questions

- What is the minimum required admin permission set to create `/Workspace/Shared/*` paths? Determines whether Option 1 is feasible for single-user workspaces.
- Does Databricks serverless Jobs environment v2 honor `env_vars` reliably across restarts? (Probably yes — spec supports it. Validate before committing to Option 1.)
- How to handle version skew between an ingestion bundle pinned to LHP v0.8.x and a shared git folder that has since moved to v0.9.x? Git folder branch/tag pin vs. bundle-level pin vs. per-env promotion.
- Do DLT pipelines (CloudFiles side) need LHP runtime at all, or only the extraction notebook? (Phase B evidence suggests: only the extraction notebook needs `lhp.extensions.watermark_manager`. DLT side is pure PySpark + DLT decorators.)

## Non-Goals For This TD

- Changing the YAML contract. Resolution must be runtime/deploy-shape-only.
- Removing the wheel build from CI entirely. Wheel may still be useful for standalone testing or PyPI-style installs outside Databricks; TD-008 is specifically about the **Databricks runtime deploy** path.
- Resolving this TD inside ADR-001. TD-007 / ADR-001 explicitly scope runtime-availability out.

## Next Step

Ideation + brainstorm session for ADR-002. Treat as its own `idea-refine` pass with the candidate resolutions above as input. Output: `docs/adr/ADR-002-lhp-runtime-availability.md` + matching `.specs/` entries for generator/template changes.
