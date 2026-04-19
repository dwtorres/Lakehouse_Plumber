# Runbook: LHP Runtime Deploy (Git Folder + Bundle)

**Status**: Stub (Phase 0 T0.3). Body populated in Phase 5 T5.1.
**Owner**: Platform engineering.
**Scope**: Deploys the `lhp_watermark` runtime library + generated extraction notebooks + DAB resources to a Databricks workspace via git-folder sync + bundle deploy.

---

## Purpose

Deploy the LHP fork's runtime + generated artifacts to a Databricks workspace per ADR-002 (Path 5). Replaces the prior wheel-based deployment (pre-ADR-002).

## Prerequisites

- Databricks CLI installed + authenticated (profile: project-specific, e.g., `dbc-8e058692-373e`)
- Git credentials for pushing to the deploy branch pattern `deploy/${env}/${COMMIT_SHA}`
- DAB bundle configured with variables: `env`, `lhp_git_folder_root` (default `/Workspace/${env}/Lakehouse_Plumber`)
- First-time setup: workspace admin has created the Git Folder via `databricks repos create` (or CI handles bootstrap)

## Deploy sequence

(Body filled in Phase 5 T5.1 — ADO recipe + GitHub Actions recipe + bootstrap handling)

1. `lhp generate --env ${env}` — regenerate notebooks + resources
2. Git: add, commit, push to `deploy/${env}/${COMMIT_SHA}`
3. `databricks repos update <path> --branch deploy/${env}/${COMMIT_SHA}` — sync runtime + generated artifacts
4. `databricks bundle deploy -t ${env}` — deploy DAB jobs referencing synced notebooks

## Rollback

(Body filled in Phase 5 T5.1)

- Revert deploy branch to previous `${COMMIT_SHA}`
- Re-run `repos update` + `bundle deploy`

## Troubleshooting

(Body filled in Phase 5 T5.1)

- `ModuleNotFoundError: lhp_watermark` → verify `sys.path` anchor (Phase 4 probe result); apply `sys.path.insert` prelude if needed
- `databricks repos update` fails on slashed branch name → use flat naming `deploy-${env}-${SHA}`
- Stale git folder (workspace still shows prior commit) → explicit `repos update` with `--branch` re-pins

## Related

- [ADR-002](../adr/ADR-002-lhp-runtime-availability.md) — decision record
- [Phase plan](../../tasks/plan.md) — full implementation plan
- `.github/workflows/` — CI pipelines (to be updated in Phase 5)
