# Runbook: LHP Runtime Deploy (DAB Workspace-File Sync)

**Status**: Active (Phase 5 T5.1, post-ADR-002 amendment 2026-04-19).
**Owner**: Platform engineering.
**Scope**: Deploys the `lhp_watermark` runtime library + LHP-generated DLT + extraction resources to a Databricks workspace via `databricks bundle deploy`.

---

## Purpose

Deploy an LHP-managed Databricks project end-to-end against a target environment. The runtime library travels as workspace files alongside generated notebooks — no wheel, no Databricks Git Folder, no `databricks repos update` step.

## Prerequisites

- Databricks CLI installed and authenticated against the target workspace. Recommend a service principal profile (e.g. `lhp-deploy-sp`) for CI and a PAT profile for ad-hoc use.
- Project repo layout:
  - `databricks.yml` — DAB bundle spec with at least one target (e.g. `dev`, `tst`, `prod`)
  - `lhp.yaml` — LHP project config
  - `pipelines/` — source YAML
  - `resources/lhp/` — LHP-generated resource files
  - `lhp_watermark/` — **vendored runtime library** (see §Vendoring)
  - `.gitignore` — must declare the repo as a known file set; `databricks bundle deploy` emits `no files to sync` when the bundle is run from a directory that is neither a git repo nor has explicit `sync.include` patterns
- LHP CLI installed in the generator's Python env (editable install from the LHP repo is fine during development; release builds install the published version).

## Vendoring the runtime library

Path 5 Option A (ADR-002 amended 2026-04-19) ships `lhp_watermark/` as a top-level directory in the user bundle. DAB syncs it to `${workspace.file_path}/lhp_watermark/` where the generated extraction notebook picks it up via a `sys.path` bootstrap.

**Current method (copy)**:

```bash
# From the user-bundle repo root, with the LHP repo as a sibling checkout:
cp -r ../Lakehouse_Plumber/src/lhp_watermark ./lhp_watermark
rm -rf ./lhp_watermark/__pycache__
```

Refresh after every LHP upgrade. Commit the updated copy. A deterministic, explicit version pin.

**Follow-up (Phase 5 backlog)**: `lhp sync-runtime` CLI subcommand that copies the installed package from site-packages into `./lhp_watermark/` automatically. Eliminates the sibling-checkout assumption.

**Do not** add the LHP repo itself to `sync.paths` in `databricks.yml` reaching outside the bundle root — DAB rebases sync to the common parent directory and every `sync.include` pattern then needs the bundle-directory prefix, breaking portability.

## Deploy sequence

Per target, per change:

```bash
# 1. Regenerate notebooks + resource files from source YAML
lhp generate --env ${env}

# 2. (Optional) Inspect pending changes
databricks --profile ${PROFILE} bundle validate -t ${env}

# 3. Deploy — uploads bundle files + registers workflow/pipeline resources
databricks --profile ${PROFILE} bundle deploy -t ${env}

# 4. Run the workflow to verify
databricks --profile ${PROFILE} bundle run -t ${env} ${workflow_name}
```

**What `bundle deploy` does** (relevant to this runbook):
- Uploads every tracked (and non-gitignored) file in the bundle root to `${workspace.file_path}/` (default `/Workspace/Users/<user>/.bundle/<bundle>/<target>/files/` for dev mode).
- `lhp_watermark/` lands at `${workspace.file_path}/lhp_watermark/`.
- `generated/${bundle.target}/.../__lhp_extract_*` extraction notebooks land at `${workspace.file_path}/generated/${bundle.target}/.../__lhp_extract_*`.
- Registers workflow + pipeline resources from `resources/lhp/*.yml`.

## Reference CI shapes

### Azure DevOps

```yaml
# azure-pipelines.yml
stages:
  - stage: deploy_dev
    jobs:
      - job: lhp_deploy
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
          - task: UsePythonVersion@0
            inputs: { versionSpec: '3.12' }
          - script: pip install -e ../Lakehouse_Plumber
            displayName: Install LHP CLI
          - script: |
              lhp generate --env dev
            displayName: Generate notebooks
          - script: |
              curl -fsSL https://databricks.com/install.sh | sh
              databricks --profile $(DATABRICKS_PROFILE) bundle validate -t dev
              databricks --profile $(DATABRICKS_PROFILE) bundle deploy -t dev
            env:
              DATABRICKS_HOST: $(DATABRICKS_HOST)
              DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
              DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)
            displayName: Deploy bundle
```

### GitHub Actions

```yaml
# .github/workflows/deploy-dev.yml
name: Deploy dev
on:
  push:
    branches: [ main ]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.12' }
      - run: pip install -e ../Lakehouse_Plumber
      - run: lhp generate --env dev
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
      - run: databricks bundle deploy -t dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
          DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
```

Service principal credentials via OIDC federation are preferred over static client secrets where available.

## Rollback

- **Code rollback**: revert the commit that changed `lhp_watermark/` or the source YAML that produced the failing generated notebook, push, rerun `bundle deploy`. Workspace files converge to the reverted state.
- **Resource rollback**: `databricks bundle destroy -t ${env}` removes the registered workflow + pipeline; re-`bundle deploy` from the prior commit rebuilds them. Destroy is scoped to bundle-managed resources only.

## Troubleshooting

**`ModuleNotFoundError: No module named 'lhp_watermark'` inside an extraction task**
  - Confirm `lhp_watermark/` exists at the bundle root and is not gitignored or excluded by `sync.exclude`.
  - Confirm the generated extraction notebook includes `_lhp_watermark_bootstrap_syspath()` followed by the `lhp_watermark` imports. If it doesn't, regenerate with LHP version ≥ the ADR-002 commit `a1c12852`.
  - Check the probe notebook output (adapt `notebooks/_probes/sys_path_probe.py` from the Wumbo reference bundle). Look at the `workspace_file_root` field — confirm it ends in `/files`. If the bundle uses a non-standard workspace layout, the `/files/` split heuristic may need adjustment.

**`Warning: There are no files to sync, please check your .gitignore`**
  - `databricks bundle deploy` walks the bundle root using git metadata when a `.git/` directory is present. Without a git repo, it needs explicit `sync.include` patterns in `databricks.yml`.
  - Fix: `git init -b main` in the bundle repo, or add `sync.include:` patterns covering the directories to upload.

**`error: unable to locate bundle root: databricks.yml not found`**
  - You ran `databricks bundle ...` from outside the bundle repo. `cd` into the directory containing `databricks.yml`.

**Stale workspace files after a deploy**
  - DAB performs a differential sync. If workspace files diverged from local (rare; manual workspace edits), re-deploy with `--force-lock` if your target is locked, or delete the target's `files/` subtree in workspace and redeploy.

**Bundle deploy rebases sync to a common parent**
  - If `sync.paths` in `databricks.yml` points to a directory outside the bundle root (e.g. a sibling LHP checkout), DAB sets its sync root to the lowest common parent of the bundle and the external path. Every `sync.include` pattern then needs the bundle-directory prefix.
  - Avoid this shape — vendor `lhp_watermark/` into the bundle instead.

**`INVALID_PARAMETER_VALUE.LOCATION_OVERLAP` during DLT streaming query**
  - Symptom (full error): `Input path url 's3://.../volumes/<vol_id>/<table>/_lhp_runs/<uuid>/part-*.parquet' overlaps with managed storage within 'CheckPathAccess' call`.
  - Root cause: Unity Catalog rejects AutoLoader reads from a volume whose storage root overlaps with the managed storage of a table in the same schema. Co-locating the landing volume with bronze streaming_tables in one schema (e.g. `main.bronze.landing` alongside `main.bronze.<table>`) triggers this.
  - Fix: move the landing volume into a dedicated schema (`main._landing.landing`) that hosts **no managed tables**. Update the bundle's `landing_path` substitution accordingly. No LHP code change; user-bundle YAML only.
  - Production-shape recommendation: use external Unity Catalog volumes (backed by user-controlled ADLS / S3 / GCS) per environment × medallion. External volumes sit outside the catalog's managed-storage root, so the overlap check does not apply. One external volume per `{env}_edp_{medallion}` catalog is the reference pattern.
  - Reference evidence: Wumbo PR #2 (2026-04-19), DLT update `338e767a-18d5-4d55-a030-587ba6a8de0e`.

**Same `LOCATION_OVERLAP` but against the *old* volume even after you moved the landing path**
  - Symptom: error cites an S3 URL for the previous landing volume (`volumes/<old_vol_id>/...`), even though `databricks.yml` and generated notebooks now point at the new path.
  - Root cause: the DLT pipeline's `_dlt_metadata/checkpoints/<stream>/` retains AutoLoader source state from the prior run. When the source path changes, the streaming query replays the old path from checkpoint.
  - Fix: `databricks bundle run -t <target> --full-refresh-all <pipeline_name>` — resets DLT checkpoints and runs a full backfill from the new source. After success, subsequent runs can drop `--full-refresh-all`.

## B2 (for_each) dependency

Flowgroups that declare `workflow.execution_mode: for_each` (B2 topology) generate
additional files — `prepare_manifest.py`, `validate.py`, and a `worker/` directory —
under `generated/<env>/<pipeline>/`. The same `lhp sync-runtime` + `bundle deploy`
sequence covers these files; no extra deploy steps are required.

B2 has one hard prerequisite: the Tier 2 load_group registry axis must be deployed
per environment before B2 workflows run. Tier 2 devtest sign-off completed 2026-04-25
(V1-V5 PASS). See [`docs/runbooks/b2-for-each-rollout.md`](./b2-for-each-rollout.md)
for the full B2 adoption walkthrough and V-checklist.

## Related

- [ADR-002](../adr/ADR-002-lhp-runtime-availability.md) — decision record (amended 2026-04-19)
- [Phase plan](../../tasks/plan.md) — implementation plan + Phase 4 evidence
- Reference bundle: [`github.com/dwtorres/Wumbo`](https://github.com/dwtorres/Wumbo) — `scooty_puff_junior` (working Path 5 Option A example)
- [B2 for_each rollout runbook](./b2-for-each-rollout.md) — operator adoption guide for `execution_mode: for_each`
