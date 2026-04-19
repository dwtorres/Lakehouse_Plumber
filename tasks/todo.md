# Todo — ADR-002 LHP Runtime Availability

Source: `tasks/plan.md`. One line per task. Legend: `[P]` parallelizable within phase, `▣` checkpoint gate.

## Phase 0 — Decision docs & governance
- [ ] T0.1 Author `docs/adr/ADR-002-lhp-runtime-availability.md` (resolves Open Qs 1–4; constitution map; compliance section)
- [ ] T0.2 Amend `.specs/constitution.md` P2 wording; bump version 1.0 → 1.1
- [ ] T0.3 [P] Create `docs/runbooks/lhp-runtime-deploy.md` stub
- [ ] ▣ C0 — Human reviewer approves ADR-002 + constitution amendment

## Phase 1 — Namespace extraction
- [ ] T1.1 `git mv src/lhp/extensions/watermark_manager src/lhp_watermark`
- [ ] T1.2 `git mv src/lhp_watermark/_manager.py src/lhp_watermark/watermark_manager.py`; update `__init__.py` re-exports
- [ ] T1.3 Remove any `lhp.extensions.watermark_manager` self-references inside the moved package
- [ ] T1.4 Update imports in `src/lhp/templates/load/jdbc_watermark_job.py.j2` (lines 19–24)
- [ ] T1.5 `git mv tests/extensions/watermark_manager tests/lhp_watermark`; sed imports
- [ ] T1.6 Update `tests/lhp_watermark/test_package_structure.py` assertions
- [ ] ▣ C1 — Full `pytest -q` green; `rg "lhp\.extensions\.watermark_manager" src/ tests/` returns zero; `git log --follow` preserved

## Phase 2 — Bundle template + generator refactor
- [ ] T2.1 Edit `src/lhp/templates/bundle/workflow_resource.yml.j2` — remove `environments:` block + `environment_key:`; add `source: WORKSPACE`; update `notebook_path` shape
- [ ] T2.2 Edit `src/lhp/generators/bundle/workflow_resource.py` — drop `lhp_whl_path`; update `notebook_path` builder
- [ ] T2.3 Emit `DeprecationWarning` when user bundle still declares `var.lhp_whl_path`
- [ ] T2.4 Update `tests/unit/generators/bundle/test_workflow_resource.py` assertions
- [ ] T2.5 Update bundle integration tests (`test_bundle_jinja2.py`, `test_orchestrator_bundle_integration.py`, `tests/e2e/test_bundle_manager_e2e.py`, `tests/scripts/test_verify_databricks_bundle.py`)
- [ ] ▣ C2 — All bundle-scoped tests green; `databricks bundle validate` dry-run green on sample

## Phase 3 — Packaging config
- [ ] T3.1 Update `pyproject.toml` to include `lhp` and `lhp_watermark` as top-level packages
- [ ] T3.2 Update `MANIFEST.in` / `setup.cfg` if present; `python -m build` produces wheel containing both
- [ ] T3.3 Extend CI wheel-build workflow with post-install import smoke step
- [ ] ▣ C3 — Fresh-venv `pip install -e .` + both imports + CI probe green

## Phase 4 — Databricks smoke
- [ ] T4.1 Author + run `sys.path` probe notebook; document anchor resolution
- [ ] T4.2 Verify `databricks repos update … && databricks bundle deploy -t dev` sequence
- [ ] T4.3 Run TD-007 Phase B-style E2E smoke with `lhp_watermark.*` imports; exercise all six safety features
- [ ] ▣ C4 — Real-workspace smoke green; evidence committed at `docs/adr/evidence/adr-002-smoke-test.md`

## Phase 5 — Rollout docs (parallel)
- [ ] T5.1 [P] Fill `docs/runbooks/lhp-runtime-deploy.md` body with ADO + GitHub Actions recipes
- [ ] T5.2 [P] Mark `docs/tech-debt/TD-008-runtime-availability-pattern.md` Resolved; link ADR-002
- [ ] T5.3 [P] Update ADR-001 cross-reference to resolved ADR-002
- [ ] T5.4 [P] Add release notes migration note for `${var.lhp_whl_path}` drop
- [ ] ▣ C5 — Docs reviewed; PR drafted; CI green; merge-ready

## Pre-Phase-1 blockers (must resolve in C0)
- [ ] B1 Worktree placement decision (A | B | C per plan §0.1)
- [ ] B2 Open Q 1 — `sys.path` anchor (probe vs pre-decide)
- [ ] B3 Open Q 2 — git folder path convention
- [ ] B4 Open Q 3 — generated-artifacts branching (persistent vs ephemeral)
- [ ] B5 Open Q 4 — notebook_path shape (`var.env` vs `bundle.target`)
- [ ] B6 Constitution amendment scope (narrow P2 bullet vs broader pattern doc)
- [ ] B7 Phase 4 dev workspace target + CLI profile confirmation
- [ ] B8 P1/P2 serialize vs parallelize
