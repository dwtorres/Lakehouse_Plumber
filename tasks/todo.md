# Todo — ADR-002 LHP Runtime Availability

Source: `tasks/plan.md` + ADR-002 (amended 2026-04-19). Legend: `[P]` parallelizable within phase, `▣` checkpoint gate.

**Status**: Phases 0–4 complete. Phase 5 partial. On branch `feature/adr-002-runtime-availability`.

## Phase 0 — Decision docs & governance
- [x] T0.1 Author `docs/adr/ADR-002-lhp-runtime-availability.md` — commit `0bc88d28`; amended 2026-04-19 + Phase 4 evidence appended
- [x] T0.2 Amend `.specs/constitution.md` P2 wording; bump version 1.0 → 1.1 — commit `0bc88d28`
- [x] T0.3 [P] Create `docs/runbooks/lhp-runtime-deploy.md` stub — commit `0bc88d28`
- [x] ▣ C0 — User-approved proceed

## Phase 1 — Namespace extraction
- [x] T1.1 `git mv src/lhp/extensions/watermark_manager src/lhp_watermark` — commit `bdaeeaa3`
- [x] T1.2 `git mv src/lhp_watermark/_manager.py src/lhp_watermark/watermark_manager.py`; update `__init__.py` re-exports
- [x] T1.3 Self-references removed (sed pass)
- [x] T1.4 Update imports in `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- [x] T1.5 `git mv tests/extensions/watermark_manager tests/lhp_watermark`; sed imports
- [x] T1.6 Update `tests/lhp_watermark/test_package_structure.py` assertions
- [x] T1.7 (added during execution) Also update `scripts/benchmark_slice_a.py`, `src/lhp/extensions/migrations/slice_a_backfill.py`, `tests/unit/generators/load/test_jdbc_watermark_job.py`
- [x] T1.8 (added during execution) Remove `tests/lhp_watermark/__init__.py` — colliding top-level-package name during pytest collection
- [x] T1.9 (added during execution) `tests/scripts/test_benchmark_slice_a.py` — pass `PYTHONPATH=src` to subprocess
- [x] T1.10 (fixup) Capture sed edits that the initial commit missed — commit `717d4e25`
- [x] ▣ C1 — 3298 tests pass; `rg "lhp\.extensions\.watermark_manager" src/ tests/` returns zero; history preserved

## Phase 2 — Bundle template + generator refactor (minimal per Option A)
- [x] T2.1 Edit `src/lhp/templates/bundle/workflow_resource.yml.j2` — remove `environments:` block + `environment_key:`; add `source: WORKSPACE` — commit `33d1b0b6`
- [x] T2.2 Edit `src/lhp/generators/bundle/workflow_resource.py` — drop `lhp_whl_path` context; keep `${workspace.file_path}/generated/${bundle.target}/…` anchor unchanged
- [ ] T2.3 Emit `DeprecationWarning` when user bundle still declares `var.lhp_whl_path` — **deferred to Phase 5 follow-up** (low value; DAB ignores unused vars)
- [x] T2.4 Update `tests/unit/generators/bundle/test_workflow_resource.py` assertions
- [x] T2.5 Bundle integration tests unchanged — Option A keeps `${workspace.file_path}/…` anchor app-wide, so 40+ fixtures/tests stay green
- [x] ▣ C2 — 3298 tests pass; `databricks bundle validate` green on Wumbo

## Phase 2.5 — Runtime lib decoupling (discovered during Phase 4)
- [x] T2.5.1 `lhp_watermark/exceptions.py` inlines minimal `LHPError` + `ErrorCategory.WATERMARK` — commit `0e7c1126`
- [x] T2.5.2 Verified standalone `import lhp_watermark` works without `lhp/` on `sys.path`

## Phase 3 — Packaging config
- [x] T3.1 `pyproject.toml` `[tool.setuptools.packages.find] where = ["src"]` already auto-discovers both packages — no edit needed
- [x] T3.2 `python -m build` verified — wheel contains `lhp/*` + `lhp_watermark/*`
- [ ] T3.3 Extend CI wheel-build workflow with post-install import smoke step — **deferred to Phase 5**
- [x] ▣ C3 — Wheel builds, both packages import

## Phase 4 — Databricks smoke
- [x] T4.1 `sys.path` probe — runs `135873449992972`, `561719421793073` (Wumbo); verdict **prelude required**; implemented in template commit `a1c12852`
- [x] T4.2 `databricks --profile lhp-deploy-sp bundle validate -t dev` + `bundle deploy -t dev` green against `scooty_puff_junior` bundle on `dbc-8e058692-373e.cloud.databricks.com`
- [x] T4.3 `bundle run -t dev jdbc_ingestion_workflow` — TERMINATED SUCCESS; 4 extraction tasks + DLT pipeline; 4m 19s — run `517674443949620`
- [x] ▣ C4 — Real-workspace smoke green; evidence captured in ADR-002 §Phase 4 Evidence

## Phase 5 — Rollout docs (parallel)
- [x] T5.1 [P] Fill `docs/runbooks/lhp-runtime-deploy.md` body — ADO + GitHub Actions recipes + troubleshooting
- [ ] T5.2 [P] Mark `docs/tech-debt/TD-008-runtime-availability-pattern.md` Resolved — **file does not exist yet in this repo**; will be created when TD-008 is formally logged
- [ ] T5.3 [P] Update ADR-001 cross-reference — **ADR-001 does not exist yet in this repo**; create when ADR-001 is authored
- [ ] T5.4 [P] Add release notes migration note for `${var.lhp_whl_path}` drop — pending (no `CHANGELOG.md` yet)
- [ ] T5.5 [P] (new) Add `lhp sync-runtime` CLI subcommand to automate vendoring the runtime library into user bundles — Phase 5 backlog
- [ ] ▣ C5 — PR drafted; review; merge

## Pre-Phase-1 blockers (resolved)
- [x] B1 Worktree placement — Option C (sibling `adr-002-runtime-availability`)
- [x] B2 Open Q 1 — **resolved empirically**: prelude required; template emits `_lhp_watermark_bootstrap_syspath()` helper
- [x] B3 Open Q 2 — **obviated** by ADR amendment (no git folder mechanism)
- [x] B4 Open Q 3 — **obviated** by ADR amendment
- [x] B5 Open Q 4 — **obviated** by ADR amendment (keep `${bundle.target}` anchor)
- [x] B6 Constitution amendment scope — narrow P2 bullet 1 only
- [x] B7 Phase 4 workspace — `dbc-8e058692-373e.cloud.databricks.com` via profile `lhp-deploy-sp`
- [x] B8 P1/P2 — serialized
