# Implementation Plan: JDBC Watermark v2 (Autoloader Landing)

## Overview

Generate three artifacts from a single YAML config: an extraction Job notebook (WatermarkManager + JDBC → Parquet landing), a DLT CloudFiles pipeline (Autoloader → Bronze streaming_table), and a DAB Workflow (Job → Pipeline orchestration). Implements the LHP creator's planned watermark feature.

## Architecture Decisions

- **Parquet landing zone over Delta**: Autoloader is LHP's strongest pattern; Parquet files are simplest for exactly-once file-based ingestion
- **WatermarkManager over self-watermark**: Implements the creator's vision; centralized HWM table readable by any external tool
- **`jdbc_watermark_v2` source type**: New enum value, not modifying v1 — zero risk to existing configs
- **CloudFiles delegation**: The new generator produces a CloudFiles stub as its primary output and stores the extraction notebook as an auxiliary file
- **Separate generator class**: `JDBCWatermarkJobGenerator` is independent of `JDBCWatermarkLoadGenerator` (v1) — different architectures, no inheritance

## Task List

### Phase 1: Foundation (Models + Config)

- [ ] **Task 1: Config model changes** `[P]`
  - **Files:** `src/lhp/models/config.py`
  - **What:** Add `JDBC_WATERMARK_V2 = "jdbc_watermark_v2"` to `LoadSourceType` enum. Add `landing_path: Optional[str] = None` to `Action` model. Update `BATCH_ONLY_SOURCE_TYPES` to include `JDBC_WATERMARK_V2`.
  - **Acceptance criteria:**
    - [ ] `LoadSourceType("jdbc_watermark_v2")` resolves without error
    - [ ] v1 `LoadSourceType("jdbc_watermark")` unchanged
    - [ ] `Action(name="x", type="load", landing_path="/Volumes/cat/sch/landing/tbl")` instantiates
    - [ ] Existing tests pass
  - **Scope:** XS (1 file, 3 lines)

- [ ] **Task 2: WatermarkConfig model changes** `[P]`
  - **Files:** `src/lhp/models/pipeline_config.py`
  - **What:** Add `source_system_id`, `catalog`, `schema` as `Optional[str]` fields to `WatermarkConfig` with `None` defaults.
  - **Acceptance criteria:**
    - [ ] v1 watermark configs (column + type only) still validate
    - [ ] New fields accessible as `wm.source_system_id`, `.catalog`, `.schema`
  - **Scope:** XS (1 file, 3 lines)
  - **Dependencies:** None

### Checkpoint: Foundation
- [ ] `pytest tests/ -x -q --deselect tests/test_cli.py::TestCLI::test_validate_not_in_project` — all pass
- [ ] No existing YAML configs break

---

### Phase 2: Core Generators (vertical slice — one complete artifact path each)

- [ ] **Task 3: Validator for v2 source type**
  - **Files:** `src/lhp/core/validators/load_validator.py`, `src/lhp/core/config_field_validator.py`
  - **What:** Add `_validate_jdbc_watermark_v2_source()` dispatched when `source.type == "jdbc_watermark_v2"`. Validate: `landing_path` required, watermark config required, `source_system_id` required, JDBC fields required. Warn (not error) for `num_partitions` and non-Volume landing paths.
  - **Acceptance criteria:**
    - [ ] Missing `landing_path` → validation error
    - [ ] Missing `watermark.source_system_id` → validation error
    - [ ] Valid v2 config → passes validation
    - [ ] v1 validator path unchanged
  - **Scope:** S (2 files)
  - **Dependencies:** Tasks 1, 2

- [ ] **Task 4: JDBCWatermarkJobGenerator class** `[P with Task 6]`
  - **Files:** `src/lhp/generators/load/jdbc_watermark_job.py`, `src/lhp/generators/load/__init__.py`
  - **What:** New generator class. `generate()` builds template context, renders extraction notebook via `jdbc_watermark_job.py.j2`, stores it in auxiliary files, constructs synthetic CloudFiles action and delegates to `CloudFilesLoadGenerator` for the DLT stub.
  - **Acceptance criteria:**
    - [ ] Returns CloudFiles stub code (contains `format("cloudFiles")`)
    - [ ] Auxiliary file contains WatermarkManager calls
    - [ ] No DLT decorators in extraction notebook
  - **Scope:** M (2 files, ~150 lines)
  - **Dependencies:** Tasks 1, 2

- [ ] **Task 5: Extraction notebook template**
  - **Files:** `src/lhp/templates/load/jdbc_watermark_job.py.j2`
  - **What:** Jinja2 template for the extraction Job notebook. Imports WatermarkManager, gets HWM, builds filtered JDBC query (full load vs. incremental), writes Parquet, inserts new watermark. Handles timestamp vs. numeric quoting, secret references, `df.cache()` before count+write.
  - **Acceptance criteria:**
    - [ ] Renders without Jinja2 errors
    - [ ] Contains `spark.read.format("jdbc")` and `format("parquet")`
    - [ ] Timestamp watermarks are quoted, numeric are not
    - [ ] Secret refs render as `dbutils.secrets.get()`
    - [ ] Passes Black formatting
  - **Scope:** M (1 file, ~100 lines of template)
  - **Dependencies:** Task 4

- [ ] **Task 6: WorkflowResourceGenerator class** `[P with Task 4]`
  - **Files:** `src/lhp/generators/bundle/workflow_resource.py`, `src/lhp/generators/bundle/__init__.py`
  - **What:** New generator that produces DAB Workflow YAML. Collects v2 load actions, builds task list (one extraction task per action + one DLT pipeline task with depends_on).
  - **Acceptance criteria:**
    - [ ] Returns parseable YAML
    - [ ] Contains notebook tasks + pipeline task
    - [ ] `depends_on` references extraction tasks
  - **Scope:** S (2 files, ~80 lines)
  - **Dependencies:** Task 1 (for type awareness)

- [ ] **Task 7: Workflow YAML template**
  - **Files:** `src/lhp/templates/bundle/workflow_resource.yml.j2`
  - **What:** Jinja2 YAML template for the DAB Workflow resource. Loop over extraction tasks, emit pipeline task with depends_on.
  - **Acceptance criteria:**
    - [ ] Rendered YAML passes `yaml.safe_load()`
    - [ ] Multi-action case produces multiple notebook tasks
  - **Scope:** XS (1 file, ~30 lines)
  - **Dependencies:** Task 6

### Checkpoint: Core Generators
- [ ] Tasks 3-7 complete
- [ ] Generator unit tests can be written (next phase)
- [ ] Manual: instantiate generators, verify template renders

---

### Phase 3: Integration (wiring the pieces together)

- [ ] **Task 8: Register v2 in ActionRegistry**
  - **Files:** `src/lhp/core/action_registry.py`
  - **What:** Map `LoadSourceType.JDBC_WATERMARK_V2` → `JDBCWatermarkJobGenerator`. v1 mapping unchanged.
  - **Acceptance criteria:**
    - [ ] Registry returns correct generator for v2
    - [ ] v1 and CloudFiles mappings unchanged
  - **Scope:** XS (1 file, 1 line)
  - **Dependencies:** Task 4

- [ ] **Task 9: Orchestrator post-generation hook for Workflow YAML**
  - **Files:** `src/lhp/core/orchestrator.py` (or `src/lhp/core/services/code_generator.py`)
  - **What:** After flowgroup generation, check for v2 load actions. If found, call `WorkflowResourceGenerator.generate()` and write Workflow YAML to `resources/lhp/`. Ensure auxiliary files (extraction notebooks) are written to `generated/{env}/{pipeline}/`.
  - **Acceptance criteria:**
    - [ ] `lhp generate` on a v2 config produces all 3 artifacts
    - [ ] Non-v2 flowgroups produce no Workflow YAML
    - [ ] Existing pipeline generation unaffected
  - **Scope:** M (1-2 files, ~30 lines)
  - **Dependencies:** Tasks 4, 6, 8

### Checkpoint: Integration
- [ ] `lhp generate` on a test v2 YAML produces 3 files
- [ ] `lhp generate` on existing v1/CloudFiles YAMLs unchanged
- [ ] Full test suite passes

---

### Phase 4: Tests

- [ ] **Task 10: Unit tests — JDBCWatermarkJobGenerator** `[P]`
  - **Files:** `tests/unit/generators/load/test_jdbc_watermark_job.py`
  - **What:** Test generate() returns CloudFiles stub, auxiliary file contains WatermarkManager, full-load vs. incremental branches, timestamp vs. numeric quoting, secret refs, Black formatting.
  - **Scope:** M (1 file, ~150 lines)
  - **Dependencies:** Tasks 4, 5

- [ ] **Task 11: Unit tests — WorkflowResourceGenerator** `[P]`
  - **Files:** `tests/unit/generators/bundle/test_workflow_resource.py`
  - **What:** Test YAML generation, single and multi-action cases, depends_on structure.
  - **Scope:** S (1 file, ~80 lines)
  - **Dependencies:** Tasks 6, 7

- [ ] **Task 12: Unit tests — v2 validator** `[P]`
  - **Files:** `tests/unit/validators/test_load_validator_watermark_v2.py`
  - **What:** Test every validation rule from Task 3. One test per error code.
  - **Scope:** S (1 file, ~100 lines)
  - **Dependencies:** Task 3

- [ ] **Task 13: Integration test — full YAML to 3 artifacts**
  - **Files:** `tests/integration/test_jdbc_watermark_v2_generation.py`
  - **What:** End-to-end: YAML config → `lhp generate` → verify 3 output files exist and are well-formed. No live Databricks connection.
  - **Scope:** M (1 file, ~120 lines)
  - **Dependencies:** Tasks 1-9

### Checkpoint: Tests
- [ ] `pytest tests/ -x` — full suite green
- [ ] All 24 acceptance criteria from L3 covered

---

### Phase 5: Wumbo Project Updates

- [ ] **Task 14: Replace pg_incremental with generic jdbc_incremental template**
  - **Files:**
    - `/Users/dwtorres/src/Wumbo/templates/jdbc_incremental.yaml` (new — replaces pg_incremental.yaml)
    - `/Users/dwtorres/src/Wumbo/presets/jdbc_pg_defaults.yaml` (existing, verify compatible)
    - `/Users/dwtorres/src/Wumbo/templates/pg_incremental.yaml` (delete — v1 generates non-functional DLT code)
    - Flowgroup YAML: update `use_template: jdbc_incremental`
  - **What:** Create a database-agnostic `jdbc_incremental.yaml` template using `source.type: jdbc_watermark_v2`. Database-specific values (URL format, driver, quoting) come from presets (`jdbc_pg_defaults`, `jdbc_sqlserver_defaults`, `jdbc_oracle_defaults`). The template itself contains no database-specific logic.
  - **Acceptance criteria:**
    - [ ] `lhp generate` against `jdbc_incremental` + `jdbc_pg_defaults` produces all 3 artifacts
    - [ ] `lhp validate` passes
    - [ ] Template works equally with a `jdbc_sqlserver_defaults` preset (URL/driver swap only)
    - [ ] Old `pg_incremental.yaml` removed
  - **Scope:** S (3-4 files)
  - **Dependencies:** Tasks 1-9

### Checkpoint: Complete
- [ ] All 14 tasks done
- [ ] Full test suite green (3098+ tests)
- [ ] `lhp generate` on Wumbo config with `jdbc_incremental` template produces extraction notebook + CloudFiles DLT pipeline + Workflow YAML
- [ ] Template is database-agnostic — works with any JDBC source via presets
- [ ] Ready for deploy and runtime testing

---

## Parallelization Map

```
Phase 1: [Task 1] ─┐
          [Task 2] ─┤
                    ▼
Phase 2: [Task 3]  [Task 4] ──→ [Task 5]   [Task 6] ──→ [Task 7]
                      │                        │
Phase 3:              └───── [Task 8] ─────────┘
                                  │
                            [Task 9]
                                  │
Phase 4: [Task 10]  [Task 11]  [Task 12]   [Task 13]
                                                │
Phase 5:                                  [Task 14]
```

Tasks marked `[P]` can run as parallel agents. Maximum parallelism: 3 agents in Phase 2 (Tasks 3, 4+5, 6+7).

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| CloudFiles delegation pattern doesn't work cleanly | High | Test Task 4 early; fallback: generate CloudFiles stub directly without delegation |
| Auxiliary files mechanism doesn't exist in orchestrator | Medium | Task 9 may need to add this; check existing code first |
| WatermarkManager not installable on Job cluster | Medium | Inline the code in the generated notebook as fallback |
| Autoloader checkpoint doesn't handle re-run correctly | Low | Test in integration; Autoloader is battle-tested for this |

## Resolved Decisions

1. **`landing_path` auto-defaults** to `/Volumes/${catalog}/${schema}/landing/${table}` when not specified. Convention over configuration.
2. **Import from installed wheel.** Generated notebook uses `from lhp.extensions.watermark_manager import WatermarkManager`. LHP is a pip-installable package — inlining code in notebooks is not how LHP works. The DAB Workflow installs the LHP wheel on the Job cluster.
3. **Replace with generic `jdbc_incremental.yaml`.** The v1 `pg_incremental.yaml` generates non-functional code (DLT rejects batch in append_flow) and is PostgreSQL-specific. Replace with a database-agnostic `jdbc_incremental.yaml` template. Database-specific values (URL, driver, quoting) come from presets: `jdbc_pg_defaults`, `jdbc_sqlserver_defaults`, `jdbc_oracle_defaults`.
