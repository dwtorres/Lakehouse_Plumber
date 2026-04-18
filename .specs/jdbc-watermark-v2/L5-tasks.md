# L5 - Execution Tasks: JDBC Watermark v2 (Autoloader Landing)

**Feature**: JDBC watermark incremental ingestion via WatermarkManager + Autoloader landing zone
**Version**: 2.0
**Date**: 2026-04-14
**Status**: Draft
**Traces to**: [L4 Design](L4-design.md), [L3 Feature Spec](L3-feature-spec.md), [L2 SRS](L2-srs.md)
**Branch**: `watermark`

---

## Dependency Order Summary

```
Task 1 (models/config.py)
Task 2 (models/pipeline_config.py)
  └─ Task 3 (load_validator.py)          [needs Task 1, 2]
  └─ Task 4 (JDBCWatermarkJobGenerator)  [needs Task 1, 2]
       └─ Task 5 (jdbc_watermark_job.py.j2)   [needs Task 4]
Task 6 (WorkflowResourceGenerator)       [independent of Task 4]
  └─ Task 7 (workflow_resource.yml.j2)   [needs Task 6]
Task 8 (action_registry.py)              [needs Task 4]
Task 9 (orchestrator.py)                 [needs Task 4, 6, 8]
Tasks 10–13 (tests)                      [need Tasks 3–9]
Task 14 (Wumbo config updates)           [needs Tasks 1–9]
```

---

## Task 1: Add `JDBC_WATERMARK_V2` to `LoadSourceType` and `landing_path` to `Action`

**Files:** `src/lhp/models/config.py`
**Depends on:** Nothing
**Parallel:** [P] with Task 2

### What

Add `JDBC_WATERMARK_V2 = "jdbc_watermark_v2"` to the `LoadSourceType` enum. Add `landing_path: Optional[str] = None` to the `Action` model. Both changes must be backward-compatible — all existing source types and existing YAML configs without `landing_path` must continue to work unchanged.

### Done Criteria

- [ ] `LoadSourceType("jdbc_watermark_v2")` does not raise
- [ ] `LoadSourceType("jdbc_watermark")` (v1) still works — v1 enum value unchanged
- [ ] `Action(name="x", type="load", source={"type": "cloudfiles"})` instantiates without `landing_path` (backward compat)
- [ ] `Action(..., landing_path="/Volumes/cat/schema/landing/tbl")` instantiates correctly
- [ ] `Action(..., landing_path=None)` explicitly passes Pydantic validation
- [ ] No existing unit tests broken

---

## Task 2: Add `source_system_id`, `catalog`, `schema` fields to `WatermarkConfig`

**Files:** `src/lhp/models/pipeline_config.py`
**Depends on:** Nothing
**Parallel:** [P] with Task 1

### What

Add three optional fields to the `WatermarkConfig` Pydantic model: `source_system_id: Optional[str] = None`, `catalog: Optional[str] = None`, and `schema: Optional[str] = None`. All three must be `Optional` with `None` defaults so existing `jdbc_watermark` (v1) configs that omit them continue to validate.

### Done Criteria

- [ ] `WatermarkConfig(column="ModifiedDate", type="timestamp")` (v1 pattern) still validates
- [ ] `WatermarkConfig(column="x", type="timestamp", source_system_id="pg_prod", catalog="metadata", schema="orchestration")` validates
- [ ] Fields are accessible as `wm_config.source_system_id`, `.catalog`, `.schema`
- [ ] No existing unit tests broken

---

## Task 3: Add `_validate_jdbc_watermark_v2_source()` to `LoadActionValidator`

**Files:** `src/lhp/core/validators/load_validator.py`
**Depends on:** Task 1, Task 2

### What

Add a private method `_validate_jdbc_watermark_v2_source(action, prefix)` that is called when `source.type == "jdbc_watermark_v2"`. It enforces all required fields from L4 §7. Wire it into the existing `validate()` dispatch so it is invoked automatically during `lhp validate`. The v1 `jdbc_watermark` validation path must remain completely unchanged.

Validation rules to enforce (each maps to an `LHP-VAL-04x` error code per L4 §7):

- `landing_path` present and non-empty → `LHP-VAL-040`
- `watermark` config present → `LHP-VAL-041`
- `watermark.column` present → `LHP-VAL-042`
- `watermark.type` in `{timestamp, numeric}` → `LHP-VAL-043`
- `watermark.source_system_id` present → `LHP-VAL-044`
- `source.schema_name` present → `LHP-VAL-045`
- `source.table_name` present → `LHP-VAL-046`
- `source.url`, `source.driver`, `source.table`, `source.user`, `source.password` all present → `LHP-VAL-047`
- Flowgroup contains exactly one `streaming_table` write → `LHP-VAL-048`
- `source.num_partitions` present → emit WARNING (not error), continue with exit 0
- `landing_path` not starting with `/Volumes/` → emit WARNING (not error)

### Done Criteria

- [ ] `lhp validate` on a config missing `landing_path` exits non-zero; error contains "landing_path is required"
- [ ] `lhp validate` on a config missing `watermark.column` exits non-zero; error contains "watermark.column is required"
- [ ] `lhp validate` on a config with `watermark.type: "date"` exits non-zero; error lists valid values
- [ ] `lhp validate` on a config with `source.num_partitions` exits 0 and prints a warning to stderr
- [ ] `lhp validate` on a config with `landing_path: "abfss://..."` exits 0 and prints a warning
- [ ] `lhp validate` on a fully valid `jdbc_watermark_v2` config exits 0
- [ ] All existing v1 `jdbc_watermark` validator tests still pass

---

## Task 4: Create `JDBCWatermarkJobGenerator` class

**Files:** `src/lhp/generators/load/jdbc_watermark_job.py`, `src/lhp/generators/load/__init__.py`
**Depends on:** Task 1, Task 2

### What

Create `JDBCWatermarkJobGenerator` as a new class (not a subclass of `JDBCWatermarkLoadGenerator`) that inherits from `BaseActionGenerator`. Implement the `generate()` method to:

1. Build the template context from the action config (see L4 §2.3 for the full key list)
2. Render `load/jdbc_watermark_job.py.j2` → extraction notebook code
3. Store the extraction notebook in `flowgroup._auxiliary_files["extract_{action_name}.py"]`
4. Construct a synthetic `cloudfiles` action targeting `landing_path` and delegate to `CloudFilesLoadGenerator.generate()` to produce the DLT CloudFiles stub
5. Return the CloudFiles stub code (this becomes the primary DLT file content for this action)

Context derivation rules:
- `source_system_id`: use `action.watermark.source_system_id` if set; otherwise derive from URL host
- `schema_name`: use `source_config.get("schema_name")`; required (validator enforces it)
- `table_name`: use `source_config.get("table_name")`; required (validator enforces it)
- `wm_catalog`: use `action.watermark.catalog` or `"metadata"`
- `wm_schema`: use `action.watermark.schema` or `"orchestration"`

Export `JDBCWatermarkJobGenerator` from `src/lhp/generators/load/__init__.py`.

### Done Criteria

- [ ] `JDBCWatermarkJobGenerator` is importable from `lhp.generators.load`
- [ ] `generate()` returns CloudFiles stub code (string contains `format("cloudFiles")`)
- [ ] After `generate()`, `flowgroup._auxiliary_files["extract_{action_name}.py"]` contains extraction notebook code
- [ ] Extraction notebook code contains `from lhp.extensions.watermark_manager import WatermarkManager`
- [ ] Extraction notebook code contains `WatermarkManager.get_latest_watermark(` call
- [ ] Extraction notebook code contains `WatermarkManager.insert_new(` call
- [ ] Extraction notebook code does NOT contain `@dp.temporary_view` or any DLT decorator
- [ ] `JDBCWatermarkLoadGenerator` (v1) is not imported or referenced in this file

---

## Task 5: Create `load/jdbc_watermark_job.py.j2` template

**Files:** `src/lhp/templates/load/jdbc_watermark_job.py.j2`
**Depends on:** Task 4

### What

Create the Jinja2 template for the extraction Job notebook. The template produces a plain Python notebook (no DLT decorators). Key behaviors to template (see L3 §4.2 and L2 FR-03 through FR-06 for exact logic):

- Import block: `from lhp.extensions.watermark_manager import WatermarkManager`
- `WatermarkManager` instantiation with `catalog`, `schema`
- `get_latest_watermark(source_system_id, schema_name, table_name)` call
- Conditional JDBC query construction:
  - Full load branch (`hwm_value is None`): no WHERE clause
  - Incremental timestamp branch: `WHERE {col} {operator} '{hwm_value}'`
  - Incremental numeric branch: `WHERE {col} {operator} {hwm_value}` (unquoted)
- `spark.read.format("jdbc")` with `.option("queryTimeout", 3600)`
- `df.cache()` before `row_count = df.count()`
- `df.repartition(1)` when `row_count < 1_000_000`, else `df.repartition(4)` (NFR-02)
- `df.write.mode("append").format("parquet").save(landing_path)`
- `run_id` constructed as `f"{pipeline_name}_{action_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"`
- `insert_new()` call with `extraction_type = "full" if hwm_value is None else "incremental"`
- NULL HWM guard: if `new_hwm is None`, fall back to `previous_watermark_value`
- Secret reference substitution: `${secret:scope/key}` rendered as `dbutils.secrets.get(scope="scope", key="key")`

### Done Criteria

- [ ] Template renders without Jinja2 errors for a minimal valid context dict
- [ ] Rendered output contains `format("jdbc")`
- [ ] Rendered output contains `format("parquet")`
- [ ] When `watermark_type == "timestamp"`, rendered WHERE clause quotes the HWM value
- [ ] When `watermark_type == "numeric"`, rendered WHERE clause does not quote the HWM value
- [ ] When `watermark_operator == ">"`, rendered WHERE clause uses `>`
- [ ] `extraction_type` is `"full"` in the None-HWM branch and `"incremental"` in the HWM-present branch
- [ ] Secret refs like `${secret:scope/jdbc_user}` render as `dbutils.secrets.get(scope="scope", key="jdbc_user")`
- [ ] Rendered output passes `black.format_str()` at line-length=88 without changes

---

## Task 6: Create `WorkflowResourceGenerator` class

**Files:** `src/lhp/generators/bundle/workflow_resource.py`, `src/lhp/generators/bundle/__init__.py`
**Depends on:** Nothing (can start once Task 1 is done for type awareness; functionally independent of Task 4)
**Parallel:** [P] with Task 4 and Task 5

### What

Create `WorkflowResourceGenerator` with a `generate(flowgroup, context)` method that:

1. Collects all `jdbc_watermark_v2` load actions from the flowgroup
2. Builds the template context per L4 §2.4:
   - `extraction_tasks`: list of `{task_name, notebook_path}` — one per v2 load action
   - `dlt_pipeline_ref`: `"${resources.pipelines.{pipeline_name}_pipeline.id}"`
   - `dlt_task_name`: `f"dlt_{pipeline_name}"`
   - `extraction_task_names`: list of task names for `depends_on`
   - `lhp_whl_path`: `"${var.lhp_whl_path}"`
3. Renders `bundle/workflow_resource.yml.j2`
4. Returns the rendered YAML string

Export `WorkflowResourceGenerator` from `src/lhp/generators/bundle/__init__.py`.

### Done Criteria

- [ ] `WorkflowResourceGenerator` is importable from `lhp.generators.bundle`
- [ ] `generate()` returns a non-empty YAML string
- [ ] Returned YAML is parseable by `yaml.safe_load()`
- [ ] YAML contains `resources.jobs.{pipeline_name}_workflow`
- [ ] YAML contains two tasks: one notebook task and one pipeline task
- [ ] Pipeline task `depends_on` references the notebook task name
- [ ] `libraries` entry contains `whl: "${var.lhp_whl_path}"`
- [ ] With two v2 load actions, YAML contains two notebook tasks both listed in pipeline task `depends_on`

---

## Task 7: Create `bundle/workflow_resource.yml.j2` template

**Files:** `src/lhp/templates/bundle/workflow_resource.yml.j2`
**Depends on:** Task 6

### What

Create the Jinja2 YAML template for the DAB Workflow resource. The template must emit valid DAB YAML (see L2 §5.4 and FR-09). Key structure:

```yaml
resources:
  jobs:
    {pipeline_name}_workflow:
      name: {pipeline_name}_workflow
      tasks:
        # One entry per extraction task (loop over extraction_tasks):
        - task_key: {task_name}
          notebook_task:
            notebook_path: {notebook_path}
          libraries:
            - whl: ${var.lhp_whl_path}
        # DLT pipeline task:
        - task_key: {dlt_task_name}
          pipeline_task:
            pipeline_id: {dlt_pipeline_ref}
          depends_on:
            # One entry per extraction task:
            - task_key: {extraction_task_name}
```

Emit a comment directing users to add JDBC driver JARs to the `libraries` block (per L2 §5.1).

### Done Criteria

- [ ] Template renders without Jinja2 errors
- [ ] Rendered YAML is valid (`yaml.safe_load()` does not raise)
- [ ] Rendered YAML contains `pipeline_id: ${resources.pipelines.`
- [ ] `depends_on` block references all extraction task keys
- [ ] A comment about JDBC driver JARs is present in the rendered output
- [ ] With two extraction tasks, rendered YAML has two notebook task entries and `depends_on` lists both

---

## Task 8: Register `JDBC_WATERMARK_V2` in `ActionRegistry`

**Files:** `src/lhp/core/action_registry.py`
**Depends on:** Task 4

### What

Add one entry to the load source type → generator mapping: `LoadSourceType.JDBC_WATERMARK_V2` → `JDBCWatermarkJobGenerator`. The v1 `JDBC_WATERMARK` → `JDBCWatermarkLoadGenerator` mapping must remain unchanged.

### Done Criteria

- [ ] `ActionRegistry().get_load_generator(LoadSourceType.JDBC_WATERMARK_V2)` returns `JDBCWatermarkJobGenerator`
- [ ] `ActionRegistry().get_load_generator(LoadSourceType.JDBC_WATERMARK)` still returns `JDBCWatermarkLoadGenerator` (v1 unchanged)
- [ ] `ActionRegistry().get_load_generator(LoadSourceType.CLOUDFILES)` still returns `CloudFilesLoadGenerator`
- [ ] No existing registry tests broken

---

## Task 9: Add post-generation hook to `ActionOrchestrator` for Workflow YAML

**Files:** `src/lhp/core/orchestrator.py`
**Depends on:** Task 4, Task 6, Task 8

### What

Add a post-generation step to `ActionOrchestrator.generate_pipeline_code()` (or the equivalent flowgroup processing loop) that:

1. After writing the primary DLT file and auxiliary files for a flowgroup, checks if any load action has `source.type == "jdbc_watermark_v2"`
2. If yes, calls `WorkflowResourceGenerator.generate(flowgroup, context)` to produce the Workflow YAML
3. Writes the result to `resources/lhp/{pipeline_name}_workflow.yml` using `BundleManager.resources_dir` (or the equivalent path resolution)

The orchestrator must also ensure auxiliary files (`extract_{action_name}.py`) are written from `flowgroup._auxiliary_files` into `generated/{env}/{pipeline}/`. Confirm this mechanism already exists for auxiliary files from v1 patterns; if not, add it.

### Done Criteria

- [ ] `lhp generate` on a valid `jdbc_watermark_v2` flowgroup produces all three artifacts:
  - `generated/{env}/{pipeline}/extract_{action_name}.py` (extraction notebook)
  - `generated/{env}/{pipeline}/{target}.py` (DLT CloudFiles file)
  - `resources/lhp/{pipeline}_workflow.yml` (DAB Workflow YAML)
- [ ] `lhp generate` on a non-`jdbc_watermark_v2` flowgroup does NOT create a Workflow YAML
- [ ] `lhp generate` exits 0 on a valid config
- [ ] Existing flowgroup processing (non-v2 source types) is unaffected

---

## Task 10: Unit tests for `JDBCWatermarkJobGenerator`

**Files:** `tests/unit/generators/load/test_jdbc_watermark_job.py`
**Depends on:** Tasks 4, 5

### What

Create unit tests for `JDBCWatermarkJobGenerator`. Use mock flowgroup and action objects. Test the template rendering paths rather than Databricks runtime behavior.

Tests to include:
- `generate()` returns a string containing `format("cloudFiles")` (CloudFiles stub output)
- `flowgroup._auxiliary_files` contains the extraction notebook after `generate()`
- Extraction notebook string contains `WatermarkManager`
- Extraction notebook with `hwm_value=None` renders no WHERE clause (full load path)
- Extraction notebook with `hwm_value="2025-06-01 00:00:00"` and `watermark_type="timestamp"` renders quoted WHERE clause
- Extraction notebook with `hwm_value=12345` and `watermark_type="numeric"` renders unquoted WHERE clause
- `watermark_operator=">"` renders strict comparison
- Secret reference `${secret:scope/jdbc_user}` renders as `dbutils.secrets.get(...)`
- Rendered extraction notebook passes Black formatting (AC-20)
- `source_system_id` defaults to URL host when not explicitly set

### Done Criteria

- [ ] All tests pass via `pytest tests/unit/generators/load/test_jdbc_watermark_job.py`
- [ ] Coverage covers both full-load and incremental branches
- [ ] Coverage covers both timestamp and numeric watermark type branches
- [ ] No imports of Databricks runtime libraries (all mocked)

---

## Task 11: Unit tests for `WorkflowResourceGenerator`

**Files:** `tests/unit/generators/bundle/__init__.py`, `tests/unit/generators/bundle/test_workflow_resource.py`
**Depends on:** Tasks 6, 7
**Parallel:** [P] with Task 10

### What

Create unit tests for `WorkflowResourceGenerator`. Mock flowgroup objects with 1 and 2 v2 load actions.

Tests to include:
- `generate()` returns a non-empty string
- Returned YAML is parseable
- Single-action: YAML has one notebook task, one pipeline task, correct `depends_on`
- Two-action: YAML has two notebook tasks, both appear in `depends_on` of the pipeline task
- `pipeline_id` contains `${resources.pipelines.`
- `libraries` entry contains `whl`

### Done Criteria

- [ ] All tests pass via `pytest tests/unit/generators/bundle/test_workflow_resource.py`
- [ ] Single-action and multi-action cases both covered
- [ ] Generated YAML passes `yaml.safe_load()` without error

---

## Task 12: Unit tests for `LoadActionValidator` (v2 rules)

**Files:** `tests/unit/validators/test_load_validator_watermark_v2.py`
**Depends on:** Task 3
**Parallel:** [P] with Tasks 10, 11

### What

Create unit tests for `_validate_jdbc_watermark_v2_source()`. Build minimal mock action objects for each validation rule.

Tests to include (one per validation rule, mapping to AC-03 through AC-07):
- Missing `landing_path` → error, exit non-zero, message contains "landing_path is required"
- Missing `watermark.column` → error
- Missing `watermark.type` → error
- Invalid `watermark.type` value → error listing valid values
- Missing `watermark.source_system_id` → error
- Missing `source.schema_name` → error
- Missing `source.table_name` → error
- Missing `source.url` → error
- `source.num_partitions` set → warning, no error, exit 0
- `landing_path` not starting with `/Volumes/` → warning, no error, exit 0
- Fully valid config → no errors, no warnings

### Done Criteria

- [ ] All tests pass via `pytest tests/unit/validators/test_load_validator_watermark_v2.py`
- [ ] Every `LHP-VAL-04x` error code has at least one test covering it
- [ ] Warning cases assert exit code 0 (no error raised)
- [ ] Existing `test_load_validator_watermark.py` (v1 tests) still pass

---

## Task 13: Integration test — full YAML to three-artifact generation

**Files:** `tests/integration/test_jdbc_watermark_v2_generation.py`
**Depends on:** Tasks 1–9

### What

Create an integration test that runs `lhp generate` against a reference `jdbc_watermark_v2` flowgroup YAML (inline or fixture file) and asserts all three output artifacts exist and are well-formed. Use a temp directory for output. Do not require a live Databricks connection.

Reference config: use the complete YAML example from L3 §4.1.

Tests to include:
- `lhp generate` exits 0 (AC-19)
- `generated/{env}/{pipeline}/extract_{action_name}.py` exists (AC-21)
- `generated/{env}/{pipeline}/{target}.py` exists and contains `format("cloudFiles")` (AC-17, AC-21)
- `resources/lhp/{pipeline}_workflow.yml` exists (AC-21)
- Workflow YAML parses as valid YAML and contains `depends_on` structure (AC-18)
- Extraction notebook passes Black formatting (AC-20)
- Secret references render as `dbutils.secrets.get(...)` calls (AC-23)
- Two-action flowgroup produces two extraction notebooks (AC-22)
- v1 `jdbc_watermark` flowgroup still generates as before (backward compat / AC-09)

### Done Criteria

- [ ] All assertions pass against generated file content
- [ ] Test runs in isolation without Databricks credentials
- [ ] `pytest tests/integration/test_jdbc_watermark_v2_generation.py` exits 0

---

## Task 14: Update Wumbo project configs for v2

**Files:**
- `/Users/dwtorres/src/Wumbo/templates/pg_incremental.yaml` (update existing)
- `/Users/dwtorres/src/Wumbo/resources/lhp/jdbc_ingestion.pipeline.yml` (may need `workflow` sibling)
**Depends on:** Tasks 1–9 (LHP must generate v2 artifacts correctly first)

### What

Update the Wumbo project's `pg_incremental.yaml` template to use `source.type: jdbc_watermark_v2` and include the new required fields (`landing_path`, `watermark_manager`, `source_system_id`, `schema_name`, `table_name`). Add a new template parameter for `landing_path`. Update or add a `_workflow.yml` pipeline resource file in `resources/lhp/` if LHP does not generate it automatically into that location.

If the `pg_incremental.yaml` template serves both v1 and v2 use cases, create a new `pg_incremental_v2.yaml` instead of modifying the existing one, to avoid breaking any live Wumbo pipelines using v1.

Changes:
- Bump `version` to `"2.0"` (or use new file `pg_incremental_v2.yaml`)
- Change `source.type` from `jdbc_watermark` to `jdbc_watermark_v2`
- Add `landing_path` parameter (required)
- Add `watermark_manager.catalog` and `watermark_manager.schema` parameters (optional, with defaults)
- Add `source_system_id`, `schema_name`, `table_name` fields to the action
- Remove `readMode: batch` if present (v2 generator does not require it)

### Done Criteria

- [ ] Running `lhp generate` against the updated Wumbo template (with sample parameter values) produces all three artifacts
- [ ] Generated extraction notebook contains correct `landing_path` value
- [ ] Generated Workflow YAML exists at `resources/lhp/{pipeline}_workflow.yml`
- [ ] Existing v1 pipelines using the old `pg_incremental.yaml` are unaffected (if file kept, or confirmed no live v1 uses)
- [ ] `lhp validate` against the updated config exits 0

---

## Acceptance Criteria Cross-Reference

| AC ID | Covered By |
|-------|-----------|
| AC-01 | Task 1 |
| AC-02 | Task 1 |
| AC-03 | Tasks 3, 12 |
| AC-04 | Tasks 3, 12 |
| AC-05 | Tasks 3, 12 |
| AC-06 | Tasks 3, 12 |
| AC-07 | Tasks 3, 12 |
| AC-08 | Task 8 |
| AC-09 | Tasks 8, 13 |
| AC-10 | Tasks 4, 5, 10 |
| AC-11 | Tasks 5, 10 |
| AC-12 | Tasks 5, 10 |
| AC-13 | Tasks 5, 10 |
| AC-14 | Tasks 5, 10 |
| AC-15 | Tasks 5, 10 |
| AC-16 | Tasks 5, 10 |
| AC-17 | Tasks 4, 5, 13 |
| AC-18 | Tasks 6, 7, 11, 13 |
| AC-19 | Tasks 9, 13 |
| AC-20 | Tasks 5, 10, 13 |
| AC-21 | Tasks 9, 13 |
| AC-22 | Tasks 4, 6, 13 |
| AC-23 | Tasks 5, 10, 13 |
| AC-24 | (WatermarkManager already tested; confirm existing test covers `_ensure_table_exists`) |
