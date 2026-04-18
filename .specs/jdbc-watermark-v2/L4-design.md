# L4 - Design Document: JDBC Watermark v2 (Autoloader Landing)

**Feature**: JDBC watermark incremental ingestion via WatermarkManager + Autoloader landing zone
**Version**: 2.0
**Date**: 2026-04-14
**Status**: Draft
**Traces to**: [L3 Feature Spec](L3-feature-spec.md), [L2 SRS](L2-srs.md)
**Supersedes**: N/A (v1 L4 covers a different architecture — self-watermark inside DLT)

---

## 1. Component Design

### 1.1 New Files

| File | Purpose |
|------|---------|
| `src/lhp/generators/load/jdbc_watermark_job.py` | `JDBCWatermarkJobGenerator` class |
| `src/lhp/generators/bundle/workflow_resource.py` | `WorkflowResourceGenerator` class |
| `src/lhp/generators/bundle/__init__.py` | Export `WorkflowResourceGenerator` |
| `src/lhp/templates/load/jdbc_watermark_job.py.j2` | Extraction notebook template |
| `src/lhp/templates/bundle/workflow_resource.yml.j2` | DAB Workflow YAML template |
| `tests/unit/generators/load/test_jdbc_watermark_job.py` | Generator unit tests |
| `tests/unit/generators/bundle/test_workflow_resource.py` | Workflow generator tests |
| `tests/unit/validators/test_load_validator_watermark_v2.py` | Validation unit tests |
| `tests/integration/test_jdbc_watermark_v2_generation.py` | Full artifact generation test |

### 1.2 Modified Files

| File | Change |
|------|--------|
| `src/lhp/models/config.py` | Add `JDBC_WATERMARK_V2` to `LoadSourceType`; add `landing_path: Optional[str]` to `Action` |
| `src/lhp/models/pipeline_config.py` | Add `source_system_id`, `catalog`, `schema` to `WatermarkConfig` |
| `src/lhp/core/action_registry.py` | Register `JDBC_WATERMARK_V2` → `JDBCWatermarkJobGenerator` |
| `src/lhp/generators/load/__init__.py` | Export `JDBCWatermarkJobGenerator` |
| `src/lhp/core/validators/load_validator.py` | Add `_validate_jdbc_watermark_v2_source()` |
| `src/lhp/core/orchestrator.py` | Add post-generation hook to write Workflow resource YAML |

### 1.3 Unchanged Files (Reused Without Modification)

| File | How Used |
|------|---------|
| `src/lhp/generators/load/cloudfiles.py` | Generates DLT Autoloader load action |
| `src/lhp/templates/load/cloudfiles.py.j2` | DLT CloudFiles template |
| `src/lhp/generators/write/streaming_table.py` | Generates Bronze streaming table write |
| `src/lhp/templates/write/streaming_table.py.j2` | Streaming table template |

---

## 2. Data Model

### 2.1 Config Model Changes (`src/lhp/models/config.py`)

```python
# In LoadSourceType enum — add after JDBC_WATERMARK:
JDBC_WATERMARK_V2 = "jdbc_watermark_v2"

# In Action model — add after existing fields:
landing_path: Optional[str] = None
```

`landing_path` must be `Optional[str]` with `None` default to preserve full backward compatibility for all existing source types.

### 2.2 WatermarkConfig Changes (`src/lhp/models/pipeline_config.py`)

Add three new optional fields to `WatermarkConfig`:

```python
class WatermarkConfig(BaseModel):
    column: str
    type: WatermarkType
    operator: str = Field(">=", pattern=r"^(>=|>)$")
    # v2 additions:
    source_system_id: Optional[str] = None       # WatermarkManager key; defaults to URL host
    catalog: Optional[str] = Field(None, alias="catalog")   # WM table catalog; default "metadata"
    schema: Optional[str] = Field(None, alias="schema")     # WM table schema; default "orchestration"
```

All three fields are optional. The generator applies defaults when absent.

### 2.3 Template Context: Extraction Notebook

The `JDBCWatermarkJobGenerator.generate()` method builds this context dict, passed to `jdbc_watermark_job.py.j2`:

| Key | Source | Example Value |
|-----|--------|---------------|
| `action_name` | `action.name` | `load_product_jdbc` |
| `pipeline_name` | `context["flowgroup"].pipeline` | `edp_product` |
| `jdbc_url` | `source_config["url"]` | `jdbc:postgresql://host:5432/db` |
| `jdbc_user` | `source_config["user"]` (secret ref preserved) | `${scope/jdbc_user}` |
| `jdbc_password` | `source_config["password"]` (secret ref preserved) | `${scope/jdbc_password}` |
| `jdbc_driver` | `source_config["driver"]` | `org.postgresql.Driver` |
| `jdbc_table` | `source_config["table"]` | `"Production"."Product"` |
| `watermark_column` | `action.watermark.column` | `ModifiedDate` |
| `watermark_type` | `action.watermark.type.value` | `timestamp` |
| `watermark_operator` | `action.watermark.operator` | `>=` |
| `source_system_id` | `action.watermark.source_system_id` or derived | `postgresql_prod` |
| `schema_name` | `source_config.get("schema_name")` or derived | `Production` |
| `table_name` | `source_config.get("table_name")` or derived | `Product` |
| `wm_catalog` | `action.watermark.catalog` or `"metadata"` | `metadata` |
| `wm_schema` | `action.watermark.schema` or `"orchestration"` | `orchestration` |
| `landing_path` | `action.landing_path` | `/Volumes/cat/schema/landing/product` |
| `add_operational_metadata` | `_get_operational_metadata()` | `False` |

### 2.4 Template Context: DAB Workflow YAML

The `WorkflowResourceGenerator.generate()` method builds this context, passed to `workflow_resource.yml.j2`:

| Key | Source | Example Value |
|-----|--------|---------------|
| `pipeline_name` | `flowgroup.pipeline` | `edp_product` |
| `extraction_tasks` | List of `{task_name, notebook_path}` per v2 load action | `[{task_name: "extract_load_product_jdbc", notebook_path: "..."}]` |
| `dlt_pipeline_ref` | DAB reference syntax | `${resources.pipelines.edp_product_pipeline.id}` |
| `dlt_task_name` | `f"dlt_{pipeline_name}"` | `dlt_edp_product` |
| `extraction_task_names` | List of task names for `depends_on` | `["extract_load_product_jdbc"]` |
| `lhp_whl_path` | From bundle substitution params | `${var.lhp_whl_path}` |

### 2.5 Parquet Landing File Structure

Each extraction Job run appends one or more Parquet part files under `landing_path`:

```
/Volumes/{catalog}/{schema}/landing/{table}/
  part-00000-{uuid}.snappy.parquet   ← run N
  part-00000-{uuid}.snappy.parquet   ← run N+1
  ...
```

Files accumulate. Autoloader processes each new file exactly once via its checkpoint. File cleanup is out of scope for v2.

### 2.6 WatermarkManager Delta Table

The table `{wm_catalog}.{wm_schema}.watermarks` is created by `WatermarkManager._ensure_table_exists()` on first instantiation. Schema is fixed and owned by the `WatermarkManager` class. The generated extraction notebook does not emit DDL — it only calls the two API methods: `get_latest_watermark()` and `insert_new()`.

---

## 3. Component Interactions

### 3.1 How `lhp generate` Produces Three Artifacts from One Flowgroup

The v2 architecture requires the orchestrator to produce output files that are **not** the normal DLT pipeline Python file. The mechanism follows the existing `_auxiliary_files` pattern on `FlowGroup`, extended with a new post-generation step for out-of-pipeline-dir resources.

```
lhp generate
  └── ActionOrchestrator.generate_pipeline_code()
        └── for each flowgroup:
              ├── CodeGenerator.generate_flowgroup_code()
              │     ├── load action (jdbc_watermark_v2):
              │     │     JDBCWatermarkJobGenerator.generate()
              │     │       → renders jdbc_watermark_job.py.j2
              │     │       → stores result in flowgroup._auxiliary_files
              │     │           key: "extract_{action_name}.py"
              │     │           value: rendered extraction notebook code
              │     │
              │     ├── load action (cloudfiles synthetic, auto-injected):
              │     │     CloudFilesLoadGenerator.generate()
              │     │       → renders cloudfiles.py.j2
              │     │       → this IS the primary DLT pipeline file
              │     │
              │     └── write action (streaming_table):
              │           StreamingTableWriteGenerator.generate()
              │             → appended to primary DLT pipeline file
              │
              ├── Orchestrator writes primary DLT file:
              │     generated/{env}/{pipeline}/{flowgroup}.py
              │
              ├── Orchestrator writes auxiliary files:
              │     generated/{env}/{pipeline}/extract_{action_name}.py
              │     (one per v2 load action; from _auxiliary_files)
              │
              └── Orchestrator (post-generation hook):
                    WorkflowResourceGenerator.generate()
                      → renders workflow_resource.yml.j2
                      → writes: resources/lhp/{pipeline}_workflow.yml
```

**Key design point**: `JDBCWatermarkJobGenerator` does two things in `generate()`:
1. Returns the CloudFiles load stub code (for the DLT file, replacing itself in the pipeline as a cloud-files action).
2. Stores the extraction notebook code in `flowgroup._auxiliary_files["extract_{action_name}.py"]`.

The orchestrator's existing auxiliary files loop writes the extraction notebook to `pipeline_output_dir / "extract_{action_name}.py"`.

The Workflow YAML is outside `pipeline_output_dir` — it goes to `resources/lhp/`. A new post-generation hook in the orchestrator calls `WorkflowResourceGenerator` after the DLT file is written, using the existing `bundle.manager.BundleManager.resources_dir` path.

### 3.2 Source Type Routing

`ActionRegistry` maps source types to generator classes. The routing table gains one new entry:

| Source Type | Generator Class | Output |
|-------------|----------------|--------|
| `jdbc_watermark` (v1) | `JDBCWatermarkLoadGenerator` | DLT `@dp.temporary_view()` |
| `jdbc_watermark_v2` (new) | `JDBCWatermarkJobGenerator` | DLT CloudFiles stub + extraction notebook auxiliary |
| `cloudfiles` | `CloudFilesLoadGenerator` | DLT CloudFiles load |

`JDBC_WATERMARK` (v1) routing is untouched. The two source types are independent enum values with independent generator registrations.

### 3.3 How CloudFiles DLT File Is Produced for v2

`JDBCWatermarkJobGenerator.generate()` returns the CloudFiles load stub directly — the same code that `CloudFilesLoadGenerator` would produce for a `cloudfiles` action targeting `landing_path`. Internally it calls `CloudFilesLoadGenerator.generate()` with a synthetic action derived from the v2 config, then stores the extraction notebook in `_auxiliary_files`. This avoids duplication of CloudFiles template logic.

The `target` field on the YAML action is used as the Autoloader view name — same as `cloudfiles` actions.

### 3.4 Multi-Action Flowgroup (Two v2 Load Actions)

When a flowgroup has two `jdbc_watermark_v2` load actions:
- Two `extract_{action_name}.py` files are stored in `_auxiliary_files`
- Two CloudFiles stubs are generated into the primary DLT file
- The Workflow YAML has two Job tasks (one per source table), both listed in `depends_on` of the single DLT pipeline task

`WorkflowResourceGenerator` receives the full list of extraction task names and emits all `depends_on` entries accordingly.

---

## 4. Failure Modes

| Failure | When | Behavior | Mitigation |
|---------|------|----------|------------|
| JDBC connection timeout | Extraction Job runtime | Spark raises exception; Job fails; watermark not updated; next run retries from same HWM | `.option("queryTimeout", 3600)` caps at 1 hour; Workflow task-level retries |
| Partial Parquet write (Job OOM/killed mid-write) | Extraction Job runtime | Parquet files may be partially written; watermark not updated; Autoloader may ingest partial file | Autoloader checkpoint records per-file; Silver CDC dedup via MERGE handles partial rows |
| `WatermarkManager` table missing | First extraction Job run | `WatermarkManager.__init__()` calls `_ensure_table_exists()` automatically; table is created; execution continues | Instantiate `WatermarkManager` before JDBC read so table exists before any insert |
| Landing path does not exist | First extraction Job run | Spark `DataFrameWriter` creates the path automatically | No mitigation needed; Volume paths are auto-created by Spark |
| Landing path permission denied | Extraction Job runtime | Spark write raises exception; Job fails; watermark not updated | IAM/Volume WRITE grant for Job cluster SP is a deployment prerequisite; LHP emits warning when `landing_path` doesn't start with `/Volumes/` |
| Job succeeds but DLT pipeline fails | Workflow runtime | DAB Workflow marks Task 2 failed; DLT retries on next trigger; Parquet files are present and will be ingested | Autoloader checkpoint ensures files are processed; no data loss; watermark is already recorded correctly |
| `run_id` collision on Job retry | Extraction Job runtime | `WatermarkManager.insert_new()` raises `ValueError` because `run_id` includes timestamp and MERGE key exists | Each retry generates a new timestamp-based `run_id`; collision only possible within the same second; acceptable in v2 |
| Zero rows returned from JDBC | Extraction Job runtime | `df.count() == 0`; no Parquet files written; `insert_new()` called with `watermark_value = previous_watermark_value`; DLT pipeline ingests nothing | No error raised; Workflow Task 1 and Task 2 both succeed with zero-row batch |
| NULL watermark column in all landed rows | Extraction Job runtime | `MAX(watermark_col)` returns null; `new_hwm` is None | Template checks `new_hwm is None`; falls back to `previous_watermark_value` so HWM does not regress |
| `lhp generate` missing `landing_path` field | Config authoring time | `lhp validate` raises `LHPValidationError` with `LHP-VAL-xxx` before generation runs | Validator enforces `landing_path` is present and non-empty for `jdbc_watermark_v2` source type |

---

## 5. Key Design Decisions

### ADR-1: Parquet files (not Delta table) for the landing zone

**Decision**: Write Parquet files to a Unity Catalog Volume path. Do not write a Delta table.

**Rationale**: Autoloader (`cloudFiles`) reads Parquet natively and maintains a file-level checkpoint. Writing to a Delta table would require either (a) a Delta-source DLT pipeline that cannot use Autoloader's incremental file tracking, or (b) a MERGE-based DLT approach that loses the streaming semantics. Parquet is the natural interchange format between a batch Job and a streaming DLT pipeline. It is schema-preserving, efficient, and the exact format that Autoloader is optimized to process. Volume paths are the recommended storage location in Unity Catalog environments.

### ADR-2: Separate generator class (`JDBCWatermarkJobGenerator`), not an extension of `JDBCWatermarkLoadGenerator`

**Decision**: Create a new `JDBCWatermarkJobGenerator` that does not inherit from `JDBCWatermarkLoadGenerator`.

**Rationale**: The two generators have fundamentally different contracts. `JDBCWatermarkLoadGenerator` (v1) produces a `@dp.temporary_view()` DLT decorator — it is a DLT artifact. `JDBCWatermarkJobGenerator` (v2) produces (a) a plain Python notebook with no DLT decorators and (b) a CloudFiles stub. These are different output shapes, different template files, and different context keys. Forcing inheritance would require a confusing override of every significant method. A clean new class with shared utilities from `BaseActionGenerator` is correct.

### ADR-3: `run_id` generation in the extraction notebook

**Decision**: `run_id` is generated at notebook runtime as `f"{pipeline_name}_{action_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"`.

**Rationale**: `run_id` must be unique per extraction run and meaningful for observability. It cannot be generated at code-generation time (it would be static). It must not rely on Databricks Job `run_id` (not available in notebook scope without extra dbutils calls). The timestamp-based pattern guarantees uniqueness across all but same-second retries, which are acceptable in v2. The pattern matches the format established by existing WatermarkManager test fixtures.

### ADR-4: WatermarkManager is imported (not inlined) in the extraction notebook

**Decision**: The generated extraction notebook contains `from lhp.extensions.watermark_manager import WatermarkManager`. The LHP wheel is installed on the Job cluster via the DAB Workflow `libraries` entry.

**Rationale**: Inlining 710 lines of WatermarkManager source into every generated notebook is unmaintainable. Bug fixes to WatermarkManager would require regenerating all notebooks. The LHP wheel is already the distribution artifact for LHP extensions. The `WorkflowResourceGenerator` emits the `libraries: [{whl: "${var.lhp_whl_path}"}]` entry in the Workflow YAML so the dependency is explicit and version-locked per bundle deploy.

### ADR-5: Workflow YAML lives in `resources/lhp/`, not `generated/`

**Decision**: The DAB Workflow YAML is written to `resources/lhp/{pipeline}_workflow.yml` by the orchestrator's post-generation hook, not inside `generated/{env}/{pipeline}/`.

**Rationale**: `resources/lhp/` is the existing LHP convention for DAB bundle resource files (pipeline YAMLs, job YAMLs). The `BundleManager` already manages this directory. The Workflow YAML is a bundle-level artifact (not environment-specific), consistent with how `pipeline_resource.yml.j2` files are written. Placing it in `generated/` would require custom DAB `include` configuration and break the existing bundle structure.

### ADR-6: `JDBCWatermarkJobGenerator` delegates CloudFiles generation to `CloudFilesLoadGenerator`

**Decision**: `JDBCWatermarkJobGenerator.generate()` constructs a synthetic `cloudfiles` action and calls `CloudFilesLoadGenerator.generate()` to produce the DLT load stub that reads from `landing_path`.

**Rationale**: This avoids duplicating CloudFiles template logic. The CloudFiles generator is already tested, handles all edge cases (schema hints, options validation, stream-only enforcement), and its output is the DLT load artifact consumed by the downstream write action. Reuse is the correct pattern and matches how the v2 feature is described in the SRS ("reuse existing `CloudFilesLoadGenerator` + `cloudfiles.py.j2` for DLT ingestion — no changes").

### ADR-7: `>=` remains the default watermark operator

**Decision**: Default `watermark.operator` is `>=`, inheriting the v1 decision.

**Rationale**: See v1 ADR-2. Silent data loss from `>` with ties is worse than harmless duplicate reads from `>=`. Silver `create_auto_cdc_flow` deduplicates via MERGE on the CDC key, which is the correct deduplication layer.

---

## 6. File Paths

### New Files

```
src/lhp/generators/load/jdbc_watermark_job.py
src/lhp/generators/bundle/__init__.py
src/lhp/generators/bundle/workflow_resource.py
src/lhp/templates/load/jdbc_watermark_job.py.j2
src/lhp/templates/bundle/workflow_resource.yml.j2
tests/unit/generators/load/test_jdbc_watermark_job.py
tests/unit/generators/bundle/__init__.py
tests/unit/generators/bundle/test_workflow_resource.py
tests/unit/validators/test_load_validator_watermark_v2.py
tests/integration/test_jdbc_watermark_v2_generation.py
```

### Modified Files

```
src/lhp/models/config.py
src/lhp/models/pipeline_config.py
src/lhp/core/action_registry.py
src/lhp/generators/load/__init__.py
src/lhp/core/validators/load_validator.py
src/lhp/core/orchestrator.py
```

### Generated Output Paths (at `lhp generate` time)

```
generated/{env}/{pipeline}/extract_{action_name}.py   ← extraction notebook (auxiliary file)
generated/{env}/{pipeline}/{flowgroup}.py             ← DLT pipeline (CloudFiles + write)
resources/lhp/{pipeline}_workflow.yml                  ← DAB Workflow resource
```

---

## 7. Validation Design

`LoadActionValidator._validate_jdbc_watermark_v2_source()` is called when `source.type == "jdbc_watermark_v2"`. It enforces:

```
_validate_jdbc_watermark_v2_source(action, prefix)
├── landing_path is present and non-empty             → LHP-VAL-040
├── watermark config is not None                      → LHP-VAL-041
├── watermark.column is present and non-empty         → LHP-VAL-042
├── watermark.type is one of [timestamp, numeric]     → LHP-VAL-043
├── watermark.source_system_id is present             → LHP-VAL-044
├── source.schema_name is present                     → LHP-VAL-045
├── source.table_name is present                      → LHP-VAL-046
├── source.url is present                             → LHP-VAL-047
├── source.driver is present                          → LHP-VAL-047
├── source.table is present                           → LHP-VAL-047
├── source.user is present                            → LHP-VAL-047
├── source.password is present                        → LHP-VAL-047
├── flowgroup has exactly one streaming_table write   → LHP-VAL-048
└── source.num_partitions present (if set) → WARNING (not error), exit 0
```

Warn (not error) when `landing_path` does not start with `/Volumes/`.

---

## 8. Rollout Plan

1. **Branch**: `watermark` (active)
2. **Order**: Models → Validator → Generator → Templates → Orchestrator hook → Tests
3. **Testing**: Unit tests per component, then integration test with reference YAML
4. **Validation**: `lhp generate` against the sample config in L3 §4.1; verify all three artifacts exist and pass `black` and `yamllint`
5. **Review**: PR against `main` with L2–L4 specs as context
6. **Merge**: After CI pass; no breaking changes to v1 paths
