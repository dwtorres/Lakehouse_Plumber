# Architecture

## Pattern

Lakehouse Plumber is a generator-oriented architecture. Users describe pipelines declaratively in YAML, the processor resolves templates, presets, and substitutions into typed `FlowGroup` objects, validators enforce configuration rules, and generators emit executable Python plus Databricks bundle resources.

## Main Layers

- CLI layer in `src/lhp/cli/` parses commands and reports errors
- Orchestration layer in `src/lhp/core/orchestrator.py` coordinates discovery, validation, generation, and bundle sync
- Domain models in `src/lhp/models/` define the typed contract for actions, write targets, and watermark config
- Generator layer in `src/lhp/generators/` renders Jinja templates and emits code artifacts
- Extension layer in `src/lhp/extensions/` holds runtime code that generated notebooks import

## Data Flow

1. YAML is discovered and parsed by `FlowgroupDiscoverer` and `YAMLParser`
2. `FlowgroupProcessor` expands templates, applies presets, performs substitutions, and validates
3. `CodeGenerator` routes each action to a type-specific generator through `ActionRegistry`
4. Primary generated files are written under `generated/<env>/<pipeline>/`
5. Auxiliary files are carried on `FlowGroup._auxiliary_files` and written beside the primary output
6. Bundle resource generation and sync write `resources/lhp/*.yml`

## jdbc_watermark_v2 Slice

- Source validation starts in `src/lhp/core/validators/load_validator.py`
- The generator `src/lhp/generators/load/jdbc_watermark_job.py` emits:
  - one extraction notebook as an auxiliary file
  - one CloudFiles-based Bronze stub as the primary generated file
- The extraction notebook imports `WatermarkManager` from `src/lhp/extensions/watermark_manager/`
- Workflow generation collects v2 actions by pipeline and emits one Databricks job that fans extraction tasks into a DLT pipeline task

## Architectural Pressure Points

- The v2 generator currently bridges two architectures: JDBC extraction and CloudFiles ingestion
- Preset/default application is source-type keyed before generation, which makes v2-to-cloudfiles inheritance non-trivial
- Runtime correctness spans generated notebook structure and watermark manager SQL semantics, so fixes often need coordinated changes across both

