# Structure

## Top-Level Layout

- `src/lhp/` - package source
- `tests/` - unit, integration, template, extension, e2e, and performance coverage
- `docs/` - user-facing documentation
- `scripts/` - local benchmark and verification helpers
- `.specs/` - prior design/spec artifacts for jdbc watermark work

## High-Value Directories

### `src/lhp/core/`

- `orchestrator.py` - main generation entrypoint
- `action_registry.py` - action subtype to generator mapping
- `validators/` - action-specific validation logic
- `services/` - flowgroup processing, config loading, and code generation helpers

### `src/lhp/models/`

- `config.py` - `Action`, `FlowGroup`, enums, write target config
- `pipeline_config.py` - `WatermarkConfig` and pipeline-specific models

### `src/lhp/generators/`

- `load/` - load generators including `cloudfiles.py`, `jdbc.py`, `jdbc_watermark_job.py`
- `bundle/` - bundle resource generators such as `workflow_resource.py`
- `transform/`, `write/`, `test/` - other action families

### `src/lhp/templates/`

- `load/` - generated notebook/template sources
- `bundle/` - Databricks resource YAML templates
- `init/` - project bootstrap templates

### `src/lhp/extensions/watermark_manager/`

- `__init__.py` - stable export surface
- `_manager.py` - SQL/DML state machine
- `runtime.py` - run id helpers
- `sql_safety.py` - SQL validator and emitter utilities
- `exceptions.py` - watermark-specific exception taxonomy

## Test Layout

- `tests/unit/` - isolated class/function coverage
- `tests/templates/` - AST and render-shape validation for templates
- `tests/extensions/watermark_manager/` - runtime hardening tests
- `tests/scripts/` - benchmark harness tests
- `tests/test_jdbc_watermark_v2_integration.py` - YAML-to-artifacts integration coverage

