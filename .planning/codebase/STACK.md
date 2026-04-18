# Stack

## Languages

- Python 3.11+ for CLI, models, orchestration, validators, generators, and bundle support
- YAML for flowgroup, preset, substitution, and bundle configuration
- Jinja2 templates for generated Databricks Python and resource YAML
- SQL for Spark transformations, validation, and watermark-table DML
- Bash for local benchmarking and helper workflows

## Runtime

- Python package defined in `pyproject.toml`
- CLI entrypoint `lhp` via `src/lhp/cli/main.py`
- Databricks Lakeflow / DLT runtime is the primary execution target for generated code
- Databricks Asset Bundles support is built into the generation flow when `databricks.yml` exists

## Core Libraries

- `click` for CLI commands in `src/lhp/cli/`
- `pydantic` for config models in `src/lhp/models/`
- `jinja2` for templates in `src/lhp/templates/`
- `ruamel.yaml` and `pyyaml` for parsing and preserving YAML structure
- `networkx` for dependency analysis in `src/lhp/core/dependency_resolver.py`
- `black`, `isort`, `flake8`, `mypy` for code quality
- `pytest` plus markers for unit, integration, e2e, and performance coverage

## Important Package Areas

- `src/lhp/core/` coordinates discovery, validation, orchestration, and state
- `src/lhp/models/` holds typed config models like `Action`, `FlowGroup`, `WatermarkConfig`
- `src/lhp/generators/` turns validated configs into executable artifacts
- `src/lhp/templates/` holds the Jinja sources for generated code and bundle YAML
- `src/lhp/extensions/watermark_manager/` contains the watermark runtime used by `jdbc_watermark_v2`
- `tests/` mixes broad legacy coverage with newer targeted v2 hardening tests

## Configuration Surfaces

- `lhp.yaml` for project metadata and global settings
- `pipelines/**/*.yaml` for flowgroups
- `presets/**/*.yaml` for defaults layered onto actions
- `substitutions/<env>.yaml` for environment values and secrets
- `config/pipeline_config.yaml` and `config/job_config.yaml` for bundle generation behavior
- `databricks.yml` for user-managed bundle deployment config

## Current Technical Focus

- `jdbc_watermark_v2` uses `src/lhp/generators/load/jdbc_watermark_job.py` plus `src/lhp/templates/load/jdbc_watermark_job.py.j2`
- Generated DAB workflow support lives in `src/lhp/generators/bundle/workflow_resource.py`
- The runtime correctness boundary is the watermark manager package in `src/lhp/extensions/watermark_manager/`

