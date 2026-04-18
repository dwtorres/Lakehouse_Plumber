# Testing

## Test Framework

- `pytest` with markers declared in `pyproject.toml`
- Source is added to `pythonpath` as `src`

## Relevant Coverage For Current Work

- `tests/unit/generators/load/test_jdbc_watermark_job.py` covers generator output shape
- `tests/templates/test_jdbc_watermark_template.py` uses AST-level checks for the extraction template
- `tests/extensions/watermark_manager/` covers watermark runtime semantics
- `tests/unit/generators/bundle/test_workflow_resource.py` covers DAB workflow rendering
- `tests/unit/core/test_orchestrator_workflow_resource.py` covers orchestrator workflow hooks
- `tests/test_jdbc_watermark_v2_integration.py` validates end-to-end generation from YAML
- `tests/scripts/test_benchmark_slice_a.py` covers the benchmark harness

## Test Characteristics

- A lot of legacy coverage is structural rather than runtime-executed against Databricks
- Template tests are strict about control-flow placement and emitted Python shape
- Newer watermark manager tests are closer to behavioral correctness and concurrency semantics

## Gaps To Keep In Mind

- No true local Databricks execution in the repo tests
- Failure-path recovery around landed-but-not-finalized extraction still needs explicit test coverage
- Stress/perf validation of repeated retries and workflow serial-vs-parallel source pressure is not yet comprehensive

