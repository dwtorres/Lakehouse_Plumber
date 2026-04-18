# Conventions

## Naming

- Modules use lowercase snake case like `workflow_resource.py`
- Pydantic models and enums live in `src/lhp/models/`
- Generator classes follow `<Subtype><Action>Generator`
- Test modules use `test_*.py`

## Style

- Black line length is 88
- `isort` uses Black profile
- `mypy` is strict for source, relaxed for tests
- Error handling prefers typed `LHPError` subclasses with formatted context

## Generator Conventions

- Generators inherit `BaseActionGenerator`
- Templates are rendered from `src/lhp/templates/...`
- Secrets are resolved in generated Python code, not by string concatenation at runtime
- Flowgroup auxiliary artifacts are stored in `FlowGroup._auxiliary_files`

## Validation Conventions

- Unknown source fields are blocked in `ConfigFieldValidator`
- Type-specific checks live in `LoadActionValidator` and sibling validators
- Validators usually accumulate human-readable errors instead of failing on first issue

## Runtime Hardening Conventions

- Watermark manager input must pass `SQLInputValidator` before SQL emission
- Runtime SQL should use emitters from `sql_safety.py`, not ad hoc quote stripping
- Generated notebooks should stay readable plain Python, not opaque generated blobs

## Workflow Conventions

- Bundle resources under `resources/lhp/` are LHP-managed
- `jdbc_watermark_v2` extraction notebooks live in sibling `<pipeline>_extract/` directories so DLT globbing does not pick them up

