# L5 - Execution Tasks: JDBC Watermark Incremental Load

**Feature**: Self-watermark JDBC incremental ingestion
**Version**: 1.0
**Date**: 2026-04-13
**Status**: Draft
**Traces to**: [L4 Design](L4-design.md)

---

## Task Legend

- `[P]` = Parallelizable (can run concurrently with other `[P]` tasks in the same wave)
- `[S]` = Sequential (must complete before dependent tasks start)
- `[B]` = Blocked by (lists prerequisite task IDs)

---

## Wave 1: Data Model Foundation

### T1 — Create `WatermarkConfig` and `WatermarkType` models `[P]`

**File**: `src/lhp/models/pipeline_config.py` (new)

**Do**:
- Create `WatermarkType(str, Enum)` with `TIMESTAMP = "timestamp"` and `NUMERIC = "numeric"`
- Create `WatermarkConfig(BaseModel)` with fields:
  - `column: str` (required)
  - `type: WatermarkType` (required)
  - `operator: str` (default `">="`, regex-validated to `>=` or `>`)
- Add module docstring

**Done when**:
- `WatermarkType.TIMESTAMP` and `WatermarkType.NUMERIC` are importable
- `WatermarkConfig(column="id", type="numeric")` validates successfully
- `WatermarkConfig(column="id", type="uuid")` raises `ValidationError`
- `WatermarkConfig(column="id", type="timestamp", operator=">")` validates
- `WatermarkConfig(column="id", type="timestamp", operator="<")` raises `ValidationError`

**Traces to**: L3 SC-1, SC-4; L2 FR-2

---

### T2 — Add `JDBC_WATERMARK` to `LoadSourceType` and `watermark` to `Action` `[P]`

**File**: `src/lhp/models/config.py` (modify)

**Do**:
- Add `JDBC_WATERMARK = "jdbc_watermark"` to `LoadSourceType` enum (after `KAFKA`)
- Add import: `from .pipeline_config import WatermarkConfig` (under `TYPE_CHECKING` guard)
- Add field to `Action`: `watermark: Optional["WatermarkConfig"] = None`
- Update `Action.model_rebuild()` call if needed for forward ref resolution

**Done when**:
- `LoadSourceType.JDBC_WATERMARK` is importable and equals `"jdbc_watermark"`
- `Action(name="test", type="load", watermark={"column": "id", "type": "numeric"})` creates model with `watermark.column == "id"`
- `Action(name="test", type="load")` creates model with `watermark is None`
- All existing tests pass (no regression)

**Traces to**: L3 SC-1, SC-6; L2 FR-1, FR-2, NFR-3

---

---

## Wave 2: Template, Generator, and Model Tests `[B: T1, T2]`

### T3 — Write model unit tests `[P]` `[B: T1, T2]`

**File**: `tests/unit/models/test_pipeline_config.py` (new)

**Do**:
- Test `WatermarkType` enum values
- Test `WatermarkConfig` valid construction (timestamp, numeric)
- Test `WatermarkConfig` invalid type rejection
- Test `WatermarkConfig` operator validation (>=, >, invalid)
- Test `WatermarkConfig` missing required fields
- Test `Action` with watermark field (present and absent)

**Done when**:
- All tests pass
- Covers SC-1 through SC-4 acceptance criteria

**Traces to**: L3 AC-1, AC-2, AC-3

---

### T4 — Create `jdbc_watermark.py.j2` template `[P]`

**File**: `src/lhp/templates/load/jdbc_watermark.py.j2` (new)

**Do**:
- Create template following the structure in L4 Section 4.1
- `@dp.temporary_view()` decorated function
- Self-watermark HWM query with try/except for first run
- Timestamp branch: quoted HWM in WHERE clause
- Numeric branch: unquoted HWM in WHERE clause
- Dynamic operator from `{{ watermark_operator }}`
- JDBC read with all connection options
- Conditional partitioning options block
- Conditional operational metadata block
- Return df

**Done when**:
- Template renders without Jinja2 errors with a complete context dict
- Rendered output is syntactically valid Python
- Timestamp and numeric branches produce correct WHERE clause formatting

**Traces to**: L3 SC-7, SC-8, SC-9, SC-12, SC-13, SC-14, SC-15; L3 AC-5, AC-6, AC-7, AC-8, AC-9, AC-10, AC-11; L2 FR-3, FR-4, FR-5

---

### T5 — Create `JDBCWatermarkLoadGenerator` `[P]`

**File**: `src/lhp/generators/load/jdbc_watermark.py` (new)

**Do**:
- Class inherits from `BaseActionGenerator`
- `__init__`: call `super().__init__()`, add `from pyspark import pipelines as dp` import
- `generate(action, context)`:
  - Validate source is dict (reuse pattern from `JDBCLoadGenerator`)
  - Apply substitution_manager if available
  - Extract watermark config from `action.watermark`
  - Resolve `bronze_target` via `_resolve_bronze_target()`
  - Get operational metadata via `_get_operational_metadata()`
  - Build template context dict with all variables from L4 Section 4.1
  - Call `self.render_template("load/jdbc_watermark.py.j2", template_context)`
- `_resolve_bronze_target(action, context)`:
  - Get catalog, bronze_schema from `sub_mgr.mappings` dict (NOT `.resolve()` — that method doesn't exist)
  - Fall back to `context.get()` if no substitution_manager
  - Extract target_table from write action or action name convention
  - Return `f"{catalog}.{bronze_schema}.{target_table}"`
- Update `src/lhp/generators/load/__init__.py` to export the class

**Done when**:
- Generator instantiates without error
- `generate()` returns valid Python string for a well-formed action
- `generate()` raises `LHPError` for string source input
- Bronze target resolution works with substitution context

**Traces to**: L3 SC-7, SC-8, SC-16, SC-17, SC-18; L2 FR-3, FR-6, FR-7, FR-8, FR-9

---

### Checkpoint: After Wave 2 (T3, T4, T5)

- [ ] `WatermarkConfig` and `WatermarkType` models importable and validated (T1)
- [ ] `LoadSourceType.JDBC_WATERMARK` exists, `Action.watermark` field works (T2)
- [ ] All model unit tests pass (T3)
- [ ] Template renders valid Python for both timestamp and numeric types (T4)
- [ ] Generator produces correct code with substitution context (T5)
- [ ] Existing tests still pass (no regression)

---

## Wave 3: Registration and Validation `[B: T3, T4, T5]`

### T6 — Register in `ActionRegistry` `[P]`

**File**: `src/lhp/core/action_registry.py` (modify)

**Do**:
- Add import: `from ..generators.load.jdbc_watermark import JDBCWatermarkLoadGenerator`
- Add to `_load_generators` dict: `LoadSourceType.JDBC_WATERMARK: JDBCWatermarkLoadGenerator`

**Done when**:
- `ActionRegistry().get_generator(ActionType.LOAD, "jdbc_watermark")` returns `JDBCWatermarkLoadGenerator` instance
- Existing `jdbc` routing still returns `JDBCLoadGenerator`

**Traces to**: L3 SC-19, SC-20; L2 FR-1

---

### T7 — Add validation rules to `LoadActionValidator` `[P]`

**File**: `src/lhp/core/validators/load_validator.py` (modify)

**Do**:
- Add `_validate_jdbc_watermark_source(action, prefix)` method
- Check `action.watermark` is not None → error if missing
- Check `action.watermark.column` is not empty → error if blank
- Check `action.watermark.type` is valid `WatermarkType` → error if invalid
- Check required JDBC fields: url, driver, table, user, password
- Wire into main `validate()` dispatch for `source.type == "jdbc_watermark"`

**Done when**:
- Valid jdbc_watermark config passes validation
- Missing watermark.column produces error
- Missing watermark.type produces error
- Missing JDBC url/driver/table produces errors
- Non-watermark JDBC actions are unaffected

**Traces to**: L3 SC-2, SC-3, SC-4, SC-5; L2 FR-10

---

## Wave 4: Testing `[B: T6, T7]`

### T8 — Generator unit tests `[P]`

**File**: `tests/unit/generators/load/test_jdbc_watermark.py` (new)

**Do**:
- Test timestamp watermark code generation (SC-7)
- Test numeric watermark code generation (SC-8)
- Test first-run handling (try/except in output) (SC-9)
- Test JDBC partitioning present (SC-12)
- Test JDBC partitioning absent (SC-13)
- Test operational metadata present (SC-14)
- Test operational metadata absent (SC-15)
- Test custom operator `>` (SC-18)
- Test string source raises error
- Test generated code is syntactically valid Python (`compile()`)
- Test generated code passes Black formatting

**Done when**:
- All tests pass
- Covers AC-4 through AC-11, AC-16

**Traces to**: L3 AC-4 through AC-11, AC-16

---

### T9 — Validation unit tests `[P]`

**File**: `tests/unit/validators/test_load_validator_watermark.py` (new)

**Do**:
- Test valid jdbc_watermark config passes validation
- Test missing watermark section produces error
- Test missing watermark.column produces error
- Test invalid watermark.type produces error
- Test missing JDBC required fields produce errors
- Test standard jdbc type with watermark section is ignored

**Done when**:
- All tests pass
- Covers AC-12, AC-13

**Traces to**: L3 SC-2 through SC-6; AC-12, AC-13

---

### T10 — Registry and integration tests `[P]`

**File**: `tests/unit/core/test_action_registry_watermark.py` (new) + existing integration test update

**Do**:
- Test `ActionRegistry` routes `jdbc_watermark` to `JDBCWatermarkLoadGenerator` (AC-14)
- Test `ActionRegistry` still routes `jdbc` to `JDBCLoadGenerator` (AC-15)
- Integration test: create sample flowgroup YAML with jdbc_watermark action, run through orchestrator, verify generated code (AC-17)

**Done when**:
- All tests pass
- Covers AC-14, AC-15, AC-17

**Traces to**: L3 SC-19, SC-20; AC-14, AC-15, AC-17

---

### Checkpoint: After Wave 4 (T8, T9, T10)

- [ ] All generator unit tests pass — timestamp, numeric, partitioning, metadata (T8)
- [ ] All validation unit tests pass — missing fields, invalid types, regression (T9)
- [ ] Registry routes correctly and integration test renders complete flowgroup (T10)
- [ ] Review with human before final verification

---

## Wave 5: Verification `[B: T8, T9, T10]`

### T11 — Full regression and verification `[S]`

**Do**:
- Run full test suite: `pytest tests/ -v`
- Verify zero regressions in existing tests
- Verify all new tests pass
- Run `mypy src/lhp/` — verify no type errors
- Run `black --check src/lhp/` — verify formatting
- Run `flake8 src/lhp/` — verify no lint errors
- Review generated code for one complete flowgroup end-to-end

**Done when**:
- All tests pass (existing + new)
- Zero mypy errors
- Zero black/flake8 violations
- Generated code is readable and correct

**Traces to**: L2 NFR-3, NFR-4; all acceptance criteria

---

## Dependency Graph

```
Wave 1 (parallel):  T1 ──┐
                    T2 ──┘
                          │
Wave 2 (parallel):  T3 ──┤ (blocked by T1, T2)
                    T4 ──┤
                    T5 ──┘
                          │
Wave 3 (parallel):  T6 ──┤ (blocked by T3, T4, T5)
                    T7 ──┘
                          │
Wave 4 (parallel):  T8 ──┤ (blocked by T6, T7)
                    T9 ──┤
                    T10 ─┘
                          │
Wave 5 (sequential): T11  (blocked by T8, T9, T10)
```

## Estimated Task Count

- **11 tasks** across **5 waves**
- **Parallelizable**: 9 of 11 tasks can run concurrently within their wave
- **New files**: 7
- **Modified files**: 4
- **New test files**: 4

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Pydantic forward ref for `WatermarkConfig` on `Action` fails at runtime | High — Action model breaks for all users | T2 done criteria includes runtime instantiation test; T3 covers explicitly |
| `sub_mgr.mappings` doesn't contain `catalog`/`bronze_schema` for some environments | Medium — generated HWM query has empty table name | T5 done criteria tests with and without substitution context; generator falls back to `context.get()` |
| Existing `_validate_jdbc_source()` rejects watermark configs (shared validation) | Medium — false validation errors | T7 ensures watermark dispatch is separate from standard JDBC; T9 regression tests |
| Template renders syntactically invalid Python for edge cases | Medium — generated notebooks fail to parse | T8 uses `compile()` check on all rendered outputs |
| Adding `JDBC_WATERMARK` to `LoadSourceType` breaks downstream enum consumers | Low — enum is additive | T2 runs full existing test suite; NFR-3 covers backward compat |

## Open Questions

- None — all design decisions resolved in L4 ADRs (2026-04-13)
