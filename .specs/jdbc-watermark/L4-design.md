# L4 - Design Document: JDBC Watermark Incremental Load

**Feature**: Self-watermark JDBC incremental ingestion
**Version**: 1.0
**Date**: 2026-04-13
**Status**: Draft
**Traces to**: [L3 Feature Spec](L3-feature-spec.md) SC-1 through SC-20

---

## 1. Component Design

### 1.1 New Files

| File | Purpose |
|------|---------|
| `src/lhp/models/pipeline_config.py` | `WatermarkConfig`, `WatermarkType` models |
| `src/lhp/generators/load/jdbc_watermark.py` | `JDBCWatermarkLoadGenerator` class |
| `src/lhp/templates/load/jdbc_watermark.py.j2` | Jinja2 template for self-watermark JDBC load |
| `tests/unit/generators/load/test_jdbc_watermark.py` | Generator unit tests |
| `tests/unit/validators/test_load_validator_watermark.py` | Validation unit tests |
| `tests/unit/models/test_pipeline_config.py` | Model unit tests |
| `tests/unit/core/test_action_registry_watermark.py` | Registry routing tests |

### 1.2 Modified Files

| File | Change |
|------|--------|
| `src/lhp/models/config.py` | Add `JDBC_WATERMARK` to `LoadSourceType` enum; add `watermark: Optional[WatermarkConfig]` to `Action` |
| `src/lhp/core/action_registry.py` | Import `JDBCWatermarkLoadGenerator`; register `LoadSourceType.JDBC_WATERMARK` |
| `src/lhp/core/validators/load_validator.py` | Add `_validate_jdbc_watermark_source()` method |
| `src/lhp/generators/load/__init__.py` | Export `JDBCWatermarkLoadGenerator` |

## 2. Data Model

### 2.1 New Models (`src/lhp/models/pipeline_config.py`)

```python
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class WatermarkType(str, Enum):
    """Supported watermark column types."""
    TIMESTAMP = "timestamp"
    NUMERIC = "numeric"


class WatermarkConfig(BaseModel):
    """Watermark configuration for incremental loads."""
    column: str = Field(..., description="Column name for high-water mark tracking")
    type: WatermarkType = Field(..., description="Watermark column type")
    operator: str = Field(
        ">=",
        description="Comparison operator for watermark filter (>= or >)",
        pattern=r"^(>=|>)$",
    )
```

### 2.2 Model Changes (`src/lhp/models/config.py`)

```python
# In LoadSourceType enum — add after KAFKA:
JDBC_WATERMARK = "jdbc_watermark"

# In Action model — add after existing fields:
watermark: Optional[WatermarkConfig] = None
```

The `WatermarkConfig` import uses `TYPE_CHECKING` guard to avoid circular imports:

```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .pipeline_config import WatermarkConfig
```

At runtime, the field type is resolved via Pydantic's deferred annotation support.

## 3. Generator Design

### 3.1 `JDBCWatermarkLoadGenerator`

Follows the exact pattern of `JDBCLoadGenerator` with these additions:
- Resolves Bronze target table name from context substitution variables
- Extracts watermark config from `action.watermark`
- Passes `bronze_target`, `watermark_column`, `watermark_type`, and `watermark_operator` to template context

```
JDBCWatermarkLoadGenerator
├── __init__()
│   └── super().__init__()  # adds dp import
│
└── generate(action, context) -> str
    ├── Validate source is dict (same as JDBCLoadGenerator)
    ├── Apply substitution_manager if in context
    ├── Extract watermark config from action.watermark
    ├── Resolve bronze_target from context substitutions
    │   └── f"{catalog}.{bronze_schema}.{target_table}"
    ├── Get operational metadata via _get_operational_metadata()
    ├── Build template_context dict
    └── render_template("load/jdbc_watermark.py.j2", template_context)
```

### 3.2 Bronze Target Resolution

The generator resolves the Bronze target using the `EnhancedSubstitutionManager`'s `mappings` dict. After the orchestrator calls `substitute_yaml()` on the source config, resolved token values are available in `sub_mgr.mappings`. The generator reads them directly:

```python
def _resolve_bronze_target(self, action: Action, context: dict) -> str:
    """Resolve Bronze target table for self-watermark HWM query."""
    sub_mgr = context.get("substitution_manager")
    if sub_mgr:
        # Read resolved values from the mappings dict (populated during
        # substitute_yaml() call earlier in generate()). This is how
        # EnhancedSubstitutionManager exposes resolved tokens — there is
        # no .resolve() method; tokens are resolved via substitute_yaml()
        # on dicts, or read directly from .mappings for individual values.
        catalog = sub_mgr.mappings.get("catalog", "")
        bronze_schema = sub_mgr.mappings.get("bronze_schema", "")
    else:
        catalog = context.get("catalog", "")
        bronze_schema = context.get("bronze_schema", "")

    # target_table from the write action's table name
    # Convention: write action table name matches the entity name
    target_table = self._extract_target_table(action, context)
    return f"{catalog}.{bronze_schema}.{target_table}"
```

**Note**: `sub_mgr.mappings` is a `Dict[str, str]` populated from the environment's `substitutions/{env}.yaml` file. The orchestrator creates the substitution manager per-environment and passes it in the context dict (see `code_generator.py:505`).

The `target_table` is extracted from:
1. The write action's `write_target.table` in the same flowgroup (preferred)
2. Fall back to deriving from the action name pattern `load_{table}_jdbc`

## 4. Template Design

### 4.1 `load/jdbc_watermark.py.j2`

The template structure mirrors the idea doc's recommended code with Jinja2 conditionals:

```
@dp.temporary_view()
def {{ target_view }}():
    """{{ description }}"""
    # Self-watermark: read HWM from Bronze target
    _target_table = "{{ bronze_target }}"
    try:
        _hwm_result = spark.sql(
            f"SELECT MAX({{ watermark_column }}) AS _hwm FROM {_target_table}"
        ).first()
        _hwm = _hwm_result["_hwm"] if _hwm_result else None
    except Exception:
        _hwm = None  # First run — table doesn't exist yet

    # Filtered JDBC read
    {% if watermark_type == "timestamp" %}
    [timestamp comparison with quoted HWM]
    {% elif watermark_type == "numeric" %}
    [numeric comparison with unquoted HWM]
    {% endif %}

    df = spark.read.format("jdbc")...
    [JDBC options]
    [optional partitioning options]
    .load()

    [optional operational metadata]

    return df
```

Key template variables:

| Variable | Source | Example |
|----------|--------|---------|
| `target_view` | `action.target` | `v_orders_raw` |
| `description` | `action.description` or generated | `JDBC watermark source: load_orders_jdbc` |
| `bronze_target` | Resolved from substitutions | `dev_catalog.bronze.orders` |
| `watermark_column` | `action.watermark.column` | `modified_date` |
| `watermark_type` | `action.watermark.type.value` | `timestamp` |
| `watermark_operator` | `action.watermark.operator` | `>=` |
| `jdbc_url` | `source.url` | `jdbc:postgresql://host:5432/db` |
| `jdbc_user` | `source.user` | `${scope/jdbc_user}` |
| `jdbc_password` | `source.password` | `${scope/jdbc_password}` |
| `jdbc_driver` | `source.driver` | `org.postgresql.Driver` |
| `jdbc_table` | `source.table` | `"sales"."orders"` |
| `num_partitions` | `source.num_partitions` | `4` (optional) |
| `partition_column` | `source.partition_column` | `order_id` (optional) |
| `lower_bound` | `source.lower_bound` | `1` (optional) |
| `upper_bound` | `source.upper_bound` | `1000000` (optional) |
| `add_operational_metadata` | Resolved from metadata service | `True` / `False` |
| `metadata_columns` | Resolved from metadata service | `{"_lhp_loaded_at": "F.current_timestamp()"}` |

## 5. Validation Design

### 5.1 `LoadActionValidator` Extension

Add a private method `_validate_jdbc_watermark_source()` called when `source.type == "jdbc_watermark"`:

```
_validate_jdbc_watermark_source(action, prefix)
├── Check action.watermark is not None
│   └── Error: "watermark configuration is required for jdbc_watermark source type"
├── Check action.watermark.column is not empty
│   └── Error: "watermark.column is required"
├── Check action.watermark.type is valid WatermarkType
│   └── Error: "watermark.type must be 'timestamp' or 'numeric'"
├── Check source.url is present
├── Check source.driver is present
├── Check source.table is present
├── Check source.user is present
└── Check source.password is present
```

The existing `_validate_jdbc_source()` handles standard JDBC validation. The watermark method extends it with watermark-specific checks.

## 6. Failure Modes

| Failure | When | Impact | Mitigation |
|---------|------|--------|------------|
| Bronze table doesn't exist | First run | HWM = None, full load | try/except in template |
| Bronze table exists but empty | After table create, before data | MAX() = NULL → full load | NULL check in template |
| JDBC source unreachable | Runtime | DLT pipeline fails | DLT retry policy handles this |
| Invalid watermark column | Runtime | spark.sql error on MAX() | Validation catches at config time if column name is wrong |
| Clock skew in source DB | Runtime | Missed rows if source clock behind | `>=` operator re-reads ties; user can add lookback buffer in Phase 2 |
| Delta column stats missing | Non-liquid-clustered table | Full scan for MAX() | Document requirement for liquid clustering |

## 7. Architecture Decisions

### ADR-1: Self-watermark over external watermark table

**Context**: Two approaches exist — read HWM from the Bronze target itself (self-watermark) or from an external `jdbc_watermarks` table (WatermarkManager pattern).

**Decision**: Self-watermark on critical path. Audit table deferred to Phase 2 post-pipeline task.

**Rationale**: Self-watermark eliminates the Phase 3 timing bug, keeps the template at ~30 lines instead of ~200, requires no external dependencies, and leverages Delta Lake's ACID guarantees.

### ADR-2: `>=` default comparison operator

**Context**: Using `>` (strict) risks missing rows with the same watermark value as the last loaded row. Using `>=` (inclusive) re-reads the last row but Silver's CDC dedup handles duplicates.

**Decision**: Default to `>=`. Allow `>` override via `watermark.operator`.

**Rationale**: Silent data loss (from `>` with ties) is worse than harmless re-reads (from `>=`). The cost of re-reading a few tied rows is negligible compared to the risk of missing data.

### ADR-3: Convention-based Bronze target resolution

**Context**: The load generator needs the Bronze table name for the HWM query. Options: (a) new YAML field, (b) cross-reference write action, (c) derive from substitution params.

**Decision**: Derive from substitution parameters: `${catalog}.${bronze_schema}.{target_table}`.

**Rationale**: No new YAML fields needed. The substitution params already exist in every flowgroup that uses environment-aware naming. This matches how Wumbo templates already work.

### ADR-4: Separate model file for watermark types

**Context**: `WatermarkConfig` could live in `config.py` or a new `pipeline_config.py`.

**Decision**: New `src/lhp/models/pipeline_config.py` file.

**Rationale**: `config.py` is already large (~370 lines). Pipeline-specific configuration models (watermark, CDC, snapshot) form a coherent group that will grow in Phase 2. Separation keeps files focused.

## 8. Rollout Plan

1. **Branch**: `watermark` (already created)
2. **Implementation**: Follow L5 task breakdown
3. **Testing**: Unit tests first, then integration test with sample YAML
4. **Review**: PR against `main` with all specs as context
5. **Merge**: After review approval and CI pass
6. **Documentation**: Update CLAUDE.md with new source type in conventions
