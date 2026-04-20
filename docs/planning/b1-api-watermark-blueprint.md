# B1 — `api_watermark_v2` Implementation Blueprint

**Status**: Blueprint (pre-implementation, wave 1 output).
**Owner**: dwtorres@gmail.com.
**Date**: 2026-04-19.
**Relates to**: [`adr-003-followups.md`](./adr-003-followups.md) §B1.
**Reference contract**: [`src/lhp/generators/load/jdbc_watermark_job.py`](../../src/lhp/generators/load/jdbc_watermark_job.py) + [`src/lhp/templates/load/jdbc_watermark_job.py.j2`](../../src/lhp/templates/load/jdbc_watermark_job.py.j2).

---

## 1. Action Type Identifier

**Decision: new `LoadSourceType` enum value — `API_WATERMARK_V2 = "api_watermark_v2"`**

Plan §B1 suggested a `connection.type: api` discriminator inside `jdbc_watermark_v2` to reduce code churn. That approach is rejected for the following concrete reasons discovered in the reference code:

1. **Registry dispatch is keyed on `LoadSourceType`** (`src/lhp/core/action_registry.py:77`). A discriminator means `JDBCWatermarkJobGenerator.generate()` must branch internally on `connection.type`, coupling two fundamentally different extraction protocols into one 252-line class. There is no clean override point.

2. **Template branching explosion**. The JDBC template is 309 lines with `{% if watermark_type %}` branches. Adding `{% if connection.type == "api" %}` throughout makes it untestable as a unit and unmaintainable.

3. **`WorkflowResourceGenerator`** (`src/lhp/generators/bundle/workflow_resource.py:42`) matches `LoadSourceType.JDBC_WATERMARK_V2.value` by string. A discriminator requires changing that match — same file-churn as a new enum.

4. **`BATCH_ONLY_SOURCE_TYPES`** (`src/lhp/models/config.py:57-60`) must include the new type. A new enum value is one line.

5. **Test isolation**: `test_jdbc_watermark_job.py` has 30+ methods. A shared generator class forces those tests to survive the API extension's development. A new class eliminates interference.

The "less code churn" claim in the plan is incorrect on inspection. The churn is equivalent and the discriminator approach imposes structural coupling that would require a future refactor to undo.

**Files touched for this decision**:
- `src/lhp/models/config.py`: +1 line to `LoadSourceType`, +1 entry in `BATCH_ONLY_SOURCE_TYPES`.
- `src/lhp/core/action_registry.py`: +1 import line, +1 registry entry.
- `src/lhp/generators/load/__init__.py`: +1 export line.

---

## 2. Config Model — Exact Field Specification

### 2.1 New Pydantic models (add to `src/lhp/models/config.py`)

`Action.source` remains `Dict[str, Any]` (untyped at the `Action` level, consistent with JDBC). The generator validates by instantiating `ApiSourceConfig` from `action.source`.

```python
class ApiAuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field("bearer")
    token_secret_ref: str = Field(...)

    @field_validator("type")
    @classmethod
    def validate_auth_type(cls, v: str) -> str:
        if v != "bearer":
            raise ValueError(f"Unsupported auth type '{v}'. Only 'bearer' is supported in B1.")
        return v


class ApiPaginationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field(...)                         # "offset" | "cursor" | "link_header"
    start_param: str = Field("_start")             # offset only
    limit_param: str = Field("_limit")             # offset only
    page_size: int = Field(100, ge=1, le=10000)
    cursor_field: Optional[str] = Field(None)      # cursor only: response key for next cursor
    cursor_param: Optional[str] = Field(None)      # cursor only: query param name for cursor value

    @field_validator("type")
    @classmethod
    def validate_pagination_type(cls, v: str) -> str:
        if v not in {"offset", "cursor", "link_header"}:
            raise ValueError(f"Unsupported pagination type '{v}'.")
        return v

    @model_validator(mode="after")
    def validate_cursor_fields(self) -> "ApiPaginationConfig":
        if self.type == "cursor":
            if self.cursor_field is None or self.cursor_param is None:
                raise ValueError(
                    "pagination.cursor_field and pagination.cursor_param are required when type='cursor'."
                )
        return self


class ApiResponseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    records_path: Optional[str] = Field(
        None,
        description="Dot-separated path to records list in response (e.g. 'data.items'). "
                    "Omit when the response body IS the array.",
    )


class ApiRetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_attempts: int = Field(3, ge=1, le=10)
    backoff_seconds: float = Field(2.0, ge=0.1, le=60.0)


class ApiSourceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")       # allow cloudfiles passthrough keys
    type: str                                      # always "api_watermark_v2"
    endpoint_url: str = Field(...)
    auth: ApiAuthConfig
    pagination: ApiPaginationConfig
    response: ApiResponseConfig = Field(default_factory=ApiResponseConfig)
    retry: ApiRetryConfig = Field(default_factory=ApiRetryConfig)
    schema_hints: Optional[str] = Field(
        None,
        description="Spark DDL schema string passed to cloudFiles.schemaHints and used as "
                    "fallback schema when the API returns 0 rows on an empty incremental run. "
                    "Strongly recommended. Example: 'id BIGINT, title STRING, body STRING'.",
    )
    # CloudFiles passthrough keys (same set as JDBC variant)
    options: Optional[Dict[str, Any]] = None
    reader_options: Optional[Dict[str, Any]] = None
    schema_location: Optional[str] = None
    schema_evolution_mode: Optional[str] = None
```

### 2.2 Field reference table

| Field | Type | Default | Required | Rule |
|---|---|---|---|---|
| `endpoint_url` | `str` | — | yes | non-empty; generator validates URL format |
| `auth.type` | `str` | `"bearer"` | yes | must equal `"bearer"` |
| `auth.token_secret_ref` | `str` | — | yes | `${secret:scope/key}` or `__SECRET_scope_key__` |
| `pagination.type` | `str` | — | yes | `offset` / `cursor` / `link_header` |
| `pagination.page_size` | `int` | `100` | no | 1–10 000 |
| `pagination.start_param` | `str` | `"_start"` | no (offset only) | |
| `pagination.limit_param` | `str` | `"_limit"` | no (offset only) | |
| `pagination.cursor_field` | `str` | `None` | required when `type=cursor` | response key carrying next cursor |
| `pagination.cursor_param` | `str` | `None` | required when `type=cursor` | query param name for cursor |
| `response.records_path` | `str` | `None` | no | dot-delimited; `None` = response IS array |
| `retry.max_attempts` | `int` | `3` | no | 1–10 |
| `retry.backoff_seconds` | `float` | `2.0` | no | 0.1–60.0 |
| `schema_hints` | `str` | `None` | strongly recommended | Spark DDL; guards empty-batch path |

### 2.3 Complete YAML example

```yaml
actions:
  - name: load_posts_api
    type: load
    source:
      type: api_watermark_v2
      endpoint_url: "https://jsonplaceholder.typicode.com/posts"
      auth:
        type: bearer
        token_secret_ref: "${secret:api-dev/token}"
      pagination:
        type: offset
        start_param: _start
        limit_param: _limit
        page_size: 50
      response:
        records_path: null    # response body IS the array
      retry:
        max_attempts: 3
        backoff_seconds: 2.0
      schema_hints: "id BIGINT, userId BIGINT, title STRING, body STRING"
    target: v_posts_raw
    landing_path: /Volumes/devtest_edp_landing/landing/landing/api_posts
    watermark:
      column: id
      type: numeric
      operator: ">"
      source_system_id: jsonplaceholder
```

---

## 3. Generator Class — `ApiWatermarkJobGenerator`

### 3.1 Hierarchy: shared abstract base class (Option B)

Three options were evaluated. Option B (extract shared base) wins over Option A (subclass JDBC directly) and Option C (standalone with module-level imports from JDBC).

**Why not Option A (subclass `JDBCWatermarkJobGenerator`)**: the existing `generate()` method computes `jdbc_url/jdbc_user/jdbc_password/jdbc_driver/jdbc_table` and passes them directly to a template context dict. There is no override point that avoids computing those JDBC-specific values. A subclass would call `super()` and then discard 60% of the output.

**Why not Option C (standalone class, import helpers from `jdbc_watermark_job.py`)**: creates a non-obvious import coupling. If `jdbc_watermark_job.py` is renamed, Option C silently breaks. It also precludes future watermark source types.

**Option B**: extract shared logic into `src/lhp/generators/load/base_watermark_job.py`. `JDBCWatermarkJobGenerator` and `ApiWatermarkJobGenerator` both subclass `BaseWatermarkJobGenerator`. Each overrides only `_build_template_context()`. The refactor is behaviorally invisible to existing tests.

### 3.2 New file: `src/lhp/generators/load/base_watermark_job.py` (~120 LoC)

Contains:
- All module-level constants and helpers extracted verbatim from `jdbc_watermark_job.py`:
  - `_CLOUDFILES_PASSTHROUGH_KEYS` (set)
  - `_SECRET_REF_RE` (compiled regex)
  - `_LANDING_READ_SUFFIX = "_lhp_runs/*"`
  - `_resolve_secret_refs(value: str) -> str`
  - `_cloudfiles_read_path(landing_path: str) -> str`
  - `_has_schema_location(source_config: Dict) -> bool`
  - `_default_schema_location(landing_path: str, action_name: str) -> str`
  - `_check_landing_schema_overlap(...)` (A3 helper) — leave on JDBC class for now since A3 only governs JDBC; promote to base only if API needs it (see §6 — API uses `schema_hints` instead).
- `class BaseWatermarkJobGenerator(BaseActionGenerator)`:
  - `generate(action, context) -> str` — full orchestration: `_build_template_context()` → `render_template()` → secret resolution → Black formatting → aux-file storage → cloudfiles stub delegation
  - `_build_template_context(action, context) -> tuple[str, Dict[str, Any]]` — abstract method; returns `(template_name, template_context_dict)`
  - `_build_cloudfiles_stub(action, context, source_config) -> str` — promoted from the current 40-line inline block in `JDBCWatermarkJobGenerator.generate()`

### 3.3 Modified file: `src/lhp/generators/load/jdbc_watermark_job.py` (~80 LoC after refactor)

```python
class JDBCWatermarkJobGenerator(BaseWatermarkJobGenerator):
    def _build_template_context(
        self, action: Action, context: Dict[str, Any]
    ) -> tuple[str, Dict[str, Any]]:
        # ... existing logic from generate() for source_system_id, wm_catalog,
        #     wm_schema, jdbc_url, jdbc_user, jdbc_password, jdbc_driver,
        #     jdbc_table derivation ...
        return ("load/jdbc_watermark_job.py.j2", template_context)
```

All existing tests in `test_jdbc_watermark_job.py` pass without modification.

### 3.4 New file: `src/lhp/generators/load/api_watermark_job.py` (~120 LoC)

```python
class ApiWatermarkJobGenerator(BaseWatermarkJobGenerator):
    def _build_template_context(
        self, action: Action, context: Dict[str, Any]
    ) -> tuple[str, Dict[str, Any]]:
        source_config = action.source if isinstance(action.source, dict) else {}
        # Validate via ApiSourceConfig (raises ValueError → LHPValidationError at boundary)
        api_cfg = ApiSourceConfig(**source_config)
        watermark = action.watermark
        flowgroup = context.get("flowgroup")

        source_system_id = (
            getattr(watermark, "source_system_id", None) if watermark else None
        ) or _slug_from_url(api_cfg.endpoint_url)

        # endpoint_name and table_name form the WatermarkManager composite key
        endpoint_name = _slug_from_url(api_cfg.endpoint_url)  # path slug, safe for SQLInputValidator.identifier
        wm_catalog = (getattr(watermark, "catalog", None) if watermark else None) or "metadata"
        wm_schema = (getattr(watermark, "schema", None) if watermark else None) or "orchestration"

        auth_token = _resolve_secret_refs(api_cfg.auth.token_secret_ref)

        if api_cfg.schema_hints is None:
            logger.warning(
                "Action '%s': schema_hints is not set. Empty API responses will produce "
                "a schema-less parquet that AutoLoader cannot read. Set schema_hints to "
                "a Spark DDL string.", action.name
            )

        ctx: Dict[str, Any] = {
            "action_name": action.name,
            "pipeline_name": flowgroup.pipeline if flowgroup else "unknown",
            "source_system_id": source_system_id,
            "endpoint_name": endpoint_name,
            "wm_catalog": wm_catalog,
            "wm_schema": wm_schema,
            "watermark_column": watermark.column if watermark else "",
            "watermark_type": watermark.type.value if watermark else "numeric",
            "watermark_operator": watermark.operator if watermark else ">",
            "endpoint_url": api_cfg.endpoint_url,
            "auth_token": auth_token,
            "pagination_type": api_cfg.pagination.type,
            "start_param": api_cfg.pagination.start_param,
            "limit_param": api_cfg.pagination.limit_param,
            "page_size": api_cfg.pagination.page_size,
            "cursor_field": api_cfg.pagination.cursor_field,
            "cursor_param": api_cfg.pagination.cursor_param,
            "records_path": api_cfg.response.records_path,
            "retry_max_attempts": api_cfg.retry.max_attempts,
            "retry_backoff_seconds": api_cfg.retry.backoff_seconds,
            "schema_hints": api_cfg.schema_hints,
            "landing_path": action.landing_path or "",
        }
        return ("load/api_watermark_job.py.j2", ctx)
```

Helper `_slug_from_url(url: str) -> str`: extracts the last path segment of the URL, lowercases, replaces non-alphanumeric with underscore. Used as `endpoint_name` (WatermarkManager `schema_name` surrogate) and `source_system_id` fallback.

---

## 4. Template Structure — `src/lhp/templates/load/api_watermark_job.py.j2`

### 4.1 Write strategy decision: aggregate-then-single-write per run

**Chosen**: accumulate all pages in `all_records: List[Dict]`, build one Spark DataFrame, write once to `run_landing_path`. Matches the JDBC single-write shape exactly.

**Rationale**:
- `mark_landed` / `mark_complete` contract requires a single `row_count` and `new_hwm` computed from the full result. Whether we write per-page or not, we must read the full parquet back to compute these values — or track them in Python. The JDBC template's post-write `spark.read.parquet().agg(F.count, F.max)` forces a two-pass read. Matching that shape means the agg pattern is reusable verbatim.
- Per-page append introduces a crash-recovery complication: a run failing mid-pagination leaves a partial `_lhp_runs/<run_id>/` directory. The recovery path (`get_recoverable_landed_run`) assumes the directory is either complete or absent. Partial writes would require a new recovery variant — out of scope for B1.
- **Acknowledged limitation**: `all_records` is a Python list held in driver memory. For large API result sets this is a problem. Phase E (parallel pagination) will switch to per-page Spark accumulation. B1 scope is bounded API payloads (JSONPlaceholder is 100 records).

### 4.2 Section-by-section outline (line counts approximate)

```
Lines 1-15:   Notebook header comment (generated-by, control flow contract note)
Lines 16-40:  Imports: json, os, sys, time, datetime, timezone, Decimal, typing,
              requests, pyspark.sql.functions as F, pyspark.sql.types.StructType
Lines 41-58:  _lhp_watermark_bootstrap_syspath() — verbatim from JDBC template
Lines 59-65:  lhp_watermark imports — verbatim from JDBC template
Lines 66-68:  spark.conf.set("spark.sql.session.timeZone", "UTC")  [FR-L-07]
Lines 69-71:  run_id = derive_run_id(dbutils)  [FR-L-09]
Lines 72-78:  WatermarkManager init — verbatim
Lines 79-88:  SQLInputValidator on identifiers:
              source_system_id, schema_name (=endpoint_name), table_name (=action_name),
              watermark_column
Lines 89-100: get_latest_watermark() + hwm_value extraction — verbatim
Lines 101-115: _log_phase(), _run_landing_path() — verbatim from JDBC
Lines 116-140: Recovery branch — verbatim from JDBC
Lines 141-185: API config binding (Jinja-rendered Python literals):
              endpoint_url, auth_token (resolved expression), pagination_type,
              page_size, start_param/limit_param (offset), cursor_param/cursor_field
              (cursor, inside {% if %}), records_path_keys, retry_max_attempts,
              retry_backoff_seconds
              {% if schema_hints %}explicit_schema_hints = ...{% endif %}
Lines 186-220: _build_hwm_params(hwm) — Jinja-branched per watermark_type:
              timestamp → {"modified_after": hwm_iso}
              numeric (offset) → {watermark_column: str(hwm_num)} or {}
Lines 221-310: _paginate() — Jinja-dispatched per pagination_type (one branch emitted):
              offset:      while True: GET(offset, limit), extend, break on short page
              cursor:      while cursor: GET(cursor_param=cursor), update cursor from response
              link_header: while url: GET_raw(), follow Link header
Lines 311-360: HTTP helpers:
              _fetch_with_retry(url, headers, params) -> Any
              _fetch_with_retry_raw(url, headers, params) -> requests.Response
              _extract_records(body) -> List[Dict]
              _parse_link_next(link_header: str) -> Optional[str]
Lines 361-385: _landing_has_parquet(path) — verbatim from JDBC
Lines 386-430: insert_new() outside try — verbatim kwargs shape
Lines 431-500: try block:
              all_records: List[Dict[str, Any]] = []
              _log_phase("api_fetch_start", ...)
              for page_records in _paginate(): all_records.extend(page_records)
              _log_phase("api_fetch_complete", total_records=len(all_records))
              df = spark.createDataFrame(all_records)
              df.write.mode("overwrite").format("parquet").save(run_landing_path)
              if _landing_has_parquet(run_landing_path):
                  agg via F.count + F.max(watermark_column)
              else:
                  {% if schema_hints %}
                  fallback_schema = StructType.fromDDL(explicit_schema_hints)
                  {% else %}
                  fallback_schema = df.schema
                  {% endif %}
                  spark.createDataFrame([], fallback_schema).coalesce(1).write...
                  _log_phase("landing_empty_schema_fallback", ...)
              extraction_succeeded = True
              except → mark_failed + raise
Lines 501-540: mark_landed + mark_complete outside try — verbatim shape
```

**Total estimated template lines**: ~540.

---

## 5. Pagination Strategies — Pseudocode

### 5.1 Offset pagination (Phase B1: implementable)

```python
def _paginate():
    offset = 0
    headers = {"Authorization": f"Bearer {auth_token}"}
    base_params = _build_hwm_params(hwm_value)
    while True:
        params = {**base_params, start_param: offset, limit_param: page_size}
        body = _fetch_with_retry(endpoint_url, headers, params)
        records = _extract_records(body)
        if not records:
            break
        yield records
        if len(records) < page_size:
            break          # final page: server returned fewer than page_size
        offset += len(records)
```

### 5.2 Cursor pagination (Phase B1: implementable)

```python
def _paginate():
    cursor = None
    headers = {"Authorization": f"Bearer {auth_token}"}
    base_params = _build_hwm_params(hwm_value)
    while True:
        params = {**base_params}
        if cursor is not None:
            params[cursor_param] = cursor
        body = _fetch_with_retry(endpoint_url, headers, params)
        records = _extract_records(body)
        if not records:
            break
        yield records
        cursor = body.get(cursor_field) if isinstance(body, dict) else None
        if not cursor:
            break
```

### 5.3 Link-header pagination (Phase B1: implementable)

```python
def _paginate():
    url = endpoint_url
    headers = {"Authorization": f"Bearer {auth_token}"}
    base_params = _build_hwm_params(hwm_value)
    first = True
    while url:
        params = base_params if first else {}
        first = False
        resp = _fetch_with_retry_raw(url, headers, params)
        records = _extract_records(resp.json())
        if not records:
            break
        yield records
        url = _parse_link_next(resp.headers.get("Link", ""))
```

All three variants are implementable in Phase B1 because they differ only in how the "next page" is determined — the surrounding try/except/mark_failed/mark_complete skeleton is identical.

---

## 6. Empty-Batch Handling

### 6.1 Problem

In the JDBC variant, `df.schema` (used for the empty-batch fallback) is always known post-`spark.read.jdbc()` — the JDBC driver returns column metadata regardless of row count. The A2 fix (`spark.createDataFrame([], df.schema)`) works because `df.schema` is a real `StructType`.

In the API variant, schema comes from the runtime response body. When `all_records == []`, `spark.createDataFrame([], [])` produces a DataFrame with an empty `StructType`. A parquet written from an empty `StructType` produces no column metadata — AutoLoader fails with `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` (same symptom, different root cause).

### 6.2 Resolution: `schema_hints` field for explicit schema

The A2 fix translates to the API variant only if a schema is available at template-render time.

**Three options considered**:
- **Option A (first-non-empty-then-cache)**: persist schema from first successful run to a workspace file. Complex, stateful, fails on first-ever-empty run. Rejected.
- **Option B (schema probe request)**: extra GET on every run to infer schema. Adds latency and still fails if probe returns `[]`. Rejected.
- **Option C (explicit `schema_hints`, chosen)**: the YAML author supplies `schema_hints: "col TYPE, ..."` (Spark DDL). The generator emits it as `cloudFiles.schemaHints` in the CloudFiles stub AND as `StructType.fromDDL(explicit_schema_hints)` in the extraction notebook's empty-batch fallback.

**Template behavior**:
```python
if _landing_has_parquet(run_landing_path):
    # normal path — agg from written parquet
    ...
else:
    # {% if schema_hints %}
    fallback_schema = StructType.fromDDL(explicit_schema_hints)
    # {% else %}
    fallback_schema = df.schema   # WARNING: may be empty StructType
    # {% endif %}
    spark.createDataFrame([], fallback_schema).coalesce(1).write.mode("overwrite").format("parquet").save(run_landing_path)
    _log_phase("landing_empty_schema_fallback", schema_source="schema_hints" if schema_hints else "inferred_empty")
```

**Generator behavior**: emit a `logger.warning()` (not error) when `schema_hints` is absent from `ApiSourceConfig`. Do not block generation — the failure mode only occurs when the batch is actually empty. **OQ-1 below escalates this to a hard error pending user decision.**

---

## 7. Retry / 429 Handling

### 7.1 Library: `requests`

`requests>=2.32.0` is confirmed in `pyproject.toml` under `[project.optional-dependencies] dev`. It is **not** in `[project.dependencies]` (production).

**Action required**: move `requests>=2.32.0` from `[dev]` to `[project.dependencies]`. The extraction notebook runs as a Databricks Job task; it imports `requests` at runtime. DBR 13.x+ ships `requests` as a transitive dependency of Databricks services, but relying on a bundled version is fragile.

`httpx` is not present anywhere in the repo. `urllib.request` is stdlib but is significantly more verbose for JSON + auth + redirect handling. `requests` is the clear choice.

### 7.2 Retry implementation

Manual retry loop in the template (not `HTTPAdapter`) because:
1. Retry config is user-controlled via YAML (`retry.max_attempts`, `retry.backoff_seconds`).
2. The loop must emit `_log_phase("retry_wait", ...)` for observability.
3. `HTTPAdapter` retry does not emit structured logs.

Retry triggers: HTTP 429 and HTTP 5xx. HTTP 4xx (except 429) are not retried.

Backoff: `wait = retry_backoff_seconds * (2 ** attempt)` (deterministic exponential; no jitter added in B1 — jitter can be a B1 follow-up).

---

## 8. HWM Semantics Per Pagination Variant

### 8.1 Offset pagination

- **HWM meaning**: `MAX(watermark_column)` across all rows in the last completed run, stored in `watermarks` table as a string.
- **Gating the next run**: `_build_hwm_params(hwm_value)` can produce `{watermark_column: str(hwm_value)}` as an optional server-side filter. Whether the API honors it is API-specific. For APIs that ignore it (e.g. JSONPlaceholder), all rows are always fetched; downstream Delta MERGE deduplicates.
- **First run** (`hwm_value = None`): `_build_hwm_params` returns `{}`. Full dataset fetched from offset 0.
- **Operator** (`>` / `>=`): relevant only for APIs that accept a server-side HWM filter param. For offset-only APIs, the operator is stored in the watermark table but has no extraction-time effect.

### 8.2 Cursor pagination (timestamp cursor)

- **HWM meaning**: `MAX(watermark_column)` (typically a `updated_at` timestamp) from last completed run.
- **Gating the next run**: `_build_hwm_params` emits `{"modified_after": hwm_iso}` on the FIRST page request. The cursor (opaque token from `cursor_field`) navigates pages WITHIN a run — it is ephemeral and does not cross run boundaries.
- **First run**: no filter; full dataset fetched. HWM set to `MAX(watermark_column)`.
- **Operator**: `>` excludes the boundary record; `>=` includes it. Prefer `>` for microsecond-precision timestamps to avoid duplicates.

### 8.3 Link-header pagination

- **HWM meaning**: `MAX(watermark_column)` from last completed run.
- **Gating the next run**: `_build_hwm_params` generates a filter for the FIRST request only (e.g. `?since=ISO` for GitHub-style APIs). Subsequent pages follow the `Link: <url>; rel="next"` header exactly as provided by the server.
- **Risk**: if the server's `next` URL does not preserve the `since` filter, subsequent pages return unfiltered data. Implementation agent must document this in the generated notebook's header comment.
- **First run**: no `since` filter; all pages fetched.

---

## 9. Test Fixture — Localhost API Server

### 9.1 Library: Flask (recommended over FastAPI)

Flask requires only `flask>=3.0.0` (and `werkzeug` as its dep). FastAPI requires `anyio`, `starlette`, `uvicorn` — more surface area for a test-only dependency.

**New dev dependency**: add `"flask>=3.0.0"` to `[project.optional-dependencies] dev` in `pyproject.toml`.

`pytest --collect-only` impact: Flask is imported only in `tests/fixtures/api_server.py` and indirectly via conftest. No existing test files change.

### 9.2 Fixture implementation sketch

```python
# tests/fixtures/api_server.py

import socket
import threading
import time
import pytest
from flask import Flask, jsonify, request

_POSTS = [
    {"id": i, "title": f"Post {i}", "body": "body text", "userId": (i % 10) + 1}
    for i in range(1, 101)
]

def _make_app() -> Flask:
    app = Flask(__name__)

    @app.route("/posts")
    def posts():
        start = int(request.args.get("_start", 0))
        limit = int(request.args.get("_limit", 10))
        min_id = int(request.args.get("id_gt", -1))
        results = [p for p in _POSTS if p["id"] > min_id]
        return jsonify(results[start:start + limit])

    @app.route("/empty")
    def empty():
        return jsonify([])

    @app.route("/cursor_posts")
    def cursor_posts():
        cursor = int(request.args.get("cursor", 0))
        limit = int(request.args.get("limit", 10))
        page = _POSTS[cursor:cursor + limit]
        next_cursor = cursor + limit if cursor + limit < len(_POSTS) else None
        return jsonify({"records": page, "next_cursor": next_cursor})

    @app.route("/link_posts")
    def link_posts():
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 10))
        start = (page - 1) * per_page
        records = _POSTS[start:start + per_page]
        headers = {}
        if start + per_page < len(_POSTS):
            next_url = f"/link_posts?page={page + 1}&per_page={per_page}"
            headers["Link"] = f'<{next_url}>; rel="next"'
        return jsonify(records), 200, headers

    return app


def _find_free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(url: str, timeout: float = 5.0) -> None:
    import urllib.request as _ur
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            _ur.urlopen(url, timeout=0.5)
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"Test server did not start in {timeout}s")


@pytest.fixture(scope="session")
def api_server():
    app = _make_app()
    port = _find_free_port()
    thread = threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=port, use_reloader=False),
        daemon=True,
    )
    thread.start()
    _wait_for_server(f"http://127.0.0.1:{port}/posts")
    yield f"http://127.0.0.1:{port}"
```

---

## 10. Test Plan

### File: `tests/unit/generators/load/test_api_watermark_job.py` (~400 LoC, ~33 methods)

Helper `_make_api_action()` mirrors `_make_v2_action()`. Builds an `Action` with `source.type="api_watermark_v2"` and sensible defaults.

```
class TestApiWatermarkJobGenerator:

  test_returns_cloudfiles_stub
    # Primary output contains 'format("cloudFiles")'

  test_auxiliary_file_contains_extraction_notebook
    # fg._auxiliary_files["__lhp_extract_load_posts_api.py"] exists and contains "WatermarkManager"

  test_extraction_notebook_has_watermark_manager_import
    # AST: ImportFrom lhp_watermark has WatermarkManager

  test_extraction_notebook_passes_spark_to_watermark_manager
    # "WatermarkManager(\n    spark," or "WatermarkManager(spark," in notebook

  test_extraction_notebook_has_get_latest_watermark
    # "get_latest_watermark(" in notebook; '["watermark_value"]' present

  test_extraction_notebook_has_insert_new_outside_try
    # AST: exactly one module-level insert_new call; none inside Try body

  test_extraction_notebook_no_dlt_decorators
    # "@dp." not in notebook; "temporary_view" not in notebook

  test_offset_pagination_renders_start_limit_params
    # pagination.type=offset; "start_param" and "limit_param" variables in notebook
    # "_paginate" function present; "offset +=" in _paginate body

  test_cursor_pagination_renders_cursor_field_and_param
    # pagination.type=cursor, cursor_field="next_cursor", cursor_param="cursor"
    # "cursor_param" and "cursor_field" variables; break on not cursor in _paginate

  test_link_header_pagination_renders_link_parser
    # pagination.type=link_header; "_parse_link_next" function in notebook

  test_offset_does_not_render_cursor_variables
    # pagination.type=offset; "cursor_param" not in notebook

  test_cursor_validation_raises_when_cursor_field_missing
    # ApiSourceConfig(pagination={"type": "cursor"}) → ValueError at instantiation

  test_auth_token_secret_ref_renders_as_dbutils
    # token_secret_ref="${secret:api-dev/token}" → 'dbutils.secrets.get(scope="api-dev", key="token")' in notebook

  test_auth_token_plain_string_renders_as_literal
    # token_secret_ref="plain-token" → '"plain-token"' in notebook

  test_extraction_notebook_has_recovery_hooks
    # "get_recoverable_landed_run(" in notebook; "mark_landed(" in notebook; "dbutils.notebook.exit(" in notebook

  test_extraction_notebook_contains_parquet_write
    # 'format("parquet")' in notebook

  test_extraction_notebook_contains_landing_path
    # landing_path value in notebook; "_lhp_runs" in notebook

  test_cloudfiles_stub_reads_from_run_scoped_glob
    # ".load(\".../api_posts/_lhp_runs/*\")" in DLT code; bare-root load absent

  test_cloudfiles_stub_sets_non_strict_globber_by_default
    # "cloudFiles.useStrictGlobber" in DLT code with value "false"

  test_schema_hints_propagated_to_cloudfiles_options
    # schema_hints="id BIGINT, title STRING" → "cloudFiles.schemaHints" in DLT code options

  test_empty_batch_with_schema_hints_uses_struct_type_from_ddl
    # schema_hints set → "StructType.fromDDL" in notebook; "landing_empty_schema_fallback" in notebook

  test_empty_batch_without_schema_hints_uses_df_schema
    # schema_hints absent → "df.schema" in notebook (not StructType.fromDDL)

  test_empty_batch_fallback_logs_phase_event
    # "landing_empty_schema_fallback" in notebook

  test_retry_helpers_present
    # "_fetch_with_retry" function defined in notebook; "retry_max_attempts" variable present

  test_retry_backoff_variable_present
    # "retry_backoff_seconds" variable in notebook

  test_watermark_column_validated_via_sql_input_validator
    # 'SQLInputValidator.string("id")' or equivalent in notebook

  test_insert_new_receives_watermark_column_name_kwarg
    # "watermark_column_name=watermark_column" in notebook

  test_extraction_notebook_passes_black
    # black.format_str(notebook, mode=black.Mode(line_length=88)) == notebook

  test_aux_key_uses_lhp_extract_prefix
    # aux key starts with "__lhp_extract_" and ends with ".py"; no bare "extract_" key

  test_source_system_id_from_watermark_config
    # watermark.source_system_id="my_api" appears in notebook as Python string literal

  test_timestamp_hwm_emits_modified_after_param
    # watermark_type=timestamp → "modified_after" in _build_hwm_params function body

  test_numeric_hwm_emits_sql_input_validator_numeric
    # watermark_type=numeric → "SQLInputValidator.numeric" in _build_hwm_params

  test_secret_placeholders_resolved_via_substitution_manager
    # __SECRET_api-dev_token__ → dbutils.secrets.get in notebook when sub_mgr provided

  test_substitution_manager_none_is_safe
    # context without substitution_manager → no crash; __SECRET_ placeholder survives
```

### File: `tests/templates/test_api_watermark_template.py` (~300 LoC, ~22 methods)

Drives `api_watermark_job.py.j2` directly via `jinja2.Environment + FileSystemLoader`. Mirrors `test_jdbc_watermark_template.py` structure with AST-level assertions.

```
class TestRenderAndFormat:
  test_template_renders_without_error           # _render() produces non-empty string
  test_rendered_passes_black                    # black.format_str idempotent
  test_rendered_is_valid_python                 # ast.parse succeeds

class TestImports:
  test_imports_derive_run_id_from_runtime       # regex: from lhp_watermark.runtime import.*derive_run_id
  test_imports_watermark_manager                # "WatermarkManager" in rendered
  test_imports_requests                         # "import requests" in rendered
  test_no_uuid_uuid4_as_primary_run_id_source   # "uuid.uuid4" not in rendered
  test_no_naive_datetime_in_template            # "datetime.utcnow" not in rendered; no bare datetime.now()

class TestUTCSession:
  test_utc_session_set_before_first_watermark_call  # AST: UTC conf.set lineno < WM call lineno

class TestControlFlowContract:
  test_insert_new_is_outside_try_block          # AST: module-level insert_new, none inside Try.body
  test_try_block_exists_and_wraps_extraction    # AST: Try exists; body has "format('parquet')" or "format(\"parquet\")"
  test_except_calls_mark_failed_then_raises     # AST: handler has mark_failed + raise
  test_mark_complete_is_outside_and_after_try   # AST: mark_complete after Try node, not inside it

class TestPaginationVariants:
  test_offset_pagination_renders_paginate_function      # parametrize pagination_type=offset: "_paginate" in rendered
  test_cursor_pagination_renders_paginate_function      # parametrize pagination_type=cursor: "_paginate" + "cursor_field"
  test_link_header_pagination_renders_link_parser       # parametrize pagination_type=link_header: "_parse_link_next"

class TestEmptyBatchSchemaFallback:
  test_empty_batch_with_schema_hints_uses_from_ddl      # render with schema_hints set → "StructType.fromDDL" in rendered
  test_empty_batch_without_schema_hints_uses_df_schema  # render without schema_hints → "df.schema" in rendered
  test_empty_batch_fallback_is_guarded_by_landing_check # AST: createDataFrame([], ...) inside if/_landing_has_parquet branch
  test_empty_batch_fallback_logs_phase_event            # "landing_empty_schema_fallback" in rendered

class TestJinjaSQLHygiene:
  test_no_bare_jinja_substitution_inside_sql_keyword_line  # textual: no {{ expr }} on same line as SELECT/WHERE/etc.
```

---

## 11. File-by-File Delta — Dependency Order

| # | File | Action | Est. net ΔLoC | Notes |
|---|---|---|---|---|
| 1 | `pyproject.toml` | Modify | +2 | Move `requests` to prod deps; add `flask>=3.0.0` to dev |
| 2 | `src/lhp/models/config.py` | Modify | +90 | Add `ApiAuthConfig`, `ApiPaginationConfig`, `ApiResponseConfig`, `ApiRetryConfig`, `ApiSourceConfig`; add `API_WATERMARK_V2` to `LoadSourceType`; add to `BATCH_ONLY_SOURCE_TYPES` |
| 3 | `src/lhp/generators/load/base_watermark_job.py` | Create | +120 | Shared helpers + `BaseWatermarkJobGenerator` abstract class |
| 4 | `src/lhp/generators/load/jdbc_watermark_job.py` | Modify | −170 (to ~80 net) | Subclass `BaseWatermarkJobGenerator`; only `_build_template_context()` remains |
| 5 | `src/lhp/templates/load/api_watermark_job.py.j2` | Create | +540 | Full extraction notebook template |
| 6 | `src/lhp/generators/load/api_watermark_job.py` | Create | +120 | `ApiWatermarkJobGenerator._build_template_context()` + `_slug_from_url()` helper |
| 7 | `src/lhp/generators/load/__init__.py` | Modify | +2 | Export `ApiWatermarkJobGenerator` |
| 8 | `src/lhp/core/action_registry.py` | Modify | +3 | Import + register `ApiWatermarkJobGenerator` |
| 9 | `src/lhp/generators/bundle/workflow_resource.py` | Modify | +3 | Add `API_WATERMARK_V2.value` to source-type check at line 42 |
| 10 | `src/lhp/core/orchestrator.py` | Modify | +2 | Add `API_WATERMARK_V2` to `_generate_workflow_resources` source-type match at line 1576 |
| 11 | `tests/fixtures/api_server.py` | Create | +80 | Flask fixture with `/posts`, `/empty`, `/cursor_posts`, `/link_posts` routes |
| 12 | `tests/conftest.py` | Modify | +5 | Expose `api_server` fixture at session scope |
| 13 | `tests/unit/generators/load/test_api_watermark_job.py` | Create | +400 | ~33 test methods |
| 14 | `tests/templates/test_api_watermark_template.py` | Create | +300 | ~22 test methods |
| 15 | `docs/errors_reference.rst` | Modify | +10 | Warning code for missing `schema_hints` on API source |

**Total new production LoC**: ~875 (models 90 + base class 120 + template 540 + generator 120 + registry/init 5 + orchestrator/workflow 5).
**Total new test LoC**: ~785 (unit 400 + template 300 + fixture 80 + conftest 5).
**Files with pure deletions**: `jdbc_watermark_job.py` shrinks by ~170 lines (helpers extracted to base class).

---

## 12. Open Questions for User

### OQ-1 — Should `schema_hints` be required (error) or recommended (warning) for `api_watermark_v2`?

**Current blueprint**: warning at `lhp generate` time, no block.

**Risk if optional**: first empty batch causes `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` in bronze AutoLoader (B2 Wumbo validation would expose this if JSONPlaceholder returns 0 new records on run 2).

**If required**: authors must supply a Spark DDL string in YAML before running. This is a friction point for large or dynamic schemas.

**Recommendation**: make it required for B1 (error, not warning). The failure mode is silent and hard to debug. Relax to warning in B2 if Wumbo evidence shows it is never triggered in practice.

### OQ-2 — HWM filtering for offset pagination: server-side param or client-side idempotency only?

The blueprint emits `_build_hwm_params()` that produces a server-side filter param for numeric HWMs (e.g. `?id_gt=42`). JSONPlaceholder does not honor this param — it returns all 100 posts on every call. This means the B2 "incremental run returns 0 new rows" validation cannot be demonstrated with JSONPlaceholder unless:

a. The Flask fixture's `/posts` route supports `?id_gt=N` filtering (it does, per §9.2 blueprint — the fixture already filters by `id_gt`), OR
b. A real external API with server-side HWM filtering is used for B2 Wumbo.

**User must decide**: is JSONPlaceholder acceptable for B2 Wumbo as a plumbing-only validation (all rows every run, HWM advancement relies on Delta MERGE deduplication), or must B2 use an API that actually filters by HWM?

### OQ-3 — Should `api_watermark_v2` actions share the same DAB Workflow as `jdbc_watermark_v2` actions in the same pipeline?

`WorkflowResourceGenerator` today generates one `{pipeline_name}_workflow.yml` containing all `jdbc_watermark_v2` extraction tasks for that pipeline. Blueprint §11 item 9 adds `API_WATERMARK_V2` to the same source-type check, meaning JDBC and API tasks would coexist in a single workflow.

This is the simplest approach and likely correct (the DLT pipeline task depends on all extraction tasks regardless of source type). However, if the team wants separate workflows per source type for independent scheduling, the generator needs a grouping key change — that is a larger design decision.

**Confirm**: mixed-source-type workflow is acceptable for B1.

---

## Appendix A — L2 §5.3 Control-Flow Contract Checklist

The API template must satisfy each item. Implementation agent verifies using AST-level assertions mirroring `TestControlFlowContract`.

| Requirement | JDBC template location | API template status |
|---|---|---|
| FR-L-07: UTC before first WM call | line 52 | lines 66-68 — verbatim |
| FR-L-09: derive_run_id, no uuid4 | line 55 | lines 69-71 — verbatim |
| FR-L-05: insert_new outside try | line 173 | lines ~420-430 — required |
| FR-L-02: extraction inside try | lines 202-252 | lines ~431-500 — required |
| FR-L-02: except → mark_failed + raise | lines 253-260 | end of try block — verbatim shape |
| FR-L-01: mark_complete outside + after try | lines 265-303 | lines ~501-540 — required |
| FR-L-06a: TerminalStateGuardError re-raised | line 301 | same pattern — required |
| Recovery branch before insert_new | lines 93-121 | lines ~116-140 — verbatim |
| No `@dp.` DLT decorators | — | confirmed |

---

## Appendix B — JDBC-to-API Template Section Mapping

| JDBC section | JDBC lines | API equivalent | Delta |
|---|---|---|---|
| Header | 1-12 | 1-15 | source type label |
| Stdlib imports | 13-21 | 16-40 | + `requests`, `time`, `typing`; − decimal unused path |
| sys.path bootstrap | 27-42 | 41-58 | verbatim |
| `lhp_watermark` imports | 44-49 | 59-65 | verbatim |
| UTC session | 51-52 | 66-68 | verbatim |
| run_id | 55 | 69-71 | verbatim |
| WatermarkManager init | 57-61 | 72-78 | verbatim |
| SQLInputValidator identifiers | 67-71 | 79-88 | `schema_name`=endpoint_name, `table_name`=action_name |
| get_latest_watermark | 73-78 | 89-100 | verbatim |
| _log_phase | 83-86 | 101-108 | verbatim |
| _run_landing_path | 89-90 | 109-115 | verbatim |
| Recovery branch | 93-121 | 116-140 | verbatim |
| _ansi_quote_identifier | 124-132 | removed | JDBC-specific |
| _build_jdbc_query | 135-168 | replaced | → _build_hwm_params + _paginate |
| insert_new | 173-187 | ~420-430 | verbatim kwargs |
| counters | 189-192 | ~431-435 | + `all_records: List` |
| _landing_has_parquet | 195-200 | 361-385 | verbatim |
| try: jdbc read | 202-214 | removed | → API fetch in _paginate |
| try: parquet write | 217 | ~460-475 | `spark.createDataFrame(all_records)` |
| try: post-write agg | 219-226 | ~476-485 | verbatim shape |
| try: empty-batch fallback | 228-244 | ~486-500 | + StructType.fromDDL branch |
| except: mark_failed | 253-260 | same position | verbatim |
| mark_landed + mark_complete | 281-303 | ~501-540 | verbatim |
