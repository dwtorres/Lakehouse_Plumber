---
quick_id: 260424-dwc
branch: spike/jdbc-sdp-a1
type: execute
wave: 1
depends_on: []
autonomous: true
files_modified:
  - spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
  - spikes/jdbc-sdp-b/tasks/prepare_manifest.py
requirements:
  - TIER-1-HWM-ISOLATION

goal: >
  Close the shared-registry HWM poisoning hazard at query level — both spike
  prepare_manifest.py copies filter by source_system_id so no foreign
  federation's HWM can advance ours for the same (schema, table) pair.

must_haves:
  truths:
    - "watermark_registry already has source_system_id column — no DDL needed."
    - "Both call sites are in the `fresh` branch only; `failed_only` reuses watermark_value_at_start from the parent manifest row and does NOT call get_current_hwm."
    - "Both spike copies must stay structurally identical (A1 and B) so behavior remains parallel."
    - "T-tkf-01 regex validation for run_id/parent_run_id and spark.sql(args=...) binding are preserved verbatim — new source_system_id param is bound via args=, never f-string."
  artifacts:
    - path: spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
      provides: "get_current_hwm with source_system_id filter; single call site updated"
      contains: "source_system_id = :source_system_id"
    - path: spikes/jdbc-sdp-b/tasks/prepare_manifest.py
      provides: "get_current_hwm with source_system_id filter; single call site updated"
      contains: "source_system_id = :source_system_id"
  key_links:
    - from: docs/planning/tier-1-hwm-fix.md
      to: "spikes/jdbc-sdp-a1/tasks/prepare_manifest.py + spikes/jdbc-sdp-b/tasks/prepare_manifest.py"
      via: "authoritative scope document; file-by-file change list in §'Files to modify'"
      pattern: "Tier 1 HWM isolation"
    - from: docs/ideas/spike-a1-vs-b-comparison.md
      to: "docs/planning/tier-1-hwm-fix.md"
      via: "risk identification → scheduled fix plan"
      pattern: "shared registry HWM poisoning"
---

<objective>
Apply Tier 1 HWM isolation per `docs/planning/tier-1-hwm-fix.md`: add a
`source_system_id` parameter to `get_current_hwm` in both spike copies of
`prepare_manifest.py`, add the matching `AND source_system_id = :source_system_id`
clause to the WHERE (before the status filter for readability), bind the new
value via `args=`, and update the single call site in each file's `fresh`
branch to pass `src_catalog` / `SOURCE_CATALOG`.

Purpose: Eliminate cross-federation HWM poisoning in the shared
`devtest_edp_orchestration.jdbc_spike.watermark_registry` so a future A1 or B
run against a populated registry stays correct without the manual DELETE
workaround that was used during retest `retest_1777031969`.

Output: Two minimally-edited prepare_manifest.py copies that compile and
whose SQL literal now carries the `source_system_id` filter. Runtime V1/V2/V3
validation from the fix plan is explicitly OUT OF SCOPE for this plan — it
runs separately after commit.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@docs/planning/tier-1-hwm-fix.md
@docs/ideas/spike-a1-vs-b-comparison.md

<interfaces>
<!-- Current shape (A1 copy; B is structurally identical) - extracted from spike sources. -->
<!-- Executor uses these directly — no codebase re-exploration needed. -->

A1 — spikes/jdbc-sdp-a1/tasks/prepare_manifest.py

Lines 101-117 (current):

```python
EPOCH_ZERO = "1900-01-01T00:00:00"


def get_current_hwm(schema_name: str, table_name: str) -> str:
    """Return the latest completed HWM for this table, or EPOCH_ZERO."""
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name   = :schema_name
          AND table_name    = :table_name
          AND status        = 'completed'
        """,
        args={"schema_name": schema_name, "table_name": table_name},
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO
```

Line 139 — the ONLY call site in A1 (inside `if rerun_mode == "fresh":` loop).
`src_catalog` is already bound on line 133 from `row["source_catalog"]`:

```python
src_catalog = row["source_catalog"]
...
hwm_at_start = get_current_hwm(src_schema, src_table)
```

Confirmed: the `failed_only` branch (lines 203-324) does NOT call
`get_current_hwm`. It reads `hwm_at_start = row["watermark_value_at_start"]`
from the parent manifest row (line 236). This matches L2 §5.3 recovery
semantics — the fix plan's hedged language ("or any other call in failed_only
branch if present") resolves to: no additional call sites.

---

B — spikes/jdbc-sdp-b/tasks/prepare_manifest.py

Lines 181-197 (current):

```python
EPOCH_ZERO = "1900-01-01T00:00:00"


def get_current_hwm(schema_name: str, table_name: str) -> str:
    """Return the latest completed HWM for this table, or EPOCH_ZERO."""
    result = spark.sql(
        """
        SELECT MAX(watermark_value) AS hwm
        FROM devtest_edp_orchestration.jdbc_spike.watermark_registry
        WHERE schema_name  = :schema_name
          AND table_name   = :table_name
          AND status       = 'completed'
        """,
        args={"schema_name": schema_name, "table_name": table_name},
    ).first()
    hwm = result["hwm"] if result else None
    return hwm if hwm is not None else EPOCH_ZERO
```

Line 209 — the ONLY call site in B (inside `if rerun_mode == "fresh":` loop).
`SOURCE_CATALOG` is a module-level constant on line 84 (`"pg_supabase"`):

```python
hwm_at_start = get_current_hwm(src_schema, src_table)
```

Confirmed: B's `failed_only` branch (lines 282-369) also does NOT call
`get_current_hwm`. It reads `hwm_at_start = row["watermark_value_at_start"]`
from the parent manifest row (line 298).

Structural difference between A1 and B: A1 passes `src_catalog` (per-row
manifest value) to the new parameter; B passes `SOURCE_CATALOG` (module
constant). Both resolve to the same `source_system_id` semantics in the
registry. Preserve this style difference — don't force symmetry.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Apply Tier 1 HWM isolation to A1 prepare_manifest.py</name>
  <files>spikes/jdbc-sdp-a1/tasks/prepare_manifest.py</files>
  <action>
Apply four edits to this file, in this order, preserving Black 88-char line
length and existing 4-space indentation.

(1) Function signature — line 104. Change:

    def get_current_hwm(schema_name: str, table_name: str) -> str:

to:

    def get_current_hwm(
        schema_name: str, table_name: str, source_system_id: str
    ) -> str:

(Wrap to respect 88-char limit; the 3-arg form fits on one line at ~76 chars
so a single-line form is also acceptable — executor's discretion based on
formatter output. Do NOT run Black as part of this task; just match existing
style.)

(2) SQL WHERE clause — lines 110-112. Add the new predicate BEFORE the status
filter (matches the ordering in docs/planning/tier-1-hwm-fix.md §"Proposed
signature" for readability: schema, table, source_system_id, status). Result:

        WHERE schema_name      = :schema_name
          AND table_name       = :table_name
          AND source_system_id = :source_system_id
          AND status           = 'completed'

Align the `=` column to accommodate the longest identifier (`source_system_id`)
so the SQL stays vertically aligned.

(3) `args=` dict — line 114. Add the new key:

        args={
            "schema_name": schema_name,
            "table_name": table_name,
            "source_system_id": source_system_id,
        },

Rationale for args-binding (not f-string): preserves T-tkf-01 SQL-injection
safety posture. The new parameter value originates from `row["source_catalog"]`
which is already a controlled manifest value, but the binding discipline is a
defense-in-depth invariant we do not relax.

(4) Call site — line 139 (inside `if rerun_mode == "fresh":` loop body).
`src_catalog` is already bound on line 133 as `row["source_catalog"]`. Change:

        hwm_at_start = get_current_hwm(src_schema, src_table)

to:

        hwm_at_start = get_current_hwm(src_schema, src_table, src_catalog)

Do NOT modify the `failed_only` branch (lines 203-324). Confirmed during
planning: that branch does not call `get_current_hwm`; it reuses
`row["watermark_value_at_start"]` from the parent manifest row per L2 §5.3
recovery semantics.

Do NOT modify the two INSERT statements (lines 146-171 and 175-200). Those
already pass `src_catalog` as `source_system_id` and are unaffected by this
fix.

Do NOT touch the module docstring, regex validators, widget definitions, or
any other logic. Keep the inline comment just above `get_current_hwm` as-is.
  </action>
  <verify>
    <automated>
# Static-only acceptance (runtime V1/V2/V3 handled separately post-commit).
cd /Users/dwtorres/src/Lakehouse_Plumber && \
python3 -c "import ast, pathlib; ast.parse(pathlib.Path('spikes/jdbc-sdp-a1/tasks/prepare_manifest.py').read_text()); print('parse OK')" && \
rg -n 'source_system_id = :source_system_id' spikes/jdbc-sdp-a1/tasks/prepare_manifest.py && \
rg -n 'def get_current_hwm\(' spikes/jdbc-sdp-a1/tasks/prepare_manifest.py | rg 'source_system_id' && \
rg -n 'get_current_hwm\(src_schema, src_table, src_catalog\)' spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
    </automated>
  </verify>
  <done>
    - `ast.parse` succeeds on the modified file (prints "parse OK").
    - `rg` locates `source_system_id = :source_system_id` in the SQL literal exactly once.
    - `rg` confirms `source_system_id` appears in the `def get_current_hwm(` signature.
    - `rg` confirms the three-arg call site `get_current_hwm(src_schema, src_table, src_catalog)` exists and the old two-arg form no longer matches.
    - `args=` dict contains the new `"source_system_id"` key (verified by inspection).
    - No edits outside the four points above. `failed_only` branch untouched. Regex validators untouched. INSERT statements untouched.
  </done>
</task>

<task type="auto">
  <name>Task 2: Apply Tier 1 HWM isolation to B prepare_manifest.py</name>
  <files>spikes/jdbc-sdp-b/tasks/prepare_manifest.py</files>
  <action>
Apply the same four edits as Task 1, with the single structural difference
that B passes a module-level constant rather than a per-row value at the call
site.

(1) Function signature — line 184. Change:

    def get_current_hwm(schema_name: str, table_name: str) -> str:

to the same three-arg form used in Task 1 (wrap to 88-char as needed):

    def get_current_hwm(
        schema_name: str, table_name: str, source_system_id: str
    ) -> str:

(2) SQL WHERE clause — lines 190-192. Add the new predicate BEFORE the status
filter. B's copy currently uses two-space alignment; preserve that style and
re-align so `=` columns line up after adding the longer `source_system_id`
identifier. Result:

        WHERE schema_name      = :schema_name
          AND table_name       = :table_name
          AND source_system_id = :source_system_id
          AND status           = 'completed'

(Match the A1 version's alignment exactly so `diff spikes/jdbc-sdp-a1/tasks/prepare_manifest.py spikes/jdbc-sdp-b/tasks/prepare_manifest.py`
continues to show only the intentional differences. Don't introduce a gratuitous alignment divergence.)

(3) `args=` dict — line 194. Add the new key (same as Task 1):

        args={
            "schema_name": schema_name,
            "table_name": table_name,
            "source_system_id": source_system_id,
        },

(4) Call site — line 209 (inside `if rerun_mode == "fresh":` loop body).
B uses the module-level constant `SOURCE_CATALOG = "pg_supabase"` (line 84),
NOT a per-row value — the B pipeline is single-federation by design. Change:

        hwm_at_start = get_current_hwm(src_schema, src_table)

to:

        hwm_at_start = get_current_hwm(src_schema, src_table, SOURCE_CATALOG)

Do NOT modify the `failed_only` branch (lines 282-369). Confirmed during
planning: that branch does not call `get_current_hwm`; it reuses
`row["watermark_value_at_start"]` from the parent manifest row.

Do NOT modify the two INSERT statements, the `AW_TABLES` list, the
`SKIP_TABLES` set, the `manifest_rows_for_foreach` construction, or the
`dbutils.jobs.taskValues.set` emission. Those are unaffected.
  </action>
  <verify>
    <automated>
cd /Users/dwtorres/src/Lakehouse_Plumber && \
python3 -c "import ast, pathlib; ast.parse(pathlib.Path('spikes/jdbc-sdp-b/tasks/prepare_manifest.py').read_text()); print('parse OK')" && \
rg -n 'source_system_id = :source_system_id' spikes/jdbc-sdp-b/tasks/prepare_manifest.py && \
rg -n 'def get_current_hwm\(' spikes/jdbc-sdp-b/tasks/prepare_manifest.py | rg 'source_system_id' && \
rg -n 'get_current_hwm\(src_schema, src_table, SOURCE_CATALOG\)' spikes/jdbc-sdp-b/tasks/prepare_manifest.py
    </automated>
  </verify>
  <done>
    - `ast.parse` succeeds on the modified B file.
    - `rg` locates `source_system_id = :source_system_id` in B's SQL literal exactly once.
    - `rg` confirms `source_system_id` appears in B's `def get_current_hwm(` signature.
    - `rg` confirms the three-arg call site `get_current_hwm(src_schema, src_table, SOURCE_CATALOG)` exists.
    - `args=` dict contains the new `"source_system_id"` key (verified by inspection).
    - No edits outside the four points above. `failed_only` branch untouched. `AW_TABLES`, `SKIP_TABLES`, `manifest_rows_for_foreach`, task-value emission all untouched.
    - Cross-file sanity: both files now carry structurally identical `get_current_hwm` implementations. The only intentional call-site difference is A1→`src_catalog` vs B→`SOURCE_CATALOG`, which is correct and reflects A1's multi-source vs B's single-source design.
  </done>
</task>

</tasks>

<verification>
Whole-plan static acceptance (run AFTER both tasks complete):

```bash
cd /Users/dwtorres/src/Lakehouse_Plumber

# 1. Both files parse.
for f in spikes/jdbc-sdp-a1/tasks/prepare_manifest.py spikes/jdbc-sdp-b/tasks/prepare_manifest.py; do
  python3 -c "import ast, pathlib; ast.parse(pathlib.Path('$f').read_text()); print('OK: $f')"
done

# 2. Both SQL literals carry the new predicate exactly once.
rg -c 'source_system_id = :source_system_id' \
  spikes/jdbc-sdp-a1/tasks/prepare_manifest.py \
  spikes/jdbc-sdp-b/tasks/prepare_manifest.py
# Expect: each file reports "1".

# 3. Neither file still has a two-arg get_current_hwm call site.
rg -n 'get_current_hwm\(src_schema, src_table\)' \
  spikes/jdbc-sdp-a1/tasks/prepare_manifest.py \
  spikes/jdbc-sdp-b/tasks/prepare_manifest.py \
  && echo "FAIL: old two-arg call site still present" \
  || echo "OK: no residual two-arg call sites"

# 4. Both new three-arg call sites exist with their expected argument names.
rg -n 'get_current_hwm\(src_schema, src_table, src_catalog\)' \
  spikes/jdbc-sdp-a1/tasks/prepare_manifest.py
rg -n 'get_current_hwm\(src_schema, src_table, SOURCE_CATALOG\)' \
  spikes/jdbc-sdp-b/tasks/prepare_manifest.py

# 5. Negative check — no f-string SQL composition introduced for the new parameter.
rg -n 'f"""' spikes/jdbc-sdp-a1/tasks/prepare_manifest.py \
              spikes/jdbc-sdp-b/tasks/prepare_manifest.py \
  && echo "FAIL: f-string SQL detected — args= binding should be preserved" \
  || echo "OK: no f-string SQL introduced"

# 6. T-tkf-01 regex validators still present.
rg -n '_RUN_ID_PATTERN = re.compile' \
  spikes/jdbc-sdp-a1/tasks/prepare_manifest.py \
  spikes/jdbc-sdp-b/tasks/prepare_manifest.py
# Expect: both files report a match.
```

All six checks must pass before the plan is considered complete.

Runtime V1/V2/V3 (fake-row probe, A1 regression retest, B regression retest)
are defined in `docs/planning/tier-1-hwm-fix.md` §"Tests / verification" and
run as a separate post-commit activity. They are OUT OF SCOPE here.
</verification>

<success_criteria>
1. Both `spikes/jdbc-sdp-{a1,b}/tasks/prepare_manifest.py` parse cleanly via
   `python3 -c "import ast; ast.parse(...)"`.
2. Both files' `get_current_hwm` signatures accept three positional string
   parameters: `schema_name`, `table_name`, `source_system_id`.
3. Both files' SQL WHERE clauses contain `AND source_system_id = :source_system_id`
   positioned BEFORE `AND status = 'completed'`.
4. Both files' `args=` dicts bind `"source_system_id": source_system_id`.
5. Both files' `fresh`-branch call sites pass a third argument:
   `src_catalog` in A1 (per-row), `SOURCE_CATALOG` in B (module constant).
6. `failed_only` branches untouched in both files. INSERT statements untouched.
   Regex validators (T-tkf-01) untouched.
7. No f-string SQL composition introduced for any parameter, old or new.
8. Commit message follows project convention:
   `fix(spike-a1,spike-b): Tier 1 HWM isolation — add source_system_id filter to get_current_hwm`
   (per `docs/planning/tier-1-hwm-fix.md` §"Rollout plan" step 5).
</success_criteria>

<output>
After completion, create
`.planning/quick/260424-dwc-apply-tier-1-hwm-isolation-add-source-sy/260424-dwc-SUMMARY.md`
recording:
- Confirmation that all six whole-plan verification checks passed.
- The four diff blocks (two per file) actually applied.
- Explicit callout that runtime V1/V2/V3 from the fix plan remain outstanding
  and are NOT part of this plan's scope.
- Pointer back to `docs/planning/tier-1-hwm-fix.md` so the post-commit retest
  sequence is easy to pick up.
</output>
