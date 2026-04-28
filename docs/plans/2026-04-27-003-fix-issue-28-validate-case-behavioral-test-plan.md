---
title: "fix(b2,#28): behavioral CASE-form test for validate.py.j2 final_status projection"
type: fix
status: active
date: 2026-04-27
origin: https://github.com/dwtorres/Lakehouse_Plumber/issues/28
---

# fix(b2,#28): behavioral CASE-form test for validate.py.j2 final_status projection

## Overview

Issue #28 documents a class of test gap: PR #26's `final_status` CASE in
`src/lhp/templates/bundle/validate.py.j2` was structurally correct but
semantically wrong on the silent-divergence input combination (`manifest='running'`
+ `worker='completed'`). The U3 tests for that PR were render-substring style —
they confirmed the new CASE *form* was present, but never executed the CASE
against the input/output pairs the plan called out. Devtest replay caught the
false-pass live and PR #27 added an explicit guard branch.

The structural fix already shipped. This plan closes the **test-coverage** gap
that allowed the bug through: add a behavioral test that executes the rendered
CASE expression against a real SQL engine for each `(manifest_status,
worker_status) → final_status` pair the plan explicitly named.

The same lemma applies to future projections in this template — once the
behavioral harness exists, it can be reused for any later CASE/coalesce
projection in `validate.py.j2`.

---

## Problem Frame

**Bug class:** SQL projection logic tested only by render-substring assertions.
Tests confirm a CASE branch *appears* in the rendered output, not that it
*evaluates correctly* on the input combinations the plan called out.

**Specific gap caught by issue #18 devtest replay (2026-04-27):**

| `manifest_status` | `worker_status` | Plan-intended `final_status` | PR #26 actual `final_status` (live devtest) |
|---|---|---|---|
| `running` | `completed` | `running` (loud failure) | `completed` (silent false pass) |

The PR #26 CASE form contained:

```sql
CASE
  WHEN m.manifest_status = 'completed'
       AND (w.worker_status IS NULL OR w.worker_status = 'completed')
    THEN 'completed'
  WHEN m.manifest_status = 'failed' THEN 'failed'
  ELSE coalesce(w.worker_status, m.manifest_status)
END
```

For the `('running', 'completed')` input the ELSE branch evaluated
`coalesce('completed', 'running') = 'completed'` — same false-pass that the
original bare `coalesce(worker_status, manifest_status)` projection produced.
PR #27 added an explicit guard branch *before* the ELSE.

**Why the unit suite missed it:** `tests/test_validate_template.py::test_final_status_running_plus_completed_watermark_guard` (lines 623-646) asserts the guard branch *string* is present and counts to 2 (count + failure-enumeration sites). It never executes the CASE.

**Two repeating shapes that this plan locks down:**

1. Behavioral coverage of the CASE projection at every `(manifest_status, worker_status)` row the plan calls out.
2. Re-usable harness so future CASE/coalesce changes in this template inherit behavioral coverage instead of render-substring coverage.

---

## Requirements Trace

- R1. Every `(manifest_status, worker_status)` input combination named in the issue #18 plan U2 scenario list has a behavioral test that executes the rendered CASE and asserts the resulting `final_status`.
- R2. Both `validate.py.j2` SQL call sites (count aggregate + failure enumeration CTE) are exercised — not just one.
- R3. The CASE under test is the *rendered* CASE extracted from `validate.py.j2`, not a copy-pasted re-statement (or the test trivially passes when the template diverges).
- R4. The harness is reusable: a future PR adding a new branch to either CASE call site can add one row to the test matrix without rebuilding the harness.
- R5. Test runs in the existing `pytest` suite without a JVM, Spark, or external service. No CI changes required.
- R6. DuckDB dialect divergence from Spark is documented in the test file with the exact list of SQL features the rendered projection relies on (so future template changes signal when DuckDB compatibility breaks).

---

## Scope Boundaries

- Behavioral testing of *other* SQL in `validate.py.j2` (the `manifest` / `worker_states` CTEs themselves, the empty-batch `count(*) = 0` path, the `count_if` aggregations) — not in scope. This plan covers only the `final_status` CASE projection.
- Behavioral testing of any other Jinja-rendered SQL template (`prepare_manifest.py.j2`, watermark templates, etc.) — not in scope. The harness is reusable but applying it elsewhere is a follow-up.
- Migrating *existing* render-substring tests to behavioral tests — not in scope. Existing assertions stay; new behavioral tests are additive.
- Spark/PySpark integration testing — explicitly rejected (see Alternatives).

### Deferred to Follow-Up Work

- Apply same harness pattern to `prepare_manifest.py.j2` MERGE projection: separate issue/PR, after harness ergonomics are validated by this PR.

---

## Context & Research

### Relevant Code and Patterns

- `src/lhp/templates/bundle/validate.py.j2:154-176` — count-query CASE projection (final CTE).
- `src/lhp/templates/bundle/validate.py.j2:234-253` — failure-enumeration CTE with same CASE form.
- `tests/test_validate_template.py:39-75` — `_render()` helper with the `tojson` post-processing fixup; reuse as-is.
- `tests/test_validate_template.py:583-646` — existing render-substring CASE tests (`test_final_status_case_projection_present`, `test_final_status_no_bare_coalesce_projection`, `test_final_status_running_plus_completed_watermark_guard`); keep, add behavioral tests alongside.
- `tests/test_validate_template.py:140-179` — `_ScriptedSpark` fake. **Not used by behavioral tests** — those execute the CASE in DuckDB directly. The fake remains for the existing notebook-execution tests.

### Institutional Learnings

- Memory `feedback_lhp_secret_regex_apostrophe` (apostrophe-in-template-comments swallows `__SECRET_*__`) — same shape as this gap: structural template change ships passing unit tests but is wrong against live behavior. Both classes argue for behavioral verification of template output, not just rendered-text inspection.
- Issue #28 body itself articulates the lesson: "Render-substring tests are insufficient for SQL projection logic. When the plan calls out a specific input/output pair, the test must exercise that input/output behaviorally — not just confirm a syntactic shape."

### External References

- DuckDB CASE expression docs: https://duckdb.org/docs/sql/expressions/case (CASE WHEN ... THEN ... ELSE ... END syntax matches Spark exactly for the constructs used here).
- DuckDB `count_if` support: https://duckdb.org/docs/sql/aggregates#count_if (supported natively; matches Spark dialect — relevant for any future expansion to test the count_if aggregation, but not strictly needed for the CASE-only scope of this plan).

---

## Key Technical Decisions

- **DuckDB chosen over sqlite3 and PySpark.** sqlite3 needs `count_if` rewritten as `sum(case ...)` and `USING (col)` is not supported in older sqlite versions — both add divergence between test SQL and template SQL. PySpark adds JVM startup overhead and a dev dep that dwarfs the value of one test file. DuckDB executes the rendered CASE verbatim, runs in-process, ~10MB wheel.
- **Extract CASE substring from rendered template, not re-state it in test code.** A regex extracts the exact `CASE ... END AS final_status` block from each call site. Test then wraps it in `SELECT <case>, ... FROM (VALUES ...) AS t(manifest_status, worker_status)` and runs it. Re-stating the CASE in the test would let template drift go undetected — exactly the failure mode this plan exists to prevent.
- **Parametrized scenario matrix using pytest `@pytest.mark.parametrize`.** One row per `(manifest_status, worker_status, expected_final_status)` tuple, executed twice (once per call site). Adding a new branch = adding a row.
- **DuckDB column aliasing for `m.` and `w.` prefixes.** The rendered CASE uses `m.manifest_status` and `w.worker_status` qualifiers. Test fixtures use a single `VALUES` table and alias columns to `m_manifest_status` / `w_worker_status`, then use a string substitution on the extracted CASE to swap `m.manifest_status` → `m_manifest_status` and `w.worker_status` → `w_worker_status` before execution. Documented in test file with the exact substitutions performed so a reader can audit fidelity.
- **Both call sites tested, not just count query.** The PR #26 → PR #27 history shows the call sites can drift independently. Test matrix iterates over `[count_case, failure_case]`.

---

## Open Questions

### Resolved During Planning

- **Test engine** → DuckDB (user choice; see Key Technical Decisions).
- **Should existing render-substring tests be removed?** → No. They are cheap, catch a different failure mode (typo / regex match), and removing them would lose coverage of the structural-correctness invariant that PR #26's tests *did* enforce correctly. Behavioral tests are additive.
- **Should the harness live in a shared helpers file?** → No, for now. One template, one test file. If/when `prepare_manifest.py.j2` adopts the same pattern (Deferred), refactor to `tests/_sql_behavioral.py` then.

### Deferred to Implementation

- Exact regex form to extract the CASE block from `_render()` output. The implementer may need to tune the regex once they see real edge cases (whitespace, trailing comma after `END AS final_status`). The constraint is: regex must extract *exactly* the substring inside each of the two CASE blocks in the rendered template, no more, no less. If a single regex cannot reliably extract both call sites, two named regexes is acceptable.
- Whether to express the scenario matrix as a pure `pytest.parametrize` or as a small helper that yields scenarios — implementer's call. Either satisfies R1 + R4.

---

## Implementation Units

- [ ] U1. **Add `duckdb` to dev dependencies**

**Goal:** Unblock import of `duckdb` from test code without affecting runtime install.

**Requirements:** R5

**Dependencies:** None

**Files:**
- Modify: `pyproject.toml` (add `duckdb>=1.0.0` under the appropriate dev/test extras section — match existing pattern for `pytest`, `pytest-mock`, etc.)

**Approach:**
- Locate the existing dev-deps block in `pyproject.toml` (project uses `[project.optional-dependencies]` or equivalent — discovery during implementation).
- Add `duckdb` with a conservative lower bound (`>=1.0.0`); no upper pin.
- Verify install with a one-off `pip install -e ".[dev]"` (or equivalent project-specific command). Not a CI-required step; it is a sanity check during implementation.

**Patterns to follow:**
- Match the existing dev-dep entry style (alphabetical position, version-bound convention) used by `pytest-cov`, `pytest-mock`.

**Test scenarios:**
- Test expectation: none — pure dependency declaration. The behavioral test in U2 is the verification that the dep is wired correctly (ImportError if not).

**Verification:**
- `python -c "import duckdb; print(duckdb.__version__)"` succeeds in the dev environment.
- `pip install -e ".[dev]"` (or project equivalent) resolves without conflict.

---

- [ ] U2. **Behavioral CASE-form test matrix**

**Goal:** Lock down `final_status` CASE behavior at every `(manifest_status, worker_status)` input the issue #18 plan U2 named, executed against both rendered call sites.

**Requirements:** R1, R2, R3, R4, R6

**Dependencies:** U1

**Files:**
- Modify: `tests/test_validate_template.py` (add a new test section at end of file, below `test_final_status_running_plus_completed_watermark_guard` — keep existing render-substring tests intact)
- Test: `tests/test_validate_template.py` (same file; this is the test file)

**Approach:**

1. Add a CASE-extraction helper. Input: rendered template string. Output: list of two CASE substrings, one per call site, in template-source order. The helper documents in its docstring exactly what each entry corresponds to (`[count_case, failure_case]`). Failure mode: if extraction does not yield exactly two CASE blocks, raise `AssertionError` with the rendered output snippet so diagnosis is fast when template structure changes.
2. Add a DuckDB-execution helper. Input: extracted CASE substring + a list of `(manifest_status, worker_status)` input tuples. Output: list of resulting `final_status` values. Internally:
   - Substitute `m.manifest_status` → `m_manifest_status` and `w.worker_status` → `w_worker_status` (documented in the helper's docstring with exact replacements).
   - Open a fresh in-memory DuckDB connection (`duckdb.connect(":memory:")`).
   - Build SQL: `SELECT <substituted-case> AS final_status FROM (VALUES <rows>) AS t(m_manifest_status, w_worker_status)` and execute.
   - Return the projected column as a list of strings (or `None`).
3. Define the scenario matrix as a module-level constant (one tuple per row: `(manifest_status, worker_status, expected_final_status, label)`). The label feeds `pytest.mark.parametrize`'s `ids=` parameter so failures name the scenario.
4. Add the parametrized behavioral test that, for each scenario, runs both call-site CASEs through the DuckDB helper and asserts the result equals `expected_final_status`. One test function, two-axis parametrize: `(scenario, call_site)`.
5. Add an in-file docstring section listing the SQL features the rendered CASE relies on (CASE/WHEN/THEN/ELSE/END, equality on string columns, `coalesce`, NULL semantics) and noting that template additions of new SQL features (window functions, lateral subqueries, etc.) require re-validating DuckDB compatibility.

**Execution note:** Test-first. Write the parametrized matrix first against the *current* template; verify all scenarios pass. Then deliberately break the template (e.g., remove the silent-divergence guard branch) in a scratch worktree and confirm the matrix catches it. Revert. This proves the test exercises the CASE rather than a tautology.

**Technical design:**

> *This illustrates the intended approach and is directional guidance for review, not implementation specification. The implementing agent should treat it as context, not code to reproduce.*

```python
# Sketch — not implementation. Names, regex, and structure are illustrative.

CASE_BLOCK_RE = re.compile(r"CASE\s+WHEN.*?END\s+AS\s+final_status", re.DOTALL)

# (manifest_status, worker_status, expected_final_status, label)
SCENARIOS = [
    ("completed", "completed",         "completed",  "happy_path"),
    ("completed", None,                "completed",  "manifest_done_no_worker_row"),
    ("failed",    "failed",            "failed",     "loud_failure"),
    ("failed",    None,                "failed",     "manifest_failed_no_worker_row"),
    ("running",   "completed",         "running",    "issue_18_silent_divergence_guard"),
    ("running",   "running",           "running",    "in_flight_passthrough"),
    ("running",   None,                "running",    "worker_never_started"),
    ("running",   "abandoned",         "abandoned",  "else_coalesce_intermediate_status"),
    ("running",   "landed_not_committed", "landed_not_committed", "else_coalesce_partial_landing"),
]

CALL_SITES = ["count_case", "failure_case"]

@pytest.mark.parametrize("manifest_status,worker_status,expected,label", SCENARIOS, ids=...)
@pytest.mark.parametrize("call_site", CALL_SITES)
def test_final_status_case_behavioral(manifest_status, worker_status, expected, label, call_site):
    rendered = _render()
    cases = _extract_case_blocks(rendered)            # returns [count_case, failure_case]
    case_sql = cases[CALL_SITES.index(call_site)]
    [actual] = _eval_case_in_duckdb(case_sql, [(manifest_status, worker_status)])
    assert actual == expected
```

**Patterns to follow:**
- `tests/test_validate_template.py::_render` for template loading + post-processing.
- Existing parametrized tests in the project for `@pytest.mark.parametrize` `ids=` convention (search the repo for `parametrize(...ids=` and match style).
- Module-level constants in UPPER_SNAKE_CASE per `STANDARDS.md`.

**Test scenarios:**
The implementation *is* the test matrix. The bullets below restate it for review traceability. Each bullet is one row of the SCENARIOS list, executed against both call sites (count + failure-enumeration), so 9 scenarios × 2 call sites = **18 behavioral assertions**.

- Happy path. `(completed, completed) → completed` covers the WHEN-1 branch on both call sites. *Why it matters:* baseline — any breakage of WHEN-1 fails this first.
- Edge case. `(completed, NULL) → completed` covers the `worker_status IS NULL` half of WHEN-1. *Why it matters:* when worker never wrote a watermarks row but manifest is terminal-completed (rare but legal), validate must trust manifest.
- Happy path. `(failed, failed) → failed` covers WHEN-2 branch.
- Edge case. `(failed, NULL) → failed` covers WHEN-2 with no worker row. *Why it matters:* worker mark_failed always writes the watermarks row, but defense-in-depth — manifest-only failure must still surface.
- Error path. `(running, completed) → running` is **the issue #18 R4 silent-divergence guard**. *Why it matters:* this is the exact false-pass devtest caught on PR #26; the WHEN-3 branch must fire before ELSE.
- Edge case. `(running, running) → running` covers ELSE coalesce with both sides 'running'. *Why it matters:* in-flight pass-through; ELSE must not corrupt to NULL or some other terminal value.
- Edge case. `(running, NULL) → running` covers ELSE coalesce when worker never started. *Why it matters:* manifest-only-pending case (matches the existing `test_manifest_only_pending_raises` scenario at the notebook level; behavioral assertion at the projection level).
- Integration. `(running, abandoned) → abandoned` covers ELSE coalesce surfacing a worker-side intermediate status that never lands in `b2_manifests`. *Why it matters:* operator visibility — these intermediate statuses are exactly why the CASE keeps a coalesce in ELSE.
- Integration. `(running, landed_not_committed) → landed_not_committed` mirrors the previous scenario for the landing-without-commit intermediate state.

**Verification:**
- `pytest tests/test_validate_template.py -k case_behavioral -v` reports 18 passes (9 scenarios × 2 call sites), each with a labeled scenario id.
- Mutation check (manual, not committed): deleting the WHEN-3 silent-divergence guard from the template makes scenario `issue_18_silent_divergence_guard` fail on both call sites with `'completed' != 'running'` — confirms the test exercises rendered template behavior, not a tautology.
- `pytest tests/test_validate_template.py -v` runs the full file (existing 18 tests + new 18 assertions) with no regressions in pre-existing tests.

---

## System-Wide Impact

- **Interaction graph:** None. The new tests are pure unit-level — `_render()` produces a string, the helpers parse and execute against in-memory DuckDB.
- **Error propagation:** Test failures must name the failing scenario via `pytest.mark.parametrize` ids; the assertion message must include the actual `final_status`, expected `final_status`, call-site name, and the (manifest_status, worker_status) input. Pytest's default parametrize id rendering plus a clear `assert actual == expected, f"..."` message are sufficient.
- **State lifecycle risks:** None — DuckDB connection is per-test, in-memory.
- **API surface parity:** None — internal test infra, not exported.
- **Integration coverage:** This plan's purpose is to *add* integration-style coverage (template-rendered SQL executed against a real engine). Outside scope: applying same pattern elsewhere (deferred).
- **Unchanged invariants:** `_render()` post-processing fixup (the `tojson` hack at lines 64-74) is unchanged. The `_ScriptedSpark` fake and notebook-execution tests are unchanged. Existing render-substring tests are unchanged.

---

## Risks & Dependencies

| Risk | Mitigation |
|---|---|
| DuckDB CASE/coalesce/NULL semantics diverge from Spark for some edge case in the test matrix. | Restrict scenarios to the constructs the rendered CASE actually uses (string equality, `coalesce`, NULL → false in equality). Document the dialect-feature list in the test file (R6). If a future template change introduces a Spark-only feature, the documented list signals the boundary. |
| Regex-based CASE extraction is fragile to formatting changes in `validate.py.j2`. | Extraction helper raises `AssertionError` with the rendered snippet if it does not yield exactly 2 CASE blocks — fast diagnosis on template reformat. Helper is one place to update if template style changes. |
| Adding `duckdb` as a dev dep enlarges the test environment by ~10MB and adds a transitive surface. | DuckDB is widely used (Pandas, dbt) and stable. Pinning `>=1.0.0` is conservative. Dev-only — not in runtime install. |
| Test matrix becomes a magnet for unrelated CASE additions ("just add one more row"). | `Test scenarios` section in U2 enumerates exactly which scenarios are in scope; future PRs adding new template branches must add their *own* row with their *own* rationale, reviewed alongside the template change. The harness is reusable; the matrix is not a shared utility. |
| Implementer extracts the CASE, then re-states it inline in the test, defeating R3. | Code-review checklist item: "verify behavioral test executes the *extracted* CASE substring, not a re-stated copy." Plan U2 step 1 explicitly says "extract from rendered template, not re-state." |

---

## Documentation / Operational Notes

- Issue #28's "Lessons" section in the issue body remains the canonical writeup; no separate runbook/ADR needed for a test-coverage fix.
- After merge, close issue #28 with a comment linking the merging PR. The `[ ] Consider` checkbox in the issue's Status section is what this plan satisfies.
- No operational impact — pure test addition.

---

## Sources & References

- **Origin issue:** https://github.com/dwtorres/Lakehouse_Plumber/issues/28
- **Antecedent PRs:** #26 (issue #18 fix), #27 (issue #18 follow-up), #29 (P3 backlog)
- **Antecedent plan:** `docs/plans/2026-04-27-001-fix-b2-issue-18-duplicate-run-id-handling-plan.md` U2 (the plan whose scenario list this test matrix mirrors)
- **Template under test:** `src/lhp/templates/bundle/validate.py.j2` lines 154-176 (count CASE), 234-253 (failure-enumeration CASE)
- **Test file under modification:** `tests/test_validate_template.py`
- **DuckDB docs:** https://duckdb.org/docs/sql/expressions/case
