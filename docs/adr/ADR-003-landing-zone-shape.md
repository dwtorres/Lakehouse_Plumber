# ADR-003 — Landing-Zone Shape for jdbc_watermark_v2 (and Future Batch Loaders)

- **Status**: Proposed / Investigating — no decision yet; research charter only.
- **Date Opened**: 2026-04-19
- **Author**: dwtorres@gmail.com
- **Supersedes**: —
- **Superseded by**: —
- **Related**: [ADR-001](ADR-001-jdbc-watermark-parquet-post-write-stats.md) — landing shape `{root}/_lhp_runs/{run_id}/` accepted; ADR-002 — runtime availability via DAB workspace-file sync; [TD-008](../tech-debt/TD-008-runtime-availability-pattern.md); Constitution §P6 (retry safety by construction).

## Context

ADR-001 accepted the run-scoped Parquet landing layout `{landing_root}/_lhp_runs/{run_id}/`. Constitution P6 depends on run-scoped isolation for retry safety. In production use (Wumbo bundle, run `388301282556004`, 2026-04-19) the DLT AutoLoader schema-inference task failed with `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` on each of 14 landing paths, despite the extractor having successfully written Parquet at `{root}/_lhp_runs/{run_id}/part-*.parquet`. Root cause: the LHP-generated DLT notebook called `.load("{landing_root}")` at the bare root; AutoLoader's schema-inference listing did not see the nested run subtree.

ADR-001 §Consequences §Negative flagged exactly this as an outstanding spot-check ("downstream cloudFiles readers must traverse the `_lhp_runs/` tier ... the existing CloudFilesLoadGenerator should be spot-checked to confirm its emitted read path is compatible") and recorded Phase B V8 evidence that the explicit glob `{root}/_lhp_runs/*` reads all runs correctly, while `recursiveFileLookup=true` at the bare root produced `UNABLE_TO_INFER_SCHEMA`.

The short-term fix (Path ③, shipped in the same PR that opens this ADR) updates `src/lhp/generators/load/jdbc_watermark_job.py` to emit `.load("{root}/_lhp_runs/*")` and default `cloudFiles.useStrictGlobber: "false"` in the generated DLT bronze notebook for `jdbc_watermark_v2` loads. That unblocks production. It does **not** answer whether `_lhp_runs/` is the right long-term landing shape.

This ADR opens the long-term question as a research charter. It does not pick a winner. It enumerates the unresolved questions, the candidate alternatives, and the success criteria that would close it.

### Why the question is load-bearing at scale

The `jdbc_watermark_v2` pattern is additive today, but we already plan to extend the same batch-extract → Parquet landing → AutoLoader streaming topology to:

- additional JDBC sources beyond AdventureWorks / Supabase
- REST API ingestion (LHP `lhp_api` future runtime package)
- CDC batch dumps (Debezium / Fivetran extracts landed as Parquet)
- flat-file drops from third-party producers (already the upstream LHP pattern)

Each additional source type amortizes or amplifies whatever landing convention ADR-001 locked in. A shape that works for 4 tables, 3 runs each, may not work at 100 tables × 1000 runs × 30-day retention.

## Research Questions

### Q1 — Why is the underscore prefix there at all?

`_lhp_runs/` starts with `_`. That prefix is recognized as "hidden" by Hadoop `hiddenFileFilter` and (empirically) by AutoLoader's initial-scan listing. Is the underscore:

- **(a) protective namespace** — intentional, to hide LHP internal state from user ad-hoc scans of the landing volume; OR
- **(b) convention carried over** from a non-Databricks origin (Parquet-on-HDFS-era idioms); OR
- **(c) arbitrary** — chosen without explicit rationale.

If (a), the current Path ③ glob fix is correct and the shape should stay. If (b) or (c), the shape can and probably should change to something that does not require consumers to know about the hidden convention.

**To resolve**: ask ADR-001 author directly (GitHub discussion / issue, or synchronous). Capture the answer in this ADR under §Rationale and close this question.

### Q2 — Retention policy for accumulated run subdirs

At steady state, `{root}/_lhp_runs/` accumulates one subdir per extraction run per table. With 14 tables running daily, that is ~5k subdirs per year per bundle. At 100 tables and sub-hourly cadence, it is 10^6+ subdirs per year. Open:

- What is the operator contract? ("keep last N runs", "keep last 30 days", "keep until DLT has consumed", "keep forever")?
- How is the contract enforced? `streaming_table` rows from deleted subdirs need coordinated DLT reprocessing; naive `rm -r <landing>/_lhp_runs/<old>` leaves orphan rows referencing deleted files.
- Does `cloudFiles.cleanSource` (DBR 16.4+) cover this or do we need our own reaper job?
- Directory-listing cost at 10^5 subdirs on Unity Catalog volumes — benchmark before committing.

### Q3 — Empty-batch schema-inference hardening

When a `jdbc_watermark_v2` run returns zero rows (source had no new data since last HWM), Spark's Parquet writer may produce either zero files or one 0-byte schema-only Parquet, depending on version and configuration. AutoLoader's schema inference on a landing with no row-bearing Parquet files raises `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE` — the same symptom that motivated Path ③.

Candidate mitigations:

- Extractor always writes a schema-bearing 0-row Parquet.
- DLT bronze uses `cloudFiles.schemaHints` declared at generator time (requires user to supply schema or us to derive from JDBC metadata).
- Generator falls back to `cloudFiles.schemaHints` derived from the watermark table's row-shape registry.

Decide as part of this ADR. Preferably empirically — run the extractor against an empty-result query and observe failure modes.

### Q4 — Hive-style partition vs underscore-prefix vs flat

Three concrete shapes are on the table:

- **A — status quo** (`_lhp_runs/{run_id}/`): ADR-001's decision. Works with Path ③'s glob fix. Underscore prefix exposes the Hadoop-hidden-file footgun.
- **B — Hive partition** (`run_id={run_id}/`): not hidden. AutoLoader auto-discovers partition column, surfaces `run_id` in bronze as a queryable column. `DELETE WHERE run_id = ...` + `rm -r <landing>/run_id=<old>/` enables retention. Supersedes ADR-001 §Decision.
- **C — drop the landing tier**: batch JDBC → direct write to bronze Delta via `dlt.append_flow`. No landing, no AutoLoader, no glob problem. Large architectural change. Breaks the "extractor decoupled from bronze" pattern.

Each changes different invariants. Decide against §Success Criteria below.

### Q5 — Where does the landing volume live relative to bronze tables? (surfaced 2026-04-19)

After Path ③ shipped (LHP PR #3) the Wumbo E2E run reached AutoLoader schema inference successfully but the DLT streaming query failed with:

```
ErrorClass=INVALID_PARAMETER_VALUE.LOCATION_OVERLAP
Input path url 's3://.../volumes/<vol_id>/<table>/_lhp_runs/<uuid>/part-*.parquet'
overlaps with managed storage within 'CheckPathAccess' call
```

Unity Catalog's `CheckPathAccess` rejects reads from a volume whose storage root overlaps with any managed entity in the same schema. Our initial Wumbo setup co-located the landing volume (`main.bronze.landing`) with bronze streaming_tables (`main.bronze.<table>`) in the same `main.bronze` schema. On managed-catalog defaults the volume's physical S3 prefix lives inside the same `__unitystorage/catalogs/<cid>/` tree as the managed-table storage, which is exactly what the overlap check forbids.

**Empirical fix** (Wumbo PR #2): move landing to a dedicated `main._landing` schema (`main._landing.landing` volume). Identical catalog, different schema, no shared managed-storage overlap. E2E validated: DLT update `338e767a-18d5-4d55-a030-587ba6a8de0e` — all 14 bronze flows COMPLETED, all 14 tables populated.

**Operational consequences that scale this question**:

- **Schema proliferation**: per-environment landing schema adds a schema-per-env (dev/qa/prod), but only one per env, and it isolates a real UC rule.
- **External ADLS volume pattern**: the reference target. One external volume per environment×medallion backed by user-controlled storage. External volumes don't participate in managed-storage overlap checks because their storage location is outside the catalog's managed root. This is the production shape. Wumbo PR #2's `landing_schema` substitution is forward-compatible — map `landing_schema` to an external-volume-backed schema per env with no YAML change.
- **Per-env catalog pattern**: future production convention for this fork is `{env}_edp_{medallion}` catalogs (e.g. `devtest_edp_bronze`, `devtest_edp_silver`, `devtest_edp_gold`). A dedicated `{env}_edp_landing` catalog or an external volume inside bronze becomes the production equivalent of `main._landing`.
- **DLT checkpoint migration**: when landing volume paths change, the DLT pipeline's `_dlt_metadata/checkpoints/<stream>/` retains state tied to the old source path and raises `LOCATION_OVERLAP` against the old volume's files on replay. Resolution requires `databricks bundle run -t <target> --full-refresh-all <pipeline_name>` to reset checkpoints. Document this explicitly in the runbook (§LOCATION_OVERLAP recovery).

**Operator contract this ADR should ratify**:

Landing volumes MUST NOT share a schema with managed tables that LHP also writes to. Mechanism of enforcement:

- **Short-term**: `lhp generate` validates `landing_path` does not resolve inside a schema that also hosts a write-action target, warning on conflict.
- **Medium-term**: generator emits `cloudFiles.schemaLocation` into a landing-schema-relative path (not into a managed-table-bearing schema).
- **Long-term**: `lhp init` / project scaffolding creates the `_landing` schema automatically and wires the substitution token.

**Status**: this Q is the first ADR-003 question with empirical evidence (Wumbo PR #2). It is **not** closed in this ADR — the scale question (per-env catalog pattern, external volume integration) remains open. Mark as "partially answered" once Wumbo PR #2 merges; full closure requires the generator-side validation work above.

## Alternatives

| ID | Alternative | Scope | ADR-001 impact |
|----|-------------|-------|----------------|
| A  | Keep `_lhp_runs/`, rely on Path ③ glob | status quo + Path ③ | none — refines the read-side spot-check |
| B  | Hive-style `run_id={run_id}/` | medium (1-line template + migration) | supersedes §Decision landing-shape clause |
| C  | Drop landing tier (direct-to-Delta) | large (template rewrite + DLT semantics shift) | supersedes most of ADR-001 |
| D  | Per-source-type divergent shapes | small-per-source, large-overall | per-source ADR each |

## Decision

**Deferred.** Not to be taken in this ADR. Decide after the Research Questions above are empirically answered, when we have evidence from at least one additional source type beyond `jdbc_watermark_v2` (so scale assumptions are validated, not extrapolated).

Current status: the codebase remains on Alternative A (Path ③-fixed). No migration pending.

## Success Criteria (to close this ADR)

- [ ] Q1 answered by ADR-001 author (rationale captured in this doc).
- [ ] Q2 retention policy ratified with operator contract + enforcement mechanism + benchmark at ≥10k subdirs.
- [ ] Q3 empty-batch case verified end-to-end (Phase B-style run with zero-row JDBC result reaches DLT bronze without `CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE`).
- [ ] A second batch-loader source type (API, CDC, or flat-file) is implemented, validating whether Alternative A generalizes or Alternative B/D is required.
- [~] Q5 partially answered by Wumbo PR #2 (2026-04-19): dedicated landing schema sidesteps UC `LOCATION_OVERLAP`. Full closure requires the generator-side validation work (landing-path-in-same-schema-as-write-target warning) and the production external-ADLS-volume pattern being ratified.
- [ ] Final decision between A/B/C/D recorded in this ADR; if B or C, superseding sections added to ADR-001.

Until all five boxes are ticked (and Q5 is fully closed), this ADR stays Proposed / Investigating. No shape change will be made ad-hoc.

## Out of Scope

- Path ③ itself (shipped separately; Track A in the opening PR).
- Runtime availability (ADR-002).
- Parquet-post-write-stats decision (ADR-001).
- Constitution amendments. P6 is agnostic about the specific subdirectory name, only about the run-scoped isolation invariant; any alternative here must preserve P6.

## Links

- ADR-001 §Decision, §Consequences §Negative, §Evidence (Phase B V8)
- Wumbo bundle run `388301282556004` (2026-04-19) — the failing E2E that surfaced the question
- Wumbo reference bundle: https://github.com/dwtorres/Wumbo
- Constitution §P6 — retry safety by construction
- TD-008 — runtime availability (related but orthogonal)
