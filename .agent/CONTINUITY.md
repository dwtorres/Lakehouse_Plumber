# Continuity

## [PLANS]

- 2026-04-17T23:25Z [USER] Bootstrap missing GSD project state first via map-codebase and new-project semantics, then implement the approved `jdbc_watermark_v2` hardening plan.

## [DECISIONS]

- 2026-04-17T23:25Z [USER] `jdbc_watermark_v2` is the active production path; legacy `jdbc_watermark` should be removed.
- 2026-04-17T23:25Z [TOOL] `gsd headless` cannot run in this repo until project state exists; bootstrap is being created manually in `.planning/`.

## [PROGRESS]

- 2026-04-17T23:25Z [TOOL] Read current v1/v2 code paths, watermark runtime, workflow generator, tests, Databricks bundle docs, and Wumbo deployment shape.
- 2026-04-18T06:06Z [CODE] Removed legacy `jdbc_watermark` support from the public path by deleting the v1 generator/template, removing enum and registry wiring, and turning validator/config lookups into migration failures.
- 2026-04-18T06:06Z [CODE] Hardened `jdbc_watermark_v2` extraction to use one JDBC read, run-scoped landing directories, recovery preflight, `mark_landed` plus `mark_complete`, and structured notebook phase logging.
- 2026-04-18T06:06Z [CODE] Preserved CloudFiles passthrough behavior for the generated Bronze stub, added optional serial extraction workflow chaining, and added explicit-profile Databricks verification plus benchmark scripts.
- 2026-04-25T23:19Z [TOOL] Direct-JDBC validation used profile `dbc-8e058692-373e`, cloned the `[dev verbena1] jdbc_ingestion_workflow` connection shape without exposing secrets, and ran scratch-isolated AdventureWorks validations.

## [DISCOVERIES]

- 2026-04-17T23:25Z [CODE] `src/lhp/generators/load/jdbc_watermark_job.py` currently synthesizes a minimal CloudFiles source and drops richer Auto Loader behavior.
- 2026-04-17T23:25Z [CODE] `src/lhp/extensions/watermark_manager/_manager.py` already guards `landed_not_committed`, but the generated notebook does not yet drive that state.
- 2026-04-17T23:25Z [TOOL] `databricks bundle validate --target dev --profile dbc-8e058692-373e` succeeded in Wumbo, while profile-less validation failed because multiple local profiles match the same host.
- 2026-04-18T06:06Z [TOOL] Consolidated local verification now passes after post-format rerun: `180 passed in 5.51s`.
- 2026-04-18T06:06Z [TOOL] `python -m black --check` is clean on touched files; the remaining warning is Black's Python 3.12 vs configured py313 safety-check limitation, not a formatting failure.
- 2026-04-18T12:18Z [CODE] Residual review found that `jdbc_watermark_v2` still interpolates `source.table` directly into the remote JDBC subquery (`jdbc_watermark_job.py.j2`) with only generic string validation; validator coverage still checks presence, not identifier/query safety.
- 2026-04-18T12:18Z [CODE] Residual review found a contract mismatch where the generator can derive `source_system_id` from hostnames containing hyphens, but `get_latest_watermark()` / `get_recoverable_landed_run()` validate those lookup keys with `SQLInputValidator.identifier`, which rejects hyphens.
- 2026-04-18T12:18Z [CODE] Residual review found that extraction marks watermarks `completed` immediately after durable landing, before the downstream DLT task runs; this protects the source DB but leaves end-to-end ingest completeness dependent on separate landing-zone durability and pipeline monitoring.
- 2026-04-25T23:19Z [TOOL] Static direct-JDBC 68-table validation completed successfully in 49.58 min with 71/71 tasks successful, but the no-change second run duplicated max-watermark boundary rows because the deployed/current static template used `>=` semantics.
- 2026-04-25T23:19Z [TOOL] B-style direct-JDBC `for_each` validation with strict `>` semantics completed 68 tables successfully: full run 45.31 min, 759,240 source/target rows, zero validation failures.
- 2026-04-25T23:19Z [TOOL] B-style direct-JDBC second no-change run completed successfully: 68 latest watermark rows all had `row_count=0`, target total remained 759,240, and validation reported zero failures.
- 2026-04-25T23:19Z [TOOL] UC table quota blocked a B run that attempted 68 registered target tables plus a scratch watermark table (`estimated count: 539/500` after cleanup); path-based Delta outputs avoided UC table creation and kept the orchestration/JDBC test moving.

## [OUTCOMES]

- 2026-04-18T06:06Z [TOOL] Implementation is complete for the approved hardening scope: v1 removed, v2 hardened, CloudFiles fidelity restored, optional serial workflows added, and operator verification tooling documented.
- 2026-04-18T12:18Z [TOOL] Senior-engineer review result: materially stronger than the 2026-04-17 Wumbo reports imply, but still not battle-hardened for broad production until query-shape validation, source-system key consistency, end-to-end completion semantics, and large-table performance strategy are addressed.
- 2026-04-25T23:19Z [TOOL] Direct-JDBC spike outcome: promote B/B2 direction over the current static `jdbc_watermark_v2` process for mass scale-out, contingent on implementing strict `>` watermark predicates, idempotent/retry-safe writes, explicit concurrency controls, and a UC table strategy that does not create one table per extract when quotas are tight.
