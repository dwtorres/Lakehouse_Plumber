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

## [DISCOVERIES]

- 2026-04-17T23:25Z [CODE] `src/lhp/generators/load/jdbc_watermark_job.py` currently synthesizes a minimal CloudFiles source and drops richer Auto Loader behavior.
- 2026-04-17T23:25Z [CODE] `src/lhp/extensions/watermark_manager/_manager.py` already guards `landed_not_committed`, but the generated notebook does not yet drive that state.
- 2026-04-17T23:25Z [TOOL] `databricks bundle validate --target dev --profile dbc-8e058692-373e` succeeded in Wumbo, while profile-less validation failed because multiple local profiles match the same host.
- 2026-04-18T06:06Z [TOOL] Consolidated local verification now passes after post-format rerun: `180 passed in 5.51s`.
- 2026-04-18T06:06Z [TOOL] `python -m black --check` is clean on touched files; the remaining warning is Black's Python 3.12 vs configured py313 safety-check limitation, not a formatting failure.

## [OUTCOMES]

- 2026-04-18T06:06Z [TOOL] Implementation is complete for the approved hardening scope: v1 removed, v2 hardened, CloudFiles fidelity restored, optional serial workflows added, and operator verification tooling documented.
