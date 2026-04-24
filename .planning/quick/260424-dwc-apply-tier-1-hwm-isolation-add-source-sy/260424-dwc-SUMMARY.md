---
quick_id: 260424-dwc
status: complete
branch: spike/jdbc-sdp-a1
commit: 63bcfdb
date: 2026-04-24
---

# Quick Task 260424-dwc — Summary

## Goal

Apply Tier 1 HWM isolation per `docs/planning/tier-1-hwm-fix.md`: add `source_system_id` filter to `get_current_hwm` in both spike `prepare_manifest.py` copies so shared-registry HWM poisoning is closed at query level.

## Result

Completed. Commit `63bcfdb`. 2 files changed, +38 / −14 lines.

## Changes

### `spikes/jdbc-sdp-a1/tasks/prepare_manifest.py`

- `get_current_hwm` signature: added `source_system_id: str` parameter.
- SQL WHERE clause: added `AND source_system_id = :source_system_id`.
- `args=` dict: added `"source_system_id": source_system_id` binding.
- Call site in `fresh` branch (line 151): now passes `src_catalog` from manifest row.
- `failed_only` branch reads HWM values directly from prior manifest rows (never calls `get_current_hwm`) — no call-site change needed.

### `spikes/jdbc-sdp-b/tasks/prepare_manifest.py`

- Same four changes to `get_current_hwm` (signature, SQL, args, docstring).
- Call site in `fresh` branch (line 221): now passes the module-level `SOURCE_CATALOG` constant.
- Only one call site in B; no `failed_only` branch in B's prepare_manifest.

## Verification

- `python3 -c "import ast; ast.parse(...)"` passes on both files.
- `grep` confirms `source_system_id = :source_system_id` in SQL literal of both files.
- `grep` confirms updated call-site signatures in both files.
- Runtime V1/V2/V3 tests per `docs/planning/tier-1-hwm-fix.md` are deferred to a separate validation run — not performed in this quick task.

## Out of scope (as planned)

- Tier 2 `load_group` column DDL migration.
- Tier 3 per-integration registry tables.
- LHP codegen integration of either spike.
- Runtime V1/V2/V3 proof (smoke probe, A1 retest without DELETE, B retest without DELETE).
- Backup table drops — 7-day retention window from `retest_1777031969`.

## Next actions

1. Run V1 smoke probe (fake `fake_federation` HWM row → trigger spike → confirm filter excludes it) — separate short validation.
2. Kick off LHP integration decision (Option A / B / C per `docs/ideas/spike-a1-vs-b-comparison.md`).
3. Schedule backup-table drops after 2026-05-01.
