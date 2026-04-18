# TD-007 Phase A Local Validation

- Timestamp (UTC): 2026-04-18T14:07:20+00:00
- PySpark: 4.1.1
- Python: 3.12.9

| ID | Invariant | Verdict | Duration (s) | Notes |
|---|---|---|---|---|
| V1 | Run-scoped write + readback | PASS | 2.133 |  |
| V2 | Post-write stats match input | PASS | 0.550 |  |
| V3 | Post-write latency within 1.0s for 1000000 rows | PASS | 0.941 |  |
| V4 | Empty-batch write is readable | PASS | 0.718 |  |

## V1 — Run-scoped write + readback

- Verdict: **PASS**
- Duration: 2.133 s
- Details:
  - `landing_root`: `/var/folders/pt/vmvb6xrn4kb0k8ljqclxsg0m0000gn/T/td007-validation-mdmlf0vv/landing_v1`
  - `run_landing_path`: `/var/folders/pt/vmvb6xrn4kb0k8ljqclxsg0m0000gn/T/td007-validation-mdmlf0vv/landing_v1/_lhp_runs/job-123-task-456-attempt-0`
  - `parquet_files_written`: `['part-00000-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00001-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00002-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00003-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00004-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00005-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00006-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00007-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00008-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00009-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00010-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet', 'part-00011-d9fb0fa0-7d01-4819-bcd5-6d11f616ed01-c000.snappy.parquet']`
  - `expected_count`: `100`
  - `readback_count`: `100`
  - `ok`: `True`

## V2 — Post-write stats match input

- Verdict: **PASS**
- Duration: 0.550 s
- Details:
  - `run_landing_path`: `/var/folders/pt/vmvb6xrn4kb0k8ljqclxsg0m0000gn/T/td007-validation-mdmlf0vv/landing_v2/_lhp_runs/job-200-task-1-attempt-0`
  - `expected_row_count`: `10000`
  - `landed_row_count`: `10000`
  - `expected_max_hwm`: `2023-11-14 19:59:59`
  - `landed_max_hwm`: `2023-11-14 19:59:59`
  - `row_count_match`: `True`
  - `hwm_match`: `True`
  - `ok`: `True`

## V3 — Post-write latency within 1.0s for 1000000 rows

- Verdict: **PASS**
- Duration: 0.941 s
- Details:
  - `rows`: `1000000`
  - `budget_s`: `1.0`
  - `measured_s`: `0.1284286668524146`
  - `observed_row_count`: `1000000`
  - `observed_max_hwm`: `2023-11-26 06:59:59`
  - `ok`: `True`

## V4 — Empty-batch write is readable

- Verdict: **PASS**
- Duration: 0.718 s
- Details:
  - `run_landing_path`: `/var/folders/pt/vmvb6xrn4kb0k8ljqclxsg0m0000gn/T/td007-validation-mdmlf0vv/landing_v4/_lhp_runs/job-400-task-1-attempt-0`
  - `landed_count`: `0`
  - `count_matches_zero`: `True`
  - `read_succeeded_without_throw`: `True`
  - `error`: `None`
  - `ok`: `True`
