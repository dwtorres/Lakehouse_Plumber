# Databricks notebook source
# B2 for_each end-to-end devtest smoke — validates the full B2 codegen against
# a live Databricks workspace after `databricks bundle run` completes.
#
# Invariants exercised (V1-V5 per B2 rollout runbook + R12 second-run check):
#
#   V1 — Workflow shape: the most recent run of ``edp_b2_smoke_jdbc_workflow``
#        has exactly three tasks: prepare_manifest, for_each_ingest, validate.
#
#   V2 — for_each iterations: the ``for_each_ingest`` task spawned exactly 3
#        iterations and all reached ``result_state: SUCCESS``.
#
#   V3 — Manifest row count: ``b2_manifests`` carries exactly 3 rows for the
#        captured ``batch_id`` (one per load action; write actions do not appear
#        in the manifest).
#
#   V4 — Watermark rows: 3 rows in ``watermarks`` for the smoke
#        ``load_group`` with ``status = 'completed'`` and ``row_count > 0``,
#        each tied to the job run_id.
#
#   V5 — Validate task exit JSON: the ``validate`` task notebook output parses
#        as JSON with ``status = "pass"`` and ``expected = completed_n = 3``.
#
#   R12 — Second-run zero-duplicates (REQUIRED per Risks table): immediately
#         trigger a second DAB run; assert every action's ``row_count`` in
#         ``b2_manifests`` equals 0, proving strict ``>`` predicate excludes the
#         watermark boundary rows from being re-ingested.
#
# Invocation:
#
#   (A) Manual — import this file as a Databricks notebook, attach it to a
#       cluster, set the widgets listed below, and run all. The final cell
#       prints a JSON summary and sets a notebook exit value for Jobs capture.
#
#   (B) Jobs-submitted — submit via ``databricks jobs create`` pointing at this
#       notebook; pass widget overrides via ``notebook_params``.
#
# Requirements:
#   - Databricks REST API access (DATABRICKS_TOKEN or cluster OAuth).
#   - Unity Catalog read access on ``{wm_catalog}.{wm_schema}.watermarks`` and
#     ``{wm_catalog}.{wm_schema}.b2_manifests``.
#   - The smoke bundle has been deployed and ``edp_b2_smoke_jdbc_workflow`` run
#     once before notebook execution.
#   - For R12: the notebook triggers a second run via the Jobs API, so the
#     cluster's service principal needs ``CAN_MANAGE_RUN`` on the job.
#
# This notebook is READ-WRITE on its configured catalog/schema. On success it
# drops the b2_smoke_* bronze tables and deletes watermark + manifest rows
# keyed on the test source_system_id ``pg_supabase_aw_b2``. On FAIL it skips
# cleanup so the operator can inspect state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget defaults
# MAGIC
# MAGIC Override from the Jobs UI or via ``notebook_params`` when submitting
# MAGIC programmatically. Defaults target the conventional devtest layout.

# COMMAND ----------

dbutils.widgets.text(
    "bundle_root",
    "/Workspace/Users/<user>/.bundle/edp_lhp_starter/devtest",
    "(Optional) Workspace path to the deployed bundle root — used for path assertions",
)
dbutils.widgets.text("wm_catalog", "metadata", "Unity Catalog for watermark + manifest tables")
dbutils.widgets.text("wm_schema", "devtest_orchestration", "Schema for watermark + manifest tables")
dbutils.widgets.text(
    "load_group",
    "edp_b2_smoke_jdbc::b2_hr_smoke",
    "load_group composite key (<pipeline>::<flowgroup>)",
)
dbutils.widgets.text(
    "lhp_workspace_path",
    "",
    "(Optional) Absolute workspace path to prepend to sys.path when the lhp package is not installed",
)
dbutils.widgets.text("cleanup_on_success", "true", "Drop smoke bronze tables + delete test rows after PASS")

bundle_root = dbutils.widgets.get("bundle_root").strip().rstrip("/")
wm_catalog = dbutils.widgets.get("wm_catalog").strip()
wm_schema = dbutils.widgets.get("wm_schema").strip()
load_group = dbutils.widgets.get("load_group").strip()
lhp_workspace_path = dbutils.widgets.get("lhp_workspace_path").strip()
cleanup_on_success = dbutils.widgets.get("cleanup_on_success").strip().lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment setup

# COMMAND ----------

import json
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

if lhp_workspace_path and lhp_workspace_path not in sys.path:
    sys.path.insert(0, lhp_workspace_path)

spark.conf.set("spark.sql.session.timeZone", "UTC")

# Databricks REST client — uses the cluster's token transparently via the
# ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN`` env vars that Databricks injects
# on every cluster.
_host = (
    spark.conf.get("spark.databricks.workspaceUrl", None)
    or dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .get()
)
_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .get()
)

import urllib.request  # noqa: E402 — stdlib, always available


def _api(method: str, path: str, body: Optional[Dict] = None) -> Dict:
    url = f"{_host.rstrip('/')}/api/2.1{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {_token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------


def _timed(fn) -> Dict[str, Any]:
    start = time.perf_counter()
    try:
        details = fn()
        elapsed = time.perf_counter() - start
        return {
            "verdict": "PASS" if details.get("ok") else "FAIL",
            "duration_s": round(elapsed, 3),
            "details": details,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001 — surface all into the report
        elapsed = time.perf_counter() - start
        return {
            "verdict": "ERROR",
            "duration_s": round(elapsed, 3),
            "details": {},
            "error": f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=10)}",
        }


def _get_job_id(job_name: str) -> int:
    resp = _api("GET", f"/jobs/list?name={urllib.request.quote(job_name)}")
    jobs = resp.get("jobs", [])
    if not jobs:
        raise RuntimeError(f"Job '{job_name}' not found — was the bundle deployed?")
    return int(jobs[0]["job_id"])


def _get_latest_run(job_id: int) -> Dict:
    resp = _api("GET", f"/jobs/runs/list?job_id={job_id}&limit=1&active_only=false")
    runs = resp.get("runs", [])
    if not runs:
        raise RuntimeError(f"No runs found for job_id={job_id}")
    return runs[0]


def _wait_for_run(run_id: int, poll_interval_s: int = 10, timeout_s: int = 600) -> Dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        run = _api("GET", f"/jobs/runs/get?run_id={run_id}")
        life = run.get("state", {}).get("life_cycle_state", "")
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            return run
        time.sleep(poll_interval_s)
    raise TimeoutError(f"run_id={run_id} did not terminate within {timeout_s}s")


def _cleanup() -> None:
    bronze_catalog = "devtest_edp_bronze"
    bronze_schema = "bronze"
    tables = ["b2_smoke_department", "b2_smoke_shift", "b2_smoke_jobcandidate"]
    for tbl in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {bronze_catalog}.{bronze_schema}.{tbl}")
        except Exception:  # noqa: BLE001 — best-effort
            pass
    try:
        spark.sql(
            f"DELETE FROM {wm_catalog}.{wm_schema}.watermarks"
            f" WHERE source_system_id = 'pg_supabase_aw_b2'"
        )
    except Exception:  # noqa: BLE001
        pass
    try:
        spark.sql(
            f"DELETE FROM {wm_catalog}.{wm_schema}.b2_manifests"
            f" WHERE load_group = '{load_group}'"
        )
    except Exception:  # noqa: BLE001
        pass


# COMMAND ----------

# MAGIC %md
# MAGIC ## Capture first-run context

# COMMAND ----------

_job_name = "edp_b2_smoke_jdbc_workflow"
_job_id = _get_job_id(_job_name)
_first_run = _get_latest_run(_job_id)
_first_run_id = _first_run["run_id"]

# Extract batch_id from the prepare_manifest task output taskValues.
# prepare_manifest emits: dbutils.jobs.taskValues.set(key="iterations", value=...)
# The batch_id is encoded in the manifest rows we query in V3.
_batch_id: Optional[str] = None
try:
    _tasks_resp = _api("GET", f"/jobs/runs/get?run_id={_first_run_id}&include_history=true")
    for _task in _tasks_resp.get("tasks", []):
        if _task.get("task_key") == "prepare_manifest":
            # Attempt to retrieve batch_id from task notebook output.
            _nb_output = _task.get("notebook_output", {})
            _result_str = _nb_output.get("result", "")
            try:
                _task_result = json.loads(_result_str)
                _batch_id = _task_result.get("batch_id")
            except (json.JSONDecodeError, TypeError):
                pass
except Exception:  # noqa: BLE001 — non-fatal; V3 falls back to load_group filter
    pass

print(f"first_run_id={_first_run_id}  batch_id={_batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V1 — Workflow shape: 3 tasks

# COMMAND ----------


def v1_workflow_shape() -> Dict[str, Any]:
    run_detail = _api("GET", f"/jobs/runs/get?run_id={_first_run_id}")
    task_keys = sorted(t["task_key"] for t in run_detail.get("tasks", []))
    expected_keys = sorted(["prepare_manifest", "for_each_ingest", "validate"])
    return {
        "run_id": _first_run_id,
        "observed_task_keys": task_keys,
        "expected_task_keys": expected_keys,
        "ok": task_keys == expected_keys,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 — for_each iterations: 3 spawned, all SUCCESS

# COMMAND ----------


def v2_for_each_iterations() -> Dict[str, Any]:
    run_detail = _api(
        "GET",
        f"/jobs/runs/get?run_id={_first_run_id}&include_history=true",
    )
    for_each_task = next(
        (t for t in run_detail.get("tasks", []) if t["task_key"] == "for_each_ingest"),
        None,
    )
    if for_each_task is None:
        return {"ok": False, "error": "for_each_ingest task not found in run"}

    iterations = for_each_task.get("iterations", [])
    total = len(iterations)
    succeeded = [it for it in iterations if it.get("state", {}).get("result_state") == "SUCCESS"]
    failed = [it for it in iterations if it.get("state", {}).get("result_state") != "SUCCESS"]

    return {
        "total_iterations": total,
        "succeeded_n": len(succeeded),
        "failed_iterations": [
            {
                "index": it.get("iteration_index"),
                "result_state": it.get("state", {}).get("result_state"),
            }
            for it in failed
        ],
        "ok": total == 3 and len(succeeded) == 3,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## V3 — Manifest row count: 3 rows for this batch

# COMMAND ----------


def v3_manifest_rows() -> Dict[str, Any]:
    # Prefer batch_id filter if we captured it; fall back to load_group + run_id prefix.
    if _batch_id:
        query = f"""
            SELECT COUNT(*) AS cnt, COLLECT_LIST(action_name) AS actions
            FROM {wm_catalog}.{wm_schema}.b2_manifests
            WHERE batch_id = '{_batch_id}'
        """
        context = {"filter": "batch_id", "batch_id": _batch_id}
    else:
        query = f"""
            SELECT COUNT(*) AS cnt, COLLECT_LIST(action_name) AS actions
            FROM {wm_catalog}.{wm_schema}.b2_manifests
            WHERE load_group = '{load_group}'
              AND run_id LIKE 'job-%'
        """
        context = {"filter": "load_group+run_id_prefix", "load_group": load_group}

    row = spark.sql(query).first()
    cnt = int(row["cnt"])
    actions = sorted(row["actions"] or [])

    return {
        **context,
        "observed_count": cnt,
        "expected_count": 3,
        "actions": actions,
        "ok": cnt == 3,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## V4 — Watermark rows: 3 rows completed, each row_count > 0

# COMMAND ----------


def v4_watermark_rows() -> Dict[str, Any]:
    rows = spark.sql(
        f"""
        SELECT action_name, status, row_count, run_id
        FROM {wm_catalog}.{wm_schema}.watermarks
        WHERE load_group = '{load_group}'
          AND run_id LIKE 'job-%'
        ORDER BY action_name
        """
    ).collect()

    total = len(rows)
    completed = [r for r in rows if r["status"] == "completed"]
    nonzero = [r for r in completed if int(r["row_count"]) > 0]

    return {
        "total_rows": total,
        "completed_n": len(completed),
        "nonzero_row_count_n": len(nonzero),
        "rows": [
            {
                "action_name": r["action_name"],
                "status": r["status"],
                "row_count": int(r["row_count"]),
                "run_id": r["run_id"],
            }
            for r in rows
        ],
        "ok": total == 3 and len(completed) == 3 and len(nonzero) == 3,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## V5 — Validate task exit JSON: status "pass", expected = completed_n = 3

# COMMAND ----------


def v5_validate_task_exit_json() -> Dict[str, Any]:
    run_detail = _api(
        "GET",
        f"/jobs/runs/get?run_id={_first_run_id}&include_history=true",
    )
    validate_task = next(
        (t for t in run_detail.get("tasks", []) if t["task_key"] == "validate"),
        None,
    )
    if validate_task is None:
        return {"ok": False, "error": "validate task not found in run"}

    nb_output = validate_task.get("notebook_output", {})
    result_str = nb_output.get("result", "")

    try:
        summary = json.loads(result_str)
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "ok": False,
            "error": f"validate task output is not valid JSON: {exc}",
            "raw_output": result_str[:500],
        }

    status = summary.get("status")
    expected = summary.get("expected")
    completed_n = summary.get("completed_n")

    return {
        "status": status,
        "expected": expected,
        "completed_n": completed_n,
        "raw_summary_keys": sorted(summary.keys()),
        "ok": status == "pass" and expected == 3 and completed_n == 3,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## R12 — Second-run zero-duplicates (strict > predicate)

# COMMAND ----------


def r12_second_run_zero_duplicates() -> Dict[str, Any]:
    # Trigger a second synchronous run of the same job.
    run_resp = _api("POST", "/jobs/runs/now", {"job_id": _job_id})
    second_run_id = run_resp["run_id"]
    print(f"R12: triggered second run_id={second_run_id} — waiting for completion...")

    second_run = _wait_for_run(second_run_id, poll_interval_s=15, timeout_s=600)
    result_state = second_run.get("state", {}).get("result_state")
    if result_state not in ("SUCCESS",):
        return {
            "ok": False,
            "second_run_id": second_run_id,
            "result_state": result_state,
            "error": (
                f"Second run did not succeed (result_state={result_state}). "
                "Cannot assert zero-duplicate row counts."
            ),
        }

    # Capture batch_id from second run's prepare_manifest output.
    second_batch_id: Optional[str] = None
    try:
        second_detail = _api(
            "GET",
            f"/jobs/runs/get?run_id={second_run_id}&include_history=true",
        )
        for task in second_detail.get("tasks", []):
            if task.get("task_key") == "prepare_manifest":
                nb_output = task.get("notebook_output", {})
                try:
                    task_result = json.loads(nb_output.get("result", ""))
                    second_batch_id = task_result.get("batch_id")
                except (json.JSONDecodeError, TypeError):
                    pass
    except Exception:  # noqa: BLE001
        pass

    # Assert every load action's row_count = 0 in b2_manifests for second run.
    if second_batch_id:
        filter_clause = f"batch_id = '{second_batch_id}'"
    else:
        filter_clause = (
            f"load_group = '{load_group}'"
            f" AND run_id LIKE 'job-%'"
            f" AND run_id != '{_first_run_id}'"
        )

    rows = spark.sql(
        f"""
        SELECT action_name, row_count
        FROM {wm_catalog}.{wm_schema}.b2_manifests
        WHERE {filter_clause}
        ORDER BY action_name
        """
    ).collect()

    nonzero = [r for r in rows if int(r["row_count"]) != 0]

    return {
        "second_run_id": second_run_id,
        "second_batch_id": second_batch_id,
        "result_state": result_state,
        "action_row_counts": [
            {"action_name": r["action_name"], "row_count": int(r["row_count"])} for r in rows
        ],
        "nonzero_on_second_run": [
            {"action_name": r["action_name"], "row_count": int(r["row_count"])} for r in nonzero
        ],
        # R12 passes only when all 3 load actions return row_count = 0.
        "ok": len(rows) == 3 and len(nonzero) == 0,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run all invariants + emit result

# COMMAND ----------

results: Dict[str, Any] = {
    "meta": {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "wm_catalog": wm_catalog,
        "wm_schema": wm_schema,
        "load_group": load_group,
        "first_run_id": _first_run_id,
        "batch_id": _batch_id,
        "job_name": _job_name,
    },
    "invariants": {},
}

for invariant_id, fn in [
    ("V1", v1_workflow_shape),
    ("V2", v2_for_each_iterations),
    ("V3", v3_manifest_rows),
    ("V4", v4_watermark_rows),
    ("V5", v5_validate_task_exit_json),
    ("R12", r12_second_run_zero_duplicates),
]:
    print(f"Running {invariant_id}...")
    results["invariants"][invariant_id] = _timed(fn)
    verdict = results["invariants"][invariant_id]["verdict"]
    print(f"  {invariant_id}: {verdict}")

failed = [
    invariant_id
    for invariant_id, payload in results["invariants"].items()
    if payload["verdict"] != "PASS"
]
results["overall_verdict"] = "PASS" if not failed else "FAIL"
results["failed_invariants"] = failed

# Summary fields for V5 compatibility check — top-level for easy Jobs capture.
_all_v_verdicts = [
    v for k, v in results["invariants"].items() if k.startswith("V")
]
results["status"] = "pass" if not failed else "fail"
results["expected"] = 3
results["completed_n"] = (
    results["invariants"]["V2"].get("details", {}).get("succeeded_n", 0)
    if results["invariants"].get("V2", {}).get("verdict") == "PASS"
    else 0
)

print(json.dumps(results, indent=2, default=str))

if results["overall_verdict"] == "PASS" and cleanup_on_success:
    print("Cleaning up smoke test artifacts...")
    _cleanup()
    print("Cleanup complete.")
else:
    print(
        "Skipping cleanup — overall_verdict is FAIL or cleanup_on_success=false. "
        "Inspect state before running cleanup commands from the runbook."
    )

# Emit a notebook exit value so ``databricks jobs get-run-output`` captures a
# machine-readable summary without parsing notebook cell output.
dbutils.notebook.exit(json.dumps(results, default=str))
