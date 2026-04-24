"""
JDBC SDP A1 Spike — Dynamic Flow Pipeline
==========================================

This is the SDP (Spark Declarative Pipelines / Lakeflow) source file for the
JDBC federation spike. It dynamically generates one pipeline flow per pending
manifest row at plan time, reading source data from the `freesql_catalog` foreign
catalog and writing to the devtest_edp_bronze.jdbc_spike target schema.

Design pattern: DLT-META factory-closure approach — each table spec is wrapped
in a `make_flow()` factory that captures `spec` by value, then
`dp.table(fn, name=...)` is called as a FUNCTION (never as a decorator) so the
Python loop closure variable is captured correctly at factory call time.

Key constraint (Databricks docs):
  "Never use methods that save or write to files or tables as part of your
   pipeline dataset code."
All watermark state transitions (insert_new / mark_landed / mark_failed) are
therefore performed in the bookend tasks (tasks/prepare_manifest.py and
tasks/reconcile.py), NOT inside any flow body.

Upstream spec: docs/ideas/jdbc-execution-spike.md
Watermark contract: src/lhp/templates/load/jdbc_watermark_job.py.j2 (L2 §5.3)
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

# SDP injects a `spark` variable into the module scope at pipeline runtime.
# Calling getActiveSession() here is a belt-and-suspenders guard for local
# static analysis; at pipeline run time the injected `spark` takes precedence.
spark = SparkSession.getActiveSession()

# Pipeline-level configuration injected via the DAB pipeline resource's
# `configuration:` block (see resources/spike_workflow.yml).
#
# NOTE on run_id: DAB `pipeline_task` does NOT expose a per-run channel to
# push job parameters into pipeline configuration — pipeline configuration
# is fixed at DEPLOY time. The spike therefore operates on a serial-run
# assumption: at pipeline plan time we read ALL rows where
# `execution_status = 'pending'`, which are the rows prepare_manifest just
# inserted for the current run. Operator MUST wait for reconcile to
# transition pending → completed|failed before triggering another run.
# A prior crashed run that never reconciled would leak stale pending rows
# into the next run; surface this via validate.py count checks.
inject_failure: str = spark.conf.get("spike.inject_failure", "false")

# ---------------------------------------------------------------------------
# Read the pending manifest rows at PLAN TIME (module-level execution).
# This determines the set of flows that SDP will create for this pipeline run.
# ---------------------------------------------------------------------------
pending_df = spark.read.table(
    "devtest_edp_orchestration.jdbc_spike.manifest"
).filter(F.col("execution_status") == "pending")

# Collect at plan time — SDP evaluates module-level code to discover flows.
pending_specs: list[dict] = [row.asDict() for row in pending_df.collect()]


# ---------------------------------------------------------------------------
# Factory function — DLT-META pattern.
#
# WHY a factory instead of a decorator in a loop:
#   In Python, a loop variable is captured by REFERENCE in a closure. Using
#   the dp.table decorator syntax inside a `for spec in pending_specs:` loop
#   would mean every flow closure references the same `spec` variable — whichever value it
#   held at the end of the loop. The factory pattern sidesteps this by
#   creating a fresh scope per call: each `make_flow(spec)` call binds `spec`
#   as a local parameter of the factory, and the inner `_flow` closure captures
#   THAT local (not the loop variable). This is the approach validated by
#   DLT-META for dynamic multi-table pipelines.
# ---------------------------------------------------------------------------


def make_flow(spec: dict) -> None:
    """Register one SDP flow for a single pending manifest row.

    Args:
        spec: A manifest row dict with keys: source_schema, source_table,
              target_table, watermark_column, watermark_value_at_start.
    """

    # Defensive default-argument capture: `spec=spec` binds the current `spec`
    # value into the function signature's default, providing a second layer of
    # closure isolation in addition to the factory scope.
    def _flow(spec: dict = spec) -> "pyspark.sql.DataFrame":  # type: ignore[name-defined]
        source_fqn = f"freesql_catalog.{spec['source_schema']}.{spec['source_table']}"

        # Failure injection for T-tkf-04 testing: point the first flow at a
        # non-existent table so it fails at table-resolution time. Other flows
        # are unaffected — SDP continue-on-failure is the mechanism under test.
        if inject_failure == "true" and spec is pending_specs[0]:
            source_fqn = "freesql_catalog.does_not_exist.nope_nope"

        df = spark.read.table(source_fqn)

        watermark_col: str = spec["watermark_column"]
        hwm: str | None = spec.get("watermark_value_at_start")

        if hwm:
            # Incremental load — filter to rows strictly after the stored HWM.
            df = df.filter(F.col(watermark_col) > F.lit(hwm))

        # Return the DataFrame ONLY — no imperative Delta writes permitted here.
        # SDP handles the materialisation to devtest_edp_bronze.jdbc_spike.<target_table>.
        return df

    # Register the flow. dp.table() is called as a FUNCTION — NOT a decorator.
    # Decorator-in-loop would share the loop variable reference (closure trap).
    dp.table(
        _flow,
        name=spec["target_table"],
        comment=f"Spike flow for {spec['source_schema']}.{spec['source_table']}",
    )


# ---------------------------------------------------------------------------
# Main registration loop — one make_flow() call per pending manifest row.
# ---------------------------------------------------------------------------
for spec in pending_specs:
    make_flow(spec)

# NO imperative writes below — SDP forbids side effects in flow bodies.
# Watermark state transitions happen in tasks/reconcile.py.
