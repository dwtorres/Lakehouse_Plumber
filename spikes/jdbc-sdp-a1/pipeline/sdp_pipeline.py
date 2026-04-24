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
# Plan-time flow registration requires driver-side materialisation of
# the pending manifest rows so that dp.table() can be called once per
# spec with a factory closure. Three options were considered:
#   - collect()          — emits an SDP static-analysis warning but works
#   - toLocalIterator()  — raises "not supported when using file-based
#                          collect" inside SDP pipeline modules
#   - .take(n)           — requires knowing n up front, same warning
# The warning against collect() targets its use INSIDE @dp.table bodies;
# at module scope it is the supported path. Acceptable for the spike.
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

        # Failure injection for acceptance testing: a RUNTIME-only error on
        # the first flow. Earlier attempts at inject_failure pointed the
        # source at a non-existent table, but that fails at SDP ANALYZE
        # (graph-build) time — which aborts the entire pipeline update
        # before any flow runs. The goal of the test is to verify per-flow
        # runtime isolation, so the injection now uses a division-by-zero
        # expression added AFTER the source read: the query analyses
        # successfully (Spark infers an integer divide) but throws at
        # materialisation time, isolating the failure to this one flow.
        should_inject_runtime_failure = (
            inject_failure == "true" and spec is pending_specs[0]
        )

        # Oracle NUMBER columns without explicit precision map to
        # DECIMAL(38,10) in Databricks federation, but real Oracle values
        # (e.g. snowflake UIDs at 39 digits) overflow that precision and
        # raise NUMERIC_VALUE_OUT_OF_RANGE AT THE FEDERATION READ BOUNDARY
        # — before any Spark transformation (including CAST) has a chance
        # to run. CAST-on-read therefore does NOT work as a workaround.
        # Spark's `to_char` expects 2 args (format string) so Oracle's
        # native TO_CHAR(number) also fails to translate.
        #
        # Spike workaround: DESCRIBE the source and build a SELECT that
        # OMITS any column whose type is decimal(*). Real ingestion needs
        # a principled type-mapping strategy (federation-connection-level
        # numeric overrides, or per-table hand-maintained column schemas
        # with explicit Oracle VARCHAR2 casts committed at source view).
        schema_rows = spark.sql(f"DESCRIBE TABLE {source_fqn}").collect()
        select_items: list[str] = []
        skipped_numeric: list[str] = []
        for r in schema_rows:
            col_name = r["col_name"]
            if not col_name or col_name.startswith("#"):
                continue
            data_type = (r["data_type"] or "").lower()
            if data_type.startswith("decimal"):
                skipped_numeric.append(col_name)
                continue
            select_items.append(f"`{col_name}`")
        if skipped_numeric:
            print(
                f"[spike] {source_fqn}: skipping overflow-prone decimal cols: "
                + ", ".join(skipped_numeric)
            )
        query = f"SELECT {', '.join(select_items)} FROM {source_fqn}"
        df = spark.sql(query)

        watermark_col: str = spec["watermark_column"]
        hwm: str | None = spec.get("watermark_value_at_start")

        if hwm:
            # Incremental load — filter to rows strictly after the stored HWM.
            df = df.filter(F.col(watermark_col) > F.lit(hwm))

        if should_inject_runtime_failure:
            # Runtime-only failure: analyse OK, throw on data access.
            df = df.withColumn("__inject_fail", F.expr("1 / 0"))

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
