"""B2 for_each iteration payload — frozen contract between two templates.

The B2 ``execution_mode: for_each`` topology emits a single worker notebook
per flowgroup that is invoked N times by the DAB ``for_each_task`` (one per
LOAD action). The driver passes a per-iteration JSON payload via the
``__lhp_iteration`` widget that ``prepare_manifest.py.j2`` builds into
``dbutils.jobs.taskValues.set("iterations", …)``.

The ``B2_ITERATION_KEYS`` frozenset below is the single source of truth for
the payload shape. Both halves of the contract reference it:

* ``prepare_manifest.py.j2`` — emits an entry per LOAD action, every entry
  carrying every key in this set.
* ``jdbc_watermark_job.py.j2`` (for_each branch) — reads its per-iteration
  values via ``iteration["<key>"]`` for every key in this set that is
  per-action (everything except ``batch_id`` and ``manifest_table``, which
  are batch-scoped but still flow through the iteration payload for
  convenience).

Devtest 2026-04-26 incident (Anomaly A) traced a silent data-corruption bug
to a previous version of this contract that carried only seven keys; the
worker template was forced to embed action[0]'s ``jdbc_table`` /
``landing_path`` / ``watermark_column`` literals at codegen time, so every
iteration ran against action[0]'s source. The contract was widened to ten
keys and ``tests/test_b2_iteration_contract.py`` enforces the symmetry at
test time.
"""

from __future__ import annotations

# Order is preserved here purely for human readability; the constant is a
# frozenset because membership and set-arithmetic are the only operations
# that matter for the contract check.
B2_ITERATION_KEYS: frozenset[str] = frozenset(
    {
        # batch-scoped (constant across all iterations of one batch)
        "batch_id",
        "manifest_table",
        # per-action identity (used for manifest UPDATE WHERE-clauses + logs)
        "source_system_id",
        "schema_name",
        "table_name",
        "action_name",
        "load_group",
        # per-action source/destination (Anomaly A regression net) — these
        # MUST come from the iteration payload because the worker is shared
        # across every action in the flowgroup.
        "jdbc_table",
        "watermark_column",
        "landing_path",
    }
)


__all__ = ["B2_ITERATION_KEYS"]
