"""End-to-end smoke test for the EDP per-env catalog starter project.

Exercises ``Example_Projects/edp_lhp_starter/`` for all three envs (devtest,
qa, prod) and asserts:

- ``lhp generate`` succeeds (exit 0).
- Per-env catalog substitution lands the correct ``<env>_edp_<medallion>``
  catalog into both the generated DLT Python code AND the DAB pipeline
  resource files.
- Cross-catalog source reads resolve to the correct env's bronze/silver
  catalog when generating silver/gold.
- Watermark registry catalog (per-env Option B) lands in the JDBC
  extraction notebook.

This guards the C2 (per-env catalog refactor) closure of ADR-003 §Q5 and
the convention captured in ``Example_Projects/edp_lhp_starter/README.md``.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_STARTER_DIR = _REPO_ROOT / "Example_Projects" / "edp_lhp_starter"
_PIPELINE_CONFIG = "config/pipeline_config.yaml"


@pytest.mark.e2e
@pytest.mark.slow
class TestEdpLhpStarterGeneration:
    """Per-env code generation against the EDP starter project."""

    def _copy_starter(self, dest: Path) -> Path:
        """Clone the starter project into ``dest`` so ``lhp generate`` cannot
        pollute the committed copy with ``generated/`` / ``resources/lhp/``
        artefacts. Excludes any pre-existing generation output.
        """
        ignore = shutil.ignore_patterns("generated", "resources", ".lhp", "__pycache__")
        shutil.copytree(_STARTER_DIR, dest, ignore=ignore)
        return dest

    def _generate(self, project_dir: Path, env: str) -> subprocess.CompletedProcess:
        """Invoke the LHP CLI as a subprocess (matches real-user workflow)."""
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "lhp.cli.main",
                "generate",
                "-e",
                env,
                "--pipeline-config",
                _PIPELINE_CONFIG,
                "--force",
            ],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=120,
        )

    @pytest.mark.parametrize(
        "env,bronze,silver,gold,landing,wm_catalog,wm_schema",
        [
            (
                "devtest",
                "devtest_edp_bronze",
                "devtest_edp_silver",
                "devtest_edp_gold",
                "devtest_edp_landing",
                "metadata",
                "devtest_orchestration",
            ),
            (
                "qa",
                "qa_edp_bronze",
                "qa_edp_silver",
                "qa_edp_gold",
                "qa_edp_landing",
                "metadata",
                "qa_orchestration",
            ),
            (
                "prod",
                "prod_edp_bronze",
                "prod_edp_silver",
                "prod_edp_gold",
                "prod_edp_landing",
                "metadata",
                "prod_orchestration",
            ),
        ],
    )
    def test_per_env_catalog_substitution(
        self,
        tmp_path: Path,
        env: str,
        bronze: str,
        silver: str,
        gold: str,
        landing: str,
        wm_catalog: str,
        wm_schema: str,
    ):
        """Each env resolves its own ``<env>_edp_<medallion>`` catalogs.

        Verifies the substitution chain end-to-end:
            substitutions/<env>.yaml  →  pipeline YAML token  →  generated .py
                                      →  pipeline_config.yaml  →  resource .yml
        """
        project = self._copy_starter(tmp_path / "starter")

        result = self._generate(project, env)
        assert (
            result.returncode == 0
        ), f"lhp generate -e {env} failed:\nSTDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"

        # 1. Silver pipeline reads bronze from the correct env's catalog.
        # Source table is HumanResources.Department (Wumbo AdventureWorks
        # devtest source); bronze write_target table is named `department`.
        silver_py = project / "generated" / env / "edp_silver_curation" / "customer_silver.py"
        silver_src = silver_py.read_text()
        assert (
            f"{bronze}.bronze.department" in silver_src
        ), f"Silver py for {env} should read from {bronze}.bronze.department; got:\n{silver_src[:1000]}"

        # 2. Gold pipeline reads silver from the correct env's catalog.
        gold_py = project / "generated" / env / "edp_gold_marts" / "customer_orders_summary.py"
        gold_src = gold_py.read_text()
        assert (
            f"{silver}.silver.department" in gold_src
        ), f"Gold py for {env} should read from {silver}.silver.department; got:\n{gold_src[:1000]}"

        # 3. Bronze JDBC extract notebook binds the ADR-004 Option C registry
        #    shape: shared `metadata` catalog, per-env `<env>_orchestration`
        #    schema. See docs/adr/ADR-004-watermark-registry-placement.md.
        extract_py = (
            project
            / "generated"
            / env
            / "edp_bronze_jdbc_ingestion_extract"
            / "__lhp_extract_load_customer_jdbc.py"
        )
        extract_src = extract_py.read_text()
        assert (
            f'catalog="{wm_catalog}"' in extract_src
        ), f"Extract notebook for {env} must bind WatermarkManager to catalog={wm_catalog!r}; got:\n{extract_src[:2000]}"
        assert (
            f'schema="{wm_schema}"' in extract_src
        ), f"Extract notebook for {env} must bind WatermarkManager to schema={wm_schema!r}; got:\n{extract_src[:2000]}"

        # 3a. Tier 2 (U3): the composite ``load_group`` literal is
        #     ``{pipeline}::{flowgroup}`` and is env-independent — the same
        #     value lands identically in devtest/qa/prod because neither
        #     ``pipeline`` nor ``flowgroup`` participate in env substitution.
        assert (
            'load_group = "edp_bronze_jdbc_ingestion::customer_bronze"'
            in extract_src
        ), (
            f"Extract notebook for {env} must bind composite load_group "
            f"literal 'edp_bronze_jdbc_ingestion::customer_bronze'; got:\n"
            f"{extract_src[:2000]}"
        )

        # 4. Bronze JDBC extract notebook lands under the per-env landing
        #    volume root.
        assert (
            f"/Volumes/{landing}/landing/landing/department" in extract_src
        ), f"Extract notebook for {env} must write under /Volumes/{landing}/landing/landing/department/_lhp_runs/<uuid>/"

        # 5. DAB resource files bind the correct per-env catalog/schema.
        bronze_resource = (
            project / "resources" / "lhp" / "edp_bronze_jdbc_ingestion.pipeline.yml"
        )
        silver_resource = (
            project / "resources" / "lhp" / "edp_silver_curation.pipeline.yml"
        )
        gold_resource = project / "resources" / "lhp" / "edp_gold_marts.pipeline.yml"
        assert (
            f"catalog: {bronze}" in bronze_resource.read_text()
        ), f"Bronze resource must bind {bronze}"
        assert (
            f"catalog: {silver}" in silver_resource.read_text()
        ), f"Silver resource must bind {silver}"
        assert (
            f"catalog: {gold}" in gold_resource.read_text()
        ), f"Gold resource must bind {gold}"

    def test_starter_lhp_yaml_includes_all_three_layers(self):
        """Sanity guard so the starter does not silently lose a layer."""
        lhp_yaml = (_STARTER_DIR / "lhp.yaml").read_text()
        assert "02_bronze/**" in lhp_yaml
        assert "03_silver/**" in lhp_yaml
        assert "04_gold/**" in lhp_yaml

    def test_starter_databricks_yml_has_three_targets(self):
        """The 2-workspace layout (devtest dedicated, qa/prod shared) is the
        documented C2 reference shape; a future edit must not collapse it.
        """
        dab = (_STARTER_DIR / "databricks.yml").read_text()
        for target in ("devtest:", "qa:", "prod:"):
            assert target in dab, f"databricks.yml must declare target {target}"
